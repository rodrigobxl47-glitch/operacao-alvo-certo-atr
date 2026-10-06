import asyncio, json, os, secrets, hashlib, base64, time
from pathlib import Path
from dataclasses import dataclass, asdict, field
from typing import List
from urllib.parse import urlencode

import httpx, websockets
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.middleware.sessions import SessionMiddleware

BASE_DIR=Path(__file__).resolve().parent
STATIC_DIR=BASE_DIR/'static'

app=FastAPI(title='Operação Alvo Certo ATR - Forex v5.2')
app.add_middleware(SessionMiddleware,secret_key=os.getenv('SESSION_SECRET',secrets.token_hex(32)),https_only=True,same_site='lax')

PUBLIC_WS='wss://api.derivws.com/trading/v1/options/ws/public'
REST_BASE='https://api.derivws.com'
OAUTH_AUTH='https://auth.deriv.com/oauth2/auth'
OAUTH_TOKEN='https://auth.deriv.com/oauth2/token'
CLIENT_ID=os.getenv('DERIV_CLIENT_ID','')
REDIRECT_URI=os.getenv('DERIV_REDIRECT_URI','https://operacao-alvo-certo-atr.onrender.com/auth/callback')

FOREX_PAIRS=['EUR/USD','GBP/USD','USD/JPY','USD/CAD','AUD/USD','USD/CHF','NZD/USD']
FOREX_ALIASES={
    'EUR/USD':['eurusd','frxeurusd','eur/usd'],
    'GBP/USD':['gbpusd','frxgbpusd','gbp/usd'],
    'USD/JPY':['usdjpy','frxusdjpy','usd/jpy'],
    'USD/CAD':['usdcad','frxusdcad','usd/cad'],
    'AUD/USD':['audusd','frxaudusd','aud/usd'],
    'USD/CHF':['usdchf','frxusdchf','usd/chf'],
    'NZD/USD':['nzdusd','frxnzdusd','nzd/usd'],
}
NEWS_URL='https://nfs.faireconomy.media/ff_calendar_thisweek.xml'
NEWS_INTERVAL=300
HISTORY_FILE=BASE_DIR/'history.json'

class Config(BaseModel):
    banca_inicial:float=1000.0
    percentual_entrada:float=1.0
    entrada_tipo:str='percentual'
    valor_entrada:float=1.0
    operacao_automatica:bool=False
    stop_gain:float=5.0
    stop_loss:float=5.0
    max_entradas:int=5
    duracao_minutos:int=1

class AccountChoice(BaseModel):
    account_id:str

config=Config()
state={
    'running':False,'scanning':False,'balance':1000.0,'start_balance':1000.0,
    'entries':0,'signals':[],'history':[],'open_trade':None,'last_scan':None,
    'status':'PARADO','auto_status':'DESLIGADA','diagnostics':{},'news':[],
    'auto_trade':False,'deriv_status':'OK','scanner_heartbeat':0,'news_updated_at':0,'news_status':'AGUARDANDO ATUALIZAÇÃO'
}
runtime={'token':None,'account_id':None,'account_type':'','currency':'USD','last_auto_signal_key':None}
scanner_task=None
scan_lock=asyncio.Lock()
trade_lock=asyncio.Lock()

# v5.1: uma conexão pública persistente + fila serializada.
_public_ws=None
_public_connect_lock=asyncio.Lock()
_public_request_lock=asyncio.Lock()
_public_req_id=10000
_public_backoff_until=0.0
_active_cache={'time':0.0,'data':[]}
_candle_cache={}

@dataclass
class Candle:
    epoch:int; open:float; high:float; low:float; close:float

@dataclass
class Signal:
    symbol:str; name:str; direction:str; price:float; analysis:str; confirmation:str; combo:str
    reasons:List[str]=field(default_factory=list)
    support:float=0.0; resistance:float=0.0
    timeframes:List[str]=field(default_factory=list)
    signal_epoch:int=0; touch_epoch:int=0

def body(c): return abs(c.close-c.open)
def rng(c): return max(c.high-c.low,1e-12)
def bull(c): return c.close>c.open
def bear(c): return c.close<c.open
def body_low(c): return min(c.open,c.close)
def body_high(c): return max(c.open,c.close)
def avg_range(cs): return sum(rng(x) for x in cs)/max(len(cs),1)
def near(a,b,tol): return abs(a-b)<=tol

def candle_pattern(cs,side):
    if len(cs)<4:return None
    a,b,c=cs[-1],cs[-2],cs[-3]
    ar=max(avg_range(cs[-20:]),1e-12);eps=ar*.08
    if side=='CALL':
        lowwick=body_low(a)-a.low;upwick=a.high-body_high(a)
        if bull(a) and lowwick>=2*max(body(a),eps) and upwick<=max(body(a),eps)*.7:return 'Martelo (Hammer)'
        if bear(b) and bull(a) and a.open<=b.close and a.close>=b.open:return 'Engolfo de Alta (Bullish Engulfing)'
        if bear(c) and body(b)<=body(c)*.5 and bull(a) and a.close>(c.open+c.close)/2:return 'Estrela da Manhã (Morning Star)'
        if bear(b) and bull(a) and a.close>(b.open+b.close)/2 and a.close<b.open:return 'Piercing Line'
        if bear(b) and bull(a) and body_low(a)>=body_low(b) and body_high(a)<=body_high(b):return 'Harami de Alta (Bullish Harami)'
        if all(bull(x) for x in cs[-3:]) and cs[-1].close>cs[-2].close>cs[-3].close:return 'Três Soldados Brancos (Three White Soldiers)'
        if bull(a) and body(a)/rng(a)>=.85:return 'Marubozu de Alta (Bullish Marubozu)'
    if side=='PUT':
        lowwick=body_low(a)-a.low;upwick=a.high-body_high(a)
        if bear(a) and upwick>=2*max(body(a),eps) and lowwick<=max(body(a),eps)*.7:return 'Estrela Cadente (Shooting Star)'
        if bull(b) and bear(a) and a.open>=b.close and a.close<=b.open:return 'Engolfo de Baixa (Bearish Engulfing)'
        if bull(c) and body(b)<=body(c)*.5 and bear(a) and a.close<(c.open+c.close)/2:return 'Estrela da Noite (Evening Star)'
        if bull(b) and bear(a) and a.close<(b.open+b.close)/2 and a.close>b.open:return 'Dark Cloud Cover'
        if bull(b) and bear(a) and body_low(a)>=body_low(b) and body_high(a)<=body_high(b):return 'Harami de Baixa (Bearish Harami)'
        if all(bear(x) for x in cs[-3:]) and cs[-1].close<cs[-2].close<cs[-3].close:return 'Três Corvos Negros (Three Black Crows)'
        if bear(a) and body(a)/rng(a)>=.85:return 'Marubozu de Baixa (Bearish Marubozu)'
    return None

def pivots(cs,span=2):
    hi=[];lo=[]
    for i in range(span,len(cs)-span):
        w=cs[i-span:i+span+1]
        if cs[i].high>=max(x.high for x in w):hi.append((i,cs[i].high))
        if cs[i].low<=min(x.low for x in w):lo.append((i,cs[i].low))
    return hi,lo

def cluster_levels(points,tol):
    groups=[]
    for idx,price in points:
        found=None
        for g in groups:
            if near(price,g['level'],tol):
                found=g;break
        if found:
            found['prices'].append(price);found['indices'].append(idx)
            found['level']=sum(found['prices'])/len(found['prices'])
        else:groups.append({'level':price,'prices':[price],'indices':[idx]})
    return groups

def strong_support_resistance(cs):
    hi,lo=pivots(cs);ar=max(avg_range(cs[-40:]),1e-12);tol=ar*.45
    supports=[g for g in cluster_levels(lo,tol) if len(g['prices'])>=2]
    resistances=[g for g in cluster_levels(hi,tol) if len(g['prices'])>=2]
    support=min((g['level'] for g in supports),default=min(x.low for x in cs))
    resistance=max((g['level'] for g in resistances),default=max(x.high for x in cs))
    return support,resistance

def trend_direction(cs):
    hi,lo=pivots(cs)
    if len(hi)<2 or len(lo)<2:return 'NEUTRA'
    if hi[-1][1]>hi[-2][1] and lo[-1][1]>lo[-2][1]:return 'ALTA'
    if hi[-1][1]<hi[-2][1] and lo[-1][1]<lo[-2][1]:return 'BAIXA'
    return 'NEUTRA'

def abc_confluence(cs,side):
    hi,lo=pivots(cs)
    if len(hi)<2 or len(lo)<2:return False
    if side=='CALL':return hi[-1][1]>hi[-2][1] and lo[-1][1]>lo[-2][1]
    return hi[-1][1]<hi[-2][1] and lo[-1][1]<lo[-2][1]

def sr_third_touch_previous(cs,side,support,resistance):
    touch_idx=len(cs)-2;hist=cs[:touch_idx];hi,lo=pivots(hist);pts=lo if side=='CALL' else hi
    ar=max(avg_range(cs[-30:]),1e-12);tol=ar*.55;level=support if side=='CALL' else resistance
    prior=sum(1 for _,p in pts if near(p,level,tol))
    touch=cs[touch_idx].low if side=='CALL' else cs[touch_idx].high
    return prior>=2 and near(touch,level,ar*.75)

def trend_third_touch_previous(cs,side):
    touch_idx=len(cs)-2;base=cs[:touch_idx];hi,lo=pivots(base);pts=(lo if side=='CALL' else hi)[-5:]
    if len(pts)<2:return False
    ar=max(avg_range(cs[-30:]),1e-12);tol=ar*.8
    for j in range(len(pts)-1,0,-1):
        i1,p1=pts[j-1];i2,p2=pts[j]
        if i2==i1:continue
        slope=(p2-p1)/(i2-i1)
        if side=='CALL' and slope<=0:continue
        if side=='PUT' and slope>=0:continue
        projected=p2+slope*(touch_idx-i2)
        touch=cs[touch_idx].low if side=='CALL' else cs[touch_idx].high
        if abs(touch-projected)<=tol:return True
    return False

def analyze(symbol,name,m1,m5,m15):
    if min(len(m1),len(m5),len(m15))<110:return None
    m1,m5,m15=m1[-110:],m5[-110:],m15[-110:]
    support,resistance=strong_support_resistance(m1);candidates=[]
    for side in ('CALL','PUT'):
        pat=candle_pattern(m1,side)
        if not pat:continue
        touch_epoch,signal_epoch=m1[-2].epoch,m1[-1].epoch
        if sr_third_touch_previous(m1,side,support,resistance):
            combo='COMBO SUPORTE' if side=='CALL' else 'COMBO RESISTÊNCIA'
            reasons=['2 contatos anteriores no nível forte','3º toque na penúltima vela M1',f'Próxima vela confirmou: {pat}']
            candidates.append((3,side,combo,f'{combo} • 3º toque • próxima vela confirmou',reasons,pat,touch_epoch,signal_epoch))
        expected='ALTA' if side=='CALL' else 'BAIXA'
        t1,t5,t15=trend_direction(m1),trend_direction(m5),trend_direction(m15)
        if t1==expected and (t5==expected or t15==expected) and abc_confluence(m1,side) and trend_third_touch_previous(m1,side):
            combo='COMBO TENDÊNCIA ALTA' if side=='CALL' else 'COMBO TENDÊNCIA BAIXA'
            reasons=[f'Tendência M1: {t1}',f'Contexto M5/M15: {t5}/{t15}','ABC confirmado','3º toque na penúltima vela M1',f'Próxima vela confirmou: {pat}']
            candidates.append((2,side,combo,f'Tendência {expected} • ABC • 3º toque • próxima vela confirmou',reasons,pat,touch_epoch,signal_epoch))
    if not candidates:return None
    _,side,combo,analysis,reasons,pat,touch_epoch,signal_epoch=max(candidates,key=lambda z:z[0])
    return Signal(symbol,name,side,m1[-1].close,analysis,pat,combo,reasons,support,resistance,['M1','M5','M15'],signal_epoch,touch_epoch)

def is_429(e):
    s=str(e).lower()
    return '429' in s or 'too many' in s or 'rate limit' in s

async def close_public_ws():
    global _public_ws
    if _public_ws is not None:
        try:await _public_ws.close()
        except Exception:pass
    _public_ws=None

async def ensure_public_ws():
    global _public_ws,_public_backoff_until
    delay=_public_backoff_until-time.monotonic()
    if delay>0:
        state['deriv_status']=f'LIMITADA 429 • aguardando {int(delay)+1}s'
        await asyncio.sleep(delay)
    async with _public_connect_lock:
        if _public_ws is not None:
            return _public_ws
        try:
            _public_ws=await websockets.connect(PUBLIC_WS,ping_interval=25,ping_timeout=20,open_timeout=15,close_timeout=5,max_queue=64)
            state['deriv_status']='OK'
            return _public_ws
        except Exception:
            _public_ws=None
            raise

async def ws_public(payload,req_id=None):
    global _public_req_id,_public_backoff_until
    async with _public_request_lock:
        last=None
        for attempt in range(4):
            try:
                ws=await ensure_public_ws()
                _public_req_id+=1;rid=_public_req_id
                p=dict(payload);p['req_id']=rid
                await ws.send(json.dumps(p))
                while True:
                    d=json.loads(await asyncio.wait_for(ws.recv(),timeout=25))
                    if d.get('req_id')!=rid:continue
                    if 'error' in d:raise RuntimeError(d['error'].get('message','Erro Deriv'))
                    state['deriv_status']='OK'
                    await asyncio.sleep(.12)
                    return d
            except asyncio.CancelledError:raise
            except Exception as e:
                last=e;await close_public_ws()
                if is_429(e):
                    wait=min(5*(2**attempt),40)
                    _public_backoff_until=time.monotonic()+wait
                    state['deriv_status']=f'LIMITADA 429 • aguardando {wait}s'
                    if config.operacao_automatica:state['auto_status']=f'AGUARDANDO DERIV • 429 • {wait}s'
                    await asyncio.sleep(wait)
                else:
                    await asyncio.sleep(min(1+attempt,4))
        raise RuntimeError(f'Deriv indisponível: {last}')

async def active_symbols():
    now=time.monotonic()
    if _active_cache['data'] and now-_active_cache['time']<1800:return _active_cache['data']
    data=(await ws_public({'active_symbols':'brief','contract_type':['CALL','PUT']})).get('active_symbols',[])
    _active_cache.update({'time':now,'data':data})
    return data

def sym_fields(a):
    return a.get('underlying_symbol') or a.get('symbol'),a.get('underlying_symbol_name') or a.get('display_name') or a.get('underlying_symbol') or a.get('symbol')


async def candles(symbol,granularity,count=111):
    # Evita repetir a mesma consulta dentro do mesmo candle fechado.
    bucket=(int(time.time())-2)//granularity
    key=(symbol,granularity)
    cached=_candle_cache.get(key)
    if cached and cached[0]==bucket:return cached[1]
    d=await ws_public({'ticks_history':symbol,'adjust_start_time':1,'count':count,'end':'latest','granularity':granularity,'style':'candles'})
    out=[];now=int(time.time())
    for c in d.get('candles',[]):
        try:
            x=Candle(int(c['epoch']),float(c['open']),float(c['high']),float(c['low']),float(c['close']))
            if x.epoch+granularity<=now:out.append(x)
        except Exception:pass
    out=out[-110:];_candle_cache[key]=(bucket,out)
    return out

def norm_pair(v):
    return ''.join(ch for ch in str(v or '').lower() if ch.isalnum())

def resolve_forex_symbols(active):
    found={}
    for a in active or []:
        market=' '.join(str(a.get(k,'')) for k in ('market','market_display_name','submarket','submarket_display_name')).lower()
        vals=[a.get(k) for k in ('symbol','display_name','underlying_symbol_name','underlying_symbol','name')]
        norms=[norm_pair(v) for v in vals if v]
        for pair,aliases in FOREX_ALIASES.items():
            if pair in found: continue
            for alias in aliases:
                na=norm_pair(alias)
                if any(na==n or na in n for n in norms):
                    # Prefer explicit Forex market when the API supplies it; otherwise accept the exact pair alias.
                    if not market or 'forex' in market or 'currenc' in market or na in ''.join(norms):
                        found[pair]=a
                        break
    return [(pair,found[pair]) for pair in FOREX_PAIRS if pair in found]

def load_history():
    try:
        if HISTORY_FILE.exists():
            data=json.loads(HISTORY_FILE.read_text(encoding='utf-8'))
            if isinstance(data,list): state['history']=data[:100]
    except Exception as e:
        print(f'[ATR] Histórico não carregado: {e}',flush=True)

def save_history():
    try:
        tmp=HISTORY_FILE.with_suffix('.tmp')
        tmp.write_text(json.dumps(state['history'][:100],ensure_ascii=False),encoding='utf-8')
        tmp.replace(HISTORY_FILE)
    except Exception as e:
        print(f'[ATR] Histórico não salvo: {e}',flush=True)

async def update_news(force=False):
    now=time.time()
    if not force and now-state.get('news_updated_at',0)<NEWS_INTERVAL:
        return state['news']
    try:
        async with httpx.AsyncClient(timeout=12,headers={'User-Agent':'ATR-Operacao-Alvo-Certo/5.2'}) as client:
            r=await client.get(NEWS_URL)
        if r.status_code>=400: raise RuntimeError(f'HTTP {r.status_code}')
        import xml.etree.ElementTree as ET
        root=ET.fromstring(r.text)
        items=[]
        for ev in root.findall('.//event'):
            title=(ev.findtext('title') or '').strip()
            country=(ev.findtext('country') or '').strip().upper()
            date=(ev.findtext('date') or '').strip()
            tm=(ev.findtext('time') or '').strip()
            impact=(ev.findtext('impact') or '').strip().lower()
            forecast=(ev.findtext('forecast') or '').strip()
            previous=(ev.findtext('previous') or '').strip()
            if not title: continue
            items.append({'title':title,'country':country,'impact':impact,'time':f'{date} {tm}'.strip(),'summary':f'{country} • Impacto: {impact or "n/d"} • Prev.: {previous or "—"} • Previsto: {forecast or "—"}'})
        state['news']=items[:40]
        state['news_status']=f'{len(state["news"])} eventos carregados'
        state['news_updated_at']=now
    except Exception as e:
        state['news_status']=f'Calendário indisponível • {str(e)[:90]}'
        state['news_updated_at']=now
    return state['news']

load_history()

async def analyze_one(a,sem):
    async with sem:
        symbol,name=sym_fields(a)
        if not symbol:return None
        try:
            m1=await candles(symbol,60)
            m5=await candles(symbol,300)
            m15=await candles(symbol,900)
            return analyze(symbol,name,m1,m5,m15)
        except Exception as e:
            print(f'[ATR] Falha {symbol}: {e}',flush=True);return None

async def scan_once():
    if scan_lock.locked():return state['signals']
    async with scan_lock:
        state['scanning']=True;state['status']='ESCANEANDO';state['scanner_heartbeat']=int(time.time())
        try:
            active=await active_symbols()
            resolved=resolve_forex_symbols(active)
            sem=asyncio.Semaphore(1)
            results=[]
            for pair,a in resolved:
                r=await analyze_one(a,sem)
                if r: r.name=pair
                results.append(r)
            sig=[x for x in results if x];sig.sort(key=lambda x:x.signal_epoch,reverse=True)
            found=[pair for pair,_ in resolved]
            missing=[p for p in FOREX_PAIRS if p not in found]
            state['signals']=[asdict(x) for x in sig[:25]]
            state['last_scan']=int(time.time());state['scanner_heartbeat']=state['last_scan']
            state['diagnostics']={'pares_forex':len(resolved),'pares_solicitados':FOREX_PAIRS,'pares_encontrados':found,'pares_nao_encontrados':missing,'sinais':len(sig),'candles_por_tf':110,'timeframes':['M1','M5','M15'],'base':'Combos independentes','confluencias':['ABC','3º toque tendência','3º toque suporte/resistência']}
            await update_news()
            return state['signals']
        finally:
            state['scanning']=False
            if state['open_trade']:state['status']='OPERAÇÃO ABERTA'
            elif state['running']:state['status']='ATIVO'
            else:state['status']='PARADO'

def auth_headers(token):
    h={'Authorization':f'Bearer {token}'}
    if CLIENT_ID:h['Deriv-App-ID']=CLIENT_ID
    return h

async def deriv_rest_token(token,method,path):
    if not token:raise HTTPException(401,'Conecte sua conta Deriv primeiro.')
    async with httpx.AsyncClient(timeout=20) as client:r=await client.request(method,REST_BASE+path,headers=auth_headers(token))
    try:d=r.json()
    except Exception:d={'error':r.text}
    if r.status_code>=400:raise HTTPException(r.status_code,d)
    return d

async def get_accounts_token(token):
    d=await deriv_rest_token(token,'GET','/trading/v1/options/accounts')
    return d.get('data',d if isinstance(d,list) else [])

def account_type(a):
    raw=' '.join(str(a.get(k,'')) for k in ('account_type','account_category','is_virtual')).lower()
    if 'demo' in raw or 'virtual' in raw or str(a.get('is_virtual','')).lower()=='true':return 'demo'
    if 'real' in raw:return 'real'
    return str(a.get('account_type','')).lower()

async def otp_url_token(token,account_id):
    d=await deriv_rest_token(token,'POST',f'/trading/v1/options/accounts/{account_id}/otp')
    data=d.get('data',d);url=data.get('url') or data.get('websocket_url')
    if not url:raise HTTPException(502,f'Deriv não retornou URL WebSocket: {d}')
    return url

async def refresh_runtime_balance():
    if not runtime['token'] or not runtime['account_id']:return None
    accounts=await get_accounts_token(runtime['token'])
    acc=next((a for a in accounts if a.get('account_id')==runtime['account_id']),None)
    if acc:
        state['balance']=float(acc.get('balance',state['balance']));runtime['currency']=acc.get('currency',runtime['currency'])
    return acc

def calculate_stake(balance):
    stake=config.valor_entrada if config.entrada_tipo.lower()=='valor' else balance*config.percentual_entrada/100
    return round(float(stake),2)

def signal_fresh(best):
    # signal_epoch é o início da vela M1 confirmadora.
    # Após o fechamento, aceita no máximo ~45 s para não entrar atrasado.
    ep=int(best.get('signal_epoch') or 0)
    return bool(ep) and time.time()<=ep+105

async def trade_ws_connect(token,account_id,best=None,automatic=False):
    last=None
    for attempt in range(3):
        if automatic and best and not signal_fresh(best):
            raise HTTPException(409,'Sinal expirou enquanto aguardava a Deriv. Aguardando novo combo.')
        try:
            url=await otp_url_token(token,account_id)
            return await websockets.connect(url,ping_interval=20,ping_timeout=20,open_timeout=15)
        except HTTPException as e:
            last=e
            if e.status_code!=429:raise
        except Exception as e:
            last=e
            if not is_429(e):raise
        wait=5*(attempt+1)
        state['deriv_status']=f'LIMITADA 429 • ordem aguardando {wait}s'
        if automatic:state['auto_status']=f'AGUARDANDO DERIV • 429 • {wait}s'
        await asyncio.sleep(wait)
    raise HTTPException(429,f'Deriv limitou as conexões. {last}')

async def monitor_contract(token,account_id,contract_id,entry):
    attempts=0
    while attempts<8:
        ws=None
        try:
            url=await otp_url_token(token,account_id)
            ws=await websockets.connect(url,ping_interval=20,ping_timeout=20,open_timeout=15)
            await ws.send(json.dumps({'proposal_open_contract':1,'contract_id':contract_id,'subscribe':1,'req_id':601}))
            while True:
                d=json.loads(await asyncio.wait_for(ws.recv(),timeout=45))
                if 'error' in d:raise RuntimeError(d['error'].get('message','Erro ao acompanhar contrato'))
                poc=d.get('proposal_open_contract') or {}
                if not poc:continue
                profit=float(poc.get('profit') or 0)
                entry.update({'profit':profit,'buy_price':float(poc.get('buy_price') or entry['stake']),'current_spot':poc.get('current_spot'),'entry_spot':poc.get('entry_spot'),'exit_tick':poc.get('exit_tick'),'is_sold':bool(poc.get('is_sold')),'status':poc.get('status','open')})
                state['open_trade']=dict(entry);state['status']='OPERAÇÃO ABERTA'
                closed=bool(poc.get('is_sold')) or poc.get('status') in ('sold','won','lost') or poc.get('is_expired')==1
                if closed:
                    result='WIN' if profit>0 else ('LOSS' if profit<0 else 'EMPATE')
                    entry['result']=result;entry['closed_time']=int(time.time());entry['status']='finalizada'
                    state['history'].insert(0,dict(entry));state['history']=state['history'][:100];save_history();state['open_trade']=None
                    try:await refresh_runtime_balance()
                    except Exception:state['balance']=round(state['balance']+profit,2)
                    state['status']='ATIVO' if state['running'] else 'PARADO'
                    return
        except asyncio.CancelledError:raise
        except Exception as e:
            attempts+=1
            if state.get('open_trade') and state['open_trade'].get('contract_id')==contract_id:
                state['open_trade']['monitor_error']=str(e);state['status']='OPERAÇÃO ABERTA'
            await asyncio.sleep(min(5*attempts if is_429(e) else 2*attempts,30))
        finally:
            if ws is not None:
                try:await ws.close()
                except Exception:pass
    if state.get('open_trade') and state['open_trade'].get('contract_id')==contract_id:
        state['open_trade']['monitor_error']='Não foi possível confirmar o encerramento automaticamente.'
        state['status']='OPERAÇÃO ABERTA'

async def execute_best_trade(token=None,account_id=None,account_typ=None,automatic=False):
    async with trade_lock:
        if state.get('open_trade'):raise HTTPException(409,'Já existe uma operação aberta. Aguarde finalizar.')
        if not state['signals']:raise HTTPException(400,'Nenhum sinal confirmado disponível.')
        token=token or runtime['token'];account_id=account_id or runtime['account_id'];account_typ=account_typ or runtime['account_type']
        if not token or not account_id:raise HTTPException(401,'Conecte a Deriv e selecione uma conta.')
        best=state['signals'][0]
        if automatic and not signal_fresh(best):raise HTTPException(409,'Sinal expirado. Aguardando novo combo.')
        signal_key=f"{best['symbol']}:{best['direction']}:{best.get('signal_epoch',0)}:{best.get('combo','')}"
        if automatic and runtime['last_auto_signal_key']==signal_key:return {'ok':False,'skipped':'Sinal já operado neste candle.'}
        accounts=await get_accounts_token(token);acc=next((a for a in accounts if a.get('account_id')==account_id),None)
        if not acc:raise HTTPException(404,'Conta selecionada não está disponível.')
        balance=float(acc.get('balance',0));state['balance']=balance
        start=state.get('start_balance') or balance;pnl=((balance-start)/start)*100 if start else 0
        if pnl>=config.stop_gain:raise HTTPException(409,'Stop Gain atingido.')
        if pnl<=-config.stop_loss:raise HTTPException(409,'Stop Loss atingido.')
        if state['entries']>=config.max_entradas:raise HTTPException(409,'Limite de operações atingido.')
        stake=calculate_stake(balance)
        if stake<=0:raise HTTPException(400,'Valor da entrada inválido.')
        if stake>balance:raise HTTPException(400,'Valor da entrada é maior que a banca.')

        ws=await trade_ws_connect(token,account_id,best,automatic)
        try:
            proposal={'proposal':1,'amount':stake,'basis':'stake','contract_type':best['direction'],'currency':acc.get('currency','USD'),'duration':config.duracao_minutos,'duration_unit':'m','underlying_symbol':best['symbol'],'req_id':501}
            await ws.send(json.dumps(proposal));pd=json.loads(await ws.recv())
            if 'error' in pd:raise HTTPException(400,pd['error'].get('message','Erro na proposta'))
            prop=pd.get('proposal',{});pid=prop.get('id')
            if not pid:raise HTTPException(400,f'Proposta sem ID: {pd}')
            if automatic and not signal_fresh(best):raise HTTPException(409,'Sinal expirou antes da compra.')
            ask=float(prop.get('ask_price',stake))
            await ws.send(json.dumps({'buy':pid,'price':ask,'req_id':502}));bd=json.loads(await ws.recv())
            if 'error' in bd:raise HTTPException(400,bd['error'].get('message','Erro ao comprar contrato'))
        finally:
            try:await ws.close()
            except Exception:pass

        buy=bd.get('buy',{});cid=buy.get('contract_id')
        if not cid:raise HTTPException(502,f'Deriv não retornou contract_id: {bd}')
        entry={'time':int(time.time()),'symbol':best['symbol'],'name':best['name'],'direction':best['direction'],'side':'COMPRA' if best['direction']=='CALL' else 'VENDA','combo':best.get('combo',''),'touch_epoch':best.get('touch_epoch',0),'stake':stake,'result':'ABERTA','mode':str(account_typ).upper(),'automatic':bool(automatic),'analysis':best['analysis'],'confirmation':best['confirmation'],'contract_id':cid,'profit':0.0,'duration_minutes':config.duracao_minutos,'signal_epoch':best.get('signal_epoch',0)}
        state['open_trade']=dict(entry);state['entries']+=1;state['status']='OPERAÇÃO ABERTA'
        runtime['last_auto_signal_key']=signal_key
        asyncio.create_task(monitor_contract(token,account_id,cid,entry))
        return {'ok':True,'entry':entry}

async def maybe_auto_trade():
    if not config.operacao_automatica:
        state['auto_status']='DESLIGADA';return
    if state.get('open_trade'):
        state['auto_status']='AGUARDANDO FINALIZAÇÃO';return
    if not state['signals']:
        state['auto_status']='AGUARDANDO COMBO';return
    if not runtime['token'] or not runtime['account_id']:
        state['auto_status']='BLOQUEADO • selecione uma conta Deriv';return
    if not signal_fresh(state['signals'][0]):
        state['auto_status']='SINAL EXPIRADO • AGUARDANDO NOVO COMBO';return
    try:
        state['auto_status']='COMBO CONFIRMADO • ENVIANDO ORDEM'
        r=await execute_best_trade(automatic=True)
        state['auto_status']='OPERAÇÃO ABERTA' if r.get('ok') else 'AGUARDANDO NOVO COMBO'
    except HTTPException as e:
        if e.status_code==429:state['auto_status']='AGUARDANDO DERIV • limite 429'
        else:state['auto_status']=f'BLOQUEADO • {e.detail}'
    except Exception as e:
        state['auto_status']=('AGUARDANDO DERIV • limite 429' if is_429(e) else f'ERRO TEMPORÁRIO • {str(e)[:90]}')

async def scanner_loop():
    try:
        while state['running']:
            try:
                await scan_once()
                state['scanner_heartbeat']=int(time.time())
                if state['running'] and not state['open_trade']:state['status']='ATIVO'
                await maybe_auto_trade()
            except asyncio.CancelledError:raise
            except Exception as e:
                if state['running']:state['status']='ATIVO'
                if is_429(e):state['deriv_status']='LIMITADA 429'
            if not state['running']:break
            # Como a entrada depende de vela M1 fechada, próxima varredura no
            # próximo minuto + 2 s. Isso reduz drasticamente o risco de 429.
            now=time.time();next_run=(int(now)//60+1)*60+2
            while state['running'] and time.time()<next_run:
                await asyncio.sleep(min(1,max(.1,next_run-time.time())))
    except asyncio.CancelledError:pass
    finally:
        if not state['running'] and not state['open_trade']:state['status']='PARADO'

def current_display_status():
    if state.get('open_trade'):return 'OPERAÇÃO ABERTA'
    if state.get('scanning'):return 'ESCANEANDO'
    if state.get('running'):return 'ATIVO'
    return 'PARADO'

@app.get('/')
async def home():return FileResponse(STATIC_DIR/'index.html')

@app.get('/health')
async def health():return {'ok':True,'service':'ATR Forex MTF v5.2','running':state['running'],'scanning':state['scanning'],'deriv_status':state['deriv_status']}

@app.get('/auth/login')
async def auth_login(request:Request):
    if not CLIENT_ID:raise HTTPException(500,'Configure DERIV_CLIENT_ID no Render.')
    verifier=secrets.token_urlsafe(64)[:96];challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode();st=secrets.token_urlsafe(24)
    request.session['pkce_verifier']=verifier;request.session['oauth_state']=st
    q=urlencode({'response_type':'code','client_id':CLIENT_ID,'redirect_uri':REDIRECT_URI,'scope':'trade','state':st,'code_challenge':challenge,'code_challenge_method':'S256'})
    return RedirectResponse(OAUTH_AUTH+'?'+q)

@app.get('/auth/callback')
async def auth_callback(request:Request,code:str='',state_q:str='',state:str='',error:str=''):
    if error:return RedirectResponse('/?deriv=error')
    returned_state=state or state_q
    if not code or returned_state!=request.session.get('oauth_state'):raise HTTPException(400,'Callback OAuth inválido/state não confere.')
    verifier=request.session.pop('pkce_verifier',None);request.session.pop('oauth_state',None)
    async with httpx.AsyncClient(timeout=20) as client:r=await client.post(OAUTH_TOKEN,data={'grant_type':'authorization_code','client_id':CLIENT_ID,'code':code,'code_verifier':verifier,'redirect_uri':REDIRECT_URI})
    d=r.json()
    if r.status_code>=400 or not d.get('access_token'):raise HTTPException(400,d)
    request.session['deriv_token']=d['access_token'];runtime['token']=d['access_token']
    return RedirectResponse('/?deriv=connected')

@app.post('/auth/logout')
async def logout(request:Request):
    request.session.clear();runtime.update({'token':None,'account_id':None,'account_type':'','currency':'USD','last_auto_signal_key':None});return {'ok':True}

@app.get('/api/deriv/accounts')
async def api_accounts(request:Request):
    token=request.session.get('deriv_token')
    if not token:return {'connected':False,'accounts':[],'selected':None}
    runtime['token']=token;accounts=await get_accounts_token(token);selected=request.session.get('deriv_account_id')
    if selected:
        acc=next((a for a in accounts if a.get('account_id')==selected),None)
        if acc:state['balance']=float(acc.get('balance',state['balance']));runtime['currency']=acc.get('currency','USD')
    return {'connected':True,'accounts':accounts,'selected':selected}

@app.post('/api/deriv/select')
async def select_account(choice:AccountChoice,request:Request):
    global config
    token=request.session.get('deriv_token');accounts=await get_accounts_token(token);acc=next((a for a in accounts if a.get('account_id')==choice.account_id),None)
    if not acc:raise HTTPException(404,'Conta não encontrada.')
    typ=account_type(acc);balance=float(acc.get('balance',0))
    request.session['deriv_account_id']=choice.account_id;request.session['deriv_account_type']=typ
    runtime.update({'token':token,'account_id':choice.account_id,'account_type':typ,'currency':acc.get('currency','USD'),'last_auto_signal_key':None})
    state['balance']=balance;state['start_balance']=balance;state['entries']=0
    config=config.model_copy(update={'banca_inicial':balance})
    return {'ok':True,'account':acc,'banca_inicial':balance,'currency':runtime['currency']}

@app.post('/api/deriv/trade')
async def deriv_trade(request:Request):
    token=request.session.get('deriv_token');aid=request.session.get('deriv_account_id');typ=request.session.get('deriv_account_type','')
    runtime['token']=token;runtime['account_id']=aid;runtime['account_type']=typ
    if not state['signals']:await scan_once()
    return await execute_best_trade(token,aid,typ,automatic=False)

@app.get('/api/status')
async def get_status():
    global scanner_task
    if state.get('running') and scanner_task and scanner_task.done() and not state.get('open_trade'):
        state['running']=False; state['status']='PARADO'; state['auto_status']='LIGADA • scanner parado' if config.operacao_automatica else 'DESLIGADA'
    start=state['start_balance'];pnl=((state['balance']-start)/start)*100 if start else 0
    display=current_display_status()
    return {**state,'status':display,'display_status':display,'config':config.model_dump(),'pnl_percent':round(pnl,2),'account_selected':bool(runtime['account_id']),'account_type':runtime['account_type'],'currency':runtime['currency'],'forex_pairs':FOREX_PAIRS}

@app.get('/api/news')
async def api_news():
    await update_news(force=True)
    return {'news':state['news'],'status':state['news_status'],'updated_at':state['news_updated_at']}

@app.post('/api/config')
async def set_config(new:Config):
    global config
    if new.entrada_tipo.lower() not in ('percentual','valor'):raise HTTPException(400,"entrada_tipo deve ser 'percentual' ou 'valor'.")
    if new.percentual_entrada<=0 or new.valor_entrada<=0:raise HTTPException(400,'Entrada deve ser maior que zero.')
    if runtime['account_id']:new=new.model_copy(update={'banca_inicial':state['start_balance']})
    config=new;state['auto_trade']=config.operacao_automatica
    state['auto_status']='AGUARDANDO COMBO' if config.operacao_automatica and state['running'] else ('LIGADA • scanner parado' if config.operacao_automatica else 'DESLIGADA')
    return {'ok':True,'config':config.model_dump()}

@app.post('/api/scan')
async def api_scan():return {'signals':await scan_once()}

@app.post('/api/start')
async def start():
    global scanner_task
    if state['running'] and scanner_task and not scanner_task.done():return {'ok':True,'message':'Scanner já está ativo.'}
    state['running']=True;state['auto_trade']=config.operacao_automatica;state['status']='ATIVO'
    state['auto_status']='AGUARDANDO COMBO' if config.operacao_automatica else 'DESLIGADA'
    scanner_task=asyncio.create_task(scanner_loop());return {'ok':True}

@app.post('/api/stop')
async def stop():
    global scanner_task
    state['running']=False
    state['status']='OPERAÇÃO ABERTA' if state['open_trade'] else 'PARADO'
    state['auto_status']='LIGADA • scanner parado' if config.operacao_automatica else 'DESLIGADA'
    if scanner_task and not scanner_task.done():scanner_task.cancel()
    scanner_task=None
    await close_public_ws()
    return {'ok':True}

app.mount('/static',StaticFiles(directory=str(STATIC_DIR)),name='static')
