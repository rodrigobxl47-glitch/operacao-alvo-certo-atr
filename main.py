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

app=FastAPI(title='Operação Alvo Certo ATR - Deriv MTF v4')
app.add_middleware(SessionMiddleware,secret_key=os.getenv('SESSION_SECRET',secrets.token_hex(32)),https_only=True,same_site='lax')

PUBLIC_WS='wss://api.derivws.com/trading/v1/options/ws/public'
REST_BASE='https://api.derivws.com'
OAUTH_AUTH='https://auth.deriv.com/oauth2/auth'
OAUTH_TOKEN='https://auth.deriv.com/oauth2/token'
CLIENT_ID=os.getenv('DERIV_CLIENT_ID','')
REDIRECT_URI=os.getenv('DERIV_REDIRECT_URI','https://operacao-alvo-certo-atr.onrender.com/auth/callback')

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
    'status':'Parado','auto_status':'DESLIGADA','diagnostics':{},'news':[],'auto_trade':False
}
runtime={'token':None,'account_id':None,'account_type':'','currency':'USD','last_auto_signal_key':None}
scanner_task=None
scan_lock=asyncio.Lock()
trade_lock=asyncio.Lock()

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

# Somente os gatilhos solicitados.
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
        else:
            groups.append({'level':price,'prices':[price],'indices':[idx]})
    return groups

def strong_support_resistance(cs):
    hi,lo=pivots(cs)
    ar=max(avg_range(cs[-40:]),1e-12);tol=ar*.45
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
    touch_idx=len(cs)-2; hist=cs[:touch_idx]
    hi,lo=pivots(hist); pts=lo if side=='CALL' else hi
    ar=max(avg_range(cs[-30:]),1e-12); tol=ar*.55; level=support if side=='CALL' else resistance
    prior=sum(1 for _,p in pts if near(p,level,tol))
    touch=cs[touch_idx].low if side=='CALL' else cs[touch_idx].high
    return prior>=2 and near(touch,level,ar*.75)

def trend_third_touch_previous(cs,side):
    touch_idx=len(cs)-2; base=cs[:touch_idx]
    hi,lo=pivots(base); pts=(lo if side=='CALL' else hi)[-5:]
    if len(pts)<2:return False
    ar=max(avg_range(cs[-30:]),1e-12); tol=ar*.8
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
    support,resistance=strong_support_resistance(m1); candidates=[]
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

async def ws_public(payload,req_id=1):
    async with websockets.connect(PUBLIC_WS,ping_interval=20,ping_timeout=20,open_timeout=15) as ws:
        p=dict(payload);p['req_id']=req_id;await ws.send(json.dumps(p))
        while True:
            d=json.loads(await ws.recv())
            if d.get('req_id')==req_id:
                if 'error' in d:raise RuntimeError(d['error'].get('message','Erro Deriv'))
                return d

async def active_symbols():
    return (await ws_public({'active_symbols':'brief','contract_type':['CALL','PUT']},100)).get('active_symbols',[])

def sym_fields(a):
    return a.get('underlying_symbol') or a.get('symbol'),a.get('underlying_symbol_name') or a.get('display_name') or a.get('underlying_symbol') or a.get('symbol')

def is_derived(a):
    vals=' '.join(str(a.get(k,'')) for k in ('market','submarket','subgroup','underlying_symbol_type','symbol_type','underlying_symbol_name','display_name')).lower()
    return any(k in vals for k in ('synthetic','derived','volatility','crash','boom','jump','step','range break','drift switch','daily reset','dex'))

async def candles(symbol,granularity,count=111):
    d=await ws_public({'ticks_history':symbol,'adjust_start_time':1,'count':count,'end':'latest','granularity':granularity,'style':'candles'},1000+granularity)
    out=[];now=int(time.time())
    for c in d.get('candles',[]):
        try:
            x=Candle(int(c['epoch']),float(c['open']),float(c['high']),float(c['low']),float(c['close']))
            if x.epoch+granularity<=now:out.append(x)
        except Exception:pass
    return out[-110:]

async def analyze_one(a,sem):
    async with sem:
        symbol,name=sym_fields(a)
        if not symbol:return None
        try:
            m1=await candles(symbol,60);await asyncio.sleep(.08)
            m5=await candles(symbol,300);await asyncio.sleep(.08)
            m15=await candles(symbol,900)
            return analyze(symbol,name,m1,m5,m15)
        except Exception as e:
            print(f'[ATR] Falha {symbol}: {e}',flush=True);return None

async def scan_once():
    if scan_lock.locked():return state['signals']
    async with scan_lock:
        state['scanning']=True;state['status']='ESCANEANDO • tendência • suporte/resistência • ABC • 3º toque'
        try:
            syms=[a for a in await active_symbols() if is_derived(a)]
            sem=asyncio.Semaphore(3)
            results=await asyncio.gather(*[analyze_one(a,sem) for a in syms])
            sig=[x for x in results if x]
            sig.sort(key=lambda x:x.signal_epoch,reverse=True)
            state['signals']=[asdict(x) for x in sig[:25]]
            state['last_scan']=int(time.time())
            state['diagnostics']={'ativos_derivados':len(syms),'sinais':len(sig),'candles_por_tf':110,'timeframes':['M1','M5','M15'],'base':'Suporte forte + Resistência forte + Tendência','confluencias':['ABC','3º toque tendência','3º toque suporte/resistência']}
            if state['open_trade']:state['status']=f"OPERAÇÃO ABERTA • {state['open_trade']['side']} • {state['open_trade']['name']}"
            elif state['running']:state['status']=f"ATIVO • {len(syms)} derivados • {len(sig)} sinais"
            else:state['status']=f"{len(syms)} derivados analisados • {len(sig)} sinais"
            return state['signals']
        finally:state['scanning']=False

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
        state['balance']=float(acc.get('balance',state['balance']))
        runtime['currency']=acc.get('currency',runtime['currency'])
    return acc

def calculate_stake(balance):
    if config.entrada_tipo.lower()=='valor':stake=config.valor_entrada
    else:stake=balance*config.percentual_entrada/100
    return round(float(stake),2)

async def monitor_contract(token,account_id,contract_id,entry):
    attempts=0
    while attempts<8:
        try:
            url=await otp_url_token(token,account_id)
            async with websockets.connect(url,ping_interval=20,ping_timeout=20) as ws:
                await ws.send(json.dumps({'proposal_open_contract':1,'contract_id':contract_id,'subscribe':1,'req_id':601}))
                while True:
                    d=json.loads(await asyncio.wait_for(ws.recv(),timeout=45))
                    if 'error' in d:raise RuntimeError(d['error'].get('message','Erro ao acompanhar contrato'))
                    poc=d.get('proposal_open_contract') or {}
                    if not poc:continue
                    profit=float(poc.get('profit') or 0)
                    entry.update({'profit':profit,'buy_price':float(poc.get('buy_price') or entry['stake']),'current_spot':poc.get('current_spot'),'entry_spot':poc.get('entry_spot'),'exit_tick':poc.get('exit_tick'),'is_sold':bool(poc.get('is_sold')),'status':poc.get('status','open')})
                    state['open_trade']=dict(entry)
                    state['status']=f"OPERAÇÃO ABERTA • {entry['side']} • {entry['name']}"
                    closed=bool(poc.get('is_sold')) or poc.get('status') in ('sold','won','lost') or poc.get('is_expired')==1
                    if closed:
                        result='WIN' if profit>0 else ('LOSS' if profit<0 else 'EMPATE')
                        entry['result']=result;entry['closed_time']=int(time.time());entry['status']='finalizada'
                        state['history'].insert(0,dict(entry));state['history']=state['history'][:100];state['open_trade']=None
                        try:await refresh_runtime_balance()
                        except Exception:state['balance']=round(state['balance']+profit,2)
                        state['status']=(f"ATIVO • última: {result} • {entry['side']} • {entry['name']}" if state['running'] else f"Finalizada: {result} • {entry['side']} • {entry['name']}")
                        return
        except asyncio.CancelledError:raise
        except Exception as e:
            attempts+=1
            if state.get('open_trade') and state['open_trade'].get('contract_id')==contract_id:
                state['open_trade']['monitor_error']=str(e);state['status']=f'OPERAÇÃO ABERTA • reconectando monitor ({attempts}/8)'
            await asyncio.sleep(min(2*attempts,10))
    if state.get('open_trade') and state['open_trade'].get('contract_id')==contract_id:
        state['open_trade']['monitor_error']='Não foi possível confirmar o encerramento automaticamente.'
        state['status']='Operação pendente de confirmação da Deriv.'

async def execute_best_trade(token=None,account_id=None,account_typ=None,automatic=False):
    async with trade_lock:
        if state.get('open_trade'):raise HTTPException(409,'Já existe uma operação aberta. Aguarde finalizar.')
        if not state['signals']:raise HTTPException(400,'Nenhum sinal confirmado disponível.')
        token=token or runtime['token'];account_id=account_id or runtime['account_id'];account_typ=account_typ or runtime['account_type']
        if not token or not account_id:raise HTTPException(401,'Conecte a Deriv e selecione uma conta.')
        best=state['signals'][0];signal_key=f"{best['symbol']}:{best['direction']}:{best.get('signal_epoch',0)}:{best.get('combo','')}"
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
        url=await otp_url_token(token,account_id)
        async with websockets.connect(url,ping_interval=20,ping_timeout=20) as ws:
            proposal={'proposal':1,'amount':stake,'basis':'stake','contract_type':best['direction'],'currency':acc.get('currency','USD'),'duration':config.duracao_minutos,'duration_unit':'m','underlying_symbol':best['symbol'],'req_id':501}
            await ws.send(json.dumps(proposal));pd=json.loads(await ws.recv())
            if 'error' in pd:raise HTTPException(400,pd['error'].get('message','Erro na proposta'))
            prop=pd.get('proposal',{});pid=prop.get('id')
            if not pid:raise HTTPException(400,f'Proposta sem ID: {pd}')
            ask=float(prop.get('ask_price',stake))
            await ws.send(json.dumps({'buy':pid,'price':ask,'req_id':502}));bd=json.loads(await ws.recv())
            if 'error' in bd:raise HTTPException(400,bd['error'].get('message','Erro ao comprar contrato'))
        buy=bd.get('buy',{});cid=buy.get('contract_id')
        if not cid:raise HTTPException(502,f'Deriv não retornou contract_id: {bd}')
        entry={'time':int(time.time()),'symbol':best['symbol'],'name':best['name'],'direction':best['direction'],'side':'COMPRA' if best['direction']=='CALL' else 'VENDA','combo':best.get('combo',''),'touch_epoch':best.get('touch_epoch',0),'stake':stake,'result':'ABERTA','mode':str(account_typ).upper(),'automatic':bool(automatic),'analysis':best['analysis'],'confirmation':best['confirmation'],'contract_id':cid,'profit':0.0,'duration_minutes':config.duracao_minutos,'signal_epoch':best.get('signal_epoch',0)}
        state['open_trade']=dict(entry);state['entries']+=1;state['status']=f"OPERAÇÃO ABERTA • {entry['side']} • {best['name']} • {entry['mode']}"
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
    try:
        state['auto_status']='COMBO CONFIRMADO • ENVIANDO ORDEM'
        r=await execute_best_trade(automatic=True)
        state['auto_status']='OPERAÇÃO ABERTA' if r.get('ok') else 'AGUARDANDO NOVO COMBO'
    except HTTPException as e:state['auto_status']=f'BLOQUEADO • {e.detail}'
    except Exception as e:state['auto_status']=f'ERRO • {str(e)[:100]}'

async def scanner_loop():
    try:
        while state['running']:
            try:
                await scan_once();await maybe_auto_trade()
            except asyncio.CancelledError:raise
            except Exception as e:state['status']=f'Erro no scanner: {str(e)[:120]}'
            for _ in range(15):
                if not state['running']:break
                await asyncio.sleep(1)
    except asyncio.CancelledError:pass

@app.get('/')
async def home():return FileResponse(STATIC_DIR/'index.html')

@app.get('/health')
async def health():return {'ok':True,'service':'ATR Deriv MTF v4','running':state['running'],'scanning':state['scanning']}

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
    start=state['start_balance'];pnl=((state['balance']-start)/start)*100 if start else 0
    display='OPERAÇÃO ABERTA' if state['open_trade'] else ('ESCANEANDO' if state['scanning'] else ('ATIVO' if state['running'] else 'PARADO'))
    return {**state,'status':display,'display_status':display,'config':config.model_dump(),'pnl_percent':round(pnl,2),'account_selected':bool(runtime['account_id']),'account_type':runtime['account_type'],'currency':runtime['currency']}

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
    state['running']=True;state['auto_trade']=config.operacao_automatica;state['status']='ATIVO • iniciando análise-base...'
    scanner_task=asyncio.create_task(scanner_loop());return {'ok':True}

@app.post('/api/stop')
async def stop():
    global scanner_task
    state['running']=False
    state['status']=f"OPERAÇÃO ABERTA • {state['open_trade']['side']} • monitorando" if state['open_trade'] else 'Parado'
    if scanner_task and not scanner_task.done():scanner_task.cancel()
    scanner_task=None;return {'ok':True}

app.mount('/static',StaticFiles(directory=str(STATIC_DIR)),name='static')
