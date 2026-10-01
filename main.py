import asyncio, json, os, secrets, hashlib, base64, time, math
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

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / 'static'

app=FastAPI(title='Operação Alvo Certo ATR - Deriv MTF')
app.add_middleware(SessionMiddleware,secret_key=os.getenv('SESSION_SECRET',secrets.token_hex(32)),https_only=True,same_site='lax')
PUBLIC_WS='wss://api.derivws.com/trading/v1/options/ws/public'; REST_BASE='https://api.derivws.com'
OAUTH_AUTH='https://auth.deriv.com/oauth2/auth'; OAUTH_TOKEN='https://auth.deriv.com/oauth2/token'
CLIENT_ID=os.getenv('DERIV_CLIENT_ID',''); REDIRECT_URI=os.getenv('DERIV_REDIRECT_URI','https://operacao-alvo-certo-atr.onrender.com/auth/callback')

class Config(BaseModel):
    banca_inicial:float=1000; percentual_entrada:float=1; stop_gain:float=5; stop_loss:float=5; max_entradas:int=5; min_score:int=7; duracao_minutos:int=1
class AccountChoice(BaseModel): account_id:str
config=Config(); state={'running':False,'scanning':False,'balance':1000.0,'start_balance':1000.0,'entries':0,'signals':[],'history':[],'open_trade':None,'last_scan':None,'status':'Parado','diagnostics':{},'news':[]}
scanner_task = None
scan_lock = asyncio.Lock()

@dataclass
class Candle: epoch:int; open:float; high:float; low:float; close:float
@dataclass
class Signal:
    symbol:str; name:str; direction:str; score:int; price:float; analysis:str; confirmation:str
    reasons:List[str]=field(default_factory=list); support:float=0; resistance:float=0; timeframes:List[str]=field(default_factory=list)

def body(c): return abs(c.close-c.open)
def rng(c): return max(c.high-c.low,1e-12)
def bull(c): return c.close>c.open
def bear(c): return c.close<c.open
def body_low(c): return min(c.open,c.close)
def body_high(c): return max(c.open,c.close)
def avg_range(cs): return sum(rng(x) for x in cs)/max(len(cs),1)
def near(a,b,tol): return abs(a-b)<=tol

def candle_pattern(cs, side):
    if len(cs)<4:return None
    a,b,c=cs[-1],cs[-2],cs[-3]
    ar=max(avg_range(cs[-20:]),1e-12); eps=ar*.08
    if side=='CALL':
        lowwick=body_low(a)-a.low; upwick=a.high-body_high(a)
        if bull(a) and lowwick>=2*max(body(a),eps) and upwick<=max(body(a),eps)*.7:return 'Martelo (Hammer)'
        if bull(a) and upwick>=2*max(body(a),eps) and lowwick<=max(body(a),eps)*.7:return 'Martelo Invertido (Inverted Hammer)'
        if bear(b) and bull(a) and a.open<=b.close and a.close>=b.open:return 'Engolfo de Alta (Bullish Engulfing)'
        if bear(c) and body(b)<=body(c)*.5 and bull(a) and a.close>(c.open+c.close)/2:return 'Estrela da Manhã (Morning Star)'
        if bear(b) and bull(a) and a.open<b.low+ar*.15 and a.close>(b.open+b.close)/2 and a.close<b.open:return 'Piercing Line'
        if bear(b) and bull(a) and body_low(a)>=body_low(b) and body_high(a)<=body_high(b):return 'Harami de Alta (Bullish Harami)'
        if all(bull(x) for x in cs[-3:]) and cs[-1].close>cs[-2].close>cs[-3].close:return 'Três Soldados Brancos (Three White Soldiers)'
        if bull(a) and body(a)/rng(a)>=.85:return 'Marubozu de Alta (Bullish Marubozu)'
    else:
        lowwick=body_low(a)-a.low; upwick=a.high-body_high(a)
        if bear(a) and upwick>=2*max(body(a),eps) and lowwick<=max(body(a),eps)*.7:return 'Estrela Cadente (Shooting Star)'
        if bear(a) and lowwick>=2*max(body(a),eps) and upwick<=max(body(a),eps)*.7:return 'Homem Enforcado (Hanging Man)'
        if bull(b) and bear(a) and a.open>=b.close and a.close<=b.open:return 'Engolfo de Baixa (Bearish Engulfing)'
        if bull(c) and body(b)<=body(c)*.5 and bear(a) and a.close<(c.open+c.close)/2:return 'Estrela da Noite (Evening Star)'
        if bull(b) and bear(a) and a.open>b.high-ar*.15 and a.close<(b.open+b.close)/2 and a.close>b.open:return 'Dark Cloud Cover'
        if bull(b) and bear(a) and body_low(a)>=body_low(b) and body_high(a)<=body_high(b):return 'Harami de Baixa (Bearish Harami)'
        if all(bear(x) for x in cs[-3:]) and cs[-1].close<cs[-2].close<cs[-3].close:return 'Três Corvos Negros (Three Black Crows)'
        if bear(a) and body(a)/rng(a)>=.85:return 'Marubozu de Baixa (Bearish Marubozu)'
    return None

def pivots(cs, span=2):
    hi=[]; lo=[]
    for i in range(span,len(cs)-span):
        if cs[i].high>=max(x.high for x in cs[i-span:i+span+1]):hi.append((i,cs[i].high))
        if cs[i].low<=min(x.low for x in cs[i-span:i+span+1]):lo.append((i,cs[i].low))
    return hi,lo

def line3(points, cs, side):
    if len(points)<3:return None
    ar=avg_range(cs[-30:]); tol=ar*.45
    # find recent triples whose pivots form a rising support or falling resistance line
    for trio in [points[-3:], points[-4:-1] if len(points)>=4 else []]:
        if len(trio)<3:continue
        (i1,p1),(i2,p2),(i3,p3)=trio
        if i3==i1:continue
        slope=(p3-p1)/(i3-i1); expected=p1+slope*(i2-i1)
        if abs(p2-expected)>tol:continue
        if side=='CALL' and slope<=0:continue
        if side=='PUT' and slope>=0:continue
        projected=p1+slope*((len(cs)-1)-i1)
        if abs((cs[-1].low if side=='CALL' else cs[-1].high)-projected)<=ar*.8:
            return projected
    return None

def sr_third_touch(cs, side):
    hi,lo=pivots(cs); pts=lo if side=='CALL' else hi
    if len(pts)<2:return None
    ar=avg_range(cs[-30:]); tol=ar*.45
    recent=pts[-8:]
    for _,level in reversed(recent):
        matches=[p for _,p in recent if near(p,level,tol)]
        if len(matches)>=2:
            # confirmation by candle BODY near level; wick alone is not enough
            a=cs[-1]; touch=near(body_low(a) if side=='CALL' else body_high(a),level,ar*.7)
            if touch:return sum(matches)/len(matches)
    return None

def double_level(cs, side):
    hi,lo=pivots(cs); pts=lo if side=='CALL' else hi
    if len(pts)<2:return None
    ar=avg_range(cs[-30:]); tol=ar*.4
    p1,p2=pts[-2][1],pts[-1][1]
    if near(p1,p2,tol):return (p1+p2)/2
    return None

def abc_confluence(cs, side):
    hi,lo=pivots(cs)
    if side=='CALL' and len(hi)>=2 and len(lo)>=2:
        return hi[-1][1]>hi[-2][1] and lo[-1][1]>lo[-2][1]
    if side=='PUT' and len(hi)>=2 and len(lo)>=2:
        return hi[-1][1]<hi[-2][1] and lo[-1][1]<lo[-2][1]
    return False

def timeframe_analysis(cs, side):
    pat=candle_pattern(cs,side); hi,lo=pivots(cs); ar=avg_range(cs[-30:]); last=cs[-1]
    sup=min(x.low for x in cs[-20:]); res=max(x.high for x in cs[-20:])
    trend=line3(lo if side=='CALL' else hi,cs,side)
    sr=sr_third_touch(cs,side); dbl=double_level(cs,side); abc=abc_confluence(cs,side)
    breakout=False
    if side=='CALL': breakout=last.close>max(x.high for x in cs[-21:-1])+ar*.05
    else: breakout=last.close<min(x.low for x in cs[-21:-1])-ar*.05
    return {'pattern':pat,'trend3':trend is not None,'abc':abc,'sr3':sr is not None,'double':dbl is not None,'breakout':breakout,'support':sup,'resistance':res}

def analyze(symbol,name,m1,m5,m15):
    if min(len(m1),len(m5),len(m15))<110:return None
    tfs={'M1':m1[-110:],'M5':m5[-110:],'M15':m15[-110:]}; candidates=[]
    for side in ('CALL','PUT'):
        A={tf:timeframe_analysis(cs,side) for tf,cs in tfs.items()}
        # Three-timeframe agreement: each timeframe must support same structural setup.
        trend_all=all(x['trend3'] and x['abc'] for x in A.values())
        sr_all=all((x['sr3'] or x['double']) for x in A.values())
        breakout_all=all(x['breakout'] for x in A.values())
        pat=A['M1']['pattern']
        # Entry confirmation is on the just-closed M1 candle, after M1/M5/M15 structural agreement.
        if not pat:continue
        if trend_all: candidates.append((10,'Tendência de alta — 3º toque + pernas A/B/C' if side=='CALL' else 'Tendência de baixa — 3º toque + pernas A/B/C',side,A,pat))
        if sr_all: candidates.append((9,'Suporte/Resistência — 3º toque / topo-fundo duplo',side,A,pat))
        if breakout_all: candidates.append((8,'Rompimento confirmado nos 3 tempos',side,A,pat))
    if not candidates:return None
    score,analysis,side,A,pat=max(candidates,key=lambda z:z[0])
    reasons=[analysis,'Confluência M1 + M5 + M15','110 candles por tempo gráfico',f'Confirmação M1: {pat}']
    if all(x['abc'] for x in A.values()):reasons.append('Pernadas A/B/C alinhadas')
    if all(x['sr3'] or x['double'] for x in A.values()):reasons.append('3º toque / nível duplo confirmado por corpo')
    if all(x['breakout'] for x in A.values()):reasons.append('Rompimento no mesmo sentido')
    return Signal(symbol,name,side,score,m1[-1].close,analysis,pat,reasons,A['M1']['support'],A['M1']['resistance'],['M1','M5','M15'])

async def ws_public(payload,req_id=1):
    async with websockets.connect(PUBLIC_WS,ping_interval=20,ping_timeout=20,open_timeout=15) as ws:
        p=dict(payload);p['req_id']=req_id;await ws.send(json.dumps(p))
        while True:
            d=json.loads(await ws.recv())
            if d.get('req_id')==req_id:
                if 'error' in d:raise RuntimeError(d['error'].get('message','Erro Deriv'))
                return d
async def active_symbols():return (await ws_public({'active_symbols':'brief','contract_type':['CALL','PUT']},100)).get('active_symbols',[])
def sym_fields(a):return a.get('underlying_symbol') or a.get('symbol'), a.get('underlying_symbol_name') or a.get('display_name') or a.get('underlying_symbol') or a.get('symbol')
def is_derived(a):
    vals=' '.join(str(a.get(k,'')) for k in ('market','submarket','subgroup','underlying_symbol_type','symbol_type','underlying_symbol_name','display_name')).lower()
    # Deriv commonly labels these markets synthetic/derived; include named synthetic families.
    keys=('synthetic','derived','volatility','crash','boom','jump','step','range break','drift switch','daily reset','dex')
    return any(k in vals for k in keys)
async def candles(symbol,granularity,count=110):
    d=await ws_public({'ticks_history':symbol,'adjust_start_time':1,'count':count,'end':'latest','granularity':granularity,'style':'candles'},1000+granularity)
    out=[]
    for c in d.get('candles',[]):
        try:out.append(Candle(int(c['epoch']),float(c['open']),float(c['high']),float(c['low']),float(c['close'])))
        except:pass
    return out
async def analyze_one(a,sem):
    async with sem:
        symbol,name=sym_fields(a)
        if not symbol:return None
        try:
            m1=await candles(symbol,60); await asyncio.sleep(.08); m5=await candles(symbol,300); await asyncio.sleep(.08); m15=await candles(symbol,900)
            return analyze(symbol,name,m1,m5,m15)
        except Exception:return None
async def scan_once():
    if scan_lock.locked():
        return state['signals']
    async with scan_lock:
        state['scanning']=True
        state['status']='Escaneando índices derivados • M1/M5/M15...'
        try:
            allsyms=await active_symbols()
            syms=[a for a in allsyms if is_derived(a)]
            sem=asyncio.Semaphore(3)
            results=await asyncio.gather(*[analyze_one(a,sem) for a in syms])
            sig=[x for x in results if x]
            sig.sort(key=lambda x:x.score,reverse=True)
            sig=[x for x in sig if x.score >= config.min_score]
            state['signals']=[asdict(x) for x in sig[:25]]
            state['last_scan']=int(time.time())
            state['diagnostics']={'ativos_derivados':len(syms),'sinais':len(sig),'candles_por_tf':110,'timeframes':['M1','M5','M15']}
            state['status']=(f'ATIVO • {len(syms)} derivados analisados • {len(sig)} sinais'
                             if state['running'] else
                             f'{len(syms)} derivados analisados • {len(sig)} sinais')
            print(f"[ATR] Scanner MTF | derivados={len(syms)} | sinais={len(sig)}", flush=True)
            return state['signals']
        finally:
            state['scanning']=False

async def scanner_loop():
    print('[ATR] Scanner automático iniciado.', flush=True)
    try:
        while state['running']:
            try:
                await scan_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                state['status']=f'Erro no scanner: {str(e)[:120]}'
                print(f'[ATR] Erro scanner: {e}', flush=True)
            for _ in range(15):
                if not state['running']:
                    break
                await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        print('[ATR] Scanner automático parado.', flush=True)

async def deriv_rest(request,method,path):
    token=request.session.get('deriv_token')
    if not token:raise HTTPException(401,'Conecte sua conta Deriv primeiro.')
    async with httpx.AsyncClient(timeout=20) as client:r=await client.request(method,REST_BASE+path,headers={'Authorization':f'Bearer {token}'})
    try:d=r.json()
    except:d={'error':r.text}
    if r.status_code>=400:raise HTTPException(r.status_code,d)
    return d
async def get_accounts(request):
    d=await deriv_rest(request,'GET','/trading/v1/options/accounts');return d.get('data',d if isinstance(d,list) else [])
def account_type(a):return str(a.get('account_type','')).lower()
@app.get('/')
async def home():return FileResponse(STATIC_DIR / 'index.html')
@app.get('/health')
async def health():return {'ok':True,'service':'ATR Deriv MTF v3'}
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
    request.session['deriv_token']=d['access_token'];return RedirectResponse('/?deriv=connected')
@app.post('/auth/logout')
async def logout(request:Request):request.session.clear();return {'ok':True}
@app.get('/api/deriv/accounts')
async def api_accounts(request:Request):
    if not request.session.get('deriv_token'):return {'connected':False,'accounts':[],'selected':None}
    accounts=await get_accounts(request);return {'connected':True,'accounts':accounts,'selected':request.session.get('deriv_account_id')}
@app.post('/api/deriv/select')
async def select_account(choice:AccountChoice,request:Request):
    accounts=await get_accounts(request);acc=next((a for a in accounts if a.get('account_id')==choice.account_id),None)
    if not acc:raise HTTPException(404,'Conta não encontrada.')
    request.session['deriv_account_id']=choice.account_id;request.session['deriv_account_type']=account_type(acc);state['balance']=float(acc.get('balance',state['balance']));state['start_balance']=state['balance'];return {'ok':True,'account':acc}
async def authenticated_ws_url(request):
    aid=request.session.get('deriv_account_id')
    if not aid:raise HTTPException(400,'Selecione uma conta DEMO ou REAL.')
    d=await deriv_rest(request,'POST',f'/trading/v1/options/accounts/{aid}/otp');data=d.get('data',d);url=data.get('url') or data.get('websocket_url')
    if not url:raise HTTPException(502,f'Deriv não retornou URL WebSocket: {d}')
    return url
async def monitor_contract(url, contract_id, entry):
    try:
        async with websockets.connect(url,ping_interval=20,ping_timeout=20) as ws:
            await ws.send(json.dumps({'proposal_open_contract':1,'contract_id':contract_id,'subscribe':1,'req_id':601}))
            while True:
                d=json.loads(await ws.recv())
                if 'error' in d: raise RuntimeError(d['error'].get('message','Erro ao acompanhar contrato'))
                poc=d.get('proposal_open_contract') or {}
                if not poc: continue
                profit=float(poc.get('profit') or 0); buy_price=float(poc.get('buy_price') or entry['stake'])
                entry.update({'profit':profit,'buy_price':buy_price,'current_spot':poc.get('current_spot'),'entry_spot':poc.get('entry_spot'),'exit_tick':poc.get('exit_tick'),'is_sold':bool(poc.get('is_sold')),'status':poc.get('status','open')})
                state['open_trade']=dict(entry)
                if poc.get('is_sold') or poc.get('status') in ('sold','won','lost'):
                    result='WIN' if profit>0 else ('LOSS' if profit<0 else 'EMPATE')
                    entry['result']=result; entry['closed_time']=int(time.time()); entry['side']='COMPRA' if entry['direction']=='CALL' else 'VENDA'
                    state['history'].insert(0,dict(entry)); state['history']=state['history'][:100]; state['open_trade']=None
                    state['balance']=round(state['balance']+profit,2); state['status']=f"Finalizada: {result} • {entry['side']} • {entry['name']}"
                    return
    except Exception as e:
        if state.get('open_trade') and state['open_trade'].get('contract_id')==contract_id:
            state['open_trade']['monitor_error']=str(e); state['status']='Ordem aberta; falha temporária ao acompanhar contrato.'

async def trade_best(request):
    if state.get('open_trade'): raise HTTPException(409,'Já existe uma operação aberta. Aguarde finalizar.')
    if not state['signals']:await scan_once()
    if not state['signals']:raise HTTPException(400,'Nenhuma confluência M1/M5/M15 confirmada.')
    aid=request.session.get('deriv_account_id');typ=request.session.get('deriv_account_type','')
    if not aid:raise HTTPException(400,'Selecione a conta Deriv.')
    best=state['signals'][0];accounts=await get_accounts(request);acc=next((a for a in accounts if a.get('account_id')==aid),None)
    if not acc:raise HTTPException(404,'Conta selecionada não está disponível.')
    balance=float(acc.get('balance',0));state['balance']=balance
    start=state.get('start_balance') or balance
    pnl=((balance-start)/start)*100 if start else 0
    if pnl >= config.stop_gain: raise HTTPException(409,'Stop Gain atingido.')
    if pnl <= -config.stop_loss: raise HTTPException(409,'Stop Loss atingido.')
    if state['entries'] >= config.max_entradas: raise HTTPException(409,'Limite de operações atingido.')
    stake=round(balance*config.percentual_entrada/100,2)
    if stake<=0:raise HTTPException(400,'Valor da entrada inválido.')
    url=await authenticated_ws_url(request)
    async with websockets.connect(url,ping_interval=20,ping_timeout=20) as ws:
        proposal={'proposal':1,'amount':stake,'basis':'stake','contract_type':best['direction'],'currency':acc.get('currency','USD'),'duration':config.duracao_minutos,'duration_unit':'m','underlying_symbol':best['symbol'],'req_id':501}
        await ws.send(json.dumps(proposal));pd=json.loads(await ws.recv())
        if 'error' in pd:raise HTTPException(400,pd['error'].get('message','Erro na proposta'))
        prop=pd.get('proposal',{});pid=prop.get('id')
        if not pid:raise HTTPException(400,f'Proposta sem ID: {pd}')
        ask=float(prop.get('ask_price',stake));await ws.send(json.dumps({'buy':pid,'price':ask,'req_id':502}));bd=json.loads(await ws.recv())
        if 'error' in bd:raise HTTPException(400,bd['error'].get('message','Erro ao comprar contrato'))
    buy=bd.get('buy',{}); cid=buy.get('contract_id')
    if not cid: raise HTTPException(502,f'Deriv não retornou contract_id: {bd}')
    entry={'time':int(time.time()),'symbol':best['symbol'],'name':best['name'],'direction':best['direction'],'side':'COMPRA' if best['direction']=='CALL' else 'VENDA','score':best['score'],'stake':stake,'result':'ABERTA','mode':typ.upper(),'analysis':best['analysis'],'confirmation':best['confirmation'],'contract_id':cid,'profit':0.0,'duration_minutes':config.duracao_minutos}
    state['open_trade']=dict(entry);state['entries']+=1;state['status']=f"ABERTA • {entry['side']} • {best['name']} • {typ.upper()}"
    monitor_url=await authenticated_ws_url(request)
    asyncio.create_task(monitor_contract(monitor_url,cid,entry))
    return {'ok':True,'entry':entry}
@app.post('/api/deriv/trade')
async def deriv_trade(request:Request):return await trade_best(request)
@app.get('/api/status')
async def get_status():
    start=state['start_balance'];pnl=((state['balance']-start)/start)*100 if start else 0;return {**state,'config':config.model_dump(),'pnl_percent':round(pnl,2)}
@app.post('/api/config')
async def set_config(new:Config):
    global config;config=new;return {'ok':True,'config':config.model_dump()}
@app.post('/api/scan')
async def api_scan():return {'signals':await scan_once()}
@app.post('/api/start')
async def start():
    global scanner_task
    if state['running']:
        return {'ok':True,'message':'Scanner já está ativo.'}
    state['running']=True
    state['status']='ATIVO • iniciando scanner M1/M5/M15...'
    scanner_task=asyncio.create_task(scanner_loop())
    return {'ok':True}

@app.post('/api/stop')
async def stop():
    global scanner_task
    state['running']=False
    state['status']='Parado'
    if scanner_task and not scanner_task.done():
        scanner_task.cancel()
    scanner_task=None
    return {'ok':True}
app.mount('/static',StaticFiles(directory=str(STATIC_DIR)),name='static')
