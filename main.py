import asyncio, os, time, uuid
from dataclasses import dataclass, asdict, field
from typing import List, Optional
import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app=FastAPI(title='Operação Alvo Certo (ATR) - Mercado Bitcoin REAL')
MB_BASE='https://api.mercadobitcoin.net/api/v4'
MB_API_ID=os.getenv('MB_API_ID','').strip(); MB_API_SECRET=os.getenv('MB_API_SECRET','').strip(); MB_ACCOUNT_ID=os.getenv('MB_ACCOUNT_ID','').strip()
SYMBOLS=['BTC-BRL','ETH-BRL']; _token={'value':None,'expires_at':0.0}

class Config(BaseModel):
    percentual_entrada: float=1.0
    stop_gain: float=3.0
    stop_loss: float=2.0
    max_entradas: int=5
    min_score: int=7
    filtro_volatilidade: bool=True
    max_movimento_m1_pct: float=1.0
    auto_trade: bool=False

config=Config()
state={'running':False,'balance':None,'available_brl':None,'start_balance':None,'mb_connected':False,'mb_error':None,'entries':0,'signals':[],'history':[],'last_scan':None,'last_balance_update':None,'status':'Parado','assets':{},'prices':{},'market_blocked':False,'block_reason':None,'mb_diag':{'credentials_configured':bool(MB_API_ID and MB_API_SECRET),'auth':False,'account':False,'balances':False,'prices':False,'account_type':None,'brl_total':None,'brl_available':None,'last_error':None}}

@dataclass
class Candle: epoch:int; open:float; high:float; low:float; close:float
@dataclass
class Signal:
    symbol:str; name:str; direction:str; score:int; price:float; reasons:List[str]=field(default_factory=list); support:float=0.0; resistance:float=0.0

async def token():
    now=time.time()
    if _token['value'] and now < _token['expires_at']-60: return _token['value']
    if not MB_API_ID or not MB_API_SECRET: raise RuntimeError('Configure MB_API_ID e MB_API_SECRET no Render.')
    async with httpx.AsyncClient(timeout=20) as c:
        r=await c.post(f'{MB_BASE}/oauth2/token',data={'grant_type':'client_credentials','scope':'global','client_id':MB_API_ID,'client_secret':MB_API_SECRET},headers={'Content-Type':'application/x-www-form-urlencoded'}); r.raise_for_status(); d=r.json()
    _token['value']=d['access_token']; _token['expires_at']=now+int(d.get('expires_in',3500)); return _token['value']

async def mb_get(path, params=None, private=False):
    headers={}
    if private: headers['Authorization']=f'Bearer {await token()}'
    async with httpx.AsyncClient(timeout=20) as c:
        r=await c.get(f'{MB_BASE}{path}',params=params,headers=headers); r.raise_for_status(); return r.json()

async def mb_post(path, payload):
    async with httpx.AsyncClient(timeout=20) as c:
        r=await c.post(f'{MB_BASE}{path}',json=payload,headers={'Authorization':f'Bearer {await token()}'}); r.raise_for_status(); return r.json()

async def account_id():
    if MB_ACCOUNT_ID:return MB_ACCOUNT_ID
    a=await mb_get('/accounts',private=True)
    live=next((x for x in a if x.get('type')=='live'),None)
    if not live: raise RuntimeError('Conta REAL (live) não encontrada.')
    return live['id']

async def market_prices():
    rows=await mb_get('/tickers',{'symbols':','.join(SYMBOLS)})
    return {x['pair']:float(x['last']) for x in rows}

async def refresh_balance():
    diag={'credentials_configured':bool(MB_API_ID and MB_API_SECRET),'auth':False,'account':False,'balances':False,'prices':False,'account_type':None,'brl_total':None,'brl_available':None,'last_error':None}
    try:
        # 1) autentica e localiza a conta REAL
        await token(); diag['auth']=True
        aid=await account_id(); diag['account']=True; diag['account_type']='live'

        # 2) saldo vem primeiro. Falha no ticker nao pode zerar a banca em BRL.
        bals=await mb_get(f'/accounts/{aid}/balances',private=True); diag['balances']=True
        assets={}; brl_total=0.0; brl_available=0.0
        for b in bals:
            sym=str(b.get('symbol','')).upper(); total=float(b.get('total',0) or 0); avail=float(b.get('available',0) or 0)
            if total>0 or avail>0: assets[sym]={'total':total,'available':avail}
            if sym=='BRL': brl_total=total; brl_available=avail
        diag['brl_total']=round(brl_total,2); diag['brl_available']=round(brl_available,2)
        state['assets']=assets; state['available_brl']=round(brl_available,2)
        state['balance']=round(brl_total,2)

        # 3) tenta somar BTC/ETH ao patrimonio; se cotacao falhar, preserva o BRL real.
        try:
            prices=await market_prices(); state['prices']=prices; diag['prices']=True
            patrimonio=brl_total
            for sym,a in assets.items():
                pair=f'{sym}-BRL'
                if sym!='BRL' and pair in prices: patrimonio += a['total']*prices[pair]
            state['balance']=round(patrimonio,2)
        except Exception as pe:
            diag['last_error']=f'Cotacoes: {type(pe).__name__}: {str(pe)[:160]}'

        if state['start_balance'] is None and state['balance'] is not None: state['start_balance']=state['balance']
        state['mb_connected']=True; state['mb_error']=diag['last_error']; state['last_balance_update']=int(time.time()); state['mb_diag']=diag
        print(f"MB saldo OK | BRL total={diag['brl_total']} disponivel={diag['brl_available']} | precos={diag['prices']}", flush=True)
        return True
    except Exception as e:
        msg=f'{type(e).__name__}: {str(e)[:220]}'
        diag['last_error']=msg; state['mb_diag']=diag; state['mb_connected']=False; state['mb_error']=msg; state['balance']=None; state['available_brl']=None
        print(f'MB saldo ERRO | {msg}', flush=True)
        return False

async def balance_loop():
    while True: await refresh_balance(); await asyncio.sleep(15)

@app.on_event('startup')
async def startup(): asyncio.create_task(balance_loop())

def ema(v,p):
    if not v:return []
    a=2/(p+1); out=[v[0]]
    for x in v[1:]: out.append(a*x+(1-a)*out[-1])
    return out

async def candles(symbol,resolution,count):
    d=await mb_get('/candles',{'symbol':symbol,'resolution':resolution,'to':int(time.time()),'countback':count})
    out=[]
    for i,t in enumerate(d.get('t',[])):
        out.append(Candle(int(t),float(d['o'][i]),float(d['h'][i]),float(d['l'][i]),float(d['c'][i])))
    return out

def volatility(m1):
    if not config.filtro_volatilidade or not m1:return None
    c=m1[-1]; move=abs(c.close-c.open)/(c.open or c.close)*100
    return f'Volatilidade alta M1: {move:.2f}%' if move>=config.max_movimento_m1_pct else None

def analyze(symbol,m1,m15):
    if len(m1)<110 or len(m15)<110:return None
    vb=volatility(m1)
    if vb:return None
    closes=[x.close for x in m1]; highs=[x.high for x in m1]; lows=[x.low for x in m1]; h3=[(x.high+x.low+x.close)/3 for x in m1]
    e10,e100,e3,e13=ema(closes,10),ema(closes,100),ema(h3,3),ema(h3,13); c0,c1,c2,c3=m1[-1],m1[-2],m1[-3],m1[-4]
    res=max(highs[-11:-1]); sup=min(lows[-11:-1]); cs=ps=0; cr=[]; pr=[]
    tests=[(c0.close>c1.close and c0.close>e10[-1] and e10[-1]>e10[-2],1,'Tendência M1 alta','c'),(c0.close<c1.close and c0.close<e10[-1] and e10[-1]<e10[-2],1,'Tendência M1 baixa','p'),(c0.close>e100[-1] and e10[-1]>e100[-1],2,'EMA10 > EMA100','c'),(c0.close<e100[-1] and e10[-1]<e100[-1],2,'EMA10 < EMA100','p'),(e3[-2]<e13[-2] and e3[-1]>e13[-1],1,'Cruzamento EMA3/13','c'),(e3[-2]>e13[-2] and e3[-1]<e13[-1],1,'Cruzamento EMA3/13','p'),(c1.close<c1.open and c0.close>c0.open and c0.open<=c1.close and c0.close>=c1.open,2,'Engolfo alta','c'),(c1.close>c1.open and c0.close<c0.open and c0.open>=c1.close and c0.close<=c1.open,2,'Engolfo baixa','p'),(c0.close>c1.close>c2.close>=c3.close,1,'Sequência alta','c'),(c0.close<c1.close<c2.close<=c3.close,1,'Sequência baixa','p')]
    c15=[x.close for x in m15]; a15,b15=ema(c15,10),ema(c15,100)
    tests += [(c15[-1]>a15[-1]>b15[-1] and a15[-1]>a15[-2],2,'M15 confirma alta','c'),(c15[-1]<a15[-1]<b15[-1] and a15[-1]<a15[-2],2,'M15 confirma baixa','p')]
    for ok,pts,msg,side in tests:
        if ok:
            if side=='c':cs+=pts;cr.append(msg)
            else:ps+=pts;pr.append(msg)
    span=max(res-sup,1e-12); pos=(c0.close-sup)/span
    if pos<=.35:cs+=1;cr.append('Próximo ao suporte')
    if pos>=.65:ps+=1;pr.append('Próximo à resistência')
    name='Bitcoin' if symbol.startswith('BTC') else 'Ethereum'
    if cs>=config.min_score and cs>ps:return Signal(symbol,name,'COMPRA',cs,c0.close,cr,sup,res)
    if ps>=config.min_score and ps>cs:return Signal(symbol,name,'VENDA',ps,c0.close,pr,sup,res)
    return None

async def scan_once():
    state['status']='Analisando Mercado Bitcoin...'; sig=[]
    for sym in SYMBOLS:
        try:
            m1,m15=await asyncio.gather(candles(sym,'1m',140),candles(sym,'15m',120)); s=analyze(sym,m1,m15)
            if s:sig.append(s)
        except Exception as e: state['mb_error']=str(e)[:240]
    sig.sort(key=lambda x:x.score,reverse=True); state['signals']=[asdict(x) for x in sig]; state['last_scan']=int(time.time()); state['status']=f'{len(SYMBOLS)} ativos analisados'; return state['signals']

def risk_ok():
    if not state['mb_connected']:return False,'API Mercado Bitcoin desconectada.'
    bal=state['balance']; start=state['start_balance']; pnl=((bal-start)/start*100) if (bal is not None and start not in (None,0)) else 0
    if pnl>=config.stop_gain:return False,'Stop Gain da sessão atingido.'
    if pnl<=-config.stop_loss:return False,'Stop Loss da sessão atingido.'
    if state['entries']>=config.max_entradas:return False,'Limite de entradas atingido.'
    return True,''

async def real_buy(signal):
    ok,msg=risk_ok()
    if not ok:return {'ok':False,'message':msg}
    avail=state['available_brl'];
    if avail is None:return {'ok':False,'message':'Saldo indisponível. Ordem REAL bloqueada.'}
    stake=round(avail*config.percentual_entrada/100,2)
    if stake<=0:return {'ok':False,'message':'Saldo BRL disponível insuficiente.'}
    aid=await account_id(); ext=f'ATR-{int(time.time())}-{uuid.uuid4().hex[:8]}'
    payload={'type':'market','side':'buy','cost':stake,'async':False,'externalId':ext}
    try:
        order=await mb_post(f'/accounts/{aid}/{signal["symbol"]}/orders',payload)
        row={'time':int(time.time()),'symbol':signal['symbol'],'name':signal['name'],'direction':'COMPRA','score':signal['score'],'stake':stake,'entry_price':signal['price'],'result':order.get('status','enviada'),'order_id':order.get('id','')}
        state['history'].insert(0,row);state['history']=state['history'][:50];state['entries']+=1;await refresh_balance();return {'ok':True,'entry':row}
    except Exception as e:return {'ok':False,'message':f'Ordem não enviada: {str(e)[:180]}'}

async def robot_loop():
    while state['running']:
        try:
            sigs=await scan_once()
            if config.auto_trade and sigs and sigs[0]['direction']=='COMPRA': await real_buy(sigs[0])
        except Exception as e:state['status']=f'Erro: {str(e)[:100]}'
        await asyncio.sleep(60)

@app.get('/')
async def home():return FileResponse('static/index.html')
@app.get('/api/status')
async def status():
    bal=state['balance']; start=state['start_balance']; pnl=((bal-start)/start*100) if (bal is not None and start not in (None,0)) else 0
    return {**state,'config':config.model_dump(),'pnl_percent':round(pnl,2)}
@app.post('/api/mb-refresh')
async def mbrefresh():return {'ok':await refresh_balance(),'balance':state['balance'],'error':state['mb_error']}
@app.post('/api/config')
async def setcfg(new:Config):
    global config;config=new;return {'ok':True,'config':config.model_dump()}
@app.post('/api/scan')
async def scan():
    s=await scan_once();return {'signals':s,'count':len(s)}
@app.post('/api/start')
async def start():
    if not state['running']:
        state['running']=True;state['status']='Ativo - conta REAL';asyncio.create_task(robot_loop())
    return {'ok':True}
@app.post('/api/stop')
async def stop():state['running']=False;state['status']='Parado';return {'ok':True}
@app.post('/api/real-entry')
async def realentry():
    if not state['signals']:return {'ok':False,'message':'Nenhum sinal disponível.'}
    best=state['signals'][0]
    if best['direction']!='COMPRA':return {'ok':False,'message':'O melhor sinal é de VENDA. Por segurança, esta versão não vende moedas preexistentes da sua conta.'}
    return await real_buy(best)

app.mount('/static',StaticFiles(directory='static'),name='static')
