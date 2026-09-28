import asyncio, os, time, uuid, math
from dataclasses import dataclass, asdict, field
from typing import List
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app=FastAPI(title="Operação Alvo Certo (ATR) - Mercado Bitcoin APRIMORADO")
MB_BASE="https://api.mercadobitcoin.net/api/v4"
GDELT="https://api.gdeltproject.org/api/v2/doc/doc"
MB_API_ID=os.getenv("MB_API_ID","").strip()
MB_API_SECRET=os.getenv("MB_API_SECRET","").strip()
MB_ACCOUNT_ID=os.getenv("MB_ACCOUNT_ID","").strip()
PREFERRED=["BTC-BRL","ETH-BRL","SOL-BRL","XRP-BRL","ADA-BRL","DOGE-BRL","LINK-BRL","LTC-BRL"]
NEWS_NAMES={"BTC":"bitcoin","ETH":"ethereum","SOL":"solana","XRP":"xrp","ADA":"cardano","DOGE":"dogecoin","LINK":"chainlink","LTC":"litecoin"}

class Config(BaseModel):
    percentual_entrada: float=1.0
    stop_gain: float=3.0
    stop_loss: float=2.0
    max_entradas: int=5
    min_score: int=7
    max_ativos: int=8
    duracao_segundos: int=60
    timeframe: str="1m"
    take_profit_operacao: float=1.5
    stop_loss_operacao: float=1.0
    auto_trade: bool=False
    max_movimento_pct: float=1.0
    news_filter: bool=True
    news_confirm_only: bool=True
    news_exit_enabled: bool=True
    news_exit_min_strength: int=3
    news_exit_confirm_pct: float=0.15
    news_check_seconds: int=60

config=Config()
state={"running":False,"balance":None,"available_brl":None,"start_balance":None,
       "entries":0,"signals":[],"analyses":[],"history":[],"last_scan":None,
       "status":"Parado","mb_connected":False,"symbols":[],"open_trade":None,
       "scan_progress":"","last_error":None,"news":{},"realized_pnl_brl":0.0}
_tok={"value":None,"expires":0}
_loop_task=None
_trade_lock=asyncio.Lock()

@dataclass
class Candle:
    epoch:int; open:float; high:float; low:float; close:float
@dataclass
class Analysis:
    symbol:str; name:str; result:str; score:int; buy_score:int; sell_score:int
    price:float; reasons:List[str]=field(default_factory=list)
    support:float=0.0; resistance:float=0.0; volatile:bool=False
    news_direction:str="NEUTRA"; news_strength:int=0

def ema(vals, period):
    if not vals:return []
    a=2/(period+1); out=[vals[0]]
    for v in vals[1:]:out.append(a*v+(1-a)*out[-1])
    return out

async def get_token():
    if _tok["value"] and time.time()<_tok["expires"]-30:return _tok["value"]
    if not MB_API_ID or not MB_API_SECRET:raise RuntimeError("Configure MB_API_ID e MB_API_SECRET no Render.")
    async with httpx.AsyncClient(timeout=20) as c:
        r=await c.post(f"{MB_BASE}/oauth2/token",data={"grant_type":"client_credentials","scope":"global","client_id":MB_API_ID,"client_secret":MB_API_SECRET})
        r.raise_for_status(); d=r.json()
    _tok["value"]=d["access_token"]; _tok["expires"]=time.time()+int(d.get("expires_in",300))
    return _tok["value"]

async def mb_get(path,params=None,private=False):
    h={}
    if private:h["Authorization"]=f"Bearer {await get_token()}"
    async with httpx.AsyncClient(timeout=30) as c:
        r=await c.get(MB_BASE+path,params=params,headers=h); r.raise_for_status(); return r.json()

async def mb_post(path,payload):
    h={"Authorization":f"Bearer {await get_token()}"}
    async with httpx.AsyncClient(timeout=30) as c:
        r=await c.post(MB_BASE+path,json=payload,headers=h)
        if r.status_code>=400:raise RuntimeError(f"MB {r.status_code}: {r.text[:250]}")
        return r.json()

async def account_id():
    if MB_ACCOUNT_ID:return MB_ACCOUNT_ID
    a=await mb_get("/accounts",private=True)
    rows=(a.get("accounts") or a.get("data") or a.get("items") or []) if isinstance(a,dict) else a
    valid=[x for x in rows if isinstance(x,dict) and x.get("id")]
    if not valid:raise RuntimeError("Nenhuma conta válida retornada por /accounts.")
    return next((x for x in valid if str(x.get("type","")).lower() in ("live","real")),valid[0])["id"]

async def balances():
    aid=await account_id(); raw=await mb_get(f"/accounts/{aid}/balances",private=True)
    rows=(raw.get("balances") or raw.get("data") or raw.get("items") or []) if isinstance(raw,dict) else raw
    return {str(x.get("symbol","")).upper():x for x in rows if isinstance(x,dict)}

async def refresh_balance():
    try:
        b=await balances(); brl=b.get("BRL",{})
        state["balance"]=float(brl.get("total",0) or 0); state["available_brl"]=float(brl.get("available",0) or 0)
        state["mb_connected"]=True
        if state["start_balance"] is None:state["start_balance"]=state["balance"]
    except Exception as e:
        state["mb_connected"]=False; state["last_error"]=f"Saldo: {type(e).__name__}: {e}"

async def symbol_table():
    d=await mb_get("/symbols"); syms=d.get("symbol",[])
    traded=d.get("exchange-traded",[True]*len(syms)); cur=d.get("currency",[""]*len(syms)); desc=d.get("description",syms)
    mincost=d.get("min-cost",["0"]*len(syms)); minvol=d.get("min-volume",["0"]*len(syms)); roundlot=d.get("round-lot",["0"]*len(syms))
    out={}
    for i,s in enumerate(syms):
        out[s]={"symbol":s,"traded":bool(traded[i]) if i<len(traded) else True,"currency":cur[i] if i<len(cur) else "",
                "description":desc[i] if i<len(desc) else s,"min_cost":float(mincost[i] or 0) if i<len(mincost) else 0,
                "min_volume":float(minvol[i] or 0) if i<len(minvol) else 0,"round_lot":float(roundlot[i] or 0) if i<len(roundlot) else 0}
    return out

async def discover_symbols():
    t=await symbol_table(); brl=[s for s,x in t.items() if x["traded"] and x["currency"]=="BRL"]
    ordered=[s for s in PREFERRED if s in brl]+[s for s in brl if s not in PREFERRED]
    return ordered[:max(1,min(config.max_ativos,20))],t

async def raw_candles(symbol,resolution,count):
    d=await mb_get("/candles",{"symbol":symbol,"resolution":resolution,"to":int(time.time()),"countback":count})
    t,o,h,l,c=d.get("t",[]),d.get("o",[]),d.get("h",[]),d.get("l",[]),d.get("c",[])
    n=min(map(len,[t,o,h,l,c])) if all(isinstance(x,list) for x in [t,o,h,l,c]) else 0
    rows=[Candle(int(t[i]),float(o[i]),float(h[i]),float(l[i]),float(c[i])) for i in range(n)]
    rows.sort(key=lambda x:x.epoch)
    return rows

def aggregate_5m(rows):
    buckets={}
    for x in rows:
        k=x.epoch-(x.epoch%300)
        buckets.setdefault(k,[]).append(x)
    out=[]
    for k in sorted(buckets):
        b=buckets[k]
        out.append(Candle(k,b[0].open,max(x.high for x in b),min(x.low for x in b),b[-1].close))
    return out

async def candles(symbol,resolution,count):
    if resolution=="5m":
        rows=await raw_candles(symbol,"1m",max(count*5+25,825))
        return aggregate_5m(rows)[-count:]
    return (await raw_candles(symbol,resolution,max(count,160)))[-count:]

async def news_signal(symbol):
    if not config.news_filter:return {"direction":"DESLIGADA","strength":0,"positive":0,"negative":0}
    base=symbol.split("-")[0]; term=NEWS_NAMES.get(base,base.lower())
    async def count(q):
        try:
            async with httpx.AsyncClient(timeout=12) as c:
                r=await c.get(GDELT,params={"query":q,"mode":"artlist","maxrecords":20,"timespan":"3h","sort":"datedesc","format":"json"})
                r.raise_for_status(); d=r.json()
                return len(d.get("articles",[]) if isinstance(d,dict) else [])
        except Exception:return 0
    pos,neg=await asyncio.gather(count(f'{term} tone>2'),count(f'{term} tone<-2'))
    diff=pos-neg
    direction="ALTA" if diff>=2 else "BAIXA" if diff<=-2 else "NEUTRA"
    return {"direction":direction,"strength":abs(diff),"positive":pos,"negative":neg}

def pivots(rows,wing=2):
    out=[]
    for i in range(wing,len(rows)-wing):
        seg=rows[i-wing:i+wing+1]
        if rows[i].high==max(x.high for x in seg): out.append(("H",i,rows[i].high))
        if rows[i].low==min(x.low for x in seg): out.append(("L",i,rows[i].low))
    return sorted(out,key=lambda z:z[1])

def structure_features(rows):
    ps=pivots(rows[-60:],2); hs=[x for x in ps if x[0]=="H"]; ls=[x for x in ps if x[0]=="L"]
    regime="LATERAL"
    if len(hs)>=2 and len(ls)>=2:
        if hs[-1][2]>hs[-2][2] and ls[-1][2]>ls[-2][2]: regime="ALTA"
        elif hs[-1][2]<hs[-2][2] and ls[-1][2]<ls[-2][2]: regime="BAIXA"
    abc_up=abc_dn=False
    for i in range(max(0,len(ps)-8),len(ps)-2):
        x,y,z=ps[i:i+3]
        if x[0]=="L" and y[0]=="H" and z[0]=="L" and z[2]>x[2] and rows[-1].close>y[2]: abc_up=True
        if x[0]=="H" and y[0]=="L" and z[0]=="H" and z[2]<x[2] and rows[-1].close<y[2]: abc_dn=True
    third_sup=third_res=False
    if len(ls)>=3:
        v=[x[2] for x in ls[-3:]]; third_sup=(max(v)-min(v))<=sum(v)/3*0.0035
    if len(hs)>=3:
        v=[x[2] for x in hs[-3:]]; third_res=(max(v)-min(v))<=sum(v)/3*0.0035
    return regime,abc_up,abc_dn,third_sup,third_res

def analyze(symbol,name,primary,m15,news):
    if len(primary)<110 or len(m15)<110:
        return Analysis(symbol,name,"AGUARDAR",0,0,0,primary[-1].close if primary else 0,
          [f"Histórico insuficiente: principal {len(primary)}/110 • M15 {len(m15)}/110"],
          news_direction=news["direction"],news_strength=news["strength"])
    closes=[x.close for x in primary]; highs=[x.high for x in primary]; lows=[x.low for x in primary]
    hlc3=[(x.high+x.low+x.close)/3 for x in primary]
    e10,e100,e3,e13=ema(closes,10),ema(closes,100),ema(hlc3,3),ema(hlc3,13)
    c0,c1,c2,c3=primary[-1],primary[-2],primary[-3],primary[-4]
    resistance=max(highs[-11:-1]); support=min(lows[-11:-1]); span=max(resistance-support,1e-12)
    regime,abc_up,abc_dn,third_sup,third_res=structure_features(primary)
    macro_up=c0.close>e100[-1] and e10[-1]>e100[-1]; macro_dn=c0.close<e100[-1] and e10[-1]<e100[-1]
    cross_up=e3[-2]<e13[-2] and e3[-1]>e13[-1]; cross_dn=e3[-2]>e13[-2] and e3[-1]<e13[-1]
    bull_eng=c1.close<c1.open and c0.close>c0.open and c0.open<=c1.close and c0.close>=c1.open
    bear_eng=c1.close>c1.open and c0.close<c0.open and c0.open>=c1.close and c0.close<=c1.open
    bull_seq=c0.close>c0.open and c1.close>c1.open and c2.close>c2.open and c0.close>c1.close>c2.close
    bear_seq=c0.close<c0.open and c1.close<c1.open and c2.close<c2.open and c0.close<c1.close<c2.close
    body=max(abs(c0.close-c0.open),1e-12); lower=min(c0.open,c0.close)-c0.low; upper=c0.high-max(c0.open,c0.close)
    reject_up=lower>=body*1.5 and c0.close>c0.open; reject_dn=upper>=body*1.5 and c0.close<c0.open
    prev_res=max(highs[-12:-2]); prev_sup=min(lows[-12:-2])
    pullback_up=c1.close>prev_res and c0.low<=prev_res*1.003 and c0.close>prev_res
    pullback_dn=c1.close<prev_sup and c0.high>=prev_sup*0.997 and c0.close<prev_sup
    cl15=[x.close for x in m15]; e1015,e10015=ema(cl15,10),ema(cl15,100)
    m15_up=cl15[-1]>e1015[-1]>e10015[-1]; m15_dn=cl15[-1]<e1015[-1]<e10015[-1]
    bs=ss=0; br=[]; sr=[]
    if regime=="ALTA": bs+=2;br.append("Tendência ALTA por topos/fundos")
    if regime=="BAIXA": ss+=2;sr.append("Tendência BAIXA por topos/fundos")
    if abc_up: bs+=2;br.append("Pernadas A-B-C alta")
    if abc_dn: ss+=2;sr.append("Pernadas A-B-C baixa")
    if third_sup: bs+=1;br.append("3º toque no suporte")
    if third_res: ss+=1;sr.append("3º toque na resistência")
    if macro_up: bs+=2;br.append("EMA10 > EMA100")
    if macro_dn: ss+=2;sr.append("EMA10 < EMA100")
    if cross_up: bs+=1;br.append("EMA3/13 cruzou para alta")
    if cross_dn: ss+=1;sr.append("EMA3/13 cruzou para baixa")
    if m15_up: bs+=2;br.append("M15 confirma alta")
    if m15_dn: ss+=2;sr.append("M15 confirma baixa")
    if pullback_up: bs+=2;br.append("Pullback após rompimento de alta")
    if pullback_dn: ss+=2;sr.append("Pullback após rompimento de baixa")
    if bull_eng: bs+=2;br.append("Engolfo comprador")
    if bear_eng: ss+=2;sr.append("Engolfo vendedor")
    if reject_up: bs+=1;br.append("Rejeição compradora")
    if reject_dn: ss+=1;sr.append("Rejeição vendedora")
    if bull_seq: bs+=1;br.append("Sequência de velas compradoras")
    if bear_seq: ss+=1;sr.append("Sequência de velas vendedoras")
    pos=(c0.close-support)/span
    lateral_buy=regime=="LATERAL" and pos<=0.25 and (bull_eng or reject_up or bull_seq) and not macro_dn
    lateral_sell=regime=="LATERAL" and pos>=0.75 and (bear_eng or reject_dn or bear_seq) and not macro_up
    if lateral_buy: bs+=3;br.append("Lateral: fundo da faixa + gatilho")
    if lateral_sell: ss+=3;sr.append("Lateral: topo da faixa + gatilho")
    trend_buy=regime=="ALTA" and (abc_up or macro_up) and (pullback_up or bull_eng or reject_up or bull_seq)
    trend_sell=regime=="BAIXA" and (abc_dn or macro_dn) and (pullback_dn or bear_eng or reject_dn or bear_seq)
    # Não compra uma alta já encostada na resistência: exige espaço mínimo até o topo da faixa.
    room_up=((resistance-c0.close)/c0.close*100) if c0.close else 0
    room_down=((c0.close-support)/c0.close*100) if c0.close else 0
    min_room=max(0.20, config.take_profit_operacao*0.50)
    buy_context=(trend_buy or lateral_buy) and (lateral_buy or room_up>=min_room or pullback_up)
    sell_context=(trend_sell or lateral_sell) and (lateral_sell or room_down>=min_room or pullback_dn)
    if not buy_context and bs>=config.min_score: br.append(f"Sem espaço suficiente até resistência ({room_up:.2f}%)")
    if not sell_context and ss>=config.min_score: sr.append(f"Sem espaço suficiente até suporte ({room_down:.2f}%)")
    technical="COMPRA" if bs>=config.min_score and bs>ss and buy_context else "VENDA" if ss>=config.min_score and ss>bs and sell_context else "AGUARDAR"
    move_signed=((c0.close-c1.close)/c1.close*100) if c1.close else 0
    move=abs(move_signed)
    volatile=move>config.max_movimento_pct

    # Volatilidade adaptativa:
    # não bloqueia automaticamente um impulso forte quando TODA a estrutura confirma.
    strong_buy_impulse=(
        volatile and move_signed>0 and regime=="ALTA" and m15_up and
        (abc_up or pullback_up) and (bull_eng or bull_seq or pullback_up) and
        bs>=max(config.min_score,8) and bs>=ss+3 and buy_context
    )
    strong_sell_impulse=(
        volatile and move_signed<0 and regime=="BAIXA" and m15_dn and
        (abc_dn or pullback_dn) and (bear_eng or bear_seq or pullback_dn) and
        ss>=max(config.min_score,8) and ss>=bs+3 and sell_context
    )
    volatility_ok=(not volatile) or strong_buy_impulse or strong_sell_impulse
    if strong_buy_impulse: br.append(f"Impulso forte validado ({move:.2f}%): tendência + M15 + estrutura/gatilho")
    if strong_sell_impulse: sr.append(f"Impulso forte de baixa validado ({move:.2f}%): tendência + M15 + estrutura/gatilho")

    nd=news["direction"]
    if not volatility_ok:
        result="AGUARDAR"
        reasons=[f"Volatilidade {config.timeframe.upper()} {move:.2f}%: movimento esticado/sem confirmação suficiente"]
    elif technical=="COMPRA" and config.news_filter and config.news_confirm_only and nd=="BAIXA": result="AGUARDAR";reasons=br+["Notícia conflita com alta"]
    elif technical=="VENDA" and config.news_filter and config.news_confirm_only and nd=="ALTA": result="AGUARDAR";reasons=sr+["Notícia conflita com baixa"]
    else:
        result=technical; reasons=(br if bs>=ss else sr) or ["Aguardando confluência/gatilho"]
        reasons.insert(0,f"Mercado {regime}")
        if config.news_filter: reasons.append(f"Notícias: {nd} (+{news['positive']}/-{news['negative']})")
    return Analysis(symbol,name,result,max(bs,ss),bs,ss,c0.close,reasons,support,resistance,volatile,nd,news["strength"])

def risk_ok():
    if state["open_trade"]:return False,"Já existe operação aberta"
    if state["balance"] is None:return False,"Banca indisponível"
    start=state["start_balance"] or state["balance"]
    pnl=(state["realized_pnl_brl"]/start*100) if start else 0
    if pnl>=config.stop_gain:return False,"Stop Gain da sessão atingido"
    if pnl<=-config.stop_loss:return False,"Stop Loss da sessão atingido"
    if state["entries"]>=config.max_entradas:return False,"Limite de entradas atingido"
    return True,"OK"

async def scan_once():
    state["status"]="Analisando mercados..."; state["last_error"]=None
    try:
        syms,table=await discover_symbols(); state["symbols"]=syms; out=[]
        for i,s in enumerate(syms,1):
            state["scan_progress"]=f"Analisando {i}/{len(syms)} • {s}"
            primary=await candles(s,config.timeframe,160); await asyncio.sleep(1.05)
            m15=await candles(s,"15m",160); await asyncio.sleep(1.05)
            news=await news_signal(s); state["news"][s]=news
            out.append(analyze(s,table.get(s,{}).get("description",s),primary,m15,news))
        out.sort(key=lambda x:x.score,reverse=True); state["analyses"]=[asdict(x) for x in out]
        state["signals"]=[asdict(x) for x in out if x.result!="AGUARDAR"]; state["last_scan"]=int(time.time())
        state["status"]=f"{len(syms)} ativos analisados"; state["scan_progress"]=""; await refresh_balance(); return state["analyses"]
    except Exception as e:
        state["status"]="Erro na análise"; state["scan_progress"]=""; state["last_error"]=f"{type(e).__name__}: {e}"; raise

async def ticker(symbol):
    d=await mb_get("/tickers",{"symbols":symbol}); return float(d[0]["last"])

async def wait_order(aid,symbol,oid,seconds=12):
    last=None
    for _ in range(seconds):
        last=await mb_get(f"/accounts/{aid}/{symbol}/orders/{oid}",private=True)
        if last.get("status")=="filled":return last
        await asyncio.sleep(1)
    return last or {}

def floor_step(q,step):
    return math.floor(q/step)*step if step else q

async def close_position(reason):
    tr=state.get("open_trade")
    if not tr:return
    aid=await account_id(); symbol=tr["symbol"]; qty=float(tr["qty"]); table=await symbol_table(); meta=table.get(symbol,{})
    qty=floor_step(qty,float(meta.get("round_lot",0) or 0))
    if qty<=0:raise RuntimeError("Quantidade de venda inválida.")
    resp=await mb_post(f"/accounts/{aid}/{symbol}/orders",{"type":"market","side":"sell","qty":f"{qty:.12f}".rstrip("0").rstrip("."),"async":False,"externalId":"ATR-"+uuid.uuid4().hex[:18]})
    od=await wait_order(aid,symbol,resp["orderId"]); exit_price=float(od.get("avgPrice") or await ticker(symbol))
    buy_fee=float(tr.get("buy_fee") or 0); sell_fee=float(od.get("fee") or 0)
    gross=(exit_price-tr["entry_price"])*qty
    net=gross-buy_fee-sell_fee
    item={**tr,"exit_time":int(time.time()),"exit_price":exit_price,"result":"FECHADA","close_reason":reason,
          "gross_pnl_brl":round(gross,6),"buy_fee":buy_fee,"sell_fee":sell_fee,"pnl_brl":round(net,6),"sell_order_id":resp["orderId"]}
    state["realized_pnl_brl"]+=net; state["history"].insert(0,item); state["history"]=state["history"][:50]
    state["open_trade"]=None; state["status"]=f"Operação fechada • {reason}"; await refresh_balance()

async def monitor_trade():
    last_news_check=0
    adverse_hits=0
    while state.get("open_trade"):
        tr=state["open_trade"]
        try:
            px=await ticker(tr["symbol"]); tr["current_price"]=px
            pct=(px-tr["entry_price"])/tr["entry_price"]*100 if tr["entry_price"] else 0
            tr["current_pnl_percent"]=pct
            reason=None

            # Proteções duras sempre têm prioridade.
            if config.take_profit_operacao>0 and pct>=config.take_profit_operacao:
                reason="TAKE PROFIT"
            elif config.stop_loss_operacao>0 and pct<=-config.stop_loss_operacao:
                reason="STOP LOSS"
            elif time.time()>=tr["expires_at"]:
                reason="TEMPO MÁXIMO"

            # Proteção por notícia: não sai apenas por manchete/tom.
            # Exige notícia contrária + força mínima + confirmação do preço em queda.
            if not reason and config.news_filter and config.news_exit_enabled and time.time()-last_news_check>=config.news_check_seconds:
                ns=await news_signal(tr["symbol"])
                state["news"][tr["symbol"]]=ns
                tr["news_direction"]=ns["direction"]; tr["news_strength"]=ns["strength"]
                last_news_check=time.time()
                adverse=(tr["direction"]=="COMPRA" and ns["direction"]=="BAIXA" and ns["strength"]>=config.news_exit_min_strength)
                price_confirms=(pct<=-abs(config.news_exit_confirm_pct))
                if adverse and price_confirms:
                    adverse_hits+=1
                else:
                    adverse_hits=0
                tr["news_adverse_hits"]=adverse_hits
                if adverse_hits>=1:
                    reason="REVERSÃO: NOTÍCIA + PREÇO"

            if reason:
                async with _trade_lock:
                    if state.get("open_trade"): await close_position(reason)
                return
        except Exception as e:
            state["last_error"]=f"Monitoramento: {type(e).__name__}: {e}"
        await asyncio.sleep(2)

async def real_buy():
    async with _trade_lock:
        ok,msg=risk_ok()
        if not ok:return {"ok":False,"message":msg}
        best=next((x for x in state["signals"] if x["result"]=="COMPRA"),None)
        if not best:return {"ok":False,"message":"Nenhum sinal de COMPRA com score mínimo."}
        await refresh_balance(); _,table=await discover_symbols(); meta=table.get(best["symbol"],{})
        stake=round((state["available_brl"] or 0)*config.percentual_entrada/100,2); min_cost=float(meta.get("min_cost",0) or 0)
        if stake<min_cost:return {"ok":False,"message":f"Entrada calculada R$ {stake:.2f} abaixo do mínimo R$ {min_cost:.2f} de {best['symbol']}."}
        aid=await account_id()
        resp=await mb_post(f"/accounts/{aid}/{best['symbol']}/orders",{"type":"market","side":"buy","cost":stake,"async":False,"externalId":"ATR-"+uuid.uuid4().hex[:18]})
        od=await wait_order(aid,best["symbol"],resp["orderId"]); qty=float(od.get("filledQty") or 0); price=float(od.get("avgPrice") or best["price"])
        if qty<=0:return {"ok":False,"message":"A compra foi enviada, mas ainda não há quantidade executada. Verifique a ordem no MB."}
        tr={"entry_time":int(time.time()),"expires_at":int(time.time())+config.duracao_segundos,"symbol":best["symbol"],"direction":"COMPRA",
            "score":best["score"],"stake":stake,"entry_price":price,"current_price":price,"current_pnl_percent":0.0,"qty":qty,
            "buy_fee":float(od.get("fee") or 0),"buy_order_id":resp["orderId"],"result":"ABERTA"}
        state["open_trade"]=tr; state["entries"]+=1; state["status"]="Operação REAL aberta • TP/SL/tempo monitorando"
        asyncio.create_task(monitor_trade()); return {"ok":True,"entry":tr}

async def scanner_loop():
    while state["running"]:
        try:
            await scan_once()
            if config.auto_trade and not state["open_trade"]:await real_buy()
        except Exception:pass
        await asyncio.sleep(12)

@app.on_event("startup")
async def startup():await refresh_balance()

@app.get("/")
async def home():return FileResponse("static/index.html")

@app.get("/api/status")
async def api_status():
    await refresh_balance()
    start=state["start_balance"]; pnl=(state["realized_pnl_brl"]/start*100) if start else 0
    countdown=max(0,int(state["open_trade"]["expires_at"]-time.time())) if state["open_trade"] else 0
    return {**state,"config":config.model_dump(),"pnl_percent":round(pnl,4),"countdown":countdown}

@app.post("/api/config")
async def set_config(new:Config):
    global config
    new.min_score=max(1,min(new.min_score,12)); new.max_ativos=max(1,min(new.max_ativos,20))
    new.timeframe="5m" if new.timeframe=="5m" else "1m"
    new.duracao_segundos=max(60,min(int(new.duracao_segundos),1800))
    new.take_profit_operacao=max(0,min(new.take_profit_operacao,100)); new.stop_loss_operacao=max(0,min(new.stop_loss_operacao,100))
    new.news_exit_min_strength=max(1,min(int(new.news_exit_min_strength),20))
    new.news_exit_confirm_pct=max(0.01,min(float(new.news_exit_confirm_pct),10))
    new.news_check_seconds=max(30,min(int(new.news_check_seconds),600))
    config=new; return {"ok":True,"config":config.model_dump()}

@app.post("/api/scan")
async def api_scan():
    try:return {"ok":True,"analyses":await scan_once()}
    except Exception as e:raise HTTPException(500,str(e))
@app.post("/api/start")
async def start():
    global _loop_task
    state["running"]=True; state["status"]="Ativo"
    if not _loop_task or _loop_task.done():_loop_task=asyncio.create_task(scanner_loop())
    return {"ok":True}
@app.post("/api/stop")
async def stop():
    state["running"]=False; state["status"]="Parado"
    return {"ok":True,"message":"Scanner parado. Operação aberta continua protegida por TP/SL/tempo."}
@app.post("/api/real-entry")
async def entry():return await real_buy()

app.mount("/static",StaticFiles(directory="static"),name="static")
