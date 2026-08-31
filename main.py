import asyncio, json, time
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Optional, Tuple
import websockets
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="Operação Alvo Certo (ATR)")
PUBLIC_WS = "wss://api.derivws.com/trading/v1/options/ws/public"

class Config(BaseModel):
    banca_inicial: float = 1000.0
    percentual_entrada: float = 1.0
    stop_gain: float = 5.0
    stop_loss: float = 5.0
    max_entradas: int = 5
    min_score: int = 7
    duracao_minutos: int = 1
    payout_demo: float = 0.80

config = Config()
state = {
    "running": False, "balance": 1000.0, "start_balance": 1000.0,
    "entries": 0, "signals": [], "history": [], "last_scan": None,
    "status": "Parado", "open_trade": None, "diagnostics": {}
}
robot_task: Optional[asyncio.Task] = None
scan_lock = asyncio.Lock()

@dataclass
class Candle:
    epoch: int
    open: float
    high: float
    low: float
    close: float

@dataclass
class Signal:
    symbol: str
    name: str
    direction: str
    score: int
    price: float
    qualified: bool
    reasons: List[str] = field(default_factory=list)
    support: float = 0.0
    resistance: float = 0.0

def ema(values, period):
    if not values: return []
    a = 2.0/(period+1.0)
    out=[values[0]]
    for v in values[1:]:
        out.append(a*v + (1-a)*out[-1])
    return out

async def ws_request(payload: Dict[str, Any], req_id=1):
    async with websockets.connect(PUBLIC_WS, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
        p=dict(payload); p["req_id"]=req_id
        await ws.send(json.dumps(p))
        while True:
            data=json.loads(await ws.recv())
            if data.get("req_id") != req_id: continue
            if "error" in data:
                raise RuntimeError(data["error"].get("message","Erro Deriv"))
            return data

async def active_symbols():
    data=await ws_request({"active_symbols":"brief","contract_type":["CALL","PUT"]},100)
    return data.get("active_symbols",[])

async def candles(symbol, granularity, count):
    data=await ws_request({
        "ticks_history":symbol,"adjust_start_time":1,"count":count,"end":"latest",
        "granularity":granularity,"style":"candles","subscribe":0
    },1000+granularity)
    out=[]
    for c in data.get("candles",[]):
        try:
            out.append(Candle(int(c["epoch"]),float(c["open"]),float(c["high"]),float(c["low"]),float(c["close"])))
        except Exception:
            pass
    return out

async def latest_price(symbol):
    data=await candles(symbol,60,2)
    return data[-1].close if data else None

def analyze(symbol,name,m1,m15)->Tuple[Optional[Signal],str,int,int]:
    if len(m1)<110: return None,f"M1 insuficiente ({len(m1)})",0,0
    if len(m15)<110: return None,f"M15 insuficiente ({len(m15)})",0,0

    closes=[x.close for x in m1]; highs=[x.high for x in m1]; lows=[x.low for x in m1]
    hlc3=[(x.high+x.low+x.close)/3 for x in m1]
    e10=ema(closes,10); e100=ema(closes,100); e3=ema(hlc3,3); e13=ema(hlc3,13)
    c0,c1,c2,c3=m1[-1],m1[-2],m1[-3],m1[-4]
    resistance=max(highs[-11:-1]); support=min(lows[-11:-1])

    ta=c0.close>c1.close and c0.close>e10[-1] and e10[-1]>e10[-2]
    tb=c0.close<c1.close and c0.close<e10[-1] and e10[-1]<e10[-2]
    cross_up=e3[-2]<e13[-2] and e3[-1]>e13[-1]
    cross_dn=e3[-2]>e13[-2] and e3[-1]<e13[-1]
    bull_eng=c1.close<c1.open and c0.close>c0.open and c0.open<=c1.close and c0.close>=c1.open
    bear_eng=c1.close>c1.open and c0.close<c0.open and c0.open>=c1.close and c0.close<=c1.open
    bull_seq=c0.close>c1.close>c2.close>=c3.close
    bear_seq=c0.close<c1.close<c2.close<=c3.close
    macro_up=c0.close>e100[-1] and e10[-1]>e100[-1]
    macro_dn=c0.close<e100[-1] and e10[-1]<e100[-1]

    closes15=[x.close for x in m15]; e10_15=ema(closes15,10); e100_15=ema(closes15,100)
    m15_up=closes15[-1]>e10_15[-1]>e100_15[-1] and e10_15[-1]>e10_15[-2]
    m15_dn=closes15[-1]<e10_15[-1]<e100_15[-1] and e10_15[-1]<e10_15[-2]

    cs=ps=0; cr=[]; pr=[]
    if ta: cs+=1; cr.append("Tendência M1 alta")
    if tb: ps+=1; pr.append("Tendência M1 baixa")
    if macro_up: cs+=2; cr.append("EMA10 > EMA100")
    if macro_dn: ps+=2; pr.append("EMA10 < EMA100")
    if cross_up: cs+=1; cr.append("Cruzamento EMA3/13")
    if cross_dn: ps+=1; pr.append("Cruzamento EMA3/13")
    if bull_eng: cs+=2; cr.append("Engolfo alta")
    if bear_eng: ps+=2; pr.append("Engolfo baixa")
    if bull_seq: cs+=1; cr.append("Sequência alta")
    if bear_seq: ps+=1; pr.append("Sequência baixa")
    if m15_up: cs+=2; cr.append("M15 confirma alta")
    if m15_dn: ps+=2; pr.append("M15 confirma baixa")
    span=max(resistance-support,1e-12); pos=(c0.close-support)/span
    if pos<=0.35: cs+=1; cr.append("Próximo ao suporte")
    if pos>=0.65: ps+=1; pr.append("Próximo à resistência")

    if cs==0 and ps==0: return None,"score zero",cs,ps
    if cs==ps: return None,f"empate CALL={cs} PUT={ps}",cs,ps
    if cs>ps:
        direction,score,reasons="CALL",cs,cr
    else:
        direction,score,reasons="PUT",ps,pr
    return Signal(symbol,name,direction,score,c0.close,score>=config.min_score,reasons,support,resistance),"ok",cs,ps

async def analyze_one(item,sem):
    symbol=item.get("symbol")
    if not symbol:
        return {"signal":None,"status":"sem símbolo","symbol":"?","m1":0,"m15":0,"call":0,"put":0}
    name=item.get("display_name") or item.get("market_display_name") or symbol
    async with sem:
        try:
            m1,m15=await asyncio.gather(candles(symbol,60,140),candles(symbol,900,120))
            signal,status,cs,ps=analyze(symbol,name,m1,m15)
            return {"signal":signal,"status":status,"symbol":symbol,"name":name,"m1":len(m1),"m15":len(m15),"call":cs,"put":ps}
        except Exception as exc:
            return {"signal":None,"status":f"erro: {exc}","symbol":symbol,"name":name,"m1":0,"m15":0,"call":0,"put":0}

async def scan_once():
    async with scan_lock:
        state["status"]="Analisando mercados..."
        syms=await active_symbols()
        sem=asyncio.Semaphore(8)
        results=await asyncio.gather(*[analyze_one(x,sem) for x in syms])
        signals=[x["signal"] for x in results if x["signal"] is not None]
        signals.sort(key=lambda x:x.score, reverse=True)
        state["signals"]=[asdict(x) for x in signals[:25]]
        state["last_scan"]=int(time.time())
        state["status"]=f"{len(syms)} ativos analisados"

        api_errors=[x for x in results if x["status"].startswith("erro:")]
        m1_short=[x for x in results if x["status"].startswith("M1 insuficiente")]
        m15_short=[x for x in results if x["status"].startswith("M15 insuficiente")]
        zero_score=[x for x in results if x["status"]=="score zero"]
        ties=[x for x in results if x["status"].startswith("empate")]
        valid=[x for x in results if x["m1"]>=110 and x["m15"]>=110]
        qualified=[s for s in signals if s.qualified]
        max_score=signals[0].score if signals else 0
        state["diagnostics"]={
            "symbols_total":len(syms),"valid_data":len(valid),"api_errors":len(api_errors),
            "m1_insufficient":len(m1_short),"m15_insufficient":len(m15_short),
            "zero_score":len(zero_score),"ties":len(ties),"candidates":len(signals),
            "qualified":len(qualified),"max_score":max_score,
            "best_name":signals[0].name if signals else "-",
            "best_direction":signals[0].direction if signals else "-"
        }
        print(
            "[ATR] DIAGNÓSTICO | "
            f"ativos={len(syms)} | dados_validos={len(valid)} | erros_api={len(api_errors)} | "
            f"m1_curto={len(m1_short)} | m15_curto={len(m15_short)} | score_zero={len(zero_score)} | "
            f"empates={len(ties)} | candidatos={len(signals)} | qualificados={len(qualified)} | maior_score={max_score}"
        )
        if signals:
            b=signals[0]
            print(f"[ATR] MELHOR | {b.name} | {b.direction} | score={b.score} | qualificado={b.qualified}")
        for item in (api_errors+m1_short+m15_short)[:5]:
            print(f"[ATR] AMOSTRA_FALHA | {item['symbol']} | {item['status']} | M1={item['m1']} | M15={item['m15']}")
        return state["signals"]

def risk_ok():
    start=state["start_balance"]
    pnl=((state["balance"]-start)/start)*100 if start else 0
    if pnl>=config.stop_gain: state["status"]="Stop Gain atingido"; return False
    if pnl<=-config.stop_loss: state["status"]="Stop Loss atingido"; return False
    if state["entries"]>=config.max_entradas: state["status"]="Limite de entradas atingido"; return False
    return True

def best_qualified_signal():
    return next((s for s in state["signals"] if s.get("qualified")),None)

def open_paper_trade(signal):
    if state["open_trade"] is not None or not risk_ok(): return None
    stake=round(state["balance"]*config.percentual_entrada/100,2)
    if stake<=0: return None
    now=int(time.time())
    trade={
        "time":now,"symbol":signal["symbol"],"name":signal["name"],"direction":signal["direction"],
        "score":signal["score"],"stake":stake,"entry_price":float(signal["price"]),
        "expires_at":now+int(config.duracao_minutos*60),"result":"ABERTA (PAPER)","qualified":True
    }
    state["open_trade"]=trade; state["entries"]+=1
    state["history"].insert(0,dict(trade)); state["history"]=state["history"][:50]
    state["status"]=f"Entrada DEMO: {signal['direction']} {signal['name']}"
    print(f"[ATR] ENTRADA DEMO | {signal['name']} | {signal['direction']} | score={signal['score']} | stake={stake}")
    return trade

async def settle_open_trade():
    trade=state.get("open_trade")
    if not trade: return
    now=int(time.time())
    if now<trade["expires_at"]:
        state["status"]=f"Operação DEMO aberta • {trade['expires_at']-now}s restantes"; return
    exit_price=await latest_price(trade["symbol"])
    if exit_price is None:
        state["status"]="Aguardando preço para encerrar DEMO"; return
    entry=float(trade["entry_price"]); stake=float(trade["stake"]); direction=trade["direction"]
    if exit_price==entry: result,pnl="EMPATE",0.0
    elif direction=="CALL":
        result,pnl=("WIN",stake*config.payout_demo) if exit_price>entry else ("LOSS",-stake)
    else:
        result,pnl=("WIN",stake*config.payout_demo) if exit_price<entry else ("LOSS",-stake)
    state["balance"]=round(state["balance"]+pnl,2)
    closed=dict(trade); closed.update({"exit_price":exit_price,"pnl":round(pnl,2),"result":result,"closed_at":int(time.time())})
    if state["history"]: state["history"][0]=closed
    else: state["history"].insert(0,closed)
    state["open_trade"]=None
    state["status"]=f"{result} • {trade['name']} • P/L {pnl:+.2f}"
    print(f"[ATR] RESULTADO | {trade['name']} | {result} | entrada={entry} | saida={exit_price} | PL={pnl:+.2f} | banca={state['balance']:.2f}")

async def robot_loop():
    print("[ATR] Robô automático DEMO iniciado.")
    try:
        while state["running"]:
            if not risk_ok():
                state["running"]=False; break
            if state["open_trade"] is not None:
                await settle_open_trade(); await asyncio.sleep(5); continue
            try:
                await scan_once()
                best=best_qualified_signal()
                if best: open_paper_trade(best)
                else:
                    ms=state["diagnostics"].get("max_score",0)
                    state["status"]=f"{state['status']} • melhor score {ms}/{config.min_score}"
            except Exception as exc:
                state["status"]=f"Erro no scanner: {exc}"
                print(f"[ATR] ERRO_SCANNER | {exc}")
            if state["running"] and state["open_trade"] is None:
                await asyncio.sleep(max(60-(time.time()%60),1)+1)
            else:
                await asyncio.sleep(5)
    except asyncio.CancelledError:
        raise
    finally:
        if not state["running"]:
            print("[ATR] Robô automático DEMO parado.")

@app.get("/")
async def home(): return FileResponse("static/index.html")

@app.get("/api/status")
async def get_status():
    start=state["start_balance"]
    pnl=((state["balance"]-start)/start)*100 if start else 0
    return {**state,"config":config.model_dump(),"pnl_percent":round(pnl,2)}

@app.post("/api/config")
async def set_config(new:Config):
    global config
    config=new
    if not state["running"]:
        state["balance"]=new.banca_inicial; state["start_balance"]=new.banca_inicial
        state["entries"]=0; state["history"]=[]; state["open_trade"]=None
    return {"ok":True,"config":config.model_dump()}

@app.post("/api/scan")
async def api_scan():
    signals=await scan_once()
    return {"signals":signals,"count":len(signals),"diagnostics":state["diagnostics"]}

@app.post("/api/start")
async def start():
    global robot_task
    if state["running"]: return {"ok":True,"message":"ATR já está ativo."}
    state["running"]=True; state["status"]="ATR automático iniciado"
    if robot_task is None or robot_task.done():
        robot_task=asyncio.create_task(robot_loop())
    return {"ok":True,"message":"ATR automático DEMO iniciado."}

@app.post("/api/stop")
async def stop():
    state["running"]=False; state["status"]="Parado"
    return {"ok":True,"message":"ATR parado."}

@app.post("/api/paper-entry")
async def paper_entry():
    if state["open_trade"] is not None:
        return {"ok":False,"message":"Já existe uma operação DEMO aberta."}
    if not state["signals"]: await scan_once()
    if not risk_ok(): return {"ok":False,"message":state["status"]}
    best=best_qualified_signal()
    if best is None:
        return {"ok":False,"message":f"Nenhum sinal atingiu o score mínimo {config.min_score}."}
    entry=open_paper_trade(best)
    return {"ok":bool(entry),"entry":entry} if entry else {"ok":False,"message":"Não foi possível abrir a entrada DEMO."}

app.mount("/static", StaticFiles(directory="static"), name="static")
