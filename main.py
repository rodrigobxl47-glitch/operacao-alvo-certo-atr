import asyncio
import os
import time
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="Operação Alvo Certo (ATR) - Mercado Bitcoin v5")

MB_BASE = "https://api.mercadobitcoin.net/api/v4"
MB_API_ID = os.getenv("MB_API_ID", "").strip()
MB_API_SECRET = os.getenv("MB_API_SECRET", "").strip()
MB_ACCOUNT_ID = os.getenv("MB_ACCOUNT_ID", "").strip()

# Ativos iniciais. O scanner também tenta descobrir outros pares BRL em /symbols.
PREFERRED = ["BTC-BRL","ETH-BRL","SOL-BRL","XRP-BRL","ADA-BRL","DOGE-BRL","LINK-BRL","LTC-BRL"]

class Config(BaseModel):
    percentual_entrada: float = 1.0
    stop_gain: float = 3.0
    stop_loss: float = 2.0
    max_entradas: int = 5
    min_score: int = 7
    max_ativos: int = 8
    auto_trade: bool = False

config = Config()
state = {
    "running": False, "balance": None, "available_brl": None, "start_balance": None,
    "entries": 0, "signals": [], "history": [], "last_scan": None,
    "status": "Parado", "mb_connected": False, "symbols": []
}
_token = {"value": None, "expires": 0}
_loop_task = None

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
    reasons: List[str] = field(default_factory=list)
    support: float = 0.0
    resistance: float = 0.0

def ema(values: List[float], period: int) -> List[float]:
    if not values: return []
    a = 2.0 / (period + 1.0)
    out = [values[0]]
    for v in values[1:]:
        out.append(a*v + (1-a)*out[-1])
    return out

async def token():
    if _token["value"] and time.time() < _token["expires"] - 30:
        return _token["value"]
    if not MB_API_ID or not MB_API_SECRET:
        raise RuntimeError("MB_API_ID/MB_API_SECRET não configurados.")
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(f"{MB_BASE}/oauth2/token", data={
            "grant_type":"client_credentials","scope":"global",
            "client_id":MB_API_ID,"client_secret":MB_API_SECRET
        })
        r.raise_for_status()
        d = r.json()
        _token["value"] = d["access_token"]
        _token["expires"] = time.time() + int(d.get("expires_in", 300))
        return _token["value"]

async def mb_get(path, params=None, private=False):
    headers = {}
    if private: headers["Authorization"] = f"Bearer {await token()}"
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{MB_BASE}{path}", params=params, headers=headers)
        r.raise_for_status()
        return r.json()

async def account_info():
    if MB_ACCOUNT_ID:
        return {"id": MB_ACCOUNT_ID}
    data = await mb_get("/accounts", private=True)
    accounts = data.get("accounts") or data.get("data") or data.get("items") or [] if isinstance(data, dict) else data
    valid = [a for a in accounts if isinstance(a, dict) and a.get("id")]
    if not valid: raise RuntimeError("Nenhuma conta válida retornada.")
    return next((a for a in valid if str(a.get("type","")).lower() in ("live","real")), valid[0])

async def refresh_balance():
    try:
        aid = (await account_info())["id"]
        data = await mb_get(f"/accounts/{aid}/balances", private=True)
        rows = data.get("balances") or data.get("data") or data.get("items") or [] if isinstance(data, dict) else data
        total = avail = 0.0
        for x in rows:
            if str(x.get("symbol","")).upper() == "BRL":
                total = float(x.get("total",0) or 0)
                avail = float(x.get("available",total) or 0)
                break
        state["balance"] = total
        state["available_brl"] = avail
        if state["start_balance"] is None: state["start_balance"] = total
        state["mb_connected"] = True
    except Exception as e:
        state["mb_connected"] = False
        state["status"] = f"Erro saldo: {type(e).__name__}"

def normalize_symbol_row(x):
    if isinstance(x, str): return x
    for k in ("symbol","pair","id"):
        if isinstance(x, dict) and x.get(k): return str(x[k])
    return ""

async def discover_symbols():
    try:
        data = await mb_get("/symbols")
        rows = data.get("symbols") or data.get("data") or data.get("items") or [] if isinstance(data, dict) else data
        found = []
        for x in rows:
            s = normalize_symbol_row(x).upper()
            if s.endswith("-BRL") and s not in found:
                found.append(s)
        ordered = [s for s in PREFERRED if s in found] + [s for s in found if s not in PREFERRED]
        return ordered[:max(1, config.max_ativos)]
    except Exception:
        return PREFERRED[:max(1, config.max_ativos)]

def parse_candles(data):
    rows = data.get("candles") or data.get("data") or data if isinstance(data, dict) else data
    out = []
    if not isinstance(rows, list): return out
    for c in rows:
        try:
            if isinstance(c, dict):
                ep = c.get("timestamp", c.get("time", c.get("epoch",0)))
                out.append(Candle(int(ep), float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])))
            elif isinstance(c, (list,tuple)) and len(c) >= 5:
                out.append(Candle(int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4])))
        except Exception: pass
    out.sort(key=lambda z:z.epoch)
    return out

async def candles(symbol, resolution, count):
    # MB v4 candles. Mantemos tentativas compatíveis sem alterar a estratégia.
    attempts = [
        {"symbol":symbol,"resolution":resolution,"count":count},
        {"symbol":symbol,"interval":resolution,"limit":count},
    ]
    last = None
    for params in attempts:
        try:
            d = await mb_get("/candles", params=params)
            c = parse_candles(d)
            if c: return c[-count:]
        except Exception as e: last = e
    if last: raise last
    return []

def analyze(symbol: str, name: str, m1: List[Candle], m15: List[Candle]) -> Optional[Signal]:
    EMA_MICRO, EMA_MACRO, EMA_FAST, EMA_SLOW, RANGE = 10,100,3,13,10
    if len(m1)<110 or len(m15)<110: return None
    closes=[x.close for x in m1]; highs=[x.high for x in m1]; lows=[x.low for x in m1]
    hlc3=[(x.high+x.low+x.close)/3 for x in m1]
    e10=ema(closes,10); e100=ema(closes,100); e3=ema(hlc3,3); e13=ema(hlc3,13)
    c0,c1,c2,c3=m1[-1],m1[-2],m1[-3],m1[-4]
    resistance=max(highs[-RANGE-1:-1]); support=min(lows[-RANGE-1:-1])
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
    if pos<=.35: cs+=1; cr.append("Próximo ao suporte")
    if pos>=.65: ps+=1; pr.append("Próximo à resistência")
    if cs>=config.min_score and cs>ps:
        return Signal(symbol,name,"COMPRA",cs,c0.close,cr,support,resistance)
    if ps>=config.min_score and ps>cs:
        return Signal(symbol,name,"SAÍDA",ps,c0.close,pr,support,resistance)
    return None

async def analyze_one(symbol):
    try:
        # Respeita a API: chamadas sequenciais; não dispara dezenas em paralelo.
        m1 = await candles(symbol, "1m", 140)
        await asyncio.sleep(1.05)
        m15 = await candles(symbol, "15m", 120)
        await asyncio.sleep(1.05)
        return analyze(symbol, symbol, m1, m15)
    except Exception:
        return None

async def scan_once():
    state["status"]="Analisando mercados..."
    syms=await discover_symbols()
    state["symbols"]=syms
    signals=[]
    for s in syms:
        x=await analyze_one(s)
        if x: signals.append(x)
    signals.sort(key=lambda x:x.score, reverse=True)
    state["signals"]=[asdict(x) for x in signals]
    state["last_scan"]=int(time.time())
    state["status"]=f"{len(syms)} ativos analisados"
    await refresh_balance()
    return state["signals"]

async def scanner_loop():
    while state["running"]:
        try: await scan_once()
        except Exception as e: state["status"]=f"Erro scanner: {type(e).__name__}"
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup():
    await refresh_balance()

@app.get("/")
async def home(): return FileResponse("static/index.html")

@app.get("/api/status")
async def status():
    await refresh_balance()
    start=state["start_balance"]
    pnl=((state["balance"]-start)/start*100) if start and state["balance"] is not None else 0
    return {**state,"config":config.model_dump(),"pnl_percent":round(pnl,2)}

@app.post("/api/config")
async def set_config(new:Config):
    global config
    config=new
    return {"ok":True,"config":config.model_dump()}

@app.post("/api/scan")
async def api_scan():
    s=await scan_once()
    return {"signals":s,"count":len(s)}

@app.post("/api/start")
async def start():
    global _loop_task
    state["running"]=True; state["status"]="Ativo"
    if not _loop_task or _loop_task.done(): _loop_task=asyncio.create_task(scanner_loop())
    return {"ok":True}

@app.post("/api/stop")
async def stop():
    state["running"]=False; state["status"]="Parado"
    return {"ok":True}

# Segurança da v5: análise real, mas SEM envio de ordens.
# A compra automática só deve ser adicionada depois de validar candles/sinais no MB.
@app.post("/api/real-entry")
async def real_entry():
    return {"ok":False,"message":"Compra real bloqueada na v5 de validação. Primeiro valide os sinais."}

app.mount("/static", StaticFiles(directory="static"), name="static")
