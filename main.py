import asyncio
import json
import math
import time
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Optional

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
    "running": False,
    "balance": 1000.0,
    "start_balance": 1000.0,
    "entries": 0,
    "signals": [],
    "history": [],
    "last_scan": None,
    "status": "Parado",
}

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
    if not values:
        return []
    a = 2.0 / (period + 1.0)
    out = [values[0]]
    for v in values[1:]:
        out.append(a * v + (1 - a) * out[-1])
    return out

async def ws_request(payload: Dict[str, Any], req_id: int = 1) -> Dict[str, Any]:
    async with websockets.connect(PUBLIC_WS, ping_interval=20, ping_timeout=20) as ws:
        p = dict(payload)
        p["req_id"] = req_id
        await ws.send(json.dumps(p))
        while True:
            data = json.loads(await ws.recv())
            if data.get("req_id") == req_id:
                if "error" in data:
                    raise RuntimeError(data["error"].get("message", "Erro Deriv"))
                return data

async def active_symbols():
    data = await ws_request({
        "active_symbols": "brief",
        "contract_type": ["CALL", "PUT"]
    }, 100)
    return data.get("active_symbols", [])

async def candles(symbol: str, granularity: int, count: int):
    data = await ws_request({
        "ticks_history": symbol,
        "adjust_start_time": 1,
        "count": count,
        "end": "latest",
        "granularity": granularity,
        "style": "candles",
        "subscribe": 0
    }, 1000 + granularity)
    out = []
    for c in data.get("candles", []):
        try:
            out.append(Candle(
                int(c["epoch"]), float(c["open"]), float(c["high"]),
                float(c["low"]), float(c["close"])
            ))
        except Exception:
            pass
    return out

def analyze(symbol: str, name: str, m1: List[Candle], m15: List[Candle]) -> Optional[Signal]:
    EMA_MICRO, EMA_MACRO, EMA_FAST, EMA_SLOW, RANGE = 10, 100, 3, 13, 10
    if len(m1) < 110 or len(m15) < 110:
        return None

    closes = [x.close for x in m1]
    highs = [x.high for x in m1]
    lows = [x.low for x in m1]
    hlc3 = [(x.high + x.low + x.close) / 3 for x in m1]

    e10 = ema(closes, EMA_MICRO)
    e100 = ema(closes, EMA_MACRO)
    e3 = ema(hlc3, EMA_FAST)
    e13 = ema(hlc3, EMA_SLOW)

    c0, c1, c2, c3 = m1[-1], m1[-2], m1[-3], m1[-4]
    resistance = max(highs[-RANGE-1:-1])
    support = min(lows[-RANGE-1:-1])

    ta = c0.close > c1.close and c0.close > e10[-1] and e10[-1] > e10[-2]
    tb = c0.close < c1.close and c0.close < e10[-1] and e10[-1] < e10[-2]
    cross_up = e3[-2] < e13[-2] and e3[-1] > e13[-1]
    cross_dn = e3[-2] > e13[-2] and e3[-1] < e13[-1]

    bull_eng = c1.close < c1.open and c0.close > c0.open and c0.open <= c1.close and c0.close >= c1.open
    bear_eng = c1.close > c1.open and c0.close < c0.open and c0.open >= c1.close and c0.close <= c1.open

    bull_seq = c0.close > c1.close > c2.close >= c3.close
    bear_seq = c0.close < c1.close < c2.close <= c3.close

    macro_up = c0.close > e100[-1] and e10[-1] > e100[-1]
    macro_dn = c0.close < e100[-1] and e10[-1] < e100[-1]

    closes15 = [x.close for x in m15]
    e10_15 = ema(closes15, 10)
    e100_15 = ema(closes15, 100)
    m15_up = closes15[-1] > e10_15[-1] > e100_15[-1] and e10_15[-1] > e10_15[-2]
    m15_dn = closes15[-1] < e10_15[-1] < e100_15[-1] and e10_15[-1] < e10_15[-2]

    cs, ps = 0, 0
    cr, pr = [], []

    if ta: cs += 1; cr.append("Tendência M1 alta")
    if tb: ps += 1; pr.append("Tendência M1 baixa")
    if macro_up: cs += 2; cr.append("EMA10 > EMA100")
    if macro_dn: ps += 2; pr.append("EMA10 < EMA100")
    if cross_up: cs += 1; cr.append("Cruzamento EMA3/13")
    if cross_dn: ps += 1; pr.append("Cruzamento EMA3/13")
    if bull_eng: cs += 2; cr.append("Engolfo alta")
    if bear_eng: ps += 2; pr.append("Engolfo baixa")
    if bull_seq: cs += 1; cr.append("Sequência alta")
    if bear_seq: ps += 1; pr.append("Sequência baixa")
    if m15_up: cs += 2; cr.append("M15 confirma alta")
    if m15_dn: ps += 2; pr.append("M15 confirma baixa")

    span = max(resistance - support, 1e-12)
    pos = (c0.close - support) / span
    if pos <= 0.35: cs += 1; cr.append("Próximo ao suporte")
    if pos >= 0.65: ps += 1; pr.append("Próximo à resistência")

    if cs >= config.min_score and cs > ps:
        return Signal(symbol, name, "CALL", cs, c0.close, cr, support, resistance)
    if ps >= config.min_score and ps > cs:
        return Signal(symbol, name, "PUT", ps, c0.close, pr, support, resistance)
    return None

async def analyze_one(item, sem):
    symbol = item.get("symbol")
    if not symbol:
        return None
    name = item.get("display_name") or item.get("market_display_name") or symbol
    async with sem:
        try:
            m1, m15 = await asyncio.gather(
                candles(symbol, 60, 140),
                candles(symbol, 900, 120),
            )
            return analyze(symbol, name, m1, m15)
        except Exception:
            return None

async def scan_once():
    state["status"] = "Analisando mercados..."
    syms = await active_symbols()
    sem = asyncio.Semaphore(8)
    results = await asyncio.gather(*[analyze_one(x, sem) for x in syms])
    signals = [x for x in results if x]
    signals.sort(key=lambda x: x.score, reverse=True)
    state["signals"] = [asdict(x) for x in signals[:25]]
    state["last_scan"] = int(time.time())
    state["status"] = f"{len(syms)} ativos analisados"
    return state["signals"]

def risk_ok():
    start = state["start_balance"]
    pnl = ((state["balance"] - start) / start) * 100 if start else 0
    if pnl >= config.stop_gain:
        state["status"] = "Stop Gain atingido"
        return False
    if pnl <= -config.stop_loss:
        state["status"] = "Stop Loss atingido"
        return False
    if state["entries"] >= config.max_entradas:
        state["status"] = "Limite de entradas atingido"
        return False
    return True

@app.get("/")
async def home():
    return FileResponse("static/index.html")

@app.get("/api/status")
async def get_status():
    start = state["start_balance"]
    pnl = ((state["balance"] - start) / start) * 100 if start else 0
    return {**state, "config": config.model_dump(), "pnl_percent": round(pnl, 2)}

@app.post("/api/config")
async def set_config(new: Config):
    global config
    config = new
    if not state["running"]:
        state["balance"] = new.banca_inicial
        state["start_balance"] = new.banca_inicial
    return {"ok": True, "config": config.model_dump()}

@app.post("/api/scan")
async def api_scan():
    signals = await scan_once()
    return {"signals": signals, "count": len(signals)}

@app.post("/api/start")
async def start():
    state["running"] = True
    state["status"] = "Ativo"
    return {"ok": True}

@app.post("/api/stop")
async def stop():
    state["running"] = False
    state["status"] = "Parado"
    return {"ok": True}

@app.post("/api/paper-entry")
async def paper_entry():
    if not state["signals"]:
        return {"ok": False, "message": "Nenhum sinal disponível."}
    if not risk_ok():
        return {"ok": False, "message": state["status"]}

    best = state["signals"][0]
    stake = round(state["balance"] * config.percentual_entrada / 100, 2)
    entry = {
        "time": int(time.time()),
        "symbol": best["symbol"],
        "name": best["name"],
        "direction": best["direction"],
        "score": best["score"],
        "stake": stake,
        "entry_price": best["price"],
        "result": "ABERTA (PAPER)",
    }
    state["history"].insert(0, entry)
    state["history"] = state["history"][:50]
    state["entries"] += 1
    return {"ok": True, "entry": entry}

app.mount("/static", StaticFiles(directory="static"), name="static")
