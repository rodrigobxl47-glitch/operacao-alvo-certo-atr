import asyncio
import json
import time
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Optional, Tuple

import websockets
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="Operação Alvo Certo (ATR)")

PUBLIC_WS = "wss://api.derivws.com/trading/v1/options/ws/public"


# ============================================================
# CONFIGURAÇÃO
# ============================================================

class Config(BaseModel):
    banca_inicial: float = 1000.0
    percentual_entrada: float = 1.0
    stop_gain: float = 5.0
    stop_loss: float = 5.0
    max_entradas: int = 5
    min_score: int = 5
    duracao_minutos: int = 1
    payout_demo: float = 0.80
    intervalo_entre_ativos_ms: int = 120


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
    "open_trade": None,
    "diagnostics": {},
}

robot_task: Optional[asyncio.Task] = None
scan_lock = asyncio.Lock()


# ============================================================
# MODELOS
# ============================================================

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


# ============================================================
# CONEXÃO DERIV
# Uma única conexão reutilizada
# ============================================================

class DerivPublicClient:
    def __init__(self):
        self.ws = None
        self.lock = asyncio.Lock()
        self.req_id = 1000

    async def connect(self):
        if self.ws is not None:
            try:
                if not self.ws.closed:
                    return
            except Exception:
                pass

        delay = 2

        while True:
            try:
                self.ws = await websockets.connect(
                    PUBLIC_WS,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=None,
                )
                print("[ATR] WebSocket Deriv conectado.")
                return

            except Exception as exc:
                msg = str(exc)

                if "429" in msg:
                    print(
                        f"[ATR] Deriv respondeu 429. "
                        f"Aguardando {delay}s antes de reconectar."
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 60)
                else:
                    print(
                        f"[ATR] Falha ao conectar na Deriv: {exc}. "
                        f"Nova tentativa em {delay}s."
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)

    async def close(self):
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None

    async def request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        async with self.lock:
            attempts = 0

            while attempts < 5:
                attempts += 1

                try:
                    await self.connect()

                    self.req_id += 1
                    req_id = self.req_id

                    data_out = dict(payload)
                    data_out["req_id"] = req_id

                    await self.ws.send(
                        json.dumps(data_out)
                    )

                    while True:
                        raw = await asyncio.wait_for(
                            self.ws.recv(),
                            timeout=30
                        )

                        data = json.loads(raw)

                        if data.get("req_id") != req_id:
                            continue

                        if "error" in data:
                            message = data["error"].get(
                                "message",
                                "Erro Deriv"
                            )

                            raise RuntimeError(message)

                        return data

                except Exception as exc:
                    message = str(exc)

                    await self.close()

                    if "429" in message:
                        delay = min(
                            3 * attempts,
                            20
                        )

                        print(
                            f"[ATR] 429 em requisição. "
                            f"Aguardando {delay}s."
                        )

                        await asyncio.sleep(delay)

                    else:
                        delay = min(
                            2 * attempts,
                            10
                        )

                        print(
                            f"[ATR] Erro WebSocket: {message}. "
                            f"Nova tentativa em {delay}s."
                        )

                        await asyncio.sleep(delay)

            raise RuntimeError(
                "Falha ao comunicar com a Deriv após várias tentativas."
            )


deriv = DerivPublicClient()


# ============================================================
# INDICADORES
# ============================================================

def ema(
    values: List[float],
    period: int
) -> List[float]:

    if not values:
        return []

    alpha = 2.0 / (
        period + 1.0
    )

    out = [
        values[0]
    ]

    for value in values[1:]:
        out.append(
            alpha * value
            +
            (
                1.0 - alpha
            )
            *
            out[-1]
        )

    return out


# ============================================================
# DADOS DE MERCADO
# ============================================================

async def active_symbols():

    data = await deriv.request(
        {
            "active_symbols": "brief",
            "contract_type": [
                "CALL",
                "PUT"
            ]
        }
    )

    return data.get(
        "active_symbols",
        []
    )


async def candles(
    symbol: str,
    count: int = 140
) -> List[Candle]:

    data = await deriv.request(
        {
            "ticks_history": symbol,
            "adjust_start_time": 1,
            "count": count,
            "end": "latest",
            "granularity": 60,
            "style": "candles",
            "subscribe": 0
        }
    )

    result: List[Candle] = []

    for candle in data.get(
        "candles",
        []
    ):

        try:
            result.append(
                Candle(
                    epoch=int(
                        candle["epoch"]
                    ),
                    open=float(
                        candle["open"]
                    ),
                    high=float(
                        candle["high"]
                    ),
                    low=float(
                        candle["low"]
                    ),
                    close=float(
                        candle["close"]
                    )
                )
            )

        except Exception:
            continue

    return result


async def latest_price(
    symbol: str
) -> Optional[float]:

    data = await candles(
        symbol,
        2
    )

    if not data:
        return None

    return data[-1].close


# ============================================================
# ANÁLISE SOMENTE M1
# ============================================================

def analyze(
    symbol: str,
    name: str,
    m1: List[Candle]
) -> Tuple[
    Optional[Signal],
    str,
    int,
    int
]:

    if len(m1) < 110:
        return (
            None,
            f"M1 insuficiente ({len(m1)})",
            0,
            0
        )

    closes = [
        x.close
        for x in m1
    ]

    highs = [
        x.high
        for x in m1
    ]

    lows = [
        x.low
        for x in m1
    ]

    hlc3 = [
        (
            x.high
            +
            x.low
            +
            x.close
        )
        /
        3.0
        for x in m1
    ]

    # Configuração original do indicador
    e10 = ema(
        closes,
        10
    )

    e100 = ema(
        closes,
        100
    )

    e3 = ema(
        hlc3,
        3
    )

    e13 = ema(
        hlc3,
        13
    )

    c0 = m1[-1]
    c1 = m1[-2]
    c2 = m1[-3]
    c3 = m1[-4]

    resistance = max(
        highs[-11:-1]
    )

    support = min(
        lows[-11:-1]
    )

    # Tendência micro
    trend_up = (
        c0.close > c1.close
        and
        c0.close > e10[-1]
        and
        e10[-1] > e10[-2]
    )

    trend_down = (
        c0.close < c1.close
        and
        c0.close < e10[-1]
        and
        e10[-1] < e10[-2]
    )

    # Macro tendência
    macro_up = (
        c0.close > e100[-1]
        and
        e10[-1] > e100[-1]
    )

    macro_down = (
        c0.close < e100[-1]
        and
        e10[-1] < e100[-1]
    )

    # Cruzamento EMA 3 / EMA 13
    cross_up = (
        e3[-2] < e13[-2]
        and
        e3[-1] > e13[-1]
    )

    cross_down = (
        e3[-2] > e13[-2]
        and
        e3[-1] < e13[-1]
    )

    # Engolfo
    bull_engulfing = (
        c1.close < c1.open
        and
        c0.close > c0.open
        and
        c0.open <= c1.close
        and
        c0.close >= c1.open
    )

    bear_engulfing = (
        c1.close > c1.open
        and
        c0.close < c0.open
        and
        c0.open >= c1.close
        and
        c0.close <= c1.open
    )

    # Sequência de velas
    bull_sequence = (
        c0.close >
        c1.close >
        c2.close >=
        c3.close
    )

    bear_sequence = (
        c0.close <
        c1.close <
        c2.close <=
        c3.close
    )

    call_score = 0
    put_score = 0

    call_reasons: List[str] = []
    put_reasons: List[str] = []

    if trend_up:
        call_score += 1
        call_reasons.append(
            "Tendência M1 alta"
        )

    if trend_down:
        put_score += 1
        put_reasons.append(
            "Tendência M1 baixa"
        )

    if macro_up:
        call_score += 2
        call_reasons.append(
            "EMA10 acima EMA100"
        )

    if macro_down:
        put_score += 2
        put_reasons.append(
            "EMA10 abaixo EMA100"
        )

    if cross_up:
        call_score += 1
        call_reasons.append(
            "Cruzamento EMA3/13 alta"
        )

    if cross_down:
        put_score += 1
        put_reasons.append(
            "Cruzamento EMA3/13 baixa"
        )

    if bull_engulfing:
        call_score += 2
        call_reasons.append(
            "Engolfo de alta"
        )

    if bear_engulfing:
        put_score += 2
        put_reasons.append(
            "Engolfo de baixa"
        )

    if bull_sequence:
        call_score += 1
        call_reasons.append(
            "Sequência de alta"
        )

    if bear_sequence:
        put_score += 1
        put_reasons.append(
            "Sequência de baixa"
        )

    # Suporte / resistência
    range_size = max(
        resistance - support,
        1e-12
    )

    position = (
        c0.close - support
    ) / range_size

    if position <= 0.35:
        call_score += 1
        call_reasons.append(
            "Região de suporte"
        )

    if position >= 0.65:
        put_score += 1
        put_reasons.append(
            "Região de resistência"
        )

    if (
        call_score == 0
        and
        put_score == 0
    ):
        return (
            None,
            "score zero",
            call_score,
            put_score
        )

    if call_score == put_score:
        return (
            None,
            f"empate CALL={call_score} PUT={put_score}",
            call_score,
            put_score
        )

    if call_score > put_score:
        direction = "CALL"
        score = call_score
        reasons = call_reasons

    else:
        direction = "PUT"
        score = put_score
        reasons = put_reasons

    signal = Signal(
        symbol=symbol,
        name=name,
        direction=direction,
        score=score,
        price=c0.close,
        qualified=(
            score >=
            config.min_score
        ),
        reasons=reasons,
        support=support,
        resistance=resistance
    )

    return (
        signal,
        "ok",
        call_score,
        put_score
    )


# ============================================================
# ANALISAR UM ATIVO
# ============================================================

async def analyze_one(
    item
):

    symbol = (
        item.get(
            "underlying_symbol"
        )
        or
        item.get(
            "symbol"
        )
    )

    if not symbol:
        return {
            "signal": None,
            "status": "sem símbolo",
            "symbol": "?",
            "name": "?",
            "m1": 0,
            "call": 0,
            "put": 0
        }

    name = (
        item.get(
            "underlying_symbol_name"
        )
        or
        item.get(
            "display_name"
        )
        or
        item.get(
            "market_display_name"
        )
        or
        symbol
    )

    try:

        m1 = await candles(
            symbol,
            140
        )

        (
            signal,
            status,
            call_score,
            put_score
        ) = analyze(
            symbol,
            name,
            m1
        )

        return {
            "signal": signal,
            "status": status,
            "symbol": symbol,
            "name": name,
            "m1": len(m1),
            "call": call_score,
            "put": put_score
        }

    except Exception as exc:

        return {
            "signal": None,
            "status": f"erro: {exc}",
            "symbol": symbol,
            "name": name,
            "m1": 0,
            "call": 0,
            "put": 0
        }


# ============================================================
# SCANNER
# Sem 71 conexões simultâneas
# ============================================================

async def scan_once():

    async with scan_lock:

        state["status"] = (
            "Analisando mercados M1..."
        )

        symbols = await active_symbols()

        results = []

        for index, item in enumerate(
            symbols,
            start=1
        ):

            if not state["running"] and state["last_scan"] is not None:
                break

            result = await analyze_one(
                item
            )

            results.append(
                result
            )

            if index % 10 == 0:
                print(
                    f"[ATR] Scanner M1: "
                    f"{index}/{len(symbols)} ativos processados."
                )

            await asyncio.sleep(
                max(
                    config.intervalo_entre_ativos_ms,
                    0
                )
                /
                1000.0
            )

        signals = [
            x["signal"]
            for x in results
            if x["signal"] is not None
        ]

        signals.sort(
            key=lambda x: x.score,
            reverse=True
        )

        state["signals"] = [
            asdict(x)
            for x in signals[:25]
        ]

        state["last_scan"] = int(
            time.time()
        )

        valid = [
            x
            for x in results
            if x["m1"] >= 110
        ]

        api_errors = [
            x
            for x in results
            if x["status"].startswith(
                "erro:"
            )
        ]

        short = [
            x
            for x in results
            if x["status"].startswith(
                "M1 insuficiente"
            )
        ]

        zero = [
            x
            for x in results
            if x["status"]
            ==
            "score zero"
        ]

        ties = [
            x
            for x in results
            if x["status"].startswith(
                "empate"
            )
        ]

        qualified = [
            s
            for s in signals
            if s.qualified
        ]

        max_score = (
            signals[0].score
            if signals
            else 0
        )

        state["diagnostics"] = {
            "symbols_total":
            len(symbols),

            "processed":
            len(results),

            "valid_data":
            len(valid),

            "api_errors":
            len(api_errors),

            "m1_insufficient":
            len(short),

            "zero_score":
            len(zero),

            "ties":
            len(ties),

            "candidates":
            len(signals),

            "qualified":
            len(qualified),

            "max_score":
            max_score,

            "best_name":
            (
                signals[0].name
                if signals
                else "-"
            ),

            "best_direction":
            (
                signals[0].direction
                if signals
                else "-"
            )
        }

        state["status"] = (
            f"{len(results)} ativos M1 analisados"
        )

        print(
            "[ATR] DIAGNÓSTICO M1 | "
            f"ativos={len(symbols)} | "
            f"processados={len(results)} | "
            f"dados_validos={len(valid)} | "
            f"erros_api={len(api_errors)} | "
            f"m1_curto={len(short)} | "
            f"score_zero={len(zero)} | "
            f"empates={len(ties)} | "
            f"candidatos={len(signals)} | "
            f"qualificados={len(qualified)} | "
            f"maior_score={max_score}"
        )

        if signals:
            best = signals[0]

            print(
                "[ATR] MELHOR M1 | "
                f"{best.name} | "
                f"{best.direction} | "
                f"score={best.score} | "
                f"qualificado={best.qualified}"
            )

        for item in (
            api_errors +
            short
        )[:5]:

            print(
                "[ATR] AMOSTRA_FALHA | "
                f"{item['symbol']} | "
                f"{item['status']} | "
                f"M1={item['m1']}"
            )

        return state["signals"]


# ============================================================
# GESTÃO DE RISCO
# ============================================================

def risk_ok():

    start = state[
        "start_balance"
    ]

    pnl = (
        (
            state["balance"]
            -
            start
        )
        /
        start
        *
        100
        if start
        else 0
    )

    if pnl >= config.stop_gain:
        state["status"] = (
            "Stop Gain atingido"
        )
        return False

    if pnl <= -config.stop_loss:
        state["status"] = (
            "Stop Loss atingido"
        )
        return False

    if (
        state["entries"]
        >=
        config.max_entradas
    ):
        state["status"] = (
            "Limite de entradas atingido"
        )
        return False

    return True


def best_qualified_signal():

    return next(
        (
            signal
            for signal
            in state["signals"]
            if signal.get(
                "qualified"
            )
        ),
        None
    )


# ============================================================
# PAPER TRADING
# ============================================================

def open_paper_trade(
    signal
):

    if (
        state["open_trade"]
        is not None
    ):
        return None

    if not risk_ok():
        return None

    stake = round(
        state["balance"]
        *
        config.percentual_entrada
        /
        100.0,
        2
    )

    if stake <= 0:
        return None

    now = int(
        time.time()
    )

    trade = {
        "time":
        now,

        "symbol":
        signal["symbol"],

        "name":
        signal["name"],

        "direction":
        signal["direction"],

        "score":
        signal["score"],

        "stake":
        stake,

        "entry_price":
        float(
            signal["price"]
        ),

        "expires_at":
        now
        +
        int(
            config.duracao_minutos
            *
            60
        ),

        "result":
        "ABERTA (PAPER)",

        "qualified":
        True
    }

    state["open_trade"] = trade

    state["entries"] += 1

    state["history"].insert(
        0,
        dict(trade)
    )

    state["history"] = (
        state["history"][:50]
    )

    state["status"] = (
        f"Entrada DEMO: "
        f"{signal['direction']} "
        f"{signal['name']}"
    )

    print(
        "[ATR] ENTRADA DEMO M1 | "
        f"{signal['name']} | "
        f"{signal['direction']} | "
        f"score={signal['score']} | "
        f"stake={stake}"
    )

    return trade


async def settle_open_trade():

    trade = state.get(
        "open_trade"
    )

    if not trade:
        return

    now = int(
        time.time()
    )

    if now < trade[
        "expires_at"
    ]:

        remaining = (
            trade["expires_at"]
            -
            now
        )

        state["status"] = (
            "Operação DEMO aberta • "
            f"{remaining}s restantes"
        )

        return

    exit_price = await latest_price(
        trade["symbol"]
    )

    if exit_price is None:
        state["status"] = (
            "Aguardando preço "
            "para encerrar DEMO"
        )
        return

    entry_price = float(
        trade["entry_price"]
    )

    stake = float(
        trade["stake"]
    )

    direction = trade[
        "direction"
    ]

    if exit_price == entry_price:

        result = "EMPATE"
        pnl = 0.0

    elif direction == "CALL":

        if exit_price > entry_price:
            result = "WIN"
            pnl = (
                stake
                *
                config.payout_demo
            )
        else:
            result = "LOSS"
            pnl = -stake

    else:

        if exit_price < entry_price:
            result = "WIN"
            pnl = (
                stake
                *
                config.payout_demo
            )
        else:
            result = "LOSS"
            pnl = -stake

    state["balance"] = round(
        state["balance"]
        +
        pnl,
        2
    )

    closed = dict(
        trade
    )

    closed.update(
        {
            "exit_price":
            exit_price,

            "pnl":
            round(
                pnl,
                2
            ),

            "result":
            result,

            "closed_at":
            int(
                time.time()
            )
        }
    )

    if state["history"]:
        state["history"][0] = (
            closed
        )
    else:
        state["history"].insert(
            0,
            closed
        )

    state["open_trade"] = None

    state["status"] = (
        f"{result} • "
        f"{trade['name']} • "
        f"P/L {pnl:+.2f}"
    )

    print(
        "[ATR] RESULTADO M1 | "
        f"{trade['name']} | "
        f"{result} | "
        f"entrada={entry_price} | "
        f"saida={exit_price} | "
        f"PL={pnl:+.2f} | "
        f"banca={state['balance']:.2f}"
    )


# ============================================================
# ROBÔ AUTOMÁTICO
# ============================================================

async def robot_loop():

    print(
        "[ATR] Robô automático M1 DEMO iniciado."
    )

    try:

        while state["running"]:

            if not risk_ok():
                state["running"] = False
                break

            if (
                state["open_trade"]
                is not None
            ):

                await settle_open_trade()

                await asyncio.sleep(
                    5
                )

                continue

            try:

                await scan_once()

                if not state["running"]:
                    break

                best = (
                    best_qualified_signal()
                )

                if best:

                    open_paper_trade(
                        best
                    )

                else:

                    best_score = (
                        state[
                            "diagnostics"
                        ].get(
                            "max_score",
                            0
                        )
                    )

                    state["status"] = (
                        f"{state['status']} • "
                        f"melhor score "
                        f"{best_score}/"
                        f"{config.min_score}"
                    )

            except Exception as exc:

                state["status"] = (
                    f"Erro no scanner: "
                    f"{exc}"
                )

                print(
                    "[ATR] ERRO_SCANNER | "
                    f"{exc}"
                )

                if "429" in str(exc):
                    await asyncio.sleep(
                        30
                    )

            if (
                state["running"]
                and
                state["open_trade"]
                is None
            ):

                # Próxima análise alinhada ao minuto seguinte.
                wait = max(
                    60 -
                    (
                        time.time()
                        %
                        60
                    ),
                    1
                )

                await asyncio.sleep(
                    wait + 1
                )

            else:

                await asyncio.sleep(
                    5
                )

    except asyncio.CancelledError:
        raise

    finally:

        if not state[
            "running"
        ]:

            print(
                "[ATR] Robô automático M1 DEMO parado."
            )


# ============================================================
# ROTAS
# ============================================================

@app.get("/")
async def home():

    return FileResponse(
        "static/index.html"
    )


@app.get(
    "/api/status"
)
async def get_status():

    start = state[
        "start_balance"
    ]

    pnl = (
        (
            state["balance"]
            -
            start
        )
        /
        start
        *
        100
        if start
        else 0
    )

    return {
        **state,

        "config":
        config.model_dump(),

        "pnl_percent":
        round(
            pnl,
            2
        )
    }


@app.post(
    "/api/config"
)
async def set_config(
    new: Config
):

    global config

    config = new

    if not state[
        "running"
    ]:

        state[
            "balance"
        ] = (
            new.banca_inicial
        )

        state[
            "start_balance"
        ] = (
            new.banca_inicial
        )

        state[
            "entries"
        ] = 0

        state[
            "history"
        ] = []

        state[
            "open_trade"
        ] = None

    return {
        "ok": True,
        "config":
        config.model_dump()
    }


@app.post(
    "/api/scan"
)
async def api_scan():

    signals = await scan_once()

    return {
        "signals":
        signals,

        "count":
        len(
            signals
        ),

        "diagnostics":
        state[
            "diagnostics"
        ]
    }


@app.post(
    "/api/start"
)
async def start():

    global robot_task

    if state[
        "running"
    ]:

        return {
            "ok": True,
            "message":
            "ATR já está ativo."
        }

    state[
        "running"
    ] = True

    state[
        "status"
    ] = (
        "ATR M1 automático iniciado"
    )

    if (
        robot_task is None
        or
        robot_task.done()
    ):

        robot_task = (
            asyncio.create_task(
                robot_loop()
            )
        )

    return {
        "ok": True,
        "message":
        "ATR automático M1 DEMO iniciado."
    }


@app.post(
    "/api/stop"
)
async def stop():

    state[
        "running"
    ] = False

    state[
        "status"
    ] = "Parado"

    return {
        "ok": True,
        "message":
        "ATR parado."
    }


@app.post(
    "/api/paper-entry"
)
async def paper_entry():

    if (
        state[
            "open_trade"
        ]
        is not None
    ):

        return {
            "ok": False,
            "message":
            "Já existe uma operação DEMO aberta."
        }

    if not state[
        "signals"
    ]:

        await scan_once()

    if not risk_ok():

        return {
            "ok": False,
            "message":
            state[
                "status"
            ]
        }

    best = (
        best_qualified_signal()
    )

    if best is None:

        return {
            "ok": False,
            "message":
            (
                "Nenhum sinal atingiu "
                "o score mínimo "
                f"{config.min_score}."
            )
        }

    entry = (
        open_paper_trade(
            best
        )
    )

    if not entry:

        return {
            "ok": False,
            "message":
            "Não foi possível abrir a entrada DEMO."
        }

    return {
        "ok": True,
        "entry":
        entry
    }


@app.on_event("shutdown")
async def shutdown_event():
    await deriv.close()


app.mount(
    "/static",
    StaticFiles(
        directory="static"
    ),
    name="static"
)
