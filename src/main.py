"""
fb-trade-decision: Calcula position sizing, SL e TP.

Fluxo:
  trade.opportunity → para cada oportunidade:
    → fetch preço atual + ATR
    → position_size = (RISK_PERCENT * capital) / (SL_ATR * atr)
    → SL = price - SL_ATR * atr
    → TP = price + TP_ATR * atr
    → publica trade.order
"""
import asyncio, logging, os, json, numpy as np, ccxt, nats
from nats.js.api import ConsumerConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fb-trade-decision")

NATS_URL = os.getenv("NATS_URL", "nats://crypto-nats:4222")
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "0.05"))  # 100% = todo saldo distribuído
SL_ATR = float(os.getenv("SL_ATR", "2.0"))
TP_ATR = float(os.getenv("TP_ATR", "4.0"))
ATR_PERIOD = 14
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "20"))
MIN_SIZE_USDT = float(os.getenv("MIN_SIZE_USDT", "5.0"))  # tamanho minimo da posicao

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")


class TradeDecision:
    def __init__(self):
        self.nc = None
        self.js = None
        self.exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
        })

    async def connect_nats(self):
        self.nc = await nats.connect(NATS_URL)
        self.js = self.nc.jetstream()
        logger.info(f"NATS conectado: {NATS_URL}")

    def compute_atr(self, highs, lows, closes):
        tr = np.maximum.reduce([
            np.array(highs[1:]) - np.array(lows[1:]),
            np.abs(np.array(highs[1:]) - np.array(closes[:-1])),
            np.abs(np.array(lows[1:]) - np.array(closes[:-1])),
        ])
        import pandas as pd
        atr = pd.Series(tr).rolling(ATR_PERIOD).mean().values
        return float(atr[-1])

    async def get_usdt_balance(self):
        try:
            balance = self.exchange.fetch_balance()
            return float(balance["USDT"]["free"])
        except Exception as e:
            logger.error(f"Erro ao buscar balance USDT: {e}")
            return 0.0

    async def process_opportunity(self, msg):
        try:
            opportunities = json.loads(msg.data.decode())
            logger.info(f"Calculando sizing para {len(opportunities)} oportunidades")
            orders = []

            usdt_balance = await self.get_usdt_balance()
            if usdt_balance <= 0:
                logger.error("Balance USDT zerado ou indisponível")
                await msg.ack()
                return

            logger.info(f"Balance USDT: {usdt_balance:.2f}")

            for opp in opportunities:
                symbol = opp["symbol"]
                score = opp["score"]
                rsi = opp.get("rsi", 0)

                try:
                    ohlcv = self.exchange.fetch_ohlcv(symbol, "15m", limit=50)
                except Exception as e:
                    logger.error(f"Erro OHLCV {symbol}: {e}")
                    continue

                highs = [c[2] for c in ohlcv]
                lows = [c[3] for c in ohlcv]
                closes = [c[4] for c in ohlcv]
                current_price = closes[-1]

                atr = self.compute_atr(highs, lows, closes)

                # Exposição total = saldo × RISK_PERCENT, distribuída igualmente
                exposed_capital = usdt_balance * RISK_PERCENT
                notional_per_trade = max(MIN_SIZE_USDT, exposed_capital / MAX_POSITIONS) if MAX_POSITIONS > 0 else exposed_capital
                position_size = notional_per_trade / current_price if current_price > 0 else 0
                risk_usdt = position_size * atr * SL_ATR  # risco real baseado no ATR

                # Arredonda e garante minimo da Binance
                try:
                    min_qty = self.exchange.market(symbol)["limits"]["amount"]["min"]
                    position_size_str = self.exchange.amount_to_precision(symbol, position_size)
                    position_size = float(position_size_str)
                    if position_size < min_qty:
                        position_size = min_qty  # garante pelo menos 1 unidade minima
                except Exception:
                    pass

                if position_size * current_price < MIN_SIZE_USDT:
                    logger.info(f"  {symbol}: size={position_size} ({position_size*current_price:.1f} USDT) < min → ignora")
                    continue

                sl_price = round(current_price - SL_ATR * atr, 4)
                tp_price = round(current_price + TP_ATR * atr, 4)

                order = {
                    "symbol": symbol,
                    "tier": opp.get("tier", ""),
                    "strategy": opp.get("strategy", ""),
                    "direction": "LONG",
                    "score": score,
                    "rsi": rsi,
                    "entry_price": current_price,
                    "quantity": position_size,
                    "sl_price": sl_price,
                    "tp_price": tp_price,
                    "atr": round(atr, 6),
                    "risk_usdt": round(risk_usdt, 2),
                    "notional": round(position_size * current_price, 2),
                    "timestamp": opp.get("timestamp", ""),
                }

                logger.info(f"  {symbol}: qty={position_size} entry={current_price} SL={sl_price} TP={tp_price} risk={risk_usdt:.1f}USDT")
                orders.append(order)

            if orders:
                payload = json.dumps(orders).encode()
                await self.js.publish("trade.order", payload)
                logger.info(f"Publicadas {len(orders)} ordens em trade.order")

            await msg.ack()
        except Exception as e:
            logger.error(f"Erro ao processar: {e}")

    async def run(self):
        await self.connect_nats()
        await self.js.subscribe("trade.opportunity", durable="TRADE_DECISION_WORKER",
                                 cb=self.process_opportunity, manual_ack=True,
                                 config=ConsumerConfig(ack_wait=30))
        logger.info(f"fb-trade-decision online (risk={RISK_PERCENT*100}%, SL={SL_ATR}xATR, TP={TP_ATR}xATR)")
        while True:
            if self.nc.is_closed:
                await self.connect_nats()
            await asyncio.sleep(10)


if __name__ == "__main__":
    td = TradeDecision()
    asyncio.run(td.run())
