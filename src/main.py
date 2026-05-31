"""
fb-trade-decision: Calcula position sizing, SL e TP para Spot e Futures.

Fluxo:
  trade.opportunity → para cada oportunidade:
    → decide rota (Spot ou Futures) com base no score e FUTURES_ENABLED
    → calcula leverage para Futures (2x, 3x, 5x)
    → fetch preço atual + ATR
    → calcula tamanho com base no saldo real e limites mínimos da Binance
    → publica no canal correspondente (trade.order ou trade.order.futures)
"""
import asyncio, logging, os, json, numpy as np, ccxt, nats
from nats.js.api import ConsumerConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fb-trade-decision")

NATS_URL = os.getenv("NATS_URL", "nats://crypto-nats:4222")
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "0.05"))
SL_ATR = float(os.getenv("SL_ATR", "2.0"))
TP_ATR = float(os.getenv("TP_ATR", "4.0"))

# Piso e Teto para SL e TP em percentual
MIN_SL_PCT = float(os.getenv("MIN_SL_PCT", "0.01"))
MAX_SL_PCT = float(os.getenv("MAX_SL_PCT", "0.03"))
MIN_TP_PCT = float(os.getenv("MIN_TP_PCT", "0.02"))
MAX_TP_PCT = float(os.getenv("MAX_TP_PCT", "0.06"))

ATR_PERIOD = 14
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "20"))
MIN_SIZE_USDT = float(os.getenv("MIN_SIZE_USDT", "5.0"))

# Configurações de Futures
FUTURES_ENABLED = os.getenv("FUTURES_ENABLED", "false").lower() == "true"
FUTURES_MIN_SCORE = float(os.getenv("FUTURES_MIN_SCORE", "0.85"))
FUTURES_MAX_POSITIONS = int(os.getenv("FUTURES_MAX_POSITIONS", "5"))
SPOT_MAX_POSITIONS = int(os.getenv("SPOT_MAX_POSITIONS", str(MAX_POSITIONS)))
LEVERAGE_HIGH = int(os.getenv("LEVERAGE_HIGH", "3"))
LEVERAGE_MAX = int(os.getenv("LEVERAGE_MAX", "5"))
COOLDOWN_HOURS = float(os.getenv("COOLDOWN_HOURS", "0"))  # 0 = desligado

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")


class TradeDecision:
    def __init__(self):
        self.nc = None
        self.js = None
        self.spot_exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
        })
        self.futures_exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
            "options": {
                "defaultType": "future"
            }
        })
        # Compatibilidade
        self.exchange = self.spot_exchange

        try:
            self.spot_exchange.load_markets()
            self.futures_exchange.load_markets()
            logger.info("Mercados carregados com sucesso (Spot + Futures).")
        except Exception as e:
            logger.error(f"Erro ao inicializar mercados: {e}")

        # Cooldown: evita reentrar na mesma moeda por X horas
        self.last_exit_time = {}  # symbol -> timestamp da última saída
        import time, psycopg2
        self._time = time
        try:
            self._db_conn = psycopg2.connect(os.getenv("DATABASE_URL", ""))
            self._db_cursor = self._db_conn.cursor()
        except Exception:
            self._db_conn = None
            self._db_cursor = None

    async def connect_nats(self):
        self.nc = await nats.connect(NATS_URL)
        self.js = self.nc.jetstream()
        try:
            self.kv = await self.js.key_value("active_positions")
        except Exception as e:
            logger.error(f"Erro ao obter KV active_positions no trade-decision: {e}")
            self.kv = None
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

    async def get_spot_usdt_balance(self):
        try:
            balance = self.spot_exchange.fetch_balance()
            return float(balance["USDT"]["free"])
        except Exception as e:
            logger.error(f"Erro ao buscar balance USDT Spot: {e}")
            return 0.0

    async def get_futures_usdt_balance(self):
        try:
            balance = self.futures_exchange.fetch_balance()
            return float(balance["USDT"]["free"])
        except Exception as e:
            logger.error(f"Erro ao buscar balance USDT Futures: {e}")
            return 0.0

    async def get_total_spot_portfolio(self):
        try:
            balance = self.spot_exchange.fetch_balance()
            total = float(balance["USDT"]["free"])
            for asset, amount in balance["total"].items():
                if asset == "USDT" or amount <= 0:
                    continue
                try:
                    ticker = self.spot_exchange.fetch_ticker(f"{asset}/USDT")
                    total += amount * ticker["last"]
                except Exception:
                    pass
            return total
        except Exception as e:
            logger.error(f"Erro ao calcular patrimônio Spot: {e}")
            return 0.0

    async def count_active_futures_positions(self):
        """Conta posições ativas de Futures no KV store."""
        if not self.kv:
            return 0
        try:
            keys = await self.kv.keys()
            count = 0
            for key in keys:
                entry = await self.kv.get(key)
                if entry:
                    val = json.loads(entry.value.decode())
                    if val.get("is_futures"):
                        count += 1
            return count
        except Exception:
            return 0

    async def process_opportunity(self, msg):
        try:
            opportunities = json.loads(msg.data.decode())
            logger.info(f"Calculando sizing para {len(opportunities)} oportunidades")
            
            spot_orders = []
            futures_orders = []

            # 1. Busca saldos reais diretamente via API (segundo especificação do usuário)
            spot_balance = await self.get_spot_usdt_balance()
            futures_balance = await self.get_futures_usdt_balance()
            spot_portfolio = await self.get_total_spot_portfolio()

            # Conta quantidade de posições ativas de Futures
            active_futures = await self.count_active_futures_positions()

            logger.info(f"Saldos Reais | Spot USDT: ${spot_balance:.2f} (Total: ${spot_portfolio:.2f}) | Futures USDT: ${futures_balance:.2f} | Posições Futures Ativas: {active_futures}/{FUTURES_MAX_POSITIONS}")

            for opp in opportunities:
                symbol = opp["symbol"]
                score = opp["score"]
                rsi = opp.get("rsi", 0)

                # Cooldown: só bloqueia se último trade foi LOSS
                if COOLDOWN_HOURS > 0 and symbol in self.last_exit_time:
                    elapsed = self._time.time() - self.last_exit_time[symbol]
                    if elapsed < COOLDOWN_HOURS * 3600:
                        # Verifica se o último trade fechado desse símbolo foi loss
                        try:
                            cur = self._db_cursor
                            cur.execute(
                                "SELECT pnl_pct FROM trade_log WHERE symbol=%s AND status='CLOSED' ORDER BY created_at DESC LIMIT 1",
                                (symbol,))
                            row = cur.fetchone()
                            if row and (row[0] is None or row[0] <= 0):
                                logger.info(f"  {symbol}: cooldown após loss ({elapsed/3600:.1f}h < {COOLDOWN_HOURS}h) → ignora")
                                continue
                        except Exception:
                            pass

                # 2. Decide a Rota Inicial (Futures vs Spot)
                is_futures_route = FUTURES_ENABLED and score >= FUTURES_MIN_SCORE
                current_exchange = self.futures_exchange if is_futures_route else self.spot_exchange

                # Se a rota inicial for Futures, fazemos a verificação preventiva de limite de posições e de saldo/margem livre
                if is_futures_route:
                    # Verifica limite de posições preventivamente
                    if active_futures >= FUTURES_MAX_POSITIONS:
                        logger.warning(f"  [FALLBACK] Limite de posições Futures atingido ({active_futures}/{FUTURES_MAX_POSITIONS}). Desviando {symbol} para SPOT.")
                        is_futures_route = False
                        current_exchange = self.spot_exchange
                    else:
                        if score >= 0.95:
                            leverage = LEVERAGE_MAX
                        elif score >= 0.90:
                            leverage = LEVERAGE_HIGH
                        else:
                            leverage = 2

                        min_notional_f = 5.0
                        try:
                            min_notional_f = float(self.futures_exchange.market(symbol)["limits"]["cost"]["min"])
                        except Exception:
                            pass

                        # 100% do saldo de Futures dividido por posições máximas
                        candidate_notional = (futures_balance * leverage) / FUTURES_MAX_POSITIONS if FUTURES_MAX_POSITIONS > 0 else (futures_balance * leverage)
                        
                        # Garante que atende ao mínimo exigido pela moeda
                        notional_per_trade = max(candidate_notional, min_notional_f)
                        
                        # Margem necessária para a posição
                        margin_required = notional_per_trade / leverage
                        
                        # Se a margem requerida for maior que o saldo real livre, ou o saldo livre for ínfimo, desvia para Spot
                        if margin_required > futures_balance * 0.98 or futures_balance <= 1.0:
                            logger.warning(f"  [FALLBACK] Saldo Futures insuficiente (${futures_balance:.2f} USDT, necessário ${margin_required:.2f} USDT). Desviando {symbol} para SPOT.")
                            is_futures_route = False
                            current_exchange = self.spot_exchange
                        else:
                            # Se passou nas validações, incrementamos preventivamente para a próxima oportunidade do mesmo loop
                            active_futures += 1

                try:
                    ohlcv = current_exchange.fetch_ohlcv(symbol, "15m", limit=50)
                except Exception as e:
                    logger.error(f"Erro ao buscar OHLCV {symbol} ({'Futures' if is_futures_route else 'Spot'}): {e}")
                    continue

                highs = [c[2] for c in ohlcv]
                lows = [c[3] for c in ohlcv]
                closes = [c[4] for c in ohlcv]
                current_price = closes[-1]

                atr = self.compute_atr(highs, lows, closes)

                # 3. Limite Mínimo da Binance
                min_notional = 5.0
                try:
                    min_notional = float(current_exchange.market(symbol)["limits"]["cost"]["min"])
                except Exception:
                    pass

                # 4. Cálculo do Position Sizing final
                if is_futures_route:
                    # (Lógica e valores de Futures já validados e calculados acima)
                    sl_price = 0.0
                else:
                    leverage = 1
                    exposed = spot_portfolio * RISK_PERCENT
                    candidate_notional = exposed / SPOT_MAX_POSITIONS if SPOT_MAX_POSITIONS > 0 else exposed

                    # SL dita o minimo para Spot (perna mais frágil do OCO)
                    if SL_ATR <= 0:
                        sl_price = 0.0
                        sl_min_notional = 0.0
                    else:
                        atr_sl_dist = SL_ATR * atr
                        sl_dist = max(atr_sl_dist, current_price * MIN_SL_PCT)
                        sl_dist = min(sl_dist, current_price * MAX_SL_PCT)
                        sl_price = current_price - sl_dist
                        sl_min_qty = min_notional / sl_price if sl_price > 0 else 0
                        sl_min_notional = sl_min_qty * current_price

                    notional_per_trade = max(candidate_notional, min_notional, sl_min_notional)
                    notional_per_trade = min(notional_per_trade, spot_balance * 0.98)

                # Calcular preço de alvo (Take Profit)
                atr_tp_dist = TP_ATR * atr
                tp_dist = max(atr_tp_dist, current_price * MIN_TP_PCT)
                tp_dist = min(tp_dist, current_price * MAX_TP_PCT)
                tp_price = current_price + tp_dist

                # 5. Quantidade final formatada
                position_size = notional_per_trade / current_price if current_price > 0 else 0
                try:
                    position_size = float(current_exchange.amount_to_precision(symbol, position_size))
                except Exception:
                    pass

                final_notional = position_size * current_price

                # Validação de Mínimo da Binance
                if final_notional < min_notional:
                    logger.info(f"  {symbol}: size={position_size} (${final_notional:.1f}) < Min Binance (${min_notional}) → ignorando")
                    continue

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
                    "notional": round(final_notional, 2),
                    "timestamp": opp.get("timestamp", ""),
                }

                if is_futures_route:
                    order["leverage"] = leverage
                    order["is_futures"] = True
                    futures_orders.append(order)
                    logger.info(f"  [FUTURES ROUTE] {symbol}: qty={position_size} entry={current_price} TP={tp_price} leverage={leverage}x notional={final_notional:.2f}")
                else:
                    order["leverage"] = 1
                    order["is_futures"] = False
                    spot_orders.append(order)
                    logger.info(f"  [SPOT ROUTE] {symbol}: qty={position_size} entry={current_price} SL={sl_price} TP={tp_price} notional={final_notional:.2f}")

            # 6. Publicação das Ordens
            if spot_orders:
                payload = json.dumps(spot_orders).encode()
                await self.js.publish("trade.order", payload)
                logger.info(f"Publicadas {len(spot_orders)} ordens em trade.order (Spot)")
                for o in spot_orders:
                    self.last_exit_time[o["symbol"]] = self._time.time()

            if futures_orders:
                payload = json.dumps(futures_orders).encode()
                await self.js.publish("trade.order.futures", payload)
                logger.info(f"Publicadas {len(futures_orders)} ordens em trade.order.futures (Futures)")
                for o in futures_orders:
                    self.last_exit_time[o["symbol"]] = self._time.time()

            await msg.ack()
        except Exception as e:
            logger.error(f"Erro ao processar: {e}")

    async def run(self):
        await self.connect_nats()
        await self.js.subscribe("trade.opportunity", durable="TRADE_DECISION_WORKER",
                                 cb=self.process_opportunity, manual_ack=True,
                                 config=ConsumerConfig(ack_wait=30))
        logger.info(f"fb-trade-decision online (Spot + Futures) [Futures={FUTURES_ENABLED}, min_score={FUTURES_MIN_SCORE}]")
        while True:
            if self.nc.is_closed:
                await self.connect_nats()
            await asyncio.sleep(10)


if __name__ == "__main__":
    td = TradeDecision()
    asyncio.run(td.run())
