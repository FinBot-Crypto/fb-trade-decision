import time
import logging
import os
import json
import redis
import ccxt

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("trade-decision")

# Configurações via Ambiente
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

class TradeDecisionService:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.pubsub = self.r.pubsub()
        self.exchange = ccxt.binance()

    def calculate_trade_levels(self, symbol, strategy_name, last_price):
        """Define níveis de preço baseados na estratégia."""
        # Exemplo simplificado de lógica:
        # Se for breakout, entra a mercado e coloca stop curto
        # Se for mean reversion, busca retração
        
        entry_price = last_price
        stop_loss = last_price * 0.98  # 2% de stop
        take_profit = last_price * 1.06 # 6% de TP (RR 1:3)
        
        return {
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "side": "buy" # Simplificado para Long
        }

    def process_decision(self, message):
        """Traduz decisões em parâmetros técnicos."""
        decisions = json.loads(message['data'])
        logger.info(f"Processando {len(decisions)} decisões para definir níveis.")
        
        trade_setups = []
        
        for decision in decisions:
            symbol = decision['symbol']
            
            # Busca preço atual rápido
            ticker = self.exchange.fetch_ticker(symbol)
            last_price = ticker['last']
            
            levels = self.calculate_trade_levels(symbol, decision['strategy'], last_price)
            
            setup = {
                **decision,
                **levels,
                "status": "ready_for_risk_eval"
            }
            
            trade_setups.append(setup)
            logger.info(f"SETUP PRONTO: {symbol} | Entry: {levels['entry_price']} | SL: {levels['stop_loss']} | TP: {levels['take_profit']}")

        if trade_setups:
            payload = json.dumps(trade_setups)
            self.r.publish("events:trade_setup_ready", payload)

    def run(self):
        self.pubsub.subscribe(**{'events:trade_decided': self.process_decision})
        logger.info("Trade Decision Service aguardando 'events:trade_decided'...")
        
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                pass

if __name__ == "__main__":
    service = TradeDecisionService()
    service.run()
