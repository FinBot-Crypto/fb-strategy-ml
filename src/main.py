import time
import logging
import os
import json
import redis
import ccxt
import pandas as pd
import numpy as np

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("strategy-ml")

# Configurações via Ambiente
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

class StrategyMLService:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.pubsub = self.r.pubsub()
        self.exchange = ccxt.binance()
        
    def fetch_historical_data(self, symbol, timeframe='1h', limit=100):
        """Busca dados históricos para alimentar os modelos."""
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            return df
        except Exception as e:
            logger.error(f"Erro ao buscar histórico para {symbol}: {e}")
            return None

    def run_models(self, symbol, df):
        """Simula a execução de 6 modelos (Breakout e Mean Reversion)."""
        # Aqui entrarão os modelos reais (XGBoost, Random Forest, etc)
        # Por enquanto, geramos um score baseado em indicadores simples (RSI/Moving Average)
        
        # Exemplo de lógica placeholder:
        close = df['close'].values
        rsi_mock = np.random.uniform(30, 70)
        
        strategies = [
            {"name": "breakout_v1", "score": np.random.random(), "tier": "Major"},
            {"name": "breakout_v2", "score": np.random.random(), "tier": "Major"},
            {"name": "mean_reversion_v1", "score": np.random.random(), "tier": "Strong Alt"},
            {"name": "mean_reversion_v2", "score": np.random.random(), "tier": "Strong Alt"},
            {"name": "trend_follower_v1", "score": np.random.random(), "tier": "High Volatility"},
            {"name": "trend_follower_v2", "score": np.random.random(), "tier": "High Volatility"}
        ]
        
        return strategies

    def process_market_update(self, message):
        """Callback acionado quando novos ativos são selecionados."""
        assets = json.loads(message['data'])
        logger.info(f"Recebido update de mercado com {len(assets)} ativos.")
        
        all_evaluations = []
        
        for asset in assets:
            symbol = asset['symbol']
            logger.info(f"Analisando {symbol}...")
            
            df = self.fetch_historical_data(symbol)
            if df is not None:
                evaluations = self.run_models(symbol, df)
                all_evaluations.append({
                    "symbol": symbol,
                    "strategies": evaluations
                })
        
        # Publicar resultados para o Decision Engine
        if all_evaluations:
            payload = json.dumps(all_evaluations)
            self.r.set("ml:strategy_scores", payload)
            self.r.publish("events:strategies_evaluated", payload)
            logger.info(f"Avaliações de ML enviadas para {len(all_evaluations)} ativos.")

    def run(self):
        self.pubsub.subscribe(**{'events:market_updated': self.process_market_update})
        logger.info("Strategy ML Service aguardando eventos 'events:market_updated'...")
        
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                # O processamento acontece no callback
                pass

if __name__ == "__main__":
    service = StrategyMLService()
    service.run()
