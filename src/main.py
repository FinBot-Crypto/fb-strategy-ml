import asyncio
import logging
import os
import json
import ccxt
import pandas as pd
import numpy as np
import nats
from nats.js.api import ConsumerConfig

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("fb-strategy-ml")

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_RECONNECT_WAIT = 5

class StrategyMLService:
    def __init__(self):
        self.nc = None
        self.js = None
        self.kv_ml = None
        self.exchange = ccxt.binance({'enableRateLimit': True})

    async def connect_nats(self):
        while True:
            try:
                self.nc = await nats.connect(NATS_URL)
                self.js = self.nc.jetstream()
                logger.info(f"Conectado ao NATS em {NATS_URL}")
                
                # KV Store
                try:
                    self.kv_ml = await self.js.create_key_value(bucket='ml_evaluations')
                except Exception:
                    self.kv_ml = await self.js.key_value(bucket='ml_evaluations')
                return
            except Exception as e:
                logger.error(f"Erro ao conectar no NATS: {e} - retry em {NATS_RECONNECT_WAIT}s")
                await asyncio.sleep(NATS_RECONNECT_WAIT)

    async def fetch_historical_data(self, symbol, timeframe='1h', limit=100):
        try:
            # fetch_ohlcv synchronous call wrapped in to_thread
            ohlcv = await asyncio.to_thread(self.exchange.fetch_ohlcv, symbol, timeframe, since=None, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            return df
        except Exception as e:
            logger.error(f"Erro ao buscar histórico para {symbol}: {e}")
            return None

    def run_models(self, symbol, df):
        # Placeholder models logic
        strategies = [
            {"name": "breakout_v1", "score": np.random.random(), "tier": "Major"},
            {"name": "breakout_v2", "score": np.random.random(), "tier": "Major"},
            {"name": "mean_reversion_v1", "score": np.random.random(), "tier": "Strong Alt"},
            {"name": "mean_reversion_v2", "score": np.random.random(), "tier": "Strong Alt"},
            {"name": "trend_follower_v1", "score": np.random.random(), "tier": "High Volatility"},
            {"name": "trend_follower_v2", "score": np.random.random(), "tier": "High Volatility"}
        ]
        return strategies

    async def process_market_update(self, msg):
        try:
            assets = json.loads(msg.data.decode())
            logger.info(f"Recebido update de mercado com {len(assets)} ativos.")
            all_evaluations = []

            for asset in assets:
                symbol = asset['symbol']
                df = await self.fetch_historical_data(symbol)
                if df is not None and not df.empty:
                    evaluations = self.run_models(symbol, df)
                    all_evaluations.append({
                        "symbol": symbol,
                        "strategies": evaluations
                    })

            if all_evaluations:
                payload = json.dumps(all_evaluations).encode()
                # Publish to JetStream
                await self.js.publish("strategies.evaluated", payload)
                # Update KV cache
                await self.kv_ml.put("latest_scores", payload)
                logger.info(f"Avaliações publicadas para {len(all_evaluations)} ativos.")
                
            # Ack the message
            await msg.ack()
        except Exception as e:
            logger.error(f"Erro processando mensagem: {e}")

    async def run(self):
        await self.connect_nats()
        
        # In NATS JetStream we subscribe using push or pull consumer
        # Push consumer is easier for basic pub/sub over streams
        sub = await self.js.subscribe(
            "market.updated",
            durable="STRATEGY_ML_WORKER",
            cb=self.process_market_update
        )
        logger.info("Strategy ML Service aguardando eventos 'market.updated'...")

        while True:
            if self.nc.is_closed:
                logger.warning("Conexão perdida. Reconectando...")
                await self.connect_nats()
            await asyncio.sleep(10)

if __name__ == "__main__":
    service = StrategyMLService()
    asyncio.run(service.run())
