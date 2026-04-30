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
                
                # KV Store para cache de avaliações
                try:
                    self.kv_ml = await self.js.create_key_value(bucket='ml_evaluations')
                except Exception:
                    self.kv_ml = await self.js.key_value(bucket='ml_evaluations')
                return
            except Exception as e:
                logger.error(f"Erro ao conectar no NATS: {e} - retry em {NATS_RECONNECT_WAIT}s")
                await asyncio.sleep(NATS_RECONNECT_WAIT)

    async def fetch_historical_data(self, symbol, timeframe='1h', limit=100):
        """Busca dados históricos para análise técnica."""
        try:
            ohlcv = await asyncio.to_thread(self.exchange.fetch_ohlcv, symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            return df
        except Exception as e:
            logger.error(f"Erro ao buscar histórico para {symbol}: {e}")
            return None

    def calculate_indicators(self, df):
        """Implementação manual de indicadores técnicos para evitar dependências pesadas na V1."""
        close = df['close']
        
        # 1. RSI (14)
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # 2. EMAs (9 e 21) para Trend Following
        df['ema_9'] = close.ewm(span=9, adjust=False).mean()
        df['ema_21'] = close.ewm(span=21, adjust=False).mean()
        
        # 3. Donchian Channels (20) para Breakout
        df['high_20'] = df['high'].rolling(window=20).max()
        df['low_20'] = df['low'].rolling(window=20).min()
        
        return df

    def run_strategies(self, symbol, tier, df):
        """Executa as 3 estratégias e retorna scores reais (0.0 a 1.0)."""
        df = self.calculate_indicators(df)
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        
        results = []
        
        # --- ESTRATÉGIA 1: TREND FOLLOWING (Cruzamento de Médias) ---
        # Score alto se EMA 9 cruzou acima de EMA 21 ou se está acima dela.
        trend_score = 0.5
        if last_row['ema_9'] > last_row['ema_21']:
            trend_score = 0.8 if prev_row['ema_9'] <= prev_row['ema_21'] else 0.7
        else:
            trend_score = 0.3
            
        results.append({"name": "trend_follower_v1", "score": trend_score, "tier": tier})

        # --- ESTRATÉGIA 2: MEAN REVERSION (RSI + Bandas) ---
        # Score alto se RSI < 30 (Sobre-venda)
        reversion_score = 0.5
        rsi = last_row['rsi']
        if rsi < 30:
            reversion_score = 0.9 # Forte reversão
        elif rsi < 40:
            reversion_score = 0.7
        elif rsi > 70:
            reversion_score = 0.2 # Sobre-comprado
            
        results.append({"name": "mean_reversion_v1", "score": reversion_score, "tier": tier})

        # --- ESTRATÉGIA 3: BREAKOUT (Donchian Channel) ---
        # Score alto se o preço atual está rompendo a máxima de 20 períodos
        breakout_score = 0.5
        if last_row['close'] >= prev_row['high_20']:
            breakout_score = 0.9
        elif last_row['close'] > last_row['ema_9']:
            breakout_score = 0.6
            
        results.append({"name": "breakout_v1", "score": breakout_score, "tier": tier})

        return results

    async def process_market_update(self, msg):
        try:
            assets = json.loads(msg.data.decode())
            logger.info(f"Analisando {len(assets)} ativos recebidos do Market Selection.")
            all_evaluations = []

            for asset in assets:
                symbol = asset['symbol']
                tier = asset.get('tier', 'Unknown')
                
                df = await self.fetch_historical_data(symbol)
                if df is not None and len(df) > 30:
                    evaluations = self.run_strategies(symbol, tier, df)
                    all_evaluations.append({
                        "symbol": symbol,
                        "tier": tier,
                        "strategies": evaluations,
                        "timestamp": asset['timestamp']
                    })

            if all_evaluations:
                payload = json.dumps(all_evaluations).encode()
                await self.js.publish("strategies.evaluated", payload)
                await self.kv_ml.put("latest_scores", payload)
                logger.info(f"Publicadas avaliações reais para {len(all_evaluations)} ativos.")
                
            await msg.ack()
        except Exception as e:
            logger.error(f"Erro no processamento: {e}")

    async def run(self):
        await self.connect_nats()
        
        # Subscribe no canal do Market Selection
        await self.js.subscribe(
            "market.updated",
            durable="STRATEGY_ML_WORKER",
            cb=self.process_market_update,
            manual_ack=True
        )
        logger.info("Strategy ML Service (V1-TA) online e aguardando 'market.updated'...")

        while True:
            if self.nc.is_closed:
                await self.connect_nats()
            await asyncio.sleep(10)

if __name__ == "__main__":
    service = StrategyMLService()
    asyncio.run(service.run())
