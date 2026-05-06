"""
fb-strategy-ml: Mean Reversion com LSTM.
Carrega 3 modelos (V1/V2/V3), busca dados 15m, computa features RSI,
roda predict_proba e publica score no strategies.evaluated.
"""
import asyncio, logging, os, json, numpy as np, pandas as pd, ccxt, nats, torch
import torch.nn as nn
from nats.js.api import ConsumerConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fb-strategy-ml")

NATS_URL = os.getenv("NATS_URL", "nats://crypto-nats:4222")
MODELS_DIR = os.getenv("MODELS_DIR", "/app/models")
SEQ_LEN = 144
RSI_PERIOD = 56  # 14h em 15m
LOOKAHEAD = 48
MIN_CANDLES = 200  # minimo para criar sequencia

MODEL_FILES = {
    "Major": "model_mean_reversion_v1_lstm_Major.pt",
    "Strong Alt": "model_mean_reversion_v1_lstm_StrongAlt.pt",
    "High Volatility": "model_mean_reversion_v1_lstm_HighVolatility.pt",
}

POST_SCORE_THRESHOLD = float(os.getenv("POST_SCORE_THRESHOLD", "0.3"))


class LSTMMeanReversion(nn.Module):
    def __init__(self, input_size=3, hidden_size=128, num_layers=1):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        return torch.sigmoid(self.fc(lstm_out[:, -1, :]))


class StrategyMLService:
    def __init__(self):
        self.nc = None
        self.js = None
        self.models = {}
        self.exchange = ccxt.binance({"enableRateLimit": True})
        self._load_models()

    def _load_models(self):
        for tier, fname in MODEL_FILES.items():
            path = os.path.join(MODELS_DIR, fname)
            if not os.path.exists(path):
                logger.warning(f"Modelo nao encontrado: {path}")
                continue
            ckpt = torch.load(path, map_location="cpu", weights_only=False)
            cfg = ckpt.get("config", {})
            model = LSTMMeanReversion(3, cfg.get("hidden", 128), cfg.get("layers", 1))
            model.load_state_dict(ckpt["model_state_dict"])
            model.eval()
            self.models[tier] = {"model": model, "seq_len": cfg.get("seq_len", SEQ_LEN)}
            logger.info(f"Modelo carregado: {tier} ({fname})")

    async def connect_nats(self):
        while True:
            try:
                self.nc = await nats.connect(NATS_URL)
                self.js = self.nc.jetstream()
                logger.info(f"NATS conectado: {NATS_URL}")
                return
            except Exception as e:
                logger.error(f"Erro NATS: {e}, retry em 5s")
                await asyncio.sleep(5)

    async def fetch_data(self, symbol: str) -> pd.DataFrame:
        """Busca dados 15m da Binance para criar sequencia LSTM."""
        try:
            ohlcv = await asyncio.to_thread(
                self.exchange.fetch_ohlcv, symbol, "15m", limit=MIN_CANDLES
            )
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            if len(df) < SEQ_LEN:
                logger.error(f"{symbol}: apenas {len(df)} candles, precisa de {SEQ_LEN}")
                return None
            return df
        except Exception as e:
            logger.error(f"Erro fetch {symbol}: {e}")
            return None

    def compute_features(self, df: pd.DataFrame) -> np.ndarray:
        """Computa as 3 features RSI e normaliza."""
        close = df["close"].values

        # RSI (period 56 = 14h em 15m)
        delta = np.diff(close, prepend=close[0])
        gain = np.maximum(delta, 0)
        loss = -np.minimum(delta, 0)
        avg_gain = pd.Series(gain).rolling(RSI_PERIOD).mean().values
        avg_loss = pd.Series(loss).rolling(RSI_PERIOD).mean().values
        rsi_14 = 100 - 100 / (1 + avg_gain / (avg_loss + 1e-10))

        # RSI smooth (EMA 2)
        rsi_smooth = pd.Series(rsi_14).ewm(span=2, adjust=False).mean().values

        # RSI 4h (rolling mean de 16 candles de 15m)
        rsi_4h = pd.Series(rsi_14).rolling(16).mean().values

        # Normalizar com stats aproximados (RSI ~ N(50,10))
        features = np.column_stack([
            (rsi_14 - 50) / 10,
            (rsi_smooth - 50) / 10,
            (rsi_4h - 50) / 10,
        ])
        features = np.nan_to_num(features, nan=0.0)
        return features[-SEQ_LEN:]  # ultimos SEQ_LEN candles

    def predict(self, tier: str, features: np.ndarray) -> float:
        """Prediz probabilidade do RSI subir em 12h."""
        if tier not in self.models:
            return 0.5
        model = self.models[tier]["model"]
        seq_len = self.models[tier]["seq_len"]
        X = torch.from_numpy(features[-seq_len:]).unsqueeze(0).float()  # (1, seq, 3)
        with torch.no_grad():
            proba = model(X).item()
        return round(proba, 4)

    async def process_market_update(self, msg):
        try:
            assets = json.loads(msg.data.decode())
            logger.info(f"Processando {len(assets)} ativos do Market Selection")
            evaluations = []

            for asset in assets:
                symbol = asset["symbol"]
                tier = asset.get("tier", "Major")

                if tier not in self.models:
                    continue

                df = await self.fetch_data(symbol)
                if df is None or len(df) < SEQ_LEN:
                    continue

                features = self.compute_features(df)
                mean_reversion_score = self.predict(tier, features)
                logger.info(f"  {symbol} ({tier}) -> score={mean_reversion_score:.4f}")

                if mean_reversion_score >= POST_SCORE_THRESHOLD:
                    evaluations.append({
                        "symbol": symbol,
                        "tier": tier,
                        "strategies": [
                            {"name": f"mean_reversion_v{tier.replace(' ','_')}", "score": mean_reversion_score, "tier": tier}
                        ],
                        "timestamp": asset.get("timestamp", ""),
                    })

            if evaluations:
                payload = json.dumps(evaluations).encode()
                await self.js.publish("strategies.evaluated", payload)
                logger.info(f"Publicados {len(evaluations)} scores")

            await msg.ack()
        except Exception as e:
            logger.error(f"Erro ao processar: {e}")

    async def run(self):
        await self.connect_nats()
        await self.js.subscribe("market.updated", durable="STRATEGY_ML_WORKER", cb=self.process_market_update, manual_ack=True)
        logger.info(f"fb-strategy-ml (LSTM) online - modelos: {list(self.models.keys())}")
        while True:
            if self.nc.is_closed:
                await self.connect_nats()
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(StrategyMLService().run())
