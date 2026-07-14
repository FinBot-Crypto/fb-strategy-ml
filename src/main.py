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

SHORT_MODEL_FILES = {
    "Major": "model_short_lstm_Major.pt",
    "Strong Alt": "model_short_lstm_StrongAlt.pt",
    "High Volatility": "model_short_lstm_HighVolatility.pt",
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
        self.short_models = {}
        self.exchange = ccxt.binance({"enableRateLimit": True})
        self._load_models()

    def _load_models(self):
        for tier, fname in MODEL_FILES.items():
            path = os.path.join(MODELS_DIR, fname)
            if not os.path.exists(path):
                logger.warning(f"Modelo LONG nao encontrado: {path}")
                continue
            ckpt = torch.load(path, map_location="cpu", weights_only=False)
            cfg = ckpt.get("config", {})
            nf = cfg.get("n_features", 3)
            model = LSTMMeanReversion(nf, cfg.get("hidden", 128), cfg.get("layers", 1))
            model.load_state_dict(ckpt["model_state_dict"])
            model.eval()
            self.models[tier] = {"model": model, "seq_len": cfg.get("seq_len", SEQ_LEN), "n_features": nf}
            logger.info(f"Modelo LONG carregado: {tier} ({fname}) nf={nf}")

        for tier, fname in SHORT_MODEL_FILES.items():
            path = os.path.join(MODELS_DIR, fname)
            if not os.path.exists(path):
                logger.warning(f"Modelo SHORT nao encontrado: {path}")
                continue
            ckpt = torch.load(path, map_location="cpu", weights_only=False)
            cfg = ckpt.get("config", {})
            nf = cfg.get("n_features", 3)
            model = LSTMMeanReversion(nf, cfg.get("hidden", 128), cfg.get("layers", 1))
            model.load_state_dict(ckpt["model_state_dict"])
            model.eval()
            self.short_models[tier] = {"model": model, "seq_len": cfg.get("seq_len", SEQ_LEN), "n_features": nf}
            logger.info(f"Modelo SHORT carregado: {tier} ({fname}) nf={nf}")

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
        """Computa 12 features: 3 RSI + BTC RSI + 4 BTC SMA + 2 funding + 2 OI."""
        close = df["close"].values
        n = len(close)

        # RSI features (3)
        delta = np.diff(close, prepend=close[0])
        gain = np.maximum(delta, 0)
        loss = -np.minimum(delta, 0)
        avg_gain = pd.Series(gain).rolling(RSI_PERIOD).mean().values
        avg_loss = pd.Series(loss).rolling(RSI_PERIOD).mean().values
        rsi_14 = 100 - 100 / (1 + avg_gain / (avg_loss + 1e-10))
        rsi_smooth = pd.Series(rsi_14).ewm(span=2, adjust=False).mean().values
        rsi_4h = pd.Series(rsi_14).rolling(16).mean().values

        feats = np.column_stack([
            (rsi_14 - 50) / 10,
            (rsi_smooth - 50) / 10,
            (rsi_4h - 50) / 10,
        ])

        # BTC features (5): RSI + 4 SMA ratios
        btc_feats = np.zeros((n, 5))
        try:
            btc_ohlcv = self.exchange.fetch_ohlcv("BTC/USDT", "1h", limit=60)
            if btc_ohlcv and len(btc_ohlcv) >= 50:
                btc_closes = np.array([c[4] for c in btc_ohlcv])
                btc_current = btc_closes[-1]
                # BTC RSI
                btc_delta = np.diff(btc_closes, prepend=btc_closes[0])
                btc_g = np.maximum(btc_delta, 0)
                btc_l = -np.minimum(btc_delta, 0)
                btc_ag = pd.Series(btc_g).rolling(14).mean().values
                btc_al = pd.Series(btc_l).rolling(14).mean().values
                btc_rsi = 100 - 100 / (1 + btc_ag / (btc_al + 1e-10))
                btc_feats[:, 0] = (btc_rsi[-1] - 50) / 10
                # BTC SMA ratios
                for j, p in enumerate([12, 24, 36, 48]):
                    if len(btc_closes) >= p:
                        btc_feats[:, j + 1] = btc_current / max(btc_closes[-p:].mean(), 1)
        except Exception:
            pass
        feats = np.hstack([feats, btc_feats])

        # Funding + OI (4 features, zeros if unavailable)
        extra = np.zeros((n, 4))
        try:
            if not hasattr(self, '_futures_ex'):
                self._futures_ex = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})
            # Funding rate
            fr_data = self._futures_ex.fetch_funding_rate_history(symbol, limit=2)
            if fr_data and len(fr_data) >= 2:
                fr = float(fr_data[-1].get('fundingRate', 0))
                fr_prev = float(fr_data[-2].get('fundingRate', 0))
                extra[:, 0] = fr * 10000
                extra[:, 1] = (fr - fr_prev) * 10000
            # Open interest (1h + 24h)
            oi_data_1h = self.exchange.fetch_open_interest_history(symbol, "1h", limit=2)
            oi_data_24h = self.exchange.fetch_open_interest_history(symbol, "1h", limit=25)
            if oi_data_1h and len(oi_data_1h) >= 2:
                oi = float(oi_data_1h[-1].get('openInterestAmount', 0))
                oi_prev = float(oi_data_1h[-2].get('openInterestAmount', 0))
                extra[:, 2] = (oi / max(oi_prev, 1) - 1) * 100
            if oi_data_24h and len(oi_data_24h) >= 25:
                oi = float(oi_data_24h[-1].get('openInterestAmount', 0))
                oi_24h_ago = float(oi_data_24h[-25].get('openInterestAmount', 0))
                extra[:, 3] = (oi / max(oi_24h_ago, 1) - 1) * 100
        except Exception:
            pass
        feats = np.hstack([feats, extra])

        feats = np.nan_to_num(feats, nan=0.0)
        return feats[-SEQ_LEN:]

    def predict(self, tier: str, features: np.ndarray) -> float:
        if tier not in self.models:
            return 0.5
        m = self.models[tier]
        nf = m.get("n_features", 3)
        seq_len = m["seq_len"]
        feats = features[-seq_len:, :nf]
        X = torch.from_numpy(feats).unsqueeze(0).float()
        with torch.no_grad():
            proba = m["model"](X).item()
        return round(proba, 4)

    def predict_short(self, tier: str, features: np.ndarray) -> float:
        if tier not in self.short_models:
            return 0.5
        m = self.short_models[tier]
        nf = m.get("n_features", 3)
        seq_len = m["seq_len"]
        feats = features[-seq_len:, :nf]
        X = torch.from_numpy(feats).unsqueeze(0).float()
        with torch.no_grad():
            proba = m["model"](X).item()
        return round(proba, 4)

    async def process_market_update(self, msg):
        try:
            data = json.loads(msg.data.decode())
            if isinstance(data, dict):
                assets = data.get("assets", [])
                btc_trend = data.get("btc_trend", "neutral")
            else:
                assets = data
                btc_trend = "neutral"
            logger.info(f"Processando {len(assets)} ativos [BTC: {btc_trend}]")
            evaluations = []

            for asset in assets:
                symbol = asset["symbol"]
                tier = asset.get("tier", "Major")

                has_long = tier in self.models
                has_short = tier in self.short_models
                if not has_long and not has_short:
                    continue

                df = await self.fetch_data(symbol)
                if df is None or len(df) < SEQ_LEN:
                    continue

                features = self.compute_features(df)
                long_score = self.predict(tier, features) if has_long else None
                short_score = self.predict_short(tier, features) if has_short else None

                strategies = []
                if long_score is not None:
                    strategies.append({"name": "mean_reversion_long", "score": long_score, "direction": "LONG"})
                if short_score is not None:
                    strategies.append({"name": "mean_reversion_short", "score": short_score, "direction": "SHORT"})

                logger.info(f"  {symbol} ({tier}) -> long={long_score} short={short_score}")

                posts = long_score is not None and long_score >= POST_SCORE_THRESHOLD
                posts = posts or (short_score is not None and short_score >= POST_SCORE_THRESHOLD)
                if posts:
                    evaluations.append({
                        "symbol": symbol,
                        "tier": tier,
                        "strategies": strategies,
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
        self._processing = False  # Guard against concurrent executions
        await self.js.subscribe(
            "market.updated",
            durable="STRATEGY_ML_WORKER",
            cb=self.process_market_update,
            manual_ack=True,
            pending_msgs_limit=512,
            pending_bytes_limit=64 * 1024 * 1024  # 64 MB
        )
        logger.info(f"fb-strategy-ml (LSTM) online - LONG: {list(self.models.keys())} | SHORT: {list(self.short_models.keys())}")
        while True:
            if self.nc.is_closed:
                await self.connect_nats()
            await asyncio.sleep(10)



if __name__ == "__main__":
    asyncio.run(StrategyMLService().run())
