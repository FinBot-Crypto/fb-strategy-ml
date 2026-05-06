# fb-strategy-ml

Microserviço de inferência LSTM para Mean Reversion em criptomoedas.

## Objetivo

Consome `market.updated` (fb-market-selection) e executa inferência com **3 modelos LSTM** treinados por tier:
- **Major** (BTC, ETH) — V1
- **Strong Alt** (SOL, MATIC, AVAX, LINK, DOGE, ADA, XRP) — V2
- **High Volatility** (ARB, OP, LDO, ATOM, NEAR, INJ, PEPE, SHIB, MEME, GALA) — V3

Cada modelo retorna score 0-1 = probabilidade do RSI subir em 12h.

## Fluxo

```
market.updated (fb-market-selection, a cada 1h)
  → fb-strategy-ml (event-driven)
    → fetch 15m OHLCV (200 candles)
    → compute RSI features
    → LSTM.predict_proba → score 0-1
    → publish strategies.evaluated
```

## Threshold de Produção

| Condição | Sinal |
|----------|-------|
| RSI < 38 + score >= 0.65 | LONG |

## Arquitetura

```
src/
└── main.py    # NATS subscriber + LSTM inference
```

3 modelos `.pt` em `/app/models/`:
- `model_mean_reversion_v1_lstm_Major.pt`
- `model_mean_reversion_v1_lstm_StrongAlt.pt`
- `model_mean_reversion_v1_lstm_HighVolatility.pt`

## Features do Modelo

- **Input:** 144 candles de 15m (36h de contexto)
- **Features:** rsi_14, rsi_smooth, rsi_14_4h
- **Output:** probabilidade 0-1
- **AUC:** 0.83 (Major), 0.82 (Strong Alt), 0.82 (High Vol)

## Deploy

```bash
docker run -e NATS_URL=nats://crypto-nats:4222 \
  -v crypto_ml_models:/app/models \
  fb-strategy-ml:latest
```

## Modelos

Treinados via `fb-ml-training` no Google Colab com 6400 candles de 15m (~67 dias).
Atualizados via GitHub Actions quando novo modelo é commitado.
