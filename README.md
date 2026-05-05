# 🧠 fb-strategy-ml

Microserviço responsável por avaliar **6 estratégias de ML independentes** em tempo real para predição de movimentos de preço.

## 🎯 Objetivo

O `fb-strategy-ml` consome eventos de ativos selecionados (`market.updated`) e executa avaliações com os 6 modelos treinados:
- **2 Estratégias**: Breakout (rompimento) e Mean Reversion (reversão à média)
- **3 Versões cada**: v1 (Major), v2 (Strong Alt), v3 (High Volatility)

Cada modelo retorna um score (0.0-1.0) indicando a força do sinal para aquela estratégia.

## 🚀 Funcionalidades

- **Integração NATS JetStream**: Consome `market.updated` e publica em `strategies.evaluated`
- **6 Modelos Independentes**: Cada um otimizado para seu tier e estratégia
- **Avaliação Assíncrona**: Executa inferências em paralelo via asyncio
- **Cache de Avaliações**: Persiste últimos scores no NATS KV Store (`ml_evaluations`)
- **Robustez**: Tratamento de erros, retry automático, logging detalhado

## 📊 Modelos Utilizados

| Modelo | Estratégia | Tier | Período | Dados de Treino |
|--------|-----------|------|--------|-----------------|
| **breakout_v1** | Breakout | Major | Donchian 15 | BTC, ETH |
| **breakout_v2** | Breakout | Strong Alt | Donchian 20 | SOL, MATIC, AVAX, LINK, DOGE, ADA, XRP |
| **breakout_v3** | Breakout | High Vol | Donchian 30 | ARB, OP, LDO, ATOM, NEAR, INJ, PEPE, SHIB, MEME, GALA |
| **mean_reversion_v1** | Mean Reversion | Major | SMA 20 | BTC, ETH |
| **mean_reversion_v2** | Mean Reversion | Strong Alt | SMA 30 | SOL, MATIC, AVAX, LINK, DOGE, ADA, XRP |
| **mean_reversion_v3** | Mean Reversion | High Vol | SMA 40 | ARB, OP, LDO, ATOM, NEAR, INJ, PEPE, SHIB, MEME, GALA |

## 🔄 Fluxo de Dados

```
market.updated (from fb-market-selection)
    ↓
assets: [
  {symbol: "BTC/USDT", tier: "Major"},
  {symbol: "SOL/USDT", tier: "Strong Alt"},
  {symbol: "ARB/USDT", tier: "High Volatility"}
]
    ↓
Para cada ativo:
  ├─ Se tier=Major: avaliar com breakout_v1 + mean_reversion_v1
  ├─ Se tier=Strong Alt: avaliar com breakout_v2 + mean_reversion_v2
  └─ Se tier=High Vol: avaliar com breakout_v3 + mean_reversion_v3
    ↓
strategies.evaluated (published)
    ↓
[
  {
    "symbol": "BTC/USDT",
    "tier": "Major",
    "strategies": [
      {"name": "breakout_v1", "score": 0.8},
      {"name": "mean_reversion_v1", "score": 0.5}
    ]
  },
  ...
]
```

## 🎯 Scores Esperados

Cada estratégia retorna um score (0.0-1.0):
- **0.0-0.3**: Sinal fraco ou ausente
- **0.3-0.6**: Sinal moderado (cautela)
- **0.6-0.8**: Sinal forte (operação viável)
- **0.8-1.0**: Sinal muito forte (alta confiança)

Exemplo:
```json
{
  "symbol": "BTC/USDT",
  "tier": "Major",
  "strategies": [
    {
      "name": "breakout_v1",
      "score": 0.85,
      "description": "Preço rompeu máxima de 15 candles + RSI > 50"
    },
    {
      "name": "mean_reversion_v1",
      "score": 0.3,
      "description": "RSI não está em sobre-venda"
    }
  ]
}
```

## 🏗️ Arquitetura

```
src/
├── main.py                      # Entry point: listener NATS
├── config.py                    # Thresholds e constantes globais
│
├── strategies/                  # INFERÊNCIA DE CADA ESTRATÉGIA
│   ├── breakout.py             # Avalia breakout_v1, v2, v3
│   └── mean_reversion.py       # Avalia mean_reversion_v1, v2, v3
│
├── models/                      # GESTÃO DOS MODELOS TREINADOS
│   ├── loader.py               # Carrega os 6 .joblib
│   └── cache.py                # Cache em memória
│
└── shared/                      # CÓDIGO REUTILIZÁVEL
    ├── indicators.py           # RSI, ATR, Donchian, Bollinger
    ├── data_fetcher.py         # Binance API
    └── utils.py                # Helpers diversos
```

## 🔑 Variáveis de Ambiente

| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `NATS_URL` | Endereço do servidor NATS | `nats://crypto-nats:4222` |
| `MODELS_DIR` | Diretório com modelos .joblib | `/app/models` |
| `DATA_FETCH_TIMEOUT` | Timeout ao buscar dados (segundos) | `10` |

## 📖 Como Funciona

1. **Inicialização**: Carrega os 6 modelos (.joblib) do `MODELS_DIR`
2. **Listener**: Aguarda mensagens em `market.updated`
3. **Processamento**:
   - Para cada ativo recebido, identifica seu tier
   - Busca últimos 100 candles de 1h
   - Calcula indicators (RSI, ATR, Donchian, Bollinger)
   - Realiza predição com modelos apropriados
   - Converte predições em scores (0.0-1.0)
4. **Publicação**: Publica resultado em `strategies.evaluated`
5. **Cache**: Atualiza KV Store para dashboard

## 🚀 Deploy

```bash
# Build
docker build -t fb-strategy-ml:latest .

# Run
docker run \
  -e NATS_URL=nats://crypto-nats:4222 \
  -v $(pwd)/models:/app/models \
  fb-strategy-ml:latest
```

## 📝 Mudanças da V1

- ❌ **Removido**: `trend_follower_v1` (não mais utilizado)
- ✅ **Adicionado**: Versões v2 e v3 de Breakout e Mean Reversion
- ✅ **Melhorado**: Separação clara de tier → versão
- ✅ **Robusto**: Tratamento de erros, retry automático

---

*FinBot-Crypto - ML Intelligence Layer (Enterprise-Grade)*
