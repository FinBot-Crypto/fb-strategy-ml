# 🧠 fb-strategy-ml

Microserviço responsável por analisar dados históricos de mercado e calcular scores de probabilidade para múltiplas estratégias de trading (V1-TA: Technical Analysis).

## 🎯 Objetivo
O `fb-strategy-ml` atua como o motor analítico do ecossistema. Ele consome os ativos selecionados pelo `fb-market-selection` e, para cada um, realiza uma análise técnica profunda para determinar a força de diferentes sinais operacionais.

## 🚀 Funcionalidades
- **Integração NATS JetStream**: Consome eventos de `market.updated` e publica em `strategies.evaluated`.
- **Análise Multi-Estratégia**: Implementa simultaneamente 3 lógicas principais:
  - **Trend Follower**: Baseado em cruzamento de médias móveis exponenciais (EMA 9/21).
  - **Mean Reversion**: Identificação de sobre-venda/sobre-compra via RSI.
  - **Breakout**: Rompimento de canais de volatilidade (Donchian Channels).
- **Consumo Assíncrono**: Utiliza `asyncio` e `to_thread` para garantir alta performance no fetching de dados da Binance.
- **Cache de Avaliações**: Persiste os últimos scores no NATS KV Store (`ml_evaluations`) para auditoria e interface.

## 🔄 Fluxo CI/CD
1. **Push para `main`**: Dispara o workflow de deploy centralizado.
2. **Build Docker**: O GitHub Actions constrói a imagem e valida as dependências.
3. **Deploy via SSH**: A imagem é atualizada na Oracle Cloud VPS.
4. **Orquestração**: O `docker-compose` reinicia apenas este serviço (`--no-deps`), garantindo zero downtime para o restante da infra.

## 🔑 Variáveis e Secrets Necessárias
| Nome | Descrição | Local |
|------|-----------|-------|
| `NATS_URL` | Endereço do servidor NATS (ex: `nats://crypto-nats:4222`) | `.env` / Docker |
| `VPS_SSH_HOST` | IP da Oracle VPS | GitHub Secrets |
| `VPS_SSH_USER` | Usuário de acesso (root) | GitHub Secrets |
| `VPS_SSH_KEY` | Chave privada SSH | GitHub Secrets |

## 🏗️ Infraestrutura Utilizada
- **NATS JetStream**: Para recebimento confiável de mensagens (Streaming).
- **NATS KV Store**: Bucket `ml_evaluations` para armazenamento persistente dos scores.
- **CCXT Library**: Integração com a API pública da Binance.
- **Pandas/Numpy**: Processamento vetorial de séries temporais.

## 📡 Simulação de Output (NATS)
```json
{
  "symbol": "BTC/USDT",
  "tier": "Major",
  "strategies": [
    { "name": "trend_follower_v1", "score": 0.7, "tier": "Major" },
    { "name": "mean_reversion_v1", "score": 0.5, "tier": "Major" },
    { "name": "breakout_v1", "score": 0.6, "tier": "Major" }
  ],
  "timestamp": "2026-04-30T19:44:00"
}
```

---
*FinBot-Crypto - ML Intelligence Layer*
