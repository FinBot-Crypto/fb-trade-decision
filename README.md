# fb-trade-decision

Calcula position sizing, Stop Loss e Take Profit para cada oportunidade de trade.

## Fluxo

```
trade.opportunity (fb-decision-engine)
  → fb-trade-decision
    → fetch balance USDT
    → position_size = (RISK_PERCENT × capital) / (SL_ATR × ATR)
    → SL = entry - max(SL_ATR × ATR, 1% do preço)
    → TP = entry + max(TP_ATR × ATR, 2% do preço)
    → trade.order
```

## Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `NATS_URL` | `nats://crypto-nats:4222` | Servidor NATS |
| `RISK_PERCENT` | `0.05` | Risco por trade (5% do capital) |
| `SL_ATR` | `2.0` | Multiplicador do ATR para Stop Loss |
| `TP_ATR` | `4.0` | Multiplicador do ATR para Take Profit |
| `MIN_SIZE_USDT` | `15.0` | Tamanho mínimo da posição em USDT |
| `BINANCE_API_KEY` | | Chave API Binance |
| `BINANCE_API_SECRET` | | Secret API Binance |

## Deploy

```bash
docker run -e NATS_URL=nats://crypto-nats:4222 \
  -e BINANCE_API_KEY=xxx -e BINANCE_API_SECRET=xxx \
  fb-trade-decision:latest
```
