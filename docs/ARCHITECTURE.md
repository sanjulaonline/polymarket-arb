# Architecture Overview

This document describes the core runtime pipeline and major modules.

## Runtime flow

1. Feed tasks stream market/oracle data into a shared broadcast channel.
2. Aggregator stores latest ticks keyed by source + asset + timeframe.
3. Detector evaluates strategy conditions and emits trade intents.
4. Risk manager approves/rejects sizing and enforces kill-switch limits.
5. Executor places paper/live orders and persists trade records.
6. Settlement loop resolves open positions and records PnL.

## Key modules

- `src/main.rs`: startup, wiring, task orchestration
- `src/feeds/`: Binance, TradingView, CryptoQuant, Polymarket live WS
- `src/polymarket/`: CLOB client and orderbook poller
- `src/detector.rs`: signal evaluation logic
- `src/risk.rs`: guardrails, drawdown limits, exposure caps
- `src/database.rs`: SQLite persistence
- `src/telegram.rs`: alerting

## Reliability notes

- Feed reconnect logic includes backoff.
- Polymarket live WS includes stale stream detection + forced reconnect.
- Paper mode is default and requires explicit flags to enable live trading.
