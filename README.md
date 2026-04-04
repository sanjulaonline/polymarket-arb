# Polymarket Latency Arbitrage Bot v2 — Rust

A production-grade latency arb bot targeting Polymarket BTC 5-minute and
15-minute up/down contracts. Implements every feature from the original Python
prompt spec, rewritten in Rust for sub-100ms execution, and heavily upgraded with Bayesian probability updates and Stoikov-aligned inventory tracking.

---

## Feature Matrix

| Feature | Status |
|---|---|
| BTC 5m and 15m Up/Down contracts | ✅ |
| Binance WebSocket real-time feed | ✅ `wss://stream.binance.com:9443` |
| Binance BTC 1s UP/DOWN signal logs | ✅ `btcusdt@kline_1s` |
| Polymarket live-data Chainlink WS reference feed | ✅ `wss://ws-live-data.polymarket.com` |
| TradingView WebSocket feed (BTC + ETH) | ✅ |
| CryptoQuant WebSocket feed | ✅ |
| Top-of-book parsing uses highest bid + lowest ask | ✅ |
| Execution uses taker ask (not midpoint) | ✅ |
| Wide-spread books (> $0.10) filtered from signal/execution | ✅ |
| Bayesian Posterior Estimation | ✅ `bayesian.rs` |
| Stoikov Inventory Model | ✅ `stoikov.rs` |
| Lag/Edge gate > 5% | ✅ `MIN_EDGE_PCT` |
| Position size < 8% of portfolio | ✅ `MAX_POSITION_PCT` |
| Confidence score gate > 85% | ✅ `MIN_CONFIDENCE` |
| Fractional Kelly position sizing with uncertainty | ✅ `kelly.rs` |
| Paper trading default (3 live flags) | ✅ `LIVE_FLAG_1/2/3` |
| Telegram alerts on every trade | ✅ `telegram.rs` |
| Telegram alerts on drawdown | ✅ |
| Kill switch at 20% daily drawdown | ✅ `DAILY_DRAWDOWN_KILL_PCT` |
| SQLite trade log with full history | ✅ `database.rs` |
| Console-only runtime logs (default) | ✅ |
| Optional terminal dashboard (ENABLE_TUI=true) | ✅ `dashboard.rs` |
| Full error handling + retry logic | ✅ |
| Rate limiting awareness | ✅ |

---

## Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                     Price Feeds (async)                      │
│  Binance WS ──┐                                             │
│  TradingView BTC WS ──┤──► raw_tx broadcast ──► Aggregator │
│  TradingView ETH WS ──┤         (DashMap, staleness guards) │
│  CryptoQuant WS ──────┘                                     │
│  Polymarket REST 50ms poll (configurable) ────────────────► │
└────────────────────────────┬────────────────────────────────┘
                             │ agg_tx broadcast
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                      Detector Loop                           │
│  For each (asset, timeframe) contract slot:                  │
│    1. Pull best real price (Binance > TV > CryptoQuant)     │
│    2. Pull Polymarket mid-probability                        │
│    3. ── BAYESIAN UPDATE ────────────────────────────────── │
│       Feed new price to BayesianEstimator → get P(H|D)       │
│    4. ── STOIKOV EVALUATION ─────────────────────────────── │
│       Compute reservation price r = s - q·γ·σ²·(T-t)         │
│    5. Gate: lag/edge ≥ threshold                             │
│    6. Score confidence (6 signals weighted)                  │
│    7. Gate: edge ≥ 5%, confidence ≥ 85%                     │
│    8. ── KELLY SIZING ───────────────────────────────────── │
│       f* = (b·posterior - q) / b (scaled by inventory)       │
│    9. Risk approval (position cap, daily drawdown)           │
│   10. Paper log OR live FOK order → Record Stoikov Fill     │
└─────────────────┬───────────────────────────────────────────┘
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
   SQLite DB  Telegram   Risk Manager
   (trades)   (alerts)   (kill switch)
        │
        ▼
   TUI Dashboard
   (ratatui — live terminal)
```

---

## Quick Start

### 1. Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
```

### 2. Clone and configure

```bash
git clone <this-repo>
cd polymarket-arb
cp .env.example .env
nano .env   # Fill in your keys
```

Set candle windows with `MARKET_TIMEFRAMES` in `.env`:

```env
MARKET_TIMEFRAMES=5m      # only 5-minute contracts
MARKET_TIMEFRAMES=15m     # only 15-minute contracts
MARKET_TIMEFRAMES=5m,15m  # both (default)
```

Optional fast Binance signal knobs (in `.env`):

```env
ENABLE_BINANCE_SIGNAL_1S=true
BINANCE_SIGNAL_THRESHOLD_PCT=0.01
```

This logs sample-style momentum messages from closed BTC 1s candles:
`[BinanceSignal] BTC 1s UP ...` / `[BinanceSignal] BTC 1s DOWN ...`

Optional Polymarket live Chainlink reference feed knobs (in `.env`):

```env
POLYMARKET_LIVE_WS_URL=wss://ws-live-data.polymarket.com
POLYMARKET_LIVE_SYMBOL_INCLUDES=btc
```

This feed tracks the UI-style current reference price topic (`crypto_prices_chainlink`) and is used as an additional real-price fallback source.

### 3. Get Polymarket API credentials

- Go to https://polymarket.com → Profile → API Keys
- Create a key — you'll get `api_key`, `secret`, `passphrase`
- Set `POLYMARKET_PRIVATE_KEY` to your wallet's private key
- Set `PROXY_WALLET` to the wallet address whose USDC.e balance should size paper bankroll
- Optionally set `POLYGON_RPC_URL` (defaults to `https://polygon.publicnode.com`)
- Optional fallback list: `POLYGON_RPC_FALLBACK_URLS` (comma-separated)

### 4. Build release binary

```bash
cargo build --release
# Takes 1-2 min first build (compiles ratatui, rusqlite etc.)
```

### 5. Run

```bash
./target/release/bot
```

By default the bot runs in console-log mode only.

To enable the terminal dashboard:

```env
ENABLE_TUI=true
```

In TUI mode, press `q`/`Esc` to quit.

---

## Paper vs Live Trading

**Default: Paper mode** — no real orders, all trades simulated and logged.

When paper mode is enabled, startup bankroll source priority is:
1. Polygon on-chain `USDC.e.balanceOf(PROXY_WALLET)` via `eth_call`
2. Polymarket CLOB collateral balance (`/balance-allowance`)
3. `PORTFOLIO_SIZE_USDC` fallback from `.env`

Trade size must also be at least `MIN_TRADE_SIZE_USDC` (default `0.05`).

The Polygon path is the Rust port of the typical ethers.js flow:
`new Contract(USDC).balanceOf(proxyWallet)` with 6 decimals.

If your primary RPC returns auth errors (401/403, disabled API key, etc.),
the bot automatically retries `POLYGON_RPC_FALLBACK_URLS` before falling back
to CLOB/env bankroll sources.

To enable live trading, **all four** conditions must be met in `.env`:

```env
PAPER_TRADING=false
LIVE_FLAG_1=true
LIVE_FLAG_2=true
LIVE_FLAG_3=true
```

This triple-flag design prevents accidental live trading from a single env var typo.
Booleans are parsed case-insensitively (`true/false`, `yes/no`, `1/0`, `on/off`).

---

## Confidence Scoring (confidence.rs)

Each opportunity is scored 0.0–1.0 across 6 signals before the ≥85% gate:

| Signal | Weight | Logic |
|---|---|---|
| Source agreement | 30% | 1 feed = 0.5, 2 = 0.8, 3 = 1.0 |
| Source spread | 20% | 0% spread = 1.0, ≥0.5% = 0.0 |
| Edge streak | 20% | Consecutive ticks with same direction |
| Edge magnitude | 15% | 5% edge = 0.5, 15%+ = 1.0 |
| Book depth | 10% | $5k+ depth = 1.0 |
| Price latency | 5% | 0ms = 1.0, 500ms+ = 0.0 |

Polymarket midpoint is used for signal comparison, but order execution uses
top-of-book ask on the token being bought (taker behavior), consistent with
Polymarket orderbook semantics.

---

## Kelly Criterion (kelly.rs)

Position size uses fractional (half) Kelly:

```
b  = (1 - entry_prob) / entry_prob   # net odds
p  = entry_prob + edge_pct/100       # adjusted win probability
f* = (b*p - q) / b                   # full Kelly fraction
size = portfolio * f* * kelly_fraction * confidence
```

Capped at `MAX_POSITION_PCT` (8%) of portfolio.

---

## Kill Switch

When daily drawdown exceeds `DAILY_DRAWDOWN_KILL_PCT` (default 20%):

1. `AtomicBool` kill switch flips — all new trades blocked immediately
2. Telegram alert sent
3. Resets at midnight UTC automatically

Portfolio exposure is also capped by `MAX_TOTAL_EXPOSURE_PCT` (default 16%),
which limits total notional across all open positions.

---

## Database Schema

```sql
trades (
  id, asset, timeframe, direction, size_usdc,
  entry_prob, cex_prob, edge_pct, confidence, kelly_frac,
  paper, order_id, pnl_usdc, outcome, opened_at, closed_at
)

daily_stats (date, pnl_usdc, trade_count, win_count)
```

Query trades directly: `sqlite3 trades.db "SELECT * FROM trades ORDER BY id DESC LIMIT 20;"`

---

## Project Structure

```
src/
├── main.rs           — task orchestration, startup, shutdown
├── config.rs         — all env-var config with validation + defaults
├── types.rs          — Asset, Timeframe, PriceTick, MarketSnapshot, TradeRecord
├── confidence.rs     — 6-signal confidence scorer + CEX probability model
├── kelly.rs          — fractional Kelly position sizing (with tests)
├── risk.rs           — atomic kill switch, daily drawdown, win-rate tracking
├── bayesian.rs       — Bayesian posterior update from price ticks
├── stoikov.rs        — Inventory-aware reservation price and spread calculation
├── detector.rs       — core strategy loop: Bayesian → Stoikov → Kelly → execute
├── database.rs       — SQLite via rusqlite (WAL mode, insert/close/query)
├── telegram.rs       — Telegram Bot API alerts
├── dashboard.rs      — ratatui TUI: P&L, positions, last 10 trades
├── feeds/
│   ├── mod.rs        — PriceAggregator (multi-source, staleness-aware)
│   ├── binance.rs    — Binance combined stream WS (BTC+ETH)
│   ├── tradingview.rs — TradingView WS (per asset)
│   └── cryptoquant.rs — CryptoQuant WS
└── polymarket/
    ├── mod.rs
    ├── client.rs     — CLOB REST: HMAC auth, order book, FOK orders
     └── poller.rs     — 50ms polling (configurable) → implied price broadcast
```

---

## Risk Warnings

- Polymarket **Terms of Service** prohibit certain automated strategies. Accounts can be banned and wallets blacklisted at the smart contract level.
- Binary prediction markets are **zero-sum** — edge compression happens fast once a strategy is public.
- Paper trade extensively before going live. The default is paper for good reason.
- Not financial advice.
