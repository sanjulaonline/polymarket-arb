use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    // ── Polymarket ────────────────────────────────────────────────────────────
    pub polymarket_api_key: String,
    pub polymarket_api_secret: String,
    pub polymarket_api_passphrase: String,
    pub polymarket_private_key: String,

    // ── CEX feeds ─────────────────────────────────────────────────────────────
    pub cryptoquant_api_key: String,
    pub tradingview_symbol_btc: String,
    pub tradingview_symbol_eth: String,

    // ── Feature toggles ─────────────────────────────────────────────────────
    pub enable_telegram: bool,
    pub enable_database: bool,

    // ── Telegram ──────────────────────────────────────────────────────────────
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,

    // ── Strategy ──────────────────────────────────────────────────────────────
    /// Min lag in percentage points to trigger a scan (3pp default)
    pub lag_threshold_pp: f64,
    /// Min edge % to actually place a trade (5% default)
    pub min_edge_pct: f64,
    /// Max position size as % of portfolio (8% default)
    pub max_position_pct: f64,
    /// Max total open exposure across all active positions as % of portfolio
    pub max_total_exposure_pct: f64,
    /// Min confidence score 0–1 (0.85 default)
    pub min_confidence: f64,
    /// Kelly multiplier — 0.5 = half-Kelly
    pub kelly_fraction: f64,
    /// Daily drawdown kill-switch (20% default)
    pub daily_drawdown_kill_pct: f64,
    /// Risk per trade as % of portfolio (0.5%)
    pub risk_pct_per_trade: f64,
    /// Hard max order size in USDC
    pub max_order_size_usdc: f64,
    /// Portfolio size in USDC (for risk calculations)
    pub portfolio_size_usdc: f64,
    /// Execution timeout in ms
    pub exec_timeout_ms: u64,
    /// Polymarket poll interval in ms
    pub poly_poll_ms: u64,

    // ── Trading mode ──────────────────────────────────────────────────────────
    /// Paper trading — no real orders placed
    pub paper_trading: bool,
    /// Three explicit live flags must all be true to go live
    pub live_flag_1: bool,
    pub live_flag_2: bool,
    pub live_flag_3: bool,

    // ── Bayesian model ────────────────────────────────────────────────────────
    /// How strongly to weight the market's prior (pseudo-observation count)
    /// Higher = slower updates, more trust in Polymarket odds
    pub bayes_prior_strength: f64,
    /// Half-life of evidence decay in seconds (older ticks count less)
    pub bayes_half_life_secs: f64,

    // ── Stoikov model ─────────────────────────────────────────────────────────
    /// Risk-aversion coefficient γ (0.1 = aggressive, 1.0 = conservative)
    pub stoikov_gamma: f64,
    /// Order arrival intensity κ (higher = tighter spread)
    pub stoikov_kappa: f64,
    /// Volatility rolling window (number of ticks)
    pub stoikov_vol_window: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            // Polymarket
            polymarket_api_key: env::var("POLYMARKET_API_KEY")
                .unwrap_or_else(|_| "PAPER_MODE".into()),
            polymarket_api_secret: env::var("POLYMARKET_API_SECRET")
                .unwrap_or_else(|_| "PAPER_MODE".into()),
            polymarket_api_passphrase: env::var("POLYMARKET_API_PASSPHRASE")
                .unwrap_or_else(|_| "PAPER_MODE".into()),
            polymarket_private_key: env::var("POLYMARKET_PRIVATE_KEY")
                .unwrap_or_else(|_| "PAPER_MODE".into()),

            // CEX
            cryptoquant_api_key: env::var("CRYPTOQUANT_API_KEY")
                .unwrap_or_else(|_| String::new()),
            tradingview_symbol_btc: env::var("TV_SYMBOL_BTC")
                .unwrap_or_else(|_| "BINANCE:BTCUSDT".into()),
            tradingview_symbol_eth: env::var("TV_SYMBOL_ETH")
                .unwrap_or_else(|_| "BINANCE:ETHUSDT".into()),

            // Feature toggles
            enable_telegram: parse_bool("ENABLE_TELEGRAM", false),
            enable_database: parse_bool("ENABLE_DATABASE", false),

            // Telegram
            telegram_bot_token: env::var("TELEGRAM_BOT_TOKEN").ok(),
            telegram_chat_id: env::var("TELEGRAM_CHAT_ID").ok(),

            // Strategy
            lag_threshold_pp: parse_f64("LAG_THRESHOLD_PP", 3.0)?,
            min_edge_pct: parse_f64("MIN_EDGE_PCT", 5.0)?,
            max_position_pct: parse_f64("MAX_POSITION_PCT", 8.0)?,
            max_total_exposure_pct: parse_f64("MAX_TOTAL_EXPOSURE_PCT", 16.0)?,
            min_confidence: parse_f64("MIN_CONFIDENCE", 0.85)?,
            kelly_fraction: parse_f64("KELLY_FRACTION", 0.5)?,
            daily_drawdown_kill_pct: parse_f64("DAILY_DRAWDOWN_KILL_PCT", 20.0)?,
            risk_pct_per_trade: parse_f64("RISK_PCT_PER_TRADE", 0.5)?,
            max_order_size_usdc: parse_f64("MAX_ORDER_SIZE_USDC", 500.0)?,
            portfolio_size_usdc: parse_f64("PORTFOLIO_SIZE_USDC", 10_000.0)?,
            exec_timeout_ms: parse_u64("EXEC_TIMEOUT_MS", 5000)?,
            poly_poll_ms: parse_u64("POLY_POLL_MS", 200)?,

            // Live trading — all three must be explicitly set to "true"
            paper_trading: parse_bool("PAPER_TRADING", true),
            live_flag_1: parse_bool("LIVE_FLAG_1", false),
            live_flag_2: parse_bool("LIVE_FLAG_2", false),
            live_flag_3: parse_bool("LIVE_FLAG_3", false),

            // Bayesian defaults
            bayes_prior_strength: parse_f64("BAYES_PRIOR_STRENGTH", 10.0)?,
            bayes_half_life_secs: parse_f64("BAYES_HALF_LIFE_SECS", 30.0)?,

            // Stoikov defaults
            stoikov_gamma: parse_f64("STOIKOV_GAMMA", 0.3)?,
            stoikov_kappa: parse_f64("STOIKOV_KAPPA", 1.5)?,
            stoikov_vol_window: parse_u64("STOIKOV_VOL_WINDOW", 60)? as usize,
        })
    }

    /// Returns true only when all three live flags are set AND paper_trading is false.
    pub fn is_live(&self) -> bool {
        !self.paper_trading && self.live_flag_1 && self.live_flag_2 && self.live_flag_3
    }

    pub fn max_loss_per_trade(&self) -> f64 {
        self.portfolio_size_usdc * self.risk_pct_per_trade / 100.0
    }

    pub fn daily_loss_cap_usdc(&self) -> f64 {
        self.portfolio_size_usdc * self.daily_drawdown_kill_pct / 100.0
    }

    pub fn max_position_usdc(&self) -> f64 {
        self.portfolio_size_usdc * self.max_position_pct / 100.0
    }

    pub fn max_total_exposure_usdc(&self) -> f64 {
        self.portfolio_size_usdc * self.max_total_exposure_pct / 100.0
    }
}

fn parse_f64(key: &str, default: f64) -> Result<f64> {
    match env::var(key) {
        Ok(v) => v.parse().with_context(|| format!("{key} must be a float")),
        Err(_) => Ok(default),
    }
}

fn parse_u64(key: &str, default: u64) -> Result<u64> {
    match env::var(key) {
        Ok(v) => v.parse().with_context(|| format!("{key} must be an integer")),
        Err(_) => Ok(default),
    }
}

fn parse_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => default,
    }
}
