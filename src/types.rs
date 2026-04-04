use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── Asset ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Asset {
    Btc,
    Eth,
}

impl std::fmt::Display for Asset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Asset::Btc => write!(f, "BTC"),
            Asset::Eth => write!(f, "ETH"),
        }
    }
}

// ── Contract timeframe ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Timeframe {
    FiveMin,
    FifteenMin,
}

impl std::fmt::Display for Timeframe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Timeframe::FiveMin => write!(f, "5m"),
            Timeframe::FifteenMin => write!(f, "15m"),
        }
    }
}

// ── Price sources ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PriceSource {
    Binance,
    TradingView,
    CryptoQuant,
    Polymarket,
    PolymarketWs,
}

impl std::fmt::Display for PriceSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PriceSource::Binance => write!(f, "Binance"),
            PriceSource::TradingView => write!(f, "TradingView"),
            PriceSource::CryptoQuant => write!(f, "CryptoQuant"),
            PriceSource::Polymarket => write!(f, "Polymarket"),
            PriceSource::PolymarketWs => write!(f, "PolymarketWS"),
        }
    }
}

// ── Price tick ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PriceTick {
    pub source: PriceSource,
    pub asset: Asset,
    /// Optional contract timeframe discriminator (used by Polymarket 5m vs 15m ticks)
    pub timeframe: Option<Timeframe>,
    /// Optional notional depth snapshot (USDC), primarily for Polymarket books.
    pub book_depth_usdc: Option<f64>,
    /// Optional top-of-book best bid probability (Polymarket only).
    pub book_best_bid_prob: Option<f64>,
    /// Optional top-of-book best ask probability (Polymarket only).
    pub book_best_ask_prob: Option<f64>,
    pub price: f64,
    pub timestamp: DateTime<Utc>,
}

// ── Model output — carries all three model results through the pipeline ───────

#[derive(Debug, Clone, Default)]
pub struct ModelOutput {
    // ── Bayesian ──────────────────────────────────────────────────────────────
    /// Posterior probability P(H|D) — updated on every price tick
    pub bayesian_posterior: f64,
    /// 90% credible interval lower bound
    pub ci_lo: f64,
    /// 90% credible interval upper bound
    pub ci_hi: f64,
    /// Posterior variance (uncertainty)
    pub posterior_variance: f64,
    /// Effective observation count
    pub effective_n: f64,
    /// Whether the Bayesian model is warmed up
    pub bayes_ready: bool,

    // ── Stoikov ───────────────────────────────────────────────────────────────
    /// Inventory-adjusted fair value r = s - q·γ·σ²·(T-t)
    pub reservation_price: f64,
    /// Optimal half-spread δ/2
    pub half_spread: f64,
    /// Optimal bid (reservation_price - half_spread)
    pub stoikov_bid: f64,
    /// Optimal ask (reservation_price + half_spread)
    pub stoikov_ask: f64,
    /// Normalised inventory q ∈ [-1, 1]
    pub inventory_q: f64,
    /// Current price variance σ²
    pub variance: f64,
    /// Whether Stoikov is warmed up
    pub stoikov_ready: bool,

    // ── Kelly ─────────────────────────────────────────────────────────────────
    /// Final recommended position size in USDC
    pub kelly_size_usdc: f64,
    /// Raw full-Kelly fraction (diagnostic)
    pub full_kelly_fraction: f64,
    /// Whether Kelly sees positive edge
    pub kelly_has_edge: bool,
}

// ── Market snapshot ───────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub asset: Asset,
    pub timeframe: Timeframe,
    pub real_price: f64,
    pub poly_implied_prob: f64, // Polymarket mid (raw market odds)
    pub poly_entry_prob: f64,   // Estimated taker entry probability (buy at ask)
    pub cex_implied_prob: f64,  // CEX-implied probability (logistic model)
    pub edge_pct: f64,          // |bayesian_posterior - poly_prob| * 100
    pub confidence: f64,        // multi-signal score 0–1
    pub direction_up: bool,
    pub timestamp: DateTime<Utc>,
    /// Outputs from all three models
    pub models: ModelOutput,
}

impl MarketSnapshot {
    pub fn new(
        asset: Asset,
        timeframe: Timeframe,
        real_price: f64,
        poly_prob: f64,
        cex_prob: f64,
        confidence: f64,
    ) -> Self {
        let edge_pct = (cex_prob - poly_prob).abs() * 100.0;
        let direction_up = cex_prob > poly_prob;
        Self {
            asset,
            timeframe,
            real_price,
            poly_implied_prob: poly_prob,
            poly_entry_prob: poly_prob,
            cex_implied_prob: cex_prob,
            edge_pct,
            confidence,
            direction_up,
            timestamp: Utc::now(),
            models: ModelOutput::default(),
        }
    }

    /// Edge computed from Bayesian posterior vs market.
    pub fn bayesian_edge_pct(&self) -> f64 {
        (self.models.bayesian_posterior - self.poly_implied_prob).abs() * 100.0
    }

    /// Use the Bayesian-derived edge if warmed up, else fall back to CEX edge.
    pub fn effective_edge_pct(&self) -> f64 {
        if self.models.bayes_ready && self.models.stoikov_ready {
            self.bayesian_edge_pct()
        } else {
            self.edge_pct
        }
    }

    pub fn has_edge(&self, min_edge_pct: f64, min_confidence: f64) -> bool {
        self.effective_edge_pct() >= min_edge_pct && self.confidence >= min_confidence
    }
}

// ── Trade record (for DB + dashboard) ────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub id: Option<i64>,
    pub asset: String,
    pub timeframe: String,
    pub direction: String,         // "UP" | "DOWN"
    pub size_usdc: f64,
    pub entry_prob: f64,
    pub cex_prob: f64,
    pub edge_pct: f64,
    pub confidence: f64,
    pub kelly_fraction: f64,
    pub paper: bool,
    pub order_id: Option<String>,
    pub pnl_usdc: Option<f64>,
    pub outcome: Option<String>,   // "WIN" | "LOSS" | "OPEN"
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
}

impl TradeRecord {
    pub fn is_open(&self) -> bool {
        self.outcome.as_deref() == Some("OPEN") || self.outcome.is_none()
    }
}
