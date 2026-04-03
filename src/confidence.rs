//! Confidence score computation.
//!
//! Combines multiple signals to produce a 0.0–1.0 confidence in the detected edge.
//! Score is used as a gate (>= 0.85 required) and as a Kelly multiplier.

/// Inputs for scoring a potential trade opportunity.
pub struct ConfidenceInput {
    /// Number of real-price sources currently agreeing (1..=3)
    pub source_count: usize,
    /// Spread between highest and lowest real-price source (pct)
    pub source_spread_pct: f64,
    /// How many consecutive ticks have shown the same edge direction
    pub streak: u32,
    /// Current edge magnitude in percentage points
    pub edge_pct: f64,
    /// Polymarket order book depth on the target side (USDC)
    pub book_depth_usdc: f64,
    /// Time since last Binance/TV price update (ms)
    pub price_latency_ms: u64,
}

/// Returns a confidence score in [0.0, 1.0].
pub fn compute_confidence(inp: &ConfidenceInput) -> f64 {
    let mut score = 0.0_f64;
    let mut weight_total = 0.0_f64;

    // 1. Source agreement (30% weight)
    //    1 source = 0.5, 2 = 0.8, 3+ = 1.0
    let source_score = match inp.source_count {
        0 => 0.0,
        1 => 0.5,
        2 => 0.8,
        _ => 1.0,
    };
    score += source_score * 0.30;
    weight_total += 0.30;

    // 2. Source spread (20% weight) — lower spread = more agreement = higher confidence
    //    0% spread = 1.0, ≥0.5% spread = 0.0
    let spread_score = (1.0 - inp.source_spread_pct / 0.5).max(0.0);
    score += spread_score * 0.20;
    weight_total += 0.20;

    // 3. Edge streak (20% weight) — sustained edges are more reliable
    //    0 = 0.2, 3 = 0.7, 5+ = 1.0
    let streak_score = match inp.streak {
        0 => 0.2,
        1 => 0.4,
        2 => 0.6,
        3 => 0.7,
        4 => 0.85,
        _ => 1.0,
    };
    score += streak_score * 0.20;
    weight_total += 0.20;

    // 4. Edge magnitude (15% weight) — bigger edge = more confident
    //    5% = 0.5, 10% = 0.8, 15%+ = 1.0
    let edge_score = (inp.edge_pct / 15.0).min(1.0);
    score += edge_score * 0.15;
    weight_total += 0.15;

    // 5. Book depth (10% weight) — need liquidity to fill
    //    <$100 = 0.0, $1k = 0.5, $5k+ = 1.0
    let depth_score = (inp.book_depth_usdc / 5_000.0).min(1.0);
    score += depth_score * 0.10;
    weight_total += 0.10;

    // 6. Price latency (5% weight) — lower latency = more trust in price
    //    0ms = 1.0, 500ms = 0.0
    let latency_score = (1.0 - inp.price_latency_ms as f64 / 500.0).max(0.0);
    score += latency_score * 0.05;
    weight_total += 0.05;

    // Normalize (shouldn't be needed but guards against floating-point drift)
    if weight_total > 0.0 {
        score / weight_total
    } else {
        0.0
    }
}

/// Implied probability of an UP move given current price vs. a strike.
/// Simple logistic function centered at strike.
pub fn cex_implied_probability(current_price: f64, strike: f64, timeframe_mins: u32) -> f64 {
    // Volatility estimate: ~0.3% per 5-min window for BTC (empirical)
    let vol_per_min = 0.003 / 5.0_f64.sqrt();
    let vol = vol_per_min * (timeframe_mins as f64).sqrt();

    // Z-score of strike vs current price
    let z = (current_price - strike) / (strike * vol);

    // Sigmoid to get probability
    1.0 / (1.0 + (-z).exp())
}
