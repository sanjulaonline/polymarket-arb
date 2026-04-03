/// Fractional Kelly Criterion — position sizing
/// =============================================
///
/// Kelly formula (binary markets):
///
///   f* = (b·p − q) / b
///
/// where:
///   p = our probability of winning       ← Bayesian posterior P(H|D)
///   q = 1 − p (probability of losing)
///   b = net odds = (1 − entry_price) / entry_price
///       (if we pay 0.40 per share → b = 0.60/0.40 = 1.5)
///
/// We apply three scaling factors before converting to dollars:
///   1. `kelly_fraction` (e.g. 0.5 = half-Kelly) — reduces variance
///   2. Bayesian uncertainty penalty — scales by (1 − posterior_variance·k)
///   3. Stoikov inventory adjustment — reduces size when already exposed
///
/// The result is clamped to [0, max_position_usdc].

/// Full Kelly inputs — all three models feed into here.
#[derive(Debug, Clone)]
pub struct KellyInput {
    /// Bayesian posterior P(H|D) — probability of our hypothesis being correct
    pub posterior: f64,
    /// Polymarket's entry price (the price we'd pay per share)
    pub entry_price: f64,
    /// Fractional Kelly multiplier (0.5 = half-Kelly)
    pub kelly_fraction: f64,
    /// Total portfolio in USDC
    pub portfolio_usdc: f64,
    /// Hard cap on a single position
    pub max_position_usdc: f64,
    /// Stoikov inventory signal: q ∈ [-1, 1]
    /// Positive = already long the same direction → reduce size
    /// Negative = already short → can increase
    pub inventory_q: f64,
    /// Posterior variance from Bayesian — used as uncertainty penalty
    pub posterior_variance: f64,
    /// Effective sample count — low N → additional penalty
    pub effective_n: f64,
}

/// Kelly output with full diagnostic breakdown.
#[derive(Debug, Clone)]
pub struct KellyOutput {
    /// Recommended position size in USDC
    pub size_usdc: f64,
    /// Full Kelly fraction (before any scaling)
    pub full_kelly: f64,
    /// After kelly_fraction applied
    pub fractional_kelly: f64,
    /// After uncertainty penalty
    pub uncertainty_penalised: f64,
    /// After inventory adjustment
    pub inventory_adjusted: f64,
    /// Net odds b
    pub net_odds: f64,
    /// Whether there's a positive edge at all
    pub has_edge: bool,
}

/// Compute the Kelly-optimal position size using the Bayesian posterior.
///
/// This is the correct way to use Kelly: feed it the *posterior* probability,
/// not the naive CEX-implied probability or the market price. The posterior
/// already incorporates the market's information AND our new data.
pub fn kelly_size(inp: &KellyInput) -> KellyOutput {
    let no_edge = KellyOutput {
        size_usdc: 0.0, full_kelly: 0.0, fractional_kelly: 0.0,
        uncertainty_penalised: 0.0, inventory_adjusted: 0.0,
        net_odds: 0.0, has_edge: false,
    };

    let entry = inp.entry_price;
    if entry <= 0.0 || entry >= 1.0 { return no_edge; }

    // Net odds on a binary contract:
    // Pay `entry` per share, win (1 - entry) if correct, lose entry if wrong.
    let b = (1.0 - entry) / entry;

    let p = inp.posterior.clamp(0.001, 0.999);
    let q = 1.0 - p;

    // Full Kelly: f* = (b·p − q) / b
    let full_kelly = (b * p - q) / b;
    if full_kelly <= 0.0 { return no_edge; } // no positive edge

    // ── Step 1: fractional Kelly ─────────────────────────────────────────────
    let fractional = full_kelly * inp.kelly_fraction;

    // ── Step 2: uncertainty penalty ──────────────────────────────────────────
    // High posterior variance = we're less certain → smaller position
    // Penalty factor: 1 - k·σ²_posterior, clamped to [0.1, 1.0]
    // k = 20.0 chosen so that σ² = 0.05 → 0x penalty (50% reduction)
    let uncertainty_factor = (1.0 - 20.0 * inp.posterior_variance)
        .clamp(0.1, 1.0);

    // Additional penalty for low effective N (not enough data yet)
    // Scales from 0.3 (N=1) to 1.0 (N≥20)
    let n_factor = (inp.effective_n / 20.0).min(1.0).max(0.3);

    let uncertainty_penalised = fractional * uncertainty_factor * n_factor;

    // ── Step 3: Stoikov inventory adjustment ─────────────────────────────────
    // If we're already long (q > 0) and want to buy more long → scale down
    // If we're already short (q < 0) and want to go short more → scale down
    // The inventory_q from Stoikov is in [-1, 1]; positive = long exposure.
    //
    // Reduction factor: max(0.1, 1 - |q|)
    // At q=0 (flat): no reduction
    // At q=0.5 (half max long): 50% reduction
    // At q=1.0 (max long): 90% reduction
    let inv_adj = (1.0 - inp.inventory_q.abs()).max(0.1);
    let inventory_adjusted = uncertainty_penalised * inv_adj;

    // ── Final: convert to USDC ────────────────────────────────────────────────
    let raw_usdc = inp.portfolio_usdc * inventory_adjusted;
    let size_usdc = raw_usdc.min(inp.max_position_usdc).max(0.0);

    KellyOutput {
        size_usdc,
        full_kelly,
        fractional_kelly: fractional,
        uncertainty_penalised,
        inventory_adjusted,
        net_odds: b,
        has_edge: true,
    }
}

/// Convenience wrapper — old-style call for backward compatibility.
pub fn kelly_position_size(
    edge_pct: f64,
    confidence: f64,
    entry_prob: f64,
    kelly_fraction: f64,
    portfolio_usdc: f64,
    max_position_usdc: f64,
) -> f64 {
    if entry_prob <= 0.0 || entry_prob >= 1.0 { return 0.0; }
    let b = (1.0 - entry_prob) / entry_prob;
    let p = (entry_prob + edge_pct / 100.0).clamp(0.001, 0.999);
    let q = 1.0 - p;
    let full_kelly = (b * p - q) / b;
    if full_kelly <= 0.0 { return 0.0; }
    let adjusted = full_kelly * kelly_fraction * confidence;
    (portfolio_usdc * adjusted).min(max_position_usdc).max(0.0)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn base_input(posterior: f64, entry: f64) -> KellyInput {
        KellyInput {
            posterior,
            entry_price: entry,
            kelly_fraction: 0.5,
            portfolio_usdc: 10_000.0,
            max_position_usdc: 1_000.0,
            inventory_q: 0.0,
            posterior_variance: 0.01,
            effective_n: 20.0,
        }
    }

    #[test]
    fn test_positive_edge() {
        // Posterior 60%, market at 50% → clear edge
        let out = kelly_size(&base_input(0.60, 0.50));
        assert!(out.has_edge, "Should detect edge");
        assert!(out.size_usdc > 0.0);
        assert!(out.size_usdc <= 1_000.0);
    }

    #[test]
    fn test_no_edge_at_fair() {
        // Posterior equals entry price → no edge
        let out = kelly_size(&base_input(0.50, 0.50));
        assert!(!out.has_edge, "No edge when posterior = entry");
        assert_eq!(out.size_usdc, 0.0);
    }

    #[test]
    fn test_inventory_reduces_size() {
        let mut inp_flat = base_input(0.65, 0.50);
        let mut inp_long = base_input(0.65, 0.50);
        inp_long.inventory_q = 0.8; // heavily long

        let flat = kelly_size(&inp_flat);
        let loaded = kelly_size(&inp_long);

        assert!(
            loaded.size_usdc < flat.size_usdc,
            "Heavy inventory should reduce position size"
        );
    }

    #[test]
    fn test_high_variance_reduces_size() {
        let mut low_var = base_input(0.60, 0.50);
        let mut high_var = base_input(0.60, 0.50);
        high_var.posterior_variance = 0.04;

        let lo = kelly_size(&low_var);
        let hi = kelly_size(&high_var);

        assert!(
            hi.size_usdc <= lo.size_usdc,
            "High variance should produce smaller or equal position"
        );
    }

    #[test]
    fn test_size_never_exceeds_cap() {
        // Extreme edge — should still be capped
        let out = kelly_size(&base_input(0.99, 0.01));
        assert!(out.size_usdc <= 1_000.0, "Must never exceed max_position_usdc");
    }
}
