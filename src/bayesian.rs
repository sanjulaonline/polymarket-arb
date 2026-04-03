//! Bayesian Probability Updater
//! ==============================
//!
//! Purpose: update our *internal* probability estimate P(H|D) faster than
//! the Polymarket crowd updates its odds.
//!
//! The hypothesis H: "BTC/ETH will resolve in direction D (UP or DOWN)
//!                    before the contract expires."
//!
//! The model maintains a Beta(α, β) conjugate prior — the natural prior
//! for a Bernoulli probability. After every new CEX price tick we update
//! the posterior analytically (no MCMC needed).
//!
//!   Prior:     Beta(α₀, β₀)   — seeded from Polymarket's current odds
//!   Likelihood: Bernoulli(p)   — each tick is a "soft observation"
//!   Posterior: Beta(α₀ + Σwᵢ·yᵢ, β₀ + Σwᵢ·(1-yᵢ))
//!
//! where:
//!   yᵢ = soft label ∈ [0,1] of how strongly tick i confirms H
//!   wᵢ = weight based on recency and magnitude of the price move
//!
//! The posterior mean E[p] = α/(α+β) is our best estimate of the true
//! probability — updated in real-time, decaying old evidence automatically
//! via a forgetting factor λ.
//!
//! Why this beats the market:
//! The crowd updates odds slowly (human liquidity). We update our posterior
//! on every tick (milliseconds). When our posterior diverges enough from
//! the market price, that IS the edge.

use std::collections::VecDeque;
use std::time::Instant;
use tracing::debug;

// ── Evidence observation ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Observation {
    /// Soft label: 1.0 = strong confirm, 0.0 = strong disconfirm, 0.5 = neutral
    pub label: f64,
    /// Weight of this observation (based on magnitude of price move)
    pub weight: f64,
    /// Timestamp for decay
    pub recorded_at: Instant,
}

// ── Bayesian state ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BayesianEstimator {
    /// Alpha parameter of Beta distribution (pseudo-count of confirmations)
    alpha: f64,
    /// Beta parameter of Beta distribution (pseudo-count of disconfirmations)
    beta: f64,
    /// Hypothesis direction: true = up, false = down
    pub hypothesis_up: bool,
    /// Strike price for this contract
    strike: f64,
    /// Sliding window of recent observations (for diagnostics)
    observations: VecDeque<Observation>,
    /// Exponential decay half-life in seconds
    /// Older evidence counts less — default 30s
    half_life_secs: f64,
    /// Minimum effective sample size before we trust the posterior
    min_observations: usize,
    /// Initial Polymarket prior (to allow resetting)
    prior_alpha: f64,
    prior_beta: f64,
}

impl BayesianEstimator {
    /// Create a new estimator seeded from the market's current probability.
    ///
    /// # Arguments
    /// * `market_prob`     - Polymarket's current YES price (0–1)
    /// * `hypothesis_up`   - Are we testing the UP hypothesis?
    /// * `strike`          - Contract strike price
    /// * `prior_strength`  - How many pseudo-observations to give the prior
    ///   Higher = slower to update (more trust in market)
    ///   Lower = faster to update (more trust in our data)
    ///   Typical: 5–20
    pub fn new(
        market_prob: f64,
        hypothesis_up: bool,
        strike: f64,
        prior_strength: f64,
        half_life_secs: f64,
    ) -> Self {
        // If hypothesis is UP, our prior is market_prob (chance of UP)
        // If hypothesis is DOWN, our prior is 1 - market_prob
        let p = if hypothesis_up {
            market_prob.clamp(0.01, 0.99)
        } else {
            (1.0 - market_prob).clamp(0.01, 0.99)
        };

        let alpha = p * prior_strength;
        let beta = (1.0 - p) * prior_strength;

        Self {
            alpha,
            beta,
            hypothesis_up,
            strike,
            observations: VecDeque::with_capacity(200),
            half_life_secs,
            min_observations: 3,
            prior_alpha: alpha,
            prior_beta: beta,
        }
    }

    /// Update the posterior with a new CEX price observation.
    ///
    /// Called every time Binance/TradingView emits a new price tick.
    /// Returns the updated posterior probability for our hypothesis.
    pub fn update(&mut self, current_price: f64, prev_price: f64) -> f64 {
        // ── Step 1: compute the soft observation ─────────────────────────────
        let price_change_pct = (current_price - prev_price) / prev_price;

        // Soft label: how much does this price move confirm our hypothesis?
        //   If hypothesis_up and price rises → confirms (label → 1.0)
        //   If hypothesis_up and price falls → disconfirms (label → 0.0)
        //   Magnitude of move scales how strongly we update
        let raw_label = if self.hypothesis_up {
            0.5 + price_change_pct * 50.0 // 1% move → label ≈ 1.0
        } else {
            0.5 - price_change_pct * 50.0
        };
        let label = raw_label.clamp(0.0, 1.0);

        // Weight = magnitude of price change (bigger move = stronger signal)
        // Minimum weight 0.1, saturates at 1.0 for moves ≥ 0.5%
        let weight = (price_change_pct.abs() * 200.0).clamp(0.1, 1.0);

        let obs = Observation {
            label,
            weight,
            recorded_at: Instant::now(),
        };
        self.observations.push_back(obs);

        // Keep window manageable
        if self.observations.len() > 500 {
            self.observations.pop_front();
        }

        // ── Step 2: recompute α, β with exponential decay ────────────────────
        // Apply decay to all observations, then sum into α, β
        let ln2 = std::f64::consts::LN_2;
        let decay_rate = ln2 / self.half_life_secs;

        let mut alpha = self.prior_alpha;
        let mut beta = self.prior_beta;

        let now = Instant::now();
        for obs in &self.observations {
            let age_secs = now.duration_since(obs.recorded_at).as_secs_f64();
            let decay = (-decay_rate * age_secs).exp();
            let effective_weight = obs.weight * decay;
            alpha += effective_weight * obs.label;
            beta += effective_weight * (1.0 - obs.label);
        }

        self.alpha = alpha;
        self.beta = beta;

        let posterior = self.posterior_mean();
        debug!(
            "[Bayes] α={:.2} β={:.2} posterior={:.4} label={:.3} w={:.3}",
            alpha, beta, posterior, label, weight
        );

        posterior
    }

    /// E[p] = α / (α + β) — the posterior mean probability.
    pub fn posterior_mean(&self) -> f64 {
        self.alpha / (self.alpha + self.beta)
    }

    /// Credible interval [lo, hi] at the given probability mass.
    /// Uses the Wilson score approximation for speed (no Beta CDF needed).
    ///
    /// Returns (lower_bound, upper_bound)
    pub fn credible_interval(&self, confidence: f64) -> (f64, f64) {
        let p = self.posterior_mean();
        let n = self.alpha + self.beta;
        // Wilson score interval
        let z = normal_quantile(1.0 - (1.0 - confidence) / 2.0);
        let center = (p + z * z / (2.0 * n)) / (1.0 + z * z / n);
        let margin = z / (1.0 + z * z / n) * (p * (1.0 - p) / n + z * z / (4.0 * n * n)).sqrt();
        ((center - margin).max(0.0), (center + margin).min(1.0))
    }

    /// How certain are we? Returns effective sample size.
    pub fn effective_n(&self) -> f64 {
        self.alpha + self.beta - self.prior_alpha - self.prior_beta
    }

    /// True if we have enough observations to trust the posterior.
    pub fn is_warmed_up(&self) -> bool {
        self.observations.len() >= self.min_observations
    }

    /// Edge vs the current Polymarket price.
    /// Positive = our posterior is higher than market → buy YES.
    /// Negative = our posterior is lower → buy NO.
    pub fn edge_vs_market(&self, market_prob: f64) -> f64 {
        self.posterior_mean() - market_prob
    }

    /// Posterior variance — lower = more confident.
    pub fn posterior_variance(&self) -> f64 {
        let p = self.posterior_mean();
        let n = self.alpha + self.beta;
        p * (1.0 - p) / (n + 1.0)
    }

    /// Reset the prior to a new market probability (call when market moves).
    pub fn reset_prior(&mut self, new_market_prob: f64, prior_strength: f64) {
        let p = if self.hypothesis_up {
            new_market_prob.clamp(0.01, 0.99)
        } else {
            (1.0 - new_market_prob).clamp(0.01, 0.99)
        };
        self.prior_alpha = p * prior_strength;
        self.prior_beta = (1.0 - p) * prior_strength;
        self.observations.clear();
    }

    /// Summary for logging / dashboard.
    pub fn summary(&self) -> BayesianSummary {
        let p = self.posterior_mean();
        let (lo, hi) = self.credible_interval(0.90);
        BayesianSummary {
            posterior: p,
            alpha: self.alpha,
            beta: self.beta,
            effective_n: self.effective_n(),
            ci_lo: lo,
            ci_hi: hi,
            variance: self.posterior_variance(),
            warmed_up: self.is_warmed_up(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BayesianSummary {
    pub posterior: f64,
    pub alpha: f64,
    pub beta: f64,
    pub effective_n: f64,
    pub ci_lo: f64,
    pub ci_hi: f64,
    pub variance: f64,
    pub warmed_up: bool,
}

// ── Normal quantile approximation (Beasley-Springer-Moro) ────────────────────

fn normal_quantile(p: f64) -> f64 {
    // Rational approximation, accurate to ~3e-9
    let a = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_374_269e1,
        2.506_628_277_459_239,
    ];
    let b = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    let c = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    let d = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];

    let p_low = 0.02425;
    let p_high = 1.0 - p_low;

    if p < p_low {
        let q = (-2.0 * p.ln()).sqrt();
        (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    } else if p <= p_high {
        let q = p - 0.5;
        let r = q * q;
        (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
            / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prior_seeding() {
        // Market at 60% → estimator seeded at 60%
        let est = BayesianEstimator::new(0.60, true, 80_000.0, 10.0, 30.0);
        let p = est.posterior_mean();
        assert!((p - 0.60).abs() < 0.01, "Prior should seed at market prob");
    }

    #[test]
    fn test_confirming_updates_raise_posterior() {
        let mut est = BayesianEstimator::new(0.50, true, 80_000.0, 5.0, 30.0);
        let mut prev = 80_000.0_f64;
        // Feed 10 consecutive bullish ticks
        for _i in 0..10 {
            let cur = prev * 1.005; // +0.5% each tick
            est.update(cur, prev);
            prev = cur;
        }
        let p = est.posterior_mean();
        assert!(p > 0.55, "Posterior should rise after confirming ticks, got {p}");
    }

    #[test]
    fn test_disconfirming_updates_lower_posterior() {
        let mut est = BayesianEstimator::new(0.70, true, 80_000.0, 5.0, 30.0);
        let mut prev = 81_000.0_f64;
        // Feed 10 consecutive bearish ticks
        for _i in 0..10 {
            let cur = prev * 0.995; // -0.5% each tick
            est.update(cur, prev);
            prev = cur;
        }
        let p = est.posterior_mean();
        assert!(p < 0.70, "Posterior should fall after disconfirming ticks, got {p}");
    }

    #[test]
    fn test_edge_vs_market() {
        let est = BayesianEstimator::new(0.50, true, 80_000.0, 10.0, 30.0);
        // No updates yet → edge should be near 0
        assert!(est.edge_vs_market(0.50).abs() < 0.01);
    }

    #[test]
    fn test_credible_interval_contains_mean() {
        let est = BayesianEstimator::new(0.60, true, 80_000.0, 20.0, 30.0);
        let (lo, hi) = est.credible_interval(0.90);
        let p = est.posterior_mean();
        assert!(lo <= p && p <= hi, "Mean should be inside CI");
    }
}
