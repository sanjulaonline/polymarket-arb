/// Avellaneda-Stoikov Market-Making Model
/// ========================================
///
/// Originally derived from:
///   Avellaneda, M. & Stoikov, S. (2008).
///   "High-frequency trading in a limit order book."
///   Quantitative Finance, 8(3), 217–224.
///
/// In the original paper, a market maker posts bid/ask quotes around a
/// *reservation price* that accounts for their current inventory and the
/// risk of holding that inventory until the market closes.
///
/// ## Key formula
///
///   Reservation price:    r = s − q·γ·σ²·(T − t)
///
///   where:
///     s   = current mid price (in probability space 0–1)
///     q   = net inventory  (positive = long YES, negative = long NO)
///     γ   = risk-aversion coefficient (calibrated, typically 0.1–1.0)
///     σ²  = variance of the underlying asset per unit time
///     T−t = time remaining until contract expiry (in years, typically tiny)
///
///   Optimal half-spread:  δ = γ·σ²·(T−t) / 2 + (1/γ)·ln(1 + γ/κ)
///
///   where κ is the order-arrival rate (liquidity parameter).
///
/// ## How we use it in an arb context (not pure market-making)
///
/// We don't post limit orders. Instead we use the reservation price as
/// our *inventory-adjusted fair value*. The decision rule is:
///
///   If market_ask < r  → buy is favourable (market underprices vs our fair value)
///   If market_bid > r  → sell / fade is favourable
///   |r - market_mid| must also exceed the minimum edge threshold
///
/// This prevents the bot from buying into a position it already holds
/// heavily (inventory-neutral discipline) and skews quoting dynamically
/// as inventory accumulates.

use tracing::debug;

// ── Volatility estimator (rolling) ────────────────────────────────────────────

/// Rolling variance estimator using Welford's online algorithm.
/// Tracks σ² of price *returns* (not raw price) over a sliding window.
#[derive(Debug, Clone)]
pub struct VolatilityEstimator {
    window: std::collections::VecDeque<f64>,
    max_window: usize,
    /// Current mean of returns in window
    mean: f64,
    /// Current M2 (for Welford)
    m2: f64,
    count: usize,
}

impl VolatilityEstimator {
    pub fn new(window_size: usize) -> Self {
        Self {
            window: std::collections::VecDeque::with_capacity(window_size + 1),
            max_window: window_size,
            mean: 0.0,
            m2: 0.0,
            count: 0,
        }
    }

    /// Add a new log-return observation and update variance online.
    pub fn add_return(&mut self, ret: f64) {
        // Welford online update
        self.count += 1;
        let delta = ret - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = ret - self.mean;
        self.m2 += delta * delta2;

        self.window.push_back(ret);

        // Remove oldest if window full
        if self.window.len() > self.max_window {
            let removed = self.window.pop_front().unwrap();
            // Reverse Welford for removed element
            if self.count > 1 {
                self.count -= 1;
                let delta = removed - self.mean;
                self.mean -= delta / self.count as f64;
                let delta2 = removed - self.mean;
                self.m2 -= delta * delta2;
            }
        }
    }

    /// Add a new raw price and compute log-return vs previous.
    pub fn add_price(&mut self, price: f64, prev_price: f64) {
        if prev_price > 0.0 && price > 0.0 {
            let ret = (price / prev_price).ln();
            self.add_return(ret);
        }
    }

    /// Sample variance of returns in window. Returns 0 if < 2 samples.
    pub fn variance(&self) -> f64 {
        if self.count < 2 {
            return 1e-6; // minimal non-zero default
        }
        self.m2 / (self.count - 1) as f64
    }

    /// Annualised volatility σ (not σ²) given ticks per year.
    /// For 1-second ticks: ticks_per_year ≈ 31_536_000
    pub fn annualised_vol(&self, ticks_per_year: f64) -> f64 {
        (self.variance() * ticks_per_year).sqrt()
    }

    pub fn sample_count(&self) -> usize {
        self.window.len()
    }

    pub fn is_ready(&self) -> bool {
        self.window.len() >= 5
    }
}

// ── Inventory tracker ─────────────────────────────────────────────────────────

/// Tracks the bot's current net inventory in shares.
/// Positive = long YES, negative = short YES (long NO).
#[derive(Debug, Clone, Default)]
pub struct Inventory {
    /// Net position in shares (probability-weighted)
    pub net_shares: f64,
    /// Total USDC committed to open positions
    pub total_usdc: f64,
    /// Max allowed net position (from config)
    pub max_net_shares: f64,
}

impl Inventory {
    pub fn new(max_net_shares: f64) -> Self {
        Self { net_shares: 0.0, total_usdc: 0.0, max_net_shares }
    }

    pub fn add_position(&mut self, shares: f64, usdc: f64, is_yes: bool) {
        if is_yes {
            self.net_shares += shares;
        } else {
            self.net_shares -= shares;
        }
        self.total_usdc += usdc;
    }

    pub fn close_position(&mut self, shares: f64, usdc: f64, was_yes: bool) {
        if was_yes {
            self.net_shares -= shares;
        } else {
            self.net_shares += shares;
        }
        self.total_usdc = (self.total_usdc - usdc).max(0.0);
    }

    /// Normalised inventory q ∈ [-1, 1] used in Stoikov formula.
    pub fn normalised(&self) -> f64 {
        if self.max_net_shares <= 0.0 {
            return 0.0;
        }
        (self.net_shares / self.max_net_shares).clamp(-1.0, 1.0)
    }

    /// True if we're at the inventory limit on the given side.
    pub fn is_maxed_out(&self, buy_yes: bool) -> bool {
        if buy_yes {
            self.net_shares >= self.max_net_shares
        } else {
            self.net_shares <= -self.max_net_shares
        }
    }
}

// ── Stoikov model ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StoikovModel {
    /// Risk-aversion coefficient γ (higher = more conservative quoting)
    pub gamma: f64,
    /// Order arrival intensity κ (higher = tighter spreads)
    pub kappa: f64,
    /// Rolling volatility estimator
    pub vol: VolatilityEstimator,
    /// Inventory state
    pub inventory: Inventory,
    /// Contract expiry time in seconds from now (updated each tick)
    pub time_to_expiry_secs: f64,
}

/// Output of the Stoikov model for one evaluation.
#[derive(Debug, Clone)]
pub struct StoikovOutput {
    /// Mid price s (polymarket mid, 0–1)
    pub mid: f64,
    /// Reservation price r = s - q·γ·σ²·(T-t)
    pub reservation_price: f64,
    /// Optimal half-spread δ/2
    pub half_spread: f64,
    /// Optimal bid quote = r - δ/2
    pub bid: f64,
    /// Optimal ask quote = r + δ/2
    pub ask: f64,
    /// Net inventory (normalised, -1 to 1)
    pub inventory_q: f64,
    /// Current σ² (variance per second)
    pub variance: f64,
    /// Time to expiry (seconds)
    pub tte_secs: f64,
    /// Is the model ready (enough vol history)?
    pub ready: bool,
}

impl StoikovOutput {
    /// True if the market ask is below our reservation price by at least `min_edge`.
    pub fn buy_is_favourable(&self, market_ask: f64, min_edge: f64) -> bool {
        self.ready && (self.reservation_price - market_ask) >= min_edge
    }

    /// True if the market bid is above our reservation price by at least `min_edge`.
    pub fn sell_is_favourable(&self, market_bid: f64, min_edge: f64) -> bool {
        self.ready && (market_bid - self.reservation_price) >= min_edge
    }

    /// Net directional signal in probability units (-1 to +1).
    /// Positive = lean long, Negative = lean short.
    pub fn direction_signal(&self) -> f64 {
        self.reservation_price - self.mid
    }
}

impl StoikovModel {
    pub fn new(
        gamma: f64,
        kappa: f64,
        max_inventory_shares: f64,
        vol_window: usize,
        time_to_expiry_secs: f64,
    ) -> Self {
        Self {
            gamma,
            kappa,
            vol: VolatilityEstimator::new(vol_window),
            inventory: Inventory::new(max_inventory_shares),
            time_to_expiry_secs,
        }
    }

    /// Add a new price observation and update volatility.
    pub fn observe_price(&mut self, price: f64, prev_price: f64) {
        self.vol.add_price(price, prev_price);
    }

    /// Update time to expiry (call every second or on each tick).
    pub fn tick_time(&mut self, elapsed_secs: f64) {
        self.time_to_expiry_secs = (self.time_to_expiry_secs - elapsed_secs).max(0.0);
    }

    /// Evaluate the model at the current mid price.
    ///
    /// # Arguments
    /// * `mid` - Current Polymarket mid probability (0–1)
    pub fn evaluate(&self, mid: f64) -> StoikovOutput {
        let q = self.inventory.normalised();
        let sigma2 = self.vol.variance(); // variance per tick (NOT annualised here)
        let tte = self.time_to_expiry_secs;
        let ready = self.vol.is_ready() && tte > 0.0;

        // ── Reservation price ────────────────────────────────────────────────
        // r = s - q·γ·σ²·(T-t)
        // q is signed inventory, so this skews r away from the overloaded side:
        //   Long YES (q > 0) → r shifts down → we don't want to buy more YES
        //   Short YES (q < 0) → r shifts up → we don't want to buy more NO
        let reservation_price = if ready {
            mid - q * self.gamma * sigma2 * tte
        } else {
            mid
        };

        // ── Optimal spread ───────────────────────────────────────────────────
        // δ* = γ·σ²·(T-t) + (2/γ)·ln(1 + γ/κ)
        //
        // First term: inventory/risk component (grows with time and variance)
        // Second term: liquidity component (constant given γ, κ)
        let half_spread = if ready {
            let risk_term = self.gamma * sigma2 * tte / 2.0;
            let liq_term = (1.0 / self.gamma) * (1.0 + self.gamma / self.kappa).ln();
            (risk_term + liq_term).max(0.001) // minimum 0.1pp half-spread
        } else {
            0.02 // default 4pp total spread while warming up
        };

        let bid = (reservation_price - half_spread).max(0.001);
        let ask = (reservation_price + half_spread).min(0.999);

        debug!(
            "[Stoikov] mid={:.4} q={:.3} σ²={:.2e} tte={:.0}s r={:.4} δ/2={:.4} bid={:.4} ask={:.4}",
            mid, q, sigma2, tte, reservation_price, half_spread, bid, ask
        );

        StoikovOutput {
            mid,
            reservation_price,
            half_spread,
            bid,
            ask,
            inventory_q: q,
            variance: sigma2,
            tte_secs: tte,
            ready,
        }
    }

    /// Add a completed trade to inventory.
    pub fn record_fill(&mut self, shares: f64, usdc: f64, bought_yes: bool) {
        self.inventory.add_position(shares, usdc, bought_yes);
    }

    /// Remove a closed position from inventory.
    pub fn record_close(&mut self, shares: f64, usdc: f64, was_yes: bool) {
        self.inventory.close_position(shares, usdc, was_yes);
    }

    /// Current normalised inventory.
    pub fn inventory_q(&self) -> f64 {
        self.inventory.normalised()
    }

    /// True if we're maxed out on the given side.
    pub fn inventory_maxed(&self, buy_yes: bool) -> bool {
        self.inventory.is_maxed_out(buy_yes)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reservation_price_long_skew() {
        // Long inventory should push reservation price down
        let mut model = StoikovModel::new(0.5, 1.5, 1000.0, 5, 300.0);
        // Warm up vol
        let mut prev = 80_000.0;
        for i in 0..10 {
            let cur = prev * (1.0 + 0.001 * (i as f64 % 2.0 - 0.5));
            model.observe_price(cur, prev);
            prev = cur;
        }
        model.inventory.add_position(500.0, 250.0, true); // long YES

        let out_long = model.evaluate(0.55);

        // Reset inventory
        model.inventory.net_shares = 0.0;
        let out_flat = model.evaluate(0.55);

        assert!(
            out_long.reservation_price <= out_flat.reservation_price,
            "Long inventory should push r down"
        );
    }

    #[test]
    fn test_spread_positive() {
        let mut model = StoikovModel::new(0.5, 1.5, 1000.0, 10, 300.0);
        let mut prev = 80_000.0;
        for i in 0..15 {
            let cur = prev * (1.0 + 0.0005 * (i as f64 % 2.0 - 0.5));
            model.observe_price(cur, prev);
            prev = cur;
        }
        let out = model.evaluate(0.50);
        assert!(out.half_spread > 0.0, "Spread must be positive");
        assert!(out.bid < out.ask, "Bid must be below ask");
        assert!(out.bid < 0.50 && out.ask > 0.50, "Mid should be inside spread");
    }

    #[test]
    fn test_vol_estimator_online() {
        let mut vol = VolatilityEstimator::new(50);
        let mut prev = 100.0;
        for i in 0..30 {
            let cur = prev * (1.0 + 0.001);
            vol.add_price(cur, prev);
            prev = cur;
        }
        assert!(vol.is_ready());
        assert!(vol.variance() > 0.0);
    }
}
