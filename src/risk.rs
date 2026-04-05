use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::config::Config;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RiskSnapshot {
    pub kill_switch: bool,
    pub kill_switch_reason: String,
    pub circuit_breaker: bool,
    pub circuit_breaker_reason: String,
    pub daily_pnl_usdc: f64,
    pub daily_trade_count: i64,
    pub daily_win_count: i64,
    pub cumulative_pnl_usdc: f64,
    pub cumulative_trade_count: i64,
    pub cumulative_win_count: i64,
    pub open_exposure_usdc: f64,
    pub open_positions: i64,
    pub consecutive_missed_fills: i64,
    pub poly_last_tick_age_ms: i64,
    pub poly_lookup_count: i64,
    pub poly_fallback_count: i64,
}

/// Thread-safe risk manager with kill-switch support.
pub struct RiskManager {
    cfg: Config,
    /// Daily realized PnL in cents (atomic, no mutex needed)
    daily_pnl_cents: Arc<AtomicI64>,
    /// Daily trade counts
    trade_count: Arc<AtomicI64>,
    win_count: Arc<AtomicI64>,
    /// Cumulative counters (never reset on midnight rollover)
    cumulative_pnl_cents: Arc<AtomicI64>,
    cumulative_trade_count: Arc<AtomicI64>,
    cumulative_win_count: Arc<AtomicI64>,
    /// Kill switch — set to true when daily drawdown cap is hit
    kill_switch: Arc<AtomicBool>,
    kill_switch_reason: Arc<RwLock<String>>,
    /// Circuit breaker — set when execution-quality anomalies are detected
    circuit_breaker: Arc<AtomicBool>,
    circuit_breaker_reason: Arc<RwLock<String>>,
    /// Consecutive live missed fills
    consecutive_missed_fills: Arc<AtomicI64>,
    /// Total notional exposure in open positions (USDC cents)
    open_exposure_cents: Arc<AtomicI64>,
    /// Count of currently open positions
    open_positions: Arc<AtomicI64>,
    /// Last observed age of Polymarket tick used by detector (ms)
    poly_last_tick_age_ms: Arc<AtomicI64>,
    /// Number of detector Polymarket lookups (for fallback-rate denominator)
    poly_lookup_count: Arc<AtomicI64>,
    /// Number of detector lookups that needed direct fallback polling
    poly_fallback_count: Arc<AtomicI64>,
    /// Live lag tracker: mapping format!("BTC {}", timeframe) -> lag_pp
    pub latest_lags: Arc<dashmap::DashMap<String, f64>>,
}

impl RiskManager {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            daily_pnl_cents: Arc::new(AtomicI64::new(0)),
            trade_count: Arc::new(AtomicI64::new(0)),
            win_count: Arc::new(AtomicI64::new(0)),
            cumulative_pnl_cents: Arc::new(AtomicI64::new(0)),
            cumulative_trade_count: Arc::new(AtomicI64::new(0)),
            cumulative_win_count: Arc::new(AtomicI64::new(0)),
            kill_switch: Arc::new(AtomicBool::new(false)),
            kill_switch_reason: Arc::new(RwLock::new(String::new())),
            circuit_breaker: Arc::new(AtomicBool::new(false)),
            circuit_breaker_reason: Arc::new(RwLock::new(String::new())),
            consecutive_missed_fills: Arc::new(AtomicI64::new(0)),
            open_exposure_cents: Arc::new(AtomicI64::new(0)),
            open_positions: Arc::new(AtomicI64::new(0)),
            poly_last_tick_age_ms: Arc::new(AtomicI64::new(-1)),
            poly_lookup_count: Arc::new(AtomicI64::new(0)),
            poly_fallback_count: Arc::new(AtomicI64::new(0)),
            latest_lags: Arc::new(dashmap::DashMap::new()),
        }
    }

    /// Returns the approved USDC position size, or None if blocked.
    /// Checks kill switch, daily loss cap, and max position size.
    pub fn approve_trade(&self, requested_size: f64) -> Option<f64> {
        // Kill switch
        if self.kill_switch.load(Ordering::Relaxed) {
            let reason = self.kill_switch_reason();
            if reason.is_empty() {
                warn!("[Risk] KILL SWITCH active — all trading halted");
            } else {
                warn!("[Risk] KILL SWITCH active ({}) — all trading halted", reason);
            }
            return None;
        }

        if self.circuit_breaker.load(Ordering::Relaxed) {
            let reason = self.circuit_breaker_reason();
            if reason.is_empty() {
                warn!("[Risk] CIRCUIT BREAKER active — all trading halted");
            } else {
                warn!("[Risk] CIRCUIT BREAKER active ({}) — all trading halted", reason);
            }
            return None;
        }

        let daily_pnl = self.daily_pnl();
        let cap = -self.cfg.daily_loss_cap_usdc();

        if daily_pnl <= cap {
            warn!(
                "[Risk] Daily drawdown cap hit: {:.2} USDC — activating kill switch",
                daily_pnl
            );
            self.activate_kill_switch("daily_drawdown_cap");
            return None;
        }

        let open_positions = self.open_position_count();
        if open_positions >= self.cfg.max_concurrent_positions as i64 {
            warn!(
                "[Risk] Open position cap hit: {}/{}",
                open_positions,
                self.cfg.max_concurrent_positions
            );
            return None;
        }

        let poly_age_ms = self.poly_last_tick_age_ms.load(Ordering::Relaxed);
        let max_stale_ms = (self.cfg.oracle_staleness_limit_secs as i64).saturating_mul(1_000);
        if poly_age_ms >= 0 && poly_age_ms > max_stale_ms {
            warn!(
                "[Risk] Oracle stale: {}ms > {}ms — skip trade",
                poly_age_ms,
                max_stale_ms
            );
            return None;
        }

        // Warn at 50% and 75% of daily cap
        let pct_of_cap = daily_pnl / self.cfg.daily_loss_cap_usdc() * -100.0;
        if pct_of_cap >= 75.0 {
            warn!("[Risk] At {:.0}% of daily drawdown cap!", pct_of_cap);
        }

        let exposure_cap = self.cfg.max_total_exposure_usdc();
        let current_exposure = self.open_exposure_usdc();
        if current_exposure >= exposure_cap {
            warn!(
                "[Risk] Open exposure cap hit: {:.2}/{:.2} USDC",
                current_exposure, exposure_cap
            );
            return None;
        }

        let remaining_capacity = (exposure_cap - current_exposure).max(0.0);

        // Clamp to per-trade limits and remaining global exposure capacity.
        let size = requested_size
            .min(self.cfg.max_position_usdc())
            .min(self.cfg.max_order_size_usdc)
            .min(remaining_capacity);

        if size < self.cfg.min_trade_size_usdc {
            debug!(
                "[Risk] size {:.4} below MIN_TRADE_SIZE_USDC={:.4} — skip",
                size,
                self.cfg.min_trade_size_usdc
            );
            return None;
        }

        info!("[Risk] Approved trade size={:.2} USDC", size);
        Some(size)
    }

    /// Record a completed trade's PnL.
    pub fn record_pnl(&self, pnl_usdc: f64, won: bool) {
        let cents = (pnl_usdc * 100.0) as i64;
        let prev_cents = self.daily_pnl_cents.fetch_add(cents, Ordering::Relaxed);
        self.cumulative_pnl_cents.fetch_add(cents, Ordering::Relaxed);
        self.trade_count.fetch_add(1, Ordering::Relaxed);
        self.cumulative_trade_count.fetch_add(1, Ordering::Relaxed);
        if won {
            self.win_count.fetch_add(1, Ordering::Relaxed);
            self.cumulative_win_count.fetch_add(1, Ordering::Relaxed);
        }
        info!(
            "[Risk] PnL recorded {:+.2} | daily_pnl={:+.2} | trades={} | wins={}",
            pnl_usdc,
            (prev_cents + cents) as f64 / 100.0,
            self.trade_count(),
            self.win_count()
        );
    }

    pub fn record_missed_fill(&self) {
        let missed = self
            .consecutive_missed_fills
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if missed >= self.cfg.cb_consecutive_missed_fills as i64 {
            self.trigger_circuit_breaker(format!("{} consecutive missed fills", missed));
        }
    }

    pub fn record_fill_success(&self) {
        self.consecutive_missed_fills.store(0, Ordering::Relaxed);
    }

    pub fn record_slippage(&self, expected_price: f64, actual_price: f64) {
        if expected_price <= 0.0 {
            return;
        }
        let slippage = (actual_price - expected_price) / expected_price;
        if slippage > self.cfg.cb_slippage_threshold {
            self.trigger_circuit_breaker(format!(
                "slippage {:.2}% exceeded threshold {:.2}%",
                slippage * 100.0,
                self.cfg.cb_slippage_threshold * 100.0
            ));
        }
    }

    pub fn record_resolution_mismatch(&self, expected_win: bool, won: bool) {
        if self.cfg.cb_resolution_mismatch && expected_win != won {
            self.trigger_circuit_breaker(format!(
                "resolution mismatch: expected {} got {}",
                if expected_win { "win" } else { "loss" },
                if won { "win" } else { "loss" }
            ));
        }
    }

    pub fn reset_daily(&self) {
        self.daily_pnl_cents.store(0, Ordering::Relaxed);
        self.trade_count.store(0, Ordering::Relaxed);
        self.win_count.store(0, Ordering::Relaxed);
        self.kill_switch.store(false, Ordering::Relaxed);
        self.circuit_breaker.store(false, Ordering::Relaxed);
        self.kill_switch_reason.write().clear();
        self.circuit_breaker_reason.write().clear();
        self.consecutive_missed_fills.store(0, Ordering::Relaxed);
        info!("[Risk] Daily counters and kill-switch reset");
    }

    pub fn daily_pnl(&self) -> f64 {
        self.daily_pnl_cents.load(Ordering::Relaxed) as f64 / 100.0
    }

    pub fn trade_count(&self) -> i64 {
        self.trade_count.load(Ordering::Relaxed)
    }

    pub fn win_count(&self) -> i64 {
        self.win_count.load(Ordering::Relaxed)
    }

    pub fn win_rate(&self) -> f64 {
        let tc = self.trade_count();
        if tc == 0 {
            return 0.0;
        }
        self.win_count() as f64 / tc as f64 * 100.0
    }

    pub fn cumulative_pnl(&self) -> f64 {
        self.cumulative_pnl_cents.load(Ordering::Relaxed) as f64 / 100.0
    }

    pub fn cumulative_trade_count(&self) -> i64 {
        self.cumulative_trade_count.load(Ordering::Relaxed)
    }

    pub fn cumulative_win_count(&self) -> i64 {
        self.cumulative_win_count.load(Ordering::Relaxed)
    }

    pub fn cumulative_win_rate(&self) -> f64 {
        let tc = self.cumulative_trade_count();
        if tc == 0 {
            return 0.0;
        }
        self.cumulative_win_count() as f64 / tc as f64 * 100.0
    }

    pub fn activate_kill_switch(&self, reason: &str) {
        self.kill_switch.store(true, Ordering::Relaxed);
        *self.kill_switch_reason.write() = reason.to_string();
        warn!("[Risk] Kill switch activated: {}", reason);
    }

    pub fn deactivate_kill_switch(&self) {
        self.kill_switch.store(false, Ordering::Relaxed);
        self.circuit_breaker.store(false, Ordering::Relaxed);
        self.consecutive_missed_fills.store(0, Ordering::Relaxed);
        self.kill_switch_reason.write().clear();
        self.circuit_breaker_reason.write().clear();
        info!("[Risk] Kill switch and circuit breaker cleared");
    }

    fn trigger_circuit_breaker(&self, reason: String) {
        self.circuit_breaker.store(true, Ordering::Relaxed);
        *self.circuit_breaker_reason.write() = reason.clone();
        warn!("[Risk] Circuit breaker triggered: {}", reason);
    }

    pub fn kill_switch_reason(&self) -> String {
        self.kill_switch_reason.read().clone()
    }

    pub fn circuit_breaker_active(&self) -> bool {
        self.circuit_breaker.load(Ordering::Relaxed)
    }

    pub fn circuit_breaker_reason(&self) -> String {
        self.circuit_breaker_reason.read().clone()
    }

    pub fn is_kill_switch_active(&self) -> bool {
        self.kill_switch.load(Ordering::Relaxed)
    }

    pub fn is_halted(&self) -> bool {
        self.kill_switch.load(Ordering::Relaxed) || self.circuit_breaker.load(Ordering::Relaxed)
    }

    /// Load exposure from persisted open positions on startup.
    pub fn set_open_exposure_usdc(&self, exposure_usdc: f64) {
        let cents = (exposure_usdc.max(0.0) * 100.0) as i64;
        self.open_exposure_cents.store(cents, Ordering::Relaxed);
        info!("[Risk] Open exposure synced: {:.2} USDC", exposure_usdc.max(0.0));
    }

    /// Load open position count from persisted open positions on startup.
    pub fn set_open_position_count(&self, count: i64) {
        self.open_positions.store(count.max(0), Ordering::Relaxed);
        info!("[Risk] Open positions synced: {}", count.max(0));
    }

    /// Reserve notional exposure after an order/trade is opened.
    pub fn reserve_open_exposure(&self, size_usdc: f64) {
        let cents = (size_usdc.max(0.0) * 100.0) as i64;
        if cents <= 0 {
            return;
        }
        self.open_exposure_cents.fetch_add(cents, Ordering::Relaxed);
        self.open_positions.fetch_add(1, Ordering::Relaxed);
        info!("[Risk] Reserved exposure: +{:.2} USDC | total={:.2}", size_usdc, self.open_exposure_usdc());
    }

    /// Release notional exposure when a position is closed.
    pub fn release_open_exposure(&self, size_usdc: f64) {
        let cents = (size_usdc.max(0.0) * 100.0) as i64;
        if cents <= 0 {
            return;
        }
        let prev = self.open_exposure_cents.fetch_sub(cents, Ordering::Relaxed);
        if prev < cents {
            self.open_exposure_cents.store(0, Ordering::Relaxed);
        }
        let prev_positions = self.open_positions.fetch_sub(1, Ordering::Relaxed);
        if prev_positions <= 0 {
            self.open_positions.store(0, Ordering::Relaxed);
        }
        info!("[Risk] Released exposure: -{:.2} USDC | total={:.2}", size_usdc, self.open_exposure_usdc());
    }

    pub fn open_exposure_usdc(&self) -> f64 {
        self.open_exposure_cents.load(Ordering::Relaxed) as f64 / 100.0
    }

    pub fn open_position_count(&self) -> i64 {
        self.open_positions.load(Ordering::Relaxed)
    }

    /// Track one Polymarket lookup from detector, and whether fallback polling was needed.
    pub fn note_poly_lookup(&self, used_fallback: bool) {
        self.poly_lookup_count.fetch_add(1, Ordering::Relaxed);
        if used_fallback {
            self.poly_fallback_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Track the age (ms) of the Polymarket quote used by detector.
    pub fn note_poly_tick_age_ms(&self, age_ms: i64) {
        self.poly_last_tick_age_ms
            .store(age_ms.max(0), Ordering::Relaxed);
    }

    /// Returns (last_tick_age_ms, fallback_rate_pct, fallback_count, lookup_count).
    pub fn poly_data_metrics(&self) -> (i64, f64, i64, i64) {
        let last_age_ms = self.poly_last_tick_age_ms.load(Ordering::Relaxed);
        let lookups = self.poly_lookup_count.load(Ordering::Relaxed).max(0);
        let fallbacks = self.poly_fallback_count.load(Ordering::Relaxed).max(0);
        let fallback_rate = if lookups > 0 {
            fallbacks as f64 / lookups as f64 * 100.0
        } else {
            0.0
        };
        (last_age_ms, fallback_rate, fallbacks, lookups)
    }

    /// Returns (daily_pnl, trade_count, win_rate_pct, halted)
    pub fn snapshot(&self) -> (f64, i64, f64, bool) {
        (
            self.daily_pnl(),
            self.trade_count(),
            self.win_rate(),
            self.is_halted(),
        )
    }

    pub fn to_snapshot(&self) -> RiskSnapshot {
        RiskSnapshot {
            kill_switch: self.kill_switch.load(Ordering::Relaxed),
            kill_switch_reason: self.kill_switch_reason(),
            circuit_breaker: self.circuit_breaker.load(Ordering::Relaxed),
            circuit_breaker_reason: self.circuit_breaker_reason(),
            daily_pnl_usdc: self.daily_pnl(),
            daily_trade_count: self.trade_count(),
            daily_win_count: self.win_count(),
            cumulative_pnl_usdc: self.cumulative_pnl(),
            cumulative_trade_count: self.cumulative_trade_count(),
            cumulative_win_count: self.cumulative_win_count(),
            open_exposure_usdc: self.open_exposure_usdc(),
            open_positions: self.open_position_count(),
            consecutive_missed_fills: self.consecutive_missed_fills.load(Ordering::Relaxed),
            poly_last_tick_age_ms: self.poly_last_tick_age_ms.load(Ordering::Relaxed),
            poly_lookup_count: self.poly_lookup_count.load(Ordering::Relaxed),
            poly_fallback_count: self.poly_fallback_count.load(Ordering::Relaxed),
        }
    }

    pub fn restore_snapshot(&self, snap: &RiskSnapshot) {
        self.kill_switch.store(snap.kill_switch, Ordering::Relaxed);
        self.circuit_breaker
            .store(snap.circuit_breaker, Ordering::Relaxed);
        *self.kill_switch_reason.write() = snap.kill_switch_reason.clone();
        *self.circuit_breaker_reason.write() = snap.circuit_breaker_reason.clone();
        self.daily_pnl_cents
            .store((snap.daily_pnl_usdc * 100.0) as i64, Ordering::Relaxed);
        self.trade_count
            .store(snap.daily_trade_count.max(0), Ordering::Relaxed);
        self.win_count
            .store(snap.daily_win_count.max(0), Ordering::Relaxed);
        self.cumulative_pnl_cents
            .store((snap.cumulative_pnl_usdc * 100.0) as i64, Ordering::Relaxed);
        self.cumulative_trade_count
            .store(snap.cumulative_trade_count.max(0), Ordering::Relaxed);
        self.cumulative_win_count
            .store(snap.cumulative_win_count.max(0), Ordering::Relaxed);
        self.open_exposure_cents
            .store((snap.open_exposure_usdc.max(0.0) * 100.0) as i64, Ordering::Relaxed);
        self.open_positions
            .store(snap.open_positions.max(0), Ordering::Relaxed);
        self.consecutive_missed_fills
            .store(snap.consecutive_missed_fills.max(0), Ordering::Relaxed);
        self.poly_last_tick_age_ms
            .store(snap.poly_last_tick_age_ms, Ordering::Relaxed);
        self.poly_lookup_count
            .store(snap.poly_lookup_count.max(0), Ordering::Relaxed);
        self.poly_fallback_count
            .store(snap.poly_fallback_count.max(0), Ordering::Relaxed);
    }
}
