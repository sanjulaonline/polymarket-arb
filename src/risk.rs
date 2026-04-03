use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use tracing::{info, warn};

use crate::config::Config;

/// Thread-safe risk manager with kill-switch support.
pub struct RiskManager {
    cfg: Config,
    /// Daily realized PnL in cents (atomic, no mutex needed)
    daily_pnl_cents: Arc<AtomicI64>,
    /// Trade counts
    trade_count: Arc<AtomicI64>,
    win_count: Arc<AtomicI64>,
    /// Kill switch — set to true when daily drawdown cap is hit
    kill_switch: Arc<AtomicBool>,
    /// Total notional exposure in open positions (USDC cents)
    open_exposure_cents: Arc<AtomicI64>,
}

impl RiskManager {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            daily_pnl_cents: Arc::new(AtomicI64::new(0)),
            trade_count: Arc::new(AtomicI64::new(0)),
            win_count: Arc::new(AtomicI64::new(0)),
            kill_switch: Arc::new(AtomicBool::new(false)),
            open_exposure_cents: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Returns the approved USDC position size, or None if blocked.
    /// Checks kill switch, daily loss cap, and max position size.
    pub fn approve_trade(&self, requested_size: f64) -> Option<f64> {
        // Kill switch
        if self.kill_switch.load(Ordering::Relaxed) {
            warn!("[Risk] KILL SWITCH active — all trading halted");
            return None;
        }

        let daily_pnl = self.daily_pnl();
        let cap = -self.cfg.daily_loss_cap_usdc();

        if daily_pnl <= cap {
            warn!(
                "[Risk] Daily drawdown cap hit: {:.2} USDC — activating kill switch",
                daily_pnl
            );
            self.kill_switch.store(true, Ordering::Relaxed);
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

        if size < 1.0 {
            return None;
        }

        info!("[Risk] Approved trade size={:.2} USDC", size);
        Some(size)
    }

    /// Record a completed trade's PnL.
    pub fn record_pnl(&self, pnl_usdc: f64, won: bool) {
        let cents = (pnl_usdc * 100.0) as i64;
        let prev_cents = self.daily_pnl_cents.fetch_add(cents, Ordering::Relaxed);
        self.trade_count.fetch_add(1, Ordering::Relaxed);
        if won {
            self.win_count.fetch_add(1, Ordering::Relaxed);
        }
        info!(
            "[Risk] PnL recorded {:+.2} | daily_pnl={:+.2} | trades={} | wins={}",
            pnl_usdc,
            (prev_cents + cents) as f64 / 100.0,
            self.trade_count(),
            self.win_count()
        );
    }

    pub fn reset_daily(&self) {
        self.daily_pnl_cents.store(0, Ordering::Relaxed);
        self.trade_count.store(0, Ordering::Relaxed);
        self.win_count.store(0, Ordering::Relaxed);
        self.kill_switch.store(false, Ordering::Relaxed);
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

    pub fn is_halted(&self) -> bool {
        self.kill_switch.load(Ordering::Relaxed)
    }

    /// Load exposure from persisted open positions on startup.
    pub fn set_open_exposure_usdc(&self, exposure_usdc: f64) {
        let cents = (exposure_usdc.max(0.0) * 100.0) as i64;
        self.open_exposure_cents.store(cents, Ordering::Relaxed);
        info!("[Risk] Open exposure synced: {:.2} USDC", exposure_usdc.max(0.0));
    }

    /// Reserve notional exposure after an order/trade is opened.
    pub fn reserve_open_exposure(&self, size_usdc: f64) {
        let cents = (size_usdc.max(0.0) * 100.0) as i64;
        if cents <= 0 {
            return;
        }
        self.open_exposure_cents.fetch_add(cents, Ordering::Relaxed);
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
        info!("[Risk] Released exposure: -{:.2} USDC | total={:.2}", size_usdc, self.open_exposure_usdc());
    }

    pub fn open_exposure_usdc(&self) -> f64 {
        self.open_exposure_cents.load(Ordering::Relaxed) as f64 / 100.0
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
}
