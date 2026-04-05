//! Detector — the strategy execution loop
//! ========================================
//!
//! Decision pipeline per (asset, timeframe) contract slot, on every price tick:
//!
//!   1. Pull best real price (Binance > TradingView > CryptoQuant)
//!   2. Pull Polymarket mid-probability from order book
//!   3. ── BAYESIAN UPDATE ──────────────────────────────────────────────────
//!      Feed the new price into BayesianEstimator → get posterior P(H|D)
//!      This updates faster than the crowd can reprice the contract
//!   4. ── STOIKOV EVALUATION ─────────────────────────────────────────────
//!      Compute reservation price r = s - q·γ·σ²·(T-t)
//!      Check if market is on the right side of our reservation price
//!   5. Gate: lag ≥ threshold (fast pre-filter before expensive checks)
//!   6. Score confidence (6-signal scorer)
//!   7. Gate: edge ≥ MIN_EDGE_PCT and confidence ≥ MIN_CONFIDENCE
//!   8. ── KELLY SIZING ────────────────────────────────────────────────────
//!      f* = (b·posterior - q_loss) / b  → scaled by uncertainty + inventory
//!   9. Risk approval (daily cap, kill switch)
//!  10. Paper log OR live FOK order → record fill in Stoikov inventory

use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::{
    bayesian::BayesianEstimator,
    confidence::{cex_implied_probability, compute_confidence, ConfidenceInput},
    config::Config,
    database::Database,
    kelly::{kelly_size, KellyInput},
    polymarket::client::{OrderRequest, OrderSide, OrderType, PolymarketClient, TimeInForce, Token},
    risk::RiskManager,
    stoikov::StoikovModel,
    telegram::Telegram,
    types::{Asset, MarketSnapshot, ModelOutput, PriceSource, PriceTick, Timeframe, TradeRecord},
};

const COMPARE_LOG_INTERVAL_MS: u64 = 1_000;
const LIVE_ORDER_PRICE_DECIMALS: usize = 4;
const LIVE_ORDER_SIZE_DECIMALS: usize = 2;
const DEFAULT_MIN_TOKEN_AMOUNT: f64 = 5.0;

type FeedKey = (PriceSource, Asset, Option<Timeframe>);
type PolySnapshot = (f64, f64, i64, Option<f64>, Option<f64>);

fn estimate_fair_up_probability(pct_change: f64) -> f64 {
    // Logistic map from short-horizon move (%) to UP probability.
    let k = 50.0;
    let p = 1.0 / (1.0 + (-k * (pct_change / 100.0)).exp());
    p.clamp(0.05, 0.95)
}

fn floor_to_decimals(value: f64, decimals: usize) -> f64 {
    if !value.is_finite() {
        return 0.0;
    }
    let multiplier = 10_f64.powi(decimals as i32);
    (value * multiplier).floor() / multiplier
}

#[derive(Clone, Debug)]
struct CompareTelemetry {
    binance_price: f64,
    binance_age_ms: i64,
    strike: f64,
    yes_bid: f64,
    yes_ask: f64,
    yes_mid: f64,
    cex_prob: f64,
    bayes_posterior: f64,
    price_lag_pct: f64,
    prob_lag_pp: f64,
    bayes_edge_pp: f64,
    direction_up: bool,
    prefilter_pass: bool,
    no_token_id: String,
    allow_no_token_refresh: bool,
}

// ── Contract slot ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ContractSlot {
    pub asset: Asset,
    pub timeframe: Timeframe,
    pub condition_id: String,
    pub title: String,
    pub yes_token: Token,
    pub no_token: Token,
    /// Dollar strike price of the contract
    pub strike: f64,
    /// Contract expiry as Unix timestamp (seconds)
    pub expiry_ts: i64,
}

impl ContractSlot {
    pub fn time_to_expiry_secs(&self) -> f64 {
        let now = Utc::now().timestamp();
        ((self.expiry_ts - now) as f64).max(0.0)
    }
}

// ── Per-slot model state ───────────────────────────────────────────────────────

struct SlotState {
    /// Bayesian estimator for the UP hypothesis
    bayes_up: BayesianEstimator,
    /// Bayesian estimator for the DOWN hypothesis
    bayes_down: BayesianEstimator,
    /// Stoikov market-making model (shared across both directions)
    stoikov: StoikovModel,
    /// Previous real price (for computing returns)
    prev_price: Option<f64>,
    /// Previous Polymarket mid (for detecting market repricing)
    prev_poly_prob: Option<f64>,
    /// Consecutive ticks with same edge direction
    streak: u32,
    last_direction_up: Option<bool>,
    /// Timestamp of last slot evaluation
    last_eval: Option<Instant>,
    /// Open price of the current timeframe bucket (used as up/down reference strike)
    candle_open_price: Option<f64>,
    /// UTC epoch timestamp of current bucket start
    candle_bucket_start_ts: Option<i64>,
    /// Last time a live compare line was emitted for this slot
    last_compare_log: Option<Instant>,
    /// Cached active NO token id for compare telemetry
    active_no_token_id: Option<String>,
    /// Last attempt time to refresh NO token mapping for this slot
    last_no_token_refresh: Option<Instant>,
    /// Last time we opened a trade on this slot
    last_trade_at: Option<Instant>,
    /// Whether there is an open paper position for this slot
    paper_position_open: bool,
    /// Rolling spot-price history used for N-second momentum checks.
    price_history: VecDeque<(i64, f64)>,
    /// Last time latency-arb signal evaluation ran for this slot.
    last_signal_eval: Option<Instant>,
}

impl SlotState {
    fn new(
        market_prob: f64,
        strike: f64,
        cfg: &Config,
        max_inventory_shares: f64,
        tte_secs: f64,
    ) -> Self {
        Self {
            bayes_up: BayesianEstimator::new(
                market_prob, true, strike,
                cfg.bayes_prior_strength, cfg.bayes_half_life_secs,
            ),
            bayes_down: BayesianEstimator::new(
                market_prob, false, strike,
                cfg.bayes_prior_strength, cfg.bayes_half_life_secs,
            ),
            stoikov: StoikovModel::new(
                cfg.stoikov_gamma, cfg.stoikov_kappa,
                max_inventory_shares, cfg.stoikov_vol_window, tte_secs,
            ),
            prev_price: None,
            prev_poly_prob: None,
            streak: 0,
            last_direction_up: None,
            last_eval: None,
            candle_open_price: None,
            candle_bucket_start_ts: None,
            last_compare_log: None,
            active_no_token_id: None,
            last_no_token_refresh: None,
            last_trade_at: None,
            paper_position_open: false,
            price_history: VecDeque::new(),
            last_signal_eval: None,
        }
    }

    fn update_streak(&mut self, direction_up: bool) -> u32 {
        if self.last_direction_up == Some(direction_up) {
            self.streak += 1;
        } else {
            self.streak = 1;
            self.last_direction_up = Some(direction_up);
        }
        self.streak
    }
}

// ── Detector ──────────────────────────────────────────────────────────────────

pub struct Detector {
    cfg: Config,
    client: Arc<PolymarketClient>,
    risk: Arc<RiskManager>,
    db: Arc<Database>,
    tg: Arc<Telegram>,
    contracts: Vec<ContractSlot>,
    latest: Arc<DashMap<FeedKey, PriceTick>>,
    /// Per-slot model state, keyed by (asset_display, timeframe_display)
    states: Mutex<HashMap<(String, String), SlotState>>,
}

impl Detector {
    pub fn new(
        cfg: Config,
        client: Arc<PolymarketClient>,
        risk: Arc<RiskManager>,
        db: Arc<Database>,
        tg: Arc<Telegram>,
        contracts: Vec<ContractSlot>,
        latest: Arc<DashMap<FeedKey, PriceTick>>,
    ) -> Self {
        // Slot state is created lazily on first valid tick so priors and strike references
        // are seeded from timeframe-correct market data.
        let states = HashMap::new();

        Self {
            cfg,
            client,
            risk,
            db,
            tg,
            contracts,
            latest,
            states: Mutex::new(states),
        }
    }

    pub async fn run(&self, mut rx: broadcast::Receiver<PriceTick>) -> Result<()> {
        info!(
            "[Detector] OracleLag pipeline | move_from_open>={:+.3}% time_left>{}s entry<=${:.2} oracle_stale<={}s spread<={:.3} | {}",
            self.cfg.price_change_threshold_pct,
            self.cfg.settlement_buffer_seconds,
            self.cfg.polymarket_max_price,
            self.cfg.oracle_staleness_limit_secs,
            self.cfg.max_spread,
            if self.cfg.is_live() { "LIVE" } else { "PAPER" }
        );

        let mut settlement_iv = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                recv = rx.recv() => {
                    let tick = match recv {
                        Ok(tick) => tick,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("[Detector] lagging by {} messages", n);
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                    };

                    if self.risk.is_halted() { continue; }

                    // Strategy is oracle-driven: evaluate on each oracle (Polymarket Chainlink) tick.
                    if !matches!(tick.source, PriceSource::PolymarketWs) {
                        continue;
                    }

                    let targets: Vec<ContractSlot> = self
                        .contracts
                        .iter()
                        .filter(|slot| {
                            slot.asset == tick.asset
                                && tick
                                    .timeframe
                                    .map(|tf| slot.timeframe == tf)
                                    .unwrap_or(true)
                        })
                        .cloned()
                        .collect();

                    for slot in targets {
                        self.check_slot(&slot).await;
                    }
                }
                _ = settlement_iv.tick() => {
                    if !self.cfg.is_live() {
                        if let Ok(opens) = self.db.open_positions() {
                            for open in opens {
                                if let Some(condition_id) = open
                                    .condition_id
                                    .as_deref()
                                    .map(str::trim)
                                    .filter(|s| !s.is_empty())
                                {
                                    let api_resolved = self
                                        .client
                                        .check_market_resolution(condition_id)
                                        .await
                                        .ok()
                                        .flatten()
                                        .map(|m| m.resolved)
                                        .unwrap_or(false);

                                    if api_resolved {
                                        if let Ok(onchain) = self
                                            .client
                                            .check_onchain_payout(&self.cfg.polygon_rpc_url, condition_id)
                                            .await
                                        {
                                            if onchain.resolved {
                                                self.simulate_paper_redeem_settlement(&open, &onchain.payouts).await;
                                                continue;
                                            }
                                        }
                                    }
                                }

                                if let Some(price) = self.best_price(Asset::Btc) { // Assuming BTC for now
                                    // We need the strike. In up/down, it's the candle open price.
                                    // For simplicity in this fix, we'll just use a rough estimate or skip if not found.
                                    let strike = {
                                        let states = self.states.lock();
                                        let key = Self::record_state_key(&open);
                                        states.get(&key).and_then(|s| s.candle_open_price).unwrap_or(price)
                                    };
                                    self.simulate_paper_settlement(&open, price, strike).await;
                                }
                            }
                        }
                    }
                }
            }
        }
        #[allow(unreachable_code)]
        Ok(())
    }

    async fn check_slot(&self, slot: &ContractSlot) {
        self.check_slot_latency_arb(slot).await;
    }

    async fn check_slot_latency_arb(&self, slot: &ContractSlot) {
        let (oracle_now, oracle_age_ms) = match self.poly_ws_snapshot(slot.asset) {
            Some(v) => v,
            None => return,
        };

        if oracle_age_ms > (self.cfg.oracle_staleness_limit_secs as i64 * 1_000) {
            return;
        }

        let poly_from_agg = self.poly_snapshot(slot.asset, slot.timeframe);
        let used_fallback = poly_from_agg.is_none();
        self.risk.note_poly_lookup(used_fallback);

        let (up_mid, _book_depth_usdc, poly_tick_age_ms, best_bid_prob, best_ask_prob) =
            match poly_from_agg {
                Some(v) => v,
                None => {
                    match self.client.get_order_book(&slot.yes_token.token_id).await {
                        Ok(book) => match book.best_bid_ask() {
                            Some((bid, ask)) => ((bid + ask) / 2.0, 0.0, 0, Some(bid), Some(ask)),
                            None => match self.client.get_token_midpoint(&slot.yes_token.token_id).await {
                                Ok(mid) => {
                                    debug!(
                                        "[Detector] {:?}/{:?} using /midpoint fallback (empty /book)",
                                        slot.asset,
                                        slot.timeframe
                                    );
                                    (mid, 0.0, 0, None, None)
                                }
                                Err(_) => return,
                            },
                        },
                        Err(_) => match self.client.get_token_midpoint(&slot.yes_token.token_id).await {
                            Ok(mid) => {
                                debug!(
                                    "[Detector] {:?}/{:?} using /midpoint fallback (/book error)",
                                    slot.asset,
                                    slot.timeframe
                                );
                                (mid, 0.0, 0, None, None)
                            }
                            Err(_) => return,
                        },
                    }
                }
            };

        self.risk.note_poly_tick_age_ms(poly_tick_age_ms);

        let key = Self::slot_state_key(slot.asset, slot.timeframe);
        let open_price = {
            let mut states = self.states.lock();
            let state = states.entry(key).or_insert_with(|| {
                let max_shares = self.cfg.max_position_usdc() / 0.50;
                let init_strike = if slot.strike > 0.0 { slot.strike } else { oracle_now };
                SlotState::new(up_mid, init_strike, &self.cfg, max_shares, slot.time_to_expiry_secs())
            });

            if state
                .last_signal_eval
                .map(|t| t.elapsed() < Duration::from_secs(1))
                .unwrap_or(false)
            {
                return;
            }
            state.last_signal_eval = Some(Instant::now());

            // For up/down contracts, the reference is the oracle price at bucket open.
            self.updown_reference_strike(state, slot.timeframe, oracle_now)
        };

        if open_price <= 0.0 {
            return;
        }

        let pct_change = ((oracle_now - open_price) / open_price) * 100.0;
        let direction_up = if pct_change >= self.cfg.price_change_threshold_pct {
            true
        } else if pct_change <= -self.cfg.price_change_threshold_pct {
            false
        } else {
            return;
        };

        let secs_left = slot.time_to_expiry_secs();
        if secs_left <= self.cfg.settlement_buffer_seconds as f64 {
            return;
        }

        let yes_bid = best_bid_prob.unwrap_or(up_mid).clamp(0.0, 1.0);
        let yes_ask = best_ask_prob.unwrap_or(up_mid).clamp(0.0, 1.0);
        let spread = (yes_ask - yes_bid).max(0.0);
        if spread > self.cfg.max_spread {
            return;
        }

        let entry_prob = if direction_up {
            yes_ask.clamp(0.01, 0.99)
        } else {
            // Approximate NO ask from YES bid when NO top-of-book is unavailable.
            (1.0 - yes_bid).clamp(0.01, 0.99)
        };

        if !(self.cfg.polymarket_min_price <= entry_prob && entry_prob <= self.cfg.polymarket_max_price) {
            return;
        }

        // Oracle-lag style sizing: fixed risk fraction per trade.
        let target_size = self.cfg.portfolio_size_usdc * (self.cfg.risk_pct_per_trade / 100.0);
        if target_size <= 0.0 {
            return;
        }

        let mut snap = MarketSnapshot::new(
            slot.asset,
            slot.timeframe,
            oracle_now,
            up_mid,
            up_mid,
            1.0,
        );
        snap.direction_up = direction_up;
        snap.poly_entry_prob = entry_prob;
        // Use oracle move magnitude as signal-strength telemetry.
        snap.edge_pct = pct_change.abs();
        snap.strategy = "ORACLE_LAG".to_string();
        snap.models = ModelOutput {
            bayesian_posterior: up_mid,
            ci_lo: up_mid,
            ci_hi: up_mid,
            posterior_variance: 0.0,
            effective_n: 1.0,
            bayes_ready: false,
            reservation_price: up_mid,
            half_spread: spread / 2.0,
            stoikov_bid: (entry_prob - spread / 2.0).clamp(0.0, 1.0),
            stoikov_ask: (entry_prob + spread / 2.0).clamp(0.0, 1.0),
            inventory_q: 0.0,
            variance: 0.0,
            stoikov_ready: false,
            kelly_size_usdc: target_size,
            full_kelly_fraction: 0.0,
            kelly_has_edge: true,
        };

        info!(
            "[OracleLag] {:?}/{:?} {} | oracle_open={:.2} oracle_now={:.2} ({:+.3}%) | time_left={:.0}s entry={:.4}",
            slot.asset,
            slot.timeframe,
            if direction_up { "UP" } else { "DOWN" },
            open_price,
            oracle_now,
            pct_change,
            secs_left,
            entry_prob,
        );

        self.execute(slot, &snap).await;
    }

    async fn check_slot_legacy(&self, slot: &ContractSlot) {
        // ── Step 1: get real price ─────────────────────────────────────────────
        let real_price = match self.best_price(slot.asset) { Some(p) => p, None => return };

        // ── Step 2: get Polymarket mid-probability ────────────────────────────
        // Use aggregated Polymarket data to avoid redundant API calls and respect staleness
        let poly_from_agg = self.poly_snapshot(slot.asset, slot.timeframe);
        let used_fallback = poly_from_agg.is_none();
        self.risk.note_poly_lookup(used_fallback);

        let (poly_prob, book_depth_usdc, poly_tick_age_ms, best_bid_prob, best_ask_prob) = match poly_from_agg {
            Some(v) => v,
            None => {
                // Fallback to direct poll if aggregator is empty
                match self.client.get_order_book(&slot.yes_token.token_id).await {
                    Ok(book) => {
                        let depth = Self::book_depth_usdc(&book);
                        match book.best_bid_ask() {
                            Some((bid, ask)) => ((bid + ask) / 2.0, depth, 0, Some(bid), Some(ask)),
                            None => {
                                match self.client.get_token_midpoint(&slot.yes_token.token_id).await {
                                    Ok(mid) => {
                                        debug!(
                                            "[Detector] empty poly book ({:?}/{:?}) — using /midpoint fallback",
                                            slot.asset,
                                            slot.timeframe
                                        );
                                        (mid, 0.0, 0, None, None)
                                    }
                                    Err(_) => {
                                        debug!(
                                            "[Detector] empty poly book ({:?}/{:?})",
                                            slot.asset,
                                            slot.timeframe
                                        );
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        match self.client.get_token_midpoint(&slot.yes_token.token_id).await {
                            Ok(mid) => {
                                debug!(
                                    "[Detector] poly_prob err ({:?}/{:?}): {e}. Using /midpoint fallback",
                                    slot.asset,
                                    slot.timeframe
                                );
                                (mid, 0.0, 0, None, None)
                            }
                            Err(_) => {
                                debug!("[Detector] poly_prob err ({:?}/{:?}): {e}", slot.asset, slot.timeframe);
                                return;
                            }
                        }
                    }
                }
            }
        };

        self.risk.note_poly_tick_age_ms(poly_tick_age_ms);

        if let (Some(bid), Some(ask)) = (best_bid_prob, best_ask_prob) {
            let spread = (ask - bid).max(0.0);
            if spread > 0.10 {
                debug!(
                    "[Detector] {:?}/{:?} wide spread {:.4} (>0.10) — skip",
                    slot.asset,
                    slot.timeframe,
                    spread
                );
                return;
            }
        }

        let key = Self::slot_state_key(slot.asset, slot.timeframe);
        let timeframe_mins = match slot.timeframe { Timeframe::FiveMin => 5, Timeframe::FifteenMin => 15 };

        // All model state updates happen inside this lock scope
        let (snap_opt, model_out, compare_log) = {
            let mut states = self.states.lock();
            let state = states.entry(key).or_insert_with(|| {
                let max_shares = self.cfg.max_position_usdc() / 0.50;
                let init_strike = if slot.strike > 0.0 { slot.strike } else { real_price };
                SlotState::new(poly_prob, init_strike, &self.cfg, max_shares, slot.time_to_expiry_secs())
            });

            let prev_price = state.prev_price;

            // ── Step 3: Bayesian update ────────────────────────────────────────
            let (posterior_up, posterior_down) = if let Some(pp) = prev_price {
                let up = state.bayes_up.update(real_price, pp);
                let dn = state.bayes_down.update(real_price, pp);
                (up, dn)
            } else {
                (state.bayes_up.posterior_mean(), state.bayes_down.posterior_mean())
            };

            // Active hypothesis: which direction does the Bayesian model lean?
            // For up/down markets (strike == 0.0), use this candle's open as the
            // directional reference price for the timeframe bucket.
            let effective_strike = if slot.strike > 0.0 {
                slot.strike
            } else {
                self.updown_reference_strike(state, slot.timeframe, real_price)
            };
            let adaptive_vol = Self::adaptive_cex_vol(state, timeframe_mins);
            let cex_prob = cex_implied_probability(real_price, effective_strike, timeframe_mins, adaptive_vol);
            let direction_up = cex_prob > poly_prob;
            let posterior = if direction_up { posterior_up } else { posterior_down };

            let bayes_summary = if direction_up {
                state.bayes_up.summary()
            } else {
                state.bayes_down.summary()
            };

            // ── Step 4: Stoikov update ─────────────────────────────────────────
            if let Some(pp) = prev_price {
                state.stoikov.observe_price(real_price, pp);
            }
            // Tick time since last eval
            if let Some(last) = state.last_eval {
                state.stoikov.tick_time(last.elapsed().as_secs_f64());
            }
            state.last_eval = Some(Instant::now());

            let stoikov_out = state.stoikov.evaluate(poly_prob);

            // Update price history
            state.prev_price = Some(real_price);
            state.prev_poly_prob = Some(poly_prob);

            // ── Step 5: fast pre-filter ────────────────────────────────────────
            let binance_opt = self.binance_snapshot(slot.asset);
            let poly_ws_opt = self.poly_ws_snapshot(slot.asset);
            let price_lag_pct = if let (Some((b_px, _)), Some((p_px, _))) = (binance_opt, poly_ws_opt) {
                if p_px > 0.0 { (b_px - p_px).abs() / p_px * 100.0 } else { 0.0 }
            } else { 0.0 };

            // Real-time pure price lag (for dashboard)
            self.risk.latest_lags.insert(format!("{}", slot.timeframe), price_lag_pct);

            let prob_lag_pp = (cex_prob - poly_prob).abs() * 100.0;
            let bayes_edge_pp = (posterior - poly_prob).abs() * 100.0;

            // Sniper uses the RAW DOLLAR PRICE lag between Binance and Poly's underlying resolution feed!
            let raw_predictor_pass = price_lag_pct >= self.cfg.lag_threshold_pp;
            let quant_predictor_pass = bayes_edge_pp >= self.cfg.min_edge_pct;
            let prefilter_pass = raw_predictor_pass || quant_predictor_pass;

            let compare_due = slot.asset == Asset::Btc
                && state
                    .last_compare_log
                    .map(|t| t.elapsed() >= Duration::from_millis(COMPARE_LOG_INTERVAL_MS))
                    .unwrap_or(true);

            let compare_log = if compare_due {
                state.last_compare_log = Some(Instant::now());
                let no_token_id = state
                    .active_no_token_id
                    .clone()
                    .unwrap_or_else(|| slot.no_token.token_id.clone());
                let allow_no_token_refresh = state
                    .last_no_token_refresh
                    .map(|t| t.elapsed() >= Duration::from_secs(20))
                    .unwrap_or(true);
                let (binance_price, binance_age_ms) = self
                    .binance_snapshot(slot.asset)
                    .unwrap_or((real_price, -1));
                Some(CompareTelemetry {
                    binance_price,
                    binance_age_ms,
                    strike: effective_strike,
                    yes_bid: best_bid_prob.unwrap_or(poly_prob),
                    yes_ask: best_ask_prob.unwrap_or(poly_prob),
                    yes_mid: poly_prob,
                    cex_prob,
                    bayes_posterior: posterior,
                    price_lag_pct,
                    prob_lag_pp,
                    bayes_edge_pp,
                    direction_up,
                    prefilter_pass,
                    no_token_id,
                    allow_no_token_refresh,
                })
            } else {
                None
            };

            if !prefilter_pass {
                debug!("[Detector] {:?}/{:?} price_lag={:.3}% prob_lag={:.1}pp bayes_edge={:.1}pp — skip",
                    slot.asset, slot.timeframe, price_lag_pct, prob_lag_pp, bayes_edge_pp);
                (None, ModelOutput::default(), compare_log)
            } else {
                // ── Step 6: confidence score ───────────────────────────────────
                let streak = state.update_streak(direction_up);
                let price_latency_ms = self.latest
                    .get(&(PriceSource::Binance, slot.asset, None))
                    .map(|t| (Utc::now() - t.timestamp).num_milliseconds().max(0) as u64)
                    .unwrap_or(999);

                let confidence = compute_confidence(&ConfidenceInput {
                    source_count: self.source_count(slot.asset),
                    source_spread_pct: self.source_spread_pct(slot.asset),
                    streak,
                    edge_pct: bayes_edge_pp,
                    book_depth_usdc,
                    price_latency_ms,
                });

                // ── Step 8: Kelly sizing ───────────────────────────────────────
                let entry_price = if direction_up {
                    best_ask_prob.unwrap_or(poly_prob)
                } else {
                    best_bid_prob
                        .map(|bid| 1.0 - bid)
                        .unwrap_or(1.0 - poly_prob)
                };
                let kelly_inp = KellyInput {
                    posterior,
                    entry_price: entry_price.clamp(0.01, 0.99),
                    kelly_fraction: self.cfg.kelly_fraction,
                    portfolio_usdc: self.cfg.portfolio_size_usdc,
                    max_position_usdc: self.cfg.max_position_usdc(),
                    inventory_q: stoikov_out.inventory_q,
                    posterior_variance: bayes_summary.variance,
                    effective_n: bayes_summary.effective_n,
                };
                let mut kelly_out = kelly_size(&kelly_inp);
                let strategy_path = if raw_predictor_pass {
                    kelly_out.has_edge = true;
                    // Calculate simple size based on fixed fraction of portfolio
                    kelly_out.size_usdc = self.cfg.portfolio_size_usdc * (self.cfg.risk_pct_per_trade / 100.0);
                    "SNIPER"
                } else {
                    // ── STOIKOV RESERVATION GATE ──
                    // If predicting UP, we buy YES at entry_price. Must be <= reservation_price.
                    // If predicting DOWN, we buy NO at (1 - entry_price). Must be <= (1 - reservation_price).
                    // => entry_price >= reservation_price
                    let stoikov_pass = if direction_up {
                        entry_price <= stoikov_out.reservation_price
                    } else {
                        entry_price >= stoikov_out.reservation_price
                    };

                    if !stoikov_pass {
                        // Stoikov blocks the trade due to adverse inventory risk!
                        kelly_out.has_edge = false;
                    }
                    
                    "QUANT"
                };

                // Build model output
                let model_out = ModelOutput {
                    bayesian_posterior: posterior,
                    ci_lo: bayes_summary.ci_lo,
                    ci_hi: bayes_summary.ci_hi,
                    posterior_variance: bayes_summary.variance,
                    effective_n: bayes_summary.effective_n,
                    bayes_ready: bayes_summary.warmed_up,
                    reservation_price: stoikov_out.reservation_price,
                    half_spread: stoikov_out.half_spread,
                    stoikov_bid: stoikov_out.bid,
                    stoikov_ask: stoikov_out.ask,
                    inventory_q: stoikov_out.inventory_q,
                    variance: stoikov_out.variance,
                    stoikov_ready: stoikov_out.ready,
                    kelly_size_usdc: kelly_out.size_usdc,
                    full_kelly_fraction: kelly_out.full_kelly,
                    kelly_has_edge: kelly_out.has_edge,
                };

                let mut snap = MarketSnapshot::new(
                    slot.asset, slot.timeframe, real_price, poly_prob, cex_prob, confidence,
                );
                snap.models = model_out.clone();
                snap.direction_up = direction_up;
                snap.poly_entry_prob = entry_price.clamp(0.01, 0.99);
                snap.strategy = strategy_path.to_string();

                (Some(snap), model_out, compare_log)
            }
        };

        if let Some(c) = compare_log {
            let mut no_live = self
                .client
                .get_order_book(&c.no_token_id)
                .await
                .ok()
                .and_then(|book| book.best_bid_ask().map(|(b, a)| (b, a, (b + a) / 2.0)));

            let mut refreshed_no_token_id: Option<String> = None;

            // Up/down token IDs rotate frequently; refresh NO token by timeframe when needed.
            if no_live.is_none() && c.allow_no_token_refresh && slot.asset == Asset::Btc {
                if let Ok(market) = self.client.get_btc_market_for_timeframe(slot.timeframe).await {
                    if let Some(no_token) = market
                        .tokens
                        .iter()
                        .find(|t| t.outcome.eq_ignore_ascii_case("no"))
                        .or_else(|| market.tokens.iter().find(|t| t.outcome.eq_ignore_ascii_case("down")))
                    {
                        refreshed_no_token_id = Some(no_token.token_id.clone());
                        no_live = self
                            .client
                            .get_order_book(&no_token.token_id)
                            .await
                            .ok()
                            .and_then(|book| book.best_bid_ask().map(|(b, a)| (b, a, (b + a) / 2.0)));
                    }
                }
            }

            if c.allow_no_token_refresh || refreshed_no_token_id.is_some() {
                let key = Self::slot_state_key(slot.asset, slot.timeframe);
                let mut states = self.states.lock();
                if let Some(state) = states.get_mut(&key) {
                    state.last_no_token_refresh = Some(Instant::now());
                    if let Some(tok) = refreshed_no_token_id {
                        state.active_no_token_id = Some(tok);
                    }
                }
            }

            let (no_bid_s, no_ask_s, no_mid_s) = if let Some((b, a, m)) = no_live {
                (
                    format!("{:.4}", b),
                    format!("{:.4}", a),
                    format!("{:.4}", m),
                )
            } else {
                ("n/a".to_string(), "n/a".to_string(), "n/a".to_string())
            };

            // Value targets (fair prices) implied by CEX signal.
            let beat_yes = c.cex_prob.clamp(0.0, 1.0);
            let beat_no = (1.0 - c.cex_prob).clamp(0.0, 1.0);
            let yes_is_value = c.yes_ask <= beat_yes;
            let no_is_value = no_live.map(|(_, ask, _)| ask <= beat_no);
            let pair_ask_sum = no_live.map(|(_, ask, _)| c.yes_ask + ask);
            let pair_edge_to_par = pair_ask_sum.map(|sum| 1.0 - sum);
            let no_value_s = no_is_value
                .map(|v| if v { "yes" } else { "no" })
                .unwrap_or("n/a");
            let pair_sum_s = pair_ask_sum
                .map(|v| format!("{:.4}", v))
                .unwrap_or_else(|| "n/a".to_string());
            let pair_edge_s = pair_edge_to_par
                .map(|v| format!("{:+.4}", v))
                .unwrap_or_else(|| "n/a".to_string());

            info!(
                "[Compare] {:?}/{:?} | binance={:.2} age={}ms strike={:.2} | YES(bid={:.4} ask={:.4} mid={:.4}) NO(bid={} ask={} mid={}) | beat(YES<={:.4} NO<={:.4}) value(YES={} NO={}) pair(ask_sum={} edge_to_$1={}) | cex_prob={:.4} bayes={:.4} price_lag={:.3}% prob_lag={:.2}pp bayes_edge={:.2}pp dir={} prefilter={}",
                slot.asset,
                slot.timeframe,
                c.binance_price,
                c.binance_age_ms,
                c.strike,
                c.yes_bid,
                c.yes_ask,
                c.yes_mid,
                no_bid_s,
                no_ask_s,
                no_mid_s,
                beat_yes,
                beat_no,
                if yes_is_value { "yes" } else { "no" },
                no_value_s,
                pair_sum_s,
                pair_edge_s,
                c.cex_prob,
                c.bayes_posterior,
                c.price_lag_pct,
                c.prob_lag_pp,
                c.bayes_edge_pp,
                if c.direction_up { "UP" } else { "DOWN" },
                if c.prefilter_pass { "pass" } else { "fail" },
            );
        }

        // ── Step 7: edge + confidence gate ────────────────────────────────────
        if let Some(snap) = snap_opt {
            if !snap.has_edge(self.cfg.min_edge_pct, self.cfg.min_confidence) {
                debug!("[Detector] {:?}/{:?} eff_edge={:.1}% conf={:.0}% — below gates",
                    slot.asset, slot.timeframe, snap.effective_edge_pct(), snap.confidence * 100.0);
                return;
            }

            if !model_out.kelly_has_edge {
                debug!("[Detector] Kelly sees no edge — skip");
                return;
            }

            info!(
                "[Detector] ✦ [{}] {:?}/{:?} | bayes={:.4} [{:.3}–{:.3}] | r={:.4} | kelly=${:.2} | edge={:.1}% conf={:.0}% dir={}",
                snap.strategy,
                slot.asset, slot.timeframe,
                snap.models.bayesian_posterior, snap.models.ci_lo, snap.models.ci_hi,
                snap.models.reservation_price,
                snap.models.kelly_size_usdc,
                snap.effective_edge_pct(), snap.confidence * 100.0,
                if snap.direction_up { "▲ UP" } else { "▼ DOWN" }
            );

            self.execute(slot, &snap).await;
        }
    }

    async fn simulate_paper_settlement(&self, record: &TradeRecord, current_real_price: f64, strike: f64) {
        let opened_ts = record.opened_at.timestamp();
        let bucket_secs = if record.timeframe.contains("5m") { 300 } else { 900 };
        let expire_ts = opened_ts - (opened_ts % bucket_secs) + bucket_secs;
        
        let now_s = Utc::now().timestamp();
        if now_s >= expire_ts + 2 { // wait until just past the official candle close boundary
            // Prefer the persisted entry reference strike for accurate up/down settlement.
            let settlement_strike = record.entry_ref_price.unwrap_or(strike);
            let won = if record.direction.eq_ignore_ascii_case("UP") {
                current_real_price > settlement_strike
            } else {
                current_real_price < settlement_strike
            };
            
            // PnL uses Polymarket's share model: If won, we get 1.00 USDC per share.
            // shares = size_usdc / entry_prob
            let shares = record.size_usdc / record.entry_prob.max(0.001);
            let pnl = if won {
                shares * 1.0 - record.size_usdc
            } else {
                -record.size_usdc
            };
            if let Some(id) = record.id {
                let _ = self.db.close_trade(id, pnl, if won { "WIN" } else { "LOSS" });
                let slot_key = Self::record_state_key(record);
                let has_more_open_for_slot = self
                    .db
                    .open_positions()
                    .map(|opens| {
                        opens.iter().any(|t| {
                            let key = Self::record_state_key(t);
                            key == slot_key && t.is_open()
                        })
                    })
                    .unwrap_or(false);
                {
                    let mut states = self.states.lock();
                    if let Some(state) = states.get_mut(&slot_key) {
                        state.paper_position_open = has_more_open_for_slot;
                    }
                }
                self.risk.release_open_exposure(record.size_usdc);
                self.risk.record_pnl(pnl, won);
                self.risk.record_resolution_mismatch(true, won);
                info!(
                    "[Paper] Settled #{} {}/{} result={} pnl={:+.2} px={:.2} strike={:.2}",
                    id,
                    record.asset,
                    record.timeframe,
                    if won { "WIN" } else { "LOSS" },
                    pnl,
                    current_real_price,
                    settlement_strike
                );
            }
        }
    }

    async fn simulate_paper_redeem_settlement(&self, record: &TradeRecord, payouts: &[f64]) {
        let outcome_idx = if record.direction.eq_ignore_ascii_case("UP") { 0 } else { 1 };
        let payout_fraction = payouts.get(outcome_idx).copied().unwrap_or(0.0).clamp(0.0, 1.0);
        let shares = record.size_usdc / record.entry_prob.max(0.001);
        let returned = payout_fraction * shares;
        let pnl = returned - record.size_usdc;
        let won = payout_fraction > 0.0;

        if let Some(id) = record.id {
            let _ = self.db.close_trade(id, pnl, if won { "WIN" } else { "LOSS" });
            let slot_key = Self::record_state_key(record);
            let has_more_open_for_slot = self
                .db
                .open_positions()
                .map(|opens| {
                    opens.iter().any(|t| {
                        let key = Self::record_state_key(t);
                        key == slot_key && t.is_open()
                    })
                })
                .unwrap_or(false);
            {
                let mut states = self.states.lock();
                if let Some(state) = states.get_mut(&slot_key) {
                    state.paper_position_open = has_more_open_for_slot;
                }
            }
            self.risk.release_open_exposure(record.size_usdc);
            self.risk.record_pnl(pnl, won);
            self.risk.record_resolution_mismatch(true, won);
            info!(
                "[PaperRedeem] Settled #{} {}/{} result={} payout={:.2} pnl={:+.2}",
                id,
                record.asset,
                record.timeframe,
                if won { "WIN" } else { "LOSS" },
                payout_fraction,
                pnl
            );
        }
    }

    async fn execute(&self, slot: &ContractSlot, snap: &MarketSnapshot) {
        let slot_key = Self::slot_state_key(slot.asset, slot.timeframe);

        if self.cfg.min_trade_interval_ms > 0 {
            let min_interval = Duration::from_millis(self.cfg.min_trade_interval_ms);
            let throttled = {
                let states = self.states.lock();
                states
                    .get(&slot_key)
                    .and_then(|s| s.last_trade_at)
                    .map(|t| t.elapsed() < min_interval)
                    .unwrap_or(false)
            };

            if throttled {
                debug!(
                    "[Executor] Entry throttled for {:?}/{:?} (MIN_TRADE_INTERVAL_MS={}ms)",
                    slot.asset,
                    slot.timeframe,
                    self.cfg.min_trade_interval_ms
                );
                return;
            }
        }

        if !self.cfg.is_live() && self.cfg.paper_single_position_per_slot {
            let in_memory_open = {
                let states = self.states.lock();
                states
                    .get(&slot_key)
                    .map(|s| s.paper_position_open)
                    .unwrap_or(false)
            };

            let db_open_for_slot = self
                .db
                .open_positions()
                .map(|opens| {
                    let asset = slot.asset.to_string();
                    let timeframe = slot.timeframe.to_string();
                    opens.iter().any(|t| {
                        t.is_open()
                            && t.asset.eq_ignore_ascii_case(&asset)
                            && t.timeframe.eq_ignore_ascii_case(&timeframe)
                    })
                })
                .unwrap_or(false);

            if db_open_for_slot && !in_memory_open {
                let mut states = self.states.lock();
                if let Some(state) = states.get_mut(&slot_key) {
                    state.paper_position_open = true;
                }
            }

            if in_memory_open || db_open_for_slot {
                debug!(
                    "[Executor] Existing open paper position for {:?}/{:?} — skip",
                    slot.asset,
                    slot.timeframe
                );
                return;
            }
        }

        let size_usdc = match self.risk.approve_trade(snap.models.kelly_size_usdc) {
            Some(s) => s,
            None => { warn!("[Executor] Blocked by risk manager"); return; }
        };

        let token = if snap.direction_up { &slot.yes_token } else { &slot.no_token };
        let mut entry_prob = snap.poly_entry_prob.clamp(0.01, 0.99);
        let mut min_token_amount = DEFAULT_MIN_TOKEN_AMOUNT;

        // For live orders, use executable top-of-book ask on the exact token being bought.
        if self.cfg.is_live() {
            match self.client.get_order_book(&token.token_id).await {
                Ok(book) => match book.best_bid_ask() {
                    Some((bid, ask)) => {
                        if let Some(min_size) = book.min_order_size {
                            if min_size.is_finite() && min_size > 0.0 {
                                min_token_amount = min_size;
                            }
                        }

                        let spread = (ask - bid).max(0.0);
                        if spread > 0.10 {
                            warn!(
                                "[Executor] Wide spread {:.4} on token {} — skip order",
                                spread,
                                token.token_id
                            );
                            return;
                        }
                        entry_prob = ask.clamp(0.01, 0.99);
                    }
                    None => {
                        match self
                            .client
                            .get_token_best_price(&token.token_id, OrderSide::Buy)
                            .await
                        {
                            Ok(ask) => {
                                entry_prob = ask.clamp(0.01, 0.99);
                                if let Ok(spread) = self.client.get_token_spread(&token.token_id).await {
                                    if spread > 0.10 {
                                        warn!(
                                            "[Executor] Wide spread {:.4} on token {} via /spread — skip order",
                                            spread,
                                            token.token_id
                                        );
                                        return;
                                    }
                                }
                                warn!(
                                    "[Executor] Missing top-of-book for token {} — using /price BUY fallback",
                                    token.token_id
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "[Executor] Missing best bid/ask for token {} and /price fallback failed: {}",
                                    token.token_id,
                                    e
                                );
                                return;
                            }
                        }
                    }
                },
                Err(e) => {
                    match self
                        .client
                        .get_token_best_price(&token.token_id, OrderSide::Buy)
                        .await
                    {
                        Ok(ask) => {
                            entry_prob = ask.clamp(0.01, 0.99);
                            if let Ok(spread) = self.client.get_token_spread(&token.token_id).await {
                                if spread > 0.10 {
                                    warn!(
                                        "[Executor] Wide spread {:.4} on token {} via /spread — skip order",
                                        spread,
                                        token.token_id
                                    );
                                    return;
                                }
                            }
                            warn!(
                                "[Executor] /book failed for token {} ({}); using /price BUY fallback",
                                token.token_id,
                                e
                            );
                        }
                        Err(e2) => {
                            warn!(
                                "[Executor] Failed to refresh quote for token {} (/book: {}; /price: {})",
                                token.token_id,
                                e,
                                e2
                            );
                            return;
                        }
                    }
                }
            }
        }

        let direction_str = if snap.direction_up { "UP" } else { "DOWN" };
        let asset_str = format!("{}", snap.asset);
        let tf_str = format!("{}", snap.timeframe);
        let entry_ref_price = {
            let key = Self::slot_state_key(slot.asset, slot.timeframe);
            let states = self.states.lock();
            states
                .get(&key)
                .and_then(|s| s.candle_open_price)
                .or(Some(snap.real_price))
        };

        let record = TradeRecord {
            id: None,
            condition_id: Some(slot.condition_id.clone()),
            asset: asset_str.clone(),
            timeframe: tf_str.clone(),
            direction: direction_str.to_string(), size_usdc, entry_prob,
            entry_ref_price,
            cex_prob: snap.cex_implied_prob, edge_pct: snap.effective_edge_pct(),
            confidence: snap.confidence, kelly_fraction: self.cfg.kelly_fraction,
            paper: !self.cfg.is_live(), order_id: None, pnl_usdc: None,
            outcome: Some("OPEN".to_string()), opened_at: Utc::now(), closed_at: None,
        };

        // Paper mode
        if !self.cfg.is_live() {
            let row_id = self.db.insert_trade(&record).unwrap_or(0);
            self.risk.reserve_open_exposure(size_usdc);
            self.risk.record_fill_success();
            {
                let mut states = self.states.lock();
                if let Some(state) = states.get_mut(&slot_key) {
                    state.last_trade_at = Some(Instant::now());
                    state.paper_position_open = true;
                }
            }
            info!(
                "[{}] [Paper] #{row_id} {asset_str}/{tf_str} {direction_str} ${size_usdc:.2} \
                 bayes={:.4} r={:.4} kelly_f={:.3} edge={:.1}%",
                snap.strategy,
                snap.models.bayesian_posterior, snap.models.reservation_price,
                snap.models.full_kelly_fraction, snap.effective_edge_pct()
            );
            self.tg.trade_alert(true, &asset_str, &tf_str, direction_str,
                size_usdc, snap.effective_edge_pct(), snap.confidence, None).await;
            return;
        }

        // Live order: quantize to Polymarket-friendly precision.
        let order_price = floor_to_decimals(entry_prob, LIVE_ORDER_PRICE_DECIMALS).clamp(0.01, 0.99);
        if !order_price.is_finite() || order_price <= 0.0 {
            warn!(
                "[Executor] Invalid quantized order price {:.6} for token {} — skip order",
                order_price,
                token.token_id
            );
            return;
        }

        let shares = floor_to_decimals(size_usdc / order_price, LIVE_ORDER_SIZE_DECIMALS);
        if !shares.is_finite() || shares < min_token_amount {
            warn!(
                "[Executor] Share size {:.2} below minimum {:.2} for token {} — skip order",
                shares,
                min_token_amount,
                token.token_id
            );
            return;
        }

        let order_notional_usdc = floor_to_decimals(shares * order_price, LIVE_ORDER_PRICE_DECIMALS);
        if order_notional_usdc < self.cfg.min_trade_size_usdc {
            warn!(
                "[Executor] Quantized notional ${:.4} below MIN_TRADE_SIZE_USDC={:.4} — skip order",
                order_notional_usdc,
                self.cfg.min_trade_size_usdc
            );
            return;
        }

        let mut live_record = record.clone();
        live_record.size_usdc = order_notional_usdc;
        live_record.entry_prob = order_price;

        let t0 = Instant::now();
        match self.client.place_order(&OrderRequest {
            token_id: token.token_id.clone(), price: order_price, size: shares,
            side: OrderSide::Buy, order_type: OrderType::Fok,
            time_in_force: TimeInForce::FillOrKill,
        }).await {
            Ok(order_id) => {
                let ms = t0.elapsed().as_millis();
                info!(
                    "[Executor] ✓ {order_id} | {asset_str}/{tf_str} {direction_str} ${:.4} @ {:.4} ({:.2} tokens) | {ms}ms",
                    order_notional_usdc,
                    order_price,
                    shares
                );
                self.risk.reserve_open_exposure(order_notional_usdc);
                self.risk.record_fill_success();
                self.risk.record_slippage(order_price, order_price);

                // Record fill in Stoikov inventory
                {
                    let mut states = self.states.lock();
                    if let Some(state) = states.get_mut(&slot_key) {
                        state.last_trade_at = Some(Instant::now());
                        state
                            .stoikov
                            .record_fill(shares, order_notional_usdc, snap.direction_up);
                    }
                }

                live_record.order_id = Some(order_id.clone());
                self.db.insert_trade(&live_record).ok();

                self.tg.trade_alert(false, &asset_str, &tf_str, direction_str,
                    order_notional_usdc, snap.effective_edge_pct(), snap.confidence, Some(&order_id)).await;
            }
            Err(e) => {
                self.risk.record_missed_fill();
                warn!("[Executor] Order failed: {e}");
            }
        }
    }

    // ── Price helpers ─────────────────────────────────────────────────────────

    fn best_price(&self, asset: Asset) -> Option<f64> {
        for (src, max_age) in [
            (PriceSource::PolymarketWs, 5i64),
            (PriceSource::Binance, 3),
            (PriceSource::TradingView, 5),
            (PriceSource::CryptoQuant, 10),
        ] {
            if let Some(t) = self.latest.get(&(src, asset, None)) {
                if (Utc::now() - t.timestamp).num_seconds() <= max_age {
                    return Some(t.price);
                }
            }
        }
        None
    }

    fn poly_snapshot(&self, asset: Asset, timeframe: Timeframe) -> Option<PolySnapshot> {
        if let Some(t) = self
            .latest
            .get(&(PriceSource::Polymarket, asset, Some(timeframe)))
        {
            let tick_age_ms = (Utc::now() - t.timestamp).num_milliseconds().max(0);
            if tick_age_ms <= self.polymarket_max_age_ms() {
                return Some((
                    t.price,
                    t.book_depth_usdc.unwrap_or(0.0),
                    tick_age_ms,
                    t.book_best_bid_prob,
                    t.book_best_ask_prob,
                ));
            }
        }
        None
    }

    fn polymarket_max_age_ms(&self) -> i64 {
        (self.cfg.poly_poll_ms.saturating_mul(5)) as i64
    }

    fn source_count(&self, asset: Asset) -> usize {
        [
            PriceSource::Binance,
            PriceSource::PolymarketWs,
            PriceSource::TradingView,
            PriceSource::CryptoQuant,
        ]
            .iter()
            .filter(|&&src| {
                self.latest.get(&(src, asset, None))
                    .map(|t| (Utc::now() - t.timestamp).num_seconds() <= 10)
                    .unwrap_or(false)
            })
            .count()
    }

    fn source_spread_pct(&self, asset: Asset) -> f64 {
        let prices: Vec<f64> = [
            PriceSource::Binance,
            PriceSource::PolymarketWs,
            PriceSource::TradingView,
            PriceSource::CryptoQuant,
        ]
        .iter()
        .filter_map(|&src| {
            self.latest.get(&(src, asset, None)).and_then(|t| {
                if (Utc::now() - t.timestamp).num_seconds() <= 10 {
                    Some(t.price)
                } else {
                    None
                }
            })
        })
        .collect();
        if prices.len() < 2 { return 0.5; }
        let min = prices.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = prices.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        if min <= 0.0 { return 0.5; }
        (max - min) / min * 100.0
    }

    fn price_n_seconds_ago(
        history: &VecDeque<(i64, f64)>,
        now_ms: i64,
        lookback_secs: i64,
    ) -> Option<f64> {
        if history.is_empty() {
            return None;
        }

        if lookback_secs <= 0 {
            return history.back().map(|(_, p)| *p);
        }

        let target_ms = now_ms - lookback_secs * 1_000;
        let mut at_or_before: Option<f64> = None;
        let mut first_after: Option<f64> = None;

        for (ts, price) in history {
            if *ts <= target_ms {
                at_or_before = Some(*price);
            } else {
                first_after = Some(*price);
                break;
            }
        }

        at_or_before.or(first_after)
    }

    fn binance_snapshot(&self, asset: Asset) -> Option<(f64, i64)> {
        self.latest
            .get(&(PriceSource::Binance, asset, None))
            .map(|t| {
                let age_ms = (Utc::now() - t.timestamp).num_milliseconds().max(0);
                (t.price, age_ms)
            })
    }

    fn poly_ws_snapshot(&self, asset: Asset) -> Option<(f64, i64)> {
        self.latest
            .get(&(PriceSource::PolymarketWs, asset, None))
            .map(|t| {
                let age_ms = (Utc::now() - t.timestamp).num_milliseconds().max(0);
                (t.price, age_ms)
            })
    }

    fn adaptive_cex_vol(state: &SlotState, timeframe_mins: u32) -> Option<f64> {
        let sigma_tick = state.stoikov.vol.variance().sqrt();
        if !sigma_tick.is_finite() || sigma_tick <= 0.0 {
            return None;
        }

        let timeframe_scale = ((timeframe_mins as f64) * 60.0).sqrt();
        let sigma = sigma_tick * timeframe_scale;
        if sigma.is_finite() && sigma > 0.0 {
            Some(sigma)
        } else {
            None
        }
    }

    fn book_depth_usdc(book: &crate::polymarket::client::OrderBook) -> f64 {
        let mut bids = book.bids.clone();
        bids.sort_by(|a, b| b.price.total_cmp(&a.price));
        let bid_depth: f64 = bids
            .iter()
            .take(5)
            .map(|l| l.price.clamp(0.0, 1.0) * l.size.max(0.0))
            .sum();

        let mut asks = book.asks.clone();
        asks.sort_by(|a, b| a.price.total_cmp(&b.price));
        let ask_depth: f64 = asks
            .iter()
            .take(5)
            .map(|l| l.price.clamp(0.0, 1.0) * l.size.max(0.0))
            .sum();

        bid_depth + ask_depth
    }

    fn slot_state_key(asset: Asset, timeframe: Timeframe) -> (String, String) {
        (asset.to_string(), timeframe.to_string())
    }

    fn record_state_key(record: &TradeRecord) -> (String, String) {
        let asset = if record.asset.eq_ignore_ascii_case("btc") {
            "BTC".to_string()
        } else if record.asset.eq_ignore_ascii_case("eth") {
            "ETH".to_string()
        } else {
            record.asset.clone()
        };

        let tf = match record.timeframe.to_ascii_lowercase().as_str() {
            "5m" | "fivemin" | "five_min" | "five-minute" => "5m".to_string(),
            "15m" | "fifteenmin" | "fifteen_min" | "fifteen-minute" => "15m".to_string(),
            _ => record.timeframe.clone(),
        };

        (asset, tf)
    }

    fn updown_reference_strike(
        &self,
        state: &mut SlotState,
        timeframe: Timeframe,
        real_price: f64,
    ) -> f64 {
        let bucket_secs = Self::timeframe_bucket_secs(timeframe);
        let now_ts = Utc::now().timestamp();
        let bucket_start = now_ts - now_ts.rem_euclid(bucket_secs);

        if state.candle_bucket_start_ts != Some(bucket_start) {
            state.candle_bucket_start_ts = Some(bucket_start);
            state.candle_open_price = Some(real_price);
            debug!(
                "[Detector] {:?} new bucket start={} open={:.2}",
                timeframe, bucket_start, real_price
            );
        }

        state.candle_open_price.unwrap_or(real_price)
    }

    fn timeframe_bucket_secs(timeframe: Timeframe) -> i64 {
        match timeframe {
            Timeframe::FiveMin => 5 * 60,
            Timeframe::FifteenMin => 15 * 60,
        }
    }
}
