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
use std::collections::HashMap;
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
    latest: Arc<DashMap<(PriceSource, Asset, Option<Timeframe>), PriceTick>>,
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
        latest: Arc<DashMap<(PriceSource, Asset, Option<Timeframe>), PriceTick>>,
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
            "[Detector] Bayesian+Stoikov+Kelly pipeline | lag≥{:.0}pp edge≥{:.0}% conf≥{:.0}% | {}",
            self.cfg.lag_threshold_pp, self.cfg.min_edge_pct,
            self.cfg.min_confidence * 100.0,
            if self.cfg.is_live() { "LIVE 🔴" } else { "PAPER 📋" }
        );

        let mut settlement_iv = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                recv = rx.recv() => {
                    let tick = match recv {
                        Ok(tick) => tick,
                        Err(_) => return Ok(()),
                    };

                    if self.risk.is_halted() { continue; }

                    // Evaluate only on Polymarket contract ticks.
                    if tick.source != PriceSource::Polymarket {
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
                                debug!(
                                    "[Detector] empty poly book ({:?}/{:?})",
                                    slot.asset,
                                    slot.timeframe
                                );
                                return;
                            }
                        }
                    }
                    Err(e) => { debug!("[Detector] poly_prob err ({:?}/{:?}): {e}", slot.asset, slot.timeframe); return; }
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

            info!(
                "[Compare] {:?}/{:?} | binance={:.2} age={}ms strike={:.2} | YES(bid={:.4} ask={:.4} mid={:.4}) NO(bid={} ask={} mid={}) | cex_prob={:.4} bayes={:.4} price_lag={:.3}% prob_lag={:.2}pp bayes_edge={:.2}pp dir={} prefilter={}",
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
        let elapsed = Utc::now() - record.opened_at;
        let limit_secs = if record.timeframe.contains("5m") { 300 } else { 900 };
        
        if elapsed.num_seconds() >= limit_secs {
            // Prefer the persisted entry reference strike for accurate up/down settlement.
            let settlement_strike = record.entry_ref_price.unwrap_or(strike);
            let won = if record.direction.eq_ignore_ascii_case("UP") {
                current_real_price > settlement_strike
            } else {
                current_real_price < settlement_strike
            };
            
            let pnl = if won { record.size_usdc * 0.9 } else { -record.size_usdc };
            if let Some(id) = record.id {
                let _ = self.db.close_trade(id, pnl, if won { "WON" } else { "LOST" });
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
                info!(
                    "[Paper] Settled #{} {}/{} result={} pnl={:+.2} px={:.2} strike={:.2}",
                    id,
                    record.asset,
                    record.timeframe,
                    if won { "WON" } else { "LOST" },
                    pnl,
                    current_real_price,
                    settlement_strike
                );
            }
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

        // For live orders, use executable top-of-book ask on the exact token being bought.
        if self.cfg.is_live() {
            match self.client.get_order_book(&token.token_id).await {
                Ok(book) => match book.best_bid_ask() {
                    Some((bid, ask)) => {
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
                        warn!(
                            "[Executor] Missing best bid/ask for token {} — skip order",
                            token.token_id
                        );
                        return;
                    }
                },
                Err(e) => {
                    warn!(
                        "[Executor] Failed to refresh top-of-book ask for token {}: {}",
                        token.token_id,
                        e
                    );
                    return;
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
            id: None, asset: asset_str.clone(), timeframe: tf_str.clone(),
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

        // Live order
        let shares = (size_usdc / entry_prob).floor();
        if shares < 1.0 { return; }

        let t0 = Instant::now();
        match self.client.place_order(&OrderRequest {
            token_id: token.token_id.clone(), price: entry_prob, size: shares,
            side: OrderSide::Buy, order_type: OrderType::Fok,
            time_in_force: TimeInForce::FillOrKill,
        }).await {
            Ok(order_id) => {
                let ms = t0.elapsed().as_millis();
                info!("[Executor] ✓ {order_id} | {asset_str}/{tf_str} {direction_str} ${size_usdc:.2} | {ms}ms");
                self.risk.reserve_open_exposure(size_usdc);

                // Record fill in Stoikov inventory
                {
                    let mut states = self.states.lock();
                    if let Some(state) = states.get_mut(&slot_key) {
                        state.last_trade_at = Some(Instant::now());
                        state.stoikov.record_fill(shares, size_usdc, snap.direction_up);
                    }
                }

                let mut r2 = record.clone();
                r2.order_id = Some(order_id.clone());
                self.db.insert_trade(&r2).ok();

                self.tg.trade_alert(false, &asset_str, &tf_str, direction_str,
                    size_usdc, snap.effective_edge_pct(), snap.confidence, Some(&order_id)).await;
            }
            Err(e) => warn!("[Executor] Order failed: {e}"),
        }
    }

    // ── Price helpers ─────────────────────────────────────────────────────────

    fn best_price(&self, asset: Asset) -> Option<f64> {
        for (src, max_age) in [
            (PriceSource::Binance, 3i64),
            (PriceSource::PolymarketWs, 5),
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

    fn poly_snapshot(&self, asset: Asset, timeframe: Timeframe) -> Option<(f64, f64, i64, Option<f64>, Option<f64>)> {
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
