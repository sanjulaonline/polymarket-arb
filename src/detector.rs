/// Detector — the strategy execution loop
/// ========================================
///
/// Decision pipeline per (asset, timeframe) contract slot, on every price tick:
///
///   1. Pull best real price (Binance > TradingView > CryptoQuant)
///   2. Pull Polymarket mid-probability from order book
///   3. ── BAYESIAN UPDATE ──────────────────────────────────────────────────
///      Feed the new price into BayesianEstimator → get posterior P(H|D)
///      This updates faster than the crowd can reprice the contract
///   4. ── STOIKOV EVALUATION ─────────────────────────────────────────────
///      Compute reservation price r = s - q·γ·σ²·(T-t)
///      Check if market is on the right side of our reservation price
///   5. Gate: lag ≥ threshold (fast pre-filter before expensive checks)
///   6. Score confidence (6-signal scorer)
///   7. Gate: edge ≥ MIN_EDGE_PCT and confidence ≥ MIN_CONFIDENCE
///   8. ── KELLY SIZING ────────────────────────────────────────────────────
///      f* = (b·posterior - q_loss) / b  → scaled by uncertainty + inventory
///   9. Risk approval (daily cap, kill switch)
///  10. Paper log OR live FOK order → record fill in Stoikov inventory

use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
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

// ── Contract slot ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ContractSlot {
    pub asset: Asset,
    pub timeframe: Timeframe,
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
    latest: Arc<DashMap<(PriceSource, Asset), PriceTick>>,
    /// Per-slot model state, keyed by (asset_str, timeframe_str)
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
        latest: Arc<DashMap<(PriceSource, Asset), PriceTick>>,
    ) -> Self {
        // Pre-init slot states with neutral priors (will be updated on first poll)
        let mut states = HashMap::new();
        for slot in &contracts {
            let key = (format!("{:?}", slot.asset), format!("{:?}", slot.timeframe));
            let max_shares = cfg.max_position_usdc() / 0.50; // approx at 50¢ per share
            states.insert(key, SlotState::new(
                0.50, slot.strike, &cfg, max_shares, slot.time_to_expiry_secs(),
            ));
        }

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

        while let Ok(_tick) = rx.recv().await {
            if self.risk.is_halted() { continue; }
            for slot in self.contracts.clone() {
                self.check_slot(&slot).await;
            }
        }
        Ok(())
    }

    async fn check_slot(&self, slot: &ContractSlot) {
        // ── Step 1: get real price ─────────────────────────────────────────────
        let real_price = match self.best_price(slot.asset) { Some(p) => p, None => return };

        // ── Step 2: get Polymarket mid-probability ────────────────────────────
        let poly_prob = match self.client.mid_probability(&slot.yes_token.token_id).await {
            Ok(p) => p,
            Err(e) => { debug!("[Detector] poly_prob err ({:?}/{:?}): {e}", slot.asset, slot.timeframe); return; }
        };

        let key = (format!("{:?}", slot.asset), format!("{:?}", slot.timeframe));
        let timeframe_mins = match slot.timeframe { Timeframe::FiveMin => 5, Timeframe::FifteenMin => 15 };

        // All model state updates happen inside this lock scope
        let (snap_opt, model_out) = {
            let mut states = self.states.lock();
            let state = states.entry(key).or_insert_with(|| {
                let max_shares = self.cfg.max_position_usdc() / 0.50;
                SlotState::new(poly_prob, slot.strike, &self.cfg, max_shares, slot.time_to_expiry_secs())
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
            let cex_prob = cex_implied_probability(real_price, slot.strike, timeframe_mins);
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
            let lag_pp = (cex_prob - poly_prob).abs() * 100.0;
            let bayes_edge_pp = (posterior - poly_prob).abs() * 100.0;

            // Must exceed lag threshold from either raw CEX or Bayesian
            if lag_pp < self.cfg.lag_threshold_pp && bayes_edge_pp < self.cfg.lag_threshold_pp {
                debug!("[Detector] {:?}/{:?} lag={:.1}pp bayes_edge={:.1}pp — skip",
                    slot.asset, slot.timeframe, lag_pp, bayes_edge_pp);
                (None, ModelOutput::default())
            } else {
                // ── Step 6: confidence score ───────────────────────────────────
                let streak = state.update_streak(direction_up);
                let price_latency_ms = self.latest
                    .get(&(PriceSource::Binance, slot.asset))
                    .map(|t| (Utc::now() - t.timestamp).num_milliseconds().max(0) as u64)
                    .unwrap_or(999);

                let confidence = compute_confidence(&ConfidenceInput {
                    source_count: self.source_count(slot.asset),
                    source_spread_pct: self.source_spread_pct(slot.asset),
                    streak,
                    edge_pct: bayes_edge_pp,
                    book_depth_usdc: 1_000.0,
                    price_latency_ms,
                });

                // ── Step 8: Kelly sizing ───────────────────────────────────────
                let entry_price = if direction_up { poly_prob } else { 1.0 - poly_prob };
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
                let kelly_out = kelly_size(&kelly_inp);

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

                (Some(snap), model_out)
            }
        };

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
                "[Detector] ✦ {:?}/{:?} | bayes={:.4} [{:.3}–{:.3}] | r={:.4} | kelly=${:.2} | edge={:.1}% conf={:.0}% dir={}",
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

    async fn execute(&self, slot: &ContractSlot, snap: &MarketSnapshot) {
        let size_usdc = match self.risk.approve_trade(snap.models.kelly_size_usdc) {
            Some(s) => s,
            None => { warn!("[Executor] Blocked by risk manager"); return; }
        };

        let token = if snap.direction_up { &slot.yes_token } else { &slot.no_token };
        let entry_prob = if snap.direction_up {
            snap.poly_implied_prob
        } else {
            1.0 - snap.poly_implied_prob
        };
        let direction_str = if snap.direction_up { "UP" } else { "DOWN" };
        let asset_str = format!("{}", snap.asset);
        let tf_str = format!("{}", snap.timeframe);

        let record = TradeRecord {
            id: None, asset: asset_str.clone(), timeframe: tf_str.clone(),
            direction: direction_str.to_string(), size_usdc, entry_prob,
            cex_prob: snap.cex_implied_prob, edge_pct: snap.effective_edge_pct(),
            confidence: snap.confidence, kelly_fraction: self.cfg.kelly_fraction,
            paper: !self.cfg.is_live(), order_id: None, pnl_usdc: None,
            outcome: Some("OPEN".to_string()), opened_at: Utc::now(), closed_at: None,
        };

        // Paper mode
        if !self.cfg.is_live() {
            let row_id = self.db.insert_trade(&record).unwrap_or(0);
            info!(
                "[Paper] #{row_id} {asset_str}/{tf_str} {direction_str} ${size_usdc:.2} \
                 bayes={:.4} r={:.4} kelly_f={:.3} edge={:.1}%",
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

                // Record fill in Stoikov inventory
                {
                    let key = (format!("{:?}", slot.asset), format!("{:?}", slot.timeframe));
                    let mut states = self.states.lock();
                    if let Some(state) = states.get_mut(&key) {
                        state.stoikov.record_fill(shares, size_usdc, snap.direction_up);
                    }
                }

                let mut r2 = record.clone();
                r2.order_id = Some(order_id.clone());
                self.db.insert_trade(&r2).ok();

                self.tg.trade_alert(false, &asset_str, &tf_str, direction_str,
                    size_usdc, snap.effective_edge_pct(), snap.confidence, Some(&order_id)).await;

                let est_pnl = size_usdc * snap.effective_edge_pct() / 100.0;
                self.risk.record_pnl(est_pnl, true);

                let daily = self.risk.daily_pnl();
                if daily < 0.0 && daily.abs() / self.cfg.daily_loss_cap_usdc() >= 0.5 {
                    self.tg.drawdown_alert(daily, self.cfg.daily_drawdown_kill_pct).await;
                }
                if self.risk.is_halted() { self.tg.kill_switch_alert(daily).await; }
            }
            Err(e) => warn!("[Executor] Order failed: {e}"),
        }
    }

    // ── Price helpers ─────────────────────────────────────────────────────────

    fn best_price(&self, asset: Asset) -> Option<f64> {
        for (src, max_age) in [
            (PriceSource::Binance, 3i64),
            (PriceSource::TradingView, 5),
            (PriceSource::CryptoQuant, 10),
        ] {
            if let Some(t) = self.latest.get(&(src, asset)) {
                if (Utc::now() - t.timestamp).num_seconds() <= max_age {
                    return Some(t.price);
                }
            }
        }
        None
    }

    fn source_count(&self, asset: Asset) -> usize {
        [PriceSource::Binance, PriceSource::TradingView, PriceSource::CryptoQuant]
            .iter()
            .filter(|&&src| {
                self.latest.get(&(src, asset))
                    .map(|t| (Utc::now() - t.timestamp).num_seconds() <= 10)
                    .unwrap_or(false)
            })
            .count()
    }

    fn source_spread_pct(&self, asset: Asset) -> f64 {
        let prices: Vec<f64> = [PriceSource::Binance, PriceSource::TradingView, PriceSource::CryptoQuant]
            .iter()
            .filter_map(|&src| {
                self.latest.get(&(src, asset)).and_then(|t| {
                    if (Utc::now() - t.timestamp).num_seconds() <= 10 { Some(t.price) } else { None }
                })
            })
            .collect();
        if prices.len() < 2 { return 0.5; }
        let min = prices.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = prices.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        if min <= 0.0 { return 0.5; }
        (max - min) / min * 100.0
    }
}
