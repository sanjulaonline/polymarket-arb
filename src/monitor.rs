use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::detector::ContractSlot;
use crate::polymarket::client::PolymarketClient;
use crate::types::Asset;

const MAX_PRICE_HISTORY: usize = 10;
const MAX_ARB_HISTORY: usize = 10;
const MONITOR_LOG_PATH: &str = "monitor.log";
const MONITOR_INTERVAL_SECS: u64 = 1;

#[derive(Debug, Clone)]
pub struct PriceData {
    pub market: String,
    pub coin: String,
    pub up_bid: f64,
    pub up_ask: f64,
    pub down_bid: f64,
    pub down_ask: f64,
    pub bid_sum: f64,
    pub ask_sum: f64,
    pub spread: f64,
    pub has_arbitrage: bool,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone)]
pub struct ArbitrageDetection {
    pub timestamp_ms: i64,
    pub up_ask: f64,
    pub down_ask: f64,
    pub ask_sum: f64,
    pub spread: f64,
    pub spread_percent: f64,
}

#[derive(Debug, Clone)]
pub struct ArbitrageRow {
    pub market: String,
    pub detection: ArbitrageDetection,
}

#[derive(Debug, Default)]
pub struct MonitorState {
    price_history: HashMap<String, Vec<PriceData>>,
    arbitrage_history: HashMap<String, Vec<ArbitrageDetection>>,
}

pub type SharedMonitorState = Arc<RwLock<MonitorState>>;

pub fn new_shared_monitor_state() -> SharedMonitorState {
    Arc::new(RwLock::new(MonitorState::default()))
}

impl MonitorState {
    fn add_price(&mut self, market: &str, price_data: PriceData) {
        let history = self
            .price_history
            .entry(market.to_string())
            .or_default();
        history.push(price_data);
        if history.len() > MAX_PRICE_HISTORY {
            history.remove(0);
        }
    }

    fn record_arbitrage(&mut self, market: &str, price_data: &PriceData) {
        let history = self
            .arbitrage_history
            .entry(market.to_string())
            .or_default();
        history.push(ArbitrageDetection {
            timestamp_ms: price_data.timestamp_ms,
            up_ask: price_data.up_ask,
            down_ask: price_data.down_ask,
            ask_sum: price_data.ask_sum,
            spread: price_data.spread,
            spread_percent: price_data.spread * 100.0,
        });

        if history.len() > MAX_ARB_HISTORY {
            history.remove(0);
        }
    }

    pub fn latest_prices(&self) -> Vec<PriceData> {
        let mut out = self
            .price_history
            .values()
            .filter_map(|rows| rows.last().cloned())
            .collect::<Vec<_>>();
        out.sort_by(|a, b| a.market.cmp(&b.market));
        out
    }

    pub fn recent_arbitrage(&self, limit: usize) -> Vec<ArbitrageRow> {
        let mut out = self
            .arbitrage_history
            .iter()
            .flat_map(|(market, rows)| {
                rows.iter().cloned().map(|detection| ArbitrageRow {
                    market: market.clone(),
                    detection,
                })
            })
            .collect::<Vec<_>>();

        out.sort_by(|a, b| b.detection.timestamp_ms.cmp(&a.detection.timestamp_ms));
        out.truncate(limit);
        out
    }
}

pub async fn run_price_monitor_loop(
    client: Arc<PolymarketClient>,
    slots: Vec<ContractSlot>,
    state: SharedMonitorState,
    arbitrage_threshold: f64,
    mut shutdown: watch::Receiver<bool>,
) {
    if slots.is_empty() {
        warn!("[Monitor] no contract slots configured; monitor loop not started");
        return;
    }

    let mut tracked_slots = slots;
    let mut interval = tokio::time::interval(Duration::from_secs(MONITOR_INTERVAL_SECS));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    info!(
        "[Monitor] started | interval={}s | pair_ask_arb_threshold={:.4}",
        MONITOR_INTERVAL_SECS,
        arbitrage_threshold
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                for slot in tracked_slots.iter_mut() {
                    let market = format!("{} {}", slot.asset, slot.timeframe);
                    let coin = slot.asset.to_string();

                    let (up_book_res, down_book_res) = tokio::join!(
                        client.get_order_book(&slot.yes_token.token_id),
                        client.get_order_book(&slot.no_token.token_id),
                    );

                    let mut missing_book = false;

                    let up_book = match up_book_res {
                        Ok(book) => book.best_bid_ask(),
                        Err(e) => {
                            if is_missing_orderbook_error(&e.to_string()) {
                                missing_book = true;
                            } else {
                                debug!(
                                    "[Monitor] {} YES book error ({}): {}",
                                    market,
                                    short_token(&slot.yes_token.token_id),
                                    e
                                );
                            }
                            None
                        }
                    };

                    let down_book = match down_book_res {
                        Ok(book) => book.best_bid_ask(),
                        Err(e) => {
                            if is_missing_orderbook_error(&e.to_string()) {
                                missing_book = true;
                            } else {
                                debug!(
                                    "[Monitor] {} NO book error ({}): {}",
                                    market,
                                    short_token(&slot.no_token.token_id),
                                    e
                                );
                            }
                            None
                        }
                    };

                    if missing_book && refresh_slot_tokens(client.as_ref(), slot).await {
                        continue;
                    }

                    let price_data = create_price_data(
                        &market,
                        &coin,
                        up_book,
                        down_book,
                        arbitrage_threshold,
                    );

                    append_monitor_log(&price_data);

                    {
                        let mut guard = state.write();
                        guard.add_price(&market, price_data.clone());
                        if price_data.has_arbitrage {
                            guard.record_arbitrage(&market, &price_data);
                        }
                    }

                    if price_data.has_arbitrage {
                        info!(
                            "[Monitor] ARB {} | UP_ASK={:.4} + DOWN_ASK={:.4} = {:.4} | spread_to_threshold={:+.4}",
                            market,
                            price_data.up_ask,
                            price_data.down_ask,
                            price_data.ask_sum,
                            price_data.spread
                        );
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    info!("[Monitor] shutdown signal received");
                    break;
                }
            }
        }
    }
}

fn create_price_data(
    market: &str,
    coin: &str,
    up_book: Option<(f64, f64)>,
    down_book: Option<(f64, f64)>,
    arbitrage_threshold: f64,
) -> PriceData {
    let (up_bid, up_ask) = up_book.unwrap_or((0.0, 0.0));
    let (down_bid, down_ask) = down_book.unwrap_or((0.0, 0.0));

    let bid_sum = up_bid + down_bid;
    let ask_sum = up_ask + down_ask;
    let spread = arbitrage_threshold - ask_sum;
    let has_arbitrage = up_ask > 0.0
        && down_ask > 0.0
        && ask_sum < arbitrage_threshold
        && spread > 0.0;

    PriceData {
        market: market.to_string(),
        coin: coin.to_string(),
        up_bid,
        up_ask,
        down_bid,
        down_ask,
        bid_sum,
        ask_sum,
        spread,
        has_arbitrage,
        timestamp_ms: Utc::now().timestamp_millis(),
    }
}

async fn refresh_slot_tokens(client: &PolymarketClient, slot: &mut ContractSlot) -> bool {
    if slot.asset != Asset::Btc {
        return false;
    }

    match client.get_btc_market_for_timeframe(slot.timeframe).await {
        Ok(market) => {
            let yes_token = market
                .tokens
                .iter()
                .find(|t| t.outcome.eq_ignore_ascii_case("yes"))
                .or_else(|| market.tokens.iter().find(|t| t.outcome.eq_ignore_ascii_case("up")));
            let no_token = market
                .tokens
                .iter()
                .find(|t| t.outcome.eq_ignore_ascii_case("no"))
                .or_else(|| market.tokens.iter().find(|t| t.outcome.eq_ignore_ascii_case("down")));

            let (Some(yes_token), Some(no_token)) = (yes_token, no_token) else {
                return false;
            };

            let prev_yes = slot.yes_token.token_id.clone();
            let prev_no = slot.no_token.token_id.clone();
            slot.yes_token = yes_token.clone();
            slot.no_token = no_token.clone();
            slot.title = market.question;
            slot.condition_id = market.condition_id;

            info!(
                "[Monitor] token refresh {} {} | YES {} -> {} | NO {} -> {}",
                slot.asset,
                slot.timeframe,
                short_token(&prev_yes),
                short_token(&slot.yes_token.token_id),
                short_token(&prev_no),
                short_token(&slot.no_token.token_id),
            );
            true
        }
        Err(e) => {
            debug!(
                "[Monitor] token refresh failed for {} {}: {}",
                slot.asset,
                slot.timeframe,
                e
            );
            false
        }
    }
}

fn append_monitor_log(price_data: &PriceData) {
    let mut file = match OpenOptions::new()
        .create(true)
        .append(true)
        .open(MONITOR_LOG_PATH)
    {
        Ok(file) => file,
        Err(e) => {
            warn!("[Monitor] unable to open {}: {}", MONITOR_LOG_PATH, e);
            return;
        }
    };

    let file_len = file.metadata().ok().map(|m| m.len()).unwrap_or(0);
    if file_len == 0 {
        let _ = writeln!(
            file,
            "time,market,coin,up_bid,up_ask,down_bid,down_ask,bid_sum,ask_sum,spread,has_arbitrage"
        );
    }

    let ts = format_timestamp(price_data.timestamp_ms);
    let _ = writeln!(
        file,
        "{},{},{},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{:+.4},{}",
        ts,
        price_data.market,
        price_data.coin,
        price_data.up_bid,
        price_data.up_ask,
        price_data.down_bid,
        price_data.down_ask,
        price_data.bid_sum,
        price_data.ask_sum,
        price_data.spread,
        price_data.has_arbitrage,
    );
}

fn short_token(token_id: &str) -> String {
    if token_id.len() <= 14 {
        return token_id.to_string();
    }
    format!("{}...{}", &token_id[..6], &token_id[token_id.len() - 6..])
}

fn is_missing_orderbook_error(err: &str) -> bool {
    err.contains("/book failed with HTTP 404")
        || err.contains("No orderbook exists for the requested token id")
}

fn format_timestamp(timestamp_ms: i64) -> String {
    let dt = DateTime::from_timestamp_millis(timestamp_ms).unwrap_or_else(Utc::now);
    let est_offset = chrono::Duration::hours(-5);
    let est_time = dt + est_offset;
    est_time.format("%Y-%m-%d %H:%M:%S EST").to_string()
}
