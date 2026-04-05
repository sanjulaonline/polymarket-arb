use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use super::client::{OrderBook, PolymarketClient};
use crate::types::{Asset, PriceSource, PriceTick, Timeframe};

pub struct PolymarketPoller {
    client: Arc<PolymarketClient>,
    token_id: String,
    asset: Asset,
    timeframe: Timeframe,
    interval_ms: u64,
    tx: broadcast::Sender<PriceTick>,
}

impl PolymarketPoller {
    pub fn new(
        client: Arc<PolymarketClient>,
        token_id: String,
        asset: Asset,
        timeframe: Timeframe,
        _strike: f64, // kept for API compat; unused for up/down markets
        interval_ms: u64,
        tx: broadcast::Sender<PriceTick>,
    ) -> Self {
        Self { client, token_id, asset, timeframe, interval_ms, tx }
    }

    pub async fn run(&self) -> Result<()> {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_millis(self.interval_ms));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut active_token_id = self.token_id.clone();
        let mut last_rotate_attempt = Instant::now() - Duration::from_secs(30);
        let mut consecutive_empty_books: u32 = 0;
        let mut last_published_tick = Instant::now() - Duration::from_secs(30);

        loop {
            interval.tick().await;

            if last_published_tick.elapsed() >= Duration::from_secs(3)
                && last_rotate_attempt.elapsed() >= Duration::from_secs(2)
            {
                last_rotate_attempt = Instant::now();
                if let Some(new_token_id) = self.refresh_active_token().await {
                    if new_token_id != active_token_id {
                        info!(
                            "[Polymarket/{:?}/{:?}] stale book recovery: {} -> {}",
                            self.asset,
                            self.timeframe,
                            Self::short_token(&active_token_id),
                            Self::short_token(&new_token_id)
                        );
                        active_token_id = new_token_id;
                        consecutive_empty_books = 0;
                        continue;
                    }
                }
            }

            // Pull the full book once: derive both mid probability and depth from it.
            match self.client.get_order_book(&active_token_id).await {
                Ok(book) => {
                    if let Some((best_bid, best_ask)) = book.best_bid_ask() {
                        consecutive_empty_books = 0;
                        let spread = (best_ask - best_bid).max(0.0);
                        if spread > 0.10 {
                            debug!(
                                "[Polymarket/{:?}/{:?}] wide spread {:.4} (>0.10) — publish for telemetry only",
                                self.asset,
                                self.timeframe,
                                spread
                            );
                        }

                        let prob = (best_bid + best_ask) / 2.0;
                        let depth = Self::top_book_depth_usdc(&book);
                        debug!(
                            "[Polymarket/{:?}/{:?}] bid={:.4} ask={:.4} mid={:.4} spread={:.4} depth=${:.2}",
                            self.asset,
                            self.timeframe,
                            best_bid,
                            best_ask,
                            prob,
                            spread,
                            depth
                        );
                        let _ = self.tx.send(PriceTick {
                            source: PriceSource::Polymarket,
                            asset: self.asset,
                            timeframe: Some(self.timeframe),
                            book_depth_usdc: Some(depth),
                            book_best_bid_prob: Some(best_bid),
                            book_best_ask_prob: Some(best_ask),
                            price: prob,
                            timestamp: Utc::now(),
                        });
                        last_published_tick = Instant::now();
                    } else {
                        consecutive_empty_books = consecutive_empty_books.saturating_add(1);
                        if self
                            .publish_midpoint_fallback(&active_token_id, "empty_or_invalid_book")
                            .await
                        {
                            consecutive_empty_books = 0;
                            last_published_tick = Instant::now();
                            continue;
                        }

                        if consecutive_empty_books >= 5
                            && last_rotate_attempt.elapsed() >= Duration::from_secs(2)
                        {
                            last_rotate_attempt = Instant::now();
                            if let Some(new_token_id) = self.refresh_active_token().await {
                                if new_token_id != active_token_id {
                                    info!(
                                        "[Polymarket/{:?}/{:?}] empty book recovery: {} -> {}",
                                        self.asset,
                                        self.timeframe,
                                        Self::short_token(&active_token_id),
                                        Self::short_token(&new_token_id)
                                    );
                                    active_token_id = new_token_id;
                                    consecutive_empty_books = 0;
                                    continue;
                                }
                            }
                        }

                        debug!(
                            "[Polymarket/{:?}/{:?}] empty or invalid top-of-book",
                            self.asset,
                            self.timeframe
                        );
                    }
                }
                Err(e) => {
                    if self
                        .publish_midpoint_fallback(&active_token_id, "book_error")
                        .await
                    {
                        consecutive_empty_books = 0;
                        last_published_tick = Instant::now();
                        continue;
                    }

                    let e_txt = e.to_string();
                    if Self::is_missing_orderbook_error(&e_txt) {
                        if last_rotate_attempt.elapsed() >= Duration::from_secs(2) {
                            last_rotate_attempt = Instant::now();
                            if let Some(new_token_id) = self.refresh_active_token().await {
                                if new_token_id != active_token_id {
                                    info!(
                                        "[Polymarket/{:?}/{:?}] token rotated: {} -> {}",
                                        self.asset,
                                        self.timeframe,
                                        Self::short_token(&active_token_id),
                                        Self::short_token(&new_token_id)
                                    );
                                    active_token_id = new_token_id;
                                    consecutive_empty_books = 0;
                                    continue;
                                }
                            }
                        }

                        debug!(
                            "[Polymarket/{:?}/{:?}] missing orderbook for token {}",
                            self.asset,
                            self.timeframe,
                            Self::short_token(&active_token_id)
                        );
                    } else {
                        warn!("[Polymarket/{:?}/{:?}] poll error: {e}", self.asset, self.timeframe);
                    }
                }
            }
        }
    }

    async fn publish_midpoint_fallback(&self, token_id: &str, reason: &str) -> bool {
        match self.client.get_token_midpoint(token_id).await {
            Ok(mid) => {
                debug!(
                    "[Polymarket/{:?}/{:?}] {}: midpoint fallback mid={:.4} token={}",
                    self.asset,
                    self.timeframe,
                    reason,
                    mid,
                    Self::short_token(token_id)
                );

                let _ = self.tx.send(PriceTick {
                    source: PriceSource::Polymarket,
                    asset: self.asset,
                    timeframe: Some(self.timeframe),
                    book_depth_usdc: None,
                    book_best_bid_prob: None,
                    book_best_ask_prob: None,
                    price: mid,
                    timestamp: Utc::now(),
                });
                true
            }
            Err(e) => {
                debug!(
                    "[Polymarket/{:?}/{:?}] {}: midpoint fallback failed for token {}: {}",
                    self.asset,
                    self.timeframe,
                    reason,
                    Self::short_token(token_id),
                    e
                );
                false
            }
        }
    }

    async fn refresh_active_token(&self) -> Option<String> {
        // Current strategy tracks BTC up/down markets. Keep this explicit for safety.
        if self.asset != Asset::Btc {
            return None;
        }

        let market = match self.client.get_btc_market_for_timeframe(self.timeframe).await {
            Ok(m) => m,
            Err(e) => {
                debug!(
                    "[Polymarket/{:?}/{:?}] token rediscovery failed: {}",
                    self.asset,
                    self.timeframe,
                    e
                );
                return None;
            }
        };

        market
            .tokens
            .iter()
            .find(|t| t.outcome.eq_ignore_ascii_case("yes"))
            .or_else(|| {
                market
                    .tokens
                    .iter()
                    .find(|t| t.outcome.eq_ignore_ascii_case("up"))
            })
            .map(|t| t.token_id.clone())
    }

    fn is_missing_orderbook_error(err: &str) -> bool {
        err.contains("/book failed with HTTP 404")
            || err.contains("No orderbook exists for the requested token id")
    }

    fn short_token(token_id: &str) -> String {
        if token_id.len() <= 14 {
            return token_id.to_string();
        }
        format!("{}...{}", &token_id[..6], &token_id[token_id.len() - 6..])
    }

    fn top_book_depth_usdc(book: &OrderBook) -> f64 {
        // CLOB payload order is not guaranteed to be top-of-book first.
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
}

