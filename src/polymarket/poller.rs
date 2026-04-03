use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, warn};

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
        loop {
            interval.tick().await;
            // Pull the full book once: derive both mid probability and depth from it.
            match self.client.get_order_book(&self.token_id).await {
                Ok(book) => {
                    if let Some((best_bid, best_ask)) = book.best_bid_ask() {
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
                    } else {
                        debug!(
                            "[Polymarket/{:?}/{:?}] empty or invalid top-of-book",
                            self.asset,
                            self.timeframe
                        );
                    }
                }
                Err(e) => warn!("[Polymarket/{:?}/{:?}] poll error: {e}", self.asset, self.timeframe),
            }
        }
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

