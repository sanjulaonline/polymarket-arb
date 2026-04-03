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
                    if let Some(prob) = book.mid_price() {
                        let depth = Self::top_book_depth_usdc(&book);
                        debug!(
                            "[Polymarket/{:?}/{:?}] mid_prob={:.4} depth=${:.2}",
                            self.asset,
                            self.timeframe,
                            prob,
                            depth
                        );
                        let _ = self.tx.send(PriceTick {
                            source: PriceSource::Polymarket,
                            asset: self.asset,
                            timeframe: Some(self.timeframe),
                            book_depth_usdc: Some(depth),
                            price: prob,
                            timestamp: Utc::now(),
                        });
                    } else {
                        warn!(
                            "[Polymarket/{:?}/{:?}] empty book (no mid)",
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
        let bid_depth: f64 = book
            .bids
            .iter()
            .take(5)
            .map(|l| l.price.max(0.0) * l.size.max(0.0))
            .sum();
        let ask_depth: f64 = book
            .asks
            .iter()
            .take(5)
            .map(|l| l.price.max(0.0) * l.size.max(0.0))
            .sum();
        bid_depth + ask_depth
    }
}

