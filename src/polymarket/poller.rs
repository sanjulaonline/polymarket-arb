use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use super::client::PolymarketClient;
use crate::types::{Asset, PriceSource, PriceTick};

pub struct PolymarketPoller {
    client: Arc<PolymarketClient>,
    token_id: String,
    asset: Asset,
    interval_ms: u64,
    tx: broadcast::Sender<PriceTick>,
}

impl PolymarketPoller {
    pub fn new(
        client: Arc<PolymarketClient>,
        token_id: String,
        asset: Asset,
        _strike: f64, // kept for API compat; unused for up/down markets
        interval_ms: u64,
        tx: broadcast::Sender<PriceTick>,
    ) -> Self {
        Self { client, token_id, asset, interval_ms, tx }
    }

    pub async fn run(&self) -> Result<()> {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_millis(self.interval_ms));
        loop {
            interval.tick().await;
            // For up/down markets the YES-token mid-probability is the signal (0.0–1.0).
            // We don't derive a dollar price here — the Detector fetches poly_prob itself.
            // This poller feeds the Aggregator only for cross-source latency tracking.
            match self.client.mid_probability(&self.token_id).await {
                Ok(prob) => {
                    debug!("[Polymarket/{:?}] mid_prob={:.4}", self.asset, prob);
                    // Emit prob scaled to a nominal dollar range so PriceTick.price
                    // stays comparable when the aggregator logs spread across sources.
                    // The detector always re-fetches poly_prob directly from the book.
                    let _ = self.tx.send(PriceTick {
                        source: PriceSource::Polymarket,
                        asset: self.asset,
                        price: prob, // raw probability in [0, 1]
                        timestamp: Utc::now(),
                    });
                }
                Err(e) => warn!("[Polymarket/{:?}] poll error: {e}", self.asset),
            }
        }
    }
}

