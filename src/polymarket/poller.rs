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
    strike: f64,
    interval_ms: u64,
    tx: broadcast::Sender<PriceTick>,
}

impl PolymarketPoller {
    pub fn new(
        client: Arc<PolymarketClient>,
        token_id: String,
        asset: Asset,
        strike: f64,
        interval_ms: u64,
        tx: broadcast::Sender<PriceTick>,
    ) -> Self {
        Self { client, token_id, asset, strike, interval_ms, tx }
    }

    pub async fn run(&self) -> Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(self.interval_ms));
        loop {
            interval.tick().await;
            match self.client.implied_btc_price(&self.token_id, self.strike).await {
                Ok(price) => {
                    debug!("[Polymarket/{:?}] implied={:.2}", self.asset, price);
                    let _ = self.tx.send(PriceTick {
                        source: PriceSource::Polymarket,
                        asset: self.asset,
                        price,
                        timestamp: Utc::now(),
                    });
                }
                Err(e) => warn!("[Polymarket/{:?}] poll error: {e}", self.asset),
            }
        }
    }
}
