pub mod binance;
pub mod cryptoquant;
pub mod tradingview;

use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::debug;

use crate::types::{Asset, PriceSource, PriceTick};

/// Key for the latest-price map: (source, asset)
type FeedKey = (PriceSource, Asset);

/// Aggregates prices from all feeds, keyed by (source, asset).
pub struct PriceAggregator {
    latest: Arc<DashMap<FeedKey, PriceTick>>,
    /// Re-broadcast channel — consumers subscribe to this
    pub tx: broadcast::Sender<PriceTick>,
}

impl PriceAggregator {
    pub fn new(tx: broadcast::Sender<PriceTick>) -> Self {
        Self {
            latest: Arc::new(DashMap::new()),
            tx,
        }
    }

    pub async fn run(&self, mut rx: broadcast::Receiver<PriceTick>) -> Result<()> {
        while let Ok(tick) = rx.recv().await {
            debug!("[Aggregator] {} {} price={:.2}", tick.source, tick.asset, tick.price);
            self.latest.insert((tick.source, tick.asset), tick.clone());
            let _ = self.tx.send(tick);
        }
        Ok(())
    }

    /// Best real-world price for `asset`. Prefers Binance → TradingView → CryptoQuant.
    /// Staleness limits: Binance 3s, TradingView 5s, CryptoQuant 10s.
    pub fn best_price(&self, asset: Asset) -> Option<f64> {
        let candidates = [
            (PriceSource::Binance, 3i64),
            (PriceSource::TradingView, 5),
            (PriceSource::CryptoQuant, 10),
        ];
        for (src, max_age_secs) in candidates {
            if let Some(t) = self.latest.get(&(src, asset)) {
                if (Utc::now() - t.timestamp).num_seconds() <= max_age_secs {
                    return Some(t.price);
                }
            }
        }
        None
    }

    /// Latest Polymarket implied price for `asset`.
    pub fn poly_price(&self, asset: Asset) -> Option<f64> {
        let t = self.latest.get(&(PriceSource::Polymarket, asset))?;
        if (Utc::now() - t.timestamp).num_seconds() <= 3 {
            Some(t.price)
        } else {
            None
        }
    }

    pub fn latest_map(&self) -> Arc<DashMap<FeedKey, PriceTick>> {
        self.latest.clone()
    }
}

