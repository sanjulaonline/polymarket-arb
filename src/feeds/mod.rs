pub mod binance;
pub mod cryptoquant;
pub mod polymarket_live_ws;
pub mod tradingview;

use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::debug;

use crate::types::{Asset, PriceSource, PriceTick, Timeframe};

/// Key for the latest-price map: (source, asset, timeframe)
/// Timeframe is None for spot feeds and Some(5m/15m) for Polymarket contract ticks.
type FeedKey = (PriceSource, Asset, Option<Timeframe>);

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
            if tick.source == PriceSource::Polymarket {
                debug!("[Aggregator] {} {} prob={:.4}", tick.source, tick.asset, tick.price);
            } else {
                debug!("[Aggregator] {} {} price={:.2}", tick.source, tick.asset, tick.price);
            }
            self.latest
                .insert((tick.source, tick.asset, tick.timeframe), tick.clone());
            let _ = self.tx.send(tick);
        }
        Ok(())
    }

    /// Best real-world price for `asset`. Prefers Binance → TradingView → CryptoQuant.
    /// Staleness limits: Binance 3s, TradingView 5s, CryptoQuant 10s.
    pub fn best_price(&self, asset: Asset) -> Option<f64> {
        let candidates = [
            (PriceSource::Binance, 3i64),
            (PriceSource::PolymarketWs, 5),
            (PriceSource::TradingView, 5),
            (PriceSource::CryptoQuant, 10),
        ];
        for (src, max_age_secs) in candidates {
            if let Some(t) = self.latest.get(&(src, asset, None)) {
                if (Utc::now() - t.timestamp).num_seconds() <= max_age_secs {
                    return Some(t.price);
                }
            }
        }
        None
    }

    /// Latest Polymarket implied price for (`asset`, `timeframe`) within `max_age_ms`.
    pub fn poly_price(&self, asset: Asset, timeframe: Timeframe, max_age_ms: i64) -> Option<f64> {
        let t = self
            .latest
            .get(&(PriceSource::Polymarket, asset, Some(timeframe)))?;
        if (Utc::now() - t.timestamp).num_milliseconds() <= max_age_ms {
            Some(t.price)
        } else {
            None
        }
    }

    pub fn latest_map(&self) -> Arc<DashMap<FeedKey, PriceTick>> {
        self.latest.clone()
    }
}

