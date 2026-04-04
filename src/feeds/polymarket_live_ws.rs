/// Polymarket live-data websocket feed for Chainlink reference prices.
/// Topic: crypto_prices_chainlink
use anyhow::Result;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, info, warn};

use crate::proxy::connect_ws_with_proxy;
use crate::types::{Asset, PriceSource, PriceTick};

pub struct PolymarketLiveWsFeed {
    ws_url: String,
    symbol_includes: String,
    tx: broadcast::Sender<PriceTick>,
}

impl PolymarketLiveWsFeed {
    pub fn new(ws_url: &str, symbol_includes: &str, tx: broadcast::Sender<PriceTick>) -> Self {
        Self {
            ws_url: ws_url.to_string(),
            symbol_includes: symbol_includes.to_ascii_lowercase(),
            tx,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut reconnect_ms = 500u64;

        loop {
            match self.connect_and_stream().await {
                Ok(_) => {
                    reconnect_ms = 500;
                    info!("[PolymarketLiveWS] Stream ended, reconnecting...");
                }
                Err(e) => {
                    warn!(
                        "[PolymarketLiveWS] Error: {}. Reconnecting in {}ms...",
                        e, reconnect_ms
                    );
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(reconnect_ms)).await;
            reconnect_ms = (reconnect_ms.saturating_mul(3) / 2).min(10_000);
        }
    }

    async fn connect_and_stream(&self) -> Result<()> {
        let (mut ws, _) = connect_ws_with_proxy(self.ws_url.as_str()).await?;
        info!(
            "[PolymarketLiveWS] Connected to {} (symbol filter='{}')",
            self.ws_url,
            self.symbol_includes
        );

        let subscribe = json!({
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": ""
                }
            ]
        });
        ws.send(Message::Text(subscribe.to_string())).await?;

        while let Some(msg) = ws.next().await {
            let msg = msg?;
            match msg {
                Message::Text(text) => {
                    if let Some(tick) = self.parse_tick(&text) {
                        let _ = self.tx.send(tick);
                    }
                }
                Message::Binary(bin) => {
                    if let Ok(text) = std::str::from_utf8(&bin) {
                        if let Some(tick) = self.parse_tick(text) {
                            let _ = self.tx.send(tick);
                        }
                    }
                }
                Message::Ping(payload) => {
                    ws.send(Message::Pong(payload)).await?;
                }
                Message::Close(frame) => {
                    debug!("[PolymarketLiveWS] Closed: {:?}", frame);
                    return Ok(());
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn parse_tick(&self, raw: &str) -> Option<PriceTick> {
        let data: Value = serde_json::from_str(raw).ok()?;
        if data.get("topic")?.as_str()? != "crypto_prices_chainlink" {
            return None;
        }

        let payload = Self::normalize_payload(data.get("payload")?)?;

        let symbol = payload
            .get("symbol")
            .or_else(|| payload.get("pair"))
            .or_else(|| payload.get("ticker"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_ascii_lowercase();

        if !self.symbol_includes.is_empty() && !symbol.contains(&self.symbol_includes) {
            return None;
        }

        let asset = if symbol.contains("btc") {
            Asset::Btc
        } else if symbol.contains("eth") {
            Asset::Eth
        } else {
            return None;
        };

        let price = payload
            .get("value")
            .and_then(Self::to_f64)
            .or_else(|| payload.get("price").and_then(Self::to_f64))
            .or_else(|| payload.get("current").and_then(Self::to_f64))
            .or_else(|| payload.get("data").and_then(Self::to_f64))?;

        // Use local receive time so age in logs reflects ingestion latency at the bot,
        // not provider-side update cadence embedded in payload timestamps.
        let timestamp = Utc::now();

        Some(PriceTick {
            source: PriceSource::PolymarketWs,
            asset,
            timeframe: None,
            book_depth_usdc: None,
            book_best_bid_prob: None,
            book_best_ask_prob: None,
            price,
            timestamp,
        })
    }

    fn normalize_payload(payload: &Value) -> Option<Value> {
        match payload {
            Value::Object(_) => Some(payload.clone()),
            Value::String(s) => serde_json::from_str::<Value>(s).ok(),
            _ => None,
        }
    }

    fn to_f64(value: &Value) -> Option<f64> {
        let parsed = match value {
            Value::Number(n) => n.as_f64(),
            Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        }?;

        if parsed.is_finite() {
            Some(parsed)
        } else {
            None
        }
    }

}
