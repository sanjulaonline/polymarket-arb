/// CryptoQuant real-time BTC price via their WebSocket API.
/// Docs: https://docs.cryptoquant.com/reference/websocket-api
use anyhow::Result;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info};
use url::Url;

use crate::types::{Asset, PriceSource, PriceTick};

const CQ_WS_URL: &str = "wss://ws.cryptoquant.com/v1/stream";

#[derive(Debug, Serialize)]
struct Subscribe {
    action: &'static str,
    channel: String,
    api_key: String,
}

#[derive(Debug, Deserialize)]
struct CQMessage {
    channel: Option<String>,
    data: Option<CQData>,
}

#[derive(Debug, Deserialize)]
struct CQData {
    price: Option<f64>,
    close: Option<f64>,    // some endpoints use 'close'
    value: Option<f64>,    // some use 'value'
}

pub struct CryptoQuantFeed {
    api_key: String,
    tx: broadcast::Sender<PriceTick>,
}

impl CryptoQuantFeed {
    pub fn new(api_key: &str, tx: broadcast::Sender<PriceTick>) -> Self {
        Self {
            api_key: api_key.to_string(),
            tx,
        }
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            match self.connect_and_stream().await {
                Ok(_) => info!("[CryptoQuant] Stream ended, reconnecting..."),
                Err(e) => {
                    error!("[CryptoQuant] Stream error: {e}, reconnecting in 3s...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                }
            }
        }
    }

    async fn connect_and_stream(&self) -> Result<()> {
        let url = Url::parse(CQ_WS_URL)?;
        let (mut ws, _) = connect_async(url).await?;
        info!("[CryptoQuant] Connected");

        // Subscribe to BTC market data (spot price)
        let sub = Subscribe {
            action: "subscribe",
            channel: "market-data/price/btcusdt".to_string(),
            api_key: self.api_key.clone(),
        };
        ws.send(Message::Text(serde_json::to_string(&sub)?)).await?;
        info!("[CryptoQuant] Subscribed to BTC price stream");

        while let Some(msg) = ws.next().await {
            let msg = msg?;
            match msg {
                Message::Text(text) => {
                    if let Some(price) = self.parse_price(&text) {
                        debug!("[CryptoQuant] price={:.2}", price);
                        let _ = self.tx.send(PriceTick {
                            source: PriceSource::CryptoQuant,
                            asset: Asset::Btc,
                            timeframe: None,
                            price,
                            timestamp: Utc::now(),
                        });
                    }
                }
                Message::Ping(p) => {
                    ws.send(Message::Pong(p)).await?;
                }
                Message::Close(_) => return Ok(()),
                _ => {}
            }
        }
        Ok(())
    }

    fn parse_price(&self, raw: &str) -> Option<f64> {
        let msg: CQMessage = serde_json::from_str(raw).ok()?;
        let data = msg.data?;
        // Try each field name in priority order
        data.price
            .or(data.close)
            .or(data.value)
            .filter(|&p| p > 0.0)
    }
}
