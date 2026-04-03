/// Binance WebSocket feed — streams BTC and ETH prices via the public
/// combined stream endpoint: wss://stream.binance.com:9443/stream
use anyhow::Result;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info};
use url::Url;

use crate::types::{Asset, PriceSource, PriceTick};

const BINANCE_WS: &str = "wss://data-stream.binance.com/stream";

#[derive(Debug, Deserialize)]
struct StreamWrapper {
    stream: String,
    data: MiniTicker,
}

#[derive(Debug, Deserialize)]
struct MiniTicker {
    #[serde(rename = "c")]
    close: String, // last price as string
}

pub struct BinanceFeed {
    tx: broadcast::Sender<PriceTick>,
}

impl BinanceFeed {
    pub fn new(tx: broadcast::Sender<PriceTick>) -> Self {
        Self { tx }
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            match self.connect_and_stream().await {
                Ok(_) => info!("[Binance] Stream ended, reconnecting..."),
                Err(e) => {
                    error!("[Binance] Error: {e}, reconnecting in 2s...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                }
            }
        }
    }

    async fn connect_and_stream(&self) -> Result<()> {
        // Subscribe to BTC and ETH mini-ticker streams
        let url = Url::parse_with_params(
            BINANCE_WS,
            &[("streams", "btcusdt@miniTicker/ethusdt@miniTicker")],
        )?;

        let (mut ws, _) = connect_async(url).await?;
        info!("[Binance] Connected — streaming BTCUSDT + ETHUSDT");

        while let Some(msg) = ws.next().await {
            let msg = msg?;
            match msg {
                Message::Text(text) => {
                    if let Some((asset, price)) = self.parse(&text) {
                        debug!("[Binance] {:?} price={:.2}", asset, price);
                        let _ = self.tx.send(PriceTick {
                            source: PriceSource::Binance,
                            asset,
                            timeframe: None,
                            book_depth_usdc: None,
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

    fn parse(&self, raw: &str) -> Option<(Asset, f64)> {
        let wrapper: StreamWrapper = serde_json::from_str(raw).ok()?;
        let price: f64 = wrapper.data.close.parse().ok()?;
        let stream = wrapper.stream.to_lowercase();
        let asset = if stream.contains("btcusdt") {
            Asset::Btc
        } else if stream.contains("ethusdt") {
            Asset::Eth
        } else {
            return None;
        };
        Some((asset, price))
    }
}
