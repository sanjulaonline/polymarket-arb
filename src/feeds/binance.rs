/// Binance WebSocket feed — streams BTC and ETH trade prices via the public
/// combined stream endpoint: wss://stream.binance.com:9443/stream
use anyhow::Result;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info};
use url::Url;

use crate::types::{Asset, PriceSource, PriceTick};

const BINANCE_WS: &str = "wss://stream.binance.com:9443/stream";

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

#[derive(Debug, Deserialize)]
struct TradeEvent {
    #[serde(rename = "p")]
    price: String,
}

#[derive(Debug, Deserialize)]
struct StreamEnvelope {
    stream: String,
    data: Value,
}

pub struct BinanceFeed {
    tx: broadcast::Sender<PriceTick>,
    enable_signal_1s: bool,
    signal_threshold_pct: f64,
}

impl BinanceFeed {
    pub fn new(
        tx: broadcast::Sender<PriceTick>,
        enable_signal_1s: bool,
        signal_threshold_pct: f64,
    ) -> Self {
        Self {
            tx,
            enable_signal_1s,
            signal_threshold_pct,
        }
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
        // Subscribe to BTC and ETH trade streams + BTC 1s closed kline signal stream.
        let url = Url::parse_with_params(
            BINANCE_WS,
            &[("streams", "btcusdt@trade/ethusdt@trade/btcusdt@kline_1s")],
        )?;

        let (mut ws, _) = connect_async(url).await?;
        info!(
            "[Binance] Connected — streaming BTCUSDT/ETHUSDT trades{}",
            if self.enable_signal_1s {
                " + BTCUSDT 1s kline signal"
            } else {
                ""
            }
        );

        let mut prev_btc_1s_close: Option<f64> = None;

        while let Some(msg) = ws.next().await {
            let msg = msg?;
            match msg {
                Message::Text(text) => {
                    let envelope = match serde_json::from_str::<StreamEnvelope>(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    // Trade stream is primary; keep miniTicker parser as compatibility fallback.
                    let parsed = self
                        .parse_trade(&envelope)
                        .or_else(|| self.parse_miniticker(&envelope));

                    if let Some((asset, price)) = parsed {
                        debug!("[Binance] {:?} price={:.2}", asset, price);
                        let _ = self.tx.send(PriceTick {
                            source: PriceSource::Binance,
                            asset,
                            timeframe: None,
                            book_depth_usdc: None,
                            book_best_bid_prob: None,
                            book_best_ask_prob: None,
                            price,
                            timestamp: Utc::now(),
                        });
                    }

                    if self.enable_signal_1s {
                        if let Some(close) = Self::parse_closed_btc_1s_kline_close(&envelope) {
                            if let Some(prev) = prev_btc_1s_close {
                                if prev > 0.0 {
                                    let change = (close - prev) / prev * 100.0;
                                    if change >= self.signal_threshold_pct {
                                        info!(
                                            "[BinanceSignal] BTC 1s UP +{:.3}% (prev={:.2} now={:.2})",
                                            change,
                                            prev,
                                            close
                                        );
                                    } else if change <= -self.signal_threshold_pct {
                                        info!(
                                            "[BinanceSignal] BTC 1s DOWN {:.3}% (prev={:.2} now={:.2})",
                                            change,
                                            prev,
                                            close
                                        );
                                    }
                                }
                            }
                            prev_btc_1s_close = Some(close);
                        }
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

    fn parse_miniticker(&self, envelope: &StreamEnvelope) -> Option<(Asset, f64)> {
        if !envelope.stream.to_lowercase().contains("@miniticker") {
            return None;
        }

        let wrapper = StreamWrapper {
            stream: envelope.stream.clone(),
            data: serde_json::from_value::<MiniTicker>(envelope.data.clone()).ok()?,
        };

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

    fn parse_trade(&self, envelope: &StreamEnvelope) -> Option<(Asset, f64)> {
        if !envelope.stream.to_lowercase().contains("@trade") {
            return None;
        }

        let trade = TradeEvent {
            price: serde_json::from_value::<TradeEvent>(envelope.data.clone()).ok()?.price,
        };
        let price: f64 = trade.price.parse().ok()?;
        let stream = envelope.stream.to_lowercase();
        let asset = if stream.contains("btcusdt") {
            Asset::Btc
        } else if stream.contains("ethusdt") {
            Asset::Eth
        } else {
            return None;
        };

        Some((asset, price))
    }

    fn parse_closed_btc_1s_kline_close(envelope: &StreamEnvelope) -> Option<f64> {
        if !envelope.stream.to_lowercase().contains("btcusdt@kline_1s") {
            return None;
        }

        let k = envelope.data.get("k")?;
        let is_closed = k.get("x")?.as_bool()?;
        if !is_closed {
            return None;
        }

        k.get("c")?.as_str()?.parse::<f64>().ok()
    }
}
