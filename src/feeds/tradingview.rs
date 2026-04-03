/// TradingView real-time price feed via their WebSocket data API.
use anyhow::Result;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info};
use url::Url;

use crate::types::{Asset, PriceSource, PriceTick};

const TV_WS_URL: &str = "wss://data.tradingview.com/socket.io/websocket";

pub struct TradingViewFeed {
    symbol: String,
    asset: Asset,
    tx: broadcast::Sender<PriceTick>,
}

impl TradingViewFeed {
    pub fn new(symbol: &str, tx: broadcast::Sender<PriceTick>, asset: Asset) -> Self {
        Self { symbol: symbol.to_string(), asset, tx }
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            match self.connect_and_stream().await {
                Ok(_) => info!("[TradingView/{}] Reconnecting...", self.asset),
                Err(e) => {
                    error!("[TradingView/{}] {e}, reconnecting in 2s...", self.asset);
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                }
            }
        }
    }

    async fn connect_and_stream(&self) -> Result<()> {
        let url = Url::parse(TV_WS_URL)?;
        let (mut ws, _) = connect_async(url).await?;
        info!("[TradingView/{}] Connected to {}", self.asset, self.symbol);

        let quote_session = format!("qs_{}", &uuid::Uuid::new_v4().to_string().replace('-', "")[..12]);

        self.send_msg(&mut ws, "set_auth_token", &json!(["unauthorized_user_token"])).await?;
        self.send_msg(&mut ws, "quote_create_session", &json!([quote_session])).await?;
        self.send_msg(&mut ws, "quote_set_fields", &json!([quote_session, "lp"])).await?;
        self.send_msg(&mut ws, "quote_add_symbols", &json!([quote_session, self.symbol, {"flags": ["force_permission"]}])).await?;
        self.send_msg(&mut ws, "quote_fast_symbols", &json!([quote_session, self.symbol])).await?;

        while let Some(msg) = ws.next().await {
            let msg = msg?;
            match msg {
                Message::Text(text) => {
                    if text.contains("~h~") {
                        if let Some(hb) = Self::extract_payload(&text) {
                            let _ = ws.send(Message::Text(Self::wrap_msg(&hb))).await;
                        }
                        continue;
                    }
                    if let Some(price) = self.parse_price(&text) {
                        debug!("[TradingView/{}] price={:.2}", self.asset, price);
                        let _ = self.tx.send(PriceTick {
                            source: PriceSource::TradingView,
                            asset: self.asset,
                            price,
                            timestamp: Utc::now(),
                        });
                    }
                }
                Message::Close(_) => return Ok(()),
                Message::Ping(p) => { ws.send(Message::Pong(p)).await?; }
                _ => {}
            }
        }
        Ok(())
    }

    fn parse_price(&self, raw: &str) -> Option<f64> {
        let payload = Self::extract_payload(raw)?;
        let v: Value = serde_json::from_str(&payload).ok()?;
        if v.get("m")?.as_str()? != "qsd" { return None; }
        let p = v.get("p")?.as_array()?;
        let lp = p.get(1)?.get("v")?.get("lp")?.as_f64()?;
        if lp > 0.0 { Some(lp) } else { None }
    }

    async fn send_msg(
        &self,
        ws: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        func: &str,
        args: &Value,
    ) -> Result<()> {
        let msg = json!({"m": func, "p": args});
        ws.send(Message::Text(Self::wrap_msg(&msg.to_string()))).await?;
        Ok(())
    }

    fn wrap_msg(msg: &str) -> String { format!("~m~{}~m~{}", msg.len(), msg) }

    fn extract_payload(raw: &str) -> Option<String> {
        let parts: Vec<&str> = raw.split("~m~").collect();
        parts.get(3).map(|s| s.to_string()).or_else(|| parts.get(2).map(|s| s.to_string()))
    }
}
