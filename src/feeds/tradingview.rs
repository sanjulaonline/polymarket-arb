/// TradingView real-time price feed via their WebSocket data API.
use anyhow::{anyhow, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_tungstenite::{
    tungstenite::{
        client::IntoClientRequest,
        http::header::{CACHE_CONTROL, ORIGIN, PRAGMA, REFERER, USER_AGENT},
        http::HeaderValue,
        protocol::Message,
        Error as WsError,
    },
};
use tracing::{debug, error, info};

use crate::proxy::connect_ws_with_proxy;
use crate::types::{Asset, PriceSource, PriceTick};

const TV_WS_URLS: [&str; 3] = [
    "wss://data.tradingview.com/socket.io/websocket",
    "wss://data.tradingview.com/socket.io/websocket?from=chart%2F",
    "wss://prodata.tradingview.com/socket.io/websocket",
];

type TvSocket = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

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
        let mut retry_secs = 2u64;
        loop {
            match self.connect_and_stream().await {
                Ok(_) => {
                    retry_secs = 2;
                    info!("[TradingView/{}] Reconnecting...", self.asset);
                }
                Err(e) => {
                    if Self::is_http_403(&e) {
                        retry_secs = 60;
                        error!(
                            "[TradingView/{}] HTTP 403 Forbidden (handshake blocked). Retrying in {}s...",
                            self.asset,
                            retry_secs
                        );
                    } else {
                        error!(
                            "[TradingView/{}] {e}, reconnecting in {}s...",
                            self.asset,
                            retry_secs
                        );
                        retry_secs = (retry_secs * 2).min(30);
                    }
                    tokio::time::sleep(Duration::from_secs(retry_secs)).await;
                }
            }
        }
    }

    async fn connect_and_stream(&self) -> Result<()> {
        let (mut ws, endpoint) = self.connect_with_fallback().await?;
        info!(
            "[TradingView/{}] Connected to {} via {}",
            self.asset,
            self.symbol,
            endpoint
        );

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
                    for payload in Self::extract_payloads(&text) {
                        if payload.starts_with("~h~") {
                            let _ = ws.send(Message::Text(Self::wrap_msg(&payload))).await;
                            continue;
                        }

                        if let Some(price) = self.parse_price_payload(&payload) {
                            debug!("[TradingView/{}] price={:.2}", self.asset, price);
                            let _ = self.tx.send(PriceTick {
                                source: PriceSource::TradingView,
                                asset: self.asset,
                                timeframe: None,
                                book_depth_usdc: None,
                                book_best_bid_prob: None,
                                book_best_ask_prob: None,
                                price,
                                timestamp: Utc::now(),
                            });
                        }
                    }
                }
                Message::Close(_) => return Ok(()),
                Message::Ping(p) => { ws.send(Message::Pong(p)).await?; }
                _ => {}
            }
        }
        Ok(())
    }

    async fn connect_with_fallback(&self) -> Result<(TvSocket, &'static str)> {
        let mut last_err: Option<anyhow::Error> = None;

        for endpoint in TV_WS_URLS {
            match self.connect_with_headers(endpoint).await {
                Ok(ws) => return Ok((ws, endpoint)),
                Err(e) => {
                    debug!(
                        "[TradingView/{}] connect failed on {}: {}",
                        self.asset,
                        endpoint,
                        e
                    );
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("no TradingView endpoints configured")))
    }

    async fn connect_with_headers(&self, endpoint: &str) -> Result<TvSocket> {
        let mut req = endpoint.into_client_request()?;
        let headers = req.headers_mut();
        headers.insert(ORIGIN, HeaderValue::from_static("https://www.tradingview.com"));
        headers.insert(REFERER, HeaderValue::from_static("https://www.tradingview.com/"));
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            ),
        );
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        headers.insert(PRAGMA, HeaderValue::from_static("no-cache"));

        let (ws, _) = connect_ws_with_proxy(req).await?;
        Ok(ws)
    }

    fn is_http_403(err: &anyhow::Error) -> bool {
        for cause in err.chain() {
            if let Some(ws_err) = cause.downcast_ref::<WsError>() {
                if let WsError::Http(resp) = ws_err {
                    if resp.status().as_u16() == 403 {
                        return true;
                    }
                }
            }
        }
        err.to_string().contains("403")
    }

    fn parse_price_payload(&self, payload: &str) -> Option<f64> {
        if !payload.starts_with('{') {
            return None;
        }
        let v: Value = serde_json::from_str(&payload).ok()?;
        if v.get("m")?.as_str()? != "qsd" {
            return None;
        }

        let p = v.get("p")?;
        let quote_obj = match p {
            // Common shape: {"m":"qsd","p":["SYMBOL",{"v":{"lp":123.45}}]}
            Value::Array(arr) => arr.get(1)?,
            // Occasionally delivered as object payload.
            Value::Object(_) => p,
            _ => return None,
        };

        let lp = quote_obj
            .get("v")
            .and_then(|v| v.get("lp"))
            .and_then(Self::value_to_f64)
            .or_else(|| quote_obj.get("lp").and_then(Self::value_to_f64))?;

        if lp > 0.0 {
            Some(lp)
        } else {
            None
        }
    }

    fn value_to_f64(v: &Value) -> Option<f64> {
        match v {
            Value::Number(n) => n.as_f64(),
            Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    async fn send_msg(
        &self,
        ws: &mut TvSocket,
        func: &str,
        args: &Value,
    ) -> Result<()> {
        let msg = json!({"m": func, "p": args});
        ws.send(Message::Text(Self::wrap_msg(&msg.to_string()))).await?;
        Ok(())
    }

    fn wrap_msg(msg: &str) -> String { format!("~m~{}~m~{}", msg.len(), msg) }

    fn extract_payloads(raw: &str) -> Vec<String> {
        let mut payloads = Vec::new();
        let mut rest = raw;

        while let Some(start) = rest.find("~m~") {
            let after_start = &rest[start + 3..];
            let Some(len_sep) = after_start.find("~m~") else {
                break;
            };

            let len_str = &after_start[..len_sep];
            let Ok(msg_len) = len_str.parse::<usize>() else {
                break;
            };

            let payload_start = len_sep + 3;
            if after_start.len() < payload_start + msg_len {
                break;
            }

            let payload = &after_start[payload_start..payload_start + msg_len];
            payloads.push(payload.to_string());
            rest = &after_start[payload_start + msg_len..];
        }

        if payloads.is_empty() {
            payloads.push(raw.to_string());
        }

        payloads
    }
}
