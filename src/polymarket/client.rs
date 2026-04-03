/// Polymarket Central Limit Order Book (CLOB) client.
/// API docs: https://docs.polymarket.com/#clob-client
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use tracing::{debug, info, warn};

use crate::config::Config;

const CLOB_BASE: &str = "https://clob.polymarket.com";

type HmacSha256 = Hmac<Sha256>;

// ── Types ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct Market {
    pub condition_id: String,
    pub question: String,
    pub tokens: Vec<Token>,
    pub active: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Token {
    pub token_id: String,
    pub outcome: String,  // "Yes" | "No"
}

#[derive(Debug, Deserialize)]
pub struct OrderBook {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Level {
    #[serde(deserialize_with = "de_str_f64")]
    pub price: f64,
    #[serde(deserialize_with = "de_str_f64")]
    pub size: f64,
}

fn de_str_f64<'de, D: serde::Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    let s: &str = serde::Deserialize::deserialize(d)?;
    s.parse().map_err(serde::de::Error::custom)
}

/// Mid-price derived from an order book
impl OrderBook {
    pub fn mid_price(&self) -> Option<f64> {
        let best_bid = self.bids.first()?.price;
        let best_ask = self.asks.first()?.price;
        Some((best_bid + best_ask) / 2.0)
    }
}

#[derive(Debug, Serialize)]
pub struct OrderRequest {
    pub token_id: String,
    pub price: f64,
    pub size: f64,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
}

#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderType {
    Limit,
    Market,
    Fok,   // Fill-or-Kill — ideal for arb
}

#[derive(Debug, Serialize)]
pub enum TimeInForce {
    #[serde(rename = "FOK")]
    FillOrKill,
    #[serde(rename = "GTC")]
    GoodTillCancel,
    #[serde(rename = "IOC")]
    ImmediateOrCancel,
}

#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    pub order_id: Option<String>,
    pub status: Option<String>,
    pub error_msg: Option<String>,
}

// ── Client ────────────────────────────────────────────────────────────────────

pub struct PolymarketClient {
    http: Client,
    api_key: String,
    api_secret: String,
    passphrase: String,
}

impl PolymarketClient {
    pub fn new(cfg: &Config) -> Result<Self> {
        let http = Client::builder()
            .timeout(std::time::Duration::from_millis(cfg.exec_timeout_ms))
            .build()?;
        Ok(Self {
            http,
            api_key: cfg.polymarket_api_key.clone(),
            api_secret: cfg.polymarket_api_secret.clone(),
            passphrase: cfg.polymarket_api_passphrase.clone(),
        })
    }

    // ── Auth helpers ──────────────────────────────────────────────────────────

    fn sign(&self, timestamp: i64, method: &str, path: &str, body: &str) -> String {
        let message = format!("{}{}{}{}", timestamp, method, path, body);
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        B64.encode(mac.finalize().into_bytes())
    }

    fn auth_headers(&self, method: &str, path: &str, body: &str) -> Vec<(String, String)> {
        let ts = Utc::now().timestamp();
        let sig = self.sign(ts, method, path, body);
        vec![
            ("POLY-API-KEY".to_string(), self.api_key.clone()),
            ("POLY-SIGNATURE".to_string(), sig),
            ("POLY-TIMESTAMP".to_string(), ts.to_string()),
            ("POLY-PASSPHRASE".to_string(), self.passphrase.clone()),
        ]
    }

    // ── API calls ─────────────────────────────────────────────────────────────

    /// Fetch active BTC end-of-day markets. Returns the first active one.
    pub async fn get_btc_market(&self) -> Result<Market> {
        let path = "/markets";
        let url = format!("{}{}", CLOB_BASE, path);

        let resp = self
            .http
            .get(&url)
            .query(&[("tag", "bitcoin"), ("active", "true")])
            .send()
            .await
            .context("GET /markets failed")?;

        let payload: Value = resp.json().await.context("parse /markets response")?;
        let markets = parse_markets(payload).context("extract markets from /markets response")?;
        markets
            .into_iter()
            .find(|m| {
                m.active
                    && (m.question.to_lowercase().contains("btc")
                        || m.question.to_lowercase().contains("bitcoin"))
            })
            .ok_or_else(|| anyhow!("No active BTC market found"))
    }

    /// Fetch order book for a token.
    pub async fn get_order_book(&self, token_id: &str) -> Result<OrderBook> {
        let path = format!("/book?token_id={}", token_id);
        let url = format!("{}{}", CLOB_BASE, path);

        let resp = self.http.get(&url).send().await.context("GET /book")?;
        let book: OrderBook = resp.json().await.context("parse /book")?;
        Ok(book)
    }

    /// Raw mid-price probability from YES token order book (0.0–1.0).
    pub async fn mid_probability(&self, token_id: &str) -> Result<f64> {
        let book = self.get_order_book(token_id).await?;
        book.mid_price().ok_or_else(|| anyhow!("Empty order book for {}", token_id))
    }

    /// Implied BTC price from a YES token's mid-price.
    pub async fn implied_btc_price(&self, token_id: &str, strike: f64) -> Result<f64> {
        let mid = self.mid_probability(token_id).await?;
        let implied = strike * (1.0 + (mid - 0.5) * 0.02);
        Ok(implied)
    }

    /// Place a Fill-or-Kill order. Returns order id on success.
    pub async fn place_order(&self, order: &OrderRequest) -> Result<String> {
        let path = "/order";
        let body = serde_json::to_string(order)?;
        let headers = self.auth_headers("POST", path, &body);

        let mut req = self.http.post(format!("{}{}", CLOB_BASE, path));
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.header("Content-Type", "application/json").body(body).send().await?;

        let status = resp.status();
        let or: OrderResponse = resp.json().await.context("parse order response")?;

        if !status.is_success() {
            return Err(anyhow!(
                "Order rejected ({}): {}",
                status,
                or.error_msg.unwrap_or_default()
            ));
        }
        or.order_id.ok_or_else(|| anyhow!("Order response missing order_id"))
    }

    /// Cancel an order by id.
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let path = format!("/order/{}", order_id);
        let headers = self.auth_headers("DELETE", &path, "");

        let mut req = self.http.delete(format!("{}{}", CLOB_BASE, path));
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            warn!("Cancel order {} failed: {}", order_id, resp.status());
        }
        Ok(())
    }
}

fn parse_markets(payload: Value) -> Result<Vec<Market>> {
    if payload.is_array() {
        let markets: Vec<Market> = serde_json::from_value(payload)
            .context("expected /markets array payload")?;
        return Ok(markets);
    }

    if let Some(v) = payload.get("markets") {
        let markets: Vec<Market> = serde_json::from_value(v.clone())
            .context("expected object payload with markets[]")?;
        return Ok(markets);
    }

    if let Some(v) = payload.get("data") {
        if v.is_array() {
            let markets: Vec<Market> = serde_json::from_value(v.clone())
                .context("expected object payload with data[]")?;
            return Ok(markets);
        }
        if let Some(markets_v) = v.get("markets") {
            let markets: Vec<Market> = serde_json::from_value(markets_v.clone())
                .context("expected object payload with data.markets[]")?;
            return Ok(markets);
        }
    }

    Err(anyhow!("Unrecognized /markets payload shape"))
}
