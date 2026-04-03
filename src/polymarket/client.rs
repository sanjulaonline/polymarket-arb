/// Polymarket Central Limit Order Book (CLOB) client.
/// API docs: https://docs.polymarket.com/#clob-client
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use chrono::{Timelike, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::types::Timeframe;

const CLOB_BASE: &str = "https://clob.polymarket.com";
const SERVER_TIME_RESYNC_SECS: i64 = 30;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Default)]
struct TimeSyncCache {
    offset_secs: i64,
    last_sync_local_ts: i64,
}

// ── Types ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct Market {
    pub condition_id: String,
    pub question: String,
    pub tokens: Vec<Token>,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
}

/// Paginated response from GET /markets
#[derive(Debug, Deserialize)]
struct MarketsPage {
    data: Vec<Market>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    #[serde(default)]
    title: String,
    #[serde(default)]
    markets: Vec<GammaMarket>,
}

#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
    outcomes: Option<String>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ServerTimeResponse {
    timestamp: i64,
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
    time_sync: Mutex<TimeSyncCache>,
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
            time_sync: Mutex::new(TimeSyncCache::default()),
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

    async fn auth_headers(&self, method: &str, path: &str, body: &str) -> Vec<(String, String)> {
        let ts = self.auth_timestamp().await;

        let sig = self.sign(ts, method, path, body);
        vec![
            ("POLY-API-KEY".to_string(), self.api_key.clone()),
            ("POLY-SIGNATURE".to_string(), sig),
            ("POLY-TIMESTAMP".to_string(), ts.to_string()),
            ("POLY-PASSPHRASE".to_string(), self.passphrase.clone()),
        ]
    }

    async fn auth_timestamp(&self) -> i64 {
        let local_ts = Utc::now().timestamp();

        {
            let cache = self.time_sync.lock().await;
            if local_ts.saturating_sub(cache.last_sync_local_ts) <= SERVER_TIME_RESYNC_SECS {
                return local_ts + cache.offset_secs;
            }
        }

        match self.fetch_server_time().await {
            Ok(server_ts) => {
                let offset = server_ts - local_ts;
                if offset.abs() > 300 {
                    warn!(
                        "[Client] Large local/server time skew detected ({}s); using cached server offset",
                        offset
                    );
                }

                let mut cache = self.time_sync.lock().await;
                cache.offset_secs = offset;
                cache.last_sync_local_ts = local_ts;
                local_ts + offset
            }
            Err(e) => {
                debug!(
                    "[Client] Failed to refresh server time: {e}; using cached offset if available"
                );
                let mut cache = self.time_sync.lock().await;
                let had_sync = cache.last_sync_local_ts != 0;
                cache.last_sync_local_ts = local_ts;
                if had_sync {
                    local_ts + cache.offset_secs
                } else {
                    local_ts
                }
            }
        }
    }

    async fn fetch_server_time(&self) -> Result<i64> {
        let url = format!("{}/time", CLOB_BASE);
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .context("GET /time")?;

        if !resp.status().is_success() {
            return Err(anyhow!("/time failed with HTTP {}", resp.status()));
        }

        let body: ServerTimeResponse = resp.json().await.context("parse /time")?;
        Ok(body.timestamp)
    }

    async fn get_updown_market_from_gamma(&self, asset_slug: &str, tf: Timeframe) -> Result<Market> {
        let (bucket_size_min, interval_secs, kline_interval) = match tf {
            Timeframe::FiveMin => (5u32, 300i64, "5m"),
            Timeframe::FifteenMin => (15u32, 900i64, "15m"),
        };

        let now = Utc::now();
        let current_bucket_minute = (now.minute() / bucket_size_min) * bucket_size_min;
        let current_bucket_start = now
            .with_minute(current_bucket_minute)
            .and_then(|dt| dt.with_second(0))
            .and_then(|dt| dt.with_nanosecond(0))
            .ok_or_else(|| anyhow!("failed to compute current candle bucket"))?;

        // Check current and next bucket to avoid edge timing misses.
        let timestamps = [
            current_bucket_start.timestamp(),
            current_bucket_start.timestamp() + interval_secs,
        ];

        for ts in timestamps {
            let slug = format!("{}-updown-{}-{}", asset_slug, kline_interval, ts);
            let url = format!("https://gamma-api.polymarket.com/events?slug={}", slug);

            let resp = self
                .http
                .get(&url)
                .send()
                .await
                .with_context(|| format!("GET gamma events for slug {slug}"))?;

            if !resp.status().is_success() {
                debug!("[Client] gamma slug {} returned HTTP {}", slug, resp.status());
                continue;
            }

            let events: Vec<GammaEvent> = resp
                .json()
                .await
                .with_context(|| format!("parse gamma events for slug {slug}"))?;

            let Some(event) = events.first() else {
                continue;
            };

            let Some(m) = event.markets.first() else {
                continue;
            };

            let expiration = m
                .end_date
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.timestamp())
                .unwrap_or(0);

            if expiration <= Utc::now().timestamp() {
                continue;
            }

            let token_ids_raw = match &m.clob_token_ids {
                Some(v) => v,
                None => continue,
            };
            let outcomes_raw = match &m.outcomes {
                Some(v) => v,
                None => continue,
            };

            let token_ids: Vec<String> = serde_json::from_str(token_ids_raw)
                .with_context(|| format!("parse clobTokenIds for slug {slug}"))?;
            let outcomes: Vec<String> = serde_json::from_str(outcomes_raw)
                .with_context(|| format!("parse outcomes for slug {slug}"))?;

            let mut yes_token = None;
            let mut no_token = None;

            for (idx, outcome) in outcomes.iter().enumerate() {
                let Some(token_id) = token_ids.get(idx) else {
                    continue;
                };
                if outcome.eq_ignore_ascii_case("UP") || outcome.eq_ignore_ascii_case("YES") {
                    yes_token = Some(token_id.clone());
                } else if outcome.eq_ignore_ascii_case("DOWN") || outcome.eq_ignore_ascii_case("NO") {
                    no_token = Some(token_id.clone());
                }
            }

            if let (Some(yes), Some(no)) = (yes_token, no_token) {
                info!(
                    "[Client] Found {} {} market via gamma slug {}",
                    asset_slug.to_uppercase(),
                    kline_interval,
                    slug
                );

                return Ok(Market {
                    condition_id: slug.clone(),
                    question: if event.title.is_empty() { slug } else { event.title.clone() },
                    tokens: vec![
                        Token {
                            token_id: yes,
                            outcome: "Yes".to_string(),
                        },
                        Token {
                            token_id: no,
                            outcome: "No".to_string(),
                        },
                    ],
                    active: true,
                    closed: false,
                });
            }
        }

        Err(anyhow!(
            "No active {} {:?} up/down market found via gamma slug",
            asset_slug,
            tf
        ))
    }

    // ── API calls ─────────────────────────────────────────────────────────────

    /// Fetch active BTC end-of-day markets. Returns the first active one.
    pub async fn get_btc_market(&self) -> Result<Market> {
        self.search_markets(&["bitcoin", "btc"], |m| {
            m.active
                && (m.question.to_lowercase().contains("btc")
                    || m.question.to_lowercase().contains("bitcoin"))
        })
        .await
    }

    /// Fetch the active BTC "up or down" market for the given timeframe.
    ///
    /// Polymarket titles look like:
    ///   "Will BTC be up 5 minutes from now?"
    ///   "Will BTC be up 15 minutes from now?"
    pub async fn get_btc_market_for_timeframe(&self, tf: Timeframe) -> Result<Market> {
        let minute_str = match tf {
            Timeframe::FiveMin    => "5",
            Timeframe::FifteenMin => "15",
        };

        info!(
            "[Client] Searching Polymarket for BTC {} minute market...",
            minute_str
        );

        // Prefer deterministic slug lookup first; fall back to text search.
        match self.get_updown_market_from_gamma("btc", tf).await {
            Ok(m) => return Ok(m),
            Err(e) => warn!(
                "[Client] Gamma slug lookup failed for BTC {}m: {e}. Falling back to /markets search.",
                minute_str
            ),
        }

        self.search_markets(&["bitcoin", "btc"], |m| {
            if !m.active || m.closed {
                return false;
            }
            let q = m.question.to_lowercase();
            // Must mention btc/bitcoin
            let is_btc = q.contains("btc") || q.contains("bitcoin");
            // Must be an up/down (directional) market for the right window
            // Accept patterns: "5 minutes", "5 min", "5-minute", etc.
            let minutes_pattern = format!("{} min", minute_str);
            let minutes_pattern2 = format!("{}-min", minute_str);
            let minutes_pattern3 = format!("{} minute", minute_str);
            let is_correct_window = q.contains(&minutes_pattern)
                || q.contains(&minutes_pattern2)
                || q.contains(&minutes_pattern3);
            // Must be directional (up | down | higher | lower)
            let is_directional = q.contains(" up") || q.contains(" down")
                || q.contains("higher") || q.contains("lower");

            is_btc && is_correct_window && is_directional
        })
        .await
        .with_context(|| format!("No active BTC {}m up/down market found", minute_str))
    }

    /// Paginate through GET /markets with bitcoin tag, applying `pred` to each market.
    /// Returns the first market matching the predicate, or an error.
    async fn search_markets<F>(&self, _tags: &[&str], pred: F) -> Result<Market>
    where
        F: Fn(&Market) -> bool,
    {
        let path = "/markets";
        let url = format!("{}{}", CLOB_BASE, path);
        let mut cursor: Option<String> = None;
        let mut page_n = 0usize;

        loop {
            page_n += 1;
            let mut query = vec![
                ("tag".to_string(), "bitcoin".to_string()),
                ("active".to_string(), "true".to_string()),
                ("limit".to_string(), "100".to_string()),
            ];
            if let Some(ref c) = cursor {
                query.push(("next_cursor".to_string(), c.clone()));
            }

            debug!("[Client] GET /markets page {} cursor={:?}", page_n, cursor);

            let resp = self
                .http
                .get(&url)
                .query(&query)
                .send()
                .await
                .context("GET /markets request failed")?;

            let status = resp.status();
            let body = resp.text().await.context("reading /markets response body")?;

            // The CLOB API returns either a paginated object or a bare array.
            // Try paginated first, fall back to bare array.
            let (markets, next) = if let Ok(page) =
                serde_json::from_str::<MarketsPage>(&body)
            {
                (page.data, page.next_cursor)
            } else if let Ok(arr) = serde_json::from_str::<Vec<Market>>(&body) {
                (arr, None)
            } else {
                return Err(anyhow!(
                    "Failed to parse /markets response (HTTP {}): {}",
                    status,
                    &body[..body.len().min(300)]
                ));
            };

            debug!("[Client] page {} returned {} markets", page_n, markets.len());

            for m in markets {
                if pred(&m) {
                    info!(
                        "[Client] Found matching market on page {}: \"{}\"",
                        page_n, m.question
                    );
                    return Ok(m);
                }
            }

            match next {
                Some(c) if !c.is_empty() && c != "LTE=" => cursor = Some(c),
                _ => break,
            }

            // Safety cap: don't loop forever
            if page_n >= 20 {
                warn!("[Client] Reached page limit (20) without finding a matching market");
                break;
            }
        }

        Err(anyhow!("No matching market found after {} pages", page_n))
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

    /// Place a Fill-or-Kill order. Returns order id on success.
    pub async fn place_order(&self, order: &OrderRequest) -> Result<String> {
        let path = "/order";
        let body = serde_json::to_string(order)?;
        let headers = self.auth_headers("POST", path, &body).await;

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
        let headers = self.auth_headers("DELETE", &path, "").await;

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
