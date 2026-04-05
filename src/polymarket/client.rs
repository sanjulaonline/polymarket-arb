/// Polymarket Central Limit Order Book (CLOB) client.
/// API docs: https://docs.polymarket.com/#clob-client
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use chrono::{Timelike, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use sha2::Sha256;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::proxy::apply_reqwest_proxy_from_env;
use crate::types::Timeframe;

const CLOB_BASE: &str = "https://clob.polymarket.com";
const SERVER_TIME_RESYNC_SECS: i64 = 30;
const POLYGON_USDC_E_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const ERC20_BALANCE_OF_SELECTOR: &str = "70a08231";
const USDC_E_DECIMALS: u32 = 6;

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
    pub market_id: Option<String>,
    #[serde(default)]
    pub market_slug: Option<String>,
    #[serde(default)]
    pub event_url: Option<String>,
    #[serde(default)]
    pub end_date_iso: Option<String>,
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
    #[serde(default)]
    id: Option<String>,
    #[serde(rename = "conditionId", default)]
    condition_id: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<Value>,
    #[serde(default)]
    outcomes: Option<Value>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UpDownMarketIds {
    pub event_slug: String,
    pub event_url: String,
    pub title: String,
    pub market_id: Option<String>,
    pub condition_id: Option<String>,
    pub end_date_iso: Option<String>,
    pub up_token_id: String,
    pub down_token_id: String,
}

#[derive(Debug, Clone)]
pub struct TokenSidePrices {
    pub best_buy: Option<f64>,
    pub best_sell: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct MarketResolution {
    pub resolved: bool,
    pub active: Option<bool>,
    pub question: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OnChainPayout {
    pub resolved: bool,
    pub payouts: Vec<f64>,
}

#[derive(Debug, Deserialize)]
struct GammaResolutionMarket {
    #[serde(default)]
    closed: Option<bool>,
    #[serde(default)]
    resolved: Option<bool>,
    #[serde(default)]
    active: Option<bool>,
    #[serde(default)]
    question: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ServerTimeResponse {
    timestamp: i64,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse {
    result: Option<String>,
    error: Option<JsonRpcError>,
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
    #[serde(default, deserialize_with = "de_opt_str_f64")]
    pub min_order_size: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Level {
    #[serde(deserialize_with = "de_str_f64")]
    pub price: f64,
    #[serde(deserialize_with = "de_str_f64")]
    pub size: f64,
}

fn de_str_f64<'de, D: serde::Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(f64),
        Str(String),
    }

    match NumOrStr::deserialize(d)? {
        NumOrStr::Num(v) => Ok(v),
        NumOrStr::Str(s) => s.parse().map_err(serde::de::Error::custom),
    }
}

fn de_opt_str_f64<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumStrOrNull {
        Num(f64),
        Str(String),
        Null,
    }

    Ok(match NumStrOrNull::deserialize(d)? {
        NumStrOrNull::Num(v) => Some(v),
        NumStrOrNull::Str(s) => s.parse::<f64>().ok(),
        NumStrOrNull::Null => None,
    })
}

/// Top-of-book helpers for Polymarket order books.
impl OrderBook {
    /// Returns highest bid and lowest ask probabilities.
    pub fn best_bid_ask(&self) -> Option<(f64, f64)> {
        let best_bid = self
            .bids
            .iter()
            .filter(|l| l.size > 0.0 && l.price.is_finite() && (0.0..=1.0).contains(&l.price))
            .map(|l| l.price)
            .max_by(|a, b| a.total_cmp(b))?;

        let best_ask = self
            .asks
            .iter()
            .filter(|l| l.size > 0.0 && l.price.is_finite() && (0.0..=1.0).contains(&l.price))
            .map(|l| l.price)
            .min_by(|a, b| a.total_cmp(b))?;

        Some((best_bid, best_ask))
    }

    pub fn best_bid(&self) -> Option<f64> {
        self.best_bid_ask().map(|(bid, _)| bid)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.best_bid_ask().map(|(_, ask)| ask)
    }

    /// Midpoint display price derived from top-of-book best bid/ask.
    pub fn mid_price(&self) -> Option<f64> {
        let (best_bid, best_ask) = self.best_bid_ask()?;
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
        let http = apply_reqwest_proxy_from_env(
            Client::builder().timeout(std::time::Duration::from_millis(cfg.exec_timeout_ms)),
        )?
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
        let ids = self.get_updown_market_ids_by_slug(asset_slug, tf).await?;
        let kline_interval = match tf {
            Timeframe::FiveMin => "5m",
            Timeframe::FifteenMin => "15m",
        };

        info!(
            "[Client] Found {} {} market via gamma slug {} | yes_token={} | no_token={} | end_date={}",
            asset_slug.to_uppercase(),
            kline_interval,
            ids.event_slug,
            ids.up_token_id,
            ids.down_token_id,
            ids.end_date_iso.as_deref().unwrap_or("unknown")
        );
        info!("[Client] Market title: {}", ids.title);

        let condition_id = ids
            .condition_id
            .clone()
            .or_else(|| ids.market_id.clone())
            .unwrap_or_else(|| ids.event_slug.clone());

        Ok(Market {
            condition_id,
            question: ids.title,
            tokens: vec![
                Token {
                    token_id: ids.up_token_id,
                    outcome: "Yes".to_string(),
                },
                Token {
                    token_id: ids.down_token_id,
                    outcome: "No".to_string(),
                },
            ],
            market_id: ids.market_id,
            market_slug: Some(format!("{}-updown-{}", asset_slug, kline_interval)),
            event_url: Some(ids.event_url),
            end_date_iso: ids.end_date_iso,
            active: true,
            closed: false,
        })
    }

    pub async fn get_updown_market_ids_by_slug(
        &self,
        asset_slug: &str,
        tf: Timeframe,
    ) -> Result<UpDownMarketIds> {
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

        let timestamps = [
            current_bucket_start.timestamp(),
            current_bucket_start.timestamp() + interval_secs,
        ];
        let now_ts = Utc::now().timestamp();

        for ts in timestamps {
            let slug = format!("{}-updown-{}-{}", asset_slug, kline_interval, ts);
            let event = match self.fetch_gamma_event_for_slug(&slug).await {
                Ok(e) => e,
                Err(e) => {
                    debug!("[Client] gamma slug {} lookup failed: {}", slug, e);
                    continue;
                }
            };

            let Some(market) = event.markets.first() else {
                continue;
            };

            let expiration = market
                .end_date
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.timestamp())
                .unwrap_or(0);

            if expiration <= Utc::now().timestamp() {
                continue;
            }

            // Mirror maker-mm detector behavior: do not enter a current slot if it is
            // already stale; instead wait for the next slot market.
            let is_current_slot = ts == current_bucket_start.timestamp();
            let current_slot_age_secs = now_ts.saturating_sub(ts);
            if is_current_slot && current_slot_age_secs > 15 {
                debug!(
                    "[Client] skipping stale current-slot slug {} (age={}s > 15s)",
                    slug,
                    current_slot_age_secs
                );
                continue;
            }

            let (up_token_id, down_token_id) = Self::extract_up_down_tokens(market, &slug)?;
            let title = if event.title.is_empty() {
                slug.clone()
            } else {
                event.title.clone()
            };

            return Ok(UpDownMarketIds {
                event_slug: slug.clone(),
                event_url: format!("https://polymarket.com/event/{}", slug),
                title,
                market_id: market.id.clone(),
                condition_id: market.condition_id.clone(),
                end_date_iso: market.end_date.clone(),
                up_token_id,
                down_token_id,
            });
        }

        Err(anyhow!(
            "No active {} {:?} up/down market found via gamma slug",
            asset_slug,
            tf
        ))
    }

    async fn fetch_gamma_event_for_slug(&self, slug: &str) -> Result<GammaEvent> {
        let canonical_url = format!("https://gamma-api.polymarket.com/events/slug/{}", slug);
        let canonical_resp = self
            .http
            .get(&canonical_url)
            .send()
            .await
            .with_context(|| format!("GET gamma event by slug {slug}"))?;

        if canonical_resp.status().is_success() {
            return canonical_resp
                .json::<GammaEvent>()
                .await
                .with_context(|| format!("parse gamma event by slug {slug}"));
        }

        debug!(
            "[Client] /events/slug/{} returned HTTP {}; trying /events?slug= fallback",
            slug,
            canonical_resp.status()
        );

        let fallback_url = format!("https://gamma-api.polymarket.com/events?slug={}", slug);
        let fallback_resp = self
            .http
            .get(&fallback_url)
            .send()
            .await
            .with_context(|| format!("GET gamma events fallback for slug {slug}"))?;

        if !fallback_resp.status().is_success() {
            return Err(anyhow!(
                "gamma slug {} not available (HTTP {})",
                slug,
                fallback_resp.status()
            ));
        }

        let events = fallback_resp
            .json::<Vec<GammaEvent>>()
            .await
            .with_context(|| format!("parse gamma events fallback for slug {slug}"))?;

        events
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("gamma events fallback returned no events for slug {}", slug))
    }

    fn extract_up_down_tokens(market: &GammaMarket, slug: &str) -> Result<(String, String)> {
        let token_ids = market
            .clob_token_ids
            .as_ref()
            .and_then(Self::json_value_to_vec)
            .ok_or_else(|| anyhow!("missing clobTokenIds for slug {}", slug))?;
        let outcomes = market
            .outcomes
            .as_ref()
            .and_then(Self::json_value_to_vec)
            .ok_or_else(|| anyhow!("missing outcomes for slug {}", slug))?;

        let mut up_token = None;
        let mut down_token = None;

        for (idx, outcome) in outcomes.iter().enumerate() {
            let Some(token_id) = token_ids.get(idx) else {
                continue;
            };
            if outcome.eq_ignore_ascii_case("UP") || outcome.eq_ignore_ascii_case("YES") {
                up_token = Some(token_id.clone());
            } else if outcome.eq_ignore_ascii_case("DOWN") || outcome.eq_ignore_ascii_case("NO") {
                down_token = Some(token_id.clone());
            }
        }

        match (up_token, down_token) {
            (Some(up), Some(down)) => Ok((up, down)),
            _ => Err(anyhow!("could not map up/down token ids for slug {}", slug)),
        }
    }

    fn json_value_to_vec(v: &Value) -> Option<Vec<String>> {
        match v {
            Value::String(s) => serde_json::from_str::<Vec<String>>(s).ok(),
            Value::Array(arr) => {
                let out = arr
                    .iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>();
                if out.is_empty() {
                    None
                } else {
                    Some(out)
                }
            }
            _ => None,
        }
    }

    fn parse_numeric_field(payload: &Value, field: &str) -> Result<f64> {
        payload
            .get(field)
            .and_then(|p| match p {
                Value::String(s) => s.trim().parse::<f64>().ok(),
                Value::Number(n) => n.as_f64(),
                _ => None,
            })
            .filter(|v| v.is_finite())
            .ok_or_else(|| anyhow!("missing or invalid {} field in response", field))
    }

    pub async fn get_token_midpoint(&self, token_id: &str) -> Result<f64> {
        let path = "/midpoint";
        let url = format!("{}{}", CLOB_BASE, path);
        let resp = self
            .http
            .get(&url)
            .query(&[("token_id", token_id)])
            .send()
            .await
            .with_context(|| format!("GET /midpoint for token {}", token_id))?;

        let status = resp.status();
        let body = resp.text().await.context("read /midpoint body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "/midpoint failed with HTTP {} for token {}: {}",
                status,
                token_id,
                &body[..body.len().min(200)]
            ));
        }

        let payload: Value = serde_json::from_str(&body).context("parse /midpoint JSON")?;
        let mid = Self::parse_numeric_field(&payload, "mid")?;
        Ok(mid.clamp(0.0, 1.0))
    }

    pub async fn get_token_spread(&self, token_id: &str) -> Result<f64> {
        let path = "/spread";
        let url = format!("{}{}", CLOB_BASE, path);
        let resp = self
            .http
            .get(&url)
            .query(&[("token_id", token_id)])
            .send()
            .await
            .with_context(|| format!("GET /spread for token {}", token_id))?;

        let status = resp.status();
        let body = resp.text().await.context("read /spread body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "/spread failed with HTTP {} for token {}: {}",
                status,
                token_id,
                &body[..body.len().min(200)]
            ));
        }

        let payload: Value = serde_json::from_str(&body).context("parse /spread JSON")?;
        let spread = Self::parse_numeric_field(&payload, "spread")?;
        Ok(spread.max(0.0))
    }

    pub async fn get_token_best_price(&self, token_id: &str, side: OrderSide) -> Result<f64> {
        let side_str = match side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };

        let path = "/price";
        let url = format!("{}{}", CLOB_BASE, path);
        let resp = self
            .http
            .get(&url)
            .query(&[("token_id", token_id), ("side", side_str)])
            .send()
            .await
            .with_context(|| format!("GET /price for token {} side {}", token_id, side_str))?;

        let status = resp.status();
        let body = resp.text().await.context("read /price body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "/price failed with HTTP {} for token {} side {}: {}",
                status,
                token_id,
                side_str,
                &body[..body.len().min(200)]
            ));
        }

        let payload: Value = serde_json::from_str(&body).context("parse /price JSON")?;
        let price = Self::parse_numeric_field(&payload, "price")?;

        Ok(price)
    }

    pub async fn get_token_side_prices(&self, token_id: &str) -> TokenSidePrices {
        let best_buy = self.get_token_best_price(token_id, OrderSide::Buy).await.ok();
        let best_sell = self.get_token_best_price(token_id, OrderSide::Sell).await.ok();
        TokenSidePrices { best_buy, best_sell }
    }

    pub async fn check_market_resolution(&self, condition_id: &str) -> Result<Option<MarketResolution>> {
        let cid = condition_id.trim();
        if cid.is_empty() {
            return Ok(None);
        }

        let url = "https://gamma-api.polymarket.com/markets";
        let resp = self
            .http
            .get(url)
            .query(&[("condition_id", cid)])
            .send()
            .await
            .context("GET gamma markets by condition_id")?;

        if !resp.status().is_success() {
            return Ok(None);
        }

        let markets = resp
            .json::<Vec<GammaResolutionMarket>>()
            .await
            .context("parse gamma resolution response")?;

        let Some(m) = markets.into_iter().next() else {
            return Ok(None);
        };

        Ok(Some(MarketResolution {
            resolved: m.closed.unwrap_or(false) || m.resolved.unwrap_or(false),
            active: m.active,
            question: m.question,
        }))
    }

    pub async fn check_onchain_payout(&self, rpc_url: &str, condition_id: &str) -> Result<OnChainPayout> {
        let cond = Self::normalize_bytes32_hex(condition_id)?;

        let denominator = self
            .eth_call_u256(
                rpc_url,
                CTF_ADDRESS,
                &Self::abi_selector("payoutDenominator(bytes32)"),
                &[cond.to_vec()],
            )
            .await
            .unwrap_or(0.0);

        if denominator <= 0.0 {
            return Ok(OnChainPayout {
                resolved: false,
                payouts: vec![],
            });
        }

        let mut payouts = Vec::with_capacity(2);
        for outcome_idx in 0u64..=1u64 {
            let mut idx_word = [0u8; 32];
            idx_word[24..].copy_from_slice(&outcome_idx.to_be_bytes());

            let numerator = self
                .eth_call_u256(
                    rpc_url,
                    CTF_ADDRESS,
                    &Self::abi_selector("payoutNumerators(bytes32,uint256)"),
                    &[cond.to_vec(), idx_word.to_vec()],
                )
                .await
                .unwrap_or(0.0);

            payouts.push((numerator / denominator).clamp(0.0, 1.0));
        }

        Ok(OnChainPayout {
            resolved: true,
            payouts,
        })
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
        let status = resp.status();
        let body = resp.text().await.context("read /book body")?;

        if !status.is_success() {
            return Err(anyhow!(
                "/book failed with HTTP {}: {}",
                status,
                &body[..body.len().min(200)]
            ));
        }

        let book: OrderBook = serde_json::from_str(&body).context("parse /book")?;
        Ok(book)
    }

    /// Fetch collateral (USDC) wallet balance from CLOB.
    ///
    /// Uses authenticated `/balance-allowance` and returns USDC-denominated
    /// amount suitable for paper bankroll sizing.
    pub async fn get_collateral_balance_usdc(&self) -> Result<f64> {
        let candidates = [
            "/balance-allowance?asset_type=COLLATERAL&signature_type=0",
            "/balance-allowance?asset_type=COLLATERAL",
            "/balance-allowance",
        ];

        let mut last_err: Option<anyhow::Error> = None;
        for path in candidates {
            match self.fetch_collateral_balance_for_path(path).await {
                Ok(v) if v.is_finite() && v >= 0.0 => return Ok(v),
                Ok(v) => {
                    last_err = Some(anyhow!(
                        "invalid collateral balance parsed from {}: {}",
                        path,
                        v
                    ));
                }
                Err(e) => {
                    debug!(
                        "[Client] balance fetch via '{}' failed: {}",
                        path,
                        e
                    );
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("unable to fetch collateral balance")))
    }

    /// TS-equivalent on-chain USDC.e fetch:
    /// `balanceOf(proxyWallet)` via Polygon JSON-RPC `eth_call`.
    pub async fn get_usdc_balance_onchain(&self, rpc_url: &str, wallet: &str) -> Result<f64> {
        let wallet_hex = Self::normalize_address_hex(wallet)?;
        let call_data = format!("0x{}{:0>64}", ERC20_BALANCE_OF_SELECTOR, wallet_hex);

        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": POLYGON_USDC_E_ADDRESS,
                    "data": call_data,
                },
                "latest"
            ]
        });

        let resp = self
            .http
            .post(rpc_url)
            .json(&payload)
            .send()
            .await
            .context("POST polygon eth_call")?;

        let status = resp.status();
        let body = resp.text().await.context("read polygon eth_call body")?;

        if !status.is_success() {
            return Err(anyhow!(
                "polygon eth_call failed with HTTP {}: {}",
                status,
                &body[..body.len().min(200)]
            ));
        }

        let rpc: JsonRpcResponse =
            serde_json::from_str(&body).context("parse polygon eth_call response")?;

        if let Some(err) = rpc.error {
            return Err(anyhow!(
                "polygon eth_call rpc error {}: {}",
                err.code,
                err.message
            ));
        }

        let raw_hex = rpc
            .result
            .ok_or_else(|| anyhow!("polygon eth_call response missing result"))?;
        let raw = Self::parse_u256_hex_to_f64(&raw_hex)?;
        Ok(raw / 10f64.powi(USDC_E_DECIMALS as i32))
    }

    async fn fetch_collateral_balance_for_path(&self, path: &str) -> Result<f64> {
        let headers = self.auth_headers("GET", path, "").await;
        let mut req = self.http.get(format!("{}{}", CLOB_BASE, path));
        for (k, v) in headers {
            req = req.header(k, v);
        }

        let resp = req.send().await.context("GET /balance-allowance")?;
        let status = resp.status();
        let body = resp.text().await.context("read /balance-allowance body")?;

        if !status.is_success() {
            return Err(anyhow!(
                "/balance-allowance failed with HTTP {}: {}",
                status,
                &body[..body.len().min(200)]
            ));
        }

        let payload: Value = serde_json::from_str(&body).context("parse /balance-allowance")?;
        Self::extract_collateral_balance_usdc(&payload)
            .ok_or_else(|| anyhow!("balance field not found in /balance-allowance payload"))
    }

    fn extract_collateral_balance_usdc(payload: &Value) -> Option<f64> {
        let (raw, decimals, integer_like) = Self::find_balance_candidate(payload)?;
        Some(Self::normalize_usdc(raw, decimals, integer_like))
    }

    fn find_balance_candidate(v: &Value) -> Option<(f64, Option<u32>, bool)> {
        const BALANCE_KEYS: &[&str] = &[
            "available_balance",
            "balance",
            "collateral_balance",
            "usdc_balance",
            "available",
            "amount",
        ];
        const DECIMAL_KEYS: &[&str] = &["decimals", "token_decimals", "asset_decimals"];

        match v {
            Value::Object(map) => {
                for key in BALANCE_KEYS {
                    if let Some(raw_v) = map.get(*key) {
                        if let Some((raw, integer_like)) = Self::parse_numeric(raw_v) {
                            let decimals = DECIMAL_KEYS
                                .iter()
                                .find_map(|k| map.get(*k).and_then(Self::parse_u32));
                            return Some((raw, decimals, integer_like));
                        }
                    }
                }

                for child in map.values() {
                    if let Some(found) = Self::find_balance_candidate(child) {
                        return Some(found);
                    }
                }

                None
            }
            Value::Array(arr) => arr.iter().find_map(Self::find_balance_candidate),
            _ => None,
        }
    }

    fn parse_numeric(v: &Value) -> Option<(f64, bool)> {
        match v {
            Value::Number(n) => n
                .as_f64()
                .map(|f| (f, n.is_i64() || n.is_u64())),
            Value::String(s) => {
                let t = s.trim();
                let integer_like = !(t.contains('.') || t.contains('e') || t.contains('E'));
                t.parse::<f64>().ok().map(|f| (f, integer_like))
            }
            _ => None,
        }
    }

    fn parse_u32(v: &Value) -> Option<u32> {
        match v {
            Value::Number(n) => n.as_u64().and_then(|u| u.try_into().ok()),
            Value::String(s) => s.trim().parse::<u32>().ok(),
            _ => None,
        }
    }

    fn normalize_address_hex(address: &str) -> Result<String> {
        let trimmed = address.trim();
        let hex = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
            .unwrap_or(trimmed);

        if hex.len() != 40 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(anyhow!("invalid wallet address: {}", address));
        }

        Ok(hex.to_ascii_lowercase())
    }

    fn normalize_bytes32_hex(value: &str) -> Result<[u8; 32]> {
        let trimmed = value.trim();
        let hex = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
            .unwrap_or(trimmed);

        if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(anyhow!("invalid bytes32 hex value: {}", value));
        }

        let bytes = hex::decode(hex).context("decode bytes32 hex")?;
        let mut out = [0u8; 32];
        out.copy_from_slice(&bytes);
        Ok(out)
    }

    fn abi_selector(signature: &str) -> [u8; 4] {
        let digest = Keccak256::digest(signature.as_bytes());
        [digest[0], digest[1], digest[2], digest[3]]
    }

    async fn eth_call_u256(
        &self,
        rpc_url: &str,
        to: &str,
        selector: &[u8; 4],
        words: &[Vec<u8>],
    ) -> Result<f64> {
        let mut payload_bytes = Vec::with_capacity(4 + words.len() * 32);
        payload_bytes.extend_from_slice(selector);
        for w in words {
            if w.len() != 32 {
                return Err(anyhow!("ABI word must be 32 bytes"));
            }
            payload_bytes.extend_from_slice(w);
        }

        let call_data = format!("0x{}", hex::encode(payload_bytes));

        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": to,
                    "data": call_data,
                },
                "latest"
            ]
        });

        let resp = self
            .http
            .post(rpc_url)
            .json(&payload)
            .send()
            .await
            .context("POST polygon eth_call")?;

        let status = resp.status();
        let body = resp.text().await.context("read polygon eth_call body")?;

        if !status.is_success() {
            return Err(anyhow!(
                "polygon eth_call failed with HTTP {}: {}",
                status,
                &body[..body.len().min(200)]
            ));
        }

        let rpc: JsonRpcResponse =
            serde_json::from_str(&body).context("parse polygon eth_call response")?;

        if let Some(err) = rpc.error {
            return Err(anyhow!(
                "polygon eth_call rpc error {}: {}",
                err.code,
                err.message
            ));
        }

        let raw_hex = rpc
            .result
            .ok_or_else(|| anyhow!("polygon eth_call response missing result"))?;
        Self::parse_u256_hex_to_f64(&raw_hex)
    }

    fn parse_u256_hex_to_f64(raw_hex: &str) -> Result<f64> {
        let hex = raw_hex
            .trim()
            .strip_prefix("0x")
            .or_else(|| raw_hex.trim().strip_prefix("0X"))
            .unwrap_or(raw_hex.trim());

        if hex.is_empty() {
            return Ok(0.0);
        }

        if let Ok(v) = u128::from_str_radix(hex, 16) {
            return Ok(v as f64);
        }

        // Fallback for values above u128 range.
        let mut out = 0.0f64;
        for c in hex.chars() {
            let digit = c
                .to_digit(16)
                .ok_or_else(|| anyhow!("invalid hex digit '{}' in eth_call result", c))?
                as f64;
            out = out * 16.0 + digit;
        }

        if !out.is_finite() {
            return Err(anyhow!("eth_call result is too large to represent as f64"));
        }

        Ok(out)
    }

    fn normalize_usdc(raw: f64, decimals: Option<u32>, integer_like: bool) -> f64 {
        if !raw.is_finite() || raw < 0.0 {
            return 0.0;
        }

        if integer_like {
            let d = decimals.unwrap_or(6).min(18);
            let denom = 10f64.powi(d as i32);
            if denom > 1.0 {
                return raw / denom;
            }
        }

        // Fallback heuristic if API returned integer micro-units without decimals metadata.
        if raw.fract() == 0.0 && raw >= 1_000_000.0 {
            return raw / 1_000_000.0;
        }

        raw
    }

    /// Raw mid-price probability from YES token order book (0.0–1.0).
    pub async fn mid_probability(&self, token_id: &str) -> Result<f64> {
        match self.get_token_midpoint(token_id).await {
            Ok(mid) => Ok(mid),
            Err(e) => {
                debug!(
                    "[Client] /midpoint unavailable for token {}: {}. Falling back to /book midpoint",
                    token_id,
                    e
                );
                let book = self.get_order_book(token_id).await?;
                book.mid_price()
                    .ok_or_else(|| anyhow!("Empty order book for {}", token_id))
            }
        }
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
