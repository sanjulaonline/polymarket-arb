use anyhow::Result;
use reqwest::Client;
use serde_json::json;
use tracing::{debug, warn};

use crate::proxy::apply_reqwest_proxy_from_env;

pub struct Telegram {
    http: Client,
    bot_token: String,
    chat_id: String,
    enabled: bool,
}

impl Telegram {
    pub fn new(enabled: bool, bot_token: Option<String>, chat_id: Option<String>) -> Self {
        let requested_enabled = enabled;
        let enabled = requested_enabled && bot_token.is_some() && chat_id.is_some();
        if requested_enabled && !enabled {
            warn!("[Telegram] No token/chat_id configured — alerts disabled");
        }
        let http = match apply_reqwest_proxy_from_env(Client::builder()) {
            Ok(builder) => match builder.build() {
                Ok(client) => client,
                Err(e) => {
                    warn!("[Telegram] HTTP client build failed ({e}) — using direct HTTP client");
                    Client::new()
                }
            },
            Err(e) => {
                warn!("[Telegram] Proxy config invalid ({e}) — using direct HTTP client");
                Client::new()
            }
        };
        Self {
            http,
            bot_token: bot_token.unwrap_or_default(),
            chat_id: chat_id.unwrap_or_default(),
            enabled,
        }
    }

    pub async fn send(&self, msg: &str) -> Result<()> {
        if !self.enabled {
            debug!("[Telegram] (disabled) {}", msg);
            return Ok(());
        }
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token
        );
        let body = json!({
            "chat_id": self.chat_id,
            "text": msg,
            "parse_mode": "HTML"
        });
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await?;
        if !resp.status().is_success() {
            warn!("[Telegram] Send failed: {}", resp.status());
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn trade_alert(
        &self,
        paper: bool,
        asset: &str,
        timeframe: &str,
        direction: &str,
        size_usdc: f64,
        edge_pct: f64,
        confidence: f64,
        order_id: Option<&str>,
    ) {
        let mode = if paper { "📋 PAPER" } else { "🔴 LIVE" };
        let dir_emoji = if direction == "UP" { "📈" } else { "📉" };
        let msg = format!(
            "{mode} TRADE\n\
             {dir_emoji} <b>{asset} {timeframe} {direction}</b>\n\
             💵 Size: ${size_usdc:.2}\n\
             📊 Edge: {edge_pct:.1}%\n\
             🎯 Confidence: {:.0}%\n\
             🔑 Order: {}",
            confidence * 100.0,
            order_id.unwrap_or("paper")
        );
        if let Err(e) = self.send(&msg).await {
            warn!("[Telegram] Alert failed: {e}");
        }
    }

    pub async fn drawdown_alert(&self, daily_pnl: f64, cap_pct: f64) {
        let msg = format!(
            "⚠️ <b>DRAWDOWN ALERT</b>\n\
             Daily P&L: <b>${daily_pnl:.2}</b>\n\
             Kill-switch fires at {cap_pct:.0}% drawdown\n\
             Trading may be halted soon."
        );
        if let Err(e) = self.send(&msg).await {
            warn!("[Telegram] Drawdown alert failed: {e}");
        }
    }

    pub async fn kill_switch_alert(&self, daily_pnl: f64) {
        let msg = format!(
            "🛑 <b>KILL SWITCH ACTIVATED</b>\n\
             Daily P&L: <b>${daily_pnl:.2}</b>\n\
             All trading halted for today."
        );
        if let Err(e) = self.send(&msg).await {
            warn!("[Telegram] Kill-switch alert failed: {e}");
        }
    }
}
