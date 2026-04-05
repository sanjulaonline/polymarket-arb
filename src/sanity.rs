use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use tracing::{info, warn};

use crate::detector::ContractSlot;
use crate::polymarket::client::PolymarketClient;
use crate::proxy::apply_reqwest_proxy_from_env;

const KNOWN_ADDRESSES: &[&str] = &[
    "0x0021dc8221add18a125d57bbcb22f4af1fb8cb9e",
    "0x5194a5f5bceabdc955d1b2bd5ed150610316c1d7",
    "0x30b0d72938c7940ae711e18fe3913e980cf2a979",
];

async fn check_gamma_markets(http: &Client) -> bool {
    let resp = match http
        .get("https://gamma-api.polymarket.com/markets")
        .query(&[("limit", "1"), ("closed", "false")])
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await
    {
        Ok(v) => v,
        Err(_) => return false,
    };

    if !resp.status().is_success() {
        return false;
    }

    let data = match resp.json::<Vec<Value>>().await {
        Ok(v) => v,
        Err(_) => return false,
    };

    data.first()
        .map(|m| {
            m.get("id").is_some() && m.get("slug").is_some() && m.get("conditionId").is_some()
        })
        .unwrap_or(false)
}

async fn check_gamma_comments(http: &Client) -> bool {
    let resp = match http
        .get("https://gamma-api.polymarket.com/comments")
        .query(&[
            ("parent_entity_type", "Event".to_string()),
            ("parent_entity_id", "1".to_string()),
            ("limit", "1".to_string()),
        ])
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await
    {
        Ok(v) => v,
        Err(_) => return false,
    };

    if !resp.status().is_success() {
        return false;
    }

    resp.json::<Vec<Value>>().await.is_ok()
}

async fn check_user_comments(http: &Client) -> bool {
    for addr in KNOWN_ADDRESSES {
        let resp = match http
            .get(format!(
                "https://gamma-api.polymarket.com/comments/user_address/{}",
                addr
            ))
            .query(&[("limit", "1")])
            .timeout(std::time::Duration::from_secs(8))
            .send()
            .await
        {
            Ok(v) => v,
            Err(_) => continue,
        };

        if !resp.status().is_success() {
            continue;
        }

        if let Ok(list) = resp.json::<Vec<Value>>().await {
            if !list.is_empty() {
                return true;
            }
        }
    }

    false
}

async fn check_clob_book(client: &PolymarketClient, slots: &[ContractSlot]) -> bool {
    let Some(slot) = slots.first() else {
        return false;
    };

    let Ok(book) = client.get_order_book(&slot.yes_token.token_id).await else {
        return false;
    };

    !book.bids.is_empty() || !book.asks.is_empty()
}

pub async fn run_startup_sanity(client: Arc<PolymarketClient>, slots: &[ContractSlot]) {
    let http = match apply_reqwest_proxy_from_env(Client::builder()) {
        Ok(builder) => builder.build().unwrap_or_else(|_| Client::new()),
        Err(_) => Client::new(),
    };

    let markets_ok = check_gamma_markets(&http).await;
    let comments_ok = check_gamma_comments(&http).await;
    let user_comments_ok = check_user_comments(&http).await;
    let clob_book_ok = check_clob_book(client.as_ref(), slots).await;

    info!(
        "[Sanity] gamma_markets={} gamma_comments={} user_comments={} clob_book={}",
        markets_ok,
        comments_ok,
        user_comments_ok,
        clob_book_ok
    );

    if !(markets_ok && comments_ok && clob_book_ok) {
        warn!(
            "[Sanity] one or more startup checks failed; continuing in best-effort mode"
        );
    }
}
