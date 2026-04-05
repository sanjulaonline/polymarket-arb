use reqwest::Client;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use tokio::sync::watch;
use tracing::info;

use crate::config::Config;
use crate::detector::ContractSlot;
use crate::proxy::apply_reqwest_proxy_from_env;

#[derive(Debug, Deserialize)]
struct GammaMarketSummary {
    #[serde(default)]
    events: Vec<GammaEventRef>,
}

#[derive(Debug, Deserialize)]
struct GammaEventRef {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct Comment {
    id: i64,
    #[serde(default)]
    #[serde(rename = "userAddress")]
    user_address: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    profile: Option<CommentProfile>,
}

#[derive(Debug, Deserialize)]
struct CommentProfile {
    #[serde(default)]
    name: Option<String>,
}

fn format_comment(c: &Comment, slot: &ContractSlot) -> String {
    let author = c
        .profile
        .as_ref()
        .and_then(|p| p.name.clone())
        .filter(|n| !n.trim().is_empty())
        .unwrap_or_else(|| {
            let addr = c.user_address.trim();
            if addr.len() > 10 {
                format!("{}...", &addr[..10])
            } else {
                addr.to_string()
            }
        });

    let mut body = c.body.replace('\n', " ").trim().to_string();
    if body.len() > 140 {
        body.truncate(137);
        body.push_str("...");
    }

    format!(
        "[Comment] {}/{} | {}: {}",
        slot.asset,
        slot.timeframe,
        author,
        body
    )
}

async fn resolve_event_id(http: &Client, condition_id: &str) -> Option<i64> {
    let resp = http
        .get("https://gamma-api.polymarket.com/markets")
        .query(&[("condition_id", condition_id)])
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        return None;
    }

    let markets = resp.json::<Vec<GammaMarketSummary>>().await.ok()?;
    let m = markets.first()?;
    let ev = m.events.first()?;
    Some(ev.id)
}

async fn fetch_comments(http: &Client, event_id: i64) -> Vec<Comment> {
    let resp = match http
        .get("https://gamma-api.polymarket.com/comments")
        .query(&[
            ("parent_entity_type", "Event".to_string()),
            ("parent_entity_id", event_id.to_string()),
            ("limit", "10".to_string()),
            ("offset", "0".to_string()),
            ("order", "createdAt".to_string()),
            ("ascending", "false".to_string()),
        ])
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await
    {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    if !resp.status().is_success() {
        return Vec::new();
    }

    resp.json::<Vec<Comment>>().await.unwrap_or_default()
}

pub async fn comment_watcher_loop(
    cfg: Config,
    slots: Vec<ContractSlot>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    if !cfg.comments_enabled || slots.is_empty() {
        return;
    }

    let http = match apply_reqwest_proxy_from_env(Client::builder()) {
        Ok(builder) => builder.build().unwrap_or_else(|_| Client::new()),
        Err(_) => Client::new(),
    };

    let mut seen_ids: HashSet<i64> = HashSet::new();
    let mut initialized_events: HashSet<i64> = HashSet::new();
    let mut event_cache: HashMap<String, i64> = HashMap::new();

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    let mut iv = tokio::time::interval(tokio::time::Duration::from_secs(
        cfg.comments_interval_secs.max(5),
    ));

    loop {
        tokio::select! {
            _ = iv.tick() => {
                for slot in &slots {
                    let event_id = if let Some(id) = event_cache.get(&slot.condition_id).copied() {
                        id
                    } else {
                        let Some(id) = resolve_event_id(&http, &slot.condition_id).await else {
                            continue;
                        };
                        event_cache.insert(slot.condition_id.clone(), id);
                        id
                    };

                    let comments = fetch_comments(&http, event_id).await;
                    if comments.is_empty() {
                        continue;
                    }

                    if !initialized_events.contains(&event_id) {
                        for c in &comments {
                            seen_ids.insert(c.id);
                        }
                        initialized_events.insert(event_id);
                        continue;
                    }

                    for c in comments.iter().rev() {
                        if seen_ids.insert(c.id) {
                            info!("{}", format_comment(c, slot));
                        }
                    }
                }

                if seen_ids.len() > 5000 {
                    seen_ids.clear();
                    initialized_events.clear();
                    event_cache.clear();
                }
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }
}
