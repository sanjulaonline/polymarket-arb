#![allow(dead_code)]

mod bayesian;
mod confidence;
mod config;
mod dashboard;
mod database;
mod detector;
mod feeds;
mod kelly;
mod polymarket;
mod risk;
mod stoikov;
mod telegram;
mod types;

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::{broadcast, watch};
use tracing::{error, info, warn};

use config::Config;
use dashboard::Dashboard;
use database::Database;
use detector::{ContractSlot, Detector};
use feeds::{
    binance::BinanceFeed,
    cryptoquant::CryptoQuantFeed,
    tradingview::TradingViewFeed,
    PriceAggregator,
};
use polymarket::{client::PolymarketClient, poller::PolymarketPoller};
use risk::RiskManager;
use telegram::Telegram;
use types::{Asset, PriceTick, Timeframe};

// ── Midnight reset ────────────────────────────────────────────────────────────

async fn midnight_reset(risk: Arc<RiskManager>, tg: Arc<Telegram>) {
    loop {
        let now = chrono::Utc::now();
        let tomorrow = (now + chrono::Duration::days(1))
            .date_naive()
            .and_hms_opt(0, 0, 5)
            .unwrap();
        let next = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(tomorrow, chrono::Utc);
        let secs = (next - now).num_seconds().max(1) as u64;
        tokio::time::sleep(tokio::time::Duration::from_secs(secs)).await;
        risk.reset_daily();
        let _ = tg.send("🌅 Daily reset — counters cleared, kill-switch lifted.").await;
    }
}

// ── Stats printer ─────────────────────────────────────────────────────────────

async fn stats_printer(risk: Arc<RiskManager>) {
    let mut iv = tokio::time::interval(tokio::time::Duration::from_secs(60));
    loop {
        iv.tick().await;
        let (pnl, trades, wr, halted) = risk.snapshot();
        info!(
            "[Stats] daily_pnl={:+.2} | trades={} | win_rate={:.1}% | halted={}",
            pnl, trades, wr, halted
        );
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .compact()
        .init();

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("  Polymarket Latency Arb Bot  v2.0              ");
    info!("  BTC+ETH | 5m+15m | Kelly | Paper-first       ");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let cfg = Config::from_env().context("Config error")?;

    // Safety check
    if cfg.is_live() {
        warn!("⚠️  LIVE TRADING MODE — real orders will be placed!");
    } else {
        info!("📋 PAPER TRADING MODE — no real orders");
    }

    // Shared services
    let poly_client = Arc::new(PolymarketClient::new(&cfg)?);
    let risk = Arc::new(RiskManager::new(cfg.clone()));
    let tg = Arc::new(Telegram::new(
        cfg.enable_telegram,
        cfg.telegram_bot_token.clone(),
        cfg.telegram_chat_id.clone(),
    ));
    let db = Arc::new(Database::open("trades.db", cfg.enable_database)?);

    // Discover markets
    info!("[Init] Discovering Polymarket markets...");
    let btc_market = poly_client.get_btc_market().await
        .context("Failed to fetch BTC market")?;
    info!("[Init] BTC market: \"{}\"", btc_market.question);

    let btc_yes = btc_market.tokens.iter().find(|t| t.outcome.to_lowercase() == "yes").cloned()
        .context("No BTC YES token")?;
    let btc_no = btc_market.tokens.iter().find(|t| t.outcome.to_lowercase() == "no").cloned()
        .context("No BTC NO token")?;
    let btc_strike = extract_strike(&btc_market.question).unwrap_or(80_000.0);

    // Build contract slots: BTC 5m, BTC 15m
    // expiry_ts: set to end-of-day UTC (you'd fetch this from the market metadata in production)
    let eod_ts = {
        let now = chrono::Utc::now();
        let eod = now.date_naive().and_hms_opt(23, 59, 59).unwrap();
        chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(eod, chrono::Utc).timestamp()
    };

    let contracts = vec![
        ContractSlot {
            asset: Asset::Btc,
            timeframe: Timeframe::FiveMin,
            yes_token: btc_yes.clone(),
            no_token: btc_no.clone(),
            strike: btc_strike,
            expiry_ts: eod_ts,
        },
        ContractSlot {
            asset: Asset::Btc,
            timeframe: Timeframe::FifteenMin,
            yes_token: btc_yes.clone(),
            no_token: btc_no.clone(),
            strike: btc_strike,
            expiry_ts: eod_ts,
        },
        // ETH slots would go here once ETH market tokens are discovered
    ];

    // Channels
    let (raw_tx, _) = broadcast::channel::<PriceTick>(2048);
    let (agg_tx, _) = broadcast::channel::<PriceTick>(2048);

    // Aggregator
    let aggregator = PriceAggregator::new(agg_tx.clone());
    let latest_prices = aggregator.latest_map();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut handles = Vec::new();

    // Binance feed (BTC + ETH)
    {
        let tx = raw_tx.clone();
        handles.push(tokio::spawn(async move {
            let feed = BinanceFeed::new(tx);
            if let Err(e) = feed.run().await { error!("[Binance] fatal: {e}"); }
        }));
    }

    // TradingView BTC
    {
        let tx = raw_tx.clone();
        let symbol = cfg.tradingview_symbol_btc.clone();
        handles.push(tokio::spawn(async move {
            let feed = TradingViewFeed::new(&symbol, tx, types::Asset::Btc);
            if let Err(e) = feed.run().await { error!("[TradingView BTC] fatal: {e}"); }
        }));
    }

    // TradingView ETH
    {
        let tx = raw_tx.clone();
        let symbol = cfg.tradingview_symbol_eth.clone();
        handles.push(tokio::spawn(async move {
            let feed = TradingViewFeed::new(&symbol, tx, types::Asset::Eth);
            if let Err(e) = feed.run().await { error!("[TradingView ETH] fatal: {e}"); }
        }));
    }

    // CryptoQuant (BTC)
    if !cfg.cryptoquant_api_key.is_empty() {
        let tx = raw_tx.clone();
        let key = cfg.cryptoquant_api_key.clone();
        handles.push(tokio::spawn(async move {
            let feed = CryptoQuantFeed::new(&key, tx);
            if let Err(e) = feed.run().await { error!("[CryptoQuant] fatal: {e}"); }
        }));
    }

    // Polymarket pollers for each contract
    for slot in &contracts {
        let client = poly_client.clone();
        let tx = raw_tx.clone();
        let token_id = slot.yes_token.token_id.clone();
        let asset = slot.asset;
        let strike = slot.strike;
        let poll_ms = cfg.poly_poll_ms;
        handles.push(tokio::spawn(async move {
            let poller = PolymarketPoller::new(client, token_id, asset, strike, poll_ms, tx);
            if let Err(e) = poller.run().await { error!("[Polymarket poller] fatal: {e}"); }
        }));
    }

    // Aggregator
    {
        let raw_rx = raw_tx.subscribe();
        handles.push(tokio::spawn(async move {
            if let Err(e) = aggregator.run(raw_rx).await { error!("[Aggregator] fatal: {e}"); }
        }));
    }

    // Detector / executor
    {
        let det_rx = agg_tx.subscribe();
        let client = poly_client.clone();
        let risk_c = risk.clone();
        let db_c = db.clone();
        let tg_c = tg.clone();
        let cfg_c = cfg.clone();
        let prices = latest_prices.clone();
        let contracts_c = contracts.clone();
        handles.push(tokio::spawn(async move {
            let det = Detector::new(cfg_c, client, risk_c, db_c, tg_c, contracts_c, prices);
            if let Err(e) = det.run(det_rx).await { error!("[Detector] fatal: {e}"); }
        }));
    }

    // Background tasks
    {
        let r = risk.clone(); let t = tg.clone();
        handles.push(tokio::spawn(async move { midnight_reset(r, t).await; }));
    }
    {
        let r = risk.clone();
        handles.push(tokio::spawn(async move { stats_printer(r).await; }));
    }

    // Startup Telegram notification
    let mode_str = if cfg.is_live() { "🔴 LIVE" } else { "📋 PAPER" };
    let _ = tg.send(&format!("🚀 Bot started | Mode: {mode_str} | Tracking BTC+ETH 5m/15m")).await;

    // Terminal dashboard (runs in foreground, press q to exit)
    let dash = Dashboard::new(db.clone(), risk.clone(), cfg.clone());
    dash.run(shutdown_rx).await?;

    // Graceful shutdown
    let _ = shutdown_tx.send(true);
    let _ = tg.send("🛑 Bot stopped.").await;
    info!("Bot stopped");

    Ok(())
}

fn extract_strike(question: &str) -> Option<f64> {
    let start = question.find('$')? + 1;
    let rest = &question[start..];
    let end = rest.find(|c: char| !c.is_ascii_digit() && c != ',').unwrap_or(rest.len());
    rest[..end].replace(',', "").parse().ok()
}
