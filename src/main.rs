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
mod proxy;
mod risk;
mod stoikov;
mod telegram;
mod types;

use anyhow::{Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{broadcast, watch};
use tracing::{error, info, warn};
use url::Url;

use config::Config;
use dashboard::Dashboard;
use database::Database;
use detector::{ContractSlot, Detector};
use feeds::{
    binance::BinanceFeed,
    cryptoquant::CryptoQuantFeed,
    polymarket_live_ws::PolymarketLiveWsFeed,
    tradingview::TradingViewFeed,
    PriceAggregator,
};
use polymarket::{client::PolymarketClient, poller::PolymarketPoller};
use risk::RiskManager;
use telegram::Telegram;
use types::{Asset, PriceSource, PriceTick, Timeframe};

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

type FeedKey = (PriceSource, Asset, Option<Timeframe>);

fn fmt_tick(
    latest: &Arc<DashMap<FeedKey, PriceTick>>,
    source: PriceSource,
    asset: Asset,
    timeframe: Option<Timeframe>,
) -> String {
    if let Some(t) = latest.get(&(source, asset, timeframe)) {
        let age_ms = (Utc::now() - t.timestamp).num_milliseconds().max(0);
        if source == PriceSource::Polymarket {
            let bid = t
                .book_best_bid_prob
                .map(|v| format!("{v:.4}"))
                .unwrap_or_else(|| "na".to_string());
            let ask = t
                .book_best_ask_prob
                .map(|v| format!("{v:.4}"))
                .unwrap_or_else(|| "na".to_string());
            format!("bid={bid} ask={ask} mid={:.4} age={}ms", t.price, age_ms)
        } else {
            format!("{:.2} age={}ms", t.price, age_ms)
        }
    } else {
        "n/a".to_string()
    }
}

async fn stats_printer(
    risk: Arc<RiskManager>,
    latest: Arc<DashMap<FeedKey, PriceTick>>,
    tracked_timeframes: Vec<Timeframe>,
) {
    let mut iv = tokio::time::interval(tokio::time::Duration::from_secs(10));
    loop {
        iv.tick().await;
        let (pnl, trades, wr, halted) = risk.snapshot();
        let (poly_age_ms, fallback_rate, fallback_count, lookup_count) = risk.poly_data_metrics();
        info!(
            "[Stats] daily_pnl={:+.2} | trades={} | win_rate={:.1}% | halted={} | poly_tick_age_ms={} | poly_fallback={:.1}% ({}/{})",
            pnl,
            trades,
            wr,
            halted,
            poly_age_ms,
            fallback_rate,
            fallback_count,
            lookup_count
        );

        let btc_binance = fmt_tick(&latest, PriceSource::Binance, Asset::Btc, None);
        let btc_poly_ws = fmt_tick(&latest, PriceSource::PolymarketWs, Asset::Btc, None);
        let btc_tv = fmt_tick(&latest, PriceSource::TradingView, Asset::Btc, None);
        let btc_cq = fmt_tick(&latest, PriceSource::CryptoQuant, Asset::Btc, None);
        let poly_parts = tracked_timeframes
            .iter()
            .map(|tf| {
                format!(
                    "{} {}",
                    tf,
                    fmt_tick(&latest, PriceSource::Polymarket, Asset::Btc, Some(*tf))
                )
            })
            .collect::<Vec<_>>()
            .join(" | ");

        info!(
            "[Tape] BTC(binance={}, poly_ws={}, tv={}, cq={}) | POLY({})",
            btc_binance,
            btc_poly_ws,
            btc_tv,
            btc_cq,
            poly_parts
        );
    }
}

fn rpc_endpoint_label(raw: &str) -> String {
    if let Ok(url) = Url::parse(raw) {
        if let Some(host) = url.host_str() {
            return host.to_string();
        }
    }
    raw.to_string()
}

fn polygon_rpc_candidates(cfg: &Config) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    for rpc in std::iter::once(cfg.polygon_rpc_url.as_str())
        .chain(cfg.polygon_rpc_fallback_urls.iter().map(String::as_str))
    {
        let trimmed = rpc.trim();
        if trimmed.is_empty() {
            continue;
        }

        let key = trimmed.to_ascii_lowercase();
        if seen.insert(key) {
            out.push(trimmed.to_string());
        }
    }

    if out.is_empty() {
        out.push("https://polygon.publicnode.com".to_string());
    }

    out
}

fn short_token(token_id: &str) -> String {
    if token_id.len() <= 14 {
        return token_id.to_string();
    }
    format!("{}...{}", &token_id[..6], &token_id[token_id.len() - 6..])
}

fn fmt_prob(v: Option<f64>) -> String {
    v.map(|p| format!("{:.4}", p))
        .unwrap_or_else(|| "n/a".to_string())
}

fn top_book_depth_usdc(book: &polymarket::client::OrderBook) -> f64 {
    let mut bids = book.bids.clone();
    bids.sort_by(|a, b| b.price.total_cmp(&a.price));
    let bid_depth: f64 = bids
        .iter()
        .take(5)
        .map(|l| l.price.clamp(0.0, 1.0) * l.size.max(0.0))
        .sum();

    let mut asks = book.asks.clone();
    asks.sort_by(|a, b| a.price.total_cmp(&b.price));
    let ask_depth: f64 = asks
        .iter()
        .take(5)
        .map(|l| l.price.clamp(0.0, 1.0) * l.size.max(0.0))
        .sum();

    bid_depth + ask_depth
}

async fn log_book_snapshot(client: &Arc<PolymarketClient>, label: &str, token_id: &str) {
    match client.get_order_book(token_id).await {
        Ok(book) => {
            if let Some((bid, ask)) = book.best_bid_ask() {
                let mid = (bid + ask) / 2.0;
                let spread = (ask - bid).max(0.0);
                let depth = top_book_depth_usdc(&book);
                info!(
                    "[Init] {} book | token={} bid={:.4} ask={:.4} mid={:.4} spread={:.4} depth=${:.2}",
                    label,
                    short_token(token_id),
                    bid,
                    ask,
                    mid,
                    spread,
                    depth
                );
            } else {
                warn!(
                    "[Init] {} book has no valid top-of-book | token={}",
                    label,
                    short_token(token_id)
                );
            }
        }
        Err(e) => warn!(
            "[Init] {} book fetch failed | token={} | {}",
            label,
            short_token(token_id),
            e
        ),
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    
    let mut cfg = Config::from_env().context("Config error")?;

    if cfg.enable_tui {
        let file = std::fs::File::create("bot.log").expect("could not create log file");
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive(tracing::Level::INFO.into()),
            )
            .with_writer(std::sync::Arc::new(file))
            .compact()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive(tracing::Level::INFO.into()),
            )
            .compact()
            .init();
    }

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("  Polymarket Latency Arb Bot  v2.0              ");
    info!("  BTC | Up/Down configurable TF | Kelly | Paper-first ");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Safety check
    if cfg.is_live() {
        warn!("⚠️  LIVE TRADING MODE — real orders will be placed!");
    } else {
        info!("📋 PAPER TRADING MODE — no real orders");
    }

    let configured_timeframes = cfg
        .market_timeframes
        .iter()
        .map(|tf| tf.to_string())
        .collect::<Vec<_>>()
        .join("+");
    info!("[Init] Enabled BTC contract timeframes: {}", configured_timeframes);
    info!("[Init] Polymarket poll cadence: {}ms", cfg.poly_poll_ms);
    info!(
        "[Init] Entry guardrails: MIN_TRADE_INTERVAL_MS={} | PAPER_SINGLE_POSITION_PER_SLOT={}",
        cfg.min_trade_interval_ms,
        cfg.paper_single_position_per_slot
    );

    // Shared services
    let poly_client = Arc::new(PolymarketClient::new(&cfg)?);

    if cfg.paper_trading {
        let mut bankroll_set = false;

        if let Some(proxy_wallet) = cfg.proxy_wallet.as_deref() {
            let rpc_candidates = polygon_rpc_candidates(&cfg);
            for (idx, rpc_url) in rpc_candidates.iter().enumerate() {
                let label = rpc_endpoint_label(rpc_url);
                match poly_client.get_usdc_balance_onchain(rpc_url, proxy_wallet).await {
                    Ok(wallet_balance) if wallet_balance > 0.0 => {
                        let prev = cfg.portfolio_size_usdc;
                        cfg.portfolio_size_usdc = wallet_balance;
                        bankroll_set = true;
                        info!(
                            "[Init] Paper bankroll set from Polygon USDC.e balanceOf(PROXY_WALLET) via {}: ${:.2} (PORTFOLIO_SIZE_USDC was ${:.2})",
                            label,
                            wallet_balance,
                            prev
                        );
                        break;
                    }
                    Ok(_) => {
                        let has_more = idx + 1 < rpc_candidates.len();
                        if has_more {
                            warn!(
                                "[Init] Polygon RPC {} returned zero balance; trying next RPC endpoint",
                                label
                            );
                        } else {
                            warn!(
                                "[Init] Polygon RPC {} returned zero balance; trying CLOB balance fallback",
                                label
                            );
                        }
                    }
                    Err(e) => {
                        let has_more = idx + 1 < rpc_candidates.len();
                        if has_more {
                            warn!(
                                "[Init] Polygon RPC {} failed: {}. Trying next RPC endpoint",
                                label,
                                e
                            );
                        } else {
                            warn!(
                                "[Init] Polygon USDC.e balanceOf(PROXY_WALLET) failed on all RPC endpoints: {}. Trying CLOB balance fallback",
                                e
                            );
                        }
                    }
                }
            }
        } else {
            info!("[Init] PROXY_WALLET not configured; skipping Polygon balanceOf fetch");
        }

        if !bankroll_set {
            let has_auth = !cfg.polymarket_api_key.trim().is_empty()
                && !cfg.polymarket_api_secret.trim().is_empty()
                && !cfg.polymarket_api_passphrase.trim().is_empty()
                && !cfg.polymarket_api_key.eq_ignore_ascii_case("paper_mode")
                && !cfg.polymarket_api_secret.eq_ignore_ascii_case("paper_mode")
                && !cfg.polymarket_api_passphrase.eq_ignore_ascii_case("paper_mode");

            if has_auth {
                match poly_client.get_collateral_balance_usdc().await {
                    Ok(wallet_balance) if wallet_balance > 0.0 => {
                        let prev = cfg.portfolio_size_usdc;
                        cfg.portfolio_size_usdc = wallet_balance;
                        bankroll_set = true;
                        info!(
                            "[Init] Paper bankroll set from CLOB collateral balance: ${:.2} (PORTFOLIO_SIZE_USDC was ${:.2})",
                            wallet_balance,
                            prev
                        );
                    }
                    Ok(_) => {
                        warn!(
                            "[Init] CLOB collateral balance returned zero; using PORTFOLIO_SIZE_USDC=${:.2}",
                            cfg.portfolio_size_usdc
                        );
                    }
                    Err(e) => {
                        warn!(
                            "[Init] CLOB collateral balance fetch failed: {}. Using PORTFOLIO_SIZE_USDC=${:.2}",
                            e,
                            cfg.portfolio_size_usdc
                        );
                    }
                }
            }
        }

        if !bankroll_set {
            info!(
                "[Init] Paper bankroll using PORTFOLIO_SIZE_USDC=${:.2} fallback",
                cfg.portfolio_size_usdc
            );
        }
    }

    let risk = Arc::new(RiskManager::new(cfg.clone()));
    let tg = Arc::new(Telegram::new(
        cfg.enable_telegram,
        cfg.telegram_bot_token.clone(),
        cfg.telegram_chat_id.clone(),
    ));
    let db = Arc::new(Database::open("trades.db", cfg.enable_database)?);

    let startup_open_exposure: f64 = db
        .open_positions()
        .unwrap_or_default()
        .iter()
        .map(|t| t.size_usdc)
        .sum();
    risk.set_open_exposure_usdc(startup_open_exposure);

    // Build contract slots from configured timeframes.
    // For up/down markets there is no fixed strike; we use 0.0 as a sentinel.
    // The detector uses candle-open price as the timeframe-specific reference.
    let eod_ts = {
        let now = chrono::Utc::now();
        let eod = now.date_naive().and_hms_opt(23, 59, 59).unwrap();
        chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(eod, chrono::Utc).timestamp()
    };

    let mut contracts = Vec::new();
    for timeframe in &cfg.market_timeframes {
        info!("[Init] Discovering Polymarket BTC {} market...", timeframe);
        let market = poly_client
            .get_btc_market_for_timeframe(*timeframe)
            .await
            .with_context(|| format!("Failed to fetch BTC {} market", timeframe))?;
        info!("[Init] BTC {}: \"{}\"", timeframe, market.question);

        if let Some(event_url) = market.event_url.as_deref() {
            info!("[Init] Market URL | tf={} {}", timeframe, event_url);
        }
        if let Some(market_id) = market.market_id.as_deref() {
            info!("[Init] Market ID | tf={} {}", timeframe, market_id);
        }

        let slug = market
            .market_slug
            .as_deref()
            .unwrap_or("n/a");
        let end_date = market
            .end_date_iso
            .as_deref()
            .unwrap_or("n/a");
        info!(
            "[Init] Market details | tf={} condition_id={} slug={} active={} closed={} end_date={}",
            timeframe,
            market.condition_id,
            slug,
            market.active,
            market.closed,
            end_date
        );

        let token_line = market
            .tokens
            .iter()
            .map(|t| format!("{}={}", t.outcome, short_token(&t.token_id)))
            .collect::<Vec<_>>()
            .join(" | ");
        info!("[Init] Market tokens | {}", token_line);

        let yes_token = market
            .tokens
            .iter()
            .find(|t| t.outcome.eq_ignore_ascii_case("yes"))
            .cloned()
            .with_context(|| format!("No YES token in BTC {} market", timeframe))?;
        let no_token = market
            .tokens
            .iter()
            .find(|t| t.outcome.eq_ignore_ascii_case("no"))
            .cloned()
            .with_context(|| format!("No NO token in BTC {} market", timeframe))?;

        log_book_snapshot(&poly_client, &format!("BTC {} YES", timeframe), &yes_token.token_id).await;
        log_book_snapshot(&poly_client, &format!("BTC {} NO", timeframe), &no_token.token_id).await;

        let yes_prices = poly_client.get_token_side_prices(&yes_token.token_id).await;
        let no_prices = poly_client.get_token_side_prices(&no_token.token_id).await;
        info!(
            "[Init] BTC {} /price | YES(buy={} sell={}) NO(buy={} sell={})",
            timeframe,
            fmt_prob(yes_prices.best_buy),
            fmt_prob(yes_prices.best_sell),
            fmt_prob(no_prices.best_buy),
            fmt_prob(no_prices.best_sell),
        );

        contracts.push(ContractSlot {
            asset: Asset::Btc,
            timeframe: *timeframe,
            title: market.question.clone(),
            yes_token,
            no_token,
            strike: 0.0,
            expiry_ts: eod_ts,
        });
    }

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
        let enable_signal_1s = cfg.enable_binance_signal_1s;
        let signal_threshold_pct = cfg.binance_signal_threshold_pct;
        handles.push(tokio::spawn(async move {
            let feed = BinanceFeed::new(tx, enable_signal_1s, signal_threshold_pct);
            if let Err(e) = feed.run().await { error!("[Binance] fatal: {e}"); }
        }));
    }

    // Polymarket live-data WS Chainlink reference feed (BTC/ETH)
    if !cfg.polymarket_live_ws_url.trim().is_empty() {
        let tx = raw_tx.clone();
        let ws_url = cfg.polymarket_live_ws_url.clone();
        let symbol_includes = cfg.polymarket_live_symbol_includes.clone();
        handles.push(tokio::spawn(async move {
            let feed = PolymarketLiveWsFeed::new(&ws_url, &symbol_includes, tx);
            if let Err(e) = feed.run().await {
                error!("[PolymarketLiveWS] fatal: {e}");
            }
        }));
    } else {
        info!("[PolymarketLiveWS] Disabled (POLYMARKET_LIVE_WS_URL is empty)");
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
    if !cfg.cryptoquant_api_key.is_empty()
        && !cfg.cryptoquant_api_key.starts_with("your_")
        && cfg.cryptoquant_api_key != "REPLACE_ME"
    {
        let tx = raw_tx.clone();
        let key = cfg.cryptoquant_api_key.clone();
        handles.push(tokio::spawn(async move {
            let feed = CryptoQuantFeed::new(&key, tx);
            if let Err(e) = feed.run().await { error!("[CryptoQuant] fatal: {e}"); }
        }));
    } else {
        info!("[CryptoQuant] Disabled (no real API key configured)");
    }

    // Polymarket pollers for each contract
    let mut seen_tokens = HashSet::new();
    for slot in &contracts {
        let client = poly_client.clone();
        let tx = raw_tx.clone();
        let token_id = slot.yes_token.token_id.clone();
        if !seen_tokens.insert(token_id.clone()) {
            continue;
        }
        let asset = slot.asset;
        let timeframe = slot.timeframe;
        let strike = slot.strike;
        let poll_ms = cfg.poly_poll_ms;
        handles.push(tokio::spawn(async move {
            let poller = PolymarketPoller::new(client, token_id, asset, timeframe, strike, poll_ms, tx);
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
        let latest = latest_prices.clone();
        let tfs = cfg.market_timeframes.clone();
        handles.push(tokio::spawn(async move { stats_printer(r, latest, tfs).await; }));
    }

    // Startup Telegram notification
    let mode_str = if cfg.is_live() { "🔴 LIVE" } else { "📋 PAPER" };
    let _ = tg
        .send(&format!(
            "🚀 Bot started | Mode: {mode_str} | Tracking BTC {} + ETH spot feeds",
            configured_timeframes
        ))
        .await;

    if cfg.enable_tui {
        let market_titles: Vec<String> = contracts.iter().map(|c| format!("BTC {}: {}", c.timeframe, c.title)).collect();
        info!("[Runtime] TUI dashboard enabled. Press q/Esc to stop.");
        let dash = Dashboard::new(db.clone(), risk.clone(), cfg.clone(), market_titles);
        if let Err(e) = dash.run(shutdown_rx).await {
            warn!("[Dashboard] failed: {}. Falling back to console mode.", e);
            info!("[Runtime] Console-only mode active. Press Ctrl+C to stop.");
            tokio::signal::ctrl_c()
                .await
                .context("wait for Ctrl+C")?;
        }
    } else {
        info!("[Runtime] Console-only mode (ENABLE_TUI=false). Press Ctrl+C to stop.");
        tokio::signal::ctrl_c()
            .await
            .context("wait for Ctrl+C")?;
    }

    // Graceful shutdown
    let _ = shutdown_tx.send(true);
    let _ = tg.send("🛑 Bot stopped.").await;
    info!("Bot stopped");

    Ok(())
}

