/// Terminal dashboard — renders a live TUI using ratatui.
/// Shows: P&L, win rate, open positions, last 10 trades, kill-switch status.
use anyhow::Result;
use chrono::Utc;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
    Frame, Terminal,
};
use std::{
    io,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::watch;

use crate::{
    config::Config,
    database::Database,
    monitor::SharedMonitorState,
    risk::RiskManager,
};

pub struct Dashboard {
    db: Arc<Database>,
    risk: Arc<RiskManager>,
    cfg: Config,
    market_titles: Vec<String>,
    monitor_state: SharedMonitorState,
}

impl Dashboard {
    pub fn new(
        db: Arc<Database>,
        risk: Arc<RiskManager>,
        cfg: Config,
        market_titles: Vec<String>,
        monitor_state: SharedMonitorState,
    ) -> Self {
        Self {
            db,
            risk,
            cfg,
            market_titles,
            monitor_state,
        }
    }

    pub async fn run(&self, shutdown: watch::Receiver<bool>) -> Result<()> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let tick = Duration::from_millis(500);
        let mut last_tick = Instant::now();

        loop {
            // Check shutdown signal
            if *shutdown.borrow() {
                break;
            }

            terminal.draw(|f| self.render(f))?;

            let timeout = tick
                .checked_sub(last_tick.elapsed())
                .unwrap_or_default();

            if event::poll(timeout)? {
                if let Event::Key(key) = event::read()? {
                    if key.code == KeyCode::Char('q') || key.code == KeyCode::Esc {
                        break;
                    }
                }
            }

            if last_tick.elapsed() >= tick {
                last_tick = Instant::now();
            }

            // Yield to tokio scheduler
            tokio::task::yield_now().await;
        }

        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        Ok(())
    }

    fn render(&self, f: &mut Frame) {
        let size = f.size();

        // Main layout: header | stats | trades | monitor | footer
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),  // header
                Constraint::Length(6),  // stats row
                Constraint::Min(8),     // trades tables
                Constraint::Length(6),  // pair monitor
                Constraint::Length(1),  // footer
            ])
            .split(size);

        // ── Header ────────────────────────────────────────────────────────────
        let mode_color = if self.cfg.is_live() { Color::Red } else { Color::Yellow };
        let mode_label = if self.cfg.is_live() { "🔴 LIVE" } else { "📋 PAPER" };
        let header = Paragraph::new(format!(
            " Polymarket Arb Bot  |  {}  |  {}\n {}",
            mode_label,
            Utc::now().format("%H:%M:%S UTC"),
            self.market_titles.join("  |  ")
        ))
        .style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD))
        .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(mode_color)));
        f.render_widget(header, chunks[0]);

        // ── Stats ─────────────────────────────────────────────────────────────
        let stats_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Ratio(1, 6),
                Constraint::Ratio(1, 6),
                Constraint::Ratio(1, 6),
                Constraint::Ratio(1, 6),
                Constraint::Ratio(1, 6),
                Constraint::Ratio(1, 6),
            ])
            .split(chunks[1]);

        let (daily_pnl, trade_count, win_rate, halted) = self.risk.snapshot();
        let (_db_pnl, db_trades, db_wins) = self.db.today_stats().unwrap_or((0.0, 0, 0));
        let (poly_age_ms, poly_fallback_rate, _poly_fallbacks, poly_lookups) = self.risk.poly_data_metrics();

        let pnl_color = if daily_pnl >= 0.0 { Color::Green } else { Color::Red };
        let halted_str = if halted { " ⛔ HALTED" } else { " ✓ RUNNING" };
        let halted_color = if halted { Color::Red } else { Color::Green };
        let poly_age_str = if poly_lookups > 0 && poly_age_ms >= 0 {
            format!("{}ms", poly_age_ms)
        } else {
            "n/a".to_string()
        };
        let poly_color = if poly_fallback_rate >= 30.0 {
            Color::Red
        } else if poly_fallback_rate >= 10.0 {
            Color::Yellow
        } else {
            Color::Green
        };

        let mut max_lag = 0.0_f64;
        let mut lag_vec: Vec<String> = self.risk.latest_lags.iter().map(|ref kv| {
            let v = *kv.value();
            if v > max_lag { max_lag = v; }
            format!("{}: {:.3}%", kv.key(), v)
        }).collect();
        lag_vec.sort();
        let lag_str = if lag_vec.is_empty() { "Waiting...".to_string() } else { lag_vec.join(" | ") };
        let lag_color = if max_lag >= self.cfg.lag_threshold_pp { Color::Green } else if max_lag >= 0.1 { Color::Yellow } else { Color::DarkGray };

        let stat_widgets = [
            (
                "Daily P&L",
                format!("${:+.2}", daily_pnl),
                pnl_color,
            ),
            (
                "Win Rate",
                format!("{:.1}%  ({}/{})", win_rate, db_wins, db_trades),
                Color::Cyan,
            ),
            (
                "Trades Today",
                format!("{}", trade_count),
                Color::White,
            ),
            (
                "Status",
                halted_str.to_string(),
                halted_color,
            ),
            (
                "Poly Data",
                format!("{} | {:.1}% fb", poly_age_str, poly_fallback_rate),
                poly_color,
            ),
            (
                "Live Lag (Poly vs CEX)",
                lag_str,
                lag_color,
            ),
        ];

        for (i, (label, value, color)) in stat_widgets.iter().enumerate() {
            let w = Paragraph::new(vec![
                Line::from(Span::styled(value.as_str(), Style::default().fg(*color).add_modifier(Modifier::BOLD))),
            ])
            .block(Block::default().title(*label).borders(Borders::ALL));
            f.render_widget(w, stats_chunks[i]);
        }

        // ── Trade tables ──────────────────────────────────────────────────────
        let table_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(chunks[2]);

        // Open positions
        let open = self.db.open_positions().unwrap_or_default();
        let open_rows: Vec<Row> = open.iter().map(|t| {
            Row::new(vec![
                Cell::from(format!("{}/{}", t.asset, t.timeframe)),
                Cell::from(t.direction.as_str()),
                Cell::from(format!("${:.0}", t.size_usdc)),
                Cell::from(format!("{:.1}%", t.edge_pct)),
                Cell::from(format!("{:.0}%", t.confidence * 100.0)),
            ])
        }).collect();
        let open_table = Table::new(
            open_rows,
            [
                Constraint::Length(10),
                Constraint::Length(5),
                Constraint::Length(7),
                Constraint::Length(7),
                Constraint::Length(6),
            ],
        )
        .header(Row::new(["Market", "Dir", "Size", "Edge", "Conf"]).style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Yellow)))
        .block(Block::default().title(format!(" Open Positions ({}) ", open.len())).borders(Borders::ALL));
        f.render_widget(open_table, table_chunks[0]);

        // Last 10 trades
        let recent = self.db.last_trades(10).unwrap_or_default();
        let recent_rows: Vec<Row> = recent.iter().map(|t| {
            let (outcome_str, outcome_color) = match t.outcome.as_deref() {
                Some("WIN") => ("WIN ", Color::Green),
                Some("LOSS") => ("LOSS", Color::Red),
                _ => ("OPEN", Color::Yellow),
            };
            let pnl_str = t.pnl_usdc.map(|p| format!("{:+.1}", p)).unwrap_or_else(|| "—".to_string());
            Row::new(vec![
                Cell::from(format!("{}/{}", t.asset, t.timeframe)),
                Cell::from(t.direction.as_str()),
                Cell::from(format!("${:.0}", t.size_usdc)),
                Cell::from(pnl_str),
                Cell::from(outcome_str).style(Style::default().fg(outcome_color)),
            ])
        }).collect();
        let recent_table = Table::new(
            recent_rows,
            [
                Constraint::Length(10),
                Constraint::Length(5),
                Constraint::Length(7),
                Constraint::Length(7),
                Constraint::Length(5),
            ],
        )
        .header(Row::new(["Market", "Dir", "Size", "PnL", "Result"]).style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Yellow)))
        .block(Block::default().title(" Last 10 Trades ").borders(Borders::ALL));
        f.render_widget(recent_table, table_chunks[1]);

        // ── Pair monitor ────────────────────────────────────────────────────
        let (latest_prices, recent_arbitrage) = {
            let guard = self.monitor_state.read();
            (guard.latest_prices(), guard.recent_arbitrage(4))
        };

        let mut monitor_lines: Vec<Line> = Vec::new();
        if latest_prices.is_empty() {
            monitor_lines.push(Line::from(Span::styled(
                "Waiting for monitor data...",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for price in latest_prices.into_iter().take(3) {
                let arb_flag = if price.has_arbitrage { "ARB" } else { "-" };
                let arb_color = if price.has_arbitrage {
                    Color::Green
                } else {
                    Color::DarkGray
                };
                monitor_lines.push(Line::from(vec![
                    Span::styled(
                        format!("{:8}", price.market),
                        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(format!(
                        " UP_ASK={:.4} DOWN_ASK={:.4} ASK_SUM={:.4} SPREAD={:+.4} ",
                        price.up_ask, price.down_ask, price.ask_sum, price.spread
                    )),
                    Span::styled(arb_flag, Style::default().fg(arb_color)),
                ]));
            }
        }

        if let Some(last_arb) = recent_arbitrage.first() {
            monitor_lines.push(Line::from(Span::styled(
                format!(
                    "Last ARB {}: ask_sum={:.4} spread={:+.4} ({:.2}%)",
                    last_arb.market,
                    last_arb.detection.ask_sum,
                    last_arb.detection.spread,
                    last_arb.detection.spread_percent
                ),
                Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
            )));
        }

        let monitor_panel = Paragraph::new(monitor_lines)
            .block(Block::default().title(" Pair Monitor (YES + NO asks) ").borders(Borders::ALL));
        f.render_widget(monitor_panel, chunks[3]);

        // ── Footer ────────────────────────────────────────────────────────────
        let footer = Paragraph::new(" [q] Quit  |  Refreshes every 500ms")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center);
        f.render_widget(footer, chunks[4]);
    }
}
