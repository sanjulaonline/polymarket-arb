use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection};
use std::sync::{Arc, Mutex};
use tracing::info;

use crate::types::TradeRecord;

pub struct Database {
    conn: Option<Arc<Mutex<Connection>>>,
    enabled: bool,
}

impl Database {
    pub fn open(path: &str, enabled: bool) -> Result<Self> {
        if !enabled {
            info!("[DB] Persistence disabled");
            return Ok(Self {
                conn: None,
                enabled: false,
            });
        }

        let conn = Connection::open(path).context("Failed to open SQLite database")?;
        let db = Self {
            conn: Some(Arc::new(Mutex::new(conn))),
            enabled: true,
        };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let conn = self.conn.as_ref().expect("db conn missing").lock().unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;

             CREATE TABLE IF NOT EXISTS trades (
                 id          INTEGER PRIMARY KEY AUTOINCREMENT,
                 asset       TEXT    NOT NULL,
                 timeframe   TEXT    NOT NULL,
                 direction   TEXT    NOT NULL,
                 size_usdc   REAL    NOT NULL,
                 entry_prob  REAL    NOT NULL,
                 cex_prob    REAL    NOT NULL,
                 edge_pct    REAL    NOT NULL,
                 confidence  REAL    NOT NULL,
                 kelly_frac  REAL    NOT NULL,
                 paper       INTEGER NOT NULL DEFAULT 1,
                 order_id    TEXT,
                 entry_ref_price REAL,
                 pnl_usdc    REAL,
                 outcome     TEXT    DEFAULT 'OPEN',
                 opened_at   TEXT    NOT NULL,
                 closed_at   TEXT
             );

             CREATE TABLE IF NOT EXISTS daily_stats (
                 date        TEXT    PRIMARY KEY,
                 pnl_usdc    REAL    DEFAULT 0,
                 trade_count INTEGER DEFAULT 0,
                 win_count   INTEGER DEFAULT 0
             );
            ",
        )
        .context("Schema migration failed")?;

        let has_entry_ref_price = {
            let mut stmt = conn
                .prepare("PRAGMA table_info(trades)")
                .context("Failed to inspect trades schema")?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .context("Failed to read trades schema columns")?;

            let mut has = false;
            for col_name in rows {
                if col_name
                    .context("Failed to parse schema column")?
                    .eq_ignore_ascii_case("entry_ref_price")
                {
                    has = true;
                    break;
                }
            }
            has
        };

        if !has_entry_ref_price {
            conn.execute(
                "ALTER TABLE trades ADD COLUMN entry_ref_price REAL",
                [],
            )
            .context("Failed adding trades.entry_ref_price")?;
            info!("[DB] Added trades.entry_ref_price column");
        }

        info!("[DB] Schema ready");
        Ok(())
    }

    /// Insert a new trade and return its row id.
    pub fn insert_trade(&self, t: &TradeRecord) -> Result<i64> {
        if !self.enabled {
            return Ok(0);
        }
        let conn = self.conn.as_ref().expect("db conn missing").lock().unwrap();
        conn.execute(
            "INSERT INTO trades
             (asset, timeframe, direction, size_usdc, entry_prob, cex_prob, edge_pct,
              confidence, kelly_frac, paper, order_id, entry_ref_price, outcome, opened_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,'OPEN',?13)",
            params![
                t.asset,
                t.timeframe,
                t.direction,
                t.size_usdc,
                t.entry_prob,
                t.cex_prob,
                t.edge_pct,
                t.confidence,
                t.kelly_fraction,
                t.paper as i32,
                t.order_id,
                t.entry_ref_price,
                t.opened_at.to_rfc3339(),
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Close out a trade with final PnL.
    pub fn close_trade(&self, id: i64, pnl_usdc: f64, outcome: &str) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let conn = self.conn.as_ref().expect("db conn missing").lock().unwrap();
        conn.execute(
            "UPDATE trades SET pnl_usdc=?1, outcome=?2, closed_at=?3 WHERE id=?4",
            params![pnl_usdc, outcome, Utc::now().to_rfc3339(), id],
        )?;

        // Update daily stats
        let date = Utc::now().format("%Y-%m-%d").to_string();
        let won = if outcome == "WIN" { 1i64 } else { 0 };
        conn.execute(
            "INSERT INTO daily_stats (date, pnl_usdc, trade_count, win_count)
             VALUES (?1, ?2, 1, ?3)
             ON CONFLICT(date) DO UPDATE SET
               pnl_usdc    = pnl_usdc + excluded.pnl_usdc,
               trade_count = trade_count + 1,
               win_count   = win_count + excluded.win_count",
            params![date, pnl_usdc, won],
        )?;
        Ok(())
    }

    /// Fetch last N trades for the dashboard.
    pub fn last_trades(&self, n: usize) -> Result<Vec<TradeRecord>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        let conn = self.conn.as_ref().expect("db conn missing").lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, asset, timeframe, direction, size_usdc, entry_prob, cex_prob,
                    edge_pct, confidence, kelly_frac, paper, order_id,
                    entry_ref_price, pnl_usdc, outcome, opened_at, closed_at
             FROM trades ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![n as i64], |row| {
            Ok(TradeRecord {
                id: row.get(0)?,
                asset: row.get(1)?,
                timeframe: row.get(2)?,
                direction: row.get(3)?,
                entry_ref_price: row.get(12)?,
                size_usdc: row.get(4)?,
                entry_prob: row.get(5)?,
                cex_prob: row.get(6)?,
                edge_pct: row.get(7)?,
                confidence: row.get(8)?,
                kelly_fraction: row.get(9)?,
                paper: row.get::<_, i32>(10)? != 0,
                order_id: row.get(11)?,
                pnl_usdc: row.get(13)?,
                outcome: row.get(14)?,
                opened_at: {
                    let s: String = row.get(15)?;
                    s.parse().unwrap_or_else(|_| Utc::now())
                },
                closed_at: row.get::<_, Option<String>>(16)?
                    .and_then(|s| s.parse().ok()),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Open positions (outcome = 'OPEN').
    pub fn open_positions(&self) -> Result<Vec<TradeRecord>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        let conn = self.conn.as_ref().expect("db conn missing").lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, asset, timeframe, direction, size_usdc, entry_prob, cex_prob,
                    edge_pct, confidence, kelly_frac, paper, order_id,
                    entry_ref_price, pnl_usdc, outcome, opened_at, closed_at
             FROM trades WHERE outcome = 'OPEN' ORDER BY id DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(TradeRecord {
                id: row.get(0)?,
                asset: row.get(1)?,
                timeframe: row.get(2)?,
                direction: row.get(3)?,
                entry_ref_price: row.get(12)?,
                size_usdc: row.get(4)?,
                entry_prob: row.get(5)?,
                cex_prob: row.get(6)?,
                edge_pct: row.get(7)?,
                confidence: row.get(8)?,
                kelly_fraction: row.get(9)?,
                paper: row.get::<_, i32>(10)? != 0,
                order_id: row.get(11)?,
                pnl_usdc: row.get(13)?,
                outcome: row.get(14)?,
                opened_at: {
                    let s: String = row.get(15)?;
                    s.parse().unwrap_or_else(|_| Utc::now())
                },
                closed_at: row.get::<_, Option<String>>(16)?
                    .and_then(|s| s.parse().ok()),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Daily stats for today.
    pub fn today_stats(&self) -> Result<(f64, i64, i64)> {
        if !self.enabled {
            return Ok((0.0, 0, 0));
        }
        let date = Utc::now().format("%Y-%m-%d").to_string();
        let conn = self.conn.as_ref().expect("db conn missing").lock().unwrap();
        let result = conn.query_row(
            "SELECT pnl_usdc, trade_count, win_count FROM daily_stats WHERE date=?1",
            params![date],
            |row| Ok((row.get::<_, f64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
        );
        match result {
            Ok(v) => Ok(v),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok((0.0, 0, 0)),
            Err(e) => Err(e.into()),
        }
    }
}
