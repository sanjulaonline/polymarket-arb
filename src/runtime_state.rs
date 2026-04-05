use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::warn;

use crate::database::Database;
use crate::risk::{RiskManager, RiskSnapshot};
use crate::types::TradeRecord;

const STATE_VERSION: u32 = 1;
const MAX_RECENT_TRADES: usize = 50;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStateSnapshot {
    pub version: u32,
    pub mode: String,
    pub started_at_unix: i64,
    pub updated_at_unix: i64,
    pub risk: RiskSnapshot,
    pub recent_trades: Vec<TradeRecord>,
}

impl RuntimeStateSnapshot {
    pub fn load(path: &str) -> Result<Option<Self>> {
        let file = Path::new(path);
        if !file.exists() {
            return Ok(None);
        }

        let raw = fs::read_to_string(file)
            .with_context(|| format!("Failed reading state file {}", file.display()))?;
        let state: Self = serde_json::from_str(&raw)
            .with_context(|| format!("Failed parsing state file {}", file.display()))?;
        Ok(Some(state))
    }

    pub fn save(&self, path: &str) -> Result<()> {
        let file = Path::new(path);
        if let Some(parent) = file.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed creating state directory {}", parent.display())
                })?;
            }
        }

        let tmp_path = temp_path(file);
        let payload = serde_json::to_string_pretty(self).context("Serialize runtime state")?;
        fs::write(&tmp_path, payload)
            .with_context(|| format!("Write temp state file {}", tmp_path.display()))?;
        fs::rename(&tmp_path, file).with_context(|| {
            format!(
                "Atomically replace state file {} from {}",
                file.display(),
                tmp_path.display()
            )
        })?;

        Ok(())
    }
}

fn temp_path(path: &Path) -> PathBuf {
    let mut out = path.to_path_buf();
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("runtime_state.json");
    out.set_file_name(format!("{}.tmp", file_name));
    out
}

fn build_snapshot(mode: &str, started_at_unix: i64, risk: &RiskManager, db: &Database) -> RuntimeStateSnapshot {
    let recent_trades = db
        .last_trades(MAX_RECENT_TRADES)
        .unwrap_or_default();

    RuntimeStateSnapshot {
        version: STATE_VERSION,
        mode: mode.to_string(),
        started_at_unix,
        updated_at_unix: Utc::now().timestamp(),
        risk: risk.to_snapshot(),
        recent_trades,
    }
}

pub async fn persist_loop(
    path: String,
    mode: String,
    started_at_unix: i64,
    interval_secs: u64,
    risk: Arc<RiskManager>,
    db: Arc<Database>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut iv = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs.max(5)));

    loop {
        tokio::select! {
            _ = iv.tick() => {
                let state = build_snapshot(&mode, started_at_unix, &risk, &db);
                if let Err(e) = state.save(&path) {
                    warn!("[State] save failed: {}", e);
                }
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }

    let state = build_snapshot(&mode, started_at_unix, &risk, &db);
    if let Err(e) = state.save(&path) {
        warn!("[State] final save failed: {}", e);
    }
}
