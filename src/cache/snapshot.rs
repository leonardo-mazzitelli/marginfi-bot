use std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solana_sdk::{clock::Clock, pubkey::Pubkey};

use super::Cache;

const SNAPSHOT_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
pub struct SnapshotAccount {
    pub address: Pubkey,
    pub slot: u64,
    pub data: Vec<u8>,
}

impl SnapshotAccount {
    pub fn new(address: Pubkey, slot: u64, data: Vec<u8>) -> Self {
        Self {
            address,
            slot,
            data,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct CacheSnapshot {
    version: u32,
    generated_at_unix: u64,
    clock: Clock,
    marginfi_accounts: Vec<SnapshotAccount>,
    banks: Vec<SnapshotAccount>,
}

impl CacheSnapshot {
    fn capture(cache: &Cache) -> Result<Self> {
        Ok(Self {
            version: SNAPSHOT_VERSION,
            generated_at_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or_default(),
            clock: cache.get_clock()?,
            marginfi_accounts: cache.marginfi_accounts.snapshot_entries()?,
            banks: cache.banks.snapshot_entries()?,
        })
    }
}

pub fn restore_cache_snapshot(cache: &Cache, path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    let bytes = fs::read(path)
        .with_context(|| format!("Failed to read cache snapshot from {}", path.display()))?;

    let snapshot: CacheSnapshot = bincode::deserialize(&bytes)
        .with_context(|| format!("Failed to deserialize cache snapshot {}", path.display()))?;

    if snapshot.version != SNAPSHOT_VERSION {
        return Ok(false);
    }

    cache.update_clock(snapshot.clock)?;
    cache
        .marginfi_accounts
        .restore_from_snapshot(&snapshot.marginfi_accounts)?;
    cache.banks.restore_from_snapshot(&snapshot.banks)?;
    Ok(true)
}

pub fn persist_cache_snapshot(cache: &Cache, path: &Path) -> Result<()> {
    let snapshot = CacheSnapshot::capture(cache)?;
    let data = bincode::serialize(&snapshot)?;
    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, data)
        .with_context(|| format!("Failed to write cache snapshot to {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "Failed to finalize cache snapshot rename from {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}
