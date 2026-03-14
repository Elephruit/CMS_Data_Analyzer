use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use super::month::YearMonth;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StoreManifest {
    pub ingested_months: Vec<YearMonth>,
    pub source_hashes: HashMap<String, String>, // yyyy-mm -> sha256
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestStats {
    pub month: YearMonth,
    pub source_url: String,
    pub source_hash: String,
    pub total_rows: u64,
    pub kept_rows: u64,
    pub star_rows: u64,
    pub malformed_rows: u64,
    pub plans_resolved: u32,
    pub counties_resolved: u32,
    pub series_touched: u32,
    pub status: String,
}
