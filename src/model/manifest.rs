use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use super::month::YearMonth;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StoreManifest {
    pub ingested_months: Vec<YearMonth>,
    pub source_hashes: HashMap<String, String>, // yyyy-mm -> sha256
}
