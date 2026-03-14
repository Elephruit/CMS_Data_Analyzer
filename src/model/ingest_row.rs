use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedRow {
    pub contract_id: String,
    pub plan_id: String,
    pub plan_name: String,
    pub state_code: String,
    pub county_name: String,
    pub enrollment: u32,
}
