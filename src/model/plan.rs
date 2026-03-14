use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanDim {
    pub plan_key: u32,
    pub contract_id: String,
    pub plan_id: String,
    pub plan_name: String,
    pub valid_from_month: u32, // yyyymm
    pub valid_to_month: Option<u32>, // yyyymm
    pub is_current: bool,
}

impl PlanDim {
}
