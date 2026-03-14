use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCountySeries {
    pub plan_key: u32,
    pub county_key: u32,
    pub start_month_key: u32, // yyyymm
    pub month_count: u16,
    pub presence_bitmap: Vec<u8>,
    pub enrollment_blob: Vec<u8>, // Compressed enrollment values
}
