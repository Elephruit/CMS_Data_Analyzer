use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountyDim {
    pub county_key: u32,
    pub state_code: String,
    pub county_name: String,
}

impl CountyDim {
}
