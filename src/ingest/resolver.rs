use anyhow::Result;
use std::collections::HashMap;
use crate::model::{PlanDim, CountyDim, NormalizedRow, YearMonth};

pub struct KeyResolver {
    pub plans: HashMap<String, PlanDim>, // Natural key (CID|PID) -> PlanDim
    pub counties: HashMap<String, CountyDim>, // Natural key (State|County) -> CountyDim
    pub next_plan_key: u32,
    pub next_county_key: u32,
}

impl KeyResolver {
    pub fn new() -> Self {
        Self {
            plans: HashMap::new(),
            counties: HashMap::new(),
            next_plan_key: 1,
            next_county_key: 1,
        }
    }

    pub fn from_existing(plans: Vec<PlanDim>, counties: Vec<CountyDim>) -> Self {
        let max_plan_key = plans.iter().map(|p| p.plan_key).max().unwrap_or(0);
        let max_county_key = counties.iter().map(|c| c.county_key).max().unwrap_or(0);

        let plans_map = plans.into_iter().map(|p| (format!("{}|{}", p.contract_id, p.plan_id), p)).collect();
        let counties_map = counties.into_iter().map(|c| (format!("{}|{}", c.state_code, c.county_name), c)).collect();

        Self {
            plans: plans_map,
            counties: counties_map,
            next_plan_key: max_plan_key + 1,
            next_county_key: max_county_key + 1,
        }
    }

    pub fn resolve_plan(&mut self, row: &NormalizedRow, month: YearMonth) -> u32 {
        let natural_key = format!("{}|{}", row.contract_id, row.plan_id);
        let month_yyyymm = month.to_yyyymm();

        if let Some(plan) = self.plans.get_mut(&natural_key) {
            // Check if metadata changed (plan_name)
            if plan.plan_name != row.plan_name {
                log::info!("Plan metadata changed for {}: {} -> {}", natural_key, plan.plan_name, row.plan_name);
                // In a full implementation, we'd version the dimension record here.
                // For the MVP, we'll just update the name if it's the current record.
                plan.plan_name = row.plan_name.clone();
            }
            plan.plan_key
        } else {
            let plan_key = self.next_plan_key;
            self.next_plan_key += 1;

            let new_plan = PlanDim {
                plan_key,
                contract_id: row.contract_id.clone(),
                plan_id: row.plan_id.clone(),
                plan_name: row.plan_name.clone(),
                valid_from_month: month_yyyymm,
                valid_to_month: None,
                is_current: true,
            };
            self.plans.insert(natural_key, new_plan);
            plan_key
        }
    }

    pub fn resolve_county(&mut self, row: &NormalizedRow) -> u32 {
        let natural_key = format!("{}|{}", row.state_code, row.county_name);

        if let Some(county) = self.counties.get(&natural_key) {
            county.county_key
        } else {
            let county_key = self.next_county_key;
            self.next_county_key += 1;

            let new_county = CountyDim {
                county_key,
                state_code: row.state_code.clone(),
                county_name: row.county_name.clone(),
            };
            self.counties.insert(natural_key, new_county);
            county_key
        }
    }
}
