use std::collections::HashMap;
use crate::model::{PlanDim, CountyDim, NormalizedRow, YearMonth};

pub struct KeyResolver {
    pub plans: HashMap<u32, PlanDim>, // Surrogate key -> PlanDim
    pub current_plans: HashMap<String, u32>, // Natural key (CID|PID) -> Current surrogate key
    pub counties: HashMap<String, CountyDim>, // Natural key (State|County) -> CountyDim
    pub next_plan_key: u32,
    pub next_county_key: u32,
}

impl KeyResolver {
    pub fn from_existing(plans: Vec<PlanDim>, counties: Vec<CountyDim>) -> Self {
        let max_plan_key = plans.iter().map(|p| p.plan_key).max().unwrap_or(0);
        let max_county_key = counties.iter().map(|c| c.county_key).max().unwrap_or(0);

        let mut plans_map = HashMap::new();
        let mut current_plans = HashMap::new();
        for p in plans {
            let key = p.plan_key;
            if p.is_current {
                current_plans.insert(format!("{}|{}", p.contract_id, p.plan_id), key);
            }
            plans_map.insert(key, p);
        }

        let counties_map = counties.into_iter().map(|c| (format!("{}|{}", c.state_code, c.county_name), c)).collect();

        Self {
            plans: plans_map,
            current_plans,
            counties: counties_map,
            next_plan_key: max_plan_key + 1,
            next_county_key: max_county_key + 1,
        }
    }

    pub fn resolve_plan(&mut self, row: &NormalizedRow, month: YearMonth) -> u32 {
        let natural_key = format!("{}|{}", row.contract_id, row.plan_id);
        let month_yyyymm = month.to_yyyymm();

        if let Some(&current_key) = self.current_plans.get(&natural_key) {
            let plan = self.plans.get_mut(&current_key).expect("Current plan must exist in map");
            // Check for material changes
            if plan.plan_name == row.plan_name && 
               plan.parent_org == row.parent_org && 
               plan.plan_type == row.plan_type &&
               plan.is_egwp == row.is_egwp &&
               plan.is_snp == row.is_snp {
                
                // Same metadata. Just ensure validity range covers this month.
                if month_yyyymm < plan.valid_from_month {
                    plan.valid_from_month = month_yyyymm;
                }
                if let Some(to) = plan.valid_to_month {
                    if month_yyyymm >= to {
                        plan.valid_to_month = None; // Extend to infinity again if we found it later
                    }
                }
                return plan.plan_key;
            }

            // Material change detected: version the record
            // If the month we are ingesting is LATER than the current version's start, 
            // the current version ends at this month, and we create a new one starting at this month.
            if month_yyyymm > plan.valid_from_month {
                log::info!("Plan metadata changed for {} at {}. Versioning.", natural_key, month_yyyymm);
                plan.is_current = false;
                plan.valid_to_month = Some(month_yyyymm);
            } else {
                // If the month we are ingesting is EARLIER than current version's start,
                // we should probably create a version for this earlier period.
                // For MVP, we'll just log it. This happens if months are ingested out of order.
                log::warn!("Plan metadata changed for {} at {} (EARLIER than current version {}).", natural_key, month_yyyymm, plan.valid_from_month);
                // We'll still create a new version, but it won't be "current" if there's already a later one
                // Actually, let's keep it simple: new version starts at month_yyyymm and ends at plan.valid_from_month
                let new_key = self.next_plan_key;
                self.next_plan_key += 1;
                let new_plan = PlanDim {
                    plan_key: new_key,
                    contract_id: row.contract_id.clone(),
                    plan_id: row.plan_id.clone(),
                    plan_name: row.plan_name.clone(),
                    parent_org: row.parent_org.clone(),
                    plan_type: row.plan_type.clone(),
                    is_egwp: row.is_egwp,
                    is_snp: row.is_snp,
                    valid_from_month: month_yyyymm,
                    valid_to_month: Some(plan.valid_from_month),
                    is_current: false,
                };
                self.plans.insert(new_key, new_plan);
                return new_key;
            }
        }

        // Create new version
        let plan_key = self.next_plan_key;
        self.next_plan_key += 1;

        let new_plan = PlanDim {
            plan_key,
            contract_id: row.contract_id.clone(),
            plan_id: row.plan_id.clone(),
            plan_name: row.plan_name.clone(),
            parent_org: row.parent_org.clone(),
            plan_type: row.plan_type.clone(),
            is_egwp: row.is_egwp,
            is_snp: row.is_snp,
            valid_from_month: month_yyyymm,
            valid_to_month: None,
            is_current: true,
        };
        
        self.plans.insert(plan_key, new_plan);
        self.current_plans.insert(natural_key, plan_key);
        plan_key
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
