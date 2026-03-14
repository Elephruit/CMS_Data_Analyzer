use anyhow::Result;
use arrow::array::{UInt32Array, AsArray};
use arrow::datatypes::UInt32Type;
use std::fs::File;
use std::path::Path;
use crate::model::{PlanDim, YearMonth, PlanCountySeries};
use crate::storage;

pub struct QueryEngine {
    pub store_dir: std::path::PathBuf,
}

impl QueryEngine {
    pub fn new(store_dir: &Path) -> Self {
        Self {
            store_dir: store_dir.to_path_buf(),
        }
    }

    pub fn get_plan_key(&self, contract_id: &str, plan_id: &str) -> Result<Option<u32>> {
        let path = self.store_dir.join("dims").join("plan_dim.parquet");
        let plans = storage::parquet_store::load_plan_dim(&path)?;
        for plan in plans {
            if plan.contract_id == contract_id && plan.plan_id == plan_id && plan.is_current {
                return Ok(Some(plan.plan_key));
            }
        }
        Ok(None)
    }

    pub fn get_plan_trend(&self, plan_key: u32) -> Result<Vec<(u32, u32)>> {
        let facts_dir = self.store_dir.join("facts");
        let mut monthly_totals = std::collections::HashMap::new();

        if !facts_dir.exists() { return Ok(Vec::new()); }

        for year_entry in std::fs::read_dir(facts_dir)? {
            let year_path = year_entry?.path();
            if year_path.is_dir() {
                for state_entry in std::fs::read_dir(year_path)? {
                    let state_path = state_entry?.path();
                    if state_path.is_dir() {
                        let series_path = state_path.join("plan_county_series.parquet");
                        let series_list = storage::parquet_store::load_series_partition(&series_path)?;
                        
                        for series in series_list {
                            if series.plan_key == plan_key {
                                // Extract all months from this series
                                // We need to iterate over the bitmap
                                let mut bitmap = series.presence_bitmap;
                                let mut pos = 0;
                                let start_year = (series.start_month_key / 100) as i32;
                                let start_month = (series.start_month_key % 100) as i32;

                                for i in 0..64 {
                                    if (bitmap >> i) & 1 != 0 {
                                        let curr_month_total = (start_month - 1 + i as i32);
                                        let year = start_year + curr_month_total / 12;
                                        let month = (curr_month_total % 12) + 1;
                                        let yyyymm = (year as u32) * 100 + (month as u32);
                                        
                                        let enrollment = series.enrollments[pos];
                                        *monthly_totals.entry(yyyymm).or_insert(0) += enrollment;
                                        pos += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut result: Vec<_> = monthly_totals.into_iter().collect();
        result.sort_by_key(|(m, _)| *m);
        Ok(result)
    }

    pub fn get_county_key(&self, state_code: &str, county_name: &str) -> Result<Option<u32>> {
        let path = self.store_dir.join("dims").join("county_dim.parquet");
        let counties = storage::parquet_store::load_county_dim(&path)?;
        for county in counties {
            if county.state_code.to_lowercase() == state_code.to_lowercase() 
               && county.county_name.to_lowercase() == county_name.to_lowercase() {
                return Ok(Some(county.county_key));
            }
        }
        Ok(None)
    }

    pub fn get_county_snapshot(&self, county_key: u32, month: YearMonth) -> Result<Vec<(String, String, String, u32)>> {
        let yyyymm = month.to_yyyymm();
        let year_dir = self.store_dir.join("facts").join(format!("year={}", month.year));
        let mut snapshot = Vec::new();

        if !year_dir.exists() { return Ok(Vec::new()); }

        for state_entry in std::fs::read_dir(year_dir)? {
            let state_path = state_entry?.path();
            if state_path.is_dir() {
                let series_path = state_path.join("plan_county_series.parquet");
                let series_list = storage::parquet_store::load_series_partition(&series_path)?;
                
                for series in series_list {
                    if series.county_key == county_key {
                        if let Some(enrollment) = series.get_enrollment(yyyymm) {
                            snapshot.push((series.plan_key, enrollment));
                        }
                    }
                }
            }
        }

        let plan_dim_path = self.store_dir.join("dims").join("plan_dim.parquet");
        let all_plans = storage::parquet_store::load_plan_dim(&plan_dim_path)?;
        let plan_map: std::collections::HashMap<u32, &PlanDim> = all_plans.iter().map(|p| (p.plan_key, p)).collect();

        let mut result = Vec::new();
        for (plan_key, enrollment) in snapshot {
            if let Some(plan) = plan_map.get(&plan_key) {
                result.push((plan.contract_id.clone(), plan.plan_id.clone(), plan.plan_name.clone(), enrollment));
            }
        }

        result.sort_by_key(|(_, _, _, e)| std::cmp::Reverse(*e));
        Ok(result)
    }
}
