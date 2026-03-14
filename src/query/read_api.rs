use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use crate::model::{PlanDim, YearMonth, PlanCountySeries, CountyDim};
use crate::storage;

pub struct QueryEngine {
    pub store_dir: std::path::PathBuf,
    pub cache_enabled: bool,
    pub plan_lookup: Option<HashMap<u32, PlanDim>>,
    pub county_lookup: Option<HashMap<String, CountyDim>>,
    pub series_cache: Option<HashMap<(u32, u32), PlanCountySeries>>,
}

impl QueryEngine {
    pub fn new(store_dir: &Path) -> Self {
        let cache_dir = store_dir.join("cache");
        let mut plan_lookup = None;
        let mut county_lookup = None;
        let mut series_cache = None;
        let mut cache_enabled = false;

        if cache_dir.exists() {
            if let Ok(pl) = storage::binary_cache::load_plan_lookup(&cache_dir.join("plan_lookup.bin")) {
                plan_lookup = Some(pl);
            }
            if let Ok(cl) = storage::binary_cache::load_county_lookup(&cache_dir.join("county_lookup.bin")) {
                county_lookup = Some(cl);
            }
            if let Ok(sc) = storage::binary_cache::load_series_cache(&cache_dir.join("series_values.bin")) {
                series_cache = Some(sc);
            }
            if plan_lookup.is_some() && county_lookup.is_some() && series_cache.is_some() {
                cache_enabled = true;
                log::info!("QueryEngine: Binary cache loaded and enabled.");
            }
        }

        Self {
            store_dir: store_dir.to_path_buf(),
            cache_enabled,
            plan_lookup,
            county_lookup,
            series_cache,
        }
    }

    pub fn get_plan_key(&self, contract_id: &str, plan_id: &str) -> Result<Option<u32>> {
        if self.cache_enabled {
            if let Some(lookup) = &self.plan_lookup {
                for plan in lookup.values() {
                    if plan.contract_id == contract_id && plan.plan_id == plan_id && plan.is_current {
                        return Ok(Some(plan.plan_key));
                    }
                }
            }
        }

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
        let mut monthly_totals = HashMap::new();

        if self.cache_enabled {
            if let Some(cache) = &self.series_cache {
                for series in cache.values() {
                    if series.plan_key == plan_key {
                        self.extract_series_trend(series, &mut monthly_totals);
                    }
                }
                let mut result: Vec<_> = monthly_totals.into_iter().collect();
                result.sort_by_key(|(m, _)| *m);
                return Ok(result);
            }
        }

        let facts_dir = self.store_dir.join("facts");
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
                                self.extract_series_trend(&series, &mut monthly_totals);
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

    fn extract_series_trend(&self, series: &PlanCountySeries, monthly_totals: &mut HashMap<u32, u32>) {
        let bitmap = series.presence_bitmap;
        let mut pos = 0;
        let start_year = (series.start_month_key / 100) as i32;
        let start_month = (series.start_month_key % 100) as i32;

        for i in 0..64 {
            if (bitmap >> i) & 1 != 0 {
                let curr_month_total = start_month - 1 + i as i32;
                let year = start_year + curr_month_total / 12;
                let month = (curr_month_total % 12) + 1;
                let yyyymm = (year as u32) * 100 + (month as u32);
                
                let enrollment = series.enrollments[pos];
                *monthly_totals.entry(yyyymm).or_insert(0) += enrollment;
                pos += 1;
            }
        }
    }

    pub fn get_county_key(&self, state_code: &str, county_name: &str) -> Result<Option<u32>> {
        if self.cache_enabled {
            if let Some(lookup) = &self.county_lookup {
                let key = format!("{}|{}", state_code.to_uppercase(), county_name.to_lowercase()); // This naming convention should match during cache build
                // Wait, my cache build uses "{}|{}". Let's just iterate to be safe for MVP.
                for county in lookup.values() {
                    if county.state_code.to_lowercase() == state_code.to_lowercase() 
                       && county.county_name.to_lowercase() == county_name.to_lowercase() {
                        return Ok(Some(county.county_key));
                    }
                }
            }
        }

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
        let mut snapshot_raw = Vec::new();

        if self.cache_enabled {
            if let Some(cache) = &self.series_cache {
                for series in cache.values() {
                    if series.county_key == county_key {
                        if let Some(enrollment) = series.get_enrollment(yyyymm) {
                            snapshot_raw.push((series.plan_key, enrollment));
                        }
                    }
                }
                return self.resolve_snapshot_metadata(snapshot_raw);
            }
        }

        let year_dir = self.store_dir.join("facts").join(format!("year={}", month.year));
        if !year_dir.exists() { return Ok(Vec::new()); }

        for state_entry in std::fs::read_dir(year_dir)? {
            let state_path = state_entry?.path();
            if state_path.is_dir() {
                let series_path = state_path.join("plan_county_series.parquet");
                let series_list = storage::parquet_store::load_series_partition(&series_path)?;
                for series in series_list {
                    if series.county_key == county_key {
                        if let Some(enrollment) = series.get_enrollment(yyyymm) {
                            snapshot_raw.push((series.plan_key, enrollment));
                        }
                    }
                }
            }
        }

        self.resolve_snapshot_metadata(snapshot_raw)
    }

    fn resolve_snapshot_metadata(&self, snapshot_raw: Vec<(u32, u32)>) -> Result<Vec<(String, String, String, u32)>> {
        let mut result = Vec::new();
        
        if self.cache_enabled {
            if let Some(plan_map) = &self.plan_lookup {
                for (plan_key, enrollment) in snapshot_raw {
                    if let Some(plan) = plan_map.get(&plan_key) {
                        result.push((plan.contract_id.clone(), plan.plan_id.clone(), plan.plan_name.clone(), enrollment));
                    }
                }
                result.sort_by_key(|(_, _, _, e)| std::cmp::Reverse(*e));
                return Ok(result);
            }
        }

        let plan_dim_path = self.store_dir.join("dims").join("plan_dim.parquet");
        let all_plans = storage::parquet_store::load_plan_dim(&plan_dim_path)?;
        let plan_map: HashMap<u32, &PlanDim> = all_plans.iter().map(|p| (p.plan_key, p)).collect();

        for (plan_key, enrollment) in snapshot_raw {
            if let Some(plan) = plan_map.get(&plan_key) {
                result.push((plan.contract_id.clone(), plan.plan_id.clone(), plan.plan_name.clone(), enrollment));
            }
        }

        result.sort_by_key(|(_, _, _, e)| std::cmp::Reverse(*e));
        Ok(result)
    }
}
