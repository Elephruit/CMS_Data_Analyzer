use anyhow::Result;
use std::collections::{HashMap, HashSet, BTreeSet};
use std::path::Path;
use crate::model::{PlanDim, YearMonth, PlanCountySeries, CountyDim};
use crate::storage;

pub struct QueryEngine {
    pub store_dir: std::path::PathBuf,
    pub cache_enabled: bool,
    pub plan_lookup: Option<HashMap<u32, PlanDim>>,
    pub county_lookup: Option<HashMap<u32, CountyDim>>, // Optimized: surrogate key -> dim
    pub series_cache: Option<HashMap<(u32, u32), PlanCountySeries>>,
    
    // Additional indexes for fast filtering
    pub state_to_county_keys: HashMap<String, HashSet<u32>>,
    pub latest_yyyymm: u32,
    pub prior_yyyymm: u32,
}

impl QueryEngine {
    pub fn new(store_dir: &Path) -> Self {
        let cache_dir = store_dir.join("cache");
        let mut plan_lookup = None;
        let mut county_lookup_raw: Option<HashMap<String, CountyDim>> = None;
        let mut series_cache = None;
        let mut cache_enabled = false;

        if cache_dir.exists() {
            plan_lookup = storage::binary_cache::load_plan_lookup(&cache_dir.join("plan_lookup.bin")).ok();
            county_lookup_raw = storage::binary_cache::load_county_lookup(&cache_dir.join("county_lookup.bin")).ok();
            series_cache = storage::binary_cache::load_series_cache(&cache_dir.join("series_values.bin")).ok();
            
            if plan_lookup.is_some() && county_lookup_raw.is_some() && series_cache.is_some() {
                cache_enabled = true;
                log::info!("QueryEngine: Binary cache loaded and enabled.");
            }
        }

        let mut county_lookup = None;
        let mut state_to_county_keys = HashMap::new();
        let mut latest_yyyymm = 0;
        let mut prior_yyyymm = 0;

        if let Some(raw) = county_lookup_raw {
            let mut optimized = HashMap::new();
            for c in raw.into_values() {
                state_to_county_keys.entry(c.state_code.clone()).or_insert_with(HashSet::new).insert(c.county_key);
                optimized.insert(c.county_key, c);
            }
            county_lookup = Some(optimized);
        }

        if let Some(cache) = &series_cache {
            let mut all_months = BTreeSet::new();
            for series in cache.values() {
                let start_year = (series.start_month_key / 100) as i32;
                let start_month = (series.start_month_key % 100) as i32;
                for i in 0..64 {
                    if (series.presence_bitmap >> i) & 1 != 0 {
                        all_months.insert((start_year as u32 * 100) + (start_month as u32 + i as u32 - 1));
                    }
                }
            }
            let months_vec: Vec<_> = all_months.into_iter().collect();
            latest_yyyymm = *months_vec.last().unwrap_or(&0);
            prior_yyyymm = if months_vec.len() >= 2 { months_vec[months_vec.len() - 2] } else { 0 };
        }

        Self {
            store_dir: store_dir.to_path_buf(),
            cache_enabled,
            plan_lookup,
            county_lookup,
            series_cache,
            state_to_county_keys,
            latest_yyyymm,
            prior_yyyymm,
        }
    }

    pub fn get_plan_key(&self, contract_id: &str, plan_id: &str) -> Result<Option<u32>> {
        if let Some(lookup) = &self.plan_lookup {
            for plan in lookup.values() {
                if plan.contract_id == contract_id && plan.plan_id == plan_id && plan.is_current {
                    return Ok(Some(plan.plan_key));
                }
            }
        }
        Ok(None)
    }

    pub fn get_plan_trend(&self, plan_key: u32) -> Result<Vec<(u32, u32)>> {
        let mut monthly_totals = HashMap::new();
        if let Some(cache) = &self.series_cache {
            for series in cache.values() {
                if series.plan_key == plan_key {
                    self.extract_series_trend(series, &mut monthly_totals);
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
                
                if let Some(&enrollment) = series.enrollments.get(pos) {
                    *monthly_totals.entry(yyyymm).or_insert(0) += enrollment;
                }
                pos += 1;
            }
        }
    }

    pub fn get_county_key(&self, state_code: &str, county_name: &str) -> Result<Option<u32>> {
        if let Some(lookup) = &self.county_lookup {
            for county in lookup.values() {
                if county.state_code.to_lowercase() == state_code.to_lowercase() 
                   && county.county_name.to_lowercase() == county_name.to_lowercase() {
                    return Ok(Some(county.county_key));
                }
            }
        }
        Ok(None)
    }

    pub fn get_county_snapshot(&self, county_key: u32, month: YearMonth) -> Result<Vec<(String, String, String, u32)>> {
        let yyyymm = month.to_yyyymm();
        let mut snapshot_raw = Vec::new();

        if let Some(cache) = &self.series_cache {
            for series in cache.values() {
                if series.county_key == county_key {
                    if let Some(enrollment) = series.get_enrollment(yyyymm) {
                        snapshot_raw.push((series.plan_key, enrollment));
                    }
                }
            }
        }

        self.resolve_snapshot_metadata(snapshot_raw)
    }

    fn resolve_snapshot_metadata(&self, snapshot_raw: Vec<(u32, u32)>) -> Result<Vec<(String, String, String, u32)>> {
        let mut result = Vec::new();
        if let Some(plan_map) = &self.plan_lookup {
            for (plan_key, enrollment) in snapshot_raw {
                if let Some(plan) = plan_map.get(&plan_key) {
                    result.push((plan.contract_id.clone(), plan.plan_id.clone(), plan.plan_name.clone(), enrollment));
                }
            }
        }
        result.sort_by_key(|(_, _, _, e)| std::cmp::Reverse(*e));
        Ok(result)
    }

    pub fn get_filter_options(&self, current_filters: &serde_json::Value) -> Result<serde_json::Value> {
        let sel_states: HashSet<String> = current_filters["states"].as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        let sel_orgs: HashSet<String> = current_filters["parentOrgs"].as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        let sel_contracts: HashSet<String> = current_filters["contracts"].as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        
        let mut states: HashMap<String, u32> = HashMap::new();
        let mut counties: HashMap<String, u32> = HashMap::new();
        let mut parent_orgs: HashMap<String, u32> = HashMap::new();
        let mut contracts: HashMap<String, u32> = HashMap::new();
        let mut plans: HashMap<String, u32> = HashMap::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = 
           (&self.plan_lookup, &self.county_lookup, &self.series_cache) {

            for series in series_cache.values() {
                let county = county_lookup.get(&series.county_key);
                let plan = plan_lookup.get(&series.plan_key);

                if let (Some(c), Some(p)) = (county, plan) {
                    let org = p.plan_name.split_whitespace().next().unwrap_or("Other").to_string();
                    
                    let state_match = sel_states.is_empty() || sel_states.contains(&c.state_code);
                    let org_match = sel_orgs.is_empty() || sel_orgs.contains(&org);
                    let contract_match = sel_contracts.is_empty() || sel_contracts.contains(&p.contract_id);
                    
                    // Cross-filtering rules:
                    // Option counts should reflect what's available given other selections
                    
                    // States: count if org and contract match
                    if org_match && contract_match {
                        *states.entry(c.state_code.clone()).or_insert(0) += 1;
                    }

                    // Counties: count if state, org, and contract match
                    if state_match && org_match && contract_match {
                        *counties.entry(c.county_name.clone()).or_insert(0) += 1;
                    }

                    // Orgs: count if state and contract match
                    if state_match && contract_match {
                        *parent_orgs.entry(org.clone()).or_insert(0) += 1;
                    }

                    // Contracts: count if state and org match
                    if state_match && org_match {
                        *contracts.entry(p.contract_id.clone()).or_insert(0) += 1;
                    }

                    // Plans: count if state, org, and contract match
                    if state_match && org_match && contract_match {
                        *plans.entry(format!("{} - {}", p.plan_id, p.plan_name)).or_insert(0) += 1;
                    }
                }
            }
        }

        let format_options = |map: HashMap<String, u32>| {
            let mut opts: Vec<serde_json::Value> = map.into_iter()
                .map(|(k, v)| serde_json::json!({ "label": k, "value": k, "count": v }))
                .collect();
            opts.sort_by_key(|o| o["label"].as_str().unwrap_or("").to_string());
            opts
        };

        Ok(serde_json::json!({
            "states": format_options(states),
            "counties": format_options(counties),
            "parentOrgs": format_options(parent_orgs),
            "contracts": format_options(contracts),
            "plans": format_options(plans),
        }))
    }

    pub fn get_state_rollup(&self, state_code: &str, start_month: YearMonth, end_month: YearMonth) -> Result<Vec<(u32, u32)>> {
        let mut monthly_totals = HashMap::new();
        let start_yyyymm = start_month.to_yyyymm();
        let end_yyyymm = end_month.to_yyyymm();

        if let (Some(series_cache), Some(county_lookup)) = (&self.series_cache, &self.county_lookup) {
            let target_counties = self.state_to_county_keys.get(state_code);
            if let Some(target_keys) = target_counties {
                for series in series_cache.values() {
                    if target_keys.contains(&series.county_key) {
                        self.extract_series_range(series, start_yyyymm, end_yyyymm, &mut monthly_totals);
                    }
                }
            }
        }

        let mut result: Vec<_> = monthly_totals.into_iter().collect();
        result.sort_by_key(|(m, _)| *m);
        Ok(result)
    }

    fn extract_series_range(&self, series: &PlanCountySeries, start: u32, end: u32, totals: &mut HashMap<u32, u32>) {
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
                
                if yyyymm >= start && yyyymm <= end {
                    if let Some(&enrollment) = series.enrollments.get(pos) {
                        *totals.entry(yyyymm).or_insert(0) += enrollment;
                    }
                }
                pos += 1;
            }
        }
    }

    pub fn get_top_movers(&self, state: Option<String>, month_a: YearMonth, month_b: YearMonth, limit: usize) -> Result<Vec<(String, String, String, i32)>> {
        let yyyymm_a = month_a.to_yyyymm();
        let yyyymm_b = month_b.to_yyyymm();
        let mut plan_changes: HashMap<u32, i32> = HashMap::new();

        if let (Some(series_cache), Some(county_lookup), Some(plan_lookup)) = 
           (&self.series_cache, &self.county_lookup, &self.plan_lookup) {
            
            let target_county_keys = state.as_ref().and_then(|s| self.state_to_county_keys.get(s));

            for series in series_cache.values() {
                if let Some(keys) = target_county_keys {
                    if !keys.contains(&series.county_key) { continue; }
                }

                let val_a = series.get_enrollment(yyyymm_a).unwrap_or(0) as i32;
                let val_b = series.get_enrollment(yyyymm_b).unwrap_or(0) as i32;
                *plan_changes.entry(series.plan_key).or_insert(0) += val_b - val_a;
            }

            let mut movers = Vec::new();
            for (plan_key, change) in plan_changes {
                if change == 0 { continue; }
                if let Some(plan) = plan_lookup.get(&plan_key) {
                    movers.push((plan.contract_id.clone(), plan.plan_id.clone(), plan.plan_name.clone(), change));
                }
            }
            movers.sort_by_key(|(_, _, _, c)| std::cmp::Reverse(c.abs()));
            return Ok(movers.into_iter().take(limit).collect());
        }
        
        Err(anyhow::anyhow!("Top movers query requires binary cache."))
    }

    pub fn get_dashboard_summary(&self, current_filters: &serde_json::Value) -> Result<serde_json::Value> {
        let sel_states: HashSet<String> = current_filters["states"].as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        
        let mut total_enrollment: u64 = 0;
        let mut unique_plans = HashSet::new();
        let mut unique_counties = HashSet::new();
        let mut unique_orgs = HashSet::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = 
           (&self.plan_lookup, &self.county_lookup, &self.series_cache) {

            for series in series_cache.values() {
                let county = county_lookup.get(&series.county_key);
                if let Some(c) = county {
                    if sel_states.is_empty() || sel_states.contains(&c.state_code) {
                        if let Some(&latest) = series.enrollments.last() {
                            total_enrollment += latest as u64;
                        }
                        unique_plans.insert(series.plan_key);
                        unique_counties.insert(series.county_key);
                        if let Some(p) = plan_lookup.get(&series.plan_key) {
                            unique_orgs.insert(p.plan_name.split_whitespace().next().unwrap_or("Other").to_string());
                        }
                    }
                }
            }
        }

        Ok(serde_json::json!({
            "totalEnrollment": total_enrollment,
            "planCount": unique_plans.len(),
            "countyCount": unique_counties.len(),
            "orgCount": unique_orgs.len(),
        }))
    }

    pub fn get_global_trend(&self, current_filters: &serde_json::Value) -> Result<Vec<(u32, u64)>> {
        let sel_states: HashSet<String> = current_filters["states"].as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        
        let mut monthly_totals: HashMap<u32, u64> = HashMap::new();

        if let (Some(county_lookup), Some(series_cache)) = (&self.county_lookup, &self.series_cache) {
            for series in series_cache.values() {
                let county = county_lookup.get(&series.county_key);
                if let Some(c) = county {
                    if sel_states.is_empty() || sel_states.contains(&c.state_code) {
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
                                
                                if let Some(&enrollment) = series.enrollments.get(pos) {
                                    *monthly_totals.entry(yyyymm).or_insert(0) += enrollment as u64;
                                }
                                pos += 1;
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

    pub fn get_explorer_data(&self, payload: &serde_json::Value) -> Result<serde_json::Value> {
        let grain = payload["grain"].as_str().unwrap_or("parentOrg");
        let filters = &payload["filters"];
        let sel_states: HashSet<String> = filters["states"].as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();

        let mut aggregates: HashMap<String, (u64, u64)> = HashMap::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = 
           (&self.plan_lookup, &self.county_lookup, &self.series_cache) {

            for series in series_cache.values() {
                let county = county_lookup.get(&series.county_key);
                if let Some(c) = county {
                    if sel_states.is_empty() || sel_states.contains(&c.state_code) {
                        let agg_key = match grain {
                            "parentOrg" => {
                                plan_lookup.get(&series.plan_key)
                                    .map(|p| p.plan_name.split_whitespace().next().unwrap_or("Other").to_string())
                                    .unwrap_or_else(|| "Unknown".to_string())
                            },
                            "contract" => {
                                plan_lookup.get(&series.plan_key)
                                    .map(|p| p.contract_id.clone()).unwrap_or_else(|| "Unknown".to_string())
                            },
                            "plan" => {
                                plan_lookup.get(&series.plan_key)
                                    .map(|p| format!("{}|{}", p.contract_id, p.plan_id)).unwrap_or_else(|| "Unknown".to_string())
                            },
                            "county" => format!("{}|{}", c.state_code, c.county_name),
                            _ => "Unknown".to_string(),
                        };

                        let latest_val = series.get_enrollment(self.latest_yyyymm).unwrap_or(0);
                        let prior_val = series.get_enrollment(self.prior_yyyymm).unwrap_or(0);
                        
                        let entry = aggregates.entry(agg_key).or_insert((0, 0));
                        entry.0 += latest_val as u64;
                        entry.1 += prior_val as u64;
                    }
                }
            }
        }

        let mut rows = Vec::new();
        for (name, (latest, prior)) in aggregates {
            let change = latest as i64 - prior as i64;
            let pct_change = if prior > 0 { (change as f64 / prior as f64) * 100.0 } else { 0.0 };
            rows.push(serde_json::json!({
                "name": name, "current": latest, "prior": prior, "change": change, "percentChange": pct_change,
            }));
        }
        rows.sort_by_key(|r| std::cmp::Reverse(r["current"].as_u64().unwrap_or(0)));

        Ok(serde_json::json!({
            "grain": grain,
            "latestMonth": self.latest_yyyymm,
            "priorMonth": self.prior_yyyymm,
            "rows": rows
        }))
    }

    pub fn get_org_analysis(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let sel_states: HashSet<String> = filters["states"].as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();

        let mut org_data: HashMap<String, (u64, HashMap<u32, u64>)> = HashMap::new(); 
        let mut total_market_enrollment: u64 = 0;

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = 
           (&self.plan_lookup, &self.county_lookup, &self.series_cache) {

            for series in series_cache.values() {
                let county = county_lookup.get(&series.county_key);
                if let Some(c) = county {
                    if sel_states.is_empty() || sel_states.contains(&c.state_code) {
                        if let Some(p) = plan_lookup.get(&series.plan_key) {
                            let org = p.plan_name.split_whitespace().next().unwrap_or("Other").to_string();
                            let latest_val = series.get_enrollment(self.latest_yyyymm).unwrap_or(0) as u64;
                            
                            let entry = org_data.entry(org).or_insert((0, HashMap::new()));
                            entry.0 += latest_val;
                            total_market_enrollment += latest_val;

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
                                    if let Some(&enrollment) = series.enrollments.get(pos) {
                                        *entry.1.entry(yyyymm).or_insert(0) += enrollment as u64;
                                    }
                                    pos += 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut orgs_list = Vec::new();
        for (name, (latest, trend_map)) in org_data {
            let share = if total_market_enrollment > 0 { (latest as f64 / total_market_enrollment as f64) * 100.0 } else { 0.0 };
            let mut trend: Vec<_> = trend_map.into_iter().collect();
            trend.sort_by_key(|(m, _)| *m);
            orgs_list.push(serde_json::json!({
                "name": name, "enrollment": latest, "marketShare": share,
                "trend": trend.into_iter().map(|(m, v)| serde_json::json!({ "month": m, "value": v })).collect::<Vec<_>>()
            }));
        }
        orgs_list.sort_by_key(|o| std::cmp::Reverse(o["enrollment"].as_u64().unwrap_or(0)));

        Ok(serde_json::json!({
            "totalMarketEnrollment": total_market_enrollment,
            "latestMonth": self.latest_yyyymm,
            "organizations": orgs_list
        }))
    }

    pub fn get_geo_analysis(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let sel_states: HashSet<String> = filters["states"].as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();

        let mut state_data: HashMap<String, u64> = HashMap::new();
        let mut county_data: HashMap<String, u64> = HashMap::new(); 

        if let (Some(county_lookup), Some(series_cache)) = (&self.county_lookup, &self.series_cache) {
            for series in series_cache.values() {
                let county = county_lookup.get(&series.county_key);
                if let Some(c) = county {
                    let val = series.get_enrollment(self.latest_yyyymm).unwrap_or(0) as u64;
                    *state_data.entry(c.state_code.clone()).or_insert(0) += val;
                    if sel_states.is_empty() || sel_states.contains(&c.state_code) {
                        let county_key = format!("{}|{}", c.state_code, c.county_name);
                        *county_data.entry(county_key).or_insert(0) += val;
                    }
                }
            }
        }

        let mut states_list: Vec<_> = state_data.into_iter().map(|(name, enrollment)| serde_json::json!({ "name": name, "enrollment": enrollment })).collect();
        states_list.sort_by_key(|s| std::cmp::Reverse(s["enrollment"].as_u64().unwrap_or(0)));

        let mut counties_list: Vec<_> = county_data.into_iter().map(|(key, enrollment)| {
            let parts: Vec<&str> = key.split('|').collect();
            serde_json::json!({ "state": parts[0], "name": parts[1], "enrollment": enrollment })
        }).collect();
        counties_list.sort_by_key(|c| std::cmp::Reverse(c["enrollment"].as_u64().unwrap_or(0)));

        Ok(serde_json::json!({
            "latestMonth": self.latest_yyyymm,
            "states": states_list,
            "counties": counties_list.into_iter().take(50).collect::<Vec<_>>()
        }))
    }

    pub fn get_growth_analytics(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let sel_states: HashSet<String> = filters["states"].as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();

        let mut total_growth: i64 = 0;
        let mut aep_growth: i64 = 0;
        let mut high_flyers = Vec::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = 
           (&self.plan_lookup, &self.county_lookup, &self.series_cache) {

            // Define AEP months for 2025 (Feb 2025 vs Dec 2024)
            // In a more generic app, we'd detect year boundaries
            let aep_target = 202502;
            let aep_base = 202412;

            let mut plan_aggregates: HashMap<u32, (u64, u64, u64, u64)> = HashMap::new(); // Latest, Prior, AEP_Target, AEP_Base

            for series in series_cache.values() {
                let county = county_lookup.get(&series.county_key);
                if let Some(c) = county {
                    if sel_states.is_empty() || sel_states.contains(&c.state_code) {
                        let latest_val = series.get_enrollment(self.latest_yyyymm).unwrap_or(0);
                        let prior_val = series.get_enrollment(self.prior_yyyymm).unwrap_or(0);
                        let aep_t_val = series.get_enrollment(aep_target).unwrap_or(0);
                        let aep_b_val = series.get_enrollment(aep_base).unwrap_or(0);

                        let entry = plan_aggregates.entry(series.plan_key).or_insert((0, 0, 0, 0));
                        entry.0 += latest_val as u64;
                        entry.1 += prior_val as u64;
                        entry.2 += aep_t_val as u64;
                        entry.3 += aep_b_val as u64;
                        
                        total_growth += (latest_val as i64) - (prior_val as i64);
                        aep_growth += (aep_t_val as i64) - (aep_b_val as i64);
                    }
                }
            }

            for (plan_key, (latest, prior, aep_t, aep_b)) in plan_aggregates {
                if latest > 500 { 
                    let change = latest as i64 - prior as i64;
                    let pct = if prior > 0 { (change as f64 / prior as f64) * 100.0 } else { 0.0 };
                    
                    let aep_change = aep_t as i64 - aep_b as i64;
                    
                    if pct > 5.0 || change > 1000 || aep_change.abs() > 1000 {
                        if let Some(p) = plan_lookup.get(&plan_key) {
                            high_flyers.push(serde_json::json!({
                                "name": p.plan_name, "contract": p.contract_id, "plan": p.plan_id,
                                "current": latest, "change": change, "percent": pct,
                                "aepChange": aep_change
                            }));
                        }
                    }
                }
            }
        }

        high_flyers.sort_by_key(|h| std::cmp::Reverse((h["percent"].as_f64().unwrap_or(0.0) * 100.0) as i64));

        Ok(serde_json::json!({
            "latestMonth": self.latest_yyyymm,
            "priorMonth": self.prior_yyyymm,
            "totalGrowth": total_growth,
            "aepGrowth": aep_growth,
            "highFlyers": high_flyers.into_iter().take(20).collect::<Vec<_>>()
        }))
    }

    pub fn get_plan_details(&self, contract_id: &str, plan_id: &str) -> Result<serde_json::Value> {
        if !self.cache_enabled {
            return Err(anyhow::anyhow!("Plan details require binary cache."));
        }

        let plan_lookup = self.plan_lookup.as_ref().unwrap();
        let county_lookup = self.county_lookup.as_ref().unwrap();
        let series_cache = self.series_cache.as_ref().unwrap();

        let plan = plan_lookup.values().find(|p| p.contract_id == contract_id && p.plan_id == plan_id && p.is_current)
            .ok_or_else(|| anyhow::anyhow!("Plan not found"))?;

        let mut footprint = Vec::new();
        let mut global_trend: HashMap<u32, u64> = HashMap::new();

        for series in series_cache.values() {
            if series.plan_key == plan.plan_key {
                let county = county_lookup.get(&series.county_key);
                if let Some(c) = county {
                    let latest_val = series.enrollments.last().cloned().unwrap_or(0);
                    footprint.push(serde_json::json!({
                        "state": c.state_code, "county": c.county_name, "enrollment": latest_val
                    }));

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
                            if let Some(&val) = series.enrollments.get(pos) {
                                *global_trend.entry(yyyymm).or_insert(0) += val as u64;
                            }
                            pos += 1;
                        }
                    }
                }
            }
        }

        footprint.sort_by_key(|f| std::cmp::Reverse(f["enrollment"].as_u64().unwrap_or(0)));
        let mut trend_list: Vec<_> = global_trend.into_iter().collect();
        trend_list.sort_by_key(|(m, _)| *m);

        Ok(serde_json::json!({
            "metadata": {
                "name": plan.plan_name, "contract_id": plan.contract_id, "plan_id": plan.plan_id,
                "org": plan.plan_name.split_whitespace().next().unwrap_or("Other")
            },
            "footprint": footprint,
            "trend": trend_list.into_iter().map(|(m, v)| serde_json::json!({ "month": m, "value": v })).collect::<Vec<_>>()
        }))
    }
}
