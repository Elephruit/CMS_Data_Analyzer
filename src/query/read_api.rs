use anyhow::Result;
use std::collections::{HashMap, HashSet, BTreeSet};
use std::path::Path;
use crate::model::{PlanDim, YearMonth, PlanCountySeries, CountyDim};
use crate::storage;

pub struct QueryEngine {
    pub cache_enabled: bool,
    pub plan_lookup: Option<HashMap<u32, PlanDim>>,
    pub county_lookup: Option<HashMap<u32, CountyDim>>,
    pub series_cache: Option<HashMap<(u32, u32), PlanCountySeries>>,
    pub state_to_county_keys: HashMap<String, HashSet<u32>>,
    pub latest_yyyymm: u32,
    pub prior_yyyymm: u32,
    pub lineage_engine: crate::query::lineage::LineageEngine,
    /// Landscape-based footprints: (year, "contract_id-plan_id") → set of "STATE:county" compound keys.
    /// These come from the official CMS landscape files and are the ground-truth source for
    /// which counties a plan operates in each year.  The compound key format means a single
    /// map lookup gives both the state and county name needed for geo filtering and display.
    pub landscape_fp: HashMap<(u32, String), HashSet<String>>,
}

impl QueryEngine {
    pub fn new(store_dir: &Path) -> Self {
        let lineage_engine = crate::query::lineage::LineageEngine::new(store_dir);
        let cache_dir = store_dir.join("cache");
        let mut plan_lookup = None;
        let mut county_lookup_raw: Option<HashMap<String, CountyDim>> = None;
        let mut series_cache: Option<HashMap<(u32, u32), PlanCountySeries>> = None;
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
                        let curr_month_total = start_month - 1 + i as i32;
                        let year = start_year + curr_month_total / 12;
                        let month = (curr_month_total % 12) + 1;
                        let yyyymm = (year as u32) * 100 + (month as u32);
                        all_months.insert(yyyymm);
                    }
                }
            }
            let months_vec: Vec<_> = all_months.into_iter().collect();
            latest_yyyymm = *months_vec.last().unwrap_or(&0);
            prior_yyyymm = if months_vec.len() >= 2 { months_vec[months_vec.len() - 2] } else { 0 };
        }

        // Load landscape footprints from the ingested parquet files.
        // Each entry is a compound "STATE:county" key for unambiguous geo filtering.
        let mut landscape_fp: HashMap<(u32, String), HashSet<String>> = HashMap::new();
        for year in [2024u32, 2025, 2026] {
            let lp = store_dir.join("landscape").join("normalized").join(format!("year={}", year)).join("landscape.parquet");
            if lp.exists() {
                match storage::parquet_store::load_landscape_footprints(&lp) {
                    Ok(rows) => {
                        for r in rows {
                            let key = format!("{}-{}", r.contract_id, r.plan_id);
                            landscape_fp.entry((year, key)).or_default().insert(r.county_key);
                        }
                        log::info!("QueryEngine: Loaded landscape footprints for year {}", year);
                    }
                    Err(e) => log::warn!("QueryEngine: Failed to load landscape for year {}: {}", year, e),
                }
            }
        }

        Self {
            cache_enabled,
            plan_lookup,
            county_lookup,
            series_cache,
            state_to_county_keys,
            latest_yyyymm,
            prior_yyyymm,
            lineage_engine,
            landscape_fp,
        }
    }

    fn get_analysis_months(&self, filters: &serde_json::Value) -> (u32, u32) {
        if let Some(month_str) = filters["analysisMonth"].as_str() {
            if let Ok(ym) = month_str.parse::<crate::model::YearMonth>() {
                let current = ym.to_yyyymm();
                let (prior_year, prior_month) = if ym.month == 1 {
                    (ym.year - 1, 12)
                } else {
                    (ym.year, ym.month - 1)
                };
                let prior = (prior_year as u32 * 100) + prior_month as u32;
                return (current, prior);
            }
        }
        (self.latest_yyyymm, self.prior_yyyymm)
    }

    fn is_plan_valid_for_month(&self, plan: &PlanDim, yyyymm: u32) -> bool {
        yyyymm >= plan.valid_from_month && (plan.valid_to_month.is_none() || yyyymm < plan.valid_to_month.unwrap())
    }

    fn matches_plan_only_filters(&self, plan: &PlanDim, filters: &serde_json::Value, target_yyyymm: u32) -> bool {
        if !self.is_plan_valid_for_month(plan, target_yyyymm) { return false; }

        if let Some(sel_orgs) = filters["parentOrgs"].as_array() {
            if !sel_orgs.is_empty() {
                let orgs: HashSet<String> = sel_orgs.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                if !orgs.contains(&plan.parent_org) { return false; }
            }
        }
        if let Some(sel_contracts) = filters["contracts"].as_array() {
            if !sel_contracts.is_empty() {
                let contracts: HashSet<String> = sel_contracts.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                if !contracts.contains(&plan.contract_id) { return false; }
            }
        }
        if let Some(sel_types) = filters["planTypes"].as_array() {
            if !sel_types.is_empty() {
                let types: HashSet<String> = sel_types.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                if !types.contains(&plan.plan_type) { return false; }
            }
        }
        if let Some(eghp) = filters["eghp"].as_bool() {
            if plan.is_egwp != eghp { return false; }
        }
        if let Some(snp) = filters["snp"].as_bool() {
            if plan.is_snp != snp { return false; }
        }
        true
    }

    fn matches_filters(&self, series: &PlanCountySeries, filters: &serde_json::Value, target_yyyymm: u32, exclude_dim: Option<&str>) -> bool {
        let plan_lookup = match &self.plan_lookup { Some(l) => l, None => return false };
        let county_lookup = match &self.county_lookup { Some(l) => l, None => return false };

        let plan = match plan_lookup.get(&series.plan_key) { Some(p) => p, None => return false };
        let county = county_lookup.get(&series.county_key);

        if !self.is_plan_valid_for_month(plan, target_yyyymm) { return false; }

        if exclude_dim != Some("states") {
            if let Some(sel_states) = filters["states"].as_array() {
                if !sel_states.is_empty() {
                    let c = match county { Some(c) => c, None => return false };
                    let states: HashSet<String> = sel_states.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                    if !states.contains(&c.state_code) { return false; }
                }
            }
        }

        if exclude_dim != Some("counties") {
            if let Some(sel_counties) = filters["counties"].as_array() {
                if !sel_counties.is_empty() {
                    let c = match county { Some(c) => c, None => return false };
                    let counties: HashSet<String> = sel_counties.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                    if !counties.contains(&c.county_name) { return false; }
                }
            }
        }

        if exclude_dim != Some("parentOrgs") {
            if let Some(sel_orgs) = filters["parentOrgs"].as_array() {
                if !sel_orgs.is_empty() {
                    let orgs: HashSet<String> = sel_orgs.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                    if !orgs.contains(&plan.parent_org) { return false; }
                }
            }
        }

        if exclude_dim != Some("contracts") {
            if let Some(sel_contracts) = filters["contracts"].as_array() {
                if !sel_contracts.is_empty() {
                    let contracts: HashSet<String> = sel_contracts.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                    if !contracts.contains(&plan.contract_id) { return false; }
                }
            }
        }

        if exclude_dim != Some("planTypes") {
            if let Some(sel_types) = filters["planTypes"].as_array() {
                if !sel_types.is_empty() {
                    let types: HashSet<String> = sel_types.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                    if !types.contains(&plan.plan_type) { return false; }
                }
            }
        }

        if let Some(eghp) = filters["eghp"].as_bool() {
            if plan.is_egwp != eghp { return false; }
        }

        if let Some(snp) = filters["snp"].as_bool() {
            if plan.is_snp != snp { return false; }
        }

        true
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
        let (target_yyyymm, _) = self.get_analysis_months(current_filters);
        let mut states: HashMap<String, u32> = HashMap::new();
        let mut counties: HashMap<String, u32> = HashMap::new();
        let mut parent_orgs: HashMap<String, u32> = HashMap::new();
        let mut contracts: HashMap<String, u32> = HashMap::new();
        let mut plans: HashMap<String, u32> = HashMap::new();
        let mut plan_types: HashMap<String, u32> = HashMap::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = 
           (&self.plan_lookup, &self.county_lookup, &self.series_cache) {

            for series in series_cache.values() {
                let county = county_lookup.get(&series.county_key);
                let plan = plan_lookup.get(&series.plan_key);

                if let (Some(c), Some(p)) = (county, plan) {
                    if self.is_plan_valid_for_month(p, target_yyyymm) {
                        if self.matches_filters(series, current_filters, target_yyyymm, Some("states")) {
                            *states.entry(c.state_code.clone()).or_insert(0) += 1;
                        }
                        if self.matches_filters(series, current_filters, target_yyyymm, Some("counties")) {
                            *counties.entry(c.county_name.clone()).or_insert(0) += 1;
                        }
                        if self.matches_filters(series, current_filters, target_yyyymm, Some("parentOrgs")) {
                            *parent_orgs.entry(p.parent_org.clone()).or_insert(0) += 1;
                        }
                        if self.matches_filters(series, current_filters, target_yyyymm, Some("contracts")) {
                            *contracts.entry(p.contract_id.clone()).or_insert(0) += 1;
                        }
                        if self.matches_filters(series, current_filters, target_yyyymm, Some("plans")) {
                            *plans.entry(format!("{} - {}", p.plan_id, p.plan_name)).or_insert(0) += 1;
                        }
                        if self.matches_filters(series, current_filters, target_yyyymm, Some("planTypes")) {
                            *plan_types.entry(p.plan_type.clone()).or_insert(0) += 1;
                        }
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
            "planTypes": format_options(plan_types),
        }))
    }

    pub fn get_state_rollup(&self, state_code: &str, start_month: YearMonth, end_month: YearMonth) -> Result<Vec<(u32, u32)>> {
        let mut monthly_totals = HashMap::new();
        let start_yyyymm = start_month.to_yyyymm();
        let end_yyyymm = end_month.to_yyyymm();
        if let Some(series_cache) = &self.series_cache {
            if let Some(target_keys) = self.state_to_county_keys.get(state_code) {
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

    pub fn get_top_movers(&self, filters: &serde_json::Value, month_a: YearMonth, month_b: YearMonth, limit: usize) -> Result<Vec<(String, String, String, i32, u32)>> {
        let yyyymm_a = month_a.to_yyyymm();
        let yyyymm_b = month_b.to_yyyymm();

        if let (Some(series_cache), Some(county_lookup), Some(plan_lookup)) =
           (&self.series_cache, &self.county_lookup, &self.plan_lookup) {

            let mut matching_nks = HashSet::new();
            for plan in plan_lookup.values() {
                if self.matches_plan_only_filters(plan, filters, yyyymm_b) {
                    matching_nks.insert(format!("{}|{}", plan.contract_id, plan.plan_id));
                }
            }

            let sel_states: HashSet<String> = filters["states"].as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();
            let sel_counties: HashSet<String> = filters["counties"].as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            let mut nk_data: HashMap<String, (i32, u32)> = HashMap::new();
            let mut nk_info: HashMap<String, (String, String, String)> = HashMap::new();

            for series in series_cache.values() {
                let plan = match plan_lookup.get(&series.plan_key) { Some(p) => p, None => continue };
                let nk = format!("{}|{}", plan.contract_id, plan.plan_id);
                if !matching_nks.contains(&nk) { continue; }

                if !sel_states.is_empty() || !sel_counties.is_empty() {
                    let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                    if !sel_states.is_empty() && !sel_states.contains(&county.state_code) { continue; }
                    if !sel_counties.is_empty() && !sel_counties.contains(&county.county_name) { continue; }
                }

                let info = nk_info.entry(nk.clone()).or_insert_with(||
                    (plan.contract_id.clone(), plan.plan_id.clone(), plan.plan_name.clone())
                );
                if self.is_plan_valid_for_month(plan, yyyymm_b) {
                    info.2 = plan.plan_name.clone();
                }

                let val_a = series.get_enrollment(yyyymm_a).unwrap_or(0);
                let val_b = series.get_enrollment(yyyymm_b).unwrap_or(0);
                let data = nk_data.entry(nk).or_insert((0, 0));
                data.0 += val_b as i32 - val_a as i32;
                data.1 += val_a;
            }

            let mut movers = Vec::new();
            for (nk, (change, prior)) in nk_data {
                if change == 0 { continue; }
                if let Some((cid, pid, name)) = nk_info.get(&nk) {
                    movers.push((cid.clone(), pid.clone(), name.clone(), change, prior));
                }
            }
            movers.sort_by_key(|(_, _, _, c, _)| std::cmp::Reverse(c.abs()));
            return Ok(movers.into_iter().take(limit).collect());
        }
        Err(anyhow::anyhow!("Binary cache required."))
    }

    pub fn get_dashboard_summary(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (current_yyyymm, _) = self.get_analysis_months(filters);
        let mut total_enrollment: u64 = 0;
        let mut unique_plans: HashSet<String> = HashSet::new();
        let mut unique_counties: HashSet<u32> = HashSet::new();
        let mut unique_orgs: HashSet<String> = HashSet::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = 
           (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            
            let mut matching_nks = HashSet::new();
            for plan in plan_lookup.values() {
                if self.matches_plan_only_filters(plan, filters, current_yyyymm) {
                    matching_nks.insert(format!("{}|{}", plan.contract_id, plan.plan_id));
                }
            }

            for series in series_cache.values() {
                let plan = match plan_lookup.get(&series.plan_key) { Some(p) => p, None => continue };
                let nk = format!("{}|{}", plan.contract_id, plan.plan_id);
                
                if matching_nks.contains(&nk) {
                    if !self.is_plan_valid_for_month(plan, current_yyyymm) { continue; }

                    let sel_states: HashSet<String> = filters["states"].as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                        .unwrap_or_default();
                    let sel_counties: HashSet<String> = filters["counties"].as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                        .unwrap_or_default();
                    if !sel_states.is_empty() || !sel_counties.is_empty() {
                        let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                        if !sel_states.is_empty() && !sel_states.contains(&county.state_code) { continue; }
                        if !sel_counties.is_empty() && !sel_counties.contains(&county.county_name) { continue; }
                    }

                    if let Some(val) = series.get_enrollment(current_yyyymm) {
                        total_enrollment += val as u64;
                        unique_plans.insert(nk);
                        unique_counties.insert(series.county_key);
                        unique_orgs.insert(plan.parent_org.clone());
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
        let (analysis_yyyymm, _) = self.get_analysis_months(current_filters);
        let mut monthly_totals: HashMap<u32, u64> = HashMap::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            let mut matching_nks = HashSet::new();
            for plan in plan_lookup.values() {
                if self.matches_plan_only_filters(plan, current_filters, analysis_yyyymm) {
                    matching_nks.insert(format!("{}|{}", plan.contract_id, plan.plan_id));
                }
            }

            for series in series_cache.values() {
                let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                let plan = match plan_lookup.get(&series.plan_key) { Some(p) => p, None => continue };
                let nk = format!("{}|{}", plan.contract_id, plan.plan_id);

                if matching_nks.contains(&nk) {
                    if let Some(sel_states) = current_filters["states"].as_array() {
                        if !sel_states.is_empty() {
                            let states: HashSet<String> = sel_states.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                            if !states.contains(&county.state_code) { continue; }
                        }
                    }
                    if let Some(sel_counties) = current_filters["counties"].as_array() {
                        if !sel_counties.is_empty() {
                            let counties: HashSet<String> = sel_counties.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                            if !counties.contains(&county.county_name) { continue; }
                        }
                    }

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
                            
                            if yyyymm <= analysis_yyyymm {
                                if self.is_plan_valid_for_month(plan, yyyymm) {
                                    if let Some(&enrollment) = series.enrollments.get(pos) {
                                        *monthly_totals.entry(yyyymm).or_insert(0) += enrollment as u64;
                                    }
                                }
                            }
                            pos += 1;
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
        let (current_yyyymm, prior_yyyymm) = self.get_analysis_months(filters);
        
        let mut aggregates: HashMap<String, (u64, u64)> = HashMap::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = 
           (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            
            let mut matching_nks = HashSet::new();
            for plan in plan_lookup.values() {
                if self.matches_plan_only_filters(plan, filters, current_yyyymm) {
                    matching_nks.insert(format!("{}|{}", plan.contract_id, plan.plan_id));
                }
            }

            for series in series_cache.values() {
                let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                let plan = match plan_lookup.get(&series.plan_key) { Some(p) => p, None => continue };
                let nk = format!("{}|{}", plan.contract_id, plan.plan_id);

                if matching_nks.contains(&nk) {
                    if let Some(sel_states) = filters["states"].as_array() {
                        if !sel_states.is_empty() {
                            let states: HashSet<String> = sel_states.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                            if !states.contains(&county.state_code) { continue; }
                        }
                    }
                    if let Some(sel_counties) = filters["counties"].as_array() {
                        if !sel_counties.is_empty() {
                            let counties: HashSet<String> = sel_counties.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                            if !counties.contains(&county.county_name) { continue; }
                        }
                    }

                    let agg_key = match grain {
                        "parentOrg" => plan.parent_org.clone(),
                        "contract" => plan.contract_id.clone(),
                        "plan" => format!("{}|{}", plan.contract_id, plan.plan_id),
                        "county" => format!("{}|{}", county.state_code, county.county_name),
                        _ => "Unknown".to_string(),
                    };
                    
                    if let Some(val) = series.get_enrollment(current_yyyymm) {
                        if self.is_plan_valid_for_month(plan, current_yyyymm) {
                            aggregates.entry(agg_key.clone()).or_insert((0, 0)).0 += val as u64;
                        }
                    }
                    if let Some(val) = series.get_enrollment(prior_yyyymm) {
                        if self.is_plan_valid_for_month(plan, prior_yyyymm) {
                            aggregates.entry(agg_key).or_insert((0, 0)).1 += val as u64;
                        }
                    }
                }
            }
        }

        let mut rows = Vec::new();
        for (name, (latest, prior)) in aggregates {
            if latest == 0 && prior == 0 { continue; }
            let change = latest as i64 - prior as i64;
            let pct_change = if prior > 0 { (change as f64 / prior as f64) * 100.0 } else { 0.0 };
            rows.push(serde_json::json!({
                "name": name, "current": latest, "prior": prior, "change": change, "percentChange": pct_change,
            }));
        }
        rows.sort_by_key(|r| std::cmp::Reverse(r["current"].as_u64().unwrap_or(0)));
        Ok(serde_json::json!({ "grain": grain, "latestMonth": current_yyyymm, "priorMonth": prior_yyyymm, "rows": rows }))
    }

    pub fn get_org_analysis(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (current_yyyymm, _) = self.get_analysis_months(filters);
        let mut org_data: HashMap<String, (u64, HashMap<u32, u64>)> = HashMap::new(); 
        let mut total_market_enrollment: u64 = 0;

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = 
           (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            
            let mut matching_nks = HashSet::new();
            for plan in plan_lookup.values() {
                if self.matches_plan_only_filters(plan, filters, current_yyyymm) {
                    matching_nks.insert(format!("{}|{}", plan.contract_id, plan.plan_id));
                }
            }

            for series in series_cache.values() {
                let plan = match plan_lookup.get(&series.plan_key) { Some(p) => p, None => continue };
                let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                let nk = format!("{}|{}", plan.contract_id, plan.plan_id);

                if matching_nks.contains(&nk) {
                    if let Some(sel_states) = filters["states"].as_array() {
                        if !sel_states.is_empty() {
                            let states: HashSet<String> = sel_states.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                            if !states.contains(&county.state_code) { continue; }
                        }
                    }
                    if let Some(sel_counties) = filters["counties"].as_array() {
                        if !sel_counties.is_empty() {
                            let counties: HashSet<String> = sel_counties.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                            if !counties.contains(&county.county_name) { continue; }
                        }
                    }

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
                                if self.is_plan_valid_for_month(plan, yyyymm) {
                                    let entry = org_data.entry(plan.parent_org.clone()).or_insert((0, HashMap::new()));
                                    *entry.1.entry(yyyymm).or_insert(0) += enrollment as u64;
                                    if yyyymm == current_yyyymm {
                                        entry.0 += enrollment as u64;
                                        total_market_enrollment += enrollment as u64;
                                    }
                                }
                            }
                            pos += 1;
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
        Ok(serde_json::json!({ "totalMarketEnrollment": total_market_enrollment, "latestMonth": current_yyyymm, "organizations": orgs_list }))
    }

    pub fn get_plan_list(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (current_yyyymm, prior_yyyymm) = self.get_analysis_months(filters);
        let analysis_year = current_yyyymm / 100;
        let aep_feb_yyyymm = analysis_year * 100 + 2;
        let aep_dec_yyyymm = (analysis_year - 1) * 100 + 12;

        struct PlanAccum {
            contract_id: String, plan_id: String, plan_name: String, parent_org: String, plan_type: String,
            current: u64, prior: u64, aep_feb: u64, aep_dec: u64, min_valid_from: u32,
        }

        let mut nk_data: HashMap<String, PlanAccum> = HashMap::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) =
            (&self.plan_lookup, &self.county_lookup, &self.series_cache)
        {
            let mut matching_nks = HashSet::new();
            let mut min_valid_from: HashMap<String, u32> = HashMap::new();
            for plan in plan_lookup.values() {
                if self.matches_plan_only_filters(plan, filters, current_yyyymm) {
                    let nk = format!("{}|{}", plan.contract_id, plan.plan_id);
                    matching_nks.insert(nk.clone());
                    let e = min_valid_from.entry(nk).or_insert(u32::MAX);
                    if plan.valid_from_month < *e { *e = plan.valid_from_month; }
                }
            }

            let sel_states: HashSet<String> = filters["states"].as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();
            let sel_counties: HashSet<String> = filters["counties"].as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            for series in series_cache.values() {
                let plan = match plan_lookup.get(&series.plan_key) { Some(p) => p, None => continue };
                let nk = format!("{}|{}", plan.contract_id, plan.plan_id);
                if !matching_nks.contains(&nk) { continue; }

                if !sel_states.is_empty() || !sel_counties.is_empty() {
                    let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                    if !sel_states.is_empty() && !sel_states.contains(&county.state_code) { continue; }
                    if !sel_counties.is_empty() && !sel_counties.contains(&county.county_name) { continue; }
                }

                let mvf = min_valid_from.get(&nk).copied().unwrap_or(0);
                let entry = nk_data.entry(nk).or_insert_with(|| PlanAccum {
                    contract_id: plan.contract_id.clone(), plan_id: plan.plan_id.clone(), plan_name: plan.plan_name.clone(),
                    parent_org: plan.parent_org.clone(), plan_type: plan.plan_type.clone(),
                    current: 0, prior: 0, aep_feb: 0, aep_dec: 0, min_valid_from: mvf,
                });

                if self.is_plan_valid_for_month(plan, current_yyyymm) {
                    entry.plan_name = plan.plan_name.clone();
                    entry.parent_org = plan.parent_org.clone();
                    entry.plan_type = plan.plan_type.clone();
                    if let Some(val) = series.get_enrollment(current_yyyymm) { entry.current += val as u64; }
                }
                if self.is_plan_valid_for_month(plan, prior_yyyymm) {
                    if let Some(val) = series.get_enrollment(prior_yyyymm) { entry.prior += val as u64; }
                }
                if self.is_plan_valid_for_month(plan, aep_feb_yyyymm) {
                    if let Some(val) = series.get_enrollment(aep_feb_yyyymm) { entry.aep_feb += val as u64; }
                }
                if self.is_plan_valid_for_month(plan, aep_dec_yyyymm) {
                    if let Some(val) = series.get_enrollment(aep_dec_yyyymm) { entry.aep_dec += val as u64; }
                }
            }
        }

        let mut rows: Vec<serde_json::Value> = nk_data
            .into_values()
            .filter(|e| e.current > 0)
            .map(|e| {
                let mom_change = e.current as i64 - e.prior as i64;
                let aep_growth = e.aep_feb as i64 - e.aep_dec as i64;
                let aep_growth_pct = if e.aep_dec > 0 { (aep_growth as f64 / e.aep_dec as f64) * 100.0 } else if e.aep_feb > 0 { 100.0 } else { 0.0 };
                let is_new = e.min_valid_from >= analysis_year * 100 + 1 && e.min_valid_from < (analysis_year + 1) * 100 + 1;
                serde_json::json!({
                    "contractId": e.contract_id, "planId": e.plan_id, "planName": e.plan_name, "parentOrg": e.parent_org,
                    "planType": e.plan_type, "enrollment": e.current, "priorEnrollment": e.prior, "momChange": mom_change,
                    "aepGrowth": aep_growth, "aepGrowthPct": aep_growth_pct, "aepDecEnrollment": e.aep_dec, "isNew": is_new,
                })
            })
            .collect();

        rows.sort_by_key(|r| std::cmp::Reverse(r["enrollment"].as_u64().unwrap_or(0)));

        Ok(serde_json::json!({ "rows": rows, "currentMonth": current_yyyymm, "priorMonth": prior_yyyymm, "aepFebMonth": aep_feb_yyyymm, "aepDecMonth": aep_dec_yyyymm }))
    }

    pub fn get_geo_analysis(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (current_yyyymm, _) = self.get_analysis_months(filters);
        let mut state_data: HashMap<String, u64> = HashMap::new();
        let mut county_data: HashMap<String, u64> = HashMap::new(); 

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            let mut matching_nks = HashSet::new();
            for plan in plan_lookup.values() {
                if self.matches_plan_only_filters(plan, filters, current_yyyymm) {
                    matching_nks.insert(format!("{}|{}", plan.contract_id, plan.plan_id));
                }
            }

            for series in series_cache.values() {
                let plan = match plan_lookup.get(&series.plan_key) { Some(p) => p, None => continue };
                let nk = format!("{}|{}", plan.contract_id, plan.plan_id);
                if matching_nks.contains(&nk) {
                    if let Some(county) = county_lookup.get(&series.county_key) {
                        if let Some(val) = series.get_enrollment(current_yyyymm) {
                            if self.is_plan_valid_for_month(plan, current_yyyymm) {
                                *state_data.entry(county.state_code.clone()).or_insert(0) += val as u64;
                                let county_key = format!("{}|{}", county.state_code, county.county_name);
                                *county_data.entry(county_key).or_insert(0) += val as u64;
                            }
                        }
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
        Ok(serde_json::json!({ "latestMonth": current_yyyymm, "states": states_list, "counties": counties_list.into_iter().take(50).collect::<Vec<_>>() }))
    }

    pub fn get_growth_analytics(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (current_yyyymm, prior_yyyymm) = self.get_analysis_months(filters);
        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            let year = current_yyyymm / 100;
            let aep_target = (year * 100) + 2;
            let aep_base = ((year - 1) * 100) + 12;
            use rayon::prelude::*;
            let mut matching_nks = HashSet::new();
            for plan in plan_lookup.values() {
                if self.matches_plan_only_filters(plan, filters, current_yyyymm) {
                    matching_nks.insert(format!("{}|{}", plan.contract_id, plan.plan_id));
                }
            }
            let series_results: Vec<(String, i64, i64, u64, u64, u64, u64, String, String, String)> = series_cache.par_iter()
                .filter_map(|(_, series)| {
                    let plan = plan_lookup.get(&series.plan_key)?;
                    let nk = format!("{}|{}", plan.contract_id, plan.plan_id);
                    if !matching_nks.contains(&nk) { return None; }
                    let county = county_lookup.get(&series.county_key)?;
                    if let Some(sel_states) = filters["states"].as_array() {
                        if !sel_states.is_empty() {
                            let states: HashSet<String> = sel_states.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                            if !states.contains(&county.state_code) { return None; }
                        }
                    }
                    let latest_val = if self.is_plan_valid_for_month(plan, current_yyyymm) { series.get_enrollment(current_yyyymm).unwrap_or(0) } else { 0 };
                    let prior_val = if self.is_plan_valid_for_month(plan, prior_yyyymm) { series.get_enrollment(prior_yyyymm).unwrap_or(0) } else { 0 };
                    let aep_t_val = if self.is_plan_valid_for_month(plan, aep_target) { series.get_enrollment(aep_target).unwrap_or(0) } else { 0 };
                    let aep_b_val = if self.is_plan_valid_for_month(plan, aep_base) { series.get_enrollment(aep_base).unwrap_or(0) } else { 0 };
                    if latest_val == 0 && prior_val == 0 && aep_t_val == 0 && aep_b_val == 0 { return None; }
                    Some((nk, (latest_val as i64) - (prior_val as i64), (aep_t_val as i64) - (aep_b_val as i64), latest_val as u64, prior_val as u64, aep_t_val as u64, aep_b_val as u64, plan.plan_name.clone(), plan.contract_id.clone(), plan.plan_id.clone()))
                }).collect();
            let mut total_growth: i64 = 0; let mut aep_growth: i64 = 0;
            let mut plan_aggregates: HashMap<String, (u64, u64, u64, u64, String, String, String)> = HashMap::new();
            for (nk, mom, aep, cur, pri, a_t, a_b, name, cid, pid) in series_results {
                total_growth += mom; aep_growth += aep;
                let entry = plan_aggregates.entry(nk).or_insert((0, 0, 0, 0, name, cid, pid));
                entry.0 += cur; entry.1 += pri; entry.2 += a_t; entry.3 += a_b;
            }
            let mut high_flyers = Vec::new();
            for (_, (latest, prior, aep_t, aep_b, name, cid, pid)) in plan_aggregates {
                if latest > 500 { 
                    let change = latest as i64 - prior as i64;
                    let pct = if prior > 0 { (change as f64 / prior as f64) * 100.0 } else { 0.0 };
                    let aep_change = aep_t as i64 - aep_b as i64;
                    if pct > 5.0 || change > 1000 || aep_change.abs() > 1000 {
                        high_flyers.push(serde_json::json!({ "name": name, "contract": cid, "plan": pid, "current": latest, "change": change, "percent": pct, "aepChange": aep_change }));
                    }
                }
            }
            high_flyers.sort_by_key(|h| std::cmp::Reverse((h["percent"].as_f64().unwrap_or(0.0) * 100.0) as i64));
            return Ok(serde_json::json!({ "latestMonth": current_yyyymm, "priorMonth": prior_yyyymm, "totalGrowth": total_growth, "aepGrowth": aep_growth, "highFlyers": high_flyers.into_iter().take(20).collect::<Vec<_>>() }));
        }
        Err(anyhow::anyhow!("Binary cache required."))
    }

    pub fn get_plan_details(&self, contract_id: &str, plan_id: &str) -> Result<serde_json::Value> {
        if !self.cache_enabled { return Err(anyhow::anyhow!("Cache required.")); }
        let plan_lookup = self.plan_lookup.as_ref().unwrap();
        let county_lookup = self.county_lookup.as_ref().unwrap();
        let series_cache = self.series_cache.as_ref().unwrap();
        let plan = plan_lookup.values().find(|p| p.contract_id == contract_id && p.plan_id == plan_id && p.is_current).ok_or_else(|| anyhow::anyhow!("Plan not found"))?;
        let mut footprint = Vec::new();
        let mut global_trend: HashMap<u32, u64> = HashMap::new();
        for series in series_cache.values() {
            if series.plan_key == plan.plan_key {
                let county = county_lookup.get(&series.county_key);
                if let Some(c) = county {
                    let latest_val = series.enrollments.last().cloned().unwrap_or(0);
                    footprint.push(serde_json::json!({ "state": c.state_code, "county": c.county_name, "enrollment": latest_val }));
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
                            if let Some(&val) = series.enrollments.get(pos) { *global_trend.entry(yyyymm).or_insert(0) += val as u64; }
                            pos += 1;
                        }
                    }
                }
            }
        }
        footprint.sort_by_key(|f| std::cmp::Reverse(f["enrollment"].as_u64().unwrap_or(0)));
        let mut trend_list: Vec<_> = global_trend.into_iter().collect();
        trend_list.sort_by_key(|(m, _)| *m);
        Ok(serde_json::json!({ "metadata": { "name": plan.plan_name, "contract_id": plan.contract_id, "plan_id": plan.plan_id, "org": plan.parent_org, "type": plan.plan_type, "egwp": plan.is_egwp, "snp": plan.is_snp }, "footprint": footprint, "trend": trend_list.into_iter().map(|(m, v)| serde_json::json!({ "month": m, "value": v })).collect::<Vec<_>>() }))
    }

    pub fn get_crosswalk_analysis(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (current_yyyymm, _) = self.get_analysis_months(filters);
        let year = current_yyyymm / 100;
        let store_dir = Path::new("store");
        let crosswalk_path = store_dir.join("crosswalk").join("normalized").join(format!("year={}", year)).join("crosswalk.parquet");
        if !crosswalk_path.exists() { return Ok(serde_json::json!({ "status": "not_loaded", "year": year })); }
        // Deduplicate by (previous_plan_key, current_plan_key): the raw CMS files sometimes
        // contain identical rows multiple times, producing phantom duplicate predecessors in the UI.
        let crosswalk_rows = {
            let raw = storage::parquet_store::load_crosswalk_data(&crosswalk_path)?;
            let mut seen: HashSet<(String, String)> = HashSet::new();
            raw.into_iter().filter(|r| seen.insert((r.previous_plan_key.clone(), r.current_plan_key.clone()))).collect::<Vec<_>>()
        };

        let plan_lookup = match &self.plan_lookup { Some(l) => l, None => return Err(anyhow::anyhow!("Plan lookup required")), };
        let county_lookup = match &self.county_lookup { Some(l) => l, None => return Err(anyhow::anyhow!("County lookup required")), };
        let series_cache = match &self.series_cache { Some(c) => c, None => return Err(anyhow::anyhow!("Series cache required")), };

        // Build str-key → Vec<u32> plan_key map (covers plans from CPSC contract/enrollment data,
        // including 800-series EGWP plans that may not appear in landscape files).
        // A plan may appear under multiple u32 keys across different valid periods, so we keep
        // ALL of them and union footprints when building county sets.
        let mut str_to_plan_keys: HashMap<String, Vec<u32>> = HashMap::new();
        for (k, p) in plan_lookup.iter() {
            str_to_plan_keys.entry(format!("{}-{}", p.contract_id, p.plan_id)).or_default().push(*k);
        }
        // Helper: find the first matching PlanDim for a string key (for metadata lookups).
        let get_plan_dim = |str_key: &str| -> Option<&PlanDim> {
            str_to_plan_keys.get(str_key)?.iter().find_map(|k| plan_lookup.get(k))
        };

        // Use landscape parquet footprints as the authoritative source of county membership.
        // Landscape footprints are keyed (year, "contract_id-plan_id") → HashSet<county_name>.
        // Fall back to enrollment series if landscape is unavailable for a given year.
        let prior_year = year - 1;
        let has_landscape_curr  = self.landscape_fp.keys().any(|(y, _)| *y == year);
        let has_landscape_prior = self.landscape_fp.keys().any(|(y, _)| *y == prior_year);

        // Series-based fallback footprints (used only if landscape isn't loaded).
        let (series_prior_fp, series_curr_fp): (HashMap<String, HashSet<String>>, HashMap<String, HashSet<String>>) =
        if !has_landscape_curr || !has_landscape_prior {
            let mut prior_fp_s: HashMap<String, HashSet<String>> = HashMap::new();
            let mut curr_fp_s:  HashMap<String, HashSet<String>> = HashMap::new();
            let prior_yr_start = prior_year * 100 + 1;
            let prior_yr_end   = prior_year * 100 + 12;
            let curr_yr_start  = year * 100 + 1;
            let curr_yr_end    = year * 100 + 12;
            if let Some(pl) = &self.plan_lookup {
                for ((pk, ck), series) in series_cache.iter() {
                    if let Some(p) = pl.get(pk) {
                        let key = format!("{}-{}", p.contract_id, p.plan_id);
                        // Use compound "STATE:county" key to match landscape format.
                        let county_key = county_lookup.get(ck).map(|c| format!("{}:{}", c.state_code, c.county_name)).unwrap_or_default();
                        if county_key.is_empty() { continue; }
                        if (prior_yr_start..=prior_yr_end).any(|m| series.get_enrollment(m).is_some()) {
                            prior_fp_s.entry(key.clone()).or_default().insert(county_key.clone());
                        }
                        if (curr_yr_start..=curr_yr_end).any(|m| series.get_enrollment(m).is_some()) {
                            curr_fp_s.entry(key).or_default().insert(county_key);
                        }
                    }
                }
            }
            (prior_fp_s, curr_fp_s)
        } else {
            (HashMap::new(), HashMap::new())
        };

        // Helper: get landscape county set for a plan-year, falling back to series.
        let get_landscape_fp = |str_key: &str, yr: u32| -> HashSet<String> {
            if let Some(set) = self.landscape_fp.get(&(yr, str_key.to_string())) {
                set.clone()
            } else if yr == year {
                series_curr_fp.get(str_key).cloned().unwrap_or_default()
            } else {
                series_prior_fp.get(str_key).cloned().unwrap_or_default()
            }
        };

        // Parse active filters — empty arrays treated as no-filter.
        let parse_str_set = |key: &str| -> Option<HashSet<String>> {
            filters[key].as_array().and_then(|a| {
                let s: HashSet<String> = a.iter().filter_map(|v| v.as_str().map(|x| x.to_string())).collect();
                if s.is_empty() { None } else { Some(s) }
            })
        };
        let sel_orgs     = parse_str_set("parentOrgs");
        let sel_states   = parse_str_set("states");
        let sel_counties = parse_str_set("counties");
        let sel_types    = parse_str_set("planTypes");
        let sel_snp      = filters["snp"].as_bool();
        let sel_egwp     = filters["eghp"].as_bool(); // filter key is "eghp" per FilterContext

        // Check whether a compound "STATE:county" key passes the active geo filter.
        // The "UNKNOWN:" prefix (from series fallback for plans with no state data) never
        // matches a real state filter, which correctly suppresses those entries when filtering.
        let county_key_passes_geo = |compound_key: &str| -> bool {
            let mut parts = compound_key.splitn(2, ':');
            let state_part  = parts.next().unwrap_or("");
            let county_part = parts.next().unwrap_or(compound_key);
            if let Some(states) = &sel_states {
                if !states.contains(state_part) { return false; }
            }
            if let Some(cties) = &sel_counties {
                if !cties.contains(county_part) { return false; }
            }
            true
        };

        // Count counties in a set that pass the geo filter.
        let count_in_geo = |counties: &HashSet<String>| -> usize {
            counties.iter().filter(|ck| county_key_passes_geo(ck)).count()
        };

        // Intermediate per-row data — stored before group-level county processing.
        struct IntermRow {
            crosswalk_year: i32,
            previous_contract_id: String,
            previous_plan_id: String,
            previous_plan_key: String,
            previous_plan_name: Option<String>,
            current_contract_id: String,
            current_plan_id: String,
            current_plan_key: String,
            current_plan_name: Option<String>,
            raw_status: String,
            final_status: String,
            display_status: String,
            is_new: bool,
            is_terminated: bool,
            is_expansion: bool,
            is_reduction: bool,
            total_counties: usize,
            filtered_counties: usize,
            counties_added: usize,
            counties_removed: usize,
            prev_counties: HashSet<String>,
            org: Option<String>,
            plan_type: Option<String>,
            is_egwp: Option<bool>,
        }

        // === Pass 1: filter rows and compute per-row metrics ===
        let mut intermed: Vec<IntermRow> = Vec::new();

        for row in &crosswalk_rows {
            // Infer termination from plan ID fields when status column is absent/generic.
            let effective_status = {
                let s_up = row.status.to_uppercase();
                if s_up.contains("TERMINATED") || s_up.contains("NON-RENEWED") {
                    row.status.clone()
                } else {
                    let cc_up = row.current_contract_id.to_uppercase();
                    let cp_up = row.current_plan_id.to_uppercase();
                    if cc_up.contains("TERMINATED") || cp_up.contains("TERMINATED") {
                        "Terminated/Non-Renewed Contract".to_string()
                    } else {
                        row.status.clone()
                    }
                }
            };
            let s_up = effective_status.to_uppercase();
            let is_terminated = s_up.contains("TERMINATED") || s_up.contains("NON-RENEWED");
            // A plan is truly "new" only when there is no real predecessor: the CMS crosswalk
            // encodes new plans by putting "NEW" in the previous plan ID column.  If the previous
            // plan ID is an actual plan number the row is a renewal even if the status label says
            // "New Plan" (a known CMS labelling inconsistency).
            let has_real_predecessor = !row.previous_plan_key.is_empty()
                && !row.previous_plan_id.to_uppercase().contains("NEW")
                && !row.previous_contract_id.to_uppercase().contains("NEW");
            let is_new_plan = (s_up.contains("NEW") || s_up.contains("INITIAL"))
                && !is_terminated
                && !has_real_predecessor;

            // Use previous plan for metadata lookups on terminated rows.
            let ref_key = if is_terminated || row.current_plan_key.is_empty() {
                &row.previous_plan_key
            } else {
                &row.current_plan_key
            };
            let plan_dim = get_plan_dim(ref_key.as_str());

            // Metadata filters (org, type, SNP, EGWP — sourced from CPSC contract data via plan_lookup).
            if let Some(orgs) = &sel_orgs {
                match plan_dim { Some(p) => if !orgs.contains(&p.parent_org) { continue; }, None => continue }
            }
            if let Some(types) = &sel_types {
                match plan_dim { Some(p) => if !types.contains(&p.plan_type) { continue; }, None => continue }
            }
            if let Some(snp) = sel_snp {
                match plan_dim { Some(p) => if p.is_snp != snp { continue; }, None => continue }
            }
            if let Some(egwp) = sel_egwp {
                match plan_dim { Some(p) => if p.is_egwp != egwp { continue; }, None => continue }
            }

            // County footprints — from landscape parquet (authoritative) or series fallback.
            let prev_counties: HashSet<String> = get_landscape_fp(&row.previous_plan_key, prior_year);
            let curr_counties: HashSet<String> = if is_terminated {
                HashSet::new()
            } else {
                get_landscape_fp(&row.current_plan_key, year)
            };

            // Geo filter — terminated plans are matched against their *prior* footprint.
            // Non-terminated plans with no current footprint fall back to prior for geo matching.
            let geo_counties = if is_terminated || curr_counties.is_empty() { &prev_counties } else { &curr_counties };

            if sel_states.is_some() || sel_counties.is_some() {
                let ok = geo_counties.iter().any(|ck| county_key_passes_geo(ck));
                if !ok { continue; }
            }

            // Raw county change (by county name).
            let raw_added   = curr_counties.difference(&prev_counties).count();
            let raw_removed = prev_counties.difference(&curr_counties).count();

            // Status reconciliation (Bug 1): a plain renewal with a non-zero footprint
            // change is a contradiction — let the geography drive the final classification.
            let is_plain_renewal = s_up.contains("RENEWAL")
                && !s_up.contains("SAE") && !s_up.contains("SAR")
                && !s_up.contains("CONSOLIDATED")
                && !is_terminated && !is_new_plan;

            let (final_status, final_added, final_removed) = if is_plain_renewal && (raw_added > 0 || raw_removed > 0) {
                if raw_added > 0 && raw_removed == 0 {
                    ("Renewal Plan with SAE".to_string(), raw_added, 0usize)
                } else if raw_removed > 0 && raw_added == 0 {
                    ("Renewal Plan with SAR".to_string(), 0usize, raw_removed)
                } else if raw_added >= raw_removed {
                    ("Renewal Plan with SAE".to_string(), raw_added, raw_removed)
                } else {
                    ("Renewal Plan with SAR".to_string(), raw_added, raw_removed)
                }
            } else {
                (effective_status.clone(), raw_added, raw_removed)
            };

            let fs_up = final_status.to_uppercase();
            // Both flags can be true simultaneously (plan added some counties and dropped others).
            let is_expansion = final_added > 0 && !is_new_plan && !is_terminated;
            let is_reduction = final_removed > 0 && !is_new_plan && !is_terminated;

            // Display status mapping.
            let display_status = if is_terminated {
                "Closed"
            } else if is_new_plan {
                "New Plan"
            } else if fs_up.contains("CONSOLIDATED") {
                "Consolidated"
            } else if is_expansion && is_reduction {
                "Service Area Change"
            } else if fs_up.contains("SAE") || is_expansion {
                "Service Area Expansion"
            } else if fs_up.contains("SAR") || is_reduction {
                "Service Area Reduction"
            } else {
                "Renewal"
            };

            // Closed plans show 0 current counties — they no longer operate anywhere.
            let total_counties = if is_terminated { 0 } else { curr_counties.len() };
            let filtered_counties = if is_terminated { 0 } else { count_in_geo(&curr_counties) };

            intermed.push(IntermRow {
                crosswalk_year: row.crosswalk_year,
                previous_contract_id: row.previous_contract_id.clone(),
                previous_plan_id: row.previous_plan_id.clone(),
                previous_plan_key: row.previous_plan_key.clone(),
                previous_plan_name: row.previous_plan_name.clone(),
                current_contract_id: row.current_contract_id.clone(),
                current_plan_id: row.current_plan_id.clone(),
                current_plan_key: row.current_plan_key.clone(),
                current_plan_name: row.current_plan_name.clone(),
                raw_status: row.status.clone(),
                final_status,
                display_status: display_status.to_string(),
                is_new: is_new_plan,
                is_terminated,
                is_expansion,
                is_reduction,
                total_counties,
                filtered_counties,
                counties_added: final_added,
                counties_removed: final_removed,
                prev_counties,
                org: plan_dim.map(|p| p.parent_org.clone()),
                plan_type: plan_dim.map(|p| p.plan_type.clone()),
                is_egwp: plan_dim.map(|p| p.is_egwp),
            });
        }

        // === Pass 2: group-level county metrics for many-to-one mappings (Bug 5) ===
        // For each unique successor plan, union all predecessor footprints and compute
        // the true county change against the successor footprint.
        let mut group_prev_union: HashMap<String, HashSet<String>> = HashMap::new();
        let mut group_sizes: HashMap<String, usize> = HashMap::new();
        for ir in &intermed {
            if !ir.is_terminated && !ir.current_plan_key.is_empty() {
                let entry = group_prev_union.entry(ir.current_plan_key.clone()).or_default();
                entry.extend(ir.prev_counties.iter().cloned());
                *group_sizes.entry(ir.current_plan_key.clone()).or_insert(0) += 1;
            }
        }
        // Helper: split compound key "STATE:county" into (state, county) parts.
        let split_county_key = |k: &str| -> (String, String) {
            let mut it = k.splitn(2, ':');
            let s = it.next().unwrap_or("").to_string();
            let c = it.next().unwrap_or(k).to_string();
            (s, c)
        };

        let mut group_added_map: HashMap<String, usize> = HashMap::new();
        let mut group_removed_map: HashMap<String, usize> = HashMap::new();
        // County name sets for map display: Vec<(state, county)> sorted by state then county.
        let mut group_renewed_sets: HashMap<String, Vec<(String, String)>> = HashMap::new();
        let mut group_added_sets:   HashMap<String, Vec<(String, String)>> = HashMap::new();
        let mut group_removed_sets: HashMap<String, Vec<(String, String)>> = HashMap::new();
        for (key, prev_union) in &group_prev_union {
            let curr_set: HashSet<String> = get_landscape_fp(key.as_str(), year);
            group_added_map.insert(key.clone(), curr_set.difference(prev_union).count());
            group_removed_map.insert(key.clone(), prev_union.difference(&curr_set).count());

            let mut renewed: Vec<(String, String)> = curr_set.intersection(prev_union).map(|k| split_county_key(k)).collect();
            let mut added:   Vec<(String, String)> = curr_set.difference(prev_union).map(|k| split_county_key(k)).collect();
            let mut removed: Vec<(String, String)> = prev_union.difference(&curr_set).map(|k| split_county_key(k)).collect();
            renewed.sort(); added.sort(); removed.sort();
            group_renewed_sets.insert(key.clone(), renewed);
            group_added_sets.insert(key.clone(), added);
            group_removed_sets.insert(key.clone(), removed);
        }
        // For terminated plans and new plans that aren't in group_prev_union, compute individually.
        // (They have no "group" so their county sets come from the row itself.)
        let mut row_added_sets:   HashMap<String, Vec<(String, String)>> = HashMap::new();
        let mut row_removed_sets: HashMap<String, Vec<(String, String)>> = HashMap::new();
        for ir in &intermed {
            if ir.is_terminated {
                // Show prior counties as removed (the plan is gone).
                let mut removed: Vec<(String, String)> = ir.prev_counties.iter().map(|k| split_county_key(k)).collect();
                removed.sort();
                row_removed_sets.insert(ir.previous_plan_key.clone(), removed);
            } else if ir.is_new {
                let curr = get_landscape_fp(&ir.current_plan_key, year);
                let mut added: Vec<(String, String)> = curr.iter().map(|k| split_county_key(k)).collect();
                added.sort();
                row_added_sets.insert(ir.current_plan_key.clone(), added);
            }
        }

        // === Pass 3: build final JSON and accumulate summary totals ===
        let mut total_renewals = 0i64; let mut total_consolidations = 0i64; let mut total_new = 0i64;
        let mut total_terminated = 0i64; let mut total_sae = 0i64; let mut total_sar = 0i64;
        let mut enriched_rows: Vec<serde_json::Value> = Vec::new();

        for ir in intermed.into_iter().take(1000) {
            let fs_up = ir.final_status.to_uppercase();
            if fs_up.contains("CONSOLIDATED") { total_consolidations += 1; }
            else if fs_up.contains("SAE") { total_sae += 1; }
            else if fs_up.contains("SAR") { total_sar += 1; }
            else if fs_up.contains("RENEWAL") { total_renewals += 1; }
            else if ir.is_new { total_new += 1; }
            else if ir.is_terminated { total_terminated += 1; }

            let group_size = group_sizes.get(&ir.current_plan_key).cloned().unwrap_or(1);
            let g_added   = group_added_map.get(&ir.current_plan_key).cloned().unwrap_or(ir.counties_added);
            let g_removed = group_removed_map.get(&ir.current_plan_key).cloned().unwrap_or(ir.counties_removed);

            // County sets for map display: use group-level sets when available, else row-level.
            let to_json_counties = |v: &[(String, String)]| -> serde_json::Value {
                serde_json::Value::Array(v.iter().map(|(s, c)| serde_json::json!({"state": s, "county": c})).collect())
            };
            let empty: Vec<(String, String)> = Vec::new();
            let renewed_counties = group_renewed_sets.get(&ir.current_plan_key)
                .map(|v| to_json_counties(v)).unwrap_or_else(|| to_json_counties(&empty));
            let added_counties = if ir.is_new {
                row_added_sets.get(&ir.current_plan_key).map(|v| to_json_counties(v)).unwrap_or_else(|| to_json_counties(&empty))
            } else {
                group_added_sets.get(&ir.current_plan_key).map(|v| to_json_counties(v)).unwrap_or_else(|| to_json_counties(&empty))
            };
            let removed_counties = if ir.is_terminated {
                row_removed_sets.get(&ir.previous_plan_key).map(|v| to_json_counties(v)).unwrap_or_else(|| to_json_counties(&empty))
            } else {
                group_removed_sets.get(&ir.current_plan_key).map(|v| to_json_counties(v)).unwrap_or_else(|| to_json_counties(&empty))
            };

            enriched_rows.push(serde_json::json!({
                "crosswalk_year":         ir.crosswalk_year,
                "previous_contract_id":   ir.previous_contract_id,
                "previous_plan_id":       ir.previous_plan_id,
                "previous_plan_key":      ir.previous_plan_key,
                "previous_plan_name":     ir.previous_plan_name,
                "current_contract_id":    ir.current_contract_id,
                "current_plan_id":        ir.current_plan_id,
                "current_plan_key":       ir.current_plan_key,
                "current_plan_name":      ir.current_plan_name,
                "status":                 ir.raw_status,
                "display_status":         ir.display_status,
                "is_new":                 ir.is_new,
                "is_terminated":          ir.is_terminated,
                "is_expansion":           ir.is_expansion,
                "is_reduction":           ir.is_reduction,
                "total_counties":         ir.total_counties,
                "filtered_counties":      ir.filtered_counties,
                "counties_added":         ir.counties_added,
                "counties_removed":       ir.counties_removed,
                "group_size":             group_size,
                "group_counties_added":   g_added,
                "group_counties_removed": g_removed,
                "renewed_counties":       renewed_counties,
                "added_counties":         added_counties,
                "removed_counties":       removed_counties,
                "org":                    ir.org,
                "plan_type":              ir.plan_type,
                "is_egwp":                ir.is_egwp,
            }));
        }

        Ok(serde_json::json!({
            "status": "success",
            "year": year,
            "metrics": {
                "renewals":       total_renewals,
                "consolidations": total_consolidations,
                "newPlans":       total_new,
                "terminated":     total_terminated,
                "sae":            total_sae,
                "sar":            total_sar,
            },
            "rows": enriched_rows
        }))
    }

    pub fn get_aep_switching(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (current_yyyymm, _) = self.get_analysis_months(filters);
        let year = current_yyyymm / 100;
        
        let aep_target_yyyymm = year * 100 + 2; // Feb
        let aep_base_yyyymm = (year - 1) * 100 + 12; // Dec prior year

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            
            // 1. Load Crosswalk for mapping
            let store_dir = Path::new("store");
            let crosswalk_path = store_dir.join("crosswalk").join("normalized").join(format!("year={}", year)).join("crosswalk.parquet");
            
            let mut crosswalk_map: HashMap<String, Vec<String>> = HashMap::new(); // prev_key -> [curr_keys]
            if crosswalk_path.exists() {
                let crosswalk_rows = storage::parquet_store::load_crosswalk_data(&crosswalk_path)?;
                for row in crosswalk_rows {
                    crosswalk_map.entry(row.previous_plan_key).or_default().push(row.current_plan_key);
                }
            }

            // 2. Aggregate actual enrollment
            let mut plan_aep_base: HashMap<String, u64> = HashMap::new();
            let mut plan_aep_target: HashMap<String, u64> = HashMap::new();
            
            // NK -> Org
            let mut nk_to_org = HashMap::new();
            for p in plan_lookup.values() {
                nk_to_org.insert(format!("{}-{}", p.contract_id, p.plan_id), p.parent_org.clone());
            }

            for series in series_cache.values() {
                let plan = match plan_lookup.get(&series.plan_key) { Some(p) => p, None => continue };
                let nk = format!("{}-{}", plan.contract_id, plan.plan_id);
                
                // Geo filters
                let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                if let Some(sel_states) = filters["states"].as_array() {
                    if !sel_states.is_empty() {
                        let states: HashSet<String> = sel_states.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                        if !states.contains(&county.state_code) { continue; }
                    }
                }

                if let Some(val) = series.get_enrollment(aep_base_yyyymm) {
                    *plan_aep_base.entry(nk.clone()).or_insert(0) += val as u64;
                }
                if let Some(val) = series.get_enrollment(aep_target_yyyymm) {
                    *plan_aep_target.entry(nk).or_insert(0) += val as u64;
                }
            }

            // 3. Estimate switching
            // For each plan in base year, where did they go?
            // expected_target[curr_plan] = sum(base[prev_plan] for all prev mapping to curr)
            let mut expected_target: HashMap<String, u64> = HashMap::new();
            for (prev_nk, base_val) in &plan_aep_base {
                if let Some(curr_nks) = crosswalk_map.get(prev_nk) {
                    // Simple split for now if multiple destinations (rare)
                    let share = base_val / curr_nks.len() as u64;
                    for curr_nk in curr_nks {
                        *expected_target.entry(curr_nk.clone()).or_insert(0) += share;
                    }
                }
            }

            let mut org_results: HashMap<String, (i64, i64)> = HashMap::new(); // org -> (total_growth, true_switching)

            for (nk, actual_val) in &plan_aep_target {
                let expected_val = expected_target.get(nk).cloned().unwrap_or(0);
                let switching_component = *actual_val as i64 - expected_val as i64;
                
                if let Some(org) = nk_to_org.get(nk) {
                    let entry = org_results.entry(org.clone()).or_insert((0, 0));
                    let base_val = plan_aep_base.get(nk).cloned().unwrap_or(0);
                    entry.0 += *actual_val as i64 - base_val as i64;
                    entry.1 += switching_component;
                }
            }

            let mut results_list = Vec::new();
            for (org, (growth, switching)) in org_results {
                results_list.push(serde_json::json!({
                    "organization": org,
                    "aepGrowth": growth,
                    "estimatedSwitching": switching,
                }));
            }
            results_list.sort_by_key(|r| std::cmp::Reverse(r["estimatedSwitching"].as_i64().unwrap_or(0)));

            return Ok(serde_json::json!({
                "year": year,
                "results": results_list
            }));
        }
        
        Err(anyhow::anyhow!("Binary cache required."))
    }
}
