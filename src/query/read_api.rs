use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use crate::model::{PlanDim, YearMonth, PlanCountySeries, CountyDim};
use crate::storage;

pub struct QueryEngine {
    pub cache_enabled: bool,
    pub plan_lookup: Option<HashMap<u32, PlanDim>>,
    pub county_lookup: Option<HashMap<u32, CountyDim>>,
    pub series_cache: Option<HashMap<(u32, u32), PlanCountySeries>>,
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
        let (latest_yyyymm, prior_yyyymm) = Self::load_default_months(store_dir);

        if let Some(raw) = county_lookup_raw {
            let mut optimized = HashMap::new();
            for c in raw.into_values() {
                optimized.insert(c.county_key, c);
            }
            county_lookup = Some(optimized);
        }

        Self {
            cache_enabled,
            plan_lookup,
            county_lookup,
            series_cache,
            latest_yyyymm,
            prior_yyyymm,
        }
    }

    fn load_default_months(store_dir: &Path) -> (u32, u32) {
        let manifest_path = store_dir.join("manifests").join("months.json");
        let mut months = storage::manifests::load_manifest(&manifest_path)
            .map(|manifest| {
                manifest
                    .ingested_months
                    .into_iter()
                    .map(|m| m.to_yyyymm())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        months.sort_unstable();
        months.dedup();

        let latest = *months.last().unwrap_or(&0);
        let prior = if months.len() >= 2 { months[months.len() - 2] } else { 0 };
        (latest, prior)
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

    fn matches_static_filters(&self, plan: &PlanDim, filters: &serde_json::Value) -> bool {
        if let Some(sel_orgs) = filters["parentOrgs"].as_array() {
            if !sel_orgs.is_empty() {
                let norm_plan_org = self.normalize_org_name(&plan.parent_org);
                let mut found = false;
                for v in sel_orgs {
                    if let Some(s) = v.as_str() {
                        if self.normalize_org_name(s) == norm_plan_org {
                            found = true;
                            break;
                        }
                    }
                }
                if !found { return false; }
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

    fn normalize_org_name(&self, name: &str) -> String {
        name.to_lowercase()
            .replace(",", "")
            .replace(".", "")
            .replace(" inc", "")
            .replace(" llc", "")
            .replace(" corp", "")
            .trim()
            .to_string()
    }

    fn yyyymm_from_offset(&self, start_yyyymm: u32, offset: i32) -> u32 {
        let start_year = (start_yyyymm / 100) as i32;
        let start_month = (start_yyyymm % 100) as i32;
        let total_months = (start_month - 1) + offset;
        
        let mut year = start_year + (total_months / 12);
        let mut month = (total_months % 12) + 1;
        
        if month <= 0 {
            month += 12;
            year -= 1;
        }
        
        (year as u32 * 100) + month as u32
    }

    pub fn get_global_trend(&self, current_filters: &serde_json::Value) -> Result<Vec<(u32, u64)>> {
        let mut monthly_totals: HashMap<u32, u64> = HashMap::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            let mut matching_plan_keys = HashSet::new();
            for (key, plan) in plan_lookup {
                if self.matches_static_filters(plan, current_filters) {
                    matching_plan_keys.insert(*key);
                }
            }

            let sel_states: HashSet<String> = current_filters["states"].as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();
            let sel_counties: HashSet<String> = current_filters["counties"].as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            let mut deduplicator: HashMap<u32, HashMap<(String, u32), u32>> = HashMap::new();

            for series in series_cache.values() {
                if !matching_plan_keys.contains(&series.plan_key) { continue; }

                if !sel_states.is_empty() || !sel_counties.is_empty() {
                    let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                    if !sel_states.is_empty() && !sel_states.contains(&county.state_code) { continue; }
                    if !sel_counties.is_empty() && !sel_counties.contains(&county.county_name) { continue; }
                }

                let plan = &plan_lookup[&series.plan_key];
                let nk = format!("{}|{}", plan.contract_id, plan.plan_id);
                let bitmap = series.presence_bitmap;
                let mut pos = 0;

                for i in 0..64 {
                    if (bitmap >> i) & 1 != 0 {
                        let yyyymm = self.yyyymm_from_offset(series.start_month_key, i as i32);
                        if let Some(&enrollment) = series.enrollments.get(pos) {
                            let month_map = deduplicator.entry(yyyymm).or_default();
                            let entry = month_map.entry((nk.clone(), series.county_key)).or_insert(0);
                            if self.is_plan_valid_for_month(plan, yyyymm) || *entry == 0 {
                                *entry = enrollment;
                            }
                        }
                        pos += 1;
                    }
                }
            }

            for (yyyymm, month_data) in deduplicator {
                monthly_totals.insert(yyyymm, month_data.values().map(|&v| v as u64).sum());
            }
        }

        let mut result: Vec<_> = monthly_totals.into_iter().collect();
        result.sort_by_key(|(m, _)| *m);
        Ok(result)
    }

    pub fn get_dashboard_summary(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (current_yyyymm, prior_yyyymm) = self.get_analysis_months(filters);
        let prior_year_yyyymm = current_yyyymm - 100;
        
        let mut monthly_aggregates: HashMap<u32, HashMap<(String, u32), (u32, String, String, String, bool, bool)>> = HashMap::new();
        let mut unique_orgs = HashSet::new();
        let mut unique_orgs_prior_year = HashSet::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = 
           (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            
            let mut matching_plan_keys = HashSet::new();
            for (key, plan) in plan_lookup {
                if self.matches_static_filters(plan, filters) {
                    matching_plan_keys.insert(*key);
                }
            }

            let sel_states: HashSet<String> = filters["states"].as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();
            let sel_counties: HashSet<String> = filters["counties"].as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            for series in series_cache.values() {
                if !matching_plan_keys.contains(&series.plan_key) { continue; }
                
                if !sel_states.is_empty() || !sel_counties.is_empty() {
                    let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                    if !sel_states.is_empty() && !sel_states.contains(&county.state_code) { continue; }
                    if !sel_counties.is_empty() && !sel_counties.contains(&county.county_name) { continue; }
                }

                let plan = &plan_lookup[&series.plan_key];
                let nk = format!("{}|{}", plan.contract_id, plan.plan_id);
                let target_months = [current_yyyymm, prior_yyyymm, prior_year_yyyymm];
                for &m in &target_months {
                    if let Some(val) = series.get_enrollment(m) {
                        let month_map = monthly_aggregates.entry(m).or_default();
                        let key = (nk.clone(), series.county_key);
                        let is_valid = self.is_plan_valid_for_month(plan, m);
                        if is_valid || !month_map.contains_key(&key) {
                            month_map.insert(key, (val, plan.parent_org.clone(), plan.plan_type.clone(), plan.plan_name.clone(), plan.is_egwp, plan.is_snp));
                        }
                    }
                }
            }
        }

        let mut total_enrollment: u64 = 0;
        let mut prior_enrollment: u64 = 0;
        let mut unique_plans = HashSet::new();
        let mut unique_counties = HashSet::new();
        
        let (mut egwp, mut egwp_pdp, mut indiv_nsnp, mut pdp, mut snp_total) = (0u64, 0u64, 0u64, 0u64, 0u64);

        if let Some(current_data) = monthly_aggregates.get(&current_yyyymm) {
            for ((nk, county_key), (val, org, pt, _, is_egwp, is_snp)) in current_data {
                total_enrollment += *val as u64;
                unique_plans.insert(nk.clone());
                unique_counties.insert(*county_key);
                unique_orgs.insert(org.clone());

                let pt_u = pt.to_uppercase();
                if *is_egwp {
                    if pt_u.contains("HMO") || pt_u.contains("PPO") { egwp += *val as u64; }
                    else if pt_u.contains("PRESCRIPTION DRUG") { egwp_pdp += *val as u64; }
                } else if *is_snp {
                    snp_total += *val as u64;
                } else {
                    if pt_u.contains("HMO") || pt_u.contains("PPO") || pt_u.contains("PFFS") { indiv_nsnp += *val as u64; }
                    else if pt_u.contains("PRESCRIPTION DRUG") { pdp += *val as u64; }
                }
            }
        }

        if let Some(prior_data) = monthly_aggregates.get(&prior_yyyymm) {
            prior_enrollment = prior_data.values().map(|(v, ..)| *v as u64).sum();
        }

        if let Some(prior_year_data) = monthly_aggregates.get(&prior_year_yyyymm) {
            for (_, org, ..) in prior_year_data.values() { unique_orgs_prior_year.insert(org.clone()); }
        }

        let (mut dsnp, mut csnp, mut isnp) = (0u64, 0u64, 0u64);
        if let Some(current_data) = monthly_aggregates.get(&current_yyyymm) {
            for (_, (val, _, _, plan_name, _, is_snp)) in current_data {
                if !*is_snp { continue; }
                let name = plan_name.to_uppercase();
                if name.contains("D-SNP") { dsnp += *val as u64; }
                else if name.contains("C-SNP") { csnp += *val as u64; }
                else if name.contains("I-SNP") { isnp += *val as u64; }
            }
        }

        Ok(serde_json::json!({
            "totalEnrollment": total_enrollment, "priorEnrollment": prior_enrollment, "planCount": unique_plans.len(),
            "countyCount": unique_counties.len(), "orgCount": unique_orgs.len(), "orgCountPriorYear": unique_orgs_prior_year.len(),
            "orgChange": unique_orgs.len() as i64 - unique_orgs_prior_year.len() as i64,
            "breakdowns": { "egwp": egwp, "egwp_pdp": egwp_pdp, "individual_non_snp": indiv_nsnp, "pdp": pdp, "snp": { "total": snp_total, "dsnp": dsnp, "csnp": csnp, "isnp": isnp } }
        }))
    }

    pub fn get_explorer_data(&self, payload: &serde_json::Value) -> Result<serde_json::Value> {
        let grain = payload["grain"].as_str().unwrap_or("parentOrg");
        let filters = &payload["filters"];
        let (current_yyyymm, prior_yyyymm) = self.get_analysis_months(filters);
        let mut aggregates: HashMap<String, (u64, u64)> = HashMap::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            let mut matching_plan_keys = HashSet::new();
            for (key, plan) in plan_lookup {
                if self.matches_static_filters(plan, filters) {
                    matching_plan_keys.insert(*key);
                }
            }

            for series in series_cache.values() {
                if !matching_plan_keys.contains(&series.plan_key) { continue; }

                let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
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

                let plan = &plan_lookup[&series.plan_key];
                let agg_key = match grain {
                    "parentOrg" => plan.parent_org.clone(),
                    "contract" => plan.contract_id.clone(),
                    "plan" => format!("{}|{}", plan.contract_id, plan.plan_id),
                    "county" => format!("{}|{}", county.state_code, county.county_name),
                    _ => "Unknown".to_string(),
                };
                
                if let Some(val) = series.get_enrollment(current_yyyymm) {
                    if self.is_plan_valid_for_month(plan, current_yyyymm) { aggregates.entry(agg_key.clone()).or_insert((0, 0)).0 += val as u64; }
                }
                if let Some(val) = series.get_enrollment(prior_yyyymm) {
                    if self.is_plan_valid_for_month(plan, prior_yyyymm) { aggregates.entry(agg_key).or_insert((0, 0)).1 += val as u64; }
                }
            }
        }

        let mut rows: Vec<_> = aggregates.into_iter().filter(|(_, (l, p))| *l > 0 || *p > 0).map(|(name, (latest, prior))| {
            let change = latest as i64 - prior as i64;
            let pct = if prior > 0 { (change as f64 / prior as f64) * 100.0 } else { 0.0 };
            serde_json::json!({ "name": name, "current": latest, "prior": prior, "change": change, "percentChange": pct })
        }).collect();
        rows.sort_by_key(|r| std::cmp::Reverse(r["current"].as_u64().unwrap_or(0)));
        Ok(serde_json::json!({ "grain": grain, "latestMonth": current_yyyymm, "priorMonth": prior_yyyymm, "rows": rows }))
    }

    pub fn get_filter_options(&self, current_filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (analysis_yyyymm, _) = self.get_analysis_months(current_filters);
        let mut orgs = HashSet::new();
        let mut contracts = HashSet::new();
        let mut plan_types = HashSet::new();
        let mut states = HashSet::new();
        let mut counties = HashSet::new();

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = 
           (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            
            let mut matching_plan_keys = HashSet::new();
            for (key, plan) in plan_lookup {
                if self.matches_static_filters(plan, current_filters) {
                    matching_plan_keys.insert(*key);
                }
            }

            let sel_states: HashSet<String> = current_filters["states"].as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();
            let sel_counties: HashSet<String> = current_filters["counties"].as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            for series in series_cache.values() {
                if !matching_plan_keys.contains(&series.plan_key) { continue; }

                if let Some(val) = series.get_enrollment(analysis_yyyymm) {
                    if val > 0 {
                        let plan = &plan_lookup[&series.plan_key];
                        if !self.is_plan_valid_for_month(plan, analysis_yyyymm) { continue; }

                        let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                        
                        let matches_state = sel_states.is_empty() || sel_states.contains(&county.state_code);
                        let matches_county = sel_counties.is_empty() || sel_counties.contains(&county.county_name);

                        if matches_state && matches_county {
                            orgs.insert(plan.parent_org.clone());
                            contracts.insert(plan.contract_id.clone());
                            plan_types.insert(plan.plan_type.clone());
                        }

                        if matches_county { states.insert(county.state_code.clone()); }
                        if matches_state { counties.insert(county.county_name.clone()); }
                    }
                }
            }
        }

        let map_to_options = |set: HashSet<String>| -> Vec<serde_json::Value> {
            let mut list: Vec<_> = set.into_iter().collect();
            list.sort();
            list.into_iter().map(|s| serde_json::json!({ "label": s, "value": s })).collect()
        };

        Ok(serde_json::json!({
            "parentOrgs": map_to_options(orgs),
            "contracts": map_to_options(contracts),
            "planTypes": map_to_options(plan_types),
            "states": map_to_options(states),
            "counties": map_to_options(counties),
            "plans": []
        }))
    }

    pub fn get_top_movers(&self, filters: &serde_json::Value, month_a: YearMonth, month_b: YearMonth, limit: usize) -> Result<serde_json::Value> {
        let yyyymm_a = month_a.to_yyyymm();
        let yyyymm_b = month_b.to_yyyymm();

        if let (Some(series_cache), Some(county_lookup), Some(plan_lookup)) =
           (&self.series_cache, &self.county_lookup, &self.plan_lookup) {

            let mut matching_plan_keys = HashSet::new();
            for (key, plan) in plan_lookup {
                if self.matches_static_filters(plan, filters) {
                    matching_plan_keys.insert(*key);
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
                if !matching_plan_keys.contains(&series.plan_key) { continue; }

                if !sel_states.is_empty() || !sel_counties.is_empty() {
                    let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                    if !sel_states.is_empty() && !sel_states.contains(&county.state_code) { continue; }
                    if !sel_counties.is_empty() && !sel_counties.contains(&county.county_name) { continue; }
                }

                let plan = &plan_lookup[&series.plan_key];
                let nk = format!("{}|{}", plan.contract_id, plan.plan_id);
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

            let mut increases = Vec::new();
            let mut decreases = Vec::new();

            for (nk, (change, prior)) in nk_data {
                if change == 0 { continue; }
                if let Some((cid, pid, name)) = nk_info.get(&nk) {
                    let item = serde_json::json!([cid, pid, name, change, prior]);
                    if change > 0 { increases.push((change, item)); }
                    else { decreases.push((change, item)); }
                }
            }

            increases.sort_by_key(|(c, _)| std::cmp::Reverse(*c));
            decreases.sort_by_key(|(c, _)| *c);

            return Ok(serde_json::json!({
                "increases": increases.into_iter().take(limit).map(|(_, v)| v).collect::<Vec<_>>(),
                "decreases": decreases.into_iter().take(limit).map(|(_, v)| v).collect::<Vec<_>>(),
            }));
        }
        Err(anyhow::anyhow!("Binary cache required."))
    }

    pub fn get_org_analysis(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (current_yyyymm, _) = self.get_analysis_months(filters);
        let mut org_data: HashMap<String, (u64, HashMap<u32, u64>)> = HashMap::new(); 
        let mut total_market: u64 = 0;

        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            let mut matching_plan_keys = HashSet::new();
            for (key, plan) in plan_lookup {
                if self.matches_static_filters(plan, filters) { matching_plan_keys.insert(*key); }
            }

            for series in series_cache.values() {
                if !matching_plan_keys.contains(&series.plan_key) { continue; }

                let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                if let Some(sel_s) = filters["states"].as_array() {
                    if !sel_s.is_empty() {
                        let states: HashSet<String> = sel_s.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                        if !states.contains(&county.state_code) { continue; }
                    }
                }

                let plan = &plan_lookup[&series.plan_key];
                let bitmap = series.presence_bitmap;
                for i in 0..64 {
                    if (bitmap >> i) & 1 != 0 {
                        let yyyymm = self.yyyymm_from_offset(series.start_month_key, i as i32);
                        if let Some(&val) = series.enrollments.get(i as usize) {
                            if self.is_plan_valid_for_month(plan, yyyymm) {
                                let entry = org_data.entry(plan.parent_org.clone()).or_insert((0, HashMap::new()));
                                *entry.1.entry(yyyymm).or_insert(0) += val as u64;
                                if yyyymm == current_yyyymm { entry.0 += val as u64; total_market += val as u64; }
                            }
                        }
                    }
                }
            }
        }
        let mut list: Vec<_> = org_data.into_iter().map(|(name, (latest, trend_map))| {
            let mut trend: Vec<_> = trend_map.into_iter().collect(); trend.sort_by_key(|(m, _)| *m);
            serde_json::json!({ "name": name, "enrollment": latest, "marketShare": if total_market > 0 { (latest as f64 / total_market as f64) * 100.0 } else { 0.0 }, "trend": trend.into_iter().map(|(m, v)| serde_json::json!({ "month": m, "value": v })).collect::<Vec<_>>() })
        }).collect();
        list.sort_by_key(|o| std::cmp::Reverse(o["enrollment"].as_u64().unwrap_or(0)));
        Ok(serde_json::json!({ "totalMarketEnrollment": total_market, "latestMonth": current_yyyymm, "organizations": list }))
    }

    pub fn get_geo_analysis(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (curr_m, _) = self.get_analysis_months(filters);
        let (mut state_data, mut county_data) = (HashMap::<String, u64>::new(), HashMap::<String, u64>::new());
        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            let mut matching_plan_keys = HashSet::new();
            for (key, plan) in plan_lookup { if self.matches_static_filters(plan, filters) { matching_plan_keys.insert(*key); } }
            for series in series_cache.values() {
                if !matching_plan_keys.contains(&series.plan_key) { continue; }
                if let Some(county) = county_lookup.get(&series.county_key) {
                    if let Some(val) = series.get_enrollment(curr_m) {
                        let plan = &plan_lookup[&series.plan_key];
                        if self.is_plan_valid_for_month(plan, curr_m) {
                            *state_data.entry(county.state_code.clone()).or_insert(0) += val as u64;
                            *county_data.entry(format!("{}|{}", county.state_code, county.county_name)).or_insert(0) += val as u64;
                        }
                    }
                }
            }
        }
        let mut sl: Vec<_> = state_data.into_iter().map(|(n, e)| serde_json::json!({ "name": n, "enrollment": e })).collect(); sl.sort_by_key(|s| std::cmp::Reverse(s["enrollment"].as_u64().unwrap_or(0)));
        let mut cl: Vec<_> = county_data.into_iter().map(|(k, e)| { let p: Vec<&str> = k.split('|').collect(); serde_json::json!({ "state": p[0], "name": p[1], "enrollment": e }) }).collect(); cl.sort_by_key(|c| std::cmp::Reverse(c["enrollment"].as_u64().unwrap_or(0)));
        Ok(serde_json::json!({ "latestMonth": curr_m, "states": sl, "counties": cl.into_iter().take(50).collect::<Vec<_>>() }))
    }

    pub fn get_growth_analytics(&self, filters: &serde_json::Value) -> Result<serde_json::Value> {
        let (curr_m, pri_m) = self.get_analysis_months(filters);
        if let (Some(plan_lookup), Some(county_lookup), Some(series_cache)) = (&self.plan_lookup, &self.county_lookup, &self.series_cache) {
            let year = curr_m / 100;
            let (aep_t, aep_b) = ((year * 100) + 2, ((year - 1) * 100) + 12);
            let mut matching_plan_keys = HashSet::new();
            for (key, plan) in plan_lookup { if self.matches_static_filters(plan, filters) { matching_plan_keys.insert(*key); } }
            let mut plan_aggregates: HashMap<String, (u64, u64, u64, u64, String, String, String)> = HashMap::new();
            for series in series_cache.values() {
                if !matching_plan_keys.contains(&series.plan_key) { continue; }
                let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                if let Some(sel_s) = filters["states"].as_array() { if !sel_s.is_empty() { let states: HashSet<String> = sel_s.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect(); if !states.contains(&county.state_code) { continue; } } }
                
                let plan = &plan_lookup[&series.plan_key];
                let nk = format!("{}|{}", plan.contract_id, plan.plan_id);
                let cur = if self.is_plan_valid_for_month(plan, curr_m) { series.get_enrollment(curr_m).unwrap_or(0) } else { 0 };
                let pri = if self.is_plan_valid_for_month(plan, pri_m) { series.get_enrollment(pri_m).unwrap_or(0) } else { 0 };
                let a_t = if self.is_plan_valid_for_month(plan, aep_t) { series.get_enrollment(aep_t).unwrap_or(0) } else { 0 };
                let a_b = if self.is_plan_valid_for_month(plan, aep_b) { series.get_enrollment(aep_b).unwrap_or(0) } else { 0 };
                if cur == 0 && pri == 0 && a_t == 0 && a_b == 0 { continue; }
                let e = plan_aggregates.entry(nk).or_insert((0, 0, 0, 0, plan.plan_name.clone(), plan.contract_id.clone(), plan.plan_id.clone()));
                e.0 += cur as u64; e.1 += pri as u64; e.2 += a_t as u64; e.3 += a_b as u64;
            }
            let mut hf: Vec<_> = plan_aggregates.into_iter().filter(|(_, (l, ..))| *l > 500).map(|(_, (l, p, at, ab, n, cid, pid))| {
                let c = l as i64 - p as i64;
                serde_json::json!({ "name": n, "contract": cid, "plan": pid, "current": l, "change": c, "percent": if p > 0 { (c as f64 / p as f64) * 100.0 } else { 0.0 }, "aepChange": at as i64 - ab as i64 })
            }).filter(|h| h["percent"].as_f64().unwrap_or(0.0) > 5.0 || h["change"].as_i64().unwrap_or(0).abs() > 1000).collect();
            hf.sort_by_key(|h| std::cmp::Reverse((h["percent"].as_f64().unwrap_or(0.0) * 100.0) as i64));
            return Ok(serde_json::json!({ "latestMonth": curr_m, "priorMonth": pri_m, "highFlyers": hf.into_iter().take(20).collect::<Vec<_>>() }));
        }
        Err(anyhow::anyhow!("Binary cache required."))
    }

    pub fn get_plan_key(&self, contract_id: &str, plan_id: &str) -> Result<Option<u32>> {
        if let Some(lookup) = &self.plan_lookup {
            let p = lookup.values().find(|p| p.contract_id == contract_id && p.plan_id == plan_id && p.is_current);
            return Ok(p.map(|p| p.plan_key));
        }
        Ok(None)
    }

    pub fn get_plan_trend(&self, plan_key: u32) -> Result<Vec<(u32, u32)>> {
        if let Some(cache) = &self.series_cache {
            let mut trend: HashMap<u32, u32> = HashMap::new();
            for series in cache.values() {
                if series.plan_key == plan_key {
                    let bitmap = series.presence_bitmap;
                    for i in 0..64 {
                        if (bitmap >> i) & 1 != 0 {
                            let yyyymm = self.yyyymm_from_offset(series.start_month_key, i as i32);
                            if let Some(&val) = series.enrollments.get(i as usize) {
                                *trend.entry(yyyymm).or_insert(0) += val;
                            }
                        }
                    }
                }
            }
            let mut result: Vec<_> = trend.into_iter().collect();
            result.sort_by_key(|(m, _)| *m);
            return Ok(result);
        }
        Err(anyhow::anyhow!("Binary cache required"))
    }

    pub fn get_county_key(&self, state: &str, county_name: &str) -> Result<Option<u32>> {
        if let Some(lookup) = &self.county_lookup {
            let s_up = state.to_uppercase();
            let c_up = county_name.to_uppercase();
            let c = lookup.values().find(|c| c.state_code.to_uppercase() == s_up && c.county_name.to_uppercase() == c_up);
            return Ok(c.map(|c| c.county_key));
        }
        Ok(None)
    }

    pub fn get_county_snapshot(&self, county_key: u32, month: crate::model::YearMonth) -> Result<Vec<(String, String, String, u32)>> {
        let yyyymm = month.to_yyyymm();
        if let (Some(cache), Some(plan_lookup)) = (&self.series_cache, &self.plan_lookup) {
            let mut snapshot = Vec::new();
            for series in cache.values() {
                if series.county_key == county_key {
                    if let Some(enrollment) = series.get_enrollment(yyyymm) {
                        if let Some(plan) = plan_lookup.get(&series.plan_key) {
                            if self.is_plan_valid_for_month(plan, yyyymm) {
                                snapshot.push((plan.contract_id.clone(), plan.plan_id.clone(), plan.plan_name.clone(), enrollment));
                            }
                        }
                    }
                }
            }
            snapshot.sort_by_key(|s| std::cmp::Reverse(s.3));
            return Ok(snapshot);
        }
        Err(anyhow::anyhow!("Binary cache required"))
    }

    pub fn get_state_rollup(&self, state: &str, from: crate::model::YearMonth, to: crate::model::YearMonth) -> Result<Vec<(u32, u64)>> {
        let start_ym = from.to_yyyymm();
        let end_ym = to.to_yyyymm();
        let s_up = state.to_uppercase();

        if let (Some(cache), Some(county_lookup), Some(plan_lookup)) = (&self.series_cache, &self.county_lookup, &self.plan_lookup) {
            let mut rollup: HashMap<u32, u64> = HashMap::new();
            for series in cache.values() {
                let county = match county_lookup.get(&series.county_key) { Some(c) => c, None => continue };
                if county.state_code.to_uppercase() != s_up { continue; }
                
                let plan = match plan_lookup.get(&series.plan_key) { Some(p) => p, None => continue };

                let bitmap = series.presence_bitmap;
                for i in 0..64 {
                    if (bitmap >> i) & 1 != 0 {
                        let yyyymm = self.yyyymm_from_offset(series.start_month_key, i as i32);
                        if yyyymm >= start_ym && yyyymm <= end_ym {
                            if self.is_plan_valid_for_month(plan, yyyymm) {
                                if let Some(&val) = series.enrollments.get(i as usize) {
                                    *rollup.entry(yyyymm).or_insert(0) += val as u64;
                                }
                            }
                        }
                    }
                }
            }
            let mut result: Vec<_> = rollup.into_iter().collect();
            result.sort_by_key(|(m, _)| *m);
            return Ok(result);
        }
        Err(anyhow::anyhow!("Binary cache required"))
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
                if let Some(c) = county_lookup.get(&series.county_key) {
                    footprint.push(serde_json::json!({ "state": c.state_code, "county": c.county_name, "enrollment": series.enrollments.last().cloned().unwrap_or(0) }));
                    let bitmap = series.presence_bitmap;
                    for i in 0..64 {
                        if (bitmap >> i) & 1 != 0 {
                            let yyyymm = self.yyyymm_from_offset(series.start_month_key, i as i32);
                            if let Some(&val) = series.enrollments.get(i as usize) { *global_trend.entry(yyyymm).or_insert(0) += val as u64; }
                        }
                    }
                }
            }
        }
        footprint.sort_by_key(|f| std::cmp::Reverse(f["enrollment"].as_u64().unwrap_or(0)));
        let mut tl: Vec<_> = global_trend.into_iter().collect(); tl.sort_by_key(|(m, _)| *m);
        Ok(serde_json::json!({ "metadata": { "name": plan.plan_name, "contract_id": plan.contract_id, "plan_id": plan.plan_id, "org": plan.parent_org, "type": plan.plan_type, "egwp": plan.is_egwp, "snp": plan.is_snp }, "footprint": footprint, "trend": tl.into_iter().map(|(m, v)| serde_json::json!({ "month": m, "value": v })).collect::<Vec<_>>() }))
    }
}
