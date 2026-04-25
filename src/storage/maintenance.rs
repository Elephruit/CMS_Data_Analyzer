use std::path::Path;
use std::collections::HashMap;
use anyhow::Result;
use crate::model::{PlanDim, CountyDim, PlanCountySeries};
use crate::storage::{parquet_store, binary_cache};

pub fn repair_dimension(store_dir: &Path) -> Result<()> {
    log::info!("Repairing plan dimension: deduplicating per-month plan versions...");

    let plan_dim_path = store_dir.join("dims").join("plan_dim.parquet");
    let plans = parquet_store::load_plan_dim(&plan_dim_path)?;
    let total_before = plans.len();

    // Group all plan_keys by (natural_key, valid_from_month).
    let mut canonical: HashMap<(String, u32), u32> = HashMap::new();
    for p in &plans {
        let nk = format!("{}|{}", p.contract_id, p.plan_id);
        let entry = canonical.entry((nk, p.valid_from_month)).or_insert(p.plan_key);
        if p.plan_key < *entry {
            *entry = p.plan_key;
        }
    }

    // Build remap: duplicate_plan_key -> canonical_plan_key
    let mut remap: HashMap<u32, u32> = HashMap::new();
    for p in &plans {
        let nk = format!("{}|{}", p.contract_id, p.plan_id);
        let canon = canonical[&(nk, p.valid_from_month)];
        if p.plan_key != canon {
            remap.insert(p.plan_key, canon);
        }
    }

    if !remap.is_empty() {
        // Rewrite plan_dim without duplicates
        let clean_plans: Vec<_> = plans.into_iter().filter(|p| !remap.contains_key(&p.plan_key)).collect();
        parquet_store::save_plan_dim(&clean_plans, &plan_dim_path)?;

        // Remap series parquets
        let facts_dir = store_dir.join("facts");
        if facts_dir.exists() {
            for year_entry in std::fs::read_dir(&facts_dir)? {
                let year_path = year_entry?.path();
                if !year_path.is_dir() { continue; }
                for state_entry in std::fs::read_dir(&year_path)? {
                    let state_path = state_entry?.path();
                    if !state_path.is_dir() { continue; }
                    let series_path = state_path.join("plan_county_series.parquet");
                    let mut series_list = parquet_store::load_series_partition(&series_path)?;
                    let mut changed = false;
                    let mut merged: HashMap<(u32, u32), PlanCountySeries> = HashMap::new();
                    for mut s in series_list.drain(..) {
                        if let Some(&canon_key) = remap.get(&s.plan_key) {
                            s.plan_key = canon_key;
                            changed = true;
                        }
                        let key = (s.plan_key, s.county_key);
                        if let Some(existing) = merged.get_mut(&key) {
                            let bitmap = s.presence_bitmap;
                            let start_year = (s.start_month_key / 100) as i32;
                            let start_month = (s.start_month_key % 100) as i32;
                            let mut pos = 0usize;
                            for i in 0..64u32 {
                                if (bitmap >> i) & 1 != 0 {
                                    let curr_months = (start_month - 1) + i as i32;
                                    let year = start_year + (curr_months / 12);
                                    let month = (curr_months % 12) + 1;
                                    let yyyymm = (year as u32) * 100 + month as u32;
                                    if let Some(&enrollment) = s.enrollments.get(pos) {
                                        existing.add_month(yyyymm, enrollment);
                                    }
                                    pos += 1;
                                }
                            }
                        } else {
                            merged.insert(key, s);
                        }
                    }
                    if changed {
                        let updated: Vec<_> = merged.into_values().collect();
                        parquet_store::save_series_partition(&updated, &series_path)?;
                    }
                }
            }
        }
    }

    // Phase 2: Fix validity chain
    let plans_v2 = parquet_store::load_plan_dim(&plan_dim_path)?;
    let mut by_natural_key: HashMap<String, Vec<PlanDim>> = HashMap::new();
    for p in plans_v2 {
        let nk = format!("{}|{}", p.contract_id, p.plan_id);
        by_natural_key.entry(nk).or_default().push(p);
    }

    let mut fixed_plans: Vec<PlanDim> = Vec::new();
    for (_, mut versions) in by_natural_key {
        versions.sort_by_key(|p| p.valid_from_month);
        let n = versions.len();
        for i in 0..n {
            versions[i].valid_to_month = if i < n - 1 {
                Some(versions[i + 1].valid_from_month)
            } else {
                None
            };
            versions[i].is_current = i == n - 1;
        }
        fixed_plans.extend(versions);
    }

    parquet_store::save_plan_dim(&fixed_plans, &plan_dim_path)?;
    log::info!("Repair complete: {} -> {} plans", total_before, fixed_plans.len());
    Ok(())
}

pub fn rebuild_cache(store_dir: &Path) -> Result<()> {
    let cache_dir = store_dir.join("cache");
    std::fs::create_dir_all(&cache_dir)?;

    // 1. Plan Lookup
    let plan_dim_path = store_dir.join("dims").join("plan_dim.parquet");
    let plans = parquet_store::load_plan_dim(&plan_dim_path)?;
    let plan_map: HashMap<u32, PlanDim> = plans.into_iter().map(|p| (p.plan_key, p)).collect();
    binary_cache::save_plan_lookup(&plan_map, &cache_dir.join("plan_lookup.bin"))?;

    // 2. County Lookup
    let county_dim_path = store_dir.join("dims").join("county_dim.parquet");
    let counties = parquet_store::load_county_dim(&county_dim_path)?;
    let county_map: HashMap<String, CountyDim> = counties.into_iter().map(|c| (format!("{}|{}", c.state_code, c.county_name), c)).collect();
    binary_cache::save_county_lookup(&county_map, &cache_dir.join("county_lookup.bin"))?;

    // 3. Series Cache
    let facts_dir = store_dir.join("facts");
    let mut all_series: HashMap<(u32, u32), PlanCountySeries> = HashMap::new();
    if facts_dir.exists() {
        let mut year_paths: Vec<_> = std::fs::read_dir(&facts_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_dir())
            .collect();
        year_paths.sort();
        for year_path in year_paths {
            let mut state_paths: Vec<_> = std::fs::read_dir(&year_path)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.is_dir())
                .collect();
            state_paths.sort();
            for state_path in state_paths {
                let series_path = state_path.join("plan_county_series.parquet");
                let series_list = parquet_store::load_series_partition(&series_path)?;
                for s in series_list {
                    let key = (s.plan_key, s.county_key);
                    if let Some(existing) = all_series.get_mut(&key) {
                        let bitmap = s.presence_bitmap;
                        let start_year = (s.start_month_key / 100) as i32;
                        let start_month = (s.start_month_key % 100) as i32;
                        let mut pos = 0usize;
                        for i in 0..64u32 {
                            if (bitmap >> i) & 1 != 0 {
                                let curr_months = (start_month - 1) + i as i32;
                                let year = start_year + (curr_months / 12);
                                let month = (curr_months % 12) + 1;
                                let yyyymm = (year as u32) * 100 + month as u32;
                                if let Some(&enrollment) = s.enrollments.get(pos) {
                                    existing.add_month(yyyymm, enrollment);
                                }
                                pos += 1;
                            }
                        }
                    } else {
                        all_series.insert(key, s);
                    }
                }
            }
        }
    }
    binary_cache::save_series_cache(&all_series, &cache_dir.join("series_values.bin"))?;
    log::info!("Cache rebuilt with {} series", all_series.len());
    Ok(())
}
