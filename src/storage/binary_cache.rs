use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use crate::model::{PlanDim, CountyDim, PlanCountySeries};

pub fn save_plan_lookup(plans: &HashMap<u32, PlanDim>, path: &Path) -> Result<()> {
    let file = File::create(path)?;
    let mut encoder = zstd::stream::Encoder::new(file, 3)?;
    bincode::serialize_into(&mut encoder, plans)?;
    encoder.finish()?;
    Ok(())
}

pub fn save_county_lookup(counties: &HashMap<String, CountyDim>, path: &Path) -> Result<()> {
    let file = File::create(path)?;
    let mut encoder = zstd::stream::Encoder::new(file, 3)?;
    bincode::serialize_into(&mut encoder, counties)?;
    encoder.finish()?;
    Ok(())
}

pub fn save_series_cache(series: &HashMap<(u32, u32), PlanCountySeries>, path: &Path) -> Result<()> {
    let file = File::create(path)?;
    let mut encoder = zstd::stream::Encoder::new(file, 3)?;
    bincode::serialize_into(&mut encoder, series)?;
    encoder.finish()?;
    Ok(())
}

pub fn load_plan_lookup(path: &Path) -> Result<HashMap<u32, PlanDim>> {
    let file = File::open(path)?;
    let mut decoder = zstd::stream::Decoder::new(file)?;
    let plans = bincode::deserialize_from(&mut decoder)?;
    Ok(plans)
}

pub fn load_county_lookup(path: &Path) -> Result<HashMap<String, CountyDim>> {
    let file = File::open(path)?;
    let mut decoder = zstd::stream::Decoder::new(file)?;
    let counties = bincode::deserialize_from(&mut decoder)?;
    Ok(counties)
}

pub fn load_series_cache(path: &Path) -> Result<HashMap<(u32, u32), PlanCountySeries>> {
    let file = File::open(path)?;
    let mut decoder = zstd::stream::Decoder::new(file)?;
    let series = bincode::deserialize_from(&mut decoder)?;
    Ok(series)
}

use crate::storage;

pub fn rebuild_cache(store_dir: &Path) -> Result<()> {
    log::info!("Rebuilding cache");
    let cache_dir = store_dir.join("cache");
    std::fs::create_dir_all(&cache_dir)?;

    let mut plan_map = HashMap::new();
    let plan_dim_path = store_dir.join("dims").join("plan_dim.parquet");
    if plan_dim_path.exists() {
        let plans = storage::parquet_store::load_plan_dim(&plan_dim_path)?;
        for p in plans {
            plan_map.insert(p.plan_key, p);
        }
    }
    save_plan_lookup(&plan_map, &cache_dir.join("plan_lookup.bin"))?;
    log::info!("Cached {} plans", plan_map.len());

    let mut county_map = HashMap::new();
    let county_dim_path = store_dir.join("dims").join("county_dim.parquet");
    if county_dim_path.exists() {
        let counties = storage::parquet_store::load_county_dim(&county_dim_path)?;
        for c in counties {
            let key = format!("{}|{}", c.state_code, c.county_name);
            county_map.insert(key, c);
        }
    }
    save_county_lookup(&county_map, &cache_dir.join("county_lookup.bin"))?;
    log::info!("Cached {} counties", county_map.len());

    let mut all_series: HashMap<(u32, u32), PlanCountySeries> = HashMap::new();
    let facts_dir = store_dir.join("facts");
    if facts_dir.exists() {
        let mut year_paths: Vec<_> = std::fs::read_dir(facts_dir)?
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
                if !series_path.exists() { continue; }
                let series_list = storage::parquet_store::load_series_partition(&series_path)?;
                for new_s in series_list {
                    let key = (new_s.plan_key, new_s.county_key);
                    if let Some(existing) = all_series.get_mut(&key) {
                        let bitmap = new_s.presence_bitmap;
                        let start_year = (new_s.start_month_key / 100) as i32;
                        let start_month = (new_s.start_month_key % 100) as i32;
                        let mut pos = 0usize;
                        for i in 0..64u32 {
                            if (bitmap >> i) & 1 != 0 {
                                let curr = start_month - 1 + i as i32;
                                let year = start_year + curr / 12;
                                let month = curr % 12 + 1;
                                let yyyymm = (year as u32) * 100 + month as u32;
                                if let Some(&enrollment) = new_s.enrollments.get(pos) {
                                    existing.add_month(yyyymm, enrollment);
                                }
                                pos += 1;
                            }
                        }
                    } else {
                        all_series.insert(key, new_s);
                    }
                }
            }
        }
    }
    save_series_cache(&all_series, &cache_dir.join("series_values.bin"))?;
    log::info!("Cached {} series", all_series.len());

    Ok(())
}
