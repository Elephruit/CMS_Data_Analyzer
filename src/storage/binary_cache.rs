use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use crate::model::{PlanDim, CountyDim, PlanCountySeries};
use std::io::{Read, Write};

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
