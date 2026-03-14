pub mod normalize;
pub mod resolver;

use anyhow::Result;
use crate::model::YearMonth;
use crate::cms;
use crate::storage;
use crate::util;
use std::path::Path;

pub async fn ingest_month(month: YearMonth, force: bool, store_dir: &Path) -> Result<()> {
    let manifest_path = store_dir.join("manifests").join("months.json");
    let mut manifest = storage::manifests::load_manifest(&manifest_path)?;

    if !force && manifest.ingested_months.contains(&month) {
        log::info!("Month {} already ingested, skipping. Use --force to re-ingest.", month);
        return Ok(());
    }

    let source_info = cms::discover::discover_month(month).await?;
    log::info!("Discovered ZIP at: {}", source_info.zip_url);

    let zip_bytes = cms::download::download_zip(&source_info.zip_url).await?;
    let hash = util::hashing::compute_sha256(&zip_bytes);
    log::info!("Downloaded ZIP with hash: {}", hash);
    
    // Extract ZIP
    let extracted = cms::parse::extract_zip(&zip_bytes)?;
    
    // 1. Parse Contract Metadata for Plan Names
    let (contract_file_name, contract_file_content) = cms::parse::detect_contract_file(&extracted)?;
    log::info!("Parsing contract metadata from {}", contract_file_name);
    
    let mut contract_rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(encoding_rs_io::DecodeReaderBytes::new(contract_file_content.as_slice()));
    
    let contract_headers = contract_rdr.headers()?.clone();
    let contract_map = normalize::map_contract_headers(&contract_headers)?;
    
    let mut plan_names = std::collections::HashMap::new();
    let mut byte_record = csv::ByteRecord::new();
    while contract_rdr.read_byte_record(&mut byte_record)? {
        let cid = String::from_utf8_lossy(byte_record.get(contract_map.contract_id_idx).unwrap_or(b"")).trim().to_string();
        let pid = String::from_utf8_lossy(byte_record.get(contract_map.plan_id_idx).unwrap_or(b"")).trim().to_string();
        let name = String::from_utf8_lossy(byte_record.get(contract_map.plan_name_idx).unwrap_or(b"")).trim().to_string();
        if !cid.is_empty() && !pid.is_empty() {
            plan_names.insert((cid, pid), name);
        }
    }
    log::info!("Loaded {} plan names from metadata", plan_names.len());

    // 2. Parse Enrollment Info
    let (enroll_file_name, enroll_file_content) = cms::parse::detect_enrollment_file(&extracted)?;
    log::info!("Parsing enrollment from {}", enroll_file_name);

    let mut enroll_rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(enroll_file_content.as_slice());
    
    let enroll_headers = enroll_rdr.headers()?.clone();
    let enroll_map = normalize::map_enrollment_headers(&enroll_headers)?;

    let mut stats = normalize::RowStats::default();
    let mut normalized_rows = Vec::new();

    let mut enroll_byte_record = csv::ByteRecord::new();
    while enroll_rdr.read_byte_record(&mut enroll_byte_record)? {
        stats.total_rows += 1;
        
        match normalize::normalize_enrollment_byte_row(&enroll_byte_record, &enroll_map, &plan_names) {
            Ok(Some(row)) => {
                normalized_rows.push(row);
                stats.kept_rows += 1;
            }
            Ok(None) => {
                stats.star_rows += 1;
            }
            Err(_) => {
                stats.malformed_rows += 1;
            }
        }
    }

    log::info!("Ingestion complete for {}: Total={}, Kept={}, Starred={}, Malformed={}", 
        month, stats.total_rows, stats.kept_rows, stats.star_rows, stats.malformed_rows);

    // 3. Key Resolution / Deduplication
    let dims_dir = store_dir.join("dims");
    std::fs::create_dir_all(&dims_dir)?;

    let plan_dim_path = dims_dir.join("plan_dim.parquet");
    let county_dim_path = dims_dir.join("county_dim.parquet");

    let existing_plans = storage::parquet_store::load_plan_dim(&plan_dim_path)?;
    let existing_counties = storage::parquet_store::load_county_dim(&county_dim_path)?;
    
    let mut resolver_inst = resolver::KeyResolver::from_existing(existing_plans, existing_counties);
    
    let mut enrollment_map = std::collections::HashMap::new();
    for row in normalized_rows {
        let plan_key = resolver_inst.resolve_plan(&row, month);
        let county_key = resolver_inst.resolve_county(&row);
        enrollment_map.insert((plan_key, county_key), row.enrollment);
    }
    log::info!("Resolved {} unique plan-county pairs", enrollment_map.len());

    // 4. Persistence
    let plans: Vec<_> = resolver_inst.plans.values().cloned().collect();
    storage::parquet_store::save_plan_dim(&plans, &plan_dim_path)?;
    log::info!("Saved {} plans to {}", plans.len(), plan_dim_path.display());

    let counties: Vec<_> = resolver_inst.counties.values().cloned().collect();
    storage::parquet_store::save_county_dim(&counties, &county_dim_path)?;
    log::info!("Saved {} counties to {}", counties.len(), county_dim_path.display());

    let year_dir = store_dir.join("facts").join(format!("year={}", month.year));
    std::fs::create_dir_all(&year_dir)?;
    let series_path = year_dir.join(format!("enrollment_{}.parquet", month.month));
    storage::parquet_store::save_series(&enrollment_map, month, &series_path)?;
    log::info!("Saved enrollment series to {}", series_path.display());

    // Update Manifest
    if !manifest.ingested_months.contains(&month) {
        manifest.ingested_months.push(month);
        manifest.ingested_months.sort();
    }
    manifest.source_hashes.insert(month.to_string(), hash);
    std::fs::create_dir_all(store_dir.join("manifests"))?;
    storage::manifests::save_manifest(&manifest, &manifest_path)?;
    log::info!("Updated manifest at {}", manifest_path.display());

    Ok(())
}
