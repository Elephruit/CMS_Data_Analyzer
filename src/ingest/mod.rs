pub mod normalize;
pub mod resolver;

use anyhow::Result;
use crate::model::{YearMonth, PlanCountySeries, NormalizedRow};
use crate::cms;
use crate::storage;
use crate::util;
use std::path::Path;
use std::collections::HashMap;

pub async fn ingest_month(month: YearMonth, force: bool, store_dir: &Path) -> Result<()> {
    let manifest_path = store_dir.join("manifests").join("months.json");
    let mut manifest = storage::manifests::load_manifest(&manifest_path)?;

    if !force && manifest.ingested_months.contains(&month) {
        log::info!("Month {} already ingested, skipping. Use --force to re-ingest.", month);
        return Ok(());
    }

    let source_info = cms::discover::discover_month(month).await?;
    let zip_bytes = cms::download::download_zip(&source_info.zip_url).await?;
    let hash = util::hashing::compute_sha256(&zip_bytes);
    
    let extracted = cms::parse::extract_zip(&zip_bytes)?;
    
    // 1. Parse Contract Metadata
    let (_, contract_file_content) = cms::parse::detect_contract_file(&extracted)?;
    let mut contract_rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(encoding_rs_io::DecodeReaderBytes::new(contract_file_content.as_slice()));
    
    let contract_headers = contract_rdr.headers()?.clone();
    let contract_map = normalize::map_contract_headers(&contract_headers)?;
    
    let mut plan_metadata = HashMap::new();
    let mut byte_record = csv::ByteRecord::new();
    while contract_rdr.read_byte_record(&mut byte_record)? {
        let cid = String::from_utf8_lossy(byte_record.get(contract_map.contract_id_idx).unwrap_or(b"")).trim().to_string();
        let pid = String::from_utf8_lossy(byte_record.get(contract_map.plan_id_idx).unwrap_or(b"")).trim().to_string();
        
        let name = String::from_utf8_lossy(byte_record.get(contract_map.plan_name_idx).unwrap_or(b"")).trim().to_string();
        let parent_org = String::from_utf8_lossy(byte_record.get(contract_map.parent_org_idx).unwrap_or(b"")).trim().to_string();
        let plan_type = String::from_utf8_lossy(byte_record.get(contract_map.plan_type_idx).unwrap_or(b"")).trim().to_string();
        let eghp_str = String::from_utf8_lossy(byte_record.get(contract_map.eghp_idx).unwrap_or(b"")).to_uppercase();
        let snp_str = String::from_utf8_lossy(byte_record.get(contract_map.snp_idx).unwrap_or(b"")).to_uppercase();

        if !cid.is_empty() && !pid.is_empty() {
            plan_metadata.insert((cid, pid), normalize::PlanMetadata {
                name,
                parent_org,
                plan_type,
                is_egwp: eghp_str == "Y" || eghp_str == "YES",
                is_snp: snp_str == "Y" || snp_str == "YES",
            });
        }
    }
    log::info!("Loaded {} plan metadata records", plan_metadata.len());

    // 2. Parse Enrollment Info
    let (_, enroll_file_content) = cms::parse::detect_enrollment_file(&extracted)?;
    let mut enroll_rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(encoding_rs_io::DecodeReaderBytes::new(enroll_file_content.as_slice()));
    
    let enroll_headers = enroll_rdr.headers()?.clone();
    let enroll_map = normalize::map_enrollment_headers(&enroll_headers)?;

    let mut stats = normalize::RowStats::default();
    let mut rows_by_state: HashMap<String, Vec<NormalizedRow>> = HashMap::new();

    let mut enroll_byte_record = csv::ByteRecord::new();
    while enroll_rdr.read_byte_record(&mut enroll_byte_record)? {
        stats.total_rows += 1;
        match normalize::normalize_enrollment_byte_row(&enroll_byte_record, &enroll_map, &plan_metadata) {
            Ok(Some(row)) => {
                rows_by_state.entry(row.state_code.clone()).or_default().push(row);
                stats.kept_rows += 1;
            }
            Ok(None) => stats.star_rows += 1,
            Err(_) => stats.malformed_rows += 1,
        }
    }

    log::info!("Ingestion complete for {}: Kept={} rows across {} states", month, stats.kept_rows, rows_by_state.len());

    // 3. Resolve Keys
    let plan_dim_path = store_dir.join("dims").join("plan_dim.parquet");
    let county_dim_path = store_dir.join("dims").join("county_dim.parquet");
    let existing_plans = storage::parquet_store::load_plan_dim(&plan_dim_path)?;
    let existing_counties = storage::parquet_store::load_county_dim(&county_dim_path)?;
    let mut resolver_inst = resolver::KeyResolver::from_existing(existing_plans, existing_counties);

    // 4. Merge and Persist
    let month_yyyymm = month.to_yyyymm();
    let start_month_key = manifest.ingested_months.first().map(|m| m.to_yyyymm()).unwrap_or(month_yyyymm);

    for (state, rows) in rows_by_state {
        let year_state_dir = store_dir.join("facts").join(format!("year={}", month.year)).join(format!("state={}", state));
        std::fs::create_dir_all(&year_state_dir)?;
        let series_path = year_state_dir.join("plan_county_series.parquet");

        let existing_series = storage::parquet_store::load_series_partition(&series_path)?;
        let mut series_map: HashMap<(u32, u32), PlanCountySeries> = existing_series.into_iter()
            .map(|s| ((s.plan_key, s.county_key), s)).collect();

        let mut month_accum: HashMap<(u32, u32), u32> = HashMap::new();
        for row in rows {
            let plan_key = resolver_inst.resolve_plan(&row, month);
            let county_key = resolver_inst.resolve_county(&row);
            *month_accum.entry((plan_key, county_key)).or_insert(0) += row.enrollment;
        }

        for ((plan_key, county_key), enrollment) in month_accum {
            let series = series_map.entry((plan_key, county_key)).or_insert_with(|| PlanCountySeries {
                plan_key,
                county_key,
                start_month_key,
                presence_bitmap: 0,
                enrollments: Vec::new(),
            });
            series.set_month(month_yyyymm, enrollment);
        }

        let updated_series: Vec<_> = series_map.into_values().collect();
        storage::parquet_store::save_series_partition(&updated_series, &series_path)?;
    }

    // Save Dimensions
    let plans: Vec<_> = resolver_inst.plans.into_values().collect();
    storage::parquet_store::save_plan_dim(&plans, &plan_dim_path)?;
    let counties: Vec<_> = resolver_inst.counties.into_values().collect();
    storage::parquet_store::save_county_dim(&counties, &county_dim_path)?;

    // Update Manifest
    if !manifest.ingested_months.contains(&month) {
        manifest.ingested_months.push(month);
        manifest.ingested_months.sort();
    }
    manifest.source_hashes.insert(month.to_string(), hash);
    std::fs::create_dir_all(store_dir.join("manifests"))?;
    storage::manifests::save_manifest(&manifest, &manifest_path)?;

    Ok(())
}
