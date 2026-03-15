use anyhow::Result;
use std::path::Path;
use std::fs::File;
use std::io::Read;
use zip::ZipArchive;
use calamine::{open_workbook_auto_from_rs, Reader};
use crate::model::landscape::{LandscapeFileDiscovery, LandscapeFileType, LandscapeManifest, NormalizedLandscapeRow};

pub async fn process_archive_from_url(url: &str, raw_dir: &Path) -> Result<(String, Vec<LandscapeFileDiscovery>)> {
    let archive_name = url.split('/').last().unwrap_or("unknown.zip").to_string();
    let local_path = raw_dir.join(&archive_name);

    let bytes = if url.starts_with("file://") {
        let path = Path::new(&url[7..]);
        let mut f = File::open(path)?;
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;
        buffer.into()
    } else {
        if local_path.exists() {
            log::info!("Using cached archive: {}", local_path.display());
            let mut f = File::open(&local_path)?;
            let mut buffer = Vec::new();
            f.read_to_end(&mut buffer)?;
            buffer.into()
        } else {
            let client = reqwest::Client::builder()
                .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                .build()?;

            let response = client.get(url).send().await?;
            if !response.status().is_success() {
                return Err(anyhow::anyhow!("Failed to download archive from {}: HTTP {}", url, response.status()));
            }

            let b = response.bytes().await?;
            std::fs::write(&local_path, &b)?;
            log::info!("Saved archive to {}", local_path.display());
            b
        }
    };

    let mut discovered_files = Vec::new();
    scan_zip_bytes_recursive(&bytes, &mut discovered_files, "", &archive_name)?;

    // De-duplicate discovered files by (year, file_name, sheet)
    discovered_files.sort_by_key(|f| (f.year, f.file_name.clone(), f.sheet.clone()));
    discovered_files.dedup_by_key(|f| (f.year, f.file_name.clone(), f.sheet.clone()));

    Ok((archive_name, discovered_files))
}

fn scan_zip_bytes_recursive(bytes: &[u8], discovered: &mut Vec<LandscapeFileDiscovery>, parent_path: &str, source_archive: &str) -> Result<()> {
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = match ZipArchive::new(cursor) {
        Ok(a) => a,
        Err(e) => {
            log::warn!("Failed to open ZIP at {}: {}", parent_path, e);
            return Ok(());
        }
    };

    for i in 0..archive.len() {
        let mut zip_file = archive.by_index(i)?;
        let name = zip_file.name().to_string();
        
        if zip_file.is_dir() || name.contains("__MACOSX") || name.ends_with(".DS_Store") {
            continue;
        }

        let full_name = if parent_path.is_empty() { name.clone() } else { format!("{}/{}", parent_path, name) };
        let extension = Path::new(&name).extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
        
        match extension.as_str() {
            "zip" => {
                let mut nested_content = Vec::new();
                zip_file.read_to_end(&mut nested_content)?;
                log::info!("Diving into nested ZIP: {}", full_name);
                scan_zip_bytes_recursive(&nested_content, discovered, &full_name, source_archive)?;
            }
            "csv" | "xlsx" | "xlsm" | "xlsb" | "xls" => {
                log::debug!("Evaluating candidate file: {}", full_name);
                match process_zip_entry(&mut zip_file, &full_name, source_archive) {
                    Ok(Some(disc)) => {
                        if disc.year > 0 {
                            log::info!("Discovered Landscape for year {}: {}", disc.year, full_name);
                            discovered.push(disc);
                        } else {
                            log::warn!("Identified data file but could not infer year: {}", full_name);
                        }
                    },
                    Ok(None) => {},
                    Err(e) => log::warn!("Failed to evaluate file {}: {}", full_name, e),
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn process_zip_entry(file: &mut zip::read::ZipFile, full_name: &str, source_archive: &str) -> Result<Option<LandscapeFileDiscovery>> {
    let extension = Path::new(full_name).extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
    
    match extension.as_str() {
        "csv" => {
            let mut content = Vec::new();
            file.read_to_end(&mut content)?;
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(true)
                .from_reader(content.as_slice());
            
            let headers = rdr.headers()?.iter().map(|s| s.to_string()).collect();
            let year = infer_year(full_name);
            
            Ok(Some(LandscapeFileDiscovery {
                year,
                file_name: full_name.to_string(),
                sheet: None,
                file_type: LandscapeFileType::Csv,
                columns: headers,
                row_count_estimate: None,
                source_archive: source_archive.to_string(),
            }))
        }
        "xlsx" | "xlsm" | "xlsb" | "xls" => {
            let mut content = Vec::new();
            file.read_to_end(&mut content)?;
            let cursor = std::io::Cursor::new(content);
            
            let mut workbook = match open_workbook_auto_from_rs(cursor) {
                Ok(wb) => wb,
                Err(e) => return Err(anyhow::anyhow!("Failed to open Excel file {}: {}", full_name, e)),
            };

            let sheet_names = workbook.sheet_names().to_owned();
            if sheet_names.is_empty() {
                return Ok(None);
            }

            // Heuristic: prefer sheets with "MA-PD" or "Landscape" or "Enrollment" or "Plan" or "Premium" or just the first one
            let sheet_name = sheet_names.iter()
                .find(|s| {
                    let s_up = s.to_uppercase();
                    s_up.contains("MA-PD") || s_up.contains("LANDSCAPE") || s_up.contains("PLAN") || s_up.contains("PREMIUM")
                })
                .cloned()
                .unwrap_or_else(|| sheet_names[0].clone());
            
            if let Ok(range) = workbook.worksheet_range(&sheet_name) {
                let headers = range.rows().next().map(|row| {
                    row.iter().map(|c| c.to_string()).collect::<Vec<_>>()
                }).unwrap_or_default();
                
                let file_type = match extension.as_str() {
                    "xls" => LandscapeFileType::Xls,
                    "xlsb" => LandscapeFileType::Xlsb,
                    _ => LandscapeFileType::Xlsx,
                };

                Ok(Some(LandscapeFileDiscovery {
                    year: infer_year(full_name),
                    file_name: full_name.to_string(),
                    sheet: Some(sheet_name),
                    file_type,
                    columns: headers,
                    row_count_estimate: Some(range.height()),
                    source_archive: source_archive.to_string(),
                }))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None)
    }
}

pub async fn ingest_landscape_year(year: i32, force: bool, store_dir: &Path) -> Result<()> {
    let landscape_dir = store_dir.join("landscape");
    let manifest_path = landscape_dir.join("manifests").join("landscape_manifest.json");
    
    if !manifest_path.exists() {
        return Err(anyhow::anyhow!("Landscape manifest not found. Run discovery first."));
    }
    
    let file = File::open(&manifest_path)?;
    let mut manifest: LandscapeManifest = serde_json::from_reader(file)?;
    
    if !force && manifest.imported_years.contains(&year) {
        log::info!("Landscape data for year {} already imported. Skipping.", year);
        return Ok(());
    }

    let files_to_process: Vec<_> = manifest.files.iter()
        .filter(|f| f.year == year)
        .cloned()
        .collect();
    
    if files_to_process.is_empty() {
        return Err(anyhow::anyhow!("No files found for year {} in manifest.", year));
    }

    log::info!("Ingesting {} files for Landscape year {}", files_to_process.len(), year);
    
    let mut normalized_rows = Vec::new();
    let import_batch_id = uuid::Uuid::new_v4().to_string();

    for f in files_to_process {
        // Find archive local path from source_archives map
        let archive_local_path_str = manifest.source_archives.get(&f.source_archive)
            .ok_or_else(|| anyhow::anyhow!("Source archive {} not found in manifest", f.source_archive))?;
        
        let archive_path = Path::new(archive_local_path_str);
        if !archive_path.exists() {
            return Err(anyhow::anyhow!("Source archive not found at {}", archive_local_path_str));
        }

        let content = match get_recursive_file_content(archive_path, &f.file_name) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("Could not find file {} in archive: {}", f.file_name, e);
                continue;
            }
        };

        match f.file_type {
            LandscapeFileType::Csv => {
                let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(content.as_slice());
                let headers: Vec<String> = rdr.headers()?.iter().map(|s| s.to_string()).collect();
                
                for result in rdr.records() {
                    let record = result?;
                    let mut map = std::collections::HashMap::new();
                    for (i, val) in record.iter().enumerate() {
                        if i < headers.len() {
                            map.insert(headers[i].clone(), val.to_string());
                        }
                    }
                    
                    if let Some(normalized) = map_row_to_normalized(&map, year, &f.file_name, f.sheet.as_deref(), &import_batch_id) {
                        normalized_rows.push(normalized);
                    }
                }
            }
            LandscapeFileType::Xls | LandscapeFileType::Xlsx | LandscapeFileType::Xlsb => {
                let cursor = std::io::Cursor::new(content);
                let mut workbook = open_workbook_auto_from_rs(cursor)?;
                
                let sheet_name = match &f.sheet {
                    Some(s) => s.clone(),
                    None => workbook.sheet_names()[0].clone(),
                };
                
                if let Ok(range) = workbook.worksheet_range(&sheet_name) {
                    let mut rows_iter = range.rows();
                    if let Some(header_row) = rows_iter.next() {
                        let headers: Vec<String> = header_row.iter().map(|c| c.to_string()).collect();
                        
                        for row in rows_iter {
                            let mut map = std::collections::HashMap::new();
                            for (i, cell) in row.iter().enumerate() {
                                if i < headers.len() {
                                    map.insert(headers[i].clone(), cell.to_string());
                                }
                            }
                            
                            if let Some(normalized) = map_row_to_normalized(&map, year, &f.file_name, f.sheet.as_deref(), &import_batch_id) {
                                normalized_rows.push(normalized);
                            }
                        }
                    }
                }
            }
        }
    }

    log::info!("Total rows normalized for year {}: {}", year, normalized_rows.len());

    if !normalized_rows.is_empty() {
        let output_path = store_dir.join("landscape").join("normalized").join(format!("year={}", year)).join("landscape.parquet");
        crate::storage::parquet_store::save_landscape_data(&normalized_rows, &output_path)?;
        log::info!("Saved normalized Landscape data to {}", output_path.display());
    }

    if !manifest.imported_years.contains(&year) {
        manifest.imported_years.push(year);
        manifest.imported_years.sort();
    }
    let file = File::create(&manifest_path)?;
    serde_json::to_writer_pretty(file, &manifest)?;

    Ok(())
}

fn get_recursive_file_content(archive_path: &Path, target_full_path: &str) -> Result<Vec<u8>> {
    let file = File::open(archive_path)?;
    let mut archive = ZipArchive::new(file)?;
    
    // Path looks like "Parent.zip/Child.zip/File.csv"
    let parts: Vec<&str> = target_full_path.split('/').collect();
    
    let mut current_bytes = Vec::new();
    
    // First part must be in the main archive
    let mut zip_file = archive.by_name(parts[0])?;
    zip_file.read_to_end(&mut current_bytes)?;
    
    for i in 1..parts.len() {
        let cursor = std::io::Cursor::new(current_bytes);
        let mut inner_archive = ZipArchive::new(cursor)?;
        let mut inner_file = inner_archive.by_name(parts[i])?;
        let mut next_bytes = Vec::new();
        inner_file.read_to_end(&mut next_bytes)?;
        current_bytes = next_bytes;
    }
    
    Ok(current_bytes)
}

fn infer_year(path: &str) -> i32 {
    let parts: Vec<&str> = path.split('/').collect();
    
    // Search from right to left (most specific part first)
    for part in parts.iter().rev() {
        let part_up = part.to_uppercase();
        
        // 1. Look for CY followed by 4 digits (e.g. CY2022)
        let re_cy4 = regex::Regex::new(r"CY(20\d{2})").unwrap();
        if let Some(cap) = re_cy4.captures(&part_up) {
            return cap[1].parse().unwrap_or(0);
        }

        // 2. Look for 4 digits starting with 20 (e.g. 2025)
        let re4 = regex::Regex::new(r"20(\d{2})").unwrap();
        if let Some(cap) = re4.captures(&part_up) {
            return format!("20{}", &cap[1]).parse().unwrap_or(0);
        }

        // 3. Look for CY followed by 2 digits (e.g. CY06)
        let re_cy2 = regex::Regex::new(r"CY(\d{2})").unwrap();
        if let Some(cap) = re_cy2.captures(&part_up) {
            let yr: i32 = cap[1].parse().unwrap_or(0);
            if yr > 50 { return 1900 + yr; }
            else { return 2000 + yr; }
        }

        // 4. Look for standalone 4-digit years in common ranges (2000-2030)
        let re_any4 = regex::Regex::new(r"(20[0-2]\d)").unwrap();
        if let Some(cap) = re_any4.captures(&part_up) {
            return cap[1].parse().unwrap_or(0);
        }
    }

    0
}

fn map_row_to_normalized(
    row: &std::collections::HashMap<String, String>, 
    year: i32, 
    file_name: &str, 
    sheet: Option<&str>, 
    batch_id: &str
) -> Option<NormalizedLandscapeRow> {
    // Basic heuristics for column names that change over time
    let find = |keys: &[&str]| {
        for &k in keys {
            if let Some(v) = row.get(k) { return Some(v); }
            for (rk, rv) in row {
                if rk.to_uppercase() == k.to_uppercase() { return Some(rv); }
            }
            for (rk, rv) in row {
                if rk.to_uppercase().contains(&k.to_uppercase()) { return Some(rv); }
            }
        }
        None
    };

    let contract_id = find(&["Contract ID", "Contract Number", "Contract#"])?;
    let plan_id = find(&["Plan ID", "Plan Number", "Plan#"])?;
    
    if contract_id.is_empty() || plan_id.is_empty() { return None; }

    let mut norm = NormalizedLandscapeRow {
        contract_year: year,
        state_abbreviation: find(&["State", "State Abbreviation"]).cloned().unwrap_or_default(),
        county_name: find(&["County", "County Name"]).cloned().unwrap_or_default(),
        contract_id: contract_id.clone(),
        plan_id: plan_id.clone(),
        parent_organization_name: find(&["Parent Organization", "Organization Name", "Parent Org"]).cloned().unwrap_or_default(),
        plan_name: find(&["Plan Name", "Plan"]).cloned().unwrap_or_default(),
        plan_type: find(&["Plan Type"]).cloned().unwrap_or_default(),
        
        source_year: year,
        source_file: file_name.to_string(),
        source_sheet: sheet.map(|s| s.to_string()),
        import_batch_id: batch_id.to_string(),
        ..Default::default()
    };

    if let Some(premium_str) = find(&["Monthly Consolidated Premium", "Monthly Premium", "Total Premium"]) {
        norm.monthly_consolidated_premium = premium_str.replace('$', "").replace(',', "").trim().parse().ok();
    }
    
    if let Some(star_str) = find(&["Overall Star Rating", "Star Rating", "Stars"]) {
        norm.overall_star_rating = star_str.parse().ok();
    }

    Some(norm)
}
