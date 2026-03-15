use anyhow::Result;
use std::path::Path;
use std::fs::File;
use std::io::Read;
use zip::ZipArchive;
use calamine::{open_workbook_auto_from_rs, Reader};
use scraper::{Html, Selector};
use crate::model::{CrosswalkFileDiscovery, CrosswalkManifest, NormalizedCrosswalkRow, LandscapeFileType};

pub async fn discover_crosswalk_archives_full() -> Result<CrosswalkManifest> {
    let discovery = crate::cms::discover::discover_crosswalk_archives().await?;
    let mut manifest = CrosswalkManifest::default();
    
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .build()?;

    for (year, page_url) in discovery.year_links {
        log::info!("Evaluating Crosswalk page for year {}: {}", year, page_url);
        
        // Fetch the year-specific page to find the ZIP link
        let response = client.get(&page_url).send().await?;
        if !response.status().is_success() {
            log::warn!("Failed to fetch year page {}: HTTP {}", page_url, response.status());
            continue;
        }

        let html = response.text().await?;
        
        let zip_url = {
            let document = Html::parse_document(&html);
            let link_selector = Selector::parse("a").unwrap();

            let mut url = None;
            for element in document.select(&link_selector) {
                if let Some(href) = element.value().attr("href") {
                    let lower = href.to_lowercase();
                    if lower.contains("crosswalk") && (lower.contains(".zip") || lower.contains(".xlsx") || lower.contains(".xls")) {
                        let full_url = if href.starts_with("http") {
                            href.to_string()
                        } else {
                            format!("https://www.cms.gov{}", href)
                        };
                        url = Some(full_url);
                        break;
                    }
                }
            }
            url
        };

        if let Some(url) = zip_url {
            let _archive_name = url.split('/').last().unwrap_or("unknown.zip").to_string();
            // We'll store the URL temporarily in source_archives if we haven't downloaded it yet
            // but for discovery we just need to know it's there.
            // We'll download during the 'ingest' phase or here if we want to scan columns.
            // Let's download and scan now to populate 'columns'
            
            let store_dir = Path::new("store");
            let raw_dir = store_dir.join("crosswalk").join("raw");
            std::fs::create_dir_all(&raw_dir)?;
            
            match process_crosswalk_archive(&url, &raw_dir, year).await {
                Ok((name, files)) => {
                    let local_path = raw_dir.join(&name);
                    manifest.source_archives.insert(name, local_path.to_string_lossy().to_string());
                    manifest.files.extend(files);
                },
                Err(e) => log::warn!("Failed to process crosswalk archive {}: {}", url, e),
            }
        }
    }

    Ok(manifest)
}

pub async fn process_crosswalk_archive(url: &str, raw_dir: &Path, year: i32) -> Result<(String, Vec<CrosswalkFileDiscovery>)> {
    let archive_name = url.split('/').last().unwrap_or("unknown.zip").to_string();
    let local_path = raw_dir.join(&archive_name);

    let bytes = if local_path.exists() {
        log::info!("Using cached crosswalk archive: {}", local_path.display());
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
        log::info!("Saved crosswalk archive to {}", local_path.display());
        b
    };

    let mut discovered_files = Vec::new();
    
    // Check if it's a direct data file or a ZIP
    let extension = Path::new(&archive_name).extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
    if extension == "zip" {
        scan_crosswalk_zip_recursive(&bytes, &mut discovered_files, "", &archive_name, year)?;
    } else {
        // Direct Excel or CSV
        let mut cursor = std::io::Cursor::new(bytes.to_vec());
        if let Some(disc) = process_crosswalk_entry_as_direct(&mut cursor, &archive_name, &archive_name, year)? {
            discovered_files.push(disc);
        }
    }

    Ok((archive_name, discovered_files))
}

fn scan_crosswalk_zip_recursive(bytes: &[u8], discovered: &mut Vec<CrosswalkFileDiscovery>, parent_path: &str, source_archive: &str, year: i32) -> Result<()> {
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = match ZipArchive::new(cursor) {
        Ok(a) => a,
        Err(e) => {
            log::warn!("Failed to open Crosswalk ZIP at {}: {}", parent_path, e);
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
                scan_crosswalk_zip_recursive(&nested_content, discovered, &full_name, source_archive, year)?;
            }
            "csv" | "xlsx" | "xlsm" | "xlsb" | "xls" | "txt" => {
                match process_crosswalk_zip_entry(&mut zip_file, &full_name, source_archive, year) {
                    Ok(Some(disc)) => discovered.push(disc),
                    Ok(None) => {},
                    Err(e) => log::warn!("Failed to evaluate crosswalk file {}: {}", full_name, e),
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn process_crosswalk_zip_entry(file: &mut zip::read::ZipFile, full_name: &str, source_archive: &str, year: i32) -> Result<Option<CrosswalkFileDiscovery>> {
    let extension = Path::new(full_name).extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
    
    match extension.as_str() {
        "csv" | "txt" => {
            let mut content = Vec::new();
            file.read_to_end(&mut content)?;
            
            let delimiter = if extension == "txt" { b'\t' } else { b',' };
            
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(true)
                .delimiter(delimiter)
                .from_reader(content.as_slice());
            
            let headers = rdr.headers()?.iter().map(|s| s.to_string()).collect();
            
            Ok(Some(CrosswalkFileDiscovery {
                year,
                file_name: full_name.to_string(),
                sheet: None,
                file_type: LandscapeFileType::Csv,
                columns: headers,
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

            let sheet_name = sheet_names.iter()
                .find(|s| {
                    let s_up = s.to_uppercase();
                    s_up.contains("CROSSWALK") || s_up.contains("PLAN")
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

                Ok(Some(CrosswalkFileDiscovery {
                    year,
                    file_name: full_name.to_string(),
                    sheet: Some(sheet_name),
                    file_type,
                    columns: headers,
                    source_archive: source_archive.to_string(),
                }))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None)
    }
}

fn process_crosswalk_entry_as_direct(cursor: &mut std::io::Cursor<Vec<u8>>, full_name: &str, source_archive: &str, year: i32) -> Result<Option<CrosswalkFileDiscovery>> {
    let extension = Path::new(full_name).extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
    
    match extension.as_str() {
        "csv" | "txt" => {
            let delimiter = if extension == "txt" { b'\t' } else { b',' };
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(true)
                .delimiter(delimiter)
                .from_reader(cursor);
            
            let headers = rdr.headers()?.iter().map(|s| s.to_string()).collect();
            
            Ok(Some(CrosswalkFileDiscovery {
                year,
                file_name: full_name.to_string(),
                sheet: None,
                file_type: LandscapeFileType::Csv,
                columns: headers,
                source_archive: source_archive.to_string(),
            }))
        }
        "xlsx" | "xlsm" | "xlsb" | "xls" => {
            let mut workbook = match open_workbook_auto_from_rs(cursor.clone()) {
                Ok(wb) => wb,
                Err(e) => return Err(anyhow::anyhow!("Failed to open Excel file {}: {}", full_name, e)),
            };

            let sheet_names = workbook.sheet_names().to_owned();
            if sheet_names.is_empty() {
                return Ok(None);
            }

            let sheet_name = sheet_names.iter()
                .find(|s| {
                    let s_up = s.to_uppercase();
                    s_up.contains("CROSSWALK") || s_up.contains("PLAN")
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

                Ok(Some(CrosswalkFileDiscovery {
                    year,
                    file_name: full_name.to_string(),
                    sheet: Some(sheet_name),
                    file_type,
                    columns: headers,
                    source_archive: source_archive.to_string(),
                }))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None)
    }
}

pub async fn ingest_crosswalk_year(year: i32, force: bool, store_dir: &Path) -> Result<()> {
    let crosswalk_dir = store_dir.join("crosswalk");
    let manifest_path = crosswalk_dir.join("manifests").join("crosswalk_manifest.json");
    
    if !manifest_path.exists() {
        return Err(anyhow::anyhow!("Crosswalk manifest not found. Run discovery first."));
    }
    
    let file = File::open(&manifest_path)?;
    let mut manifest: CrosswalkManifest = serde_json::from_reader(file)?;
    
    if !force && manifest.imported_years.contains(&year) {
        log::info!("Crosswalk data for year {} already imported. Skipping.", year);
        return Ok(());
    }

    let files_to_process: Vec<_> = manifest.files.iter()
        .filter(|f| f.year == year)
        .cloned()
        .collect();
    
    if files_to_process.is_empty() {
        return Err(anyhow::anyhow!("No files found for year {} in manifest.", year));
    }

    log::info!("Ingesting {} files for Crosswalk year {}", files_to_process.len(), year);
    
    let mut normalized_rows = Vec::new();
    let import_batch_id = uuid::Uuid::new_v4().to_string();

    for f in files_to_process {
        log::info!("Processing file: {} from archive: {}", f.file_name, f.source_archive);
        let archive_local_path_str = manifest.source_archives.get(&f.source_archive)
            .ok_or_else(|| anyhow::anyhow!("Source archive {} not found in manifest", f.source_archive))?;
        
        let archive_path = Path::new(archive_local_path_str);
        
        let extension = Path::new(&f.file_name).extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
        
        let content = if archive_local_path_str.ends_with(&f.file_name) {
            // Direct file
            let mut f_obj = File::open(archive_path)?;
            let mut buf = Vec::new();
            f_obj.read_to_end(&mut buf)?;
            buf
        } else {
            match get_recursive_file_content(archive_path, &f.file_name) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to extract {} from {}: {}", f.file_name, archive_local_path_str, e);
                    continue;
                }
            }
        };

        let mut file_rows = 0;
        match f.file_type {
            LandscapeFileType::Csv => {
                let delimiter = if extension == "txt" { b'\t' } else { b',' };
                let mut rdr = csv::ReaderBuilder::new()
                    .has_headers(true)
                    .delimiter(delimiter)
                    .from_reader(content.as_slice());
                
                let headers: Vec<String> = rdr.headers()?.iter().map(|s| s.to_string()).collect();
                
                for result in rdr.records() {
                    let record = result?;
                    let mut map = std::collections::HashMap::new();
                    for (i, val) in record.iter().enumerate() {
                        if i < headers.len() {
                            map.insert(headers[i].clone(), val.to_string());
                        }
                    }
                    
                    if let Some(normalized) = map_crosswalk_row(&map, year, &f.file_name, f.sheet.as_deref(), &import_batch_id) {
                        normalized_rows.push(normalized);
                        file_rows += 1;
                    }
                }
            }
            LandscapeFileType::Xls | LandscapeFileType::Xlsx | LandscapeFileType::Xlsb => {
                let cursor = std::io::Cursor::new(content);
                let mut workbook = match open_workbook_auto_from_rs(cursor) {
                    Ok(wb) => wb,
                    Err(e) => {
                        log::error!("Failed to open Excel workbook for {}: {}", f.file_name, e);
                        continue;
                    }
                };
                
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
                            
                            if let Some(normalized) = map_crosswalk_row(&map, year, &f.file_name, f.sheet.as_deref(), &import_batch_id) {
                                normalized_rows.push(normalized);
                                file_rows += 1;
                            }
                        }
                    }
                }
            }
        }
        log::info!("Extracted {} normalized rows from {}", file_rows, f.file_name);
    }

    log::info!("Total crosswalk rows normalized for year {}: {}", year, normalized_rows.len());

    if normalized_rows.is_empty() {
        return Err(anyhow::anyhow!("No valid crosswalk rows found for year {}. Mapping may be failing.", year));
    }

    let output_path = store_dir.join("crosswalk").join("normalized").join(format!("year={}", year)).join("crosswalk.parquet");
    crate::storage::parquet_store::save_crosswalk_data(&normalized_rows, &output_path)?;
    log::info!("Saved normalized Crosswalk data to {}", output_path.display());

    if !manifest.imported_years.contains(&year) {
        manifest.imported_years.push(year);
        manifest.imported_years.sort();
    }
    let file = File::create(&manifest_path)?;
    serde_json::to_writer_pretty(file, &manifest)?;

    Ok(())
}

fn map_crosswalk_row(
    row: &std::collections::HashMap<String, String>, 
    year: i32, 
    file_name: &str, 
    sheet: Option<&str>, 
    batch_id: &str
) -> Option<NormalizedCrosswalkRow> {
    let find = |keys: &[&str]| {
        for &k in keys {
            if let Some(v) = row.get(k) { 
                let trimmed = v.trim();
                if !trimmed.is_empty() { return Some(trimmed.to_string()); }
            }
            for (rk, rv) in row {
                let rk_up = rk.to_uppercase();
                let k_up = k.to_uppercase();
                if rk_up == k_up { 
                    let trimmed = rv.trim();
                    if !trimmed.is_empty() { return Some(trimmed.to_string()); }
                }
                if rk_up.contains(&k_up) { 
                    let trimmed = rv.trim();
                    if !trimmed.is_empty() { return Some(trimmed.to_string()); }
                }
            }
        }
        None
    };

    let prev_contract = find(&["Previous Contract ID", "Contract ID (Previous)", "PREV_CONTRACT_ID", "Contract Number (Previous)", "OLD_CONTRACT_ID", "PRV_CNT_ID"]);
    let prev_plan = find(&["Previous Plan ID", "Plan ID (Previous)", "PREV_PLAN_ID", "OLD_PLAN_ID", "PRV_PLN_ID"]);
    
    let curr_contract = find(&["Current Contract ID", "Contract ID (Current)", "CURR_CONTRACT_ID", "Contract Number (Current)", "NEW_CONTRACT_ID", "CUR_CNT_ID"]);
    let curr_plan = find(&["Current Plan ID", "Plan ID (Current)", "CURR_PLAN_ID", "NEW_PLAN_ID", "CUR_PLN_ID"]);

    let status = find(&["Status", "Crosswalk Status", "CROSSWALK_STATUS"]).unwrap_or_else(|| "Renewal Plan".to_string());

    // Validation: must have at least one side identified
    if (prev_contract.is_none() || prev_plan.is_none()) && (curr_contract.is_none() || curr_plan.is_none()) {
        return None;
    }

    let pc = prev_contract.unwrap_or_default();
    let pp = prev_plan.unwrap_or_default();
    let cc = curr_contract.unwrap_or_default();
    let cp = curr_plan.unwrap_or_default();

    let norm = NormalizedCrosswalkRow {
        crosswalk_year: year,
        previous_contract_id: pc.clone(),
        previous_plan_id: pp.clone(),
        previous_plan_key: if pc.is_empty() { String::new() } else { format!("{}-{}", pc, pp) },
        previous_plan_name: find(&["Previous Plan Name", "Plan Name (Previous)", "PRV_PLN_NM"]),
        previous_snp_type: find(&["Previous SNP Type"]),
        
        current_contract_id: cc.clone(),
        current_plan_id: cp.clone(),
        current_plan_key: if cc.is_empty() { String::new() } else { format!("{}-{}", cc, cp) },
        current_plan_name: find(&["Current Plan Name", "Plan Name (Current)", "CUR_PLN_NM"]),
        
        status,
        source_file: file_name.to_string(),
        source_sheet: sheet.map(|s| s.to_string()),
        import_batch_id: batch_id.to_string(),
        ..Default::default()
    };

    Some(norm)
}

// Helper to get recursive ZIP content (reused from landscape but we can duplicate or move to util)
fn get_recursive_file_content(archive_path: &Path, target_full_path: &str) -> Result<Vec<u8>> {
    let file = File::open(archive_path)?;
    let mut archive = ZipArchive::new(file)?;
    
    let parts: Vec<&str> = target_full_path.split('/').collect();
    let mut current_bytes = Vec::new();
    
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
