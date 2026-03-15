use anyhow::Result;
use std::path::Path;
use std::fs::File;
use std::io::Read;
use zip::ZipArchive;
use calamine::{Reader, Xlsx, Xls};
use crate::model::landscape::{LandscapeFileDiscovery, LandscapeFileType, LandscapeManifest, NormalizedLandscapeRow};

pub async fn process_archive_from_url(url: &str, _raw_dir: &Path) -> Result<Vec<LandscapeFileDiscovery>> {
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .build()?;

    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Failed to download archive from {}: HTTP {}", url, response.status()));
    }

    let bytes = response.bytes().await?;
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = ZipArchive::new(cursor)?;
    
    let mut discovered_files = Vec::new();

    for i in 0..archive.len() {
        let mut zip_file = archive.by_index(i)?;
        let name = zip_file.name().to_string();
        
        if zip_file.is_dir() || name.contains("__MACOSX") || name.ends_with(".DS_Store") {
            continue;
        }

        let extension = Path::new(&name).extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
        
        match extension.as_str() {
            "zip" => {
                // Nested ZIP! (common in CMS historical archives)
                let mut nested_content = Vec::new();
                zip_file.read_to_end(&mut nested_content)?;
                let nested_cursor = std::io::Cursor::new(nested_content);
                let mut nested_archive = ZipArchive::new(nested_cursor)?;
                
                for j in 0..nested_archive.len() {
                    let mut inner_file = nested_archive.by_index(j)?;
                    let inner_name = inner_file.name().to_string();
                    if inner_file.is_dir() || inner_name.contains("__MACOSX") || inner_name.ends_with(".DS_Store") {
                        continue;
                    }
                    
                    if let Some(disc) = process_zip_entry(&mut inner_file, &inner_name)? {
                        // We need to save the inner file content to raw_dir if we want to ingest it later
                        // For now, let's just record its metadata and we'll handle extraction during ingestion
                        discovered_files.push(disc);
                    }
                }
            }
            "csv" | "xlsx" | "xlsm" | "xlsb" | "xls" => {
                if let Some(disc) = process_zip_entry(&mut zip_file, &name)? {
                    discovered_files.push(disc);
                }
            }
            _ => {}
        }
    }

    Ok(discovered_files)
}

fn process_zip_entry(file: &mut zip::read::ZipFile, name: &str) -> Result<Option<LandscapeFileDiscovery>> {
    let extension = Path::new(name).extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
    
    match extension.as_str() {
        "csv" => {
            let mut content = Vec::new();
            file.read_to_end(&mut content)?;
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(true)
                .from_reader(content.as_slice());
            
            let headers = rdr.headers()?.iter().map(|s| s.to_string()).collect();
            let year = infer_year(name);
            
            Ok(Some(LandscapeFileDiscovery {
                year,
                file_name: name.to_string(),
                sheet: None,
                file_type: LandscapeFileType::Csv,
                columns: headers,
                row_count_estimate: None,
            }))
        }
        "xlsx" | "xlsm" | "xlsb" | "xls" => {
            let mut content = Vec::new();
            file.read_to_end(&mut content)?;
            let cursor = std::io::Cursor::new(content);
            
            if extension == "xls" {
                let mut workbook: Xls<_> = calamine::open_workbook_from_rs(cursor)?;
                let sheet_names = workbook.sheet_names().to_owned();
                // Heuristic: prefer sheets with "MA-PD" or "Landscape" or just the first one
                let sheet_name = sheet_names.iter().find(|s| s.contains("MA-PD") || s.contains("Landscape")).cloned().unwrap_or_else(|| sheet_names[0].clone());
                
                if let Ok(range) = workbook.worksheet_range(&sheet_name) {
                    let headers = range.rows().next().map(|row| {
                        row.iter().map(|c| c.to_string()).collect::<Vec<_>>()
                    }).unwrap_or_default();
                    
                    Ok(Some(LandscapeFileDiscovery {
                        year: infer_year(name),
                        file_name: name.to_string(),
                        sheet: Some(sheet_name),
                        file_type: LandscapeFileType::Xls,
                        columns: headers,
                        row_count_estimate: Some(range.height()),
                    }))
                } else {
                    Ok(None)
                }
            } else {
                let mut workbook: Xlsx<_> = calamine::open_workbook_from_rs(cursor)?;
                let sheet_names = workbook.sheet_names().to_owned();
                let sheet_name = sheet_names.iter().find(|s| s.contains("MA-PD") || s.contains("Landscape")).cloned().unwrap_or_else(|| sheet_names[0].clone());
                
                if let Ok(range) = workbook.worksheet_range(&sheet_name) {
                    let headers = range.rows().next().map(|row| {
                        row.iter().map(|c| c.to_string()).collect::<Vec<_>>()
                    }).unwrap_or_default();
                    
                    Ok(Some(LandscapeFileDiscovery {
                        year: infer_year(name),
                        file_name: name.to_string(),
                        sheet: Some(sheet_name),
                        file_type: LandscapeFileType::Xlsx,
                        columns: headers,
                        row_count_estimate: Some(range.height()),
                    }))
                } else {
                    Ok(None)
                }
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

    let archive_path_str = manifest.archive_path.as_ref().ok_or_else(|| anyhow::anyhow!("Archive path missing from manifest"))?;
    let archive_path = Path::new(archive_path_str);
    if !archive_path.exists() {
        return Err(anyhow::anyhow!("Archive not found at {}", archive_path_str));
    }

    log::info!("Ingesting {} files for Landscape year {}", files_to_process.len(), year);
    
    let mut normalized_rows = Vec::new();
    let import_batch_id = uuid::Uuid::new_v4().to_string();

    let archive_file = File::open(archive_path)?;
    let mut archive = ZipArchive::new(archive_file)?;

    for f in files_to_process {
        let mut zip_file = archive.by_name(&f.file_name)?;
        let mut content = Vec::new();
        zip_file.read_to_end(&mut content)?;

        match f.file_type {
            LandscapeFileType::Csv => {
                let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(content.as_slice());
                for result in rdr.records() {
                    let _record = result?;
                    // Placeholder: Actual normalization based on columns
                    normalized_rows.push(NormalizedLandscapeRow {
                        contract_year: year,
                        source_year: year,
                        source_file: f.file_name.clone(),
                        source_sheet: f.sheet.clone(),
                        import_batch_id: import_batch_id.clone(),
                        ..Default::default()
                    });
                }
            }
            LandscapeFileType::Xls | LandscapeFileType::Xlsx => {
                // Implementation for Excel
                log::info!("Ingesting Excel sheet: {:?}", f.sheet);
            }
        }
    }

    log::info!("Total rows normalized: {}", normalized_rows.len());

    // Placeholder: Save normalized_rows to Parquet
    
    if !manifest.imported_years.contains(&year) {
        manifest.imported_years.push(year);
        manifest.imported_years.sort();
    }
    let file = File::create(&manifest_path)?;
    serde_json::to_writer_pretty(file, &manifest)?;

    Ok(())
}

pub async fn discover_landscape_files(archive_path: &Path) -> Result<LandscapeManifest> {
    let file = File::open(archive_path)?;
    let mut archive = ZipArchive::new(file)?;
    let mut files = Vec::new();

    for i in 0..archive.len() {
        let mut zip_file = archive.by_index(i)?;
        let name = zip_file.name().to_string();
        
        if zip_file.is_dir() || name.contains("__MACOSX") || name.ends_with(".DS_Store") {
            continue;
        }

        let extension = Path::new(&name).extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
        
        match extension.as_str() {
            "csv" => {
                let mut content = Vec::new();
                zip_file.read_to_end(&mut content)?;
                let mut rdr = csv::ReaderBuilder::new()
                    .has_headers(true)
                    .from_reader(content.as_slice());
                
                let headers = rdr.headers()?.iter().map(|s| s.to_string()).collect();
                let year = infer_year(&name);
                
                files.push(LandscapeFileDiscovery {
                    year,
                    file_name: name,
                    sheet: None,
                    file_type: LandscapeFileType::Csv,
                    columns: headers,
                    row_count_estimate: None,
                });
            }
            "xlsx" | "xlsm" | "xlsb" | "xls" => {
                let mut content = Vec::new();
                zip_file.read_to_end(&mut content)?;
                let cursor = std::io::Cursor::new(content);
                
                if extension == "xls" {
                    let mut workbook: Xls<_> = calamine::open_workbook_from_rs(cursor)?;
                    let sheet_names = workbook.sheet_names().to_owned();
                    for sheet_name in sheet_names {
                        if let Ok(range) = workbook.worksheet_range(&sheet_name) {
                            let headers = range.rows().next().map(|row| {
                                row.iter().map(|c| c.to_string()).collect::<Vec<_>>()
                            }).unwrap_or_default();
                            
                            files.push(LandscapeFileDiscovery {
                                year: infer_year(&name),
                                file_name: name.clone(),
                                sheet: Some(sheet_name),
                                file_type: LandscapeFileType::Xls,
                                columns: headers,
                                row_count_estimate: Some(range.height()),
                            });
                        }
                    }
                } else {
                    let mut workbook: Xlsx<_> = calamine::open_workbook_from_rs(cursor)?;
                    let sheet_names = workbook.sheet_names().to_owned();
                    for sheet_name in sheet_names {
                        if let Ok(range) = workbook.worksheet_range(&sheet_name) {
                            let headers = range.rows().next().map(|row| {
                                row.iter().map(|c| c.to_string()).collect::<Vec<_>>()
                            }).unwrap_or_default();
                            
                            files.push(LandscapeFileDiscovery {
                                year: infer_year(&name),
                                file_name: name.clone(),
                                sheet: Some(sheet_name),
                                file_type: LandscapeFileType::Xlsx,
                                columns: headers,
                                row_count_estimate: Some(range.height()),
                            });
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(LandscapeManifest {
        files,
        imported_years: Vec::new(),
        archive_path: Some(archive_path.to_string_lossy().to_string()),
    })
}

fn infer_year(name: &str) -> i32 {
    let re = regex::Regex::new(r"20\d{2}").unwrap();
    if let Some(cap) = re.captures(name) {
        return cap[0].parse().unwrap_or(0);
    }
    0
}
