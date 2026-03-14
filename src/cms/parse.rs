use anyhow::{Context, Result};
use std::io::Read;
use zip::ZipArchive;
use std::io::Cursor;

pub struct ExtractedFiles {
    pub files: Vec<(String, Vec<u8>)>,
}

pub fn extract_zip(zip_bytes: &[u8]) -> Result<ExtractedFiles> {
    log::info!("Extracting ZIP content");
    let reader = Cursor::new(zip_bytes);
    let mut archive = ZipArchive::new(reader)?;
    let mut extracted = Vec::new();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let name = file.name().to_string();
        
        if file.is_file() {
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;
            log::info!("Extracted file: {} ({} bytes)", name, buffer.len());
            extracted.push((name, buffer));
        }
    }

    Ok(ExtractedFiles { files: extracted })
}

pub fn detect_enrollment_file(extracted: &ExtractedFiles) -> Result<(&String, &Vec<u8>)> {
    // CMS enrollment files often start with "CPSC_Enrollment_Info"
    // We prioritize "Enrollment_Info" over "Contract_Info"
    for (name, content) in &extracted.files {
        let lower_name = name.to_lowercase();
        if lower_name.contains("cpsc") && lower_name.contains("enrollment_info") && lower_name.ends_with(".csv") {
            log::info!("Detected enrollment file: {}", name);
            return Ok((name, content));
        }
    }
    Err(anyhow::anyhow!("Could not find enrollment file in ZIP"))
}

pub fn detect_contract_file(extracted: &ExtractedFiles) -> Result<(&String, &Vec<u8>)> {
    for (name, content) in &extracted.files {
        let lower_name = name.to_lowercase();
        if lower_name.contains("cpsc") && lower_name.contains("contract_info") && lower_name.ends_with(".csv") {
            log::info!("Detected contract metadata file: {}", name);
            return Ok((name, content));
        }
    }
    Err(anyhow::anyhow!("Could not find contract metadata file in ZIP"))
}
