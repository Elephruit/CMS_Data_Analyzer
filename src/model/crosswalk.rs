use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct NormalizedCrosswalkRow {
    pub crosswalk_year: i32,
    pub previous_contract_id: String,
    pub previous_plan_id: String,
    pub previous_plan_key: String,
    pub previous_plan_name: Option<String>,
    pub previous_snp_type: Option<String>,
    pub previous_snp_institutional: Option<String>,
    pub current_contract_id: String,
    pub current_plan_id: String,
    pub current_plan_key: String,
    pub current_plan_name: Option<String>,
    pub current_snp_type: Option<String>,
    pub current_snp_institutional: Option<String>,
    pub status: String,
    
    // Metadata
    pub source_file: String,
    pub source_sheet: Option<String>,
    pub import_batch_id: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct CrosswalkManifest {
    pub files: Vec<CrosswalkFileDiscovery>,
    pub imported_years: Vec<i32>,
    pub source_archives: std::collections::HashMap<String, String>, // name -> local_path
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CrosswalkFileDiscovery {
    pub year: i32,
    #[serde(rename = "fileName")]
    pub file_name: String,
    pub sheet: Option<String>,
    #[serde(rename = "fileType")]
    pub file_type: crate::model::landscape::LandscapeFileType, // Reuse the same enum
    pub columns: Vec<String>,
    #[serde(rename = "sourceArchive")]
    pub source_archive: String,
}
