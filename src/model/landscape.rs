use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum LandscapeFileType {
    #[serde(rename = "csv")]
    Csv,
    #[serde(rename = "xls")]
    Xls,
    #[serde(rename = "xlsx")]
    Xlsx,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LandscapeFileDiscovery {
    pub year: i32,
    #[serde(rename = "fileName")]
    pub file_name: String,
    pub sheet: Option<String>,
    #[serde(rename = "fileType")]
    pub file_type: LandscapeFileType,
    pub columns: Vec<String>,
    #[serde(rename = "rowCountEstimate")]
    pub row_count_estimate: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct LandscapeManifest {
    pub files: Vec<LandscapeFileDiscovery>,
    pub imported_years: Vec<i32>,
    pub archive_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct NormalizedLandscapeRow {
    pub contract_year: i32,
    pub state_abbreviation: String,
    pub state_name: Option<String>,
    pub county_name: String,
    pub contract_id: String,
    pub plan_id: String,
    pub segment_id: Option<String>,
    pub parent_organization_name: String,
    pub organization_marketing_name: Option<String>,
    pub organization_type: Option<String>,
    pub plan_name: String,
    pub plan_type: String,
    pub snp_indicator: Option<bool>,
    pub snp_type: Option<String>,
    pub part_d_coverage_indicator: Option<bool>,
    pub national_pdp: Option<bool>,
    pub drug_benefit_type: Option<String>,
    pub monthly_consolidated_premium: Option<f64>,
    pub part_c_premium: Option<f64>,
    pub part_d_basic_premium: Option<f64>,
    pub part_d_supplemental_premium: Option<f64>,
    pub part_d_total_premium: Option<f64>,
    pub in_network_moop_amount: Option<f64>,
    pub overall_star_rating: Option<f64>,
    pub ma_region: Option<String>,
    pub pdp_region: Option<String>,
    
    // Metadata
    pub source_year: i32,
    pub source_file: String,
    pub source_sheet: Option<String>,
    pub import_batch_id: String,
}
