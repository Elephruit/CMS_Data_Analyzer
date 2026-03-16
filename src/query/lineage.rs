use anyhow::Result;
use std::path::Path;
use crate::storage;
use crate::model::NormalizedCrosswalkRow;

pub struct LineageEngine {
    pub store_dir: std::path::PathBuf,
}

impl LineageEngine {
    pub fn new(store_dir: &Path) -> Self {
        Self {
            store_dir: store_dir.to_path_buf(),
        }
    }

    pub fn get_lineage(&self, contract_id: &str, plan_id: &str, target_year: i32) -> Result<Vec<NormalizedCrosswalkRow>> {
        let mut lineage = Vec::new();
        let plan_key = format!("{}-{}", contract_id, plan_id);
        
        let mut current_year = target_year;
        let mut current_plan_key = plan_key;

        // Trace backwards, collecting ALL predecessors per year (supports consolidations).
        while current_year >= 2006 {
            let path = self.store_dir.join("crosswalk").join("normalized").join(format!("year={}", current_year)).join("crosswalk.parquet");
            if !path.exists() { break; }

            let rows = storage::parquet_store::load_crosswalk_data(&path)?;
            // Collect all rows where this plan is the successor (many-to-one = consolidation).
            let predecessors: Vec<NormalizedCrosswalkRow> = rows.into_iter()
                .filter(|r| r.current_plan_key == current_plan_key)
                .collect();

            if predecessors.is_empty() { break; }

            // Trace backwards through the first predecessor's key (conventional primary chain).
            let prev_key = predecessors[0].previous_plan_key.clone();
            lineage.extend(predecessors);

            if prev_key.to_uppercase().contains("NEW") || prev_key == current_plan_key {
                break;
            }
            current_plan_key = prev_key;
            current_year -= 1;
        }

        Ok(lineage)
    }
}
