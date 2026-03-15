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

        // Trace backwards
        while current_year >= 2006 {
            let path = self.store_dir.join("crosswalk").join("normalized").join(format!("year={}", current_year)).join("crosswalk.parquet");
            if !path.exists() { break; }

            let rows = storage::parquet_store::load_crosswalk_data(&path)?;
            let predecessor = rows.into_iter().find(|r| r.current_plan_key == current_plan_key);

            if let Some(row) = predecessor {
                let prev_key = row.previous_plan_key.clone();
                lineage.push(row);
                
                if prev_key == "NEW" || prev_key == current_plan_key && current_year < target_year {
                    break;
                }
                
                current_plan_key = prev_key;
                current_year -= 1;
            } else {
                break;
            }
        }

        Ok(lineage)
    }
}
