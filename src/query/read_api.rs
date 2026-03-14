use anyhow::Result;
use arrow::array::{UInt32Array, StringArray, AsArray};
use arrow::datatypes::UInt32Type;
use std::fs::File;
use std::path::Path;
use crate::model::{PlanDim, CountyDim, YearMonth};

pub struct QueryEngine {
    pub store_dir: std::path::PathBuf,
}

impl QueryEngine {
    pub fn new(store_dir: &Path) -> Self {
        Self {
            store_dir: store_dir.to_path_buf(),
        }
    }

    pub fn get_plan_key(&self, contract_id: &str, plan_id: &str) -> Result<Option<u32>> {
        let path = self.store_dir.join("dims").join("plan_dim.parquet");
        if !path.exists() { return Ok(None); }

        let file = File::open(path)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        for batch in reader {
            let batch = batch?;
            let plan_keys = batch.column(0).as_primitive::<UInt32Type>();
            let contract_ids = batch.column(1).as_string::<i32>();
            let plan_ids = batch.column(2).as_string::<i32>();

            for i in 0..batch.num_rows() {
                if contract_ids.value(i) == contract_id && plan_ids.value(i) == plan_id {
                    return Ok(Some(plan_keys.value(i)));
                }
            }
        }
        Ok(None)
    }

    pub fn get_plan_trend(&self, plan_key: u32) -> Result<Vec<(u32, u32)>> {
        let facts_dir = self.store_dir.join("facts");
        let mut trend = Vec::new();

        if !facts_dir.exists() { return Ok(trend); }

        for year_entry in std::fs::read_dir(facts_dir)? {
            let year_path = year_entry?.path();
            if year_path.is_dir() {
                for month_entry in std::fs::read_dir(year_path)? {
                    let month_path = month_entry?.path();
                    if month_path.extension().map_or(false, |ext| ext == "parquet") {
                        let file = File::open(month_path)?;
                        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
                        let reader = builder.build()?;

                        for batch in reader {
                            let batch = batch?;
                            let plan_keys = batch.column(0).as_primitive::<UInt32Type>();
                            let yyyymms = batch.column(2).as_primitive::<UInt32Type>();
                            let enrollments = batch.column(3).as_primitive::<UInt32Type>();

                            for i in 0..batch.num_rows() {
                                if plan_keys.value(i) == plan_key {
                                    trend.push((yyyymms.value(i), enrollments.value(i)));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Aggregate by month (since one plan can be in multiple counties)
        let mut monthly_totals = std::collections::HashMap::new();
        for (month, enrollment) in trend {
            *monthly_totals.entry(month).or_insert(0) += enrollment;
        }

        let mut result: Vec<_> = monthly_totals.into_iter().collect();
        result.sort_by_key(|(m, _)| *m);
        Ok(result)
    }

    pub fn get_county_key(&self, state_code: &str, county_name: &str) -> Result<Option<u32>> {
        let path = self.store_dir.join("dims").join("county_dim.parquet");
        if !path.exists() { return Ok(None); }

        let file = File::open(path)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        for batch in reader {
            let batch = batch?;
            let county_keys = batch.column(0).as_primitive::<UInt32Type>();
            let state_codes = batch.column(1).as_string::<i32>();
            let county_names = batch.column(2).as_string::<i32>();

            for i in 0..batch.num_rows() {
                if state_codes.value(i).to_lowercase() == state_code.to_lowercase() 
                   && county_names.value(i).to_lowercase() == county_name.to_lowercase() {
                    return Ok(Some(county_keys.value(i)));
                }
            }
        }
        Ok(None)
    }

    pub fn get_county_snapshot(&self, county_key: u32, month: YearMonth) -> Result<Vec<(String, String, String, u32)>> {
        let yyyymm = month.to_yyyymm();
        let year_dir = self.store_dir.join("facts").join(format!("year={}", month.year));
        let mut raw_snapshot = Vec::new();

        if !year_dir.exists() { return Ok(Vec::new()); }

        for entry in std::fs::read_dir(year_dir)? {
            let path = entry?.path();
            if path.extension().map_or(false, |ext| ext == "parquet") {
                let file = File::open(path)?;
                let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
                let reader = builder.build()?;

                for batch in reader {
                    let batch = batch?;
                    let plan_keys = batch.column(0).as_primitive::<UInt32Type>();
                    let county_keys = batch.column(1).as_primitive::<UInt32Type>();
                    let yyyymms = batch.column(2).as_primitive::<UInt32Type>();
                    let enrollments = batch.column(3).as_primitive::<UInt32Type>();

                    for i in 0..batch.num_rows() {
                        if county_keys.value(i) == county_key && yyyymms.value(i) == yyyymm {
                            raw_snapshot.push((plan_keys.value(i), enrollments.value(i)));
                        }
                    }
                }
            }
        }

        // Load plan metadata for names
        let plan_dim_path = self.store_dir.join("dims").join("plan_dim.parquet");
        let all_plans = crate::storage::parquet_store::load_plan_dim(&plan_dim_path)?;
        let plan_map: std::collections::HashMap<u32, &PlanDim> = all_plans.iter().map(|p| (p.plan_key, p)).collect();

        let mut result = Vec::new();
        for (plan_key, enrollment) in raw_snapshot {
            if let Some(plan) = plan_map.get(&plan_key) {
                result.push((plan.contract_id.clone(), plan.plan_id.clone(), plan.plan_name.clone(), enrollment));
            }
        }

        result.sort_by_key(|(_, _, _, e)| std::cmp::Reverse(*e));
        Ok(result)
    }
}
