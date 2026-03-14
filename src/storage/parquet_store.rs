use anyhow::{Context, Result};
use arrow::array::{UInt32Array, StringArray, BooleanArray, ArrayRef, AsArray};
use arrow::datatypes::UInt32Type;

pub fn load_plan_dim(path: &Path) -> Result<Vec<PlanDim>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut plans = Vec::new();
    for batch in reader {
        let batch = batch?;
        let plan_keys = batch.column(0).as_primitive::<UInt32Type>();
        let contract_ids = batch.column(1).as_string::<i32>();
        let plan_ids = batch.column(2).as_string::<i32>();
        let plan_names = batch.column(3).as_string::<i32>();
        let valid_froms = batch.column(4).as_primitive::<UInt32Type>();
        let is_currents = batch.column(5).as_boolean();

        for i in 0..batch.num_rows() {
            plans.push(PlanDim {
                plan_key: plan_keys.value(i),
                contract_id: contract_ids.value(i).to_string(),
                plan_id: plan_ids.value(i).to_string(),
                plan_name: plan_names.value(i).to_string(),
                valid_from_month: valid_froms.value(i),
                valid_to_month: None, // Simplified
                is_current: is_currents.value(i),
            });
        }
    }
    Ok(plans)
}

pub fn load_county_dim(path: &Path) -> Result<Vec<CountyDim>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut counties = Vec::new();
    for batch in reader {
        let batch = batch?;
        let county_keys = batch.column(0).as_primitive::<UInt32Type>();
        let state_codes = batch.column(1).as_string::<i32>();
        let county_names = batch.column(2).as_string::<i32>();

        for i in 0..batch.num_rows() {
            counties.push(CountyDim {
                county_key: county_keys.value(i),
                state_code: state_codes.value(i).to_string(),
                county_name: county_names.value(i).to_string(),
            });
        }
    }
    Ok(counties)
}
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use crate::model::{PlanDim, CountyDim};

pub fn save_plan_dim(plans: &[PlanDim], path: &Path) -> Result<()> {
    let plan_keys = UInt32Array::from(plans.iter().map(|p| p.plan_key).collect::<Vec<_>>());
    let contract_ids = StringArray::from(plans.iter().map(|p| p.contract_id.clone()).collect::<Vec<_>>());
    let plan_ids = StringArray::from(plans.iter().map(|p| p.plan_id.clone()).collect::<Vec<_>>());
    let plan_names = StringArray::from(plans.iter().map(|p| p.plan_name.clone()).collect::<Vec<_>>());
    let valid_froms = UInt32Array::from(plans.iter().map(|p| p.valid_from_month).collect::<Vec<_>>());
    let is_currents = BooleanArray::from(plans.iter().map(|p| p.is_current).collect::<Vec<_>>());

    let batch = RecordBatch::try_from_iter(vec![
        ("plan_key", Arc::new(plan_keys) as ArrayRef),
        ("contract_id", Arc::new(contract_ids) as ArrayRef),
        ("plan_id", Arc::new(plan_ids) as ArrayRef),
        ("plan_name", Arc::new(plan_names) as ArrayRef),
        ("valid_from_month", Arc::new(valid_froms) as ArrayRef),
        ("is_current", Arc::new(is_currents) as ArrayRef),
    ])?;

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

pub fn save_county_dim(counties: &[CountyDim], path: &Path) -> Result<()> {
    let county_keys = UInt32Array::from(counties.iter().map(|c| c.county_key).collect::<Vec<_>>());
    let state_codes = StringArray::from(counties.iter().map(|c| c.state_code.clone()).collect::<Vec<_>>());
    let county_names = StringArray::from(counties.iter().map(|c| c.county_name.clone()).collect::<Vec<_>>());

    let batch = RecordBatch::try_from_iter(vec![
        ("county_key", Arc::new(county_keys) as ArrayRef),
        ("state_code", Arc::new(state_codes) as ArrayRef),
        ("county_name", Arc::new(county_names) as ArrayRef),
    ])?;

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

pub fn save_series(
    enrollment_map: &std::collections::HashMap<(u32, u32), u32>,
    month: crate::model::YearMonth,
    path: &std::path::Path,
) -> Result<()> {
    let mut plan_keys = Vec::new();
    let mut county_keys = Vec::new();
    let mut enrollments = Vec::new();

    for (&(plan_key, county_key), &enrollment) in enrollment_map {
        plan_keys.push(plan_key);
        county_keys.push(county_key);
        enrollments.push(enrollment);
    }

    let plan_keys_array = UInt32Array::from(plan_keys);
    let county_keys_array = UInt32Array::from(county_keys);
    let enrollment_array = UInt32Array::from(enrollments);
    let month_array = UInt32Array::from(vec![month.to_yyyymm(); enrollment_map.len()]);

    let batch = RecordBatch::try_from_iter(vec![
        ("plan_key", Arc::new(plan_keys_array) as ArrayRef),
        ("county_key", Arc::new(county_keys_array) as ArrayRef),
        ("yyyymm", Arc::new(month_array) as ArrayRef),
        ("enrollment", Arc::new(enrollment_array) as ArrayRef),
    ])?;

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
