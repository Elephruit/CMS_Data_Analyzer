use anyhow::Result;
use arrow::array::{UInt32Array, StringArray, BooleanArray, ArrayRef, AsArray, UInt64Array, ListArray, Array, Float64Array, Int32Array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{UInt32Type, Field, DataType};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use crate::model::{PlanDim, CountyDim, PlanCountySeries};

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
        let parent_orgs = batch.column(4).as_string::<i32>();
        let plan_types = batch.column(5).as_string::<i32>();
        let is_egwps = batch.column(6).as_boolean();
        let is_snps = batch.column(7).as_boolean();
        let valid_froms = batch.column(8).as_primitive::<UInt32Type>();
        let valid_tos = batch.column(9).as_primitive::<UInt32Type>();
        let is_currents = batch.column(10).as_boolean();

        for i in 0..batch.num_rows() {
            let valid_to = if valid_tos.is_valid(i) {
                Some(valid_tos.value(i))
            } else {
                None
            };

            plans.push(PlanDim {
                plan_key: plan_keys.value(i),
                contract_id: contract_ids.value(i).to_string(),
                plan_id: plan_ids.value(i).to_string(),
                plan_name: plan_names.value(i).to_string(),
                parent_org: parent_orgs.value(i).to_string(),
                plan_type: plan_types.value(i).to_string(),
                is_egwp: is_egwps.value(i),
                is_snp: is_snps.value(i),
                valid_from_month: valid_froms.value(i),
                valid_to_month: valid_to,
                is_current: is_currents.value(i),
            });
        }
    }
    Ok(plans)
}

pub fn save_plan_dim(plans: &[PlanDim], path: &Path) -> Result<()> {
    let plan_keys = UInt32Array::from(plans.iter().map(|p| p.plan_key).collect::<Vec<_>>());
    let contract_ids = StringArray::from(plans.iter().map(|p| p.contract_id.clone()).collect::<Vec<_>>());
    let plan_ids = StringArray::from(plans.iter().map(|p| p.plan_id.clone()).collect::<Vec<_>>());
    let plan_names = StringArray::from(plans.iter().map(|p| p.plan_name.clone()).collect::<Vec<_>>());
    let parent_orgs = StringArray::from(plans.iter().map(|p| p.parent_org.clone()).collect::<Vec<_>>());
    let plan_types = StringArray::from(plans.iter().map(|p| p.plan_type.clone()).collect::<Vec<_>>());
    let is_egwps = BooleanArray::from(plans.iter().map(|p| p.is_egwp).collect::<Vec<_>>());
    let is_snps = BooleanArray::from(plans.iter().map(|p| p.is_snp).collect::<Vec<_>>());
    let valid_froms = UInt32Array::from(plans.iter().map(|p| p.valid_from_month).collect::<Vec<_>>());
    let valid_tos = UInt32Array::from(plans.iter().map(|p| p.valid_to_month).collect::<Vec<_>>());
    let is_currents = BooleanArray::from(plans.iter().map(|p| p.is_current).collect::<Vec<_>>());

    let batch = RecordBatch::try_from_iter(vec![
        ("plan_key", Arc::new(plan_keys) as ArrayRef),
        ("contract_id", Arc::new(contract_ids) as ArrayRef),
        ("plan_id", Arc::new(plan_ids) as ArrayRef),
        ("plan_name", Arc::new(plan_names) as ArrayRef),
        ("parent_org", Arc::new(parent_orgs) as ArrayRef),
        ("plan_type", Arc::new(plan_types) as ArrayRef),
        ("is_egwp", Arc::new(is_egwps) as ArrayRef),
        ("is_snp", Arc::new(is_snps) as ArrayRef),
        ("valid_from_month", Arc::new(valid_froms) as ArrayRef),
        ("valid_to_month", Arc::new(valid_tos) as ArrayRef),
        ("is_current", Arc::new(is_currents) as ArrayRef),
    ])?;

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
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

pub fn load_series_partition(path: &Path) -> Result<Vec<PlanCountySeries>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut series_list = Vec::new();
    for batch in reader {
        let batch = batch?;
        let plan_keys = batch.column(0).as_primitive::<UInt32Type>();
        let county_keys = batch.column(1).as_primitive::<UInt32Type>();
        let start_months = batch.column(2).as_primitive::<UInt32Type>();
        let bitmaps = batch.column(3).as_primitive::<arrow::datatypes::UInt64Type>();
        let enrollments_list = batch.column(4).as_list::<i32>();

        for i in 0..batch.num_rows() {
            let enrollments_array = enrollments_list.value(i);
            let enrollments_primitive = enrollments_array.as_primitive::<UInt32Type>();
            let mut enrollments = Vec::new();
            for j in 0..enrollments_primitive.len() {
                enrollments.push(enrollments_primitive.value(j));
            }

            series_list.push(PlanCountySeries {
                plan_key: plan_keys.value(i),
                county_key: county_keys.value(i),
                start_month_key: start_months.value(i),
                presence_bitmap: bitmaps.value(i),
                enrollments,
            });
        }
    }
    Ok(series_list)
}

pub fn save_series_partition(series_list: &[PlanCountySeries], path: &Path) -> Result<()> {
    let plan_keys = UInt32Array::from(series_list.iter().map(|s| s.plan_key).collect::<Vec<_>>());
    let county_keys = UInt32Array::from(series_list.iter().map(|s| s.county_key).collect::<Vec<_>>());
    let start_months = UInt32Array::from(series_list.iter().map(|s| s.start_month_key).collect::<Vec<_>>());
    let bitmaps = UInt64Array::from(series_list.iter().map(|s| s.presence_bitmap).collect::<Vec<_>>());

    let mut values = Vec::new();
    let mut offsets = vec![0i32];
    for s in series_list {
        values.extend_from_slice(&s.enrollments);
        offsets.push(values.len() as i32);
    }
    
    let values_array = Arc::new(UInt32Array::from(values)) as ArrayRef;
    let enrollments_list = ListArray::try_new(
        Arc::new(Field::new("item", DataType::UInt32, true)),
        OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets)),
        values_array,
        None,
    )?;

    let batch = RecordBatch::try_from_iter(vec![
        ("plan_key", Arc::new(plan_keys) as ArrayRef),
        ("county_key", Arc::new(county_keys) as ArrayRef),
        ("start_month_key", Arc::new(start_months) as ArrayRef),
        ("presence_bitmap", Arc::new(bitmaps) as ArrayRef),
        ("enrollments", Arc::new(enrollments_list) as ArrayRef),
    ])?;

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

pub fn save_landscape_data(rows: &[crate::model::NormalizedLandscapeRow], path: &Path) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let contract_years = Int32Array::from(rows.iter().map(|r| r.contract_year).collect::<Vec<_>>());
    let states = StringArray::from(rows.iter().map(|r| r.state_abbreviation.clone()).collect::<Vec<_>>());
    let counties = StringArray::from(rows.iter().map(|r| r.county_name.clone()).collect::<Vec<_>>());
    let contract_ids = StringArray::from(rows.iter().map(|r| r.contract_id.clone()).collect::<Vec<_>>());
    let plan_ids = StringArray::from(rows.iter().map(|r| r.plan_id.clone()).collect::<Vec<_>>());
    let org_names = StringArray::from(rows.iter().map(|r| r.parent_organization_name.clone()).collect::<Vec<_>>());
    let plan_names = StringArray::from(rows.iter().map(|r| r.plan_name.clone()).collect::<Vec<_>>());
    let plan_types = StringArray::from(rows.iter().map(|r| r.plan_type.clone()).collect::<Vec<_>>());
    
    let premiums = Float64Array::from(rows.iter().map(|r| r.monthly_consolidated_premium).collect::<Vec<_>>());
    let star_ratings = Float64Array::from(rows.iter().map(|r| r.overall_star_rating).collect::<Vec<_>>());
    
    let batch = RecordBatch::try_from_iter(vec![
        ("contract_year", Arc::new(contract_years) as ArrayRef),
        ("state", Arc::new(states) as ArrayRef),
        ("county", Arc::new(counties) as ArrayRef),
        ("contract_id", Arc::new(contract_ids) as ArrayRef),
        ("plan_id", Arc::new(plan_ids) as ArrayRef),
        ("parent_org", Arc::new(org_names) as ArrayRef),
        ("plan_name", Arc::new(plan_names) as ArrayRef),
        ("plan_type", Arc::new(plan_types) as ArrayRef),
        ("premium", Arc::new(premiums) as ArrayRef),
        ("star_rating", Arc::new(star_ratings) as ArrayRef),
    ])?;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
