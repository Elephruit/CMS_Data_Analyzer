use anyhow::{Context, Result};
use crate::model::NormalizedRow;
use csv::StringRecord;
use std::collections::HashMap;

pub struct RowStats {
    pub total_rows: u64,
    pub star_rows: u64,
    pub malformed_rows: u64,
    pub kept_rows: u64,
}

impl Default for RowStats {
    fn default() -> Self {
        Self {
            total_rows: 0,
            star_rows: 0,
            malformed_rows: 0,
            kept_rows: 0,
        }
    }
}

pub struct ContractHeaderMap {
    pub contract_id_idx: usize,
    pub plan_id_idx: usize,
    pub plan_name_idx: usize,
}

pub fn map_contract_headers(headers: &StringRecord) -> Result<ContractHeaderMap> {
    let mut map = HashMap::new();
    for (i, h) in headers.iter().enumerate() {
        map.insert(h.to_lowercase().replace(" ", "_").replace("-", "_"), i);
    }

    let find = |names: &[&str]| -> Result<usize> {
        for &n in names {
            if let Some(&idx) = map.get(n) {
                return Ok(idx);
            }
        }
        Err(anyhow::anyhow!("Could not find contract header: {:?}", names))
    };

    Ok(ContractHeaderMap {
        contract_id_idx: find(&["contract_id", "contract_number"])?,
        plan_id_idx: find(&["plan_id", "plan_number"])?,
        plan_name_idx: find(&["plan_name", "organization_name", "org_name"])?,
    })
}

pub struct EnrollmentHeaderMap {
    pub contract_id_idx: usize,
    pub plan_id_idx: usize,
    pub state_idx: usize,
    pub county_idx: usize,
    pub enrollment_idx: usize,
}

pub fn map_enrollment_headers(headers: &StringRecord) -> Result<EnrollmentHeaderMap> {
    let mut map = HashMap::new();
    for (i, h) in headers.iter().enumerate() {
        map.insert(h.to_lowercase().replace(" ", "_").replace("-", "_"), i);
    }

    let find = |names: &[&str]| -> Result<usize> {
        for &n in names {
            if let Some(&idx) = map.get(n) {
                return Ok(idx);
            }
        }
        Err(anyhow::anyhow!("Could not find enrollment header: {:?}", names))
    };

    Ok(EnrollmentHeaderMap {
        contract_id_idx: find(&["contract_id", "contract_number"])?,
        plan_id_idx: find(&["plan_id", "plan_number"])?,
        state_idx: find(&["state", "state_name", "state_code"])?,
        county_idx: find(&["county", "county_name"])?,
        enrollment_idx: find(&["enrollment", "enrolled_count"])?,
    })
}

pub fn normalize_enrollment_byte_row(
    record: &csv::ByteRecord,
    headers: &EnrollmentHeaderMap,
    plan_names: &HashMap<(String, String), String>,
) -> Result<Option<NormalizedRow>> {
    let contract_id = String::from_utf8_lossy(record.get(headers.contract_id_idx).context("Missing contract_id")?).trim().to_string();
    let plan_id = String::from_utf8_lossy(record.get(headers.plan_id_idx).context("Missing plan_id")?).trim().to_string();
    let state_code = String::from_utf8_lossy(record.get(headers.state_idx).context("Missing state")?).trim().to_string();
    let county_name = String::from_utf8_lossy(record.get(headers.county_idx).context("Missing county")?).trim().to_string();
    let enrollment_bytes = record.get(headers.enrollment_idx).context("Missing enrollment")?;
    let enrollment_str = String::from_utf8_lossy(enrollment_bytes).trim().to_string();

    if enrollment_str == "*" {
        return Ok(None);
    }

    let enrollment = enrollment_str.replace(",", "").parse::<u32>().map_err(|_| anyhow::anyhow!("Malformed enrollment: {}", enrollment_str))?;

    let plan_name = plan_names.get(&(contract_id.clone(), plan_id.clone()))
        .cloned()
        .unwrap_or_else(|| "Unknown Plan".to_string());

    Ok(Some(NormalizedRow {
        contract_id,
        plan_id,
        plan_name,
        state_code,
        county_name,
        enrollment,
    }))
}
