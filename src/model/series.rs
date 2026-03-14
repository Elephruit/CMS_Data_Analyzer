use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCountySeries {
    pub plan_key: u32,
    pub county_key: u32,
    pub start_month_key: u32, // yyyymm of the first month in the system
    pub presence_bitmap: u64, // Simple bitmap for up to 64 months for now
    pub enrollments: Vec<u32>, // Compact ordered vector of values present in bitmap
}

impl PlanCountySeries {
    pub fn add_month(&mut self, month_yyyymm: u32, enrollment: u32) {
        // Calculate offset from start_month
        // For simplicity in MVP, we'll assume months are consecutive or handled by bitmap
        // Real implementation would calculate month index based on a global month dimension.
        // Let's assume start_month_key is 202501.
        let start_year = (self.start_month_key / 100) as i32;
        let start_month = (self.start_month_key % 100) as i32;
        let curr_year = (month_yyyymm / 100) as i32;
        let curr_month = (month_yyyymm % 100) as i32;
        
        let month_index = ((curr_year - start_year) * 12 + (curr_month - start_month)) as u32;
        
        if month_index >= 64 {
            log::warn!("Month index {} out of range for bitmap", month_index);
            return;
        }

        let mask = 1u64 << month_index;
        if self.presence_bitmap & mask != 0 {
            // Month already exists, update it
            // We need to find its position in the enrollments vector
            let pos = (self.presence_bitmap & (mask - 1)).count_ones() as usize;
            self.enrollments[pos] = enrollment;
        } else {
            // New month, insert it
            let pos = (self.presence_bitmap & (mask - 1)).count_ones() as usize;
            self.enrollments.insert(pos, enrollment);
            self.presence_bitmap |= mask;
        }
    }

    pub fn get_enrollment(&self, month_yyyymm: u32) -> Option<u32> {
        let start_year = (self.start_month_key / 100) as i32;
        let start_month = (self.start_month_key % 100) as i32;
        let curr_year = (month_yyyymm / 100) as i32;
        let curr_month = (month_yyyymm % 100) as i32;
        
        let month_index = ((curr_year - start_year) * 12 + (curr_month - start_month)) as u32;
        if month_index >= 64 { return None; }

        let mask = 1u64 << month_index;
        if self.presence_bitmap & mask != 0 {
            let pos = (self.presence_bitmap & (mask - 1)).count_ones() as usize;
            Some(self.enrollments[pos])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_series_add_and_get() {
        let mut series = PlanCountySeries {
            plan_key: 1,
            county_key: 1,
            start_month_key: 202501,
            presence_bitmap: 0,
            enrollments: Vec::new(),
        };

        series.add_month(202501, 100);
        series.add_month(202503, 120);
        series.add_month(202502, 110);

        assert_eq!(series.get_enrollment(202501), Some(100));
        assert_eq!(series.get_enrollment(202502), Some(110));
        assert_eq!(series.get_enrollment(202503), Some(120));
        assert_eq!(series.get_enrollment(202504), None);
        
        // Check internal order
        assert_eq!(series.enrollments, vec![100, 110, 120]);
        assert_eq!(series.presence_bitmap, 0b111);
    }

    #[test]
    fn test_series_update() {
        let mut series = PlanCountySeries {
            plan_key: 1,
            county_key: 1,
            start_month_key: 202501,
            presence_bitmap: 0,
            enrollments: Vec::new(),
        };

        series.add_month(202501, 100);
        series.add_month(202501, 105);
        assert_eq!(series.get_enrollment(202501), Some(105));
        assert_eq!(series.enrollments.len(), 1);
    }
}
