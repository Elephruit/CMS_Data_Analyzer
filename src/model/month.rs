use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct YearMonth {
    pub year: i32,
    pub month: u8,
}

#[derive(Error, Debug)]
pub enum YearMonthError {
    #[error("Invalid format. Expected YYYY-MM")]
    InvalidFormat,
    #[error("Invalid year: {0}")]
    InvalidYear(i32),
    #[error("Invalid month: {0}")]
    InvalidMonth(u8),
}

impl YearMonth {
    pub fn new(year: i32, month: u8) -> Result<Self, YearMonthError> {
        if year < 1900 || year > 2100 {
            return Err(YearMonthError::InvalidYear(year));
        }
        if month < 1 || month > 12 {
            return Err(YearMonthError::InvalidMonth(month));
        }
        Ok(Self { year, month })
    }

    pub fn to_yyyymm(&self) -> u32 {
        (self.year as u32) * 100 + (self.month as u32)
    }
}

impl FromStr for YearMonth {
    type Err = YearMonthError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 2 {
            return Err(YearMonthError::InvalidFormat);
        }

        let year = parts[0].parse::<i32>().map_err(|_| YearMonthError::InvalidFormat)?;
        let month = parts[1].parse::<u8>().map_err(|_| YearMonthError::InvalidFormat)?;

        Self::new(year, month)
    }
}

impl fmt::Display for YearMonth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}", self.year, self.month)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_year_month_parsing() {
        let ym: YearMonth = "2025-01".parse().unwrap();
        assert_eq!(ym.year, 2025);
        assert_eq!(ym.month, 1);
        assert_eq!(ym.to_yyyymm(), 202501);
    }

    #[test]
    fn test_year_month_display() {
        let ym = YearMonth::new(2025, 3).unwrap();
        assert_eq!(format!("{}", ym), "2025-03");
    }

    #[test]
    fn test_invalid_month() {
        assert!(YearMonth::new(2025, 13).is_err());
        assert!(YearMonth::new(2025, 0).is_err());
    }
}
