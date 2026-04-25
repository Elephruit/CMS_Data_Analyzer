use anyhow::{Context, Result};
use scraper::{Html, Selector};
use crate::model::YearMonth;

pub struct CmsSourceInfo {
    pub zip_url: String,
}

pub async fn discover_month(month: YearMonth) -> Result<CmsSourceInfo> {
    let landing_page = "https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-enrollment-contract/plan/state/county";

    log::info!("Fetching CMS landing page: {}", landing_page);

    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .build()?;

    let response = client.get(landing_page).send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Failed to fetch CMS landing page: HTTP {}", response.status()));
    }

    let html_content = response.text().await?;

    let month_page_url = {
        let document = Html::parse_document(&html_content);
        let month_link_selector = Selector::parse("a").unwrap();

        const MONTH_NAMES: [&str; 12] = [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december",
        ];
        let month_name = MONTH_NAMES[(month.month as usize) - 1];

        // Slugs to look for. We use regex to avoid "2025-1" matching "2025-12".
        // The pattern ensures the month/year combination is either at the end of the string
        // or followed by something other than a digit.
        let patterns = vec![
            format!("{}-{:02}", month.year, month.month),
            format!("{}-{}", month_name, month.year),
            format!("{}-{}", month.year, month.month),
        ];

        let mut url = None;

        for element in document.select(&month_link_selector) {
            if let Some(href) = element.value().attr("href") {
                let href_lower = href.to_lowercase();
                
                for p in &patterns {
                    if let Some(idx) = href_lower.find(p) {
                        // Check if the character after the match is a digit
                        let next_char = href_lower.get(idx + p.len()..idx + p.len() + 1);
                        let is_followed_by_digit = next_char.map_or(false, |c| c.chars().next().unwrap().is_ascii_digit());
                        
                        if !is_followed_by_digit {
                            let full_url = if href.starts_with("http") {
                                href.to_string()
                            } else {
                                format!("https://www.cms.gov{}", href)
                            };
                            url = Some(full_url);
                            break;
                        }
                    }
                }
                if url.is_some() { break; }
            }
        }
        url.context(format!("Could not find link for month {} on landing page", month))?
    };

    log::info!("Found monthly page URL: {}", month_page_url);

    // Now fetch the monthly page to find the ZIP link
    let response = client.get(&month_page_url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Failed to fetch monthly page {}: HTTP {}", month_page_url, response.status()));
    }

    let html_content = response.text().await?;

    let zip_url = {
        let document = Html::parse_document(&html_content);
        let link_selector = Selector::parse("a").unwrap();
        let mut url = None;

        const MONTH_NAMES: [&str; 12] = [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december",
        ];
        let month_name = MONTH_NAMES[(month.month as usize) - 1];
        let year_str = month.year.to_string();

        for element in document.select(&link_selector) {
            if let Some(href) = element.value().attr("href") {
                let lower_href = href.to_lowercase();
                
                // Be extremely defensive. CMS naming is inconsistent.
                // We look for:
                // 1. Contains ".zip" (anywhere, to catch .zip-0)
                // 2. Contains the year
                // 3. Contains either the numeric month or the word-based month
                let has_zip = lower_href.contains(".zip");
                let has_year = lower_href.contains(&year_str);
                let has_month = lower_href.contains(&format!("{:02}", month.month)) || lower_href.contains(month_name);
                
                if has_zip && has_year && has_month {
                    let full_url = if href.starts_with("http") {
                        href.to_string()
                    } else {
                        format!("https://www.cms.gov{}", href)
                    };
                    url = Some(full_url);
                    break;
                }
            }
        }
        url.context(format!("Could not find ZIP link on page {}", month_page_url))?
    };

    log::info!("Found ZIP URL: {}", zip_url);

    Ok(CmsSourceInfo {
        zip_url,
    })
}
