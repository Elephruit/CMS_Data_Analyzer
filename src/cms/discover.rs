use anyhow::{Context, Result};
use scraper::{Html, Selector};
use crate::model::YearMonth;

pub struct CmsSourceInfo {
    pub month: YearMonth,
    pub page_url: String,
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
    let document = Html::parse_document(&html_content);

    // Find the link for the specific month
    // The links are usually like "Monthly Enrollment by CPSC YYYY MM"
    let month_link_selector = Selector::parse("a").unwrap();
    let mut month_page_url = None;

    let target_month_text = format!("Monthly Enrollment by CPSC {} {}", month.year, format!("{:02}", month.month));
    let target_month_slug = format!("{}-{:02}", month.year, month.month);

    for element in document.select(&month_link_selector) {
        if let Some(href) = element.value().attr("href") {
            if href.contains(&target_month_slug) {
                let full_url = if href.starts_with("http") {
                    href.to_string()
                } else {
                    format!("https://www.cms.gov{}", href)
                };
                month_page_url = Some(full_url);
                break;
            }
        }
    }

    let month_page_url = month_page_url.context(format!("Could not find link for month {} on landing page", month))?;
    log::info!("Found monthly page URL: {}", month_page_url);

    // Now fetch the monthly page to find the ZIP link
    let response = client.get(&month_page_url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Failed to fetch monthly page {}: HTTP {}", month_page_url, response.status()));
    }

    let html_content = response.text().await?;
    let document = Html::parse_document(&html_content);

    let zip_selector = Selector::parse("a[href$='.zip']").unwrap();
    let mut zip_url = None;

    for element in document.select(&zip_selector) {
        if let Some(href) = element.value().attr("href") {
            let full_url = if href.starts_with("http") {
                href.to_string()
            } else {
                format!("https://www.cms.gov{}", href)
            };
            zip_url = Some(full_url);
            break;
        }
    }

    let zip_url = zip_url.context(format!("Could not find ZIP link on page {}", month_page_url))?;
    log::info!("Found ZIP URL: {}", zip_url);

    Ok(CmsSourceInfo {
        month,
        page_url: month_page_url,
        zip_url,
    })
}

