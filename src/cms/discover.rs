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

        let slugs: Vec<String> = vec![
            format!("{}-{:02}", month.year, month.month),
            format!("{}-{}", month_name, month.year),
            format!("{}-{}", month.year, month.month),
        ];

        let mut url = None;

        for element in document.select(&month_link_selector) {
            if let Some(href) = element.value().attr("href") {
                let href_lower = href.to_lowercase();
                if slugs.iter().any(|s| href_lower.contains(s.as_str())) {
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

        // CMS sometimes appends suffixes like ".zip-0" to ZIP URLs, so we
        // cannot use href$='.zip'. Instead scan all links for any href that
        // contains ".zip" and prefer links that look like the enrollment file
        // (contain "enrollment" or "cpsc" — contract/plan/state/county).
        let mut best: Option<String> = None;
        let mut fallback: Option<String> = None;

        for element in document.select(&link_selector) {
            if let Some(href) = element.value().attr("href") {
                if !href.contains(".zip") { continue; }
                let full_url = if href.starts_with("http") {
                    href.to_string()
                } else {
                    format!("https://www.cms.gov{}", href)
                };
                let lower = href.to_lowercase();
                if lower.contains("enrollment") || lower.contains("cpsc") {
                    best = Some(full_url);
                    break;
                }
                if fallback.is_none() {
                    fallback = Some(full_url);
                }
            }
        }

        best.or(fallback)
            .context(format!("Could not find ZIP link on page {}", month_page_url))?
    };

    log::info!("Found ZIP URL: {}", zip_url);

    Ok(CmsSourceInfo {
        zip_url,
    })
}
