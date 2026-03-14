use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use crate::util::hashing::compute_sha256;

pub async fn download_zip(url: &str) -> Result<Vec<u8>> {
    log::info!("Downloading ZIP from: {}", url);

    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .build()?;

    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Failed to download ZIP: HTTP {}", response.status()));
    }

    let bytes = response.bytes().await?.to_vec();
    log::info!("Downloaded {} bytes", bytes.len());

    Ok(bytes)
}
