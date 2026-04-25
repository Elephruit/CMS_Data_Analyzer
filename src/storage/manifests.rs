use anyhow::Result;
use std::fs::File;
use std::path::Path;
use crate::model::StoreManifest;

pub fn load_manifest(path: &Path) -> Result<StoreManifest> {
    if !path.exists() {
        return Ok(StoreManifest::default());
    }
    let file = File::open(path)?;
    let manifest = serde_json::from_reader(file)?;
    Ok(manifest)
}

pub fn save_manifest(manifest: &StoreManifest, path: &Path) -> Result<()> {
    crate::util::io::ensure_parent_dir(path)?;
    let file = File::create(path)?;
    serde_json::to_writer_pretty(file, manifest)?;
    Ok(())
}
