use std::path::Path;
use std::fs;
use anyhow::Result;

/// Ensures the parent directory of a path exists.
pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}
