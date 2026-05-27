pub mod indexer;
pub mod reader;
pub mod schema;
pub mod size;
pub mod writer;

use anyhow::{Context, Result};
use std::path::Path;
use tantivy::Index;

pub fn open_or_create(dir: impl AsRef<Path>) -> Result<Index> {
    let dir = dir.as_ref();

    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create index directory: {}", dir.display()))?;

    let meta_path = dir.join("meta.json");

    if meta_path.exists() {
        Index::open_in_dir(dir)
            .with_context(|| format!("failed to open existing index at {}", dir.display()))
    } else {
        let schema = schema::build();
        Index::create_in_dir(dir, schema)
            .with_context(|| format!("failed to create new index at {}", dir.display()))
    }
}
