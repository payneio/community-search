//! Collection information types shared between the local API and federation.

use serde::{Deserialize, Serialize};

/// Summary of a collection exposed to peers via `/api/collections`.
///
/// Shape: `{ name: String, description: Option<String> }`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub description: Option<String>,
}
