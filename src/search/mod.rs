pub mod ranking;
pub mod result;
pub mod service;

// Re-export the key search types so federation and other modules can use the
// stable path `crate::search::{SearchRequest, SearchResult}`.
pub use crate::api::public::SearchRequest;
pub use result::SearchResult;
