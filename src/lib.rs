pub mod api;
pub mod auth;
pub mod collections;
pub mod config;
pub mod crawler;
pub mod db;
pub mod federation;
pub mod index;
pub mod middleware;
pub mod protocol;
pub mod search;
#[cfg(any(test, feature = "testing"))]
pub mod testing;

// ---------------------------------------------------------------------------
// Runtime configuration
// ---------------------------------------------------------------------------

/// Runtime-tunable configuration applied immediately to new requests.
///
/// All fields are optional: only fields present in a PUT body are updated.
/// The struct is stored as a JSON blob in the `app_config` table under the
/// key `runtime_config` and loaded on startup so changes survive restarts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct RuntimeConfig {
    /// Fan-out depth for peer-search requests (Phase 5).
    pub fanout_depth: Option<u32>,
    /// Maximum search requests per IP per minute (applied to /api/search).
    pub search_rate_limit_per_minute: Option<u32>,
    /// Default interval between re-crawls of a URL, in seconds.
    pub crawl_default_recrawl_interval_secs: Option<u64>,
    /// Minimum delay between consecutive fetches to the same host, in ms.
    pub crawl_politeness_delay_ms: Option<u64>,
    /// Maximum number of concurrent in-flight crawl tasks.
    pub max_concurrent_crawls: Option<u32>,
}

// ---------------------------------------------------------------------------
// Test support helpers
// ---------------------------------------------------------------------------
//
// Compiled unconditionally so that integration tests in `tests/` (which
// compile the crate as a library, without `cfg(test)`) can use them without
// requiring the `testing` feature flag.

/// Helpers for building test routers and spawning test servers.
pub mod test_support;
