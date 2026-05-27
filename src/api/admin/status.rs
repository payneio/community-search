//! GET /api/admin/status — system status snapshot.
//!
//! Returns a JSON object with the following fields:
//! - `index_size_bytes`        — current on-disk size of the search index
//! - `max_index_bytes`         — configured maximum index size
//! - `crawl_targets_total`     — total number of configured crawl targets
//! - `crawls_active`           — number of crawls currently running (Phase 2)
//! - `crawls_queued`           — number of crawls waiting to run (Phase 2)
//! - `crawl_paused`            — whether the scheduler is paused
//! - `indexing_pending_count`  — crawled pages awaiting Tantivy commit
//! - `peers`                   — list of known peers (Phase 5; empty array in Phase 4)

use std::sync::atomic::Ordering;

use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use serde_json::Value;

use crate::api::public::AppState;
use crate::db::crawl_targets;
use crate::index::size::index_dir_size_bytes;

// ---------------------------------------------------------------------------
// Response type
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct StatusResponse {
    /// Current total size of the search index directory in bytes.
    index_size_bytes: i64,
    /// Maximum allowed index size in bytes (from config).
    max_index_bytes: i64,
    /// Total number of crawl targets configured in the database.
    crawl_targets_total: i64,
    /// Number of crawl targets currently being crawled.
    ///
    /// TODO(Phase 2): populate from `state.crawler.active_count()` once the
    /// Phase 2 crawler exposes this metric.
    crawls_active: i64,
    /// Number of crawl targets waiting to be crawled.
    ///
    /// TODO(Phase 2): populate from `state.crawler.queued_count()` once the
    /// Phase 2 crawler exposes this metric.
    crawls_queued: i64,
    /// Known peer nodes.  Empty array in Phase 4; populated in Phase 5.
    peers: Vec<Value>,
    /// Whether the crawler is currently paused. Toggled via
    /// `POST /api/admin/crawl` with `{"paused": true|false}`.
    crawl_paused: bool,
    /// Number of `crawled_pages` rows whose content has been fetched but
    /// whose `indexed_content_hash` does not yet match `content_hash` —
    /// i.e. the indexer task has not yet committed them to Tantivy. When
    /// this is `0` and `crawl_paused` is `true`, an export will see every
    /// document that the crawler has fetched.
    indexing_pending_count: i64,
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// GET /api/admin/status
///
/// Returns a system status snapshot. Requires a valid admin token (enforced
/// by the `route_layer` in [`crate::api::admin::admin_router`]).
async fn get_status(State(state): State<AppState>) -> Json<StatusResponse> {
    // Compute index disk usage; fall back to 0 on I/O error.
    let index_size_bytes = index_dir_size_bytes(&state.index_path)
        .unwrap_or(0)
        .min(i64::MAX as u64) as i64;

    // Count all crawl targets in the database.
    let crawl_targets_total = {
        let conn = state.db.lock().expect("db mutex poisoned");
        crawl_targets::count_all(&conn).unwrap_or(0)
    };

    // True in-flight indexing count. Source of truth is the AtomicI64 the
    // crawler and import handler bump on every IndexJob send, and the
    // indexer decrements after each successful Tantivy commit. Unlike the
    // earlier SQL-based count this is not confused by URL aliasing
    // (redirects, `<link rel="canonical">`) — those produce a single send
    // and a single decrement regardless of how many `crawled_pages` rows
    // share the canonicalised content. Reaches zero exactly when the
    // index is durably caught up; safe to gate an export on.
    let indexing_pending_count = state.indexing_inflight.load(Ordering::Relaxed).max(0);

    // TODO(Phase 2): replace with state.crawler.active_count() once the
    // Phase 2 crawler exposes live crawl metrics.
    let crawls_active: i64 = 0;

    // TODO(Phase 2): replace with state.crawler.queued_count() once the
    // Phase 2 crawler exposes queue depth metrics.
    let crawls_queued: i64 = 0;

    Json(StatusResponse {
        index_size_bytes,
        max_index_bytes: state.max_index_bytes.min(i64::MAX as u64) as i64,
        crawl_targets_total,
        crawls_active,
        crawls_queued,
        peers: vec![], // Phase 5: will be populated with known peer nodes
        crawl_paused: state.crawl_paused.load(Ordering::Relaxed),
        indexing_pending_count,
    })
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Register the status route.
///
/// Auth is enforced by the `route_layer` in the parent [`admin_router`].
///
/// [`admin_router`]: crate::api::admin::admin_router
pub fn routes() -> Router<AppState> {
    Router::new().route("/api/admin/status", get(get_status))
}
