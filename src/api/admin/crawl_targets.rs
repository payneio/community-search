use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::api::public::AppState;
use crate::crawler::canonical::detect_canonical_prefix;
use crate::db::collections::exists;
use crate::db::crawl_targets::{
    add, list, list_page_urls, remove, set_crawl_delay, set_interval, CrawlTargetListItem,
};

// ---------------------------------------------------------------------------
// Request type
// ---------------------------------------------------------------------------

/// Action-discriminated request body for the crawl-targets admin endpoint.
///
/// Uses `serde(tag = "action")` so that the JSON field `"action"` selects
/// the variant (snake_case):
///
/// - `{"action":"add","collection_id":1,"url_prefix":"https://example.com/","recrawl_interval_secs":3600}`
/// - `{"action":"remove","id":1}`
/// - `{"action":"set_interval","id":1,"recrawl_interval_secs":7200}`
#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum CrawlTargetAction {
    Add {
        collection_id: i64,
        url_prefix: String,
        recrawl_interval_secs: i64,
    },
    Remove {
        id: i64,
    },
    SetInterval {
        id: i64,
        recrawl_interval_secs: i64,
    },
    /// Set or clear the per-target politeness-delay override.
    /// `crawl_delay_secs = null` clears the override; positive integer sets it.
    SetCrawlDelay {
        id: i64,
        crawl_delay_secs: Option<i64>,
    },
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ListResponse {
    crawl_targets: Vec<CrawlTargetListItem>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// GET /api/admin/crawl-targets
///
/// Returns all configured crawl targets, each joined to its parent collection
/// for display purposes.
async fn list_crawl_targets(
    State(state): State<AppState>,
) -> Result<Response, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let rows = list(&conn).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ListResponse {
        crawl_targets: rows,
    })
    .into_response())
}

/// POST /api/admin/crawl-targets
///
/// Dispatches add / remove / set_interval based on the `action` field.
///
/// ## Status codes
/// - 200 OK          — operation succeeded (add returns JSON body)
/// - 400 Bad Request — unknown or malformed action
/// - 404 Not Found   — collection or target does not exist
/// - 500 Internal    — database error
async fn handle_crawl_targets(
    State(state): State<AppState>,
    // Extract raw Value first so that an unrecognised `action` produces 400
    // (axum's built-in Json extractor returns 422 on serde errors).
    Json(raw): Json<serde_json::Value>,
) -> Result<Response, (StatusCode, String)> {
    // Deserialise into the typed enum; map serde errors to 400.
    let action = serde_json::from_value::<CrawlTargetAction>(raw)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid action: {e}")))?;

    // The DB lock is acquired per-arm rather than once at the top: the
    // Remove arm has to release it before `.await`-ing on the indexer
    // channel, otherwise the resulting future is `!Send` and axum's
    // `Handler` trait stops being satisfied.
    match action {
        CrawlTargetAction::Add {
            collection_id,
            url_prefix,
            recrawl_interval_secs,
        } => {
            // Detect the canonical form *before* taking the DB lock — the
            // detection call awaits an HTTP roundtrip, and we don't want to
            // hold the mutex across that. Falls back to the admin's input
            // on any error (network, timeout, path-changing redirect).
            let effective_prefix = detect_canonical_prefix(&url_prefix, &state.crawler_user_agent)
                .await
                .unwrap_or(url_prefix);

            let conn = lock_db(&state)?;
            let col_exists = exists(&conn, collection_id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            if !col_exists {
                return Err((
                    StatusCode::NOT_FOUND,
                    format!("collection {collection_id} not found"),
                ));
            }

            let record = add(
                &conn,
                collection_id,
                &effective_prefix,
                recrawl_interval_secs,
            )
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            Ok(Json(record).into_response())
        }

        CrawlTargetAction::Remove { id } => {
            // Snapshot URLs *and* delete inside a block scope so the
            // MutexGuard drops at `}` — must happen before the `.await`
            // below for the future to be `Send`.
            let (urls, found) = {
                let conn = lock_db(&state)?;
                let urls = list_page_urls(&conn, id)
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let found = remove(&conn, id)
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                (urls, found)
            };

            if !found {
                return Err((
                    StatusCode::NOT_FOUND,
                    format!("crawl target {id} not found"),
                ));
            }

            // Tell the indexer to drop the corresponding documents. A
            // SendError here means the indexer task has exited; the DB
            // delete already succeeded, so log and return 200 — the index
            // will catch up on the next rebuild, but the target really is
            // gone from the operational store.
            if !urls.is_empty() {
                if let Err(e) = state.indexer_delete_tx.send(urls).await {
                    tracing::warn!(
                        "crawl target {id} removed from DB, but failed to \
                         queue index deletions: {e}"
                    );
                }
            }
            Ok(StatusCode::OK.into_response())
        }

        CrawlTargetAction::SetInterval {
            id,
            recrawl_interval_secs,
        } => {
            let conn = lock_db(&state)?;
            let found = set_interval(&conn, id, recrawl_interval_secs)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            if found {
                Ok(StatusCode::OK.into_response())
            } else {
                Err((
                    StatusCode::NOT_FOUND,
                    format!("crawl target {id} not found"),
                ))
            }
        }

        CrawlTargetAction::SetCrawlDelay {
            id,
            crawl_delay_secs,
        } => {
            let conn = lock_db(&state)?;
            let found = set_crawl_delay(&conn, id, crawl_delay_secs)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            if found {
                Ok(StatusCode::OK.into_response())
            } else {
                Err((
                    StatusCode::NOT_FOUND,
                    format!("crawl target {id} not found"),
                ))
            }
        }
    }
}

fn lock_db(
    state: &AppState,
) -> Result<std::sync::MutexGuard<'_, rusqlite::Connection>, (StatusCode, String)> {
    state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn routes() -> Router<AppState> {
    Router::new().route(
        "/api/admin/crawl-targets",
        get(list_crawl_targets).post(handle_crawl_targets),
    )
}
