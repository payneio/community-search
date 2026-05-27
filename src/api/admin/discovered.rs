//! Admin API for discovered engines.
//!
//! Routes:
//! - `GET    /api/admin/discovered`         — list all discovered engines
//! - `POST   /api/admin/discovered/promote` — promote a discovered engine to a node peer
//! - `DELETE /api/admin/discovered`         — remove a discovered engine (rejects self-entry)

use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use crate::api::public::AppState;
use crate::federation::discovered;
use crate::federation::storage::insert_node_peer;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Response body for `GET /api/admin/discovered`.
#[derive(serde::Serialize)]
pub struct DiscoveredListResp {
    pub engines: Vec<discovered::DiscoveredEngine>,
}

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Body for `POST /api/admin/discovered/promote`.
#[derive(Deserialize)]
pub struct PromoteReq {
    pub url: String,
}

/// Query parameters for `DELETE /api/admin/discovered`.
#[derive(Deserialize)]
pub struct DeleteQuery {
    pub url: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// GET /api/admin/discovered
///
/// Returns all entries from the `discovered_engines` table as a JSON object
/// `{"engines": [...]}` ordered by `last_seen DESC`.
///
/// ## Status codes
/// - 200 OK           — list returned (may be empty)
/// - 401 Unauthorized — missing or invalid `Authorization: Bearer` header
/// - 500 Internal     — database error
pub async fn list_discovered(
    State(state): State<AppState>,
) -> Result<Json<DiscoveredListResp>, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let engines =
        discovered::list(&conn).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(DiscoveredListResp { engines }))
}

/// POST /api/admin/discovered/promote
///
/// Looks up the given URL in `discovered_engines`, then inserts it as a node
/// peer via `insert_node_peer`.
///
/// ## Status codes
/// - 200 OK           — promoted successfully
/// - 401 Unauthorized — missing or invalid `Authorization: Bearer` header
/// - 404 Not Found    — URL not in discovered engines
/// - 500 Internal     — database error
pub async fn promote_discovered(
    State(state): State<AppState>,
    Json(req): Json<PromoteReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let engine = discovered::get(&conn, &req.url)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "unknown discovered engine".to_string(),
            )
        })?;

    insert_node_peer(&conn, &engine.url, Some(&engine.name))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({"status": "ok"})))
}

/// DELETE /api/admin/discovered?url=...
///
/// Removes the discovered engine with the given URL.
///
/// Returns 400 if the URL equals `self_url` (cannot delete the self-entry).
/// Returns 404 if no matching row exists.
///
/// ## Status codes
/// - 200 OK           — engine removed; body is `{"status":"ok","removed":N}`
/// - 400 Bad Request  — attempted to remove the self-entry
/// - 401 Unauthorized — missing or invalid `Authorization: Bearer` header
/// - 404 Not Found    — no such discovered engine
/// - 500 Internal     — database error
pub async fn remove_discovered(
    State(state): State<AppState>,
    Query(q): Query<DeleteQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if q.url == state.self_url {
        return Err((
            StatusCode::BAD_REQUEST,
            "cannot remove self-entry".to_string(),
        ));
    }

    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let n = discovered::remove(&conn, &q.url)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if n == 0 {
        return Err((
            StatusCode::NOT_FOUND,
            "no such discovered engine".to_string(),
        ));
    }

    Ok(Json(serde_json::json!({"status": "ok", "removed": n})))
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the discovered-engines sub-router.
///
/// Registers:
/// - `GET    /api/admin/discovered`         → [`list_discovered`]
/// - `POST   /api/admin/discovered/promote` → [`promote_discovered`]
/// - `DELETE /api/admin/discovered`         → [`remove_discovered`]
pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/admin/discovered",
            get(list_discovered).delete(remove_discovered),
        )
        .route("/api/admin/discovered/promote", post(promote_discovered))
}
