use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, put},
    Json, Router,
};
use serde::Deserialize;

use crate::api::public::AppState;
use crate::db::collections::exists;
use crate::db::ranking_config;

// ---------------------------------------------------------------------------
// Query / request types
// ---------------------------------------------------------------------------

/// Query parameters for GET /api/admin/ranking.
#[derive(Deserialize)]
struct GetParams {
    collection_id: i64,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// PUT /api/admin/ranking
///
/// Body: `{"collection_id": N, "source_weights": {...}, "freshness_half_life_days": N, "domain_boosts": {...}}`
///
/// Validates that the collection exists (404 if not), then persists the entire
/// payload as a JSON blob in `ranking_config.config_json` via UPSERT.
///
/// ## Status codes
/// - 200 OK         — config persisted
/// - 404 Not Found  — collection does not exist
/// - 500 Internal   — database error
async fn put_ranking(
    State(state): State<AppState>,
    Json(raw): Json<serde_json::Value>,
) -> Result<Response, (StatusCode, String)> {
    // Extract collection_id from the payload for validation.
    let collection_id = raw
        .get("collection_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "collection_id must be an integer".to_string(),
            )
        })?;

    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    // Validate that the collection exists.
    let col_exists = exists(&conn, collection_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !col_exists {
        return Err((
            StatusCode::NOT_FOUND,
            format!("collection {collection_id} not found"),
        ));
    }

    // Serialise the raw payload back to a JSON string for storage.
    let payload_json = serde_json::to_string(&raw)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    ranking_config::upsert(&conn, collection_id, &payload_json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(StatusCode::OK.into_response())
}

/// GET /api/admin/ranking?collection_id=N
///
/// Returns the persisted ranking config JSON for the given collection.
///
/// ## Status codes
/// - 200 OK        — config found; body is the stored JSON object
/// - 404 Not Found — no config has been saved for this collection
/// - 500 Internal  — database error
async fn get_ranking(
    State(state): State<AppState>,
    Query(params): Query<GetParams>,
) -> Result<Response, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let stored = ranking_config::get(&conn, params.collection_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    match stored {
        None => Err((
            StatusCode::NOT_FOUND,
            format!("no ranking config for collection {}", params.collection_id),
        )),
        Some(json_str) => {
            // Parse back into a Value so axum serialises it as JSON.
            let value: serde_json::Value = serde_json::from_str(&json_str)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            Ok(Json(value).into_response())
        }
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/admin/ranking", put(put_ranking))
        .route("/api/admin/ranking", get(get_ranking))
}
