use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;

use crate::api::public::AppState;
use crate::db::collections::{create_item, delete_item, list_items, update_item};

// ---------------------------------------------------------------------------
// Request type
// ---------------------------------------------------------------------------

/// Action-discriminated request body for the collections admin endpoint.
///
/// Uses `serde(tag = "action")` so that the JSON field `"action"` selects
/// the variant:
///
/// - `{"action":"create","name":"...","description":"..."}`
/// - `{"action":"update","id":1,"name":"...","description":"..."}`
/// - `{"action":"delete","id":1}`
#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum CollectionAction {
    Create {
        name: String,
        description: String,
    },
    Update {
        id: i64,
        name: String,
        description: String,
    },
    Delete {
        id: i64,
    },
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// POST /api/admin/collections
///
/// Dispatches create / update / delete based on the `action` field.
///
/// ## Status codes
/// - 200 OK         — operation succeeded (create/update returns JSON body)
/// - 400 Bad Request — unknown or malformed action
/// - 404 Not Found  — update/delete target does not exist
/// - 500 Internal   — database error
async fn handle_collections(
    State(state): State<AppState>,
    // Extract raw Value first so that an unrecognised `action` produces 400
    // (axum's built-in Json extractor returns 422 on serde errors).
    Json(raw): Json<serde_json::Value>,
) -> Result<Response, (StatusCode, String)> {
    // Deserialise into the typed enum; map serde errors to 400.
    let action = serde_json::from_value::<CollectionAction>(raw)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid action: {e}")))?;

    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    match action {
        CollectionAction::Create { name, description } => {
            let record = create_item(&conn, &name, &description)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            Ok(Json(record).into_response())
        }

        CollectionAction::Update {
            id,
            name,
            description,
        } => {
            match update_item(&conn, id, &name, &description)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            {
                Some(record) => Ok(Json(record).into_response()),
                None => Err((StatusCode::NOT_FOUND, format!("collection {id} not found"))),
            }
        }

        CollectionAction::Delete { id } => {
            let found = delete_item(&conn, id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            if found {
                Ok(StatusCode::OK.into_response())
            } else {
                Err((StatusCode::NOT_FOUND, format!("collection {id} not found")))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// GET /api/admin/collections
///
/// Lists all collections with their SQLite rowid as the integer `id`, so the
/// admin UI can route to a specific collection and pass `collection_id` to
/// the other admin endpoints (crawl-targets, outlinks, ranking).
///
/// ## Status codes
/// - 200 OK         — JSON array of `{id, name, description}` records
/// - 500 Internal   — database error
async fn list_collections(State(state): State<AppState>) -> Result<Response, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let rows = list_items(&conn).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(rows).into_response())
}

pub fn routes() -> Router<AppState> {
    Router::new().route(
        "/api/admin/collections",
        get(list_collections).post(handle_collections),
    )
}
