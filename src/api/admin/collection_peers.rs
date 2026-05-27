//! Admin API for collection peer subscriptions.
//!
//! Routes:
//! - `GET    /api/admin/collection-peers`      — list all collection peers
//! - `POST   /api/admin/collection-peers`      — subscribe a local collection to a remote
//! - `DELETE /api/admin/collection-peers/:id`  — remove a subscription

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::Deserialize;

use crate::api::public::AppState;
use crate::federation::storage::{
    delete_collection_peer, get_node_peer, insert_collection_peer, list_collection_peers,
    CollectionPeer,
};

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Body for `POST /api/admin/collection-peers`.
#[derive(Deserialize)]
pub struct SubscribeReq {
    pub local_collection: String,
    pub node_peer_id: i64,
    pub remote_collection: String,
    pub source_weight: f32,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /api/admin/collection-peers
///
/// Validates that the referenced `node_peer_id` exists, then inserts a new
/// collection peer mapping and returns its row ID with HTTP 201 CREATED.
///
/// ## Status codes
/// - 201 Created      — row inserted; body is the new row ID as a JSON integer
/// - 400 Bad Request  — `node_peer_id` does not refer to an existing node peer
/// - 401 Unauthorized — missing or invalid `Authorization: Bearer` header
/// - 500 Internal     — database error
pub async fn add_collection_peer_handler(
    State(state): State<AppState>,
    Json(body): Json<SubscribeReq>,
) -> Result<(StatusCode, Json<i64>), (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let peer = get_node_peer(&conn, body.node_peer_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if peer.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            "node_peer_id does not exist".into(),
        ));
    }

    let id = insert_collection_peer(
        &conn,
        &body.local_collection,
        body.node_peer_id,
        &body.remote_collection,
        body.source_weight,
    )
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((StatusCode::CREATED, Json(id)))
}

/// GET /api/admin/collection-peers
///
/// Returns all collection peer mappings ordered by creation time as a JSON
/// array with HTTP 200 OK.
///
/// ## Status codes
/// - 200 OK           — list returned (may be empty)
/// - 401 Unauthorized — missing or invalid `Authorization: Bearer` header
/// - 500 Internal     — database error
pub async fn list_collection_peers_handler(
    State(state): State<AppState>,
) -> Result<Json<Vec<CollectionPeer>>, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let peers = list_collection_peers(&conn, None)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(peers))
}

/// DELETE /api/admin/collection-peers/:id
///
/// Removes the collection peer identified by `:id`.  Returns HTTP 204 NO
/// CONTENT on success (even if the row did not exist).
///
/// ## Status codes
/// - 204 No Content   — row deleted (or was already absent)
/// - 401 Unauthorized — missing or invalid `Authorization: Bearer` header
/// - 500 Internal     — database error
pub async fn delete_collection_peer_handler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    delete_collection_peer(&conn, id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Register the collection-peers routes.
///
/// Auth is enforced by the `route_layer` in the parent [`admin_router`].
///
/// [`admin_router`]: crate::api::admin::admin_router
pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/admin/collection-peers",
            get(list_collection_peers_handler).post(add_collection_peer_handler),
        )
        .route(
            "/api/admin/collection-peers/:id",
            axum::routing::delete(delete_collection_peer_handler),
        )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod collection_peers_tests {
    use axum::{
        body::Body,
        http::{header, Request, StatusCode},
    };
    use tower::ServiceExt;

    /// Full round-trip: add node peer → subscribe → list → delete.
    ///
    /// 1. POST /api/admin/nodes                       → 201 (node peer ID)
    /// 2. POST /api/admin/collection-peers            → 201 (collection peer ID)
    /// 3. GET  /api/admin/collection-peers            → 200 (list contains the peer)
    /// 4. DELETE /api/admin/collection-peers/:id      → 204
    #[tokio::test]
    async fn collection_peers_crud_round_trip() {
        let ta = crate::test_support::test_app("test-admin-token");

        // 1. Add a node peer (prerequisite)
        let resp = ta
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/admin/nodes")
                    .header(header::AUTHORIZATION, "Bearer test-admin-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"url":"https://peer.example","name":"Peer A"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "POST /api/admin/nodes must return 201"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let node_peer_id: i64 =
            serde_json::from_slice(&bytes).expect("body must be node peer ID as JSON integer");
        assert!(node_peer_id > 0, "node peer ID must be positive");

        // 2. Subscribe — POST /api/admin/collection-peers → 201
        let sub_body = serde_json::json!({
            "local_collection": "local-col",
            "node_peer_id": node_peer_id,
            "remote_collection": "remote-col",
            "source_weight": 1.0_f32,
        });
        let resp = ta
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/admin/collection-peers")
                    .header(header::AUTHORIZATION, "Bearer test-admin-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(sub_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "POST /api/admin/collection-peers must return 201"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let cp_id: i64 = serde_json::from_slice(&bytes)
            .expect("body must be collection peer ID as JSON integer");
        assert!(cp_id > 0, "collection peer ID must be positive");

        // 3. List — GET /api/admin/collection-peers → 200
        let resp = ta
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/admin/collection-peers")
                    .header(header::AUTHORIZATION, "Bearer test-admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /api/admin/collection-peers must return 200"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let peers: Vec<serde_json::Value> =
            serde_json::from_slice(&bytes).expect("body must be a JSON array");
        assert_eq!(
            peers.len(),
            1,
            "list must contain exactly the one created peer"
        );
        assert_eq!(
            peers[0]["id"], cp_id,
            "listed peer id must match the created peer id"
        );

        // 4. Delete — DELETE /api/admin/collection-peers/:id → 204
        let resp = ta
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/admin/collection-peers/{cp_id}"))
                    .header(header::AUTHORIZATION, "Bearer test-admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NO_CONTENT,
            "DELETE /api/admin/collection-peers/:id must return 204"
        );
    }

    /// POST /api/admin/collection-peers with an unknown node_peer_id must
    /// return 400 BAD_REQUEST.
    #[tokio::test]
    async fn subscribe_rejects_unknown_node_peer() {
        let ta = crate::test_support::test_app("test-admin-token");

        let body = serde_json::json!({
            "local_collection": "local-col",
            "node_peer_id": 9999_i64,
            "remote_collection": "remote-col",
            "source_weight": 1.0_f32,
        });
        let resp = ta
            .router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/admin/collection-peers")
                    .header(header::AUTHORIZATION, "Bearer test-admin-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "POST with unknown node_peer_id must return 400 BAD_REQUEST"
        );
    }
}
