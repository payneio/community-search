//! Admin API for federation node peers.
//!
//! Routes:
//! - `GET    /api/admin/nodes`      — list all node peers
//! - `POST   /api/admin/nodes`      — register a new node peer
//! - `PUT    /api/admin/nodes/:id`  — enable or disable a node peer
//! - `DELETE /api/admin/nodes/:id`  — remove a node peer

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, put},
    Json, Router,
};
use serde::Deserialize;

use crate::api::public::AppState;
use crate::collections::CollectionInfo;
use crate::federation::gossip::spawn_gossip_exchange_with_peer;
use crate::federation::storage::{
    delete_node_peer, get_node_peer, insert_node_peer, list_node_peers, set_node_peer_enabled,
    NodePeer,
};

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Body for `POST /api/admin/nodes`.
#[derive(Deserialize)]
pub struct AddNodePeerReq {
    /// The base URL of the peer node.
    pub url: String,
    /// An optional human-readable display name for the peer.
    pub name: Option<String>,
}

/// Body for `PUT /api/admin/nodes/:id`.
#[derive(Deserialize)]
pub struct UpdateNodePeerReq {
    /// Whether the peer should be enabled (`true`) or disabled (`false`).
    pub enabled: bool,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// GET /api/admin/nodes
///
/// Returns all node peers ordered by creation time (oldest first) as a JSON
/// array of [`NodePeer`] objects with HTTP 200 OK.
///
/// ## Status codes
/// - 200 OK           — list returned (may be empty)
/// - 401 Unauthorized — missing or invalid `Authorization: Bearer` header
/// - 500 Internal     — database error
pub async fn list_node_peers_handler(
    State(state): State<AppState>,
) -> Result<Json<Vec<NodePeer>>, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let peers =
        list_node_peers(&conn).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(peers))
}

/// POST /api/admin/nodes
///
/// Inserts a new node peer and returns its auto-assigned row ID as a JSON
/// integer with HTTP 201 CREATED.
///
/// ## Status codes
/// - 201 Created       — peer inserted; body is the new row ID as a JSON integer
/// - 401 Unauthorized  — missing or invalid `Authorization: Bearer` header
///   (enforced by the `route_layer` in [`crate::api::admin::admin_router`])
/// - 500 Internal      — database error
pub async fn add_node_peer(
    State(state): State<AppState>,
    Json(body): Json<AddNodePeerReq>,
) -> Result<(StatusCode, Json<i64>), (StatusCode, String)> {
    let id = {
        let conn = state.db.lock().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "db mutex poisoned".into(),
            )
        })?;

        insert_node_peer(&conn, &body.url, body.name.as_deref())
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        // MutexGuard dropped here — lock released before spawning the task.
    };

    // Trigger an immediate gossip exchange with the new peer without blocking
    // the HTTP response.  Errors are logged inside the task.
    spawn_gossip_exchange_with_peer(
        state.http_client.clone(),
        state.db.clone(),
        body.url.clone(),
    );

    Ok((StatusCode::CREATED, Json(id)))
}

/// PUT /api/admin/nodes/:id
///
/// Enables or disables the node peer identified by `:id`.  Returns HTTP 204
/// NO CONTENT on success (even if the peer does not exist — the state is
/// idempotent).
///
/// ## Status codes
/// - 204 No Content   — peer updated
/// - 401 Unauthorized — missing or invalid `Authorization: Bearer` header
/// - 500 Internal     — database error
pub async fn update_node_peer_handler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<UpdateNodePeerReq>,
) -> Result<StatusCode, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    set_node_peer_enabled(&conn, id, body.enabled)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/admin/nodes/:id/collections
///
/// Proxies `GET /api/collections` to the remote peer identified by `:id` and
/// returns its collection list as `Vec<CollectionInfo>`.
///
/// ## Status codes
/// - 200 OK            — collections returned from the remote peer
/// - 401 Unauthorized  — missing or invalid `Authorization: Bearer` header
/// - 404 Not Found     — no node peer with the given `:id`
/// - 500 Internal      — database error or peer communication error
pub async fn browse_node_collections_handler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Vec<CollectionInfo>>, (StatusCode, String)> {
    let peer_url = {
        let conn = state.db.lock().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "db mutex poisoned".into(),
            )
        })?;
        let peer = get_node_peer(&conn, id)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        match peer {
            Some(p) => p.url,
            None => {
                return Err((StatusCode::NOT_FOUND, format!("node peer {id} not found")));
            }
        }
        // conn (MutexGuard) is released here before the await below
    };

    let collections = state
        .peer_client
        .list_collections(&peer_url)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(collections))
}

/// DELETE /api/admin/nodes/:id
///
/// Removes the node peer identified by `:id`.  Returns HTTP 204 NO CONTENT
/// on success (even if the peer did not exist).
///
/// ## Status codes
/// - 204 No Content   — peer deleted (or was already absent)
/// - 401 Unauthorized — missing or invalid `Authorization: Bearer` header
/// - 500 Internal     — database error
pub async fn delete_node_peer_handler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    delete_node_peer(&conn, id).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Register the nodes routes.
///
/// Auth is enforced by the `route_layer` in the parent [`admin_router`].
///
/// [`admin_router`]: crate::api::admin::admin_router
pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/admin/nodes",
            get(list_node_peers_handler).post(add_node_peer),
        )
        .route(
            "/api/admin/nodes/:id",
            put(update_node_peer_handler).delete(delete_node_peer_handler),
        )
        .route(
            "/api/admin/nodes/:id/collections",
            get(browse_node_collections_handler),
        )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod federation_admin_tests {
    use axum::{
        body::Body,
        http::{header, Request, StatusCode},
    };
    use tower::ServiceExt;

    /// GET /api/admin/nodes/:id/collections proxies to the remote peer via
    /// HttpPeerClient and returns the collection list as JSON.
    #[tokio::test]
    async fn get_admin_nodes_collections_proxies_to_peer() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // 1. Spin up a wiremock server that returns a single collection.
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/collections"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocol_version": "1.0",
                "collections": [{"name": "rust", "description": null}]
            })))
            .mount(&mock_server)
            .await;

        let ta = crate::test_support::test_app("test-admin-token");

        // 2. Register the wiremock server as a node peer.
        let resp = ta
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/admin/nodes")
                    .header(header::AUTHORIZATION, "Bearer test-admin-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(
                        r#"{{"url":"{}","name":"wiremock-peer"}}"#,
                        mock_server.uri()
                    )))
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
        let id: i64 =
            serde_json::from_slice(&bytes).expect("body must be the new peer ID as JSON integer");

        // 3. Browse remote collections via the admin endpoint.
        let resp = ta
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/admin/nodes/{id}/collections"))
                    .header(header::AUTHORIZATION, "Bearer test-admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // 4. Assert 200 with body[0].name == "rust".
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /api/admin/nodes/:id/collections must return 200"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let cols: Vec<serde_json::Value> =
            serde_json::from_slice(&bytes).expect("body must be a JSON array");
        assert_eq!(cols.len(), 1, "must return exactly one collection");
        assert_eq!(cols[0]["name"], "rust", "collection name must be 'rust'");
    }

    /// POST /api/admin/nodes with a valid Bearer token and a JSON body
    /// containing `url` and `name` must return 201 CREATED with the new
    /// peer ID as a JSON integer.
    #[tokio::test]
    async fn post_admin_nodes_creates_peer() {
        let ta = crate::test_support::test_app("test-admin-token");

        let resp = ta
            .router
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
            "POST /api/admin/nodes with valid auth must return 201"
        );

        // The body must be a JSON integer (the new peer's row ID).
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let id: i64 = serde_json::from_slice(&bytes)
            .expect("response body must be a JSON integer (peer row ID)");
        assert!(id > 0, "returned peer ID must be positive, got {id}");
    }

    /// POST /api/admin/nodes without an `Authorization` header must return
    /// 401 UNAUTHORIZED — auth is enforced by the admin router's middleware.
    #[tokio::test]
    async fn post_admin_nodes_rejects_without_token() {
        let ta = crate::test_support::test_app("test-admin-token");

        let resp = ta
            .router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/admin/nodes")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"url":"https://peer.example"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "POST /api/admin/nodes without auth must return 401"
        );
    }

    /// Full round-trip: create → list → disable → delete node peer.
    ///
    /// 1. POST /api/admin/nodes                      → 201 (new peer ID)
    /// 2. GET  /api/admin/nodes                      → 200 (list contains peer)
    /// 3. PUT  /api/admin/nodes/:id {enabled:false}  → 204
    /// 4. DELETE /api/admin/nodes/:id                → 204
    #[tokio::test]
    async fn list_disable_enable_delete_node_peer_round_trip() {
        let ta = crate::test_support::test_app("test-admin-token");

        // 1. Create a peer — POST /api/admin/nodes → 201
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
        let id: i64 =
            serde_json::from_slice(&bytes).expect("body must be the new peer ID as JSON integer");
        assert!(id > 0, "peer ID must be positive");

        // 2. List peers — GET /api/admin/nodes → 200, contains the created peer
        let resp = ta
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/admin/nodes")
                    .header(header::AUTHORIZATION, "Bearer test-admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /api/admin/nodes must return 200"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let peers: Vec<serde_json::Value> =
            serde_json::from_slice(&bytes).expect("GET /api/admin/nodes body must be a JSON array");
        assert_eq!(
            peers.len(),
            1,
            "list must contain exactly the one created peer"
        );
        assert_eq!(
            peers[0]["id"], id,
            "listed peer id must match the created peer id"
        );

        // 3. Disable the peer — PUT /api/admin/nodes/:id {enabled:false} → 204
        let resp = ta
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/api/admin/nodes/{id}"))
                    .header(header::AUTHORIZATION, "Bearer test-admin-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"enabled":false}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NO_CONTENT,
            "PUT /api/admin/nodes/:id must return 204"
        );

        // 4. Delete the peer — DELETE /api/admin/nodes/:id → 204
        let resp = ta
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/admin/nodes/{id}"))
                    .header(header::AUTHORIZATION, "Bearer test-admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NO_CONTENT,
            "DELETE /api/admin/nodes/:id must return 204"
        );
    }
}
