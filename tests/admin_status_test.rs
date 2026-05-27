/// Integration tests for GET /api/admin/status.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn auth_get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .body(Body::empty())
        .unwrap()
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// GET /api/admin/status with a valid token must return 200 with all 6
/// required fields in the correct types.  In Phase 4, `peers` is always `[]`.
#[tokio::test]
async fn status_returns_expected_shape() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    let resp = app.oneshot(auth_get("/api/admin/status")).await.unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "status endpoint must return 200"
    );

    let body = body_json(resp).await;

    // All 6 required fields must be present with integer types.
    assert!(
        body["index_size_bytes"].as_i64().is_some(),
        "index_size_bytes must be a JSON integer, got: {}",
        body["index_size_bytes"]
    );
    assert!(
        body["max_index_bytes"].as_i64().is_some(),
        "max_index_bytes must be a JSON integer, got: {}",
        body["max_index_bytes"]
    );
    assert!(
        body["crawl_targets_total"].as_i64().is_some(),
        "crawl_targets_total must be a JSON integer, got: {}",
        body["crawl_targets_total"]
    );
    assert!(
        body["crawls_active"].as_i64().is_some(),
        "crawls_active must be a JSON integer, got: {}",
        body["crawls_active"]
    );
    assert!(
        body["crawls_queued"].as_i64().is_some(),
        "crawls_queued must be a JSON integer, got: {}",
        body["crawls_queued"]
    );

    // peers must be an empty array in Phase 4.
    let peers = body["peers"]
        .as_array()
        .expect("peers must be a JSON array");
    assert!(peers.is_empty(), "peers must be an empty array in Phase 4");
}

/// GET /api/admin/status without an Authorization header must return 401.
#[tokio::test]
async fn status_without_token_is_401() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/admin/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "missing auth must return 401"
    );
}
