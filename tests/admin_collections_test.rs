/// Integration tests for `POST /api/admin/collections`.
///
/// Tests: create, update, delete, auth check, invalid-action handling.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn auth_post(uri: &str, json_body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .body(Body::from(json_body.to_string()))
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

/// POST with `action:'create'` must return 200 with `{id: <i64>, name: 'rust', ...}`.
#[tokio::test]
async fn create_collection_returns_id() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    let resp = app
        .oneshot(auth_post(
            "/api/admin/collections",
            r#"{"action":"create","name":"rust","description":"Rust programming"}"#,
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK, "create should return 200");

    let body = body_json(resp).await;
    assert!(
        body["id"].as_i64().is_some(),
        "id should be a JSON integer (i64), got: {}",
        body["id"]
    );
    assert_eq!(body["name"].as_str(), Some("rust"), "name should be 'rust'");
}

/// Full CRUD lifecycle: create → update → delete, each step must return 200.
#[tokio::test]
async fn create_then_update_then_delete() {
    // Clone the router each time so requests share the same in-memory DB.
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    // Step 1 — create
    let resp = app
        .clone()
        .oneshot(auth_post(
            "/api/admin/collections",
            r#"{"action":"create","name":"lifecycle","description":"initial"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "create step should return 200"
    );

    let created = body_json(resp).await;
    let id = created["id"]
        .as_i64()
        .expect("create must return an integer id");

    // Step 2 — update
    let update_payload = format!(
        r#"{{"action":"update","id":{id},"name":"lifecycle-updated","description":"changed"}}"#
    );
    let resp = app
        .clone()
        .oneshot(auth_post("/api/admin/collections", &update_payload))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "update step should return 200"
    );

    // Step 3 — delete
    let delete_payload = format!(r#"{{"action":"delete","id":{id}}}"#);
    let resp = app
        .clone()
        .oneshot(auth_post("/api/admin/collections", &delete_payload))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "delete step should return 200"
    );
}

/// Requests without a valid `Authorization` header must return 401.
#[tokio::test]
async fn create_collection_without_auth_is_401() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin/collections")
                .header("content-type", "application/json")
                // No Authorization header
                .body(Body::from(
                    r#"{"action":"create","name":"nope","description":""}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "missing auth should return 401"
    );
}

/// An unknown `action` value must return 400 (not 422).
#[tokio::test]
async fn invalid_action_returns_400() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    let resp = app
        .oneshot(auth_post("/api/admin/collections", r#"{"action":"bogus"}"#))
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "unknown action should return 400"
    );
}
