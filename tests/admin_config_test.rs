/// Integration tests for PUT /api/admin/config and GET /api/admin/config.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn auth_put(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

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

/// PUT with fanout_depth=2 and search_rate_limit_per_minute=60; GET must
/// return the same values.
#[tokio::test]
async fn put_config_persists_and_get_returns_it() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    // PUT config with two fields.
    let resp = app
        .clone()
        .oneshot(auth_put(
            "/api/admin/config",
            r#"{"fanout_depth": 2, "search_rate_limit_per_minute": 60}"#,
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "PUT /api/admin/config must return 200"
    );

    // GET config and verify the values are returned.
    let resp = app
        .clone()
        .oneshot(auth_get("/api/admin/config"))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "GET /api/admin/config must return 200"
    );

    let body = body_json(resp).await;
    assert_eq!(
        body["fanout_depth"], 2,
        "fanout_depth must be 2, got: {}",
        body["fanout_depth"]
    );
    assert_eq!(
        body["search_rate_limit_per_minute"], 60,
        "search_rate_limit_per_minute must be 60, got: {}",
        body["search_rate_limit_per_minute"]
    );
}

/// PUT search_rate_limit_per_minute=2; first 2 requests to /api/search succeed,
/// 3rd returns 429 (rate limited).
#[tokio::test]
async fn put_config_rate_limit_takes_effect_immediately() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    // Lower the rate limit to 2 requests per minute.
    let resp = app
        .clone()
        .oneshot(auth_put(
            "/api/admin/config",
            r#"{"search_rate_limit_per_minute": 2}"#,
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "PUT /api/admin/config must return 200"
    );

    // First 2 search requests must succeed.
    for i in 0..2 {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/search")
                    .header("content-type", "application/json")
                    .header("x-forwarded-for", "10.0.0.99")
                    .body(Body::from(r#"{"query": "test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "search request {} (within new limit of 2) must return 200",
            i + 1
        );
    }

    // 3rd request must be rate-limited.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/search")
                .header("content-type", "application/json")
                .header("x-forwarded-for", "10.0.0.99")
                .body(Body::from(r#"{"query": "test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "3rd search request must be rate-limited (429) after lowering limit to 2"
    );
}
