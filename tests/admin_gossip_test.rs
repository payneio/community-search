/// Integration tests for POST /api/admin/gossip/trigger.
use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use tower::ServiceExt;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

const ADMIN_TOKEN: &str = "test-admin-token";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn auth_post(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
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

/// POST /api/admin/gossip/trigger with a valid bearer token and an arbitrary
/// peer URL performs an immediate gossip exchange: the mock peer receives
/// exactly 1 POST, and the response body has status="ok" and received_count>=1.
#[tokio::test]
async fn admin_can_trigger_gossip_with_arbitrary_url() {
    // Start a mock peer that simulates /api/gossip/exchange.
    let peer = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/gossip/exchange"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "protocol_version": "1.0",
            "engines": [
                {"url": "https://from-peer.example.com", "name": "Peer Engine", "description": ""}
            ]
        })))
        .expect(1)
        .mount(&peer)
        .await;

    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    let resp = app
        .oneshot(auth_post(
            "/api/admin/gossip/trigger",
            &format!(r#"{{"url":"{}"}}"#, peer.uri()),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK, "must return 200");

    let body = body_json(resp).await;
    assert_eq!(
        body["status"].as_str(),
        Some("ok"),
        "status must be 'ok', got: {}",
        body["status"]
    );
    assert!(
        body["received_count"].as_u64().unwrap_or(0) >= 1,
        "received_count must be >= 1, got: {}",
        body["received_count"]
    );

    // Verify the mock received exactly 1 request (enforced by .expect(1)).
    peer.verify().await;
}

/// POST /api/admin/gossip/trigger without an Authorization header must return
/// 401 UNAUTHORIZED.
#[tokio::test]
async fn admin_gossip_trigger_requires_auth() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin/gossip/trigger")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"url":"http://some.peer"}"#))
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
