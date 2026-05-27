use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

use community_search::test_support::test_router_with_search;

// ---------------------------------------------------------------------------
// Integration tests for rate-limit middleware on POST /api/search
// ---------------------------------------------------------------------------

/// The 31st request from the same IP within the sliding window must return 429
/// with a `Retry-After` header.  The first 30 must return 200.
#[tokio::test]
async fn search_endpoint_returns_429_after_limit() {
    let app = test_router_with_search();

    // Send 30 requests (at the default limit) — all must succeed.
    for i in 0..30 {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/search")
                    .header("content-type", "application/json")
                    .header("x-forwarded-for", "1.2.3.4")
                    .body(Body::from(r#"{"query":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "request {i} (within limit) must return 200"
        );
    }

    // The 31st request must be rate-limited.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/search")
                .header("content-type", "application/json")
                .header("x-forwarded-for", "1.2.3.4")
                .body(Body::from(r#"{"query":"test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "31st request must return 429"
    );
    assert!(
        resp.headers().contains_key("retry-after"),
        "retry-after header must be present on 429 response"
    );

    // Body must say "rate limited".
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        body_bytes.as_ref(),
        b"rate limited",
        "body must be 'rate limited'"
    );
}

/// Requests from distinct IPs must be tracked independently.
///
/// After 30 requests from 1.1.1.1, a single request from 2.2.2.2 must NOT
/// be rate-limited.
#[tokio::test]
async fn different_ips_are_tracked_independently() {
    let app = test_router_with_search();

    // Saturate the limit for 1.1.1.1.
    for i in 0..30 {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/search")
                    .header("content-type", "application/json")
                    .header("x-forwarded-for", "1.1.1.1")
                    .body(Body::from(r#"{"query":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "1.1.1.1 request {i} must return 200"
        );
    }

    // A single request from 2.2.2.2 must NOT be rate-limited.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/search")
                .header("content-type", "application/json")
                .header("x-forwarded-for", "2.2.2.2")
                .body(Body::from(r#"{"query":"test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "2.2.2.2 must not be rate-limited after 1.1.1.1 saturated its limit"
    );
}
