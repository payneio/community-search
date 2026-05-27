/// Integration tests for the admin outlinks endpoints.
///
/// Tests: list pending outlinks, promote outlink (creates crawl target),
/// dismiss outlink (filters from list).
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token";

// ---------------------------------------------------------------------------
// Request helpers
// ---------------------------------------------------------------------------

fn auth_get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .body(Body::empty())
        .unwrap()
}

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

/// GET /api/admin/outlinks?collection_id=1 returns the seeded pending row.
#[tokio::test]
async fn list_outlinks_returns_seeded_rows() {
    let app = community_search::test_support::test_app(ADMIN_TOKEN);

    let _outlink_id =
        community_search::test_support::seed_outlink("https://target.example.com/", "a link", &app)
            .await;

    // seed_outlink creates the first collection (rowid=1).
    let resp = app
        .router
        .clone()
        .oneshot(auth_get("/api/admin/outlinks?collection_id=1"))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK, "list should return 200");

    let body = body_json(resp).await;
    let outlinks = body["outlinks"]
        .as_array()
        .expect("response must have 'outlinks' array");
    assert_eq!(outlinks.len(), 1, "should have exactly 1 seeded outlink");
    assert_eq!(
        outlinks[0]["host"].as_str(),
        Some("target.example.com"),
        "host must match the seeded URL's host"
    );
    // Examples array carries the original URL + anchor text.
    let examples = outlinks[0]["examples"]
        .as_array()
        .expect("examples must be present");
    assert_eq!(examples.len(), 1);
    assert_eq!(
        examples[0]["target_url"].as_str(),
        Some("https://target.example.com/")
    );
}

/// POST /api/admin/outlinks/:id/promote creates a real crawl target and returns its id.
#[tokio::test]
async fn promote_outlink_creates_crawl_target() {
    let app = community_search::test_support::test_app(ADMIN_TOKEN);

    let outlink_id = community_search::test_support::seed_outlink(
        "https://promote-target.example.com/",
        "promoted link",
        &app,
    )
    .await;

    let resp = app
        .router
        .clone()
        .oneshot(auth_post(
            &format!("/api/admin/outlinks/{outlink_id}/promote"),
            r#"{"recrawl_interval_secs": 3600}"#,
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK, "promote should return 200");

    let body = body_json(resp).await;
    let ct_id = body["crawl_target_id"]
        .as_i64()
        .expect("response must contain 'crawl_target_id' as integer");
    assert!(ct_id > 0, "crawl_target_id must be a positive integer");
}

/// POST /api/admin/outlinks/:id/dismiss marks the outlink dismissed so
/// subsequent list calls no longer include it.
#[tokio::test]
async fn dismiss_outlink_marks_it_dismissed() {
    let app = community_search::test_support::test_app(ADMIN_TOKEN);

    let outlink_id = community_search::test_support::seed_outlink(
        "https://dismiss-target.example.com/",
        "dismiss me",
        &app,
    )
    .await;

    // Confirm it appears in the pending list before dismissal.
    let resp = app
        .router
        .clone()
        .oneshot(auth_get("/api/admin/outlinks?collection_id=1"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(
        body["outlinks"].as_array().unwrap().len(),
        1,
        "outlink should appear in list before dismissal"
    );

    // Dismiss the outlink.
    let resp = app
        .router
        .clone()
        .oneshot(auth_post(
            &format!("/api/admin/outlinks/{outlink_id}/dismiss"),
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "dismiss should return 200");

    // Verify it no longer appears in the pending list.
    let resp = app
        .router
        .clone()
        .oneshot(auth_get("/api/admin/outlinks?collection_id=1"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(
        body["outlinks"].as_array().unwrap().len(),
        0,
        "dismissed outlink must not appear in the pending list"
    );
}
