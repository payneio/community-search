/// Integration tests for `POST /api/admin/crawl-targets`.
///
/// Tests: add, set_interval, remove lifecycle; nonexistent collection 404.
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

/// Full lifecycle: create collection → add target → set_interval → remove.
///
/// Each step must return 200; add must return `{id, collection_id, url_prefix,
/// recrawl_interval_secs}`.
#[tokio::test]
async fn add_remove_target_and_change_interval() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    // Step 1 — create a collection via the existing admin endpoint
    let resp = app
        .clone()
        .oneshot(auth_post(
            "/api/admin/collections",
            r#"{"action":"create","name":"test-col","description":"test"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "create collection must return 200"
    );
    let body = body_json(resp).await;
    let col_id = body["id"].as_i64().expect("collection id must be i64");

    // Step 2 — add crawl target → 200 with target body
    let add_payload = format!(
        r#"{{"action":"add","collection_id":{col_id},"url_prefix":"https://example.com/","recrawl_interval_secs":3600}}"#
    );
    let resp = app
        .clone()
        .oneshot(auth_post("/api/admin/crawl-targets", &add_payload))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "add target must return 200");
    let body = body_json(resp).await;
    let target_id = body["id"].as_i64().expect("target id must be i64");
    assert_eq!(
        body["collection_id"].as_i64(),
        Some(col_id),
        "collection_id must match"
    );
    assert_eq!(
        body["url_prefix"].as_str(),
        Some("https://example.com/"),
        "url_prefix must match"
    );
    assert_eq!(
        body["recrawl_interval_secs"].as_i64(),
        Some(3600),
        "recrawl_interval_secs must match"
    );

    // Step 3 — set_interval → 200
    let interval_payload =
        format!(r#"{{"action":"set_interval","id":{target_id},"recrawl_interval_secs":7200}}"#);
    let resp = app
        .clone()
        .oneshot(auth_post("/api/admin/crawl-targets", &interval_payload))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "set_interval must return 200"
    );

    // Step 4 — remove → 200
    let remove_payload = format!(r#"{{"action":"remove","id":{target_id}}}"#);
    let resp = app
        .clone()
        .oneshot(auth_post("/api/admin/crawl-targets", &remove_payload))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "remove must return 200");
}

/// GET /api/admin/crawl-targets returns all configured targets, each joined
/// to its parent collection (collection_name and collection_id present).
#[tokio::test]
async fn list_crawl_targets_returns_added_target_with_collection_name() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    // Empty list before any setup.
    let resp = app
        .clone()
        .oneshot(auth_get("/api/admin/crawl-targets"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "GET must return 200");
    let body = body_json(resp).await;
    assert_eq!(
        body["crawl_targets"].as_array().map(|a| a.len()),
        Some(0),
        "list must be empty initially"
    );

    // Create collection.
    let resp = app
        .clone()
        .oneshot(auth_post(
            "/api/admin/collections",
            r#"{"action":"create","name":"list-col","description":""}"#,
        ))
        .await
        .unwrap();
    let col_id = body_json(resp).await["id"].as_i64().unwrap();

    // Add a crawl target.
    let add_payload = format!(
        r#"{{"action":"add","collection_id":{col_id},"url_prefix":"https://list.example/","recrawl_interval_secs":1800}}"#
    );
    let resp = app
        .clone()
        .oneshot(auth_post("/api/admin/crawl-targets", &add_payload))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // List should now contain the target with collection_name populated.
    let resp = app
        .clone()
        .oneshot(auth_get("/api/admin/crawl-targets"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let targets = body["crawl_targets"].as_array().expect("array");
    assert_eq!(targets.len(), 1, "expected one target");
    let t = &targets[0];
    assert_eq!(t["collection_id"].as_i64(), Some(col_id));
    assert_eq!(t["collection_name"].as_str(), Some("list-col"));
    assert_eq!(t["url_prefix"].as_str(), Some("https://list.example/"));
    assert_eq!(t["recrawl_interval_secs"].as_i64(), Some(1800));
    assert_eq!(t["enabled"].as_bool(), Some(true));
    assert!(t["last_crawled_at"].is_null());
    assert!(t["created_at"].is_string());
}

/// GET /api/admin/crawl-targets requires admin auth.
#[tokio::test]
async fn list_crawl_targets_requires_auth() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    let req = Request::builder()
        .method("GET")
        .uri("/api/admin/crawl-targets")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// Removing a crawl target must (a) cascade-delete its `crawled_pages` rows
/// in SQLite and (b) push the affected URLs to the indexer's delete channel
/// so the documents disappear from the search index too. Without (b) the
/// pages stay searchable until the index is rebuilt.
#[tokio::test]
async fn remove_target_pushes_page_urls_to_indexer_delete_channel() {
    use rusqlite::params;

    let mut app = community_search::test_support::test_app_with_delete_rx(ADMIN_TOKEN);

    // Seed: one collection, one crawl target, two crawled pages under it.
    {
        let conn = app.db.lock().unwrap();
        conn.execute_batch(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES ('col-uuid', 'demo', '', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'); \
             INSERT INTO crawl_targets \
                (id, collection_id, url_prefix, recrawl_interval_s, recrawl_interval_secs, enabled, created_at) \
             VALUES ('ct-uuid', 'col-uuid', 'https://example.com/', 86400, 86400, 1, '2024-01-01T00:00:00Z');",
        )
        .unwrap();
        for url in ["https://example.com/a", "https://example.com/b"] {
            conn.execute(
                "INSERT INTO crawled_pages \
                  (collection_id, crawl_target_id, url, content_hash, last_status, last_crawled_at) \
                 VALUES ('col-uuid', 'ct-uuid', ?1, 'h', 200, 1)",
                params![url],
            )
            .unwrap();
        }
    }

    // Look up the rowid the admin API uses to identify the target.
    let target_rowid: i64 = {
        let conn = app.db.lock().unwrap();
        conn.query_row(
            "SELECT rowid FROM crawl_targets WHERE id = 'ct-uuid'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    };

    let resp = app
        .router
        .clone()
        .oneshot(auth_post(
            "/api/admin/crawl-targets",
            &format!(r#"{{"action":"remove","id":{target_rowid}}}"#),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "remove must return 200");

    // The handler should have queued exactly one batch with both URLs.
    let batch = tokio::time::timeout(std::time::Duration::from_secs(1), app.delete_rx.recv())
        .await
        .expect("admin handler must push a delete batch within 1s")
        .expect("delete channel must still be open");
    let mut sorted = batch.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec![
            "https://example.com/a".to_string(),
            "https://example.com/b".to_string(),
        ],
        "all URLs that belonged to the removed target must be queued for index deletion"
    );

    // And the DB cascade must have already cleared `crawled_pages`.
    let remaining: i64 = {
        let conn = app.db.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM crawled_pages", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(remaining, 0, "FK cascade must wipe crawled_pages rows");
}

/// Removing a target that has no crawled pages must still succeed and must
/// NOT push an empty batch onto the indexer channel (empty batches are
/// pointless work for the indexer).
#[tokio::test]
async fn remove_empty_target_does_not_queue_delete_batch() {
    let mut app = community_search::test_support::test_app_with_delete_rx(ADMIN_TOKEN);

    {
        let conn = app.db.lock().unwrap();
        conn.execute_batch(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES ('col-uuid', 'demo', '', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'); \
             INSERT INTO crawl_targets \
                (id, collection_id, url_prefix, recrawl_interval_s, recrawl_interval_secs, enabled, created_at) \
             VALUES ('ct-uuid', 'col-uuid', 'https://example.com/', 86400, 86400, 1, '2024-01-01T00:00:00Z');",
        )
        .unwrap();
    }
    let target_rowid: i64 = {
        let conn = app.db.lock().unwrap();
        conn.query_row(
            "SELECT rowid FROM crawl_targets WHERE id = 'ct-uuid'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    };

    let resp = app
        .router
        .clone()
        .oneshot(auth_post(
            "/api/admin/crawl-targets",
            &format!(r#"{{"action":"remove","id":{target_rowid}}}"#),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let got = tokio::time::timeout(std::time::Duration::from_millis(200), app.delete_rx.recv())
        .await;
    assert!(
        got.is_err(),
        "removing a target with no pages must not push a batch; got: {got:?}"
    );
}

/// Adding a target for a collection that does not exist must return 404 or 400.
#[tokio::test]
async fn add_target_to_nonexistent_collection_is_400_or_404() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    let resp = app
        .oneshot(auth_post(
            "/api/admin/crawl-targets",
            r#"{"action":"add","collection_id":9999,"url_prefix":"https://example.com/","recrawl_interval_secs":86400}"#,
        ))
        .await
        .unwrap();

    assert!(
        resp.status() == StatusCode::NOT_FOUND || resp.status() == StatusCode::BAD_REQUEST,
        "nonexistent collection must return 404 or 400, got: {}",
        resp.status()
    );
}
