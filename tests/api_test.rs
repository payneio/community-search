use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

use rusqlite::Connection;
use tantivy::Index;
use tokio::net::TcpListener;

use community_search::api::public::{AppState, SharedDb};
use community_search::api::router::build_router;
use community_search::federation::peer::HttpPeerClient;
use community_search::index::{reader::Searcher, schema};
use community_search::search::service::SearchService;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Build an in-memory `AppState` suitable for integration tests.
fn build_test_state() -> AppState {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    conn.execute_batch(include_str!("../src/db/migrations/001_init.sql"))
        .expect("apply consolidated schema");
    let db: SharedDb = Arc::new(Mutex::new(conn));

    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));

    let default_rl = community_search::api::rate_limit::RateLimitConfig::default();
    AppState {
        admin_token: String::from("test-admin-token"),
        self_url: String::new(),
        db,
        search: Arc::new(service),
        rate_limit_config: Arc::new(RwLock::new(default_rl.clone())),
        peer_rate_limit_config: Arc::new(RwLock::new(
            community_search::api::rate_limit::RateLimitConfig {
                limit: 120,
                ..default_rl
            },
        )),
        peer_ip_cache: Arc::new(RwLock::new(
            community_search::api::rate_limit::PeerIpCache::new(),
        )),
        runtime_config: Arc::new(RwLock::new(community_search::RuntimeConfig::default())),
        index_path: std::path::PathBuf::from("/tmp/community-search-test-index-nonexistent"),
        max_index_bytes: 10_737_418_240,
        peer_client: Arc::new(
            HttpPeerClient::new(Duration::from_secs(10)).expect("create HttpPeerClient"),
        ),
        http_client: reqwest::Client::builder()
            .user_agent("community-search-test/0.1")
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build reqwest client"),
        crawler_user_agent: "community-search-test/0.1".into(),
        indexer_delete_tx: community_search::test_support::sink_indexer_delete_tx(),
        indexer_upsert_tx: community_search::test_support::sink_indexer_upsert_tx(),
        crawl_paused: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        indexing_inflight: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
    }
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

/// The root endpoint (`GET /`) must return 200 after router wiring.
#[tokio::test]
async fn root_endpoint_returns_ok() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = build_test_state();

    let server = tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });

    let resp = reqwest::get(format!("http://{addr}/")).await.unwrap();
    assert_eq!(resp.status(), 200, "GET / must return 200");

    server.abort();
}

/// Unregistered paths must yield 404.
#[tokio::test]
async fn unknown_path_returns_404() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = build_test_state();

    let server = tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });

    let resp = reqwest::get(format!("http://{addr}/nope")).await.unwrap();
    assert_eq!(resp.status(), 404, "unknown path must return 404");

    server.abort();
}

// ── SSE search integration test ────────────────────────────────────────────
//
// Requires community_search::testing::TestApp (Task 13).
// Uncomment once Task 13 provides that module.
//
// The fixture data expected by this test:
//   - collection "tech" contains a document with url "a.example/1" and body mentioning "tokio"
//   - "b.example/2" does NOT appear in results for query "tokio" in collection "tech"

#[cfg(feature = "testing")]
#[tokio::test]
async fn search_streams_local_results_then_done() {
    let app = community_search::testing::TestApp::new().await;

    let resp = reqwest::Client::new()
        .post(format!("{}/api/search", app.base_url()))
        .json(&serde_json::json!({
            "query": "tokio",
            "collection": "tech",
            "depth": 0
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "expected 200 from /api/search");

    let ct = resp
        .headers()
        .get("content-type")
        .expect("missing content-type header")
        .to_str()
        .unwrap();
    assert!(
        ct.starts_with("text/event-stream"),
        "expected text/event-stream content-type, got: {ct}"
    );

    assert_eq!(
        resp.headers()
            .get("x-communitysearch-version")
            .expect("missing x-communitysearch-version header")
            .to_str()
            .unwrap(),
        "1.0",
        "x-communitysearch-version must be 1.0"
    );

    let body = resp.text().await.unwrap();

    assert!(
        body.contains("event: result"),
        "body should contain 'event: result'; got:\n{body}"
    );
    assert!(
        body.contains("event: source_complete"),
        "body should contain 'event: source_complete'; got:\n{body}"
    );
    assert!(
        body.contains("event: done"),
        "body should contain 'event: done'; got:\n{body}"
    );
    assert!(
        body.contains("a.example/1"),
        "body should contain 'a.example/1' (tech/tokio result); got:\n{body}"
    );
    assert!(
        !body.contains("b.example/2"),
        "body should NOT contain 'b.example/2' (not in tech/tokio); got:\n{body}"
    );
}
