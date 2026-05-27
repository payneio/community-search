use std::sync::{Arc, Mutex};
use std::time::Duration;

use community_search::api::public::{AppState, SharedDb};
use community_search::api::rate_limit::{PeerIpCache, RateLimitConfig};
use community_search::api::router::build_router;
use community_search::federation::peer::HttpPeerClient;
use community_search::federation::storage::get_node_peer;
use community_search::index::{reader::Searcher, schema};
use community_search::search::service::SearchService;
use community_search::RuntimeConfig;

use rusqlite::Connection;
use tantivy::Index;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn build_test_state() -> AppState {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    community_search::db::run_migrations(&conn).expect("run all migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));
    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));
    let default_rl = RateLimitConfig::default();
    AppState {
        admin_token: String::from("test-admin-token"),
        self_url: String::new(),
        db,
        search: Arc::new(service),
        rate_limit_config: Arc::new(RwLock::new(default_rl.clone())),
        peer_rate_limit_config: Arc::new(RwLock::new(RateLimitConfig {
            limit: 120,
            ..default_rl
        })),
        peer_ip_cache: Arc::new(RwLock::new(PeerIpCache::new())),
        runtime_config: Arc::new(RwLock::new(RuntimeConfig::default())),
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
    }
}

/// Build a test server bound to a random port with specific anonymous and peer
/// rate-limit buckets.  Returns the base URL, a reusable HTTP client, and a
/// join handle for the server task (call `.abort()` after the test).
async fn test_app_with_limits(
    anon_limit: u32,
    peer_limit: u32,
) -> (String, reqwest::Client, tokio::task::JoinHandle<()>) {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    community_search::db::run_migrations(&conn).expect("run all migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));
    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));

    let anon_cfg = RateLimitConfig {
        limit: anon_limit,
        ..RateLimitConfig::default()
    };
    let peer_cfg = RateLimitConfig {
        limit: peer_limit,
        ..RateLimitConfig::default()
    };

    let state = AppState {
        admin_token: String::from("test-admin-token"),
        self_url: String::new(),
        db,
        search: Arc::new(service),
        rate_limit_config: Arc::new(RwLock::new(anon_cfg)),
        peer_rate_limit_config: Arc::new(RwLock::new(peer_cfg)),
        peer_ip_cache: Arc::new(RwLock::new(PeerIpCache::new())),
        runtime_config: Arc::new(RwLock::new(RuntimeConfig::default())),
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
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });

    let base_url = format!("http://{addr}");
    let client = reqwest::Client::new();

    (base_url, client, server_handle)
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

/// Helper: spawn a wiremock peer returning one SSE result with the given
/// title, register it as a node + collection peer for collection "rust", POST
/// /api/search with remaining_depth:1, and return the full response body.
///
/// Extracted from the setup pattern used in integration tests so each test
/// only asserts its specific concern.
async fn setup_peer_and_search(result_title: &str) -> String {
    let peer_server = MockServer::start().await;

    let remote_result = serde_json::json!({
        "title": result_title,
        "url": format!("https://remote.example/{}", result_title.to_lowercase()),
        "snippet_html": "",
        "source": "local",
        "timestamp": 0,
        "score": 1.0
    });
    let sse_body = format!(
        "event: result\ndata: {}\n\nevent: complete\ndata: {{}}\n\n",
        remote_result
    );

    Mock::given(method("POST"))
        .and(path("/api/search"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_body)
                .set_delay(Duration::from_millis(50)),
        )
        .mount(&peer_server)
        .await;

    let state = build_test_state();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });

    let base_url = format!("http://{addr}");
    let client = reqwest::Client::new();

    // Register mock peer as a node peer via admin API.
    let node_resp = client
        .post(format!("{base_url}/api/admin/nodes"))
        .header("Authorization", "Bearer test-admin-token")
        .json(&serde_json::json!({
            "url": peer_server.uri(),
            "name": "test-peer"
        }))
        .send()
        .await
        .expect("POST /api/admin/nodes");
    assert_eq!(
        node_resp.status(),
        201,
        "POST /api/admin/nodes must return 201"
    );
    let node_id: i64 = node_resp.json().await.expect("parse node peer id");

    // Register as a collection peer for "rust".
    let cp_resp = client
        .post(format!("{base_url}/api/admin/collection-peers"))
        .header("Authorization", "Bearer test-admin-token")
        .json(&serde_json::json!({
            "local_collection": "rust",
            "node_peer_id": node_id,
            "remote_collection": "rust",
            "source_weight": 1.0
        }))
        .send()
        .await
        .expect("POST /api/admin/collection-peers");
    assert_eq!(
        cp_resp.status(),
        201,
        "POST /api/admin/collection-peers must return 201"
    );

    // POST /api/search with remaining_depth:1 to trigger fanout.
    let search_resp = client
        .post(format!("{base_url}/api/search"))
        .json(&serde_json::json!({
            "query": "q",
            "collection": "rust",
            "remaining_depth": 1
        }))
        .send()
        .await
        .expect("POST /api/search");
    assert_eq!(
        search_resp.status(),
        200,
        "POST /api/search must return 200"
    );

    let body = search_resp.text().await.expect("read response body");
    server_handle.abort();
    body
}

/// Helper: register a node peer and collection peer for collection "rust" via the
/// admin API.  Returns the newly created `node_peer_id`.
async fn setup_node_and_collection_peer(
    base_url: &str,
    client: &reqwest::Client,
    peer_uri: &str,
    peer_name: &str,
) -> i64 {
    let node_resp = client
        .post(format!("{base_url}/api/admin/nodes"))
        .header("Authorization", "Bearer test-admin-token")
        .json(&serde_json::json!({
            "url": peer_uri,
            "name": peer_name
        }))
        .send()
        .await
        .expect("POST /api/admin/nodes");
    assert_eq!(
        node_resp.status(),
        201,
        "POST /api/admin/nodes must return 201"
    );
    let node_id: i64 = node_resp.json().await.expect("parse node peer id");

    let cp_resp = client
        .post(format!("{base_url}/api/admin/collection-peers"))
        .header("Authorization", "Bearer test-admin-token")
        .json(&serde_json::json!({
            "local_collection": "rust",
            "node_peer_id": node_id,
            "remote_collection": "rust",
            "source_weight": 1.0
        }))
        .send()
        .await
        .expect("POST /api/admin/collection-peers");
    assert_eq!(
        cp_resp.status(),
        201,
        "POST /api/admin/collection-peers must return 201"
    );

    node_id
}

/// After each peer's results a `source_complete` SSE event must be emitted.
///
/// The event gives the UI a per-peer progress signal so it can show a
/// completion indicator as each peer finishes rather than waiting for the
/// terminal `done` event.
///
/// With one peer in the fanout there should be at least two `source_complete`
/// events: one for the local source and one for the peer source.  Before the
/// implementation only the local event is emitted, so we assert count >= 2 to
/// ensure the peer-specific event is present.
#[tokio::test]
async fn fanout_emits_source_complete_per_peer() {
    let body = setup_peer_and_search("REMOTE").await;

    let count = body.matches("event: source_complete").count();
    assert!(
        count >= 2,
        "SSE body must contain at least 2 'event: source_complete' events \
         (one for local, one for the peer); found {count}; body:\n{body}"
    );
}

/// POST /api/search fans out to collection peers when remaining_depth > 0.
///
/// Sets up a wiremock peer returning a single SSE result with title "REMOTE",
/// registers it as a node peer + collection peer for collection "rust",
/// then verifies that the search response includes the "REMOTE" result
/// before the terminal `event: done` event.
#[tokio::test]
async fn public_search_streams_local_then_peer_results() {
    // --- Step 1: Set up wiremock peer ---
    let peer_server = MockServer::start().await;

    let remote_result = serde_json::json!({
        "title": "REMOTE",
        "url": "https://remote.example/1",
        "snippet_html": "",
        "source": "local",
        "timestamp": 0,
        "score": 1.0
    });
    let sse_body = format!(
        "event: result\ndata: {}\n\nevent: complete\ndata: {{}}\n\n",
        remote_result
    );

    Mock::given(method("POST"))
        .and(path("/api/search"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_body)
                .set_delay(Duration::from_millis(50)),
        )
        .mount(&peer_server)
        .await;

    // --- Step 2: Build and start the local server ---
    let state = build_test_state();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });

    let base_url = format!("http://{addr}");
    let client = reqwest::Client::new();

    // --- Step 3: Register mock peer as a node peer via admin API ---
    let node_resp = client
        .post(format!("{base_url}/api/admin/nodes"))
        .header("Authorization", "Bearer test-admin-token")
        .json(&serde_json::json!({
            "url": peer_server.uri(),
            "name": "test-peer"
        }))
        .send()
        .await
        .expect("POST /api/admin/nodes");
    assert_eq!(
        node_resp.status(),
        201,
        "POST /api/admin/nodes must return 201"
    );
    let node_id: i64 = node_resp.json().await.expect("parse node peer id");

    // --- Step 4: Register as a collection peer for "rust" ---
    let cp_resp = client
        .post(format!("{base_url}/api/admin/collection-peers"))
        .header("Authorization", "Bearer test-admin-token")
        .json(&serde_json::json!({
            "local_collection": "rust",
            "node_peer_id": node_id,
            "remote_collection": "rust",
            "source_weight": 1.0
        }))
        .send()
        .await
        .expect("POST /api/admin/collection-peers");
    assert_eq!(
        cp_resp.status(),
        201,
        "POST /api/admin/collection-peers must return 201"
    );

    // --- Step 5: POST /api/search with remaining_depth:1 ---
    let search_resp = client
        .post(format!("{base_url}/api/search"))
        .json(&serde_json::json!({
            "query": "q",
            "collection": "rust",
            "remaining_depth": 1
        }))
        .send()
        .await
        .expect("POST /api/search");

    // --- Step 6: Assert 200 ---
    assert_eq!(
        search_resp.status(),
        200,
        "POST /api/search must return 200"
    );

    // --- Step 7: Collect body ---
    let body = search_resp.text().await.expect("read response body");

    // --- Step 8: Assert body contains "REMOTE" ---
    assert!(
        body.contains("REMOTE"),
        "SSE body must contain 'REMOTE' from peer; body:\n{body}"
    );

    // --- Step 9: Assert REMOTE appears before the terminal done event ---
    let remote_pos = body.find("REMOTE").expect("'REMOTE' in body");
    let done_pos = body.find("event: done").expect("'event: done' in body");
    assert!(
        remote_pos < done_pos,
        "'REMOTE' (pos {remote_pos}) must appear before 'event: done' (pos {done_pos})\nbody:\n{body}"
    );

    server.abort();
}

/// A peer that returns HTTP 500 on POST /api/search must cause
/// `consecutive_failures` to be incremented on the corresponding node peer
/// after a fanout search.
///
/// This validates that `record_result` is called with `success = false` when
/// the peer dispatch returns an error.
#[tokio::test]
async fn failed_peer_increments_failure_counter() {
    // Set up a mock peer that always returns 500.
    let peer_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/search"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&peer_server)
        .await;

    // Build the app state and retain a handle to the db for later assertions.
    let state = build_test_state();
    let db = Arc::clone(&state.db);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });

    let base_url = format!("http://{addr}");
    let client = reqwest::Client::new();

    let node_id =
        setup_node_and_collection_peer(&base_url, &client, &peer_server.uri(), "fail-peer").await;

    // Issue a search with remaining_depth:1 to trigger fanout.
    let search_resp = client
        .post(format!("{base_url}/api/search"))
        .json(&serde_json::json!({
            "query": "q",
            "collection": "rust",
            "remaining_depth": 1
        }))
        .send()
        .await
        .expect("POST /api/search");
    assert_eq!(search_resp.status(), 200, "search must return 200");

    // Consume the full body so the SSE stream (and the spawned handler task)
    // completes before we inspect the database.
    let _body = search_resp.text().await.expect("read response body");

    // The fanout task calls record_result before sending Done, so by the time
    // the SSE stream is fully consumed the DB update is committed.
    let peer = {
        let conn = db.lock().expect("db mutex poisoned");
        get_node_peer(&conn, node_id)
            .expect("query node peer")
            .expect("peer should exist")
    };

    assert_eq!(
        peer.consecutive_failures, 1,
        "a failed fanout search must increment consecutive_failures to 1; got {}",
        peer.consecutive_failures
    );

    server_handle.abort();
}

/// Known peer node IPs must use the more generous peer rate-limit bucket.
///
/// Sets up a server with anon_limit=2 and peer_limit=5.  Registers
/// `http://127.0.0.1:9999` as a node peer so that `127.0.0.1` is a known peer
/// IP.  Issues 4 sequential POST /api/search requests with
/// `X-Forwarded-For: 127.0.0.1`.  Without the peer bucket all 4 would use the
/// anon limit and the third request would get 429; with the peer bucket all 4
/// must succeed (peer_limit=5 ≥ 4 requests).
#[tokio::test]
async fn peer_ips_use_more_generous_rate_limit() {
    // anon limit = 2/min, peer limit = 5/min
    let (base_url, client, server_handle) = test_app_with_limits(2, 5).await;

    // Register http://127.0.0.1:9999 as a node peer.
    // The rate-limiter will extract "127.0.0.1" as the peer host and match it
    // against the X-Forwarded-For client IP on search requests.
    let node_resp = client
        .post(format!("{base_url}/api/admin/nodes"))
        .header("Authorization", "Bearer test-admin-token")
        .json(&serde_json::json!({
            "url": "http://127.0.0.1:9999",
            "name": "test-peer"
        }))
        .send()
        .await
        .expect("POST /api/admin/nodes");
    assert_eq!(
        node_resp.status(),
        201,
        "POST /api/admin/nodes must return 201"
    );

    // Issue 4 sequential POST /api/search requests from IP 127.0.0.1.
    // With anon_limit=2 only the first two would succeed; with peer_limit=5 all
    // four must return 200.
    for i in 1u32..=4 {
        let resp = client
            .post(format!("{base_url}/api/search"))
            .header("X-Forwarded-For", "127.0.0.1")
            .json(&serde_json::json!({ "query": "rust" }))
            .send()
            .await
            .unwrap_or_else(|e| panic!("POST /api/search request {i} failed: {e}"));
        assert_eq!(
            resp.status(),
            200,
            "request {i} from peer IP 127.0.0.1 must succeed under peer_limit=5 (anon_limit=2); \
             got status {}",
            resp.status()
        );
        // Consume the body to release the connection before the next request.
        let _ = resp.text().await;
    }

    server_handle.abort();
}

/// End-to-end integration gate for Phase 5: two-peer fanout.
///
/// Spins up two wiremock servers (p1, p2), registers both as node peers with
/// collection peers for the local "rust" collection (source_weight 1.0 and 0.5
/// respectively), issues a federated search with remaining_depth:1, and asserts
/// that:
///   - Both peer results ("P1", "P2") appear in the SSE stream.
///   - Exactly two `source_complete` events are emitted (one per peer source).
#[tokio::test]
async fn end_to_end_two_peer_search() {
    // --- Spin up two wiremock peer servers ---

    let p1_result = serde_json::json!({
        "title": "P1",
        "url": "https://p1.example/1",
        "snippet_html": "",
        "source": "local",
        "timestamp": 0,
        "score": 2.0
    });
    let p1_sse = format!(
        "event: result\ndata: {}\n\nevent: complete\ndata: {{}}\n\n",
        p1_result
    );

    let p1_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/search"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(p1_sse)
                .set_delay(Duration::from_millis(50)),
        )
        .mount(&p1_server)
        .await;

    let p2_result = serde_json::json!({
        "title": "P2",
        "url": "https://p2.example/1",
        "snippet_html": "",
        "source": "local",
        "timestamp": 0,
        "score": 2.0
    });
    let p2_sse = format!(
        "event: result\ndata: {}\n\nevent: complete\ndata: {{}}\n\n",
        p2_result
    );

    let p2_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/search"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(p2_sse)
                .set_delay(Duration::from_millis(50)),
        )
        .mount(&p2_server)
        .await;

    // --- Build and start the local server ---

    let state = build_test_state();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });
    let base_url = format!("http://{addr}");
    let client = reqwest::Client::new();

    // --- Register p1: node peer + collection peer for "rust" with source_weight 1.0 ---

    let p1_node_resp = client
        .post(format!("{base_url}/api/admin/nodes"))
        .header("Authorization", "Bearer test-admin-token")
        .json(&serde_json::json!({
            "url": p1_server.uri(),
            "name": "p1"
        }))
        .send()
        .await
        .expect("POST /api/admin/nodes for p1");
    assert_eq!(p1_node_resp.status(), 201, "p1 node must return 201");
    let p1_node_id: i64 = p1_node_resp.json().await.expect("parse p1 node id");

    let p1_cp_resp = client
        .post(format!("{base_url}/api/admin/collection-peers"))
        .header("Authorization", "Bearer test-admin-token")
        .json(&serde_json::json!({
            "local_collection": "rust",
            "node_peer_id": p1_node_id,
            "remote_collection": "rust",
            "source_weight": 1.0
        }))
        .send()
        .await
        .expect("POST /api/admin/collection-peers for p1");
    assert_eq!(
        p1_cp_resp.status(),
        201,
        "p1 collection peer must return 201"
    );

    // --- Register p2: node peer + collection peer for "rust" with source_weight 0.5 ---

    let p2_node_resp = client
        .post(format!("{base_url}/api/admin/nodes"))
        .header("Authorization", "Bearer test-admin-token")
        .json(&serde_json::json!({
            "url": p2_server.uri(),
            "name": "p2"
        }))
        .send()
        .await
        .expect("POST /api/admin/nodes for p2");
    assert_eq!(p2_node_resp.status(), 201, "p2 node must return 201");
    let p2_node_id: i64 = p2_node_resp.json().await.expect("parse p2 node id");

    let p2_cp_resp = client
        .post(format!("{base_url}/api/admin/collection-peers"))
        .header("Authorization", "Bearer test-admin-token")
        .json(&serde_json::json!({
            "local_collection": "rust",
            "node_peer_id": p2_node_id,
            "remote_collection": "rust",
            "source_weight": 0.5
        }))
        .send()
        .await
        .expect("POST /api/admin/collection-peers for p2");
    assert_eq!(
        p2_cp_resp.status(),
        201,
        "p2 collection peer must return 201"
    );

    // --- POST /api/search with remaining_depth:1 to trigger fanout to both peers ---

    let search_resp = client
        .post(format!("{base_url}/api/search"))
        .json(&serde_json::json!({
            "query": "q",
            "collection": "rust",
            "remaining_depth": 1
        }))
        .send()
        .await
        .expect("POST /api/search");
    assert_eq!(search_resp.status(), 200, "search must return 200");

    let body = search_resp.text().await.expect("read response body");

    // --- Assertions ---

    assert!(
        body.contains("P1"),
        "SSE body must contain 'P1' from p1 peer; body:\n{body}"
    );
    assert!(
        body.contains("P2"),
        "SSE body must contain 'P2' from p2 peer; body:\n{body}"
    );

    // Expect exactly 3 source_complete events: 1 for the local source and 1 for
    // each of the 2 peer sources (p1 and p2).
    let source_complete_count = body.matches("event: source_complete").count();
    assert_eq!(
        source_complete_count, 3,
        "SSE body must contain exactly 3 'event: source_complete' events \
         (1 for local + 1 per peer source); found {source_complete_count}; body:\n{body}"
    );

    server_handle.abort();
}
