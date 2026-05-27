use std::sync::{Arc, Mutex};
use std::time::Duration;

use community_search::api::public::{AppState, SharedDb};
use community_search::api::rate_limit::{PeerIpCache, RateLimitConfig};
use community_search::federation::discovered;
use community_search::federation::peer::HttpPeerClient;
use community_search::index::{reader::Searcher, schema};
use community_search::search::service::SearchService;
use community_search::RuntimeConfig;
use rusqlite::Connection;
use serde_json::Value;
use tantivy::Index;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, RwLock};

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// A running test server with access to the underlying shared database so
/// that tests can seed rows directly via the DB lock.
struct GossipTestServer {
    pub base_url: String,
    pub client: reqwest::Client,
    pub db: SharedDb,
    /// Dropping this triggers graceful shutdown.
    _shutdown: oneshot::Sender<()>,
}

/// Spawn a TCP test server backed by an in-memory SQLite database (all
/// migrations applied) and return a [`GossipTestServer`] with a direct
/// handle to the database for pre-test seeding.
async fn make_gossip_test_server() -> GossipTestServer {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    community_search::db::run_migrations(&conn).expect("run all migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));

    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));

    let default_rl = RateLimitConfig::default();
    let state = AppState {
        db: Arc::clone(&db),
        search: Arc::new(service),
        admin_token: String::from("test-admin-token"),
        self_url: String::new(),
        rate_limit_config: Arc::new(RwLock::new(default_rl.clone())),
        peer_rate_limit_config: Arc::new(RwLock::new(RateLimitConfig {
            limit: 120,
            ..default_rl
        })),
        peer_ip_cache: Arc::new(RwLock::new(PeerIpCache::new())),
        runtime_config: Arc::new(RwLock::new(RuntimeConfig::default())),
        index_path: std::path::PathBuf::from("/tmp/gossip-test-index-nonexistent"),
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

    let router = community_search::api::build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TCP listener");
    let addr = listener.local_addr().expect("local_addr");
    let base_url = format!("http://{addr}");

    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await
            .expect("axum serve error");
    });

    GossipTestServer {
        base_url,
        client: reqwest::Client::new(),
        db,
        _shutdown: tx,
    }
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

/// POST /api/gossip/exchange returns 200 with our pre-merge engine list and
/// `protocol_version: "1.0"`.  The response must include the self_url that
/// was seeded into `discovered_engines` before the request.
#[tokio::test]
async fn exchange_returns_our_list_and_merges_theirs() {
    let server = make_gossip_test_server().await;

    // Seed a self-entry so the local list is non-empty.
    let self_url = "https://self.example.com";
    {
        let conn = server.db.lock().expect("db mutex poisoned");
        discovered::ensure_self_entry(&conn, self_url, "Self Engine", "My engine", 1000)
            .expect("ensure_self_entry");
    }

    let resp = server
        .client
        .post(format!("{}/api/gossip/exchange", server.base_url))
        .json(&serde_json::json!({
            "protocol_version": "1.0",
            "engines": [
                {"url": "https://peer-a.example.com"},
                {"url": "https://peer-b.example.com"}
            ]
        }))
        .send()
        .await
        .expect("POST /api/gossip/exchange");

    assert_eq!(
        resp.status(),
        200,
        "exchange with compatible version must return 200"
    );

    let body: Value = resp.json().await.expect("parse JSON response");

    assert_eq!(
        body["protocol_version"], "1.0",
        "response.protocol_version must be '1.0'"
    );

    let engines = body["engines"]
        .as_array()
        .expect("engines must be an array");
    let urls: Vec<&str> = engines.iter().filter_map(|e| e["url"].as_str()).collect();

    assert!(
        urls.contains(&self_url),
        "response.engines must include self_url={self_url}; got: {urls:?}"
    );
}

/// Engines received in a gossip exchange are persisted.  A subsequent POST
/// with an empty engine list must still return the previously-seen engine.
#[tokio::test]
async fn exchange_persists_incoming_engines() {
    let server = make_gossip_test_server().await;

    let new_engine_url = "https://new-engine.example.com";

    // First POST: introduce a new peer engine.
    let resp1 = server
        .client
        .post(format!("{}/api/gossip/exchange", server.base_url))
        .json(&serde_json::json!({
            "protocol_version": "1.0",
            "engines": [{"url": new_engine_url}]
        }))
        .send()
        .await
        .expect("POST 1 /api/gossip/exchange");

    assert_eq!(resp1.status(), 200, "first POST must return 200");
    // Consume the body so the connection is released.
    let _ = resp1.json::<Value>().await;

    // Second POST: empty engines list — the previously persisted engine must
    // appear in the response (pre-merge local list).
    let resp2 = server
        .client
        .post(format!("{}/api/gossip/exchange", server.base_url))
        .json(&serde_json::json!({
            "protocol_version": "1.0",
            "engines": []
        }))
        .send()
        .await
        .expect("POST 2 /api/gossip/exchange");

    assert_eq!(resp2.status(), 200, "second POST must return 200");

    let body2: Value = resp2
        .json()
        .await
        .expect("parse JSON response for second POST");
    let engines2 = body2["engines"]
        .as_array()
        .expect("engines must be an array");
    let urls2: Vec<&str> = engines2.iter().filter_map(|e| e["url"].as_str()).collect();

    assert!(
        urls2.contains(&new_engine_url),
        "second response must include the previously-persisted engine={new_engine_url}; got: {urls2:?}"
    );
}

/// A peer that sends a major-version mismatch (e.g. "2.0") must receive
/// HTTP 400 with a JSON body whose `error` field mentions "version".
#[tokio::test]
async fn exchange_rejects_major_version_mismatch() {
    let server = make_gossip_test_server().await;

    let resp = server
        .client
        .post(format!("{}/api/gossip/exchange", server.base_url))
        .json(&serde_json::json!({
            "protocol_version": "2.0",
            "engines": []
        }))
        .send()
        .await
        .expect("POST /api/gossip/exchange with version 2.0");

    assert_eq!(
        resp.status(),
        400,
        "major version mismatch (2.0 vs 1.0) must return 400"
    );

    let body: Value = resp.json().await.expect("parse JSON error body");
    let error_msg = body["error"]
        .as_str()
        .expect("error field must be a string");

    assert!(
        error_msg.to_lowercase().contains("version"),
        "error message must mention 'version'; got: \"{error_msg}\""
    );
}

/// A peer that sends a minor-version mismatch (e.g. "1.5") must still
/// receive HTTP 200 — the mismatch is logged as a warning but not rejected.
#[tokio::test]
async fn exchange_accepts_minor_version_mismatch_with_warning_status() {
    let server = make_gossip_test_server().await;

    let resp = server
        .client
        .post(format!("{}/api/gossip/exchange", server.base_url))
        .json(&serde_json::json!({
            "protocol_version": "1.5",
            "engines": []
        }))
        .send()
        .await
        .expect("POST /api/gossip/exchange with version 1.5");

    assert_eq!(
        resp.status(),
        200,
        "minor version mismatch (1.5 vs 1.0) must return 200"
    );
}
