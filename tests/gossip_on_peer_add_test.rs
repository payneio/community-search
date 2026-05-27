use std::sync::{Arc, Mutex};
use std::time::Duration;

use community_search::api::public::{AppState, SharedDb};
use community_search::api::rate_limit::{PeerIpCache, RateLimitConfig};
use community_search::federation::peer::HttpPeerClient;
use community_search::index::{reader::Searcher, schema};
use community_search::search::service::SearchService;
use community_search::RuntimeConfig;
use rusqlite::Connection;
use serde_json::json;
use tantivy::Index;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, RwLock};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// A running test server with the admin token exposed for auth.
struct AdminTestServer {
    pub base_url: String,
    pub client: reqwest::Client,
    pub admin_token: String,
    /// Dropping this triggers graceful server shutdown.
    _shutdown: oneshot::Sender<()>,
}

/// Spin up a TCP test server with admin auth enabled.
/// Returns an `AdminTestServer` ready to accept requests.
async fn spawn_test_server_with_admin() -> AdminTestServer {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    community_search::db::run_migrations(&conn).expect("run all migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));

    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));

    let http_client = reqwest::Client::builder()
        .user_agent("community-search-test/0.1")
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build reqwest client");

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
        index_path: std::path::PathBuf::from("/tmp/gossip-peer-add-test-index-nonexistent"),
        max_index_bytes: 10_737_418_240,
        peer_client: Arc::new(
            HttpPeerClient::new(Duration::from_secs(10)).expect("create HttpPeerClient"),
        ),
        http_client,
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

    AdminTestServer {
        base_url,
        client: reqwest::Client::new(),
        admin_token: String::from("test-admin-token"),
        _shutdown: tx,
    }
}

// ---------------------------------------------------------------------------
// Integration test
// ---------------------------------------------------------------------------

/// Adding a node peer via POST /api/admin/nodes must trigger an immediate
/// gossip exchange with the new peer.
///
/// Verification:
/// 1. wiremock's `.expect(1)` on drop confirms exactly one outbound POST to
///    the peer's /api/gossip/exchange endpoint.
/// 2. The engine returned by the peer is persisted and appears in our next
///    gossip exchange response.
#[tokio::test]
async fn adding_a_node_peer_triggers_immediate_gossip() {
    let peer = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/gossip/exchange"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "protocol_version":"1.0",
            "engines":[{"url":"https://discovered-via-add.example.com","name":"Found","description":"ok"}]
        })))
        .expect(1)
        .mount(&peer)
        .await;

    let app = spawn_test_server_with_admin().await;

    // POST /api/admin/nodes — register the mock peer as a node peer.
    let resp = app
        .client
        .post(format!("{}/api/admin/nodes", app.base_url))
        .bearer_auth(&app.admin_token)
        .json(&json!({"url": peer.uri(), "name": "fake-peer"}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "POST /api/admin/nodes must succeed; got {}",
        resp.status()
    );

    // Give the background gossip task time to complete.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Dropping `peer` triggers wiremock's `.expect(1)` verification —
    // this panics if the exchange endpoint was NOT called exactly once.
    drop(peer);

    // Now verify the discovered engine was persisted and appears in our
    // own gossip exchange response.
    let resp = app
        .client
        .post(format!("{}/api/gossip/exchange", app.base_url))
        .json(&json!({"protocol_version":"1.0","engines":[]}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let urls: Vec<&str> = body["engines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["url"].as_str().unwrap())
        .collect();
    assert!(
        urls.contains(&"https://discovered-via-add.example.com"),
        "discovered engine must appear in subsequent gossip exchange; got: {urls:?}"
    );
}
