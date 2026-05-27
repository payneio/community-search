/// Integration tests for the admin discovered-engines endpoints.
///
/// Routes under test:
/// - GET    /api/admin/discovered          — list discovered engines
/// - POST   /api/admin/discovered/promote  — promote to node peer
/// - DELETE /api/admin/discovered?url=...  — remove discovered engine
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

const ADMIN_TOKEN: &str = "test-admin-token";
const SELF_URL: &str = "https://self.example.com";

/// A running test server backed by an in-memory database.
struct TestServer {
    pub base_url: String,
    pub client: reqwest::Client,
    /// Holds the DB alive so in-memory SQLite state persists for the test.
    #[allow(dead_code)]
    db: SharedDb,
    _shutdown: oneshot::Sender<()>,
}

/// Spin up a test server with a known `self_url` for discovered-engine tests.
async fn spawn_server() -> TestServer {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    community_search::db::run_migrations(&conn).expect("run all migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));

    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));

    let default_rl = RateLimitConfig::default();
    let http_client = reqwest::Client::builder()
        .user_agent("community-search-test/0.1")
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build reqwest client");
    let state = AppState {
        db: Arc::clone(&db),
        search: Arc::new(service),
        admin_token: ADMIN_TOKEN.to_string(),
        self_url: SELF_URL.to_string(),
        rate_limit_config: Arc::new(RwLock::new(default_rl.clone())),
        peer_rate_limit_config: Arc::new(RwLock::new(RateLimitConfig {
            limit: 120,
            ..default_rl
        })),
        peer_ip_cache: Arc::new(RwLock::new(PeerIpCache::new())),
        runtime_config: Arc::new(RwLock::new(RuntimeConfig::default())),
        index_path: std::path::PathBuf::from("/tmp/admin-discovered-test-index"),
        max_index_bytes: 10_737_418_240,
        peer_client: Arc::new(
            HttpPeerClient::new(Duration::from_secs(10)).expect("create HttpPeerClient"),
        ),
        http_client,
        crawler_user_agent: "community-search-test/0.1".into(),
        indexer_delete_tx: community_search::test_support::sink_indexer_delete_tx(),
    };

    // Seed the self-entry (main does this at startup).
    {
        let conn = state.db.lock().expect("db lock");
        let now = 1_000_000i64;
        discovered::ensure_self_entry(&conn, SELF_URL, "Self Engine", "My engine", now)
            .expect("seed self-entry");
    }

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

    TestServer {
        base_url,
        client: reqwest::Client::new(),
        db,
        _shutdown: tx,
    }
}

/// Convenience: send a request with the admin Bearer token.
fn admin_header() -> (&'static str, String) {
    ("Authorization", format!("Bearer {ADMIN_TOKEN}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// GET /api/admin/discovered returns JSON `{engines: [...]}` and includes the
/// self-entry that was seeded at startup.
#[tokio::test]
async fn admin_can_list_discovered_engines() {
    let server = spawn_server().await;
    let (hdr_name, hdr_val) = admin_header();

    let resp = server
        .client
        .get(format!("{}/api/admin/discovered", server.base_url))
        .header(hdr_name, &hdr_val)
        .send()
        .await
        .expect("GET /api/admin/discovered");

    assert_eq!(resp.status(), 200, "expected 200 OK");

    let body: Value = resp.json().await.expect("parse JSON body");
    let engines = body["engines"]
        .as_array()
        .expect("engines must be an array");

    let urls: Vec<&str> = engines.iter().filter_map(|e| e["url"].as_str()).collect();

    assert!(
        urls.contains(&SELF_URL),
        "engines list must include self_url={SELF_URL}; got: {urls:?}"
    );
}

/// POST /api/admin/discovered/promote with `{url}` looks up the discovered
/// entry and inserts it as a node peer.  After promotion, the URL should
/// appear in GET /api/admin/nodes.
#[tokio::test]
async fn admin_can_promote_discovered_engine_to_node_peer() {
    let server = spawn_server().await;
    let (hdr_name, hdr_val) = admin_header();
    let peer_url = "https://peer-a.example.com";

    // Seed a foreign peer via POST /api/gossip/exchange so it lands in
    // discovered_engines.
    let gossip_resp = server
        .client
        .post(format!("{}/api/gossip/exchange", server.base_url))
        .json(&serde_json::json!({
            "protocol_version": "1.0",
            "engines": [{"url": peer_url, "name": "Peer A"}]
        }))
        .send()
        .await
        .expect("POST /api/gossip/exchange");
    assert_eq!(gossip_resp.status(), 200, "gossip exchange must succeed");

    // Promote it.
    let promote_resp = server
        .client
        .post(format!("{}/api/admin/discovered/promote", server.base_url))
        .header(hdr_name, &hdr_val)
        .json(&serde_json::json!({"url": peer_url}))
        .send()
        .await
        .expect("POST /api/admin/discovered/promote");
    assert_eq!(
        promote_resp.status(),
        200,
        "promote must succeed: {}",
        promote_resp.text().await.unwrap_or_default()
    );

    // Verify it now appears in the nodes list.
    let nodes_resp = server
        .client
        .get(format!("{}/api/admin/nodes", server.base_url))
        .header(hdr_name, &hdr_val)
        .send()
        .await
        .expect("GET /api/admin/nodes");
    assert_eq!(
        nodes_resp.status(),
        200,
        "GET /api/admin/nodes must succeed"
    );

    let nodes: Value = nodes_resp.json().await.expect("parse nodes JSON");
    let node_urls: Vec<&str> = nodes
        .as_array()
        .expect("nodes must be an array")
        .iter()
        .filter_map(|n| n["url"].as_str())
        .collect();

    assert!(
        node_urls.contains(&peer_url),
        "promoted URL must appear in nodes; got: {node_urls:?}"
    );
}

/// DELETE /api/admin/discovered?url=... removes the entry.  A subsequent GET
/// must not contain that URL.
#[tokio::test]
async fn admin_can_remove_discovered_engine() {
    let server = spawn_server().await;
    let (hdr_name, hdr_val) = admin_header();
    let peer_url = "https://remove-me.example.com";

    // Seed directly via gossip exchange.
    let gossip_resp = server
        .client
        .post(format!("{}/api/gossip/exchange", server.base_url))
        .json(&serde_json::json!({
            "protocol_version": "1.0",
            "engines": [{"url": peer_url, "name": "Remove Me"}]
        }))
        .send()
        .await
        .expect("POST /api/gossip/exchange");
    assert_eq!(gossip_resp.status(), 200, "gossip exchange must succeed");

    // Delete it.
    let del_resp = server
        .client
        .delete(format!("{}/api/admin/discovered", server.base_url))
        .query(&[("url", peer_url)])
        .header(hdr_name, &hdr_val)
        .send()
        .await
        .expect("DELETE /api/admin/discovered");
    assert_eq!(
        del_resp.status(),
        200,
        "DELETE must succeed: {}",
        del_resp.text().await.unwrap_or_default()
    );

    // Verify it is gone.
    let list_resp = server
        .client
        .get(format!("{}/api/admin/discovered", server.base_url))
        .header(hdr_name, &hdr_val)
        .send()
        .await
        .expect("GET /api/admin/discovered");
    let body: Value = list_resp.json().await.expect("parse JSON");
    let engines = body["engines"]
        .as_array()
        .expect("engines must be an array");
    let urls: Vec<&str> = engines.iter().filter_map(|e| e["url"].as_str()).collect();
    assert!(
        !urls.contains(&peer_url),
        "removed URL must not appear in subsequent list; got: {urls:?}"
    );
}

/// DELETE /api/admin/discovered?url={self_url} must be rejected with 400 Bad Request.
#[tokio::test]
async fn admin_cannot_remove_self_entry() {
    let server = spawn_server().await;
    let (hdr_name, hdr_val) = admin_header();

    let del_resp = server
        .client
        .delete(format!("{}/api/admin/discovered", server.base_url))
        .query(&[("url", SELF_URL)])
        .header(hdr_name, &hdr_val)
        .send()
        .await
        .expect("DELETE /api/admin/discovered (self)");

    assert_eq!(
        del_resp.status(),
        400,
        "deleting self-entry must return 400 Bad Request"
    );
}
