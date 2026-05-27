//! Test infrastructure for integration tests.
//!
//! This module is compiled unconditionally so that integration tests in
//! `tests/` (which compile the crate as a library, without `cfg(test)`) can
//! use it without requiring the `testing` feature flag.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use rusqlite::Connection;
use tantivy::Index;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

use crate::api::public::{AppState, SharedDb};

/// Mint a live `indexer_delete_tx` for tests: spawns a background task that
/// drains and discards every URL batch sent. Tests that don't actually
/// exercise the indexer still need a connected sender to construct
/// `AppState`. Must be called inside a tokio runtime (e.g. from
/// `#[tokio::test]`).
pub fn sink_indexer_delete_tx() -> mpsc::Sender<Vec<String>> {
    let (tx, mut rx) = mpsc::channel::<Vec<String>>(8);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    tx
}

/// Same idea as [`sink_indexer_delete_tx`] but for the upsert channel:
/// returns a connected `Sender<IndexJob>` whose messages are silently
/// drained, so tests that don't actually run the indexer can still
/// construct `AppState`.
pub fn sink_indexer_upsert_tx() -> mpsc::Sender<crate::index::indexer::IndexJob> {
    let (tx, mut rx) = mpsc::channel::<crate::index::indexer::IndexJob>(8);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    tx
}
use crate::api::rate_limit::{PeerIpCache, RateLimitConfig};
use crate::federation::peer::{HttpPeerClient, PeerClient};
use crate::index::{reader::Searcher, schema};
use crate::search::service::SearchService;
use crate::RuntimeConfig;

// ---------------------------------------------------------------------------
// TestApp (oneshot / in-process)
// ---------------------------------------------------------------------------

/// A fully-wired test application that exposes both the HTTP router and the
/// shared database so that tests can seed data directly via the DB.
pub struct TestApp {
    /// The HTTP router; clone it before passing to `oneshot`.
    pub router: Router,
    /// Direct access to the in-memory database (for seeding test data).
    pub db: SharedDb,
}

/// Build a [`Router`] backed by an in-memory SQLite database (all
/// migrations applied) and an in-RAM Tantivy index.
///
/// The router is wired through [`crate::api::build_router`], which means
/// the rate-limit middleware is active on `POST /api/search`.
///
/// Designed for integration tests that drive the full HTTP stack via
/// [`tower::ServiceExt::oneshot`] without starting a real TCP listener.
pub fn test_router_with_search() -> Router {
    let conn = Connection::open_in_memory().expect("open in-memory SQLite");
    crate::db::run_migrations(&conn).expect("apply migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));

    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram Searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));

    let default_rl = RateLimitConfig::default();
    let state = AppState {
        db,
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
        index_path: PathBuf::from("/tmp/community-search-test-index-nonexistent"),
        max_index_bytes: 10_737_418_240,
        peer_client: Arc::new(
            HttpPeerClient::new(Duration::from_secs(10)).expect("create HttpPeerClient"),
        ) as Arc<dyn PeerClient>,
        http_client: reqwest::Client::builder()
            .user_agent("community-search-test/0.1")
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build reqwest client"),
        crawler_user_agent: "community-search-test/0.1".into(),
        indexer_delete_tx: sink_indexer_delete_tx(),
        indexer_upsert_tx: sink_indexer_upsert_tx(),
        crawl_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        indexing_inflight: Arc::new(std::sync::atomic::AtomicI64::new(0)),
    };

    crate::api::build_router(state)
}

/// Build the full application [`Router`] with the given admin token.
///
/// Backed by an in-memory SQLite database (all migrations applied) and an
/// in-RAM Tantivy index. Passes the token as `AppState::admin_token` so
/// that `/api/admin/*` routes validate against it.
///
/// Designed for integration tests that exercise admin-protected endpoints
/// via [`tower::ServiceExt::oneshot`] without starting a real TCP listener.
pub fn test_router_full(token: &str) -> Router {
    let conn = Connection::open_in_memory().expect("open in-memory SQLite");
    crate::db::run_migrations(&conn).expect("apply migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));

    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram Searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));

    let default_rl = RateLimitConfig::default();
    let state = AppState {
        db,
        search: Arc::new(service),
        admin_token: token.to_string(),
        self_url: String::new(),
        rate_limit_config: Arc::new(RwLock::new(default_rl.clone())),
        peer_rate_limit_config: Arc::new(RwLock::new(RateLimitConfig {
            limit: 120,
            ..default_rl
        })),
        peer_ip_cache: Arc::new(RwLock::new(PeerIpCache::new())),
        runtime_config: Arc::new(RwLock::new(RuntimeConfig::default())),
        index_path: PathBuf::from("/tmp/community-search-test-index-nonexistent"),
        max_index_bytes: 10_737_418_240,
        peer_client: Arc::new(
            HttpPeerClient::new(Duration::from_secs(10)).expect("create HttpPeerClient"),
        ) as Arc<dyn PeerClient>,
        http_client: reqwest::Client::builder()
            .user_agent("community-search-test/0.1")
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build reqwest client"),
        crawler_user_agent: "community-search-test/0.1".into(),
        indexer_delete_tx: sink_indexer_delete_tx(),
        indexer_upsert_tx: sink_indexer_upsert_tx(),
        crawl_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        indexing_inflight: Arc::new(std::sync::atomic::AtomicI64::new(0)),
    };

    crate::api::build_router(state)
}

/// Build a [`TestApp`] — a full router paired with its backing in-memory
/// database — for tests that need to seed data directly.
///
/// Use this instead of [`test_router_full`] when a test also calls
/// [`seed_outlink`] or other helpers that insert rows directly into the DB.
pub fn test_app(token: &str) -> TestApp {
    let conn = Connection::open_in_memory().expect("open in-memory SQLite");
    crate::db::run_migrations(&conn).expect("apply migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));

    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram Searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));

    let default_rl = RateLimitConfig::default();
    let state = AppState {
        db: Arc::clone(&db),
        search: Arc::new(service),
        admin_token: token.to_string(),
        self_url: String::new(),
        rate_limit_config: Arc::new(RwLock::new(default_rl.clone())),
        peer_rate_limit_config: Arc::new(RwLock::new(RateLimitConfig {
            limit: 120,
            ..default_rl
        })),
        peer_ip_cache: Arc::new(RwLock::new(PeerIpCache::new())),
        runtime_config: Arc::new(RwLock::new(RuntimeConfig::default())),
        index_path: PathBuf::from("/tmp/community-search-test-index-nonexistent"),
        max_index_bytes: 10_737_418_240,
        peer_client: Arc::new(
            HttpPeerClient::new(Duration::from_secs(10)).expect("create HttpPeerClient"),
        ) as Arc<dyn PeerClient>,
        http_client: reqwest::Client::builder()
            .user_agent("community-search-test/0.1")
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build reqwest client"),
        crawler_user_agent: "community-search-test/0.1".into(),
        indexer_delete_tx: sink_indexer_delete_tx(),
        indexer_upsert_tx: sink_indexer_upsert_tx(),
        crawl_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        indexing_inflight: Arc::new(std::sync::atomic::AtomicI64::new(0)),
    };

    TestApp {
        router: crate::api::build_router(state),
        db,
    }
}

/// A [`TestApp`] paired with the receiving end of the indexer-delete
/// channel, so tests can observe which URL batches the admin handlers
/// queue for the indexer task.
pub struct TestAppWithDeleteRx {
    pub router: Router,
    pub db: SharedDb,
    pub delete_rx: mpsc::Receiver<Vec<String>>,
}

/// Build a [`TestAppWithDeleteRx`]: same wiring as [`test_app`] but the
/// indexer-delete channel's receiver is handed back to the caller instead
/// of being drained by a sink consumer. Use this when a test asserts on
/// what the admin layer pushes toward the indexer.
pub fn test_app_with_delete_rx(token: &str) -> TestAppWithDeleteRx {
    let conn = Connection::open_in_memory().expect("open in-memory SQLite");
    crate::db::run_migrations(&conn).expect("apply migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));

    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram Searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));

    let (delete_tx, delete_rx) = mpsc::channel::<Vec<String>>(16);

    let default_rl = RateLimitConfig::default();
    let state = AppState {
        db: Arc::clone(&db),
        search: Arc::new(service),
        admin_token: token.to_string(),
        self_url: String::new(),
        rate_limit_config: Arc::new(RwLock::new(default_rl.clone())),
        peer_rate_limit_config: Arc::new(RwLock::new(RateLimitConfig {
            limit: 120,
            ..default_rl
        })),
        peer_ip_cache: Arc::new(RwLock::new(PeerIpCache::new())),
        runtime_config: Arc::new(RwLock::new(RuntimeConfig::default())),
        index_path: PathBuf::from("/tmp/community-search-test-index-nonexistent"),
        max_index_bytes: 10_737_418_240,
        peer_client: Arc::new(
            HttpPeerClient::new(Duration::from_secs(10)).expect("create HttpPeerClient"),
        ) as Arc<dyn PeerClient>,
        http_client: reqwest::Client::builder()
            .user_agent("community-search-test/0.1")
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build reqwest client"),
        crawler_user_agent: "community-search-test/0.1".into(),
        indexer_delete_tx: delete_tx,
        indexer_upsert_tx: sink_indexer_upsert_tx(),
        crawl_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        indexing_inflight: Arc::new(std::sync::atomic::AtomicI64::new(0)),
    };

    TestAppWithDeleteRx {
        router: crate::api::build_router(state),
        db,
        delete_rx,
    }
}

/// Build a [`TestApp`] with a configured `self_url`, for tests that need
/// the discovered-engines self-entry protection.
pub fn test_app_with_self_url(token: &str, self_url: &str) -> TestApp {
    let conn = Connection::open_in_memory().expect("open in-memory SQLite");
    crate::db::run_migrations(&conn).expect("apply migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));

    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram Searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));

    let default_rl = RateLimitConfig::default();
    let state = AppState {
        db: Arc::clone(&db),
        search: Arc::new(service),
        admin_token: token.to_string(),
        self_url: self_url.to_string(),
        rate_limit_config: Arc::new(RwLock::new(default_rl.clone())),
        peer_rate_limit_config: Arc::new(RwLock::new(RateLimitConfig {
            limit: 120,
            ..default_rl
        })),
        peer_ip_cache: Arc::new(RwLock::new(PeerIpCache::new())),
        runtime_config: Arc::new(RwLock::new(RuntimeConfig::default())),
        index_path: PathBuf::from("/tmp/community-search-test-index-nonexistent"),
        max_index_bytes: 10_737_418_240,
        peer_client: Arc::new(
            HttpPeerClient::new(Duration::from_secs(10)).expect("create HttpPeerClient"),
        ) as Arc<dyn PeerClient>,
        http_client: reqwest::Client::builder()
            .user_agent("community-search-test/0.1")
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build reqwest client"),
        crawler_user_agent: "community-search-test/0.1".into(),
        indexer_delete_tx: sink_indexer_delete_tx(),
        indexer_upsert_tx: sink_indexer_upsert_tx(),
        crawl_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        indexing_inflight: Arc::new(std::sync::atomic::AtomicI64::new(0)),
    };

    TestApp {
        router: crate::api::build_router(state),
        db,
    }
}

/// Insert a pending host-level outlink into the test database and return its
/// SQLite rowid.
///
/// Creates a new collection (with a unique name derived from a UUID) and
/// records one hit for `url`'s host within that collection. The collection's
/// SQLite rowid is deterministic for the first call on a fresh [`TestApp`]
/// (rowid = 1), so tests that pass `collection_id=1` to the list endpoint
/// will see the seeded row.
///
/// # Arguments
/// * `url`       — example target URL (its host is what's tracked).
/// * `link_text` — anchor text recorded in the example.
/// * `app`       — the [`TestApp`] whose database receives the seed row.
///
/// # Returns
/// The SQLite rowid of the newly inserted `outlink_host_suggestions` row.
pub async fn seed_outlink(url: &str, link_text: &str, app: &TestApp) -> i64 {
    use uuid::Uuid;

    let conn = app.db.lock().expect("db mutex poisoned in seed_outlink");

    let col_name = format!("seed-col-{}", Uuid::new_v4());
    let col_record = crate::db::collections::create_item(&conn, &col_name, "")
        .expect("seed_outlink: create_item failed");

    let col_uuid: String = conn
        .query_row(
            "SELECT id FROM collections WHERE rowid = ?1",
            rusqlite::params![col_record.id],
            |row| row.get(0),
        )
        .expect("seed_outlink: resolve collection UUID failed");

    let host = crate::crawler::url_class::host_of(url)
        .expect("seed_outlink: url must have a parseable host");

    let example = crate::db::outlink_hosts::OutlinkExample {
        source_url: "https://source.example.com/".into(),
        target_url: url.into(),
        link_text: link_text.into(),
    };
    crate::db::outlink_hosts::record_hit(&conn, &col_uuid, &host, &example, 1_000_000)
        .expect("seed_outlink: record_hit failed");

    conn.query_row(
        "SELECT rowid FROM outlink_host_suggestions \
         WHERE collection_id = ?1 AND host = ?2",
        rusqlite::params![col_uuid, host],
        |row| row.get(0),
    )
    .expect("seed_outlink: get rowid failed")
}

// ---------------------------------------------------------------------------
// TestServer (real TCP listener — for integration tests that use reqwest)
// ---------------------------------------------------------------------------

/// A running test server with public fields for `base_url` and `client`.
///
/// Dropping this struct triggers a graceful shutdown of the background server
/// task via the internal oneshot channel.
pub struct TestServer {
    /// Base URL of the running server (e.g. `http://127.0.0.1:51234`).
    pub base_url: String,
    /// Shared HTTP client pre-configured to talk to this server.
    pub client: reqwest::Client,
    /// Dropping this sends the graceful-shutdown signal.
    _shutdown: oneshot::Sender<()>,
}

/// Spin up a real `axum` server on a random port and return a [`TestServer`]
/// that is ready to accept requests.
///
/// The server is backed by an in-memory SQLite database (all migrations
/// applied) and an in-RAM Tantivy index, so no disk state is required.
///
/// # Example
/// ```no_run
/// # use community_search::test_support::spawn_test_server;
/// # #[tokio::main] async fn main() {
/// let app = spawn_test_server().await;
/// let resp = app.client.get(format!("{}/", app.base_url)).send().await.unwrap();
/// assert_eq!(resp.status(), 200);
/// # }
/// ```
pub async fn spawn_test_server() -> TestServer {
    let conn = Connection::open_in_memory().expect("open in-memory SQLite");
    crate::db::run_migrations(&conn).expect("apply migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));

    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram Searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));

    let default_rl = RateLimitConfig::default();
    let state = AppState {
        db,
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
        index_path: PathBuf::from("/tmp/community-search-test-index-nonexistent"),
        max_index_bytes: 10_737_418_240,
        peer_client: Arc::new(
            HttpPeerClient::new(Duration::from_secs(10)).expect("create HttpPeerClient"),
        ) as Arc<dyn PeerClient>,
        http_client: reqwest::Client::builder()
            .user_agent("community-search-test/0.1")
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build reqwest client"),
        crawler_user_agent: "community-search-test/0.1".into(),
        indexer_delete_tx: sink_indexer_delete_tx(),
        indexer_upsert_tx: sink_indexer_upsert_tx(),
        crawl_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        indexing_inflight: Arc::new(std::sync::atomic::AtomicI64::new(0)),
    };

    let router = crate::api::build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TcpListener");
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

    let client = reqwest::Client::new();

    TestServer {
        base_url,
        client,
        _shutdown: tx,
    }
}
