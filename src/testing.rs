//! Test helpers — in-process HTTP server backed by an in-RAM database and index.
//!
//! [`TestApp`] spins up a real axum server on a random port so integration
//! tests can drive the full HTTP stack without any external dependencies.

use rusqlite::Connection;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tantivy::Index;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

use crate::api::public::{AppState, SharedDb};
use crate::api::router::build_router;
use crate::federation::peer::HttpPeerClient;
use crate::index::writer::{Document, IndexWriter};
use crate::index::{reader::Searcher, schema};
use crate::search::service::SearchService;

// ── TestApp ───────────────────────────────────────────────────────────────────

/// A running test server with an in-RAM SQLite database and Tantivy index.
///
/// Dropping the `TestApp` sends a graceful-shutdown signal to the server task
/// via `_shutdown`.
pub struct TestApp {
    base: String,
    client: reqwest::Client,
    /// Dropping this triggers graceful server shutdown.
    _shutdown: oneshot::Sender<()>,
}

impl TestApp {
    /// Return the base URL of the running server (e.g. `http://127.0.0.1:51234`).
    pub fn base_url(&self) -> &str {
        &self.base
    }

    /// Return a reference to the shared reqwest client.
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Spin up a test server pre-populated with the given documents.
    ///
    /// Each tuple is `(title, body, url, collection, timestamp_secs)`.
    ///
    /// Distinct collection names are collected into a [`BTreeSet`] (alphabetical
    /// order) and inserted into the SQLite `collections` table with integer IDs
    /// starting at 1.
    pub async fn new_with_docs(docs: &[(&str, &str, &str, &str, i64)]) -> Self {
        // ── 1. In-memory SQLite + migrations ─────────────────────────────────
        let conn = Connection::open_in_memory().expect("open in-memory SQLite");
        crate::db::run_migrations(&conn).expect("run migrations");

        // ── 2. Insert distinct collections (BTreeSet → alphabetical order) ───
        let collection_names: BTreeSet<&str> = docs.iter().map(|(_, _, _, c, _)| *c).collect();

        for (idx, name) in collection_names.iter().enumerate() {
            let id = format!("{}", idx + 1);
            conn.execute(
                "INSERT INTO collections \
                 (id, name, description, created_at, updated_at) \
                 VALUES (?1, ?2, '', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')",
                rusqlite::params![id, name],
            )
            .expect("insert collection");
        }

        let db: SharedDb = Arc::new(Mutex::new(conn));

        // ── 3. In-RAM Tantivy index ───────────────────────────────────────────
        let index = Index::create_in_ram(schema::build());

        // ── 4. Add documents and commit ───────────────────────────────────────
        {
            let mut writer = IndexWriter::open(&index).expect("create IndexWriter");
            for &(title, body, url, collection, ts) in docs {
                writer
                    .upsert(&Document {
                        title,
                        body,
                        url,
                        collection,
                        indexed_at: ts,
                        collection_id: collection,
                        content_hash: "",
                    })
                    .expect("upsert document");
            }
            writer.commit().expect("commit index");
        }

        // ── 5. IndexSearcher + SearchService + AppState ───────────────────────
        let searcher = Searcher::open(index).expect("open Searcher");
        let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));
        let default_rl = crate::api::rate_limit::RateLimitConfig::default();
        let state = AppState {
            db,
            search: Arc::new(service),
            admin_token: String::from("test-admin-token"),
            self_url: String::new(),
            rate_limit_config: Arc::new(RwLock::new(default_rl.clone())),
            peer_rate_limit_config: Arc::new(RwLock::new(
                crate::api::rate_limit::RateLimitConfig {
                    limit: 120,
                    ..default_rl
                },
            )),
            peer_ip_cache: Arc::new(RwLock::new(crate::api::rate_limit::PeerIpCache::new())),
            runtime_config: Arc::new(RwLock::new(crate::RuntimeConfig::default())),
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
            indexer_delete_tx: crate::test_support::sink_indexer_delete_tx(),
            indexer_upsert_tx: crate::test_support::sink_indexer_upsert_tx(),
            crawl_paused: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            indexing_inflight: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
        };

        // ── 6. Router + TcpListener + graceful shutdown via oneshot ───────────
        let router = build_router(state);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind TcpListener");
        let addr = listener.local_addr().expect("local_addr");
        let base = format!("http://{addr}");

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

        TestApp {
            base,
            client,
            _shutdown: tx,
        }
    }

    /// Convenience constructor with a standard fixture set matching the
    /// integration-test expectations:
    ///
    /// | url         | collection | body      |
    /// |-------------|------------|-----------|
    /// | a.example/1 | tech       | Tokio     |
    /// | b.example/2 | cooking    | cast iron |
    ///
    /// Query `"tokio"` scoped to collection `"tech"` must return `a.example/1`
    /// and must NOT return `b.example/2`.
    pub async fn new() -> Self {
        Self::new_with_docs(&[
            ("Rust async", "Tokio", "a.example/1", "tech", 1_700_000_000),
            (
                "Cooking with Rust",
                "cast iron",
                "b.example/2",
                "cooking",
                1_700_000_001,
            ),
        ])
        .await
    }
}
