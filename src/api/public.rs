use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    extract::{FromRef, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    response::{IntoResponse, Response},
    Json,
};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio_stream::{wrappers::ReceiverStream, StreamExt as _};

use crate::api::rate_limit::{PeerIpCache, RateLimitConfig};
use crate::api::sse::SseEvent;
use crate::federation::fanout::{active_collection_peers_for, dispatch};
use crate::federation::health::record_result;
use crate::federation::peer::PeerClient;
use crate::protocol::PROTOCOL_VERSION;
use crate::search::result::SearchResult;
use crate::search::service::SearchService;
use crate::RuntimeConfig;

// -- Shared state type --------------------------------------------------------

/// Raw rusqlite connection shared across handlers via axum State.
pub type SharedDb = Arc<Mutex<Connection>>;

// -- App State ----------------------------------------------------------------

/// Application state shared across all request handlers.
///
/// Provides the database handle, the search service, and the resolved admin
/// token. Handlers that only need the database can use `State<SharedDb>` via
/// the `FromRef` impl.
#[derive(Clone)]
pub struct AppState {
    pub db: SharedDb,
    pub search: Arc<SearchService>,
    pub admin_token: String,
    /// Public URL of this engine (from `SELF_URL` env var).
    ///
    /// Used by the discovered-engines admin endpoint to prevent removal of the
    /// self-entry.  Defaults to an empty string when not configured.
    pub self_url: String,
    /// Rate-limit configuration applied to the public search API (anonymous bucket).
    ///
    /// Wrapped in `Arc<RwLock<>>` so that the admin config endpoint can update
    /// it at runtime and have the change take effect immediately for new
    /// requests, without restarting the server.
    pub rate_limit_config: Arc<RwLock<RateLimitConfig>>,
    /// Rate-limit configuration for known peer node IPs (peer bucket).
    ///
    /// IPs matching any enabled `node_peers.url` host use this more generous
    /// bucket instead of [`rate_limit_config`].
    pub peer_rate_limit_config: Arc<RwLock<RateLimitConfig>>,
    /// Cached set of peer IP host names, refreshed periodically from the DB.
    ///
    /// Avoids a SQLite query on every request to determine whether the client
    /// IP belongs to a known peer node.
    pub peer_ip_cache: Arc<RwLock<PeerIpCache>>,
    /// Runtime-tunable configuration (fanout depth, crawl settings, etc.).
    ///
    /// Persisted in `app_config` as a JSON blob and loaded on startup so
    /// changes survive restarts.
    pub runtime_config: Arc<RwLock<RuntimeConfig>>,
    /// Path to the Tantivy search index directory.
    ///
    /// Used by the admin status endpoint to compute `index_size_bytes`.
    pub index_path: PathBuf,
    /// Maximum allowed index size in bytes.
    ///
    /// Reported by the admin status endpoint as `max_index_bytes`.
    pub max_index_bytes: u64,
    /// HTTP client used to proxy requests to federation peer nodes.
    ///
    /// Shared across all handlers that need to communicate with remote peers
    /// (e.g. `GET /api/admin/nodes/:id/collections`).
    pub peer_client: Arc<dyn PeerClient>,
    /// Plain `reqwest::Client` used for outbound federation requests such as
    /// the gossip exchange initiated when a new node peer is added.
    ///
    /// Distinct from `peer_client` (which wraps a trait object) so that
    /// gossip logic can call the `exchange_with_peer` function directly
    /// without going through the peer abstraction layer.
    pub http_client: reqwest::Client,
    /// User-Agent string used by the crawler when fetching pages.
    ///
    /// Also used by `detect_canonical_prefix` on the admin add-target path
    /// so canonical detection sees the same UA the crawler will later see —
    /// important for sites that 403/429 the default reqwest UA.
    pub crawler_user_agent: String,
    /// Sender used by the admin crawl-target Remove handler to ask the
    /// indexer task to drop a batch of URLs from Tantivy after the SQLite
    /// cascade removes the matching `crawled_pages` rows.
    ///
    /// Without this, deleting a crawl target leaves its pages searchable
    /// until the index is rebuilt.
    pub indexer_delete_tx: tokio::sync::mpsc::Sender<Vec<String>>,
    /// Sender used by the admin import handler to enqueue documents on the
    /// same single-writer Tantivy channel that the crawler uses. Tantivy
    /// allows only one `IndexWriter` per process; this channel funnels all
    /// upserts through the dedicated indexer task spawned by the scheduler.
    pub indexer_upsert_tx: tokio::sync::mpsc::Sender<crate::index::indexer::IndexJob>,
    /// When `true`, the scheduler skips dispatching new crawl-target tasks.
    /// In-flight targets finish their current BFS — pause is "no new
    /// crawls," not "stop now." Exposed so the admin endpoint can flip it
    /// before running an export.
    ///
    /// Transient: not persisted across restarts.
    pub crawl_paused: Arc<AtomicBool>,
    /// Live counter of `IndexJob`s in flight: incremented when a job is
    /// pushed onto `indexer_upsert_tx`, decremented once the indexer
    /// commits its containing batch to Tantivy. Reaches zero exactly when
    /// the indexer has drained everything queued. Used by the status
    /// endpoint as the source of truth for "is it safe to export."
    pub indexing_inflight: Arc<AtomicI64>,
}

// Manual Debug impl: Connection and Searcher do not implement Debug.
impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState").finish_non_exhaustive()
    }
}

/// Allow handlers that only need `SharedDb` to extract it from `AppState`.
///
/// This enables `State<SharedDb>` in `list_collections` while the router's
/// overall state type is `AppState`.
impl FromRef<AppState> for SharedDb {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.db)
    }
}

// -- Request types ------------------------------------------------------------

/// Body of a POST /api/search request.
#[derive(Clone, Deserialize, Serialize)]
pub struct SearchRequest {
    pub query: String,
    pub collection: Option<String>,
    /// Fan-out depth: an engine receiving `depth > 0` MAY fan out to its own
    /// collection peers with `depth - 1`, stopping at `0`. The `remaining_depth`
    /// alias is accepted on the wire for compatibility with engines predating
    /// the rename.
    #[serde(default, alias = "remaining_depth")]
    pub depth: u32,
}

// -- Constants ----------------------------------------------------------------

/// Maximum number of results returned per search request.
pub const SEARCH_LIMIT: usize = 25;

// -- Response types -----------------------------------------------------------

#[derive(Serialize)]
pub struct CollectionListItem {
    pub name: String,
    pub description: String,
    /// Number of documents indexed under this collection's name.
    pub documents: u64,
}

#[derive(Serialize)]
pub struct CollectionsResponse {
    pub protocol_version: &'static str,
    pub collections: Vec<CollectionListItem>,
}

// -- Handlers -----------------------------------------------------------------

/// Liveness probe. Returns 200 with {"status":"ok"}.
pub async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// GET /api/collections — list all collections ordered by name.
pub async fn list_collections(State(state): State<AppState>) -> Json<CollectionsResponse> {
    // Pull (name, description) under the DB lock, then drop the guard
    // before querying Tantivy to keep the lock window short.
    let basics: Vec<(String, String)> = {
        let conn = state.db.lock().expect("db mutex poisoned");
        let mut stmt = conn
            .prepare("SELECT name, COALESCE(description, '') FROM collections ORDER BY name")
            .expect("prepare list_collections query");
        stmt.query_map([], |row| {
            Ok::<(String, String), rusqlite::Error>((row.get(0)?, row.get(1)?))
        })
        .expect("execute list_collections query")
        .map(|r| r.expect("map collection row"))
        .collect()
    };

    let collections = basics
        .into_iter()
        .map(|(name, description)| {
            // count_in_collection failures (e.g. transient reader reload)
            // shouldn't break the listing — fall back to 0.
            let documents = state.search.count_in_collection(&name).unwrap_or(0);
            CollectionListItem {
                name,
                description,
                documents,
            }
        })
        .collect();

    Json(CollectionsResponse {
        protocol_version: PROTOCOL_VERSION,
        collections,
    })
}

/// Maximum wall-clock time spent collecting peer fan-out results for the
/// non-streaming JSON / MCP search paths. Local results are always returned;
/// peers that exceed this budget are simply omitted from the response.
pub const FANOUT_COLLECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum federation fan-out depth honored by the non-streaming search paths.
/// Mirrors the streaming handler's depth contract (peers decrement and stop at 0).
pub const MAX_FANOUT_DEPTH: u32 = 2;

/// Resolve a collection name to its numeric id for ranking config.
///
/// Returns `None` when `collection` is `None` or no row matches. The DB lock is
/// taken and released entirely within this call — never held across an await.
fn lookup_collection_id(db: &SharedDb, collection: Option<&str>) -> Option<i64> {
    let name = collection?;
    let conn = db.lock().expect("db mutex poisoned");
    conn.query_row(
        "SELECT id FROM collections WHERE name = ?1",
        rusqlite::params![name],
        |row| row.get::<_, i64>(0),
    )
    .ok()
}

/// Run a search and collect the results into a single `Vec` (non-streaming).
///
/// This is the synchronous counterpart to [`search_handler`]'s SSE stream,
/// shared by the GET `/api/search` JSON endpoint and the MCP `search` tool —
/// both of which need one bounded value rather than an open stream.
///
/// Local-index results are always included. When `depth > 0`, the call also
/// fans out to the active collection peers for `collection`, bounded by
/// [`FANOUT_COLLECT_TIMEOUT`]; peers that error or exceed the budget are dropped
/// rather than failing the whole call. The merged results are sorted by score
/// descending so callers get a single ranked list.
pub async fn collect_search(
    state: &AppState,
    query: &str,
    collection: Option<&str>,
    depth: u32,
) -> Vec<SearchResult> {
    let collection_id = lookup_collection_id(&state.db, collection);
    let now = chrono::Utc::now().timestamp();

    // --- Local search (blocking) ---------------------------------------------
    let search = Arc::clone(&state.search);
    let q = query.to_string();
    let col = collection.map(str::to_string);
    let local = tokio::task::spawn_blocking(move || {
        search.local_search(&q, col.as_deref(), collection_id, SEARCH_LIMIT, now)
    })
    .await;

    let mut results: Vec<SearchResult> = match local {
        Ok(Ok(items)) => items,
        Ok(Err(e)) => {
            tracing::error!("local_search error: {e}");
            Vec::new()
        }
        Err(e) => {
            tracing::error!("spawn_blocking join error: {e}");
            Vec::new()
        }
    };

    // --- Peer fan-out (bounded) ----------------------------------------------
    if depth > 0 {
        let local_col = collection.unwrap_or("");
        let peers = {
            let conn = state.db.lock().expect("db mutex poisoned");
            active_collection_peers_for(&conn, local_col).unwrap_or_else(|e| {
                tracing::warn!("active_collection_peers_for error: {e}");
                vec![]
            })
            // MutexGuard released here before any await
        };

        if !peers.is_empty() {
            let req = SearchRequest {
                query: query.to_string(),
                collection: collection.map(str::to_string),
                depth,
            };
            let outgoing_depth = depth.saturating_sub(1).min(u8::MAX as u32) as u8;
            let mut peer_stream =
                dispatch(Arc::clone(&state.peer_client), peers, req, outgoing_depth);

            // Drain peer outcomes until the stream ends or the budget elapses.
            // Accumulating into `results` outside the timeout means a slow peer
            // costs us its results, not the fast peers' results already in hand.
            let deadline = tokio::time::Instant::now() + FANOUT_COLLECT_TIMEOUT;
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, peer_stream.next()).await {
                    Ok(Some(outcome)) => {
                        {
                            let conn = state.db.lock().expect("db mutex poisoned");
                            if let Err(e) = record_result(
                                &conn,
                                outcome.node_peer_id,
                                outcome.result.is_ok(),
                                Some(outcome.elapsed_ms),
                            ) {
                                tracing::warn!("record_result error: {e}");
                            }
                            // MutexGuard released before next await
                        }
                        match outcome.result {
                            Ok(rs) => results.extend(rs),
                            Err(e) => tracing::warn!("peer search error: {e}"),
                        }
                    }
                    Ok(None) => break, // all peers reported
                    Err(_) => break,   // deadline reached
                }
            }
        }
    }

    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results
}

/// Query parameters for `GET /api/search`.
#[derive(Debug, Deserialize)]
pub struct SearchGetParams {
    /// The search query. `query` is accepted as an alias for ergonomics.
    #[serde(default, alias = "query")]
    pub q: String,
    /// Optional collection name to scope the search.
    pub collection: Option<String>,
    /// Federation fan-out depth. `0` (default) searches this engine only;
    /// `1`–`2` also query federated peers (bounded by [`FANOUT_COLLECT_TIMEOUT`]).
    #[serde(default)]
    pub depth: u32,
}

/// Single-shot JSON search response. Mirrors the non-streaming fallback shape
/// in `docs/COMMUNITY_SEARCH_PROTOCOL.md` §4.3.
#[derive(Serialize)]
pub struct JsonSearchResponse {
    pub protocol_version: &'static str,
    pub results: Vec<SearchResult>,
    pub duration_ms: u64,
}

/// Run a non-streaming search and build the JSON envelope (protocol §4.3).
/// Shared by the GET endpoint and the POST `Accept: application/json` fallback.
async fn run_json_search(
    state: &AppState,
    query: &str,
    collection: Option<&str>,
    depth: u32,
) -> JsonSearchResponse {
    let depth = depth.min(MAX_FANOUT_DEPTH);
    let start = tokio::time::Instant::now();
    let results = collect_search(state, query, collection, depth).await;
    JsonSearchResponse {
        protocol_version: PROTOCOL_VERSION,
        results,
        duration_ms: start.elapsed().as_millis() as u64,
    }
}

/// Whether the client has asked for the non-streaming JSON response.
///
/// Returns `true` only when `Accept` explicitly names `application/json` and
/// does *not* name `text/event-stream`, so the default (`*/*`, no header, or an
/// SSE-aware client) keeps the canonical streaming response.
fn wants_json(headers: &HeaderMap) -> bool {
    match headers.get(header::ACCEPT).and_then(|v| v.to_str().ok()) {
        Some(accept) => {
            accept.contains("application/json") && !accept.contains("text/event-stream")
        }
        None => false,
    }
}

/// GET /api/search?q=…&collection=…&depth=N — non-streaming JSON search.
///
/// The machine-friendly counterpart to the SSE `POST /api/search`: one GET, one
/// JSON document. Built for `curl`, scripts, and LLM fetchers that cannot
/// consume an event stream. Local-only by default; pass `depth=1` (or `2`) to
/// opt into bounded federated fan-out.
pub async fn search_get(
    State(state): State<AppState>,
    Query(params): Query<SearchGetParams>,
) -> Result<Json<JsonSearchResponse>, (StatusCode, String)> {
    let query = params.q.trim();
    if query.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "query parameter `q` is required".to_string(),
        ));
    }

    Ok(Json(
        run_json_search(&state, query, params.collection.as_deref(), params.depth).await,
    ))
}

/// POST /api/search — stream search results as Server-Sent Events.
///
/// Validates that `query` is non-empty, then streams up to [`SEARCH_LIMIT`]
/// results from the local index, followed by a `source_complete` event.
///
/// When `depth > 0`, the handler also fans out to all active collection peers
/// for the requested collection before emitting the final `done` event. `depth`
/// is decremented by one (saturating) before forwarding so that chains of peers
/// do not loop indefinitely.
///
/// Peer errors are logged with `tracing::warn` but never abort the stream.
///
/// **Non-streaming fallback (protocol §4.3):** a client that sends
/// `Accept: application/json` (and not `text/event-stream`) gets a single JSON
/// object instead of the SSE stream — for peers/clients that cannot consume SSE.
pub async fn search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SearchRequest>,
) -> Result<Response, (StatusCode, String)> {
    if req.query.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "query is required".to_string()));
    }

    // Content negotiation: non-streaming JSON fallback for non-SSE clients.
    if wants_json(&headers) {
        let resp = run_json_search(
            &state,
            req.query.trim(),
            req.collection.as_deref(),
            req.depth,
        )
        .await;
        return Ok(Json(resp).into_response());
    }

    // Look up the numeric collection id for ranking config (synchronous, not
    // held across any await point).
    let collection_id = lookup_collection_id(&state.db, req.collection.as_deref());

    let now = chrono::Utc::now().timestamp();
    let (tx, rx) = mpsc::channel::<SseEvent>(64);

    // Extract fields needed in the spawned task before consuming `req`.
    let depth = req.depth;
    // Clone the full request for fanout forwarding; the original's fields are
    // moved into the local-search closure below.
    let fanout_req = req.clone();
    let query = req.query;
    let collection = req.collection;
    let search = Arc::clone(&state.search);
    let db = Arc::clone(&state.db);
    let peer_client = Arc::clone(&state.peer_client);

    tokio::spawn(async move {
        // --- Local search (blocking) -----------------------------------------
        let result = tokio::task::spawn_blocking(move || {
            search.local_search(
                &query,
                collection.as_deref(),
                collection_id,
                SEARCH_LIMIT,
                now,
            )
        })
        .await;

        match result {
            Ok(Ok(items)) => {
                for item in items {
                    let _ = tx.send(SseEvent::Result(item)).await;
                }
            }
            Ok(Err(e)) => {
                tracing::error!("local_search error: {e}");
            }
            Err(e) => {
                tracing::error!("spawn_blocking join error: {e}");
            }
        }

        let _ = tx
            .send(SseEvent::SourceComplete {
                source: "local".to_string(),
            })
            .await;

        // --- Peer fan-out (Phase 5) ------------------------------------------
        // Only fan out when the request still has remaining depth, preventing
        // infinite forwarding chains.
        if depth > 0 {
            let local_col = fanout_req.collection.as_deref().unwrap_or("");
            let peers = {
                let conn = db.lock().expect("db mutex poisoned");
                active_collection_peers_for(&conn, local_col).unwrap_or_else(|e| {
                    tracing::warn!("active_collection_peers_for error: {e}");
                    vec![]
                })
                // MutexGuard released here before any await
            };
            let outgoing_depth = depth.saturating_sub(1).min(u8::MAX as u32) as u8;
            let mut peer_stream = dispatch(peer_client, peers, fanout_req, outgoing_depth);
            while let Some(outcome) = peer_stream.next().await {
                // Record peer health for every outcome, success or failure.
                {
                    let conn = db.lock().expect("db mutex poisoned");
                    if let Err(e) = record_result(
                        &conn,
                        outcome.node_peer_id,
                        outcome.result.is_ok(),
                        Some(outcome.elapsed_ms),
                    ) {
                        tracing::warn!("record_result error: {e}");
                    }
                    // MutexGuard released here before next await
                }
                match outcome.result {
                    Ok(results) => {
                        let source_label = outcome.source_label;
                        for r in results {
                            let _ = tx.send(SseEvent::Result(r)).await;
                        }
                        let _ = tx
                            .send(SseEvent::SourceComplete {
                                source: source_label,
                            })
                            .await;
                    }
                    Err(e) => {
                        tracing::warn!("peer search error: {e}");
                    }
                }
            }
        }

        // Always terminate the stream.
        let _ = tx.send(SseEvent::Done).await;
    });

    let stream = ReceiverStream::new(rx).map(|event| {
        Ok::<Event, Infallible>(Event::default().event(event.name()).data(event.data_json()))
    });

    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response())
}

// -- Tests --------------------------------------------------------------------

// -- Test helpers -------------------------------------------------------------

#[cfg(test)]
impl AppState {
    /// Build an `AppState` backed by an in-memory SQLite database and an
    /// in-RAM Tantivy index, using the given string as the admin token.
    ///
    /// Intended for unit tests that need a fully initialised `AppState`
    /// without any external dependencies.
    pub fn for_tests_with_token(token: &str) -> Self {
        use crate::federation::peer::HttpPeerClient;
        use crate::index::{reader::Searcher, schema};
        use crate::search::service::SearchService;
        use rusqlite::Connection;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        use tantivy::Index;

        let conn = Connection::open_in_memory().expect("open in-memory db");
        crate::db::run_migrations(&conn).expect("run migrations");
        let db: SharedDb = Arc::new(Mutex::new(conn));
        let index = Index::create_in_ram(schema::build());
        let searcher = Searcher::open(index).expect("open in-ram searcher");
        let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));
        AppState {
            admin_token: token.to_string(),
            self_url: String::new(),
            db,
            search: Arc::new(service),
            rate_limit_config: Arc::new(RwLock::new(RateLimitConfig::default())),
            peer_rate_limit_config: Arc::new(RwLock::new(RateLimitConfig {
                limit: 120,
                ..RateLimitConfig::default()
            })),
            peer_ip_cache: Arc::new(RwLock::new(PeerIpCache::new())),
            runtime_config: Arc::new(RwLock::new(RuntimeConfig::default())),
            // Use a non-existent path so index_dir_size_bytes returns 0.
            index_path: PathBuf::from("/tmp/community-search-test-index-nonexistent"),
            max_index_bytes: 10_737_418_240, // 10 GiB
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
            crawl_paused: Arc::new(AtomicBool::new(false)),
            indexing_inflight: Arc::new(AtomicI64::new(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::{get, post},
        Router,
    };
    use tantivy::Index;
    use tower::ServiceExt;

    use crate::index::{reader::Searcher, schema};

    /// Open an in-memory SQLite database, apply all migrations, and insert two
    /// fixture collections.
    fn test_db() -> SharedDb {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        crate::db::run_migrations(&conn).expect("run migrations");

        conn.execute(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES ('1', 'tech', 'Tech blogs', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')",
            [],
        )
        .expect("insert tech collection");

        conn.execute(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES ('2', 'cooking', 'Recipes', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')",
            [],
        )
        .expect("insert cooking collection");

        Arc::new(Mutex::new(conn))
    }

    /// Build a minimal `AppState` backed by an in-RAM index and the given db.
    fn test_app_state(db: SharedDb) -> AppState {
        use crate::federation::peer::HttpPeerClient;
        use std::time::Duration;
        let index = Index::create_in_ram(schema::build());
        let searcher = Searcher::open(index).unwrap();
        let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));
        AppState {
            db,
            search: Arc::new(service),
            admin_token: String::from("test-admin-token"),
            self_url: String::new(),
            rate_limit_config: Arc::new(RwLock::new(RateLimitConfig::default())),
            peer_rate_limit_config: Arc::new(RwLock::new(RateLimitConfig {
                limit: 120,
                ..RateLimitConfig::default()
            })),
            peer_ip_cache: Arc::new(RwLock::new(PeerIpCache::new())),
            runtime_config: Arc::new(RwLock::new(RuntimeConfig::default())),
            index_path: PathBuf::from("/tmp/community-search-test-index-nonexistent"),
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
            crawl_paused: Arc::new(AtomicBool::new(false)),
            indexing_inflight: Arc::new(AtomicI64::new(0)),
        }
    }

    #[tokio::test]
    async fn lists_collections_with_version() {
        let db = test_db();
        let state = test_app_state(db);

        let app = Router::new()
            .route("/api/collections", get(list_collections))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/collections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(json["protocol_version"], "1.0");

        let collections = json["collections"].as_array().unwrap();
        assert_eq!(collections.len(), 2);
        assert_eq!(
            collections[0]["name"], "cooking",
            "first item must be 'cooking' (alpha order)"
        );
        assert_eq!(
            collections[1]["name"], "tech",
            "second item must be 'tech' (alpha order)"
        );
    }

    #[tokio::test]
    async fn search_handler_rejects_empty_query() {
        let db = test_db();
        let app_state = test_app_state(db);

        let app = Router::new()
            .route("/api/search", post(search_handler))
            .with_state(app_state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/search")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"query":""}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "empty query should return 400"
        );

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains("query is required"), "body: {body}");
    }

    #[tokio::test]
    async fn search_handler_rejects_whitespace_only_query() {
        let db = test_db();
        let app_state = test_app_state(db);

        let app = Router::new()
            .route("/api/search", post(search_handler))
            .with_state(app_state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/search")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"query":"   "}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "whitespace-only query should return 400"
        );
    }

    #[tokio::test]
    async fn search_handler_streams_done_for_valid_query() {
        use axum::http::header;

        let db = test_db();
        let app_state = test_app_state(db);

        let app = Router::new()
            .route("/api/search", post(search_handler))
            .with_state(app_state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/search")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"query":"rust"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "valid query must return 200"
        );

        let ct = response
            .headers()
            .get(header::CONTENT_TYPE)
            .expect("missing content-type")
            .to_str()
            .unwrap();
        assert!(
            ct.starts_with("text/event-stream"),
            "expected text/event-stream, got: {ct}"
        );

        // Collect body bytes with a short timeout so the test doesn't hang.
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body_bytes.to_vec()).unwrap();

        assert!(body.contains("event: source_complete"), "body: {body}");
        assert!(body.contains("event: done"), "body: {body}");
    }

    #[tokio::test]
    async fn search_get_returns_json_envelope() {
        let db = test_db();
        let app_state = test_app_state(db);

        let app = Router::new()
            .route("/api/search", get(search_get))
            .with_state(app_state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?q=rust")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(json["protocol_version"], "1.0");
        assert!(json["results"].is_array(), "results must be an array");
        assert!(
            json["duration_ms"].is_number(),
            "duration_ms must be present"
        );
    }

    #[test]
    fn search_request_accepts_remaining_depth_alias() {
        // Wire compatibility: engines predating the rename send `remaining_depth`.
        let req: SearchRequest =
            serde_json::from_str(r#"{"query":"x","remaining_depth":2}"#).unwrap();
        assert_eq!(req.depth, 2);
        // And the canonical `depth` name works too.
        let req: SearchRequest = serde_json::from_str(r#"{"query":"x","depth":1}"#).unwrap();
        assert_eq!(req.depth, 1);
    }

    #[tokio::test]
    async fn search_post_json_fallback_on_accept_header() {
        let db = test_db();
        let app_state = test_app_state(db);

        let app = Router::new()
            .route("/api/search", post(search_handler))
            .with_state(app_state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/search")
                    .header("content-type", "application/json")
                    .header("accept", "application/json")
                    .body(Body::from(r#"{"query":"rust"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let ct = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            ct.starts_with("application/json"),
            "fallback must be JSON, got: {ct}"
        );

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["protocol_version"], "1.0");
        assert!(json["results"].is_array());
    }

    #[tokio::test]
    async fn search_get_rejects_missing_query() {
        let db = test_db();
        let app_state = test_app_state(db);

        let app = Router::new()
            .route("/api/search", get(search_get))
            .with_state(app_state);

        // No `q`, and whitespace-only `q`, both 400.
        for uri in ["/api/search", "/api/search?q=%20%20"] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "uri {uri} should 400"
            );
        }
    }
}
