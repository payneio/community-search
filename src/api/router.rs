use axum::{
    middleware,
    routing::{get, post},
    Router,
};

use crate::api::admin::admin_router;
use crate::api::public::{health, list_collections, search_get, search_handler, AppState};
use crate::api::rate_limit::require_rate_limit;
use crate::middleware::peer_version::add_peer_version_header;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the full application router.
///
/// **Peer-facing routes** (`/api/collections`, `/api/search`,
/// `/api/gossip/exchange`) are grouped into a sub-router that carries the
/// [`add_peer_version_header`] middleware, so every response from those
/// endpoints includes `X-CommunitySearch-Version: 1.0`.
///
/// **UI / admin routes** (`/`, `/static/*path`, `/admin`, `/health`,
/// `/api/admin/*`) do **not** carry that header — they are merged directly
/// into the root router without the middleware layer.
///
/// The rate-limit middleware is applied **only** to the `/api/search` route
/// (both the streaming `POST` and the non-streaming `GET`) via `route_layer`.
/// All other routes are unaffected.
///
/// State is applied with `.with_state(state)` on the outermost router so
/// that both the peer sub-router and the admin router can extract it.
pub fn build_router(state: AppState) -> Router {
    // Rate-limited search route — middleware is scoped to this sub-router only.
    // `POST` streams SSE; `GET` returns a single JSON document (machine clients).
    let search_route = Router::new()
        .route("/api/search", post(search_handler).get(search_get))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_rate_limit,
        ));

    // Peer-facing routes: these get the X-CommunitySearch-Version header injected
    // on every response via the middleware layer below.
    let peer_routes = Router::new()
        .route("/api/collections", get(list_collections))
        .merge(search_route)
        .merge(crate::api::gossip::routes())
        .layer(middleware::from_fn(add_peer_version_header));

    // Root router: merge UI routes, admin routes, and the peer sub-router.
    // Note: no version-header layer here, so UI routes stay clean.
    Router::new()
        .route("/", get(crate::api::ui::serve_index))
        .route("/static/*path", get(crate::api::ui::serve_static))
        .route("/admin", get(crate::api::admin_page))
        .route("/health", get(health))
        // Machine-discovery + integration surfaces (no version header — these
        // are UI/agent-facing, not peer-protocol, endpoints).
        .route("/robots.txt", get(crate::api::ui::serve_robots))
        .route("/opensearch.xml", get(crate::api::ui::serve_opensearch))
        .route("/mcp", post(crate::api::mcp::mcp_handler))
        .merge(peer_routes)
        .merge(admin_router(state.clone()))
        // State is applied after all merges so every handler can extract it
        // regardless of which sub-router it was declared in.
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use rusqlite::Connection;
    use tantivy::Index;
    use tower::ServiceExt;

    use crate::api::public::SharedDb;
    use crate::index::{reader::Searcher, schema};
    use crate::search::service::SearchService;

    /// In-memory database with *all* migrations applied and one fixture collection.
    ///
    /// Using `run_migrations` ensures that tables added by later migrations
    /// (e.g. `discovered_engines` from migration 010) are available to all
    /// handlers under test, including the gossip exchange handler.
    fn test_db() -> SharedDb {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        crate::db::run_migrations(&conn).expect("run all migrations");
        conn.execute(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES ('1', 'tech', 'Tech blogs', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')",
            [],
        )
        .expect("insert tech collection");
        Arc::new(Mutex::new(conn))
    }

    /// Minimal `AppState` backed by an in-RAM Tantivy index and the given db.
    fn test_state(db: SharedDb) -> AppState {
        use std::path::PathBuf;
        use std::time::Duration;
        use tokio::sync::RwLock;
        let index = Index::create_in_ram(schema::build());
        let searcher = Searcher::open(index).expect("open in-ram searcher");
        let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));
        let default_rl = crate::api::rate_limit::RateLimitConfig::default();
        AppState {
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
            index_path: PathBuf::from("/tmp/community-search-test-index-nonexistent"),
            max_index_bytes: 10_737_418_240,
            peer_client: Arc::new(
                crate::federation::peer::HttpPeerClient::new(Duration::from_secs(10))
                    .expect("create HttpPeerClient"),
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
        }
    }

    #[tokio::test]
    async fn root_route_responds_ok() {
        let app = build_router(test_state(test_db()));
        let resp = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "GET / must return 200");
    }

    #[tokio::test]
    async fn collections_route_responds_ok() {
        let app = build_router(test_state(test_db()));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/collections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /api/collections must return 200"
        );
    }

    #[tokio::test]
    async fn version_header_present_on_peer_routes() {
        let app = build_router(test_state(test_db()));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/collections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            resp.headers().contains_key("x-communitysearch-version"),
            "version header must be present on peer routes"
        );
        assert_eq!(
            resp.headers().get("x-communitysearch-version").unwrap(),
            "1.0",
            "version header must equal '1.0'"
        );
    }

    #[tokio::test]
    async fn version_header_absent_on_root_ui() {
        let app = build_router(test_state(test_db()));
        let resp = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert!(
            !resp.headers().contains_key("x-communitysearch-version"),
            "version header must NOT be present on the root UI route"
        );
    }

    #[tokio::test]
    async fn gossip_exchange_returns_ok_with_version_header() {
        let app = build_router(test_state(test_db()));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/gossip/exchange")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"engines":[],"protocol_version":"1.0"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "gossip exchange with compatible version must return 200 OK"
        );
        assert!(
            resp.headers().contains_key("x-communitysearch-version"),
            "gossip exchange must carry the X-CommunitySearch-Version header"
        );
    }

    #[tokio::test]
    async fn search_route_rejects_empty_query() {
        let app = build_router(test_state(test_db()));
        let resp = app
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
            resp.status(),
            StatusCode::BAD_REQUEST,
            "empty query must return 400"
        );
    }
}
