use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use community_search::api;
use community_search::api::public::AppState;
use community_search::api::rate_limit::{PeerIpCache, RateLimitConfig};
use community_search::auth::token::ensure_and_announce_admin_token;
use community_search::config::Config;
use community_search::db::Database;
use community_search::federation::discovered;
use community_search::federation::peer::HttpPeerClient;
use community_search::index;
use community_search::index::reader::Searcher;
use community_search::search::service::SearchService;
use community_search::RuntimeConfig;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialise structured logging with RUST_LOG (default: info)
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Load configuration (.env + environment variables)
    let cfg = Config::load().context("failed to load configuration")?;

    info!(
        bind_addr = %cfg.bind_addr,
        port = cfg.port,
        data_dir = %cfg.data_dir.display(),
        "starting community-search"
    );

    // Ensure data directory exists
    std::fs::create_dir_all(&cfg.data_dir)
        .with_context(|| format!("failed to create data dir: {}", cfg.data_dir.display()))?;

    // Open SQLite database (used by the crawler scheduler and admin logic)
    let db = Database::open(cfg.data_dir.join("data.sqlite")).context("failed to open database")?;

    // Ensure admin token exists (generate on first run, announce if newly minted).
    let admin_token = ensure_and_announce_admin_token(&db, cfg.admin_token.as_deref())
        .context("failed to ensure admin token")?;

    // Ensure this engine's own URL is recorded in the discovered_engines table.
    {
        let conn = db.connection();
        let now = chrono::Utc::now().timestamp();
        discovered::ensure_self_entry(
            &conn,
            &cfg.self_url,
            &cfg.self_name,
            &cfg.self_description,
            now,
        )
        .context("failed to ensure self-entry in discovered_engines")?;
    }

    // Open (or create) Tantivy search index
    let index = index::open_or_create(cfg.data_dir.join("index"))
        .context("failed to open/create search index")?;

    let index_path = cfg.data_dir.join("index");
    let scheduler = community_search::crawler::scheduler::Scheduler {
        config: std::sync::Arc::new(cfg.clone()),
        db: std::sync::Arc::new(db),
        index: std::sync::Arc::new(index.clone()),
        index_path,
    };

    // Admin-side delete channel: the admin crawl-target Remove handler
    // pushes URL batches in here so the indexer can drop the corresponding
    // documents from Tantivy after the DB cascade. Sized small — admin
    // removals are infrequent and each batch may contain many URLs.
    let (indexer_delete_tx, indexer_delete_rx) =
        tokio::sync::mpsc::channel::<Vec<String>>(32);

    let _sched_handle =
        scheduler.spawn(std::time::Duration::from_secs(60), indexer_delete_rx);

    // Build the search infrastructure.
    //
    // Open a dedicated read connection for API handlers, separate from the
    // crawler scheduler's connection. SQLite WAL mode supports concurrent
    // readers from multiple connections without blocking.
    let index_searcher =
        Arc::new(Searcher::open(index.clone()).context("failed to open index reader")?);

    let api_conn = rusqlite::Connection::open(cfg.data_dir.join("data.sqlite"))
        .context("failed to open API database connection")?;

    // Load persisted runtime config before wrapping the connection in Arc<Mutex>.
    let initial_runtime_config: RuntimeConfig = {
        use rusqlite::OptionalExtension;
        let stored: Option<String> = api_conn
            .query_row(
                "SELECT value FROM app_config WHERE key = 'runtime_config'",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .ok()
            .flatten()
            .flatten();
        stored
            .and_then(|json| serde_json::from_str::<RuntimeConfig>(&json).ok())
            .unwrap_or_default()
    };

    // Apply any persisted rate-limit setting.
    let initial_rate_limit_config = {
        let mut rl = RateLimitConfig::default();
        if let Some(limit) = initial_runtime_config.search_rate_limit_per_minute {
            rl.limit = limit;
        }
        rl
    };

    let db = Arc::new(Mutex::new(api_conn));

    let search_service = Arc::new(SearchService::new(index_searcher, db.clone()));
    let peer_client = Arc::new(
        HttpPeerClient::new(std::time::Duration::from_secs(10))
            .context("failed to create HTTP peer client")?,
    );
    let initial_peer_rate_limit_config = RateLimitConfig {
        limit: cfg.peer_rate_limit_per_minute,
        ..RateLimitConfig::default()
    };

    let http_client = reqwest::Client::builder()
        .user_agent(format!("community-search/{}", env!("CARGO_PKG_VERSION")))
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .context("failed to build reqwest HTTP client")?;

    let state = AppState {
        db: db.clone(),
        search: search_service,
        admin_token,
        self_url: cfg.self_url.clone(),
        rate_limit_config: Arc::new(RwLock::new(initial_rate_limit_config)),
        peer_rate_limit_config: Arc::new(RwLock::new(initial_peer_rate_limit_config)),
        peer_ip_cache: Arc::new(RwLock::new(PeerIpCache::new())),
        runtime_config: Arc::new(RwLock::new(initial_runtime_config)),
        index_path: cfg.data_dir.join("index"),
        max_index_bytes: cfg.max_index_bytes,
        peer_client,
        http_client,
        crawler_user_agent: cfg.crawler_user_agent.clone(),
        indexer_delete_tx,
    };
    let _health_task = community_search::federation::health::spawn_health_check_task(
        state.db.clone(),
        state.peer_client.clone(),
        std::time::Duration::from_secs(60 * 60 * 24),
    );

    let _gossip_sync_task = tokio::spawn(
        community_search::federation::gossip::run_periodic_sync_loop(
            state.http_client.clone(),
            db.clone(),
            std::time::Duration::from_secs(cfg.gossip_sync_interval_secs),
        ),
    );

    let app = api::router::build_router(state);

    // Bind TCP listener and start HTTP server
    let addr: SocketAddr = format!("{}:{}", cfg.bind_addr, cfg.port)
        .parse()
        .context("invalid bind address")?;

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind to {addr}"))?;

    info!("listening on http://{addr}");

    if let Err(e) = axum::serve(listener, app).await {
        warn!("server error: {e}");
    }

    Ok(())
}
