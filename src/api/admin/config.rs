//! Admin runtime-config endpoints.
//!
//! `PUT /api/admin/config`  — patch one or more runtime-config fields.
//! `GET /api/admin/config`  — return the current runtime config as JSON.
//!
//! Changes take effect immediately for new requests and are persisted to the
//! `app_config` table (key `runtime_config`) so they survive a restart.

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, put},
    Json, Router,
};
use serde::Deserialize;

use crate::api::public::AppState;
use crate::RuntimeConfig;

// ---------------------------------------------------------------------------
// Persistence key
// ---------------------------------------------------------------------------

const RUNTIME_CONFIG_KEY: &str = "runtime_config";

// ---------------------------------------------------------------------------
// Request type
// ---------------------------------------------------------------------------

/// Body for `PUT /api/admin/config`.
///
/// All fields are optional — only those present in the request body are applied
/// to the current runtime config. Absent fields are left unchanged.
#[derive(Deserialize)]
struct PutConfigBody {
    fanout_depth: Option<u32>,
    search_rate_limit_per_minute: Option<u32>,
    crawl_default_recrawl_interval_secs: Option<u64>,
    crawl_politeness_delay_ms: Option<u64>,
    max_concurrent_crawls: Option<u32>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// PUT /api/admin/config
///
/// Applies the provided patch to the current [`RuntimeConfig`].  Only fields
/// present in the request body are updated; absent fields are left unchanged.
///
/// If `search_rate_limit_per_minute` is provided the rate-limit config is
/// also updated so that the new limit takes effect for the very next request.
///
/// The updated config is persisted to `app_config` (key `runtime_config`) via
/// UPSERT so it survives a server restart.
///
/// Returns the full updated config as JSON.
async fn put_config(
    State(state): State<AppState>,
    Json(body): Json<PutConfigBody>,
) -> Result<Json<RuntimeConfig>, (StatusCode, String)> {
    // --- 1. Apply patch to runtime_config (released before DB write) ---------
    let (snapshot, new_rl_limit) = {
        let mut cfg = state.runtime_config.write().await;

        if let Some(v) = body.fanout_depth {
            cfg.fanout_depth = Some(v);
        }
        if let Some(v) = body.search_rate_limit_per_minute {
            cfg.search_rate_limit_per_minute = Some(v);
        }
        if let Some(v) = body.crawl_default_recrawl_interval_secs {
            cfg.crawl_default_recrawl_interval_secs = Some(v);
        }
        if let Some(v) = body.crawl_politeness_delay_ms {
            cfg.crawl_politeness_delay_ms = Some(v);
        }
        if let Some(v) = body.max_concurrent_crawls {
            cfg.max_concurrent_crawls = Some(v);
        }

        let new_rl = body.search_rate_limit_per_minute;
        (cfg.clone(), new_rl)
        // write lock on runtime_config released here
    };

    // --- 2. If rate limit changed, propagate immediately ---------------------
    if let Some(limit) = new_rl_limit {
        let mut rl = state.rate_limit_config.write().await;
        rl.limit = limit;
        // write lock on rate_limit_config released here
    }

    // --- 3. Persist the full snapshot to app_config -------------------------
    let json = serde_json::to_string(&snapshot)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    {
        let conn = state.db.lock().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "db mutex poisoned".to_string(),
            )
        })?;

        conn.execute(
            "INSERT INTO app_config (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            rusqlite::params![RUNTIME_CONFIG_KEY, json],
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        // MutexGuard released here
    }

    Ok(Json(snapshot))
}

/// GET /api/admin/config
///
/// Returns the current [`RuntimeConfig`] as JSON.  The returned value
/// reflects any in-memory updates made via `PUT /api/admin/config`; it does
/// not re-read from the database.
async fn get_config(State(state): State<AppState>) -> Json<RuntimeConfig> {
    let cfg = state.runtime_config.read().await.clone();
    Json(cfg)
}

// ---------------------------------------------------------------------------
// Startup helper
// ---------------------------------------------------------------------------

/// Load the persisted [`RuntimeConfig`] from `app_config` on startup.
///
/// Returns the stored config (or [`RuntimeConfig::default()`] if none is
/// found), together with the `search_rate_limit_per_minute` value if present,
/// so the caller can initialise [`crate::api::rate_limit::RateLimitConfig`]
/// accordingly.
///
/// # Errors
/// Returns `None` for both values if the row is missing or the stored JSON
/// cannot be parsed (logged as a warning in that case).
pub fn load_from_db(db: &crate::api::public::SharedDb) -> RuntimeConfig {
    let conn = db.lock().expect("db mutex poisoned");

    let stored: Option<String> = match conn.query_row(
        "SELECT value FROM app_config WHERE key = ?1",
        rusqlite::params![RUNTIME_CONFIG_KEY],
        |row| row.get::<_, Option<String>>(0),
    ) {
        Ok(val) => val,
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => {
            tracing::warn!("failed to read runtime_config from app_config: {e}");
            None
        }
    };

    match stored {
        None => RuntimeConfig::default(),
        Some(json) => serde_json::from_str(&json).unwrap_or_else(|e| {
            tracing::warn!("failed to parse runtime_config JSON: {e}");
            RuntimeConfig::default()
        }),
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/admin/config", put(put_config))
        .route("/api/admin/config", get(get_config))
}
