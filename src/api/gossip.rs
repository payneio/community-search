//! `POST /api/gossip/exchange` — engine-list gossip endpoint.
//!
//! Peers call this endpoint to exchange lists of discovered engines.  The
//! handler validates the peer's protocol version, merges the incoming engine
//! list into the local database, and returns the *pre-merge* local list so
//! that the caller learns what this node already knew.

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};

use crate::{
    api::public::AppState,
    federation::{discovered, discovered::DiscoveredEngine, gossip::merge_engine_lists},
    protocol::{check_compatibility, Compatibility, PROTOCOL_VERSION},
};

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

/// Body of `POST /api/gossip/exchange`.
#[derive(Serialize, Deserialize)]
pub struct ExchangeRequest {
    /// The calling peer's protocol version string (e.g. `"1.0"`).
    pub protocol_version: String,
    /// Engines the peer wants to share with us.  Defaults to an empty list.
    #[serde(default)]
    pub engines: Vec<EngineSummary>,
}

/// A compact engine summary used in gossip payloads.
///
/// Fields are `pub` so that they can be reused by the Task 9 gossip client.
#[derive(Serialize, Deserialize, Clone)]
pub struct EngineSummary {
    pub url: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
}

/// Successful response from `POST /api/gossip/exchange`.
#[derive(Serialize, Deserialize)]
pub struct ExchangeResponse {
    /// This node's protocol version, always `"1.0"`.
    pub protocol_version: String,
    /// The engine list as it existed *before* merging the peer's contribution.
    pub engines: Vec<EngineSummary>,
}

/// JSON error body returned on `4xx` / `5xx` responses.
#[derive(Serialize)]
pub struct ErrorBody {
    error: String,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Return a router that handles `POST /api/gossip/exchange`.
pub fn routes() -> Router<AppState> {
    Router::new().route("/api/gossip/exchange", post(exchange))
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// Handle `POST /api/gossip/exchange`.
///
/// Protocol-version validation:
/// - Same major version → accept.
/// - Same major, different minor → log a warning and accept.
/// - Different major → reject with HTTP 400.
///
/// After validation the handler:
/// 1. Reads the local discovered-engine list (pre-merge snapshot).
/// 2. Merges the peer's incoming list into the local database.
/// 3. Returns the pre-merge snapshot so the peer learns what we already knew.
pub async fn exchange(
    State(state): State<AppState>,
    Json(req): Json<ExchangeRequest>,
) -> Result<Json<ExchangeResponse>, (StatusCode, Json<ErrorBody>)> {
    // ── Protocol-version check ────────────────────────────────────────────
    match check_compatibility(&req.protocol_version) {
        Compatibility::Incompatible => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: format!(
                        "incompatible protocol version: theirs={} ours={}",
                        req.protocol_version, PROTOCOL_VERSION
                    ),
                }),
            ));
        }
        Compatibility::MinorMismatch => {
            tracing::warn!(
                their = %req.protocol_version,
                ours = PROTOCOL_VERSION,
                "minor protocol version mismatch on gossip exchange"
            );
        }
        Compatibility::Compatible => {}
    }

    // ── Merge and persist ─────────────────────────────────────────────────
    let now = chrono::Utc::now().timestamp();

    let incoming: Vec<DiscoveredEngine> = req
        .engines
        .into_iter()
        .map(|s| DiscoveredEngine {
            url: s.url,
            name: s.name,
            description: s.description,
            first_seen: now,
            last_seen: now,
        })
        .collect();

    // Hold the database lock for the entire read-merge-write sequence.
    // No `.await` is called while holding the guard, so this is safe.
    let pre_merge_engines = {
        let conn = state
            .db
            .lock()
            .map_err(|_| internal_error("db mutex poisoned"))?;

        let local = discovered::list(&conn).map_err(|e| internal_error(e.to_string()))?;

        let merged = merge_engine_lists(local.clone(), incoming, now);
        for e in &merged {
            discovered::upsert(&conn, e).map_err(|e| internal_error(e.to_string()))?;
        }

        // Convert the *pre-merge* local list for the response.
        local
            .into_iter()
            .map(|e| EngineSummary {
                url: e.url,
                name: e.name,
                description: e.description,
            })
            .collect::<Vec<_>>()
    };

    Ok(Json(ExchangeResponse {
        protocol_version: PROTOCOL_VERSION.to_string(),
        engines: pre_merge_engines,
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn internal_error<E: std::fmt::Display>(e: E) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: e.to_string(),
        }),
    )
}
