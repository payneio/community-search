//! Admin API for manual gossip triggers.
//!
//! Routes:
//! - `POST /api/admin/gossip/trigger` — immediately exchange gossip with an arbitrary peer URL

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};

use crate::api::gossip::{EngineSummary, ExchangeRequest, ExchangeResponse};
use crate::api::public::AppState;
use crate::federation::discovered::{self, DiscoveredEngine};
use crate::federation::gossip::merge_engine_lists;
use crate::protocol::{check_compatibility, Compatibility, PROTOCOL_VERSION};

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

/// Body of `POST /api/admin/gossip/trigger`.
#[derive(Deserialize)]
pub struct TriggerGossipReq {
    /// Base URL of the peer to exchange gossip with.
    pub url: String,
}

/// Successful response from `POST /api/admin/gossip/trigger`.
#[derive(Serialize)]
pub struct TriggerGossipResp {
    /// Always `"ok"` on success.
    pub status: &'static str,
    /// Number of engines sent to the peer.
    pub sent_count: usize,
    /// Number of engines received from the peer.
    pub received_count: usize,
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// POST /api/admin/gossip/trigger
///
/// Immediately performs an outbound gossip exchange with the given arbitrary
/// `url`, merges the received engine list into the local database, and returns
/// the counts of engines sent and received.
///
/// Uses the split-lock pattern so that no `MutexGuard` is held across an
/// `.await` point (required for the `Send` bound on axum handlers):
///
/// 1. Lock → snapshot local list → drop guard.
/// 2. Perform async HTTP exchange (no lock held).
/// 3. Lock → merge + persist → drop guard.
///
/// ## Status codes
/// - 200 OK            — exchange succeeded; body is `TriggerGossipResp`
/// - 400 Bad Request   — peer returned a non-2xx status, an incompatible
///   protocol version, or any other exchange error
/// - 401 Unauthorized  — missing or invalid `Authorization: Bearer` header
///   (enforced by the `route_layer` in [`crate::api::admin::admin_router`])
/// - 500 Internal      — database error
pub async fn trigger_gossip(
    State(state): State<AppState>,
    Json(req): Json<TriggerGossipReq>,
) -> Result<Json<TriggerGossipResp>, (StatusCode, String)> {
    // ── 1. Snapshot local list while holding the lock (no await inside) ───
    let local_list = {
        let conn = state.db.lock().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "db mutex poisoned".to_string(),
            )
        })?;
        discovered::list(&conn).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        // MutexGuard dropped here — safe to .await after this block
    };

    // ── 2. Build outbound payload (no lock held) ───────────────────────────
    let our_payload = ExchangeRequest {
        protocol_version: PROTOCOL_VERSION.to_string(),
        engines: local_list
            .iter()
            .map(|e| EngineSummary {
                url: e.url.clone(),
                name: e.name.clone(),
                description: e.description.clone(),
            })
            .collect(),
    };
    let sent_count = our_payload.engines.len();

    // ── 3. Async HTTP exchange (no lock held) ──────────────────────────────
    let exchange_url = format!("{}/api/gossip/exchange", req.url.trim_end_matches('/'));
    let http_resp = state
        .http_client
        .post(&exchange_url)
        .json(&our_payload)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    if !http_resp.status().is_success() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("peer returned status {}", http_resp.status().as_u16()),
        ));
    }

    let body: ExchangeResponse = http_resp
        .json()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    // ── 4. Protocol version check (no lock held) ───────────────────────────
    match check_compatibility(&body.protocol_version) {
        Compatibility::Incompatible => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "incompatible protocol version: peer={} ours={}",
                    body.protocol_version, PROTOCOL_VERSION
                ),
            ));
        }
        Compatibility::MinorMismatch => {
            tracing::warn!(
                peer = %req.url,
                their = %body.protocol_version,
                "minor protocol version mismatch in manual gossip trigger"
            );
        }
        Compatibility::Compatible => {}
    }

    // ── 5. Merge engine lists (no lock held) ──────────────────────────────
    let now = chrono::Utc::now().timestamp();
    let incoming: Vec<DiscoveredEngine> = body
        .engines
        .iter()
        .map(|s| DiscoveredEngine {
            url: s.url.clone(),
            name: s.name.clone(),
            description: s.description.clone(),
            first_seen: now,
            last_seen: now,
        })
        .collect();
    let received_count = incoming.len();
    let merged = merge_engine_lists(local_list, incoming, now);

    // ── 6. Persist merged list (re-acquire lock, no await inside) ─────────
    {
        let conn = state.db.lock().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "db mutex poisoned".to_string(),
            )
        })?;
        for e in &merged {
            discovered::upsert(&conn, e)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
        // MutexGuard dropped here
    }

    Ok(Json(TriggerGossipResp {
        status: "ok",
        sent_count,
        received_count,
    }))
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Register the gossip trigger route.
///
/// Auth is enforced by the `route_layer` in the parent [`admin_router`].
///
/// [`admin_router`]: crate::api::admin::admin_router
pub fn routes() -> Router<AppState> {
    Router::new().route("/api/admin/gossip/trigger", post(trigger_gossip))
}
