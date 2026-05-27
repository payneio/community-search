//! POST /api/admin/crawl — pause or resume the crawl scheduler.
//!
//! Pause is "no new target tasks dispatched." In-flight per-target BFS
//! runs continue until they hit their natural stopping point. Combined
//! with the `indexing_pending_count` field in `/api/admin/status`, this
//! lets an admin reach a known-quiet state before running an export.
//!
//! The flag is transient — not persisted across restarts. Restarting the
//! process re-enables the scheduler.

use std::sync::atomic::Ordering;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::api::public::AppState;

#[derive(Deserialize)]
struct PauseRequest {
    paused: bool,
}

#[derive(Serialize)]
struct PauseResponse {
    paused: bool,
}

async fn handle_pause(
    State(state): State<AppState>,
    Json(req): Json<PauseRequest>,
) -> Result<Response, (StatusCode, String)> {
    state.crawl_paused.store(req.paused, Ordering::Relaxed);
    Ok(Json(PauseResponse { paused: req.paused }).into_response())
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/api/admin/crawl", post(handle_pause))
}
