use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::api::public::AppState;
use crate::db::crawl_targets;
use crate::db::outlink_hosts;

// ── Request / response types ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct ListParams {
    collection_id: i64,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Deserialize)]
struct PromoteBody {
    recrawl_interval_secs: i64,
    /// Override the crawl-target URL prefix. When omitted, defaults to
    /// `https://<host>/` — a reasonable starting point that the admin can
    /// edit afterwards from the crawl-targets tab.
    url_prefix: Option<String>,
}

#[derive(Serialize)]
struct ListResponse {
    outlinks: Vec<outlink_hosts::OutlinkHostRow>,
}

#[derive(Serialize)]
struct PromoteResponse {
    crawl_target_id: i64,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// GET /api/admin/outlinks?collection_id=N[&limit=N][&offset=N]
///
/// Lists pending host-level outlink suggestions for the given collection,
/// sorted by `link_count DESC` so the highest-signal review candidates
/// surface first. Default limit 50, max 500.
async fn list_outlinks(
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Response, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let rows =
        outlink_hosts::list_pending(&conn, params.collection_id, params.limit, params.offset)
            .map_err(|e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(ListResponse { outlinks: rows }).into_response())
}

/// POST /api/admin/outlinks/:id/promote
///
/// Body: `{"recrawl_interval_secs": <i64>, "url_prefix": <str?>}`
///
/// Creates a crawl target for the host's collection. The URL prefix defaults
/// to `https://<host>/` if not provided. Marks the row as promoted and
/// returns the new crawl target's id.
async fn promote_outlink(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<PromoteBody>,
) -> Result<Response, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let row = outlink_hosts::find(&conn, id)
        .map_err(|e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("outlink {id} not found")))?;

    let collection_rowid: i64 = conn
        .query_row(
            "SELECT rowid FROM collections WHERE id = ?1",
            rusqlite::params![&row.collection_id],
            |r| r.get(0),
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let url_prefix = body
        .url_prefix
        .unwrap_or_else(|| format!("https://{}/", row.host));

    let ct = crawl_targets::add(
        &conn,
        collection_rowid,
        &url_prefix,
        body.recrawl_interval_secs,
    )
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    outlink_hosts::mark_promoted(&conn, id, ct.id)
        .map_err(|e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(PromoteResponse {
        crawl_target_id: ct.id,
    })
    .into_response())
}

/// POST /api/admin/outlinks/:id/dismiss
///
/// Marks the host as dismissed. The crawler treats dismissed rows as a
/// per-collection blacklist — future outlinks to the same host are silently
/// ignored without ever growing the queue.
async fn dismiss_outlink(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Response, (StatusCode, String)> {
    let conn = state.db.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "db mutex poisoned".into(),
        )
    })?;

    let updated = outlink_hosts::mark_dismissed(&conn, id)
        .map_err(|e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if updated {
        Ok(StatusCode::OK.into_response())
    } else {
        Err((StatusCode::NOT_FOUND, format!("outlink {id} not found")))
    }
}

// ── Router ────────────────────────────────────────────────────────────────────

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/admin/outlinks", get(list_outlinks))
        .route("/api/admin/outlinks/:id/promote", post(promote_outlink))
        .route("/api/admin/outlinks/:id/dismiss", post(dismiss_outlink))
}
