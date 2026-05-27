//! Admin API router.
//!
//! All routes under `/api/admin/` require a valid `Authorization: Bearer`
//! token via the [`crate::api::auth::require_admin_token`] middleware.

pub mod collection_peers;
pub mod collections;
pub mod config;
pub mod crawl_targets;
pub mod discovered;
pub mod gossip;
pub mod nodes;
pub mod outlinks;
pub mod ranking;
pub mod status;

use axum::{middleware, routing::get, Router};

use crate::api::public::AppState;

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// GET /api/admin/ping — health probe for authenticated callers.
async fn ping() -> &'static str {
    "pong"
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the admin sub-router.
///
/// Registers:
/// - `GET /api/admin/ping`   → [`ping`]
/// - `GET /api/admin/status` → [`status::get_status`]
///
/// All routes in this router are protected by
/// [`crate::api::auth::require_admin_token`] via `route_layer`.
pub fn admin_router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/api/admin/ping", get(ping))
        .merge(collection_peers::routes())
        .merge(collections::routes())
        .merge(config::routes())
        .merge(crawl_targets::routes())
        .merge(discovered::routes())
        .merge(gossip::routes())
        .merge(nodes::routes())
        .merge(outlinks::routes())
        .merge(ranking::routes())
        .merge(status::routes())
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            crate::api::auth::require_admin_token,
        ))
}
