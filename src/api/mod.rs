pub mod admin;
pub mod auth;
pub mod auth_attempts;
pub mod gossip;
pub mod mcp;
pub mod public;
pub mod rate_limit;
pub mod router;
pub mod sse;
pub mod ui;

// Re-export the router builder at the `api` level so callers can use
// `crate::api::build_router` (or `community_search::api::build_router` from
// integration tests) without knowing the internal `router` sub-module.
pub use router::build_router;

// ---------------------------------------------------------------------------
// Admin web UI
// ---------------------------------------------------------------------------

/// The single-file admin HTML page, embedded at compile time.
///
/// Served at `GET /admin` without authentication — the page itself stores the
/// admin token in `localStorage` and injects an `Authorization: Bearer` header
/// on every `/api/admin/*` request it makes.
const ADMIN_HTML: &str = include_str!("../ui/static/admin.html");

/// Handler for `GET /admin`.
///
/// Returns the embedded admin HTML page.  This route is intentionally **not**
/// behind the admin-auth middleware — the page is static and public.  All API
/// calls originating from the page target `/api/admin/*`, which are protected.
pub async fn admin_page() -> axum::response::Html<&'static str> {
    axum::response::Html(ADMIN_HTML)
}
