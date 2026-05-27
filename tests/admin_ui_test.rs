//! UI markup tests for the admin page.
//!
//! Verifies that `GET /admin` returns HTML containing required sections
//! and elements. These are markup-only assertions; no admin token is needed
//! since the page itself is public (all API calls it makes carry the token).
// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Spawn a real TCP server backed by the full application router.
///
/// Wraps `community_search::test_support::spawn_test_server` so the test
/// function can use the local name `spawn_test_server_with_admin`.
async fn spawn_test_server_with_admin() -> community_search::test_support::TestServer {
    community_search::test_support::spawn_test_server().await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// GET /admin must return 200 with the SPA shell intact.
///
/// The admin UI is now a hash-routed single-page app, so dynamic sections
/// (`#node-peers`, `#discovered-engines`, `#collection-peers`, ...) are
/// rendered into `#view-root` from JavaScript and aren't present in the
/// initial HTML payload. We assert on the shell and on labels/identifiers
/// that the JS emits so the markup-level contract stays meaningful.
#[tokio::test]
async fn admin_ui_serves_spa_shell_and_routes() {
    let app = spawn_test_server_with_admin().await;
    let resp = app
        .client
        .get(format!("{}/admin", app.base_url))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let html = resp.text().await.unwrap();

    // Shell + mount point
    assert!(html.contains("Community Search Admin"));
    assert!(html.contains("id=\"view-root\""));
    assert!(html.contains("id=\"toast-host\""));

    // Top-nav routes
    assert!(html.contains("href=\"#/dashboard\""));
    assert!(html.contains("href=\"#/collections\""));
    assert!(html.contains("href=\"#/federation\""));
    assert!(html.contains("href=\"#/settings\""));

    // Federation view renders these card IDs and the discovered-engines table.
    // The card IDs are emitted from inlined JS (`id: 'node-peers'`), so we
    // match on the JS-literal form. `discovered-engines-table` appears in
    // both forms (the sentinel empty <table> uses an HTML attribute).
    assert!(html.contains("'node-peers'"));
    assert!(html.contains("'collection-peers'"));
    assert!(html.contains("'discovered-engines'"));
    assert!(html.contains("discovered-engines-table"));
    assert!(html.contains("'data-action': 'promote'"));
}
