use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

use axum::{
    body::Body,
    http::{Request, Response, StatusCode},
    middleware,
    routing::get,
    Router,
};
use rusqlite::Connection;
use tantivy::Index;
use tower::ServiceExt;

use community_search::api::auth::require_admin_token;
use community_search::api::public::{AppState, SharedDb};
use community_search::federation::peer::HttpPeerClient;
use community_search::index::{reader::Searcher, schema};
use community_search::search::service::SearchService;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn build_test_state(token: &str) -> AppState {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    community_search::db::run_migrations(&conn).expect("run migrations");
    let db: SharedDb = Arc::new(Mutex::new(conn));
    let index = Index::create_in_ram(schema::build());
    let searcher = Searcher::open(index).expect("open in-ram searcher");
    let service = SearchService::new(Arc::new(searcher), Arc::clone(&db));
    let default_rl = community_search::api::rate_limit::RateLimitConfig::default();
    AppState {
        admin_token: token.to_string(),
        self_url: String::new(),
        db,
        search: Arc::new(service),
        rate_limit_config: Arc::new(RwLock::new(default_rl.clone())),
        peer_rate_limit_config: Arc::new(RwLock::new(
            community_search::api::rate_limit::RateLimitConfig {
                limit: 120,
                ..default_rl
            },
        )),
        peer_ip_cache: Arc::new(RwLock::new(
            community_search::api::rate_limit::PeerIpCache::new(),
        )),
        runtime_config: Arc::new(RwLock::new(community_search::RuntimeConfig::default())),
        index_path: std::path::PathBuf::from("/tmp/community-search-test-index-nonexistent"),
        max_index_bytes: 10_737_418_240,
        peer_client: Arc::new(
            HttpPeerClient::new(Duration::from_secs(10)).expect("create HttpPeerClient"),
        ),
        http_client: reqwest::Client::builder()
            .user_agent("community-search-test/0.1")
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build reqwest client"),
        crawler_user_agent: "community-search-test/0.1".into(),
        indexer_delete_tx: community_search::test_support::sink_indexer_delete_tx(),
        indexer_upsert_tx: community_search::test_support::sink_indexer_upsert_tx(),
        crawl_paused: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        indexing_inflight: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
    }
}

fn protected_app(state: AppState) -> Router {
    Router::new()
        .route("/protected", get(|| async { "ok" }))
        .layer(middleware::from_fn_with_state(state, require_admin_token))
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

/// A request with no Authorization header must return 401.
#[tokio::test]
async fn missing_authorization_header_returns_401() {
    let app = protected_app(build_test_state("secret-token"));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/protected")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "missing Authorization header must return 401"
    );

    // WWW-Authenticate header must be present
    let www_auth = resp
        .headers()
        .get("www-authenticate")
        .expect("WWW-Authenticate header must be present")
        .to_str()
        .unwrap();
    assert!(
        www_auth.contains("Bearer"),
        "WWW-Authenticate must contain Bearer, got: {www_auth}"
    );
    assert!(
        www_auth.contains("realm=\"admin\""),
        "WWW-Authenticate must contain realm=\"admin\", got: {www_auth}"
    );

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        body.as_ref(),
        b"unauthorized",
        "body must be 'unauthorized'"
    );
}

/// A request with an incorrect token must return 401.
#[tokio::test]
async fn wrong_token_returns_401() {
    let app = protected_app(build_test_state("secret-token"));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/protected")
                .header("Authorization", "Bearer wrong-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "wrong token must return 401"
    );

    let www_auth = resp
        .headers()
        .get("www-authenticate")
        .expect("WWW-Authenticate header must be present")
        .to_str()
        .unwrap();
    assert!(
        www_auth.contains("Bearer"),
        "WWW-Authenticate must contain Bearer"
    );
}

/// A request with the correct token must pass through (200 from the handler).
#[tokio::test]
async fn correct_token_returns_200() {
    let app = protected_app(build_test_state("secret-token"));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/protected")
                .header("Authorization", "Bearer secret-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "correct token must return 200"
    );
}

/// Even a request that appears to come from localhost (via X-Forwarded-For)
/// must require a valid token — no localhost exemption.
#[tokio::test]
async fn localhost_still_requires_token() {
    let app = protected_app(build_test_state("secret-token"));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/protected")
                .header("x-forwarded-for", "127.0.0.1")
                // No Authorization header
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "localhost (via x-forwarded-for: 127.0.0.1) must still return 401 without a token"
    );
}

// ---------------------------------------------------------------------------
// Lockout integration tests
// ---------------------------------------------------------------------------

/// Helper: make a single request to the protected endpoint with an explicit
/// `X-Forwarded-For` IP and an optional bearer token.
///
/// Creates a fresh `Router` each call (consuming it via `oneshot`) but clones
/// `AppState`, so all calls in the same test share the same in-memory DB and
/// the attempt counter accumulates correctly.
async fn make_auth_request(state: &AppState, ip: &str, token: Option<&str>) -> Response<Body> {
    let app = protected_app(state.clone());
    let mut builder = Request::builder()
        .uri("/protected")
        .header("x-forwarded-for", ip);
    if let Some(t) = token {
        builder = builder.header("Authorization", format!("Bearer {t}"));
    }
    app.oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

/// Five consecutive failed attempts from the same IP must lock that IP out
/// for 15 minutes (900 s).  The sixth request — even with the correct token —
/// must return 429 with a `Retry-After` header in the range (0, 900].
#[tokio::test]
async fn five_failed_attempts_lock_out_ip_for_15_minutes() {
    let state = build_test_state("secret-token");
    let test_ip = "10.0.0.42";

    // Five bad attempts: each must return 401.
    for i in 1..=5 {
        let resp = make_auth_request(&state, test_ip, Some("wrong-token")).await;
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "attempt {i}: expected 401 before lockout"
        );
    }

    // Sixth attempt — correct token — must be locked out (429).
    let resp = make_auth_request(&state, test_ip, Some("secret-token")).await;
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "sixth attempt must return 429 after lockout"
    );

    let retry_after = resp
        .headers()
        .get("retry-after")
        .expect("Retry-After header must be present after lockout")
        .to_str()
        .expect("Retry-After must be a valid string")
        .parse::<i64>()
        .expect("Retry-After must be a numeric string");

    assert!(
        retry_after > 0,
        "Retry-After must be > 0, got {retry_after}"
    );
    assert!(
        retry_after <= 900,
        "Retry-After must be <= 900, got {retry_after}"
    );

    // The body must also convey the right message.
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        body.as_ref(),
        b"too many failed attempts",
        "body must be 'too many failed attempts'"
    );
}

/// A successful authentication must reset the failed-attempt counter so that
/// the next N failures (where N < MAX_FAILURES) are all treated as 401, not 429.
#[tokio::test]
async fn successful_auth_resets_failed_count() {
    let state = build_test_state("secret-token");
    let test_ip = "10.0.0.99";

    // Four bad attempts — not enough to lock out yet.
    for i in 1..=4 {
        let resp = make_auth_request(&state, test_ip, Some("wrong")).await;
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "attempt {i}: expected 401 before good auth"
        );
    }

    // One good attempt: must succeed and reset the counter.
    let resp = make_auth_request(&state, test_ip, Some("secret-token")).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "correct token must return 200 and reset attempt counter"
    );

    // Four more bad attempts after the reset: all 401, NOT 429.
    for i in 1..=4 {
        let resp = make_auth_request(&state, test_ip, Some("wrong")).await;
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "post-reset attempt {i}: expected 401, not 429"
        );
    }
}

// ---------------------------------------------------------------------------
// Admin router – token-gating
// ---------------------------------------------------------------------------

/// GET /admin without a token must return 200 and serve the admin HTML UI.
///
/// The /admin route is intentionally public — the page itself is static HTML
/// that stores the token in localStorage.  All /api/admin/* calls made from
/// the page still require a valid Bearer token.
#[tokio::test]
async fn admin_page_is_served_at_slash_admin() {
    let app = community_search::test_support::test_router_full("the-token");
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/admin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "GET /admin must return 200 without a token"
    );

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = std::str::from_utf8(&body).expect("body must be valid UTF-8");

    assert!(
        body_str.contains("<title>Community Search Admin</title>"),
        "body must contain '<title>Community Search Admin</title>'"
    );
    assert!(
        body_str.contains("Authorization"),
        "body must contain 'Authorization' (injected into API calls via the api() helper)"
    );
}

/// GET /api/admin/ping must return 401 without a token and 200 with the
/// correct Bearer token.
#[tokio::test]
async fn admin_ping_route_requires_token() {
    // No token → 401
    let app = community_search::test_support::test_router_full("the-token");
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/admin/ping")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "missing token must return 401"
    );

    // Correct token → 200
    let app = community_search::test_support::test_router_full("the-token");
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/admin/ping")
                .header("Authorization", "Bearer the-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "correct Bearer token must return 200"
    );
}
