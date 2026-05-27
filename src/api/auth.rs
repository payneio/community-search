//! Bearer-token authentication middleware for admin-protected routes.
//!
//! [`require_admin_token`] is an axum middleware that extracts and validates an
//! `Authorization: Bearer <token>` header on every request.  It uses
//! constant-time comparison to prevent timing-based token enumeration and does
//! **not** exempt localhost — every client must present a valid token.
//!
//! ## Lockout policy
//!
//! After [`crate::api::auth_attempts::MAX_FAILURES`] consecutive failed attempts
//! from the same IP address, further requests are rejected with `429 Too Many
//! Requests` for [`crate::api::auth_attempts::LOCKOUT_SECONDS`] seconds.  The
//! lockout is checked **before** token validation so that even a correct token
//! cannot bypass a lockout.

use std::net::SocketAddr;

use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{header, HeaderValue, Request, Response, StatusCode},
    middleware::Next,
    response::IntoResponse,
};

use crate::api::auth_attempts;
use crate::api::public::AppState;

// ---------------------------------------------------------------------------
// Public helpers (also used by unit tests)
// ---------------------------------------------------------------------------

/// Extract the bearer token from an `Authorization` header value.
///
/// Accepts `Bearer` (case-insensitive) as the scheme.  Returns `None` when:
/// - The scheme is not `bearer`
/// - There is no space-separated token value after the scheme
/// - The token value is empty or all-whitespace
pub fn extract_bearer(authorization: &str) -> Option<&str> {
    let mut parts = authorization.splitn(2, ' ');
    let scheme = parts.next()?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = parts.next()?.trim();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

/// Constant-time byte-slice equality.
///
/// XORs every byte pair and ORs the results so there is no early exit on a
/// mismatch.  Slices of different lengths are always unequal (returns `false`
/// immediately — length is not secret).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ---------------------------------------------------------------------------
// Public helpers (re-exported for use by rate_limit middleware)
// ---------------------------------------------------------------------------

/// Public wrapper around the private [`client_ip`] helper.
///
/// Exposed so that [`crate::api::rate_limit::require_rate_limit`] can reuse
/// the same IP-extraction logic without duplicating it.
pub fn client_ip_pub(
    req: &axum::http::Request<axum::body::Body>,
    connect_info: Option<&ConnectInfo<SocketAddr>>,
) -> String {
    client_ip(req, connect_info)
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Current Unix timestamp in seconds.
fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Determine the client IP address for rate-limiting purposes.
///
/// Checks (in order):
/// 1. The first comma-separated value in the `X-Forwarded-For` header (trimmed).
/// 2. The `ConnectInfo` extension injected by axum when the server is bound
///    with `.into_make_service_with_connect_info::<SocketAddr>()`.
/// 3. Falls back to `"unknown"` when neither source is available (e.g. in
///    unit tests using `tower::ServiceExt::oneshot`).
fn client_ip(req: &Request<Body>, connect_info: Option<&ConnectInfo<SocketAddr>>) -> String {
    // X-Forwarded-For takes priority (proxy-injected).
    if let Some(v) = req.headers().get("x-forwarded-for") {
        if let Ok(s) = v.to_str() {
            let first = s.split(',').next().unwrap_or("").trim();
            if !first.is_empty() {
                return first.to_string();
            }
        }
    }

    // Fall back to the direct TCP peer address.
    if let Some(ci) = connect_info {
        return ci.0.ip().to_string();
    }

    "unknown".to_string()
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

/// Axum middleware: require a valid `Authorization: Bearer <token>` header.
///
/// ### Lockout check (before token validation)
///
/// If the requesting IP has accumulated [`auth_attempts::MAX_FAILURES`]
/// consecutive failures, the middleware returns:
///
/// ```text
/// HTTP/1.1 429 Too Many Requests
/// Retry-After: <seconds remaining>
///
/// too many failed attempts
/// ```
///
/// ### Token validation
///
/// On success the request is forwarded to the next handler.  On any failure
/// (missing header, wrong scheme, empty value, mismatched token) the
/// middleware returns:
///
/// ```text
/// HTTP/1.1 401 Unauthorized
/// WWW-Authenticate: Bearer realm="admin"
///
/// unauthorized
/// ```
///
/// Localhost is **not** exempt — the token is required from all clients
/// regardless of IP address or `X-Forwarded-For` header.
pub async fn require_admin_token(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response<Body> {
    let now = unix_now();
    let connect_info = req.extensions().get::<ConnectInfo<SocketAddr>>();
    let ip = client_ip(&req, connect_info);

    // -----------------------------------------------------------------------
    // Step 1: Check lockout BEFORE token validation.
    // -----------------------------------------------------------------------
    let attempt_state =
        auth_attempts::current_state(&state.db, &ip, now).unwrap_or(auth_attempts::AttemptState {
            failed_count: 0,
            lockout_until: 0,
        });

    if attempt_state.lockout_until > now {
        let seconds_remaining = attempt_state.lockout_until - now;
        let mut response =
            (StatusCode::TOO_MANY_REQUESTS, "too many failed attempts").into_response();
        // SAFETY: `seconds_remaining` is a non-negative integer ≤ LOCKOUT_SECONDS;
        // its decimal representation contains only ASCII digits, so `from_str`
        // always succeeds.
        if let Ok(hv) = HeaderValue::from_str(&seconds_remaining.to_string()) {
            response.headers_mut().insert("retry-after", hv);
        }
        return response;
    }

    // -----------------------------------------------------------------------
    // Step 2: Validate the bearer token.
    // -----------------------------------------------------------------------
    let authorized = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(extract_bearer)
        .map(|presented| constant_time_eq(presented.as_bytes(), state.admin_token.as_bytes()))
        .unwrap_or(false);

    if authorized {
        // -----------------------------------------------------------------------
        // Step 3a: Success — clear any prior failed attempts for this IP.
        // -----------------------------------------------------------------------
        let _ = auth_attempts::record_success(&state.db, &ip);
        next.run(req).await
    } else {
        // -----------------------------------------------------------------------
        // Step 3b: Failure — record the attempt and return 401.
        // -----------------------------------------------------------------------
        let _ = auth_attempts::record_failure(&state.db, &ip, now);
        let mut response = (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"admin\""),
        );
        response
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- extract_bearer ------------------------------------------------------

    #[test]
    fn extract_bearer_happy_path() {
        assert_eq!(
            extract_bearer("Bearer mytoken123"),
            Some("mytoken123"),
            "standard Bearer scheme must succeed"
        );
        assert_eq!(
            extract_bearer("bearer mytoken123"),
            Some("mytoken123"),
            "lowercase 'bearer' must succeed (case-insensitive)"
        );
        assert_eq!(
            extract_bearer("BEARER mytoken123"),
            Some("mytoken123"),
            "uppercase 'BEARER' must succeed (case-insensitive)"
        );
    }

    #[test]
    fn extract_bearer_rejects_basic() {
        assert_eq!(
            extract_bearer("Basic dXNlcjpwYXNz"),
            None,
            "Basic scheme must be rejected"
        );
    }

    #[test]
    fn extract_bearer_rejects_empty_value() {
        assert_eq!(
            extract_bearer("Bearer "),
            None,
            "Bearer with only whitespace must be rejected"
        );
        assert_eq!(
            extract_bearer("Bearer"),
            None,
            "Bearer with no value at all must be rejected"
        );
    }

    #[test]
    fn extract_bearer_rejects_no_scheme() {
        assert_eq!(
            extract_bearer("mytoken"),
            None,
            "a bare token with no scheme must be rejected"
        );
    }

    // -- constant_time_eq ----------------------------------------------------

    #[test]
    fn constant_time_eq_equal() {
        assert!(
            constant_time_eq(b"supersecret", b"supersecret"),
            "identical slices must be equal"
        );
        assert!(constant_time_eq(b"", b""), "two empty slices must be equal");
    }

    #[test]
    fn constant_time_eq_differ() {
        assert!(
            !constant_time_eq(b"supersecret", b"wrongsecret"),
            "slices with different content must not be equal"
        );
        assert!(
            !constant_time_eq(b"abc", b"xyz"),
            "completely different slices must not be equal"
        );
    }

    #[test]
    fn constant_time_eq_different_length() {
        assert!(
            !constant_time_eq(b"short", b"longer_string"),
            "slices of different lengths must not be equal"
        );
        assert!(
            !constant_time_eq(b"abc", b"ab"),
            "slices where one is a prefix of the other must not be equal"
        );
    }
}
