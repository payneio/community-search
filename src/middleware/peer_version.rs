//! Middleware that injects the `X-CommunitySearch-Version` header on
//! peer-facing route responses.
//!
//! This is applied **only** to the peer routes sub-router (i.e. the routes
//! exposed to other search-engine nodes), so the root UI (`/`) intentionally
//! does not carry this header.

use axum::{
    body::Body,
    http::{HeaderName, HeaderValue, Request, Response},
    middleware::Next,
};

use crate::protocol::{PROTOCOL_VERSION, VERSION_HEADER};

/// Header name in lowercase (HTTP header names are case-insensitive but the
/// `http` crate requires them to be stored in lowercase form).
///
/// This mirrors [`VERSION_HEADER`] (`"X-CommunitySearch-Version"`) in lowercase.
const VERSION_HEADER_LC: &str = "x-communitysearch-version";

/// Axum middleware function that appends the `X-CommunitySearch-Version`
/// response header to every response it sees.
///
/// Mount this with [`axum::middleware::from_fn`] on the peer-routes
/// sub-router so that only `/api/collections`, `/api/search`, and
/// `/api/gossip/exchange` carry the header.
pub async fn add_peer_version_header(req: Request<Body>, next: Next) -> Response<Body> {
    let _ = VERSION_HEADER; // referenced for documentation / lint purposes

    let mut resp = next.run(req).await;
    resp.headers_mut().insert(
        HeaderName::from_static(VERSION_HEADER_LC),
        HeaderValue::from_static(PROTOCOL_VERSION),
    );
    resp
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{middleware, routing::get, Router};
    use tower::ServiceExt;

    #[tokio::test]
    async fn header_is_added_to_response() {
        let app = Router::new()
            .route("/ping", get(|| async { "pong" }))
            .layer(middleware::from_fn(add_peer_version_header));

        let resp = app
            .oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(
            resp.headers()
                .get(VERSION_HEADER_LC)
                .expect("header must be present"),
            "1.0",
        );
    }
}
