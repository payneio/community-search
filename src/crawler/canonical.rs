//! Canonical-URL detection for newly added crawl targets.
//!
//! When an admin types `https://www.lesswrong.com/` and the site actually
//! redirects to `https://lesswrong.com/`, we'd rather store the bare-host
//! form so all subsequent crawls start at the canonical URL — no redirect
//! hop, no double-storing the same site as both `www.X` and `X`.
//!
//! This is intentionally narrow: we only accept the server's redirect
//! target when the **path is unchanged**. A path-changing redirect (locale
//! routing, login walls, default-doc) is not canonicalization — accepting
//! it would silently narrow the prefix, which is worse than storing what
//! the admin typed.

use std::time::Duration;

use reqwest::redirect;
use url::Url;

use crate::crawler::url_class::normalize_url;

/// Tight timeout — this runs synchronously inside the admin "Add" handler,
/// so a slow upstream blocks the form submission. Three seconds is enough
/// for a healthy site and short enough that a stalled one falls back to
/// the admin's input without an obvious hang.
const DETECTION_TIMEOUT: Duration = Duration::from_secs(3);

/// Try to detect the canonical form of `input` by following HTTP redirects.
///
/// Pass the same User-Agent the crawler will later use (typically
/// `state.crawler_user_agent`). Sites that 403/429 reqwest's default UA
/// would otherwise look unreachable here even though crawling them works.
///
/// Returns `Some(canonical)` only when:
/// - The request reached a final response and that response was 2xx.
/// - A redirect actually happened (final URL differs from the input).
/// - The final URL's **path equals** the input's path, modulo trailing
///   slash. Path changes other than trailing-slash are treated as routing,
///   not canonicalization.
///
/// Returns `None` on parse failure, network error, timeout, non-2xx final
/// status, no-redirect, or path-changing redirect. Callers should fall
/// back to the admin's input in all these cases.
pub async fn detect_canonical_prefix(input: &str, user_agent: &str) -> Option<String> {
    let input_url = Url::parse(input).ok()?;
    let input_path = input_url.path().to_string();

    let mut builder = reqwest::Client::builder()
        .redirect(redirect::Policy::limited(5))
        .timeout(DETECTION_TIMEOUT);
    if !user_agent.is_empty() {
        builder = builder.user_agent(user_agent);
    }
    let client = builder.build().ok()?;

    // GET, not HEAD — many sites mis-handle HEAD on root URLs (CDN edge
    // configs, app routers) and return 405 or even close the connection.
    // We only need the headers + final URL, so the body cost is small.
    let resp = client.get(input).send().await.ok()?;
    let final_url = resp.url().clone();
    let status = resp.status();
    drop(resp);

    if !status.is_success() {
        return None;
    }
    if final_url.as_str() == input_url.as_str() {
        return None;
    }
    if !paths_equivalent(&input_path, final_url.path()) {
        return None;
    }

    // Run the detected URL through the crawler's own normalization so the
    // stored prefix matches the form the crawler will later compare against.
    normalize_url(final_url.as_str())
}

/// Compare paths with a trailing-slash tolerance.
///
/// Most servers redirect `/section` → `/section/` (or vice versa) as a
/// canonicalization step — that's the same resource, not a routing change.
/// `/foo` and `/foo/` count as equivalent; `/foo` and `/foo/bar` do not.
fn paths_equivalent(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    let a_stripped = a.strip_suffix('/').unwrap_or(a);
    let b_stripped = b.strip_suffix('/').unwrap_or(b);
    a_stripped == b_stripped
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Helper: build a "https-canonical" URL that the test server would
    /// normally redirect *to*. wiremock listens on http://127.0.0.1:<port>,
    /// so the "canonical" we use in tests is just a different path on the
    /// same mock server — enough to exercise the redirect logic.
    async fn server() -> MockServer {
        MockServer::start().await
    }

    #[tokio::test]
    async fn no_redirect_returns_none() {
        let srv = server().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&srv)
            .await;

        let result = detect_canonical_prefix(&format!("{}/", srv.uri()), "").await;
        assert_eq!(result, None, "no redirect → None");
    }

    #[tokio::test]
    async fn host_only_redirect_with_same_path_is_accepted() {
        // Stand up TWO mock servers. The first 301s to the root of the second.
        // Both paths are `/`, so the path-equality check passes and we
        // accept the second server's URL as canonical.
        let bare = server().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&bare)
            .await;

        let www = server().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(301).insert_header("location", format!("{}/", bare.uri())),
            )
            .mount(&www)
            .await;

        let result = detect_canonical_prefix(&format!("{}/", www.uri()), "").await;
        assert_eq!(
            result,
            normalize_url(&format!("{}/", bare.uri())),
            "host-only redirect with unchanged path must be accepted"
        );
    }

    #[tokio::test]
    async fn path_changing_redirect_is_rejected() {
        // Simulates a locale redirect: / → /en/. We must NOT accept this,
        // because using `/en/` as the prefix would exclude all other paths.
        let srv = server().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("location", format!("{}/en/", srv.uri())),
            )
            .mount(&srv)
            .await;
        Mock::given(method("GET"))
            .and(path("/en/"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&srv)
            .await;

        let result = detect_canonical_prefix(&format!("{}/", srv.uri()), "").await;
        assert_eq!(
            result, None,
            "path-changing redirect must be ignored (would narrow prefix)"
        );
    }

    #[tokio::test]
    async fn non_2xx_final_status_returns_none() {
        // Redirect to a 404. Even though the canonical URL would have an
        // unchanged path, the destination isn't actually reachable —
        // don't pin the admin to a broken URL.
        let srv = server().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(301).insert_header("location", format!("{}/dead", srv.uri())),
            )
            .mount(&srv)
            .await;
        Mock::given(method("GET"))
            .and(path("/dead"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&srv)
            .await;

        let result = detect_canonical_prefix(&format!("{}/", srv.uri()), "").await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn unparseable_input_returns_none() {
        assert_eq!(detect_canonical_prefix("not a url", "").await, None);
    }

    #[tokio::test]
    async fn trailing_slash_redirect_is_accepted() {
        // `/section` → `/section/` is a common server-side canonicalization;
        // treating those paths as different would silently miss the rewrite.
        let srv = server().await;
        Mock::given(method("GET"))
            .and(path("/section"))
            .respond_with(
                ResponseTemplate::new(301)
                    .insert_header("location", format!("{}/section/", srv.uri())),
            )
            .mount(&srv)
            .await;
        Mock::given(method("GET"))
            .and(path("/section/"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&srv)
            .await;

        let result = detect_canonical_prefix(&format!("{}/section", srv.uri()), "").await;
        assert_eq!(
            result,
            normalize_url(&format!("{}/section/", srv.uri())),
            "trailing-slash addition must be treated as canonical"
        );
    }

    #[tokio::test]
    async fn unreachable_host_returns_none() {
        // Port 1 is essentially always closed; the connect attempt will
        // fail quickly. Confirms the network-error branch falls back cleanly.
        let result = detect_canonical_prefix("http://127.0.0.1:1/", "").await;
        assert_eq!(result, None);
    }
}
