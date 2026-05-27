use std::time::Duration;

use reqwest::header::{
    HeaderMap, HeaderValue, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED,
    RETRY_AFTER, USER_AGENT,
};
use reqwest::redirect;

use super::{CrawlError, CrawlResult};

/// Input to a single HTTP fetch operation, including optional cache validators.
#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

/// The outcome of a `Fetcher::fetch` call.
#[derive(Debug, Clone)]
pub enum FetchOutcome {
    /// Server returned 2xx with an HTML body.
    Fresh {
        status: u16,
        body: String,
        etag: Option<String>,
        last_modified: Option<String>,
        /// URL after following HTTP redirects. Equals the requested URL when
        /// no redirect occurred. Callers can compare against the request URL
        /// to dedupe content reachable from multiple URLs.
        final_url: String,
    },
    /// Server returned 304 Not Modified.
    NotModified,
    /// Server returned 429 Too Many Requests.
    ///
    /// `retry_after` is set when the server included a `Retry-After` header
    /// with an integer (seconds) value.  HTTP-date values are not supported
    /// and result in `None`.
    RateLimited { retry_after: Option<Duration> },
    /// Server returned a non-2xx, non-304, non-429 status.
    OtherStatus { status: u16 },
}

/// Parse a `Retry-After` header value (integer seconds form only).
///
/// Returns `None` for HTTP-date values or any value that cannot be parsed
/// as a non-negative integer.
pub(crate) fn parse_retry_after(value: &str) -> Option<Duration> {
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

/// HTTP client wrapper that adds conditional cache headers to every request.
pub struct Fetcher {
    client: reqwest::Client,
    user_agent: String,
}

impl Fetcher {
    /// Build a `Fetcher` with the given user-agent string and request timeout.
    ///
    /// Redirects are capped at 5 hops.
    pub fn new(user_agent: impl Into<String>, timeout: Duration) -> CrawlResult<Self> {
        let user_agent = user_agent.into();
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(redirect::Policy::limited(5))
            .build()?;
        Ok(Self { client, user_agent })
    }

    /// Perform a GET request, sending conditional cache headers when present.
    ///
    /// - `Fresh { … }` – 2xx with HTML content-type
    /// - `NotModified`  – 304
    /// - `OtherStatus`  – any other status code
    /// - `Err(NotHtml)` – 2xx but content-type doesn't contain "html"
    pub async fn fetch(&self, req: &FetchRequest) -> CrawlResult<FetchOutcome> {
        let mut headers = HeaderMap::new();

        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&self.user_agent)
                .map_err(|e| CrawlError::Other(e.to_string()))?,
        );

        if let Some(etag) = &req.etag {
            headers.insert(
                IF_NONE_MATCH,
                HeaderValue::from_str(etag).map_err(|e| CrawlError::Other(e.to_string()))?,
            );
        }

        if let Some(lm) = &req.last_modified {
            headers.insert(
                IF_MODIFIED_SINCE,
                HeaderValue::from_str(lm).map_err(|e| CrawlError::Other(e.to_string()))?,
            );
        }

        let response = self.client.get(&req.url).headers(headers).send().await?;
        let status = response.status();

        if status == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(FetchOutcome::NotModified);
        }

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_retry_after);
            return Ok(FetchOutcome::RateLimited { retry_after });
        }

        if !status.is_success() {
            return Ok(FetchOutcome::OtherStatus {
                status: status.as_u16(),
            });
        }

        // Check that the response is HTML before consuming the body.
        let ctype = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        if !ctype.to_lowercase().contains("html") {
            return Err(CrawlError::NotHtml(ctype));
        }

        // Extract optional cache-control headers from the response.
        let etag = response
            .headers()
            .get(ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let last_modified = response
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Capture the post-redirect URL before consuming the response body.
        let final_url = response.url().to_string();
        let body = response.text().await?;

        Ok(FetchOutcome::Fresh {
            status: status.as_u16(),
            body,
            etag,
            last_modified,
            final_url,
        })
    }

    /// Plain GET that returns the body as text, with no conditional headers
    /// and no HTML content-type gate.
    ///
    /// Used by the sitemap discovery path, which deals with XML rather than
    /// the conditional-cache HTML flow that [`fetch`] is built for. Returns
    /// `None` on any non-2xx status; returns `Err` on transport failure.
    /// Bodies larger than `max_bytes` are truncated.
    pub async fn fetch_text_raw(&self, url: &str, max_bytes: usize) -> CrawlResult<Option<String>> {
        let response = self
            .client
            .get(url)
            .header(USER_AGENT, &self.user_agent)
            .send()
            .await?;
        if !response.status().is_success() {
            return Ok(None);
        }
        let bytes = response.bytes().await?;
        let slice = if bytes.len() > max_bytes {
            &bytes[..max_bytes]
        } else {
            &bytes[..]
        };
        Ok(Some(String::from_utf8_lossy(slice).into_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // ── helper ────────────────────────────────────────────────────────────────

    fn test_fetcher() -> Fetcher {
        Fetcher::new("test-agent/1.0", Duration::from_secs(10)).unwrap()
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// 200 HTML response → FetchOutcome::Fresh with correct fields.
    #[tokio::test]
    async fn fetch_returns_fresh_on_200_html() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/page"))
            .respond_with(
                // set_body_raw sets both body and the Content-Type mime together;
                // set_body_string would force "text/plain" and override insert_header.
                ResponseTemplate::new(200)
                    .set_body_raw(
                        "<html><body>hello</body></html>",
                        "text/html; charset=utf-8",
                    )
                    .insert_header("etag", "\"abc123\""),
            )
            .mount(&server)
            .await;

        let req = FetchRequest {
            url: format!("{}/page", server.uri()),
            etag: None,
            last_modified: None,
        };

        let outcome = test_fetcher().fetch(&req).await.unwrap();

        match outcome {
            FetchOutcome::Fresh {
                status,
                body,
                etag,
                last_modified,
                final_url,
            } => {
                assert_eq!(status, 200);
                assert!(body.contains("hello"), "body should contain 'hello'");
                assert_eq!(etag, Some("\"abc123\"".to_string()));
                assert!(last_modified.is_none());
                assert_eq!(
                    final_url,
                    format!("{}/page", server.uri()),
                    "final_url should equal the request URL when no redirect occurred"
                );
            }
            other => panic!("Expected Fresh but got {:?}", other),
        }
    }

    /// 301 → 200 chain: final_url reflects the redirect target, not the request URL.
    #[tokio::test]
    async fn fetch_follows_redirect_and_records_final_url() {
        let server = MockServer::start().await;

        let target_path = "/destination";
        Mock::given(method("GET"))
            .and(path("/aliased"))
            .respond_with(
                ResponseTemplate::new(301).insert_header("location", target_path),
            )
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(target_path))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw("<html><body>hi</body></html>", "text/html"),
            )
            .mount(&server)
            .await;

        let req = FetchRequest {
            url: format!("{}/aliased", server.uri()),
            etag: None,
            last_modified: None,
        };

        let outcome = test_fetcher().fetch(&req).await.unwrap();

        match outcome {
            FetchOutcome::Fresh {
                status, final_url, ..
            } => {
                assert_eq!(status, 200);
                assert_eq!(
                    final_url,
                    format!("{}{}", server.uri(), target_path),
                    "final_url should be the redirect target, not the requested URL"
                );
            }
            other => panic!("Expected Fresh but got {:?}", other),
        }
    }

    /// When etag is provided, If-None-Match header is sent; 304 → NotModified.
    ///
    /// The mock only matches when the `if-none-match` header is present with the
    /// correct value, so if we forget to send it the server returns 404 and the
    /// `matches!(outcome, FetchOutcome::NotModified)` assertion fails.
    #[tokio::test]
    async fn fetch_returns_not_modified_on_304() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/page"))
            .and(header("if-none-match", "\"abc123\""))
            .respond_with(ResponseTemplate::new(304))
            .mount(&server)
            .await;

        let req = FetchRequest {
            url: format!("{}/page", server.uri()),
            etag: Some("\"abc123\"".to_string()),
            last_modified: None,
        };

        let outcome = test_fetcher().fetch(&req).await.unwrap();
        assert!(
            matches!(outcome, FetchOutcome::NotModified),
            "Expected NotModified but got {:?}",
            outcome
        );
    }

    /// Non-HTML content-type → Err(NotHtml(…)).
    #[tokio::test]
    async fn fetch_rejects_non_html_content_type() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/file.pdf"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"PDF content")
                    .insert_header("content-type", "application/pdf"),
            )
            .mount(&server)
            .await;

        let req = FetchRequest {
            url: format!("{}/file.pdf", server.uri()),
            etag: None,
            last_modified: None,
        };

        let err = test_fetcher().fetch(&req).await.unwrap_err();
        assert!(
            matches!(err, CrawlError::NotHtml(_)),
            "Expected NotHtml error but got {:?}",
            err
        );
    }

    // ── tests for RateLimited and parse_retry_after ────────────────────────────

    /// parse_retry_after parses an integer value into a Duration.
    #[test]
    fn parse_retry_after_parses_integer_seconds() {
        assert_eq!(parse_retry_after("60"), Some(Duration::from_secs(60)));
        assert_eq!(parse_retry_after("0"), Some(Duration::from_secs(0)));
        assert_eq!(parse_retry_after("  30  "), Some(Duration::from_secs(30)));
    }

    /// parse_retry_after returns None for non-integer values (HTTP-date form not supported).
    #[test]
    fn parse_retry_after_returns_none_for_non_integer() {
        assert_eq!(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), None);
        assert_eq!(parse_retry_after(""), None);
        assert_eq!(parse_retry_after("abc"), None);
    }

    /// 429 with no Retry-After header → RateLimited { retry_after: None }.
    #[tokio::test]
    async fn fetch_returns_rate_limited_on_429_no_header() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/limited"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;

        let req = FetchRequest {
            url: format!("{}/limited", server.uri()),
            etag: None,
            last_modified: None,
        };
        let outcome = test_fetcher().fetch(&req).await.unwrap();
        assert!(
            matches!(outcome, FetchOutcome::RateLimited { retry_after: None }),
            "Expected RateLimited {{ retry_after: None }} but got {:?}",
            outcome
        );
    }

    /// 429 with `Retry-After: 60` → RateLimited { retry_after: Some(60s) }.
    #[tokio::test]
    async fn fetch_returns_rate_limited_with_retry_after_seconds() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/limited"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "60"))
            .mount(&server)
            .await;

        let req = FetchRequest {
            url: format!("{}/limited", server.uri()),
            etag: None,
            last_modified: None,
        };
        let outcome = test_fetcher().fetch(&req).await.unwrap();
        assert!(
            matches!(
                outcome,
                FetchOutcome::RateLimited { retry_after: Some(d) } if d == Duration::from_secs(60)
            ),
            "Expected RateLimited {{ retry_after: Some(60s) }} but got {:?}",
            outcome
        );
    }

    /// 404 → OtherStatus { status: 404 }.
    #[tokio::test]
    async fn fetch_returns_other_status_for_404() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/missing"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let req = FetchRequest {
            url: format!("{}/missing", server.uri()),
            etag: None,
            last_modified: None,
        };

        let outcome = test_fetcher().fetch(&req).await.unwrap();
        assert!(
            matches!(outcome, FetchOutcome::OtherStatus { status: 404 }),
            "Expected OtherStatus(404) but got {:?}",
            outcome
        );
    }
}
