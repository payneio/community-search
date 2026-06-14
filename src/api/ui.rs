use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode},
    response::Response,
};
use rust_embed::RustEmbed;

use crate::api::public::AppState;
use crate::search::result::html_escape;

/// All files under `src/ui/static/` are baked into the binary at compile time.
#[derive(RustEmbed)]
#[folder = "src/ui/static/"]
pub struct StaticAssets;

/// Serve the search UI at `GET /`.
pub async fn serve_index() -> Response<Body> {
    serve_asset("index.html").await
}

/// Serve any embedded static asset at `GET /static/<path>`.
pub async fn serve_static(Path(path): Path<String>) -> Response<Body> {
    serve_asset(&path).await
}

/// Look up `name` in the embedded asset bundle and return a response with the
/// appropriate `Content-Type`.  Returns 404 if the asset does not exist.
async fn serve_asset(name: &str) -> Response<Body> {
    match StaticAssets::get(name) {
        Some(asset) => {
            let mime = mime_guess::from_path(name)
                .first_or_octet_stream()
                .to_string();
            let mut res = Response::new(Body::from(asset.data.into_owned()));
            res.headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_str(&mime).unwrap());
            res
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("not found"))
            .unwrap(),
    }
}

/// Serve `GET /robots.txt`.
///
/// Crawlers are steered *away* from result/API surfaces and toward the
/// homepage. We deliberately disallow query-string URLs (`/*?…`, i.e. every
/// `/?q=` results page) and the API/admin/MCP paths: a search engine's own
/// result pages are thin, duplicative, infinite-URL-space content that engines
/// should not index. Discovery happens via the OpenSearch descriptor instead.
pub async fn serve_robots() -> Response<Body> {
    const ROBOTS: &str = "\
User-agent: *
Allow: /$
Disallow: /api/
Disallow: /admin
Disallow: /mcp
Disallow: /*?
";
    let mut res = Response::new(Body::from(ROBOTS));
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    res
}

/// Serve `GET /opensearch.xml` — an OpenSearch description document.
///
/// This is the standard way for browsers (address-bar search) and other tools
/// to discover that this host *is* a search engine and learn its query-URL
/// templates. Absolute URLs are built from the configured `self_url`; when that
/// is unset we fall back to site-relative templates (accepted by most
/// consumers, though absolute is preferred).
pub async fn serve_opensearch(State(state): State<AppState>) -> Response<Body> {
    let base = state.self_url.trim_end_matches('/');
    let base = html_escape(base);
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/">
  <ShortName>Community Search</ShortName>
  <Description>Self-hosted, federated peer-to-peer search</Description>
  <InputEncoding>UTF-8</InputEncoding>
  <Url type="text/html" method="get" template="{base}/?q={{searchTerms}}"/>
  <Url type="application/json" method="get" template="{base}/api/search?q={{searchTerms}}"/>
</OpenSearchDescription>
"#
    );
    let mut res = Response::new(Body::from(xml));
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/opensearchdescription+xml"),
    );
    res
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn index_html_served() {
        let res = serve_index().await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html"
        );
    }

    #[tokio::test]
    async fn missing_returns_404() {
        let res = serve_asset("does-not-exist.js").await;
        assert_eq!(res.status(), 404);
    }

    #[tokio::test]
    async fn robots_disallows_query_and_api_paths() {
        let res = serve_robots().await;
        assert_eq!(res.status(), 200);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Disallow: /*?"), "must block query URLs");
        assert!(body.contains("Disallow: /api/"), "must block the API");
    }

    #[tokio::test]
    async fn opensearch_embeds_self_url_and_content_type() {
        let mut state = AppState::for_tests_with_token("t");
        state.self_url = "https://search.example/".to_string();
        let res = serve_opensearch(State(state)).await;
        assert_eq!(
            res.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/opensearchdescription+xml"
        );
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        // Trailing slash trimmed; both HTML and JSON templates present.
        assert!(
            body.contains(r#"template="https://search.example/?q={searchTerms}""#),
            "html template missing/incorrect: {body}"
        );
        assert!(
            body.contains(r#"template="https://search.example/api/search?q={searchTerms}""#),
            "json template missing/incorrect: {body}"
        );
    }
}
