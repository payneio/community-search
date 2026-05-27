use axum::{
    body::Body,
    extract::Path,
    http::{header, HeaderValue, StatusCode},
    response::Response,
};
use rust_embed::RustEmbed;

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
}
