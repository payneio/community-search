use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CrawlError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("robots.txt disallows {0}")]
    RobotsDisallowed(String),

    #[error("not HTML: {0}")]
    NotHtml(String),

    #[error("index full: used={used}, max={max}")]
    IndexFull { used: u64, max: u64 },

    /// The server returned 429 Too Many Requests.
    #[error("rate limited (retry after {retry_after:?})")]
    RateLimited { retry_after: Option<Duration> },

    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),

    #[error("tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type CrawlResult<T> = Result<T, CrawlError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn robots_disallowed_displays_url() {
        let url = "https://example.com/secret";
        let err = CrawlError::RobotsDisallowed(url.to_string());
        assert_eq!(err.to_string(), format!("robots.txt disallows {url}"));
    }
}
