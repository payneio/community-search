use std::path::PathBuf;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: String,
    pub port: u16,
    pub data_dir: PathBuf,
    pub admin_token: Option<String>,

    // -- Index settings -------------------------------------------------------
    /// Directory where the Tantivy search index is stored.
    ///
    /// Defaults to `~/.community-search/index` (via the `HOME` environment
    /// variable) when `COMMUNITY_SEARCH_INDEX_PATH` is not set.  In `main`,
    /// the computed value of `data_dir.join("index")` is preferred and wired
    /// into both the scheduler and `AppState`.
    pub index_path: PathBuf,
    /// Maximum on-disk size (bytes) allowed for the search index.
    /// The scheduler will skip crawl ticks once this threshold is reached.
    /// Default: 10 GiB (10_737_418_240 bytes).
    pub max_index_bytes: u64,

    // -- Crawler settings -----------------------------------------------------
    /// User-agent sent in every HTTP request made by the crawler.
    pub crawler_user_agent: String,
    /// Maximum time (ms) to wait for a single HTTP response.
    pub crawler_request_timeout_ms: u64,
    /// Minimum delay (ms) between successive page fetches to a single host.
    pub crawler_politeness_delay_ms: u64,
    /// Maximum number of domains being crawled concurrently by the scheduler.
    pub crawler_max_concurrent_domains: usize,

    // -- Rate limit settings --------------------------------------------------
    /// Maximum search requests per minute for known peer node IPs.
    ///
    /// Peer IPs are those whose hostname appears in any enabled `node_peers.url`.
    /// Defaults to 120.  Set via `COMMUNITY_SEARCH_PEER_RATE_LIMIT_PER_MINUTE`.
    pub peer_rate_limit_per_minute: u32,

    // -- Self-identification --------------------------------------------------
    /// Public URL of this engine, used to seed the discovered_engines table.
    /// Set via `SELF_URL`.  Defaults to empty string.
    pub self_url: String,
    /// Display name of this engine.  Set via `SELF_NAME`.  Defaults to empty string.
    pub self_name: String,
    /// Short description of this engine.  Set via `SELF_DESCRIPTION`.  Defaults to empty string.
    pub self_description: String,

    // -- Federation sync settings --------------------------------------------
    /// Interval in seconds between periodic gossip sync rounds.
    /// Set via `GOSSIP_SYNC_INTERVAL_SECS`.  Defaults to 86400 (1 day).
    pub gossip_sync_interval_secs: u64,
}

impl Config {
    /// Load configuration from process environment only, applying defaults for missing values.
    ///
    /// Defaults:
    /// - `COMMUNITY_SEARCH_BIND_ADDR` -> `"127.0.0.1"`
    /// - `COMMUNITY_SEARCH_PORT`      -> `8080`
    /// - `COMMUNITY_SEARCH_DATA_DIR`  -> `"./data"`
    /// - `COMMUNITY_SEARCH_ADMIN_TOKEN` -> `None` (empty string is treated as absent)
    /// - `COMMUNITY_SEARCH_INDEX_PATH` -> `~/.community-search/index`
    /// - `COMMUNITY_SEARCH_MAX_INDEX_BYTES` -> `10737418240` (10 GiB)
    pub fn from_env_only() -> Result<Self> {
        let bind_addr =
            std::env::var("COMMUNITY_SEARCH_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".to_string());

        let port = std::env::var("COMMUNITY_SEARCH_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse::<u16>()
            .context("COMMUNITY_SEARCH_PORT must be a valid u16")?;

        let data_dir = PathBuf::from(
            std::env::var("COMMUNITY_SEARCH_DATA_DIR").unwrap_or_else(|_| "./data".to_string()),
        );

        let admin_token = std::env::var("COMMUNITY_SEARCH_ADMIN_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());

        let index_path = std::env::var("COMMUNITY_SEARCH_INDEX_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
                PathBuf::from(home).join(".community-search").join("index")
            });

        // Note: `crawler_user_agent` is computed below, after `self_url` is read,
        // because its default contact URL comes from the operator's own SELF_URL.

        let crawler_request_timeout_ms =
            std::env::var("COMMUNITY_SEARCH_CRAWLER_REQUEST_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(30_000);

        let crawler_politeness_delay_ms =
            std::env::var("COMMUNITY_SEARCH_CRAWLER_POLITENESS_DELAY_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(250);

        let crawler_max_concurrent_domains =
            std::env::var("COMMUNITY_SEARCH_CRAWLER_MAX_CONCURRENT_DOMAINS")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(4);

        let max_index_bytes = std::env::var("COMMUNITY_SEARCH_MAX_INDEX_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10_737_418_240); // 10 GiB

        let peer_rate_limit_per_minute =
            std::env::var("COMMUNITY_SEARCH_PEER_RATE_LIMIT_PER_MINUTE")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(120);

        let self_url = std::env::var("SELF_URL").unwrap_or_default();
        let self_name = std::env::var("SELF_NAME").unwrap_or_default();
        let self_description = std::env::var("SELF_DESCRIPTION").unwrap_or_default();

        // Default User-Agent identifies this peer to site admins. Precedence:
        //   1. COMMUNITY_SEARCH_CRAWLER_USER_AGENT — full UA string, verbatim.
        //   2. COMMUNITY_SEARCH_CRAWLER_CONTACT_URL — operator-controlled
        //      "more info" URL (typically the project page); version is
        //      interpolated from CARGO_PKG_VERSION so it tracks releases.
        //   3. SELF_URL — fallback contact is this peer's own federation URL.
        //   4. No URL — bare bot marker.
        let crawler_user_agent = std::env::var("COMMUNITY_SEARCH_CRAWLER_USER_AGENT")
            .unwrap_or_else(|_| {
                let version = env!("CARGO_PKG_VERSION");
                let contact_url = std::env::var("COMMUNITY_SEARCH_CRAWLER_CONTACT_URL")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .or_else(|| Some(self_url.clone()).filter(|s| !s.is_empty()));
                match contact_url {
                    Some(url) => format!("community-search/{} (+{}; bot)", version, url),
                    None => format!("community-search/{} (bot)", version),
                }
            });

        let gossip_sync_interval_secs = std::env::var("GOSSIP_SYNC_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(86400);

        Ok(Config {
            bind_addr,
            port,
            data_dir,
            admin_token,
            index_path,
            max_index_bytes,
            crawler_user_agent,
            crawler_request_timeout_ms,
            crawler_politeness_delay_ms,
            crawler_max_concurrent_domains,
            peer_rate_limit_per_minute,
            self_url,
            self_name,
            self_description,
            gossip_sync_interval_secs,
        })
    }

    /// Load configuration from a `.env` file (if present) and then the process environment.
    ///
    /// A missing `.env` file is silently ignored; parse errors inside the file are propagated.
    pub fn load() -> Result<Self> {
        match dotenvy::dotenv() {
            Ok(_) => {}
            Err(dotenvy::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Self::from_env_only()
    }
}
