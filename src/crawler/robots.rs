use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use reqwest::header::USER_AGENT;
use texting_robots::Robot;
use url::Url;

use super::{CrawlError, CrawlResult};

/// Fetches and caches `robots.txt` rules per origin, answering whether a given
/// URL is allowed for our user agent.
pub struct RobotsChecker {
    client: reqwest::Client,
    user_agent: String,
    /// `None` means robots.txt was missing or unparseable → treat as "allow all".
    cache: Mutex<HashMap<String, Option<Robot>>>,
}

impl RobotsChecker {
    /// Build a `RobotsChecker` with the given user-agent string and request timeout.
    pub fn new(user_agent: impl Into<String>, timeout: Duration) -> CrawlResult<Self> {
        let user_agent = user_agent.into();
        let client = reqwest::Client::builder().timeout(timeout).build()?;
        Ok(Self {
            client,
            user_agent,
            cache: Mutex::new(HashMap::new()),
        })
    }

    /// Return `true` if `url` is allowed for our user agent according to the
    /// origin's `robots.txt`.
    ///
    /// The parsed rule set is cached per origin so each origin is fetched at
    /// most once.  On any fetch or parse failure the default is to allow.
    pub async fn is_allowed(&self, url: &str) -> CrawlResult<bool> {
        let origin = self.ensure_cached(url).await?;
        let cache = self.cache.lock().unwrap();
        Ok(match cache.get(&origin) {
            Some(Some(robot)) => robot.allowed(url),
            _ => true,
        })
    }

    /// Return the `Crawl-Delay` value from the origin's `robots.txt`, if any.
    ///
    /// The same cache used by [`is_allowed`] is shared, so calling both
    /// methods for the same origin results in only one network request.
    pub async fn crawl_delay_for(&self, url: &str) -> CrawlResult<Option<Duration>> {
        let origin = self.ensure_cached(url).await?;
        let cache = self.cache.lock().unwrap();
        let delay = match cache.get(&origin) {
            Some(Some(robot)) => robot
                .delay
                .map(|secs| Duration::from_secs(secs.round() as u64)),
            _ => None,
        };
        Ok(delay)
    }

    /// Return the `Sitemap:` URLs declared in the origin's `robots.txt`.
    ///
    /// Shares the same cache as [`is_allowed`] / [`crawl_delay_for`], so
    /// fetching robots.txt happens at most once per origin.
    pub async fn sitemaps_for(&self, url: &str) -> CrawlResult<Vec<String>> {
        let origin = self.ensure_cached(url).await?;
        let cache = self.cache.lock().unwrap();
        Ok(match cache.get(&origin) {
            Some(Some(robot)) => robot.sitemaps.clone(),
            _ => Vec::new(),
        })
    }

    // ── private helpers ────────────────────────────────────────────────────────

    /// Ensure the robots.txt for the origin of `url` is loaded into the cache,
    /// returning the canonical origin string.
    ///
    /// If the origin is already cached this is a fast, lock-only path with no
    /// network I/O.
    async fn ensure_cached(&self, url: &str) -> CrawlResult<String> {
        let parsed = Url::parse(url)?;
        let host = parsed
            .host_str()
            .ok_or_else(|| CrawlError::Other("URL has no host".to_string()))?;
        let origin = match parsed.port() {
            Some(port) => format!("{}://{}:{}", parsed.scheme(), host, port),
            None => format!("{}://{}", parsed.scheme(), host),
        };

        // Fast path: already cached.
        if self.cache.lock().unwrap().contains_key(&origin) {
            return Ok(origin);
        }

        // Slow path: fetch robots.txt for this origin.
        let robots_url = format!("{}/robots.txt", origin);
        let robot = match self
            .client
            .get(&robots_url)
            .header(USER_AGENT, &self.user_agent)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => match response.bytes().await {
                Ok(bytes) => Robot::new(&self.user_agent, &bytes).ok(),
                Err(_) => None,
            },
            _ => None,
        };

        self.cache.lock().unwrap().insert(origin.clone(), robot);
        Ok(origin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_checker() -> RobotsChecker {
        RobotsChecker::new("TestBot/1.0", Duration::from_secs(10)).unwrap()
    }

    /// 404 robots.txt → everything is allowed.
    #[tokio::test]
    async fn allows_everything_when_robots_missing() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/robots.txt"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let checker = test_checker();
        let url = format!("{}/some/page", server.uri());
        assert!(
            checker.is_allowed(&url).await.unwrap(),
            "should allow when robots.txt returns 404"
        );
    }

    /// robots.txt with `Disallow: /private/` blocks /private/secret but allows /public/ok.
    #[tokio::test]
    async fn disallows_paths_matching_rule() {
        let server = MockServer::start().await;

        let robots_txt = "User-agent: *\nDisallow: /private/\n";

        Mock::given(method("GET"))
            .and(path("/robots.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_string(robots_txt))
            .mount(&server)
            .await;

        let checker = test_checker();
        let private_url = format!("{}/private/secret", server.uri());
        let public_url = format!("{}/public/ok", server.uri());

        assert!(
            !checker.is_allowed(&private_url).await.unwrap(),
            "/private/secret should be disallowed"
        );
        assert!(
            checker.is_allowed(&public_url).await.unwrap(),
            "/public/ok should be allowed"
        );
    }

    // ── tests for crawl_delay_for ───────────────────────────────────────────────

    /// `Crawl-delay: 2` in robots.txt → `crawl_delay_for` returns `Some(2s)`.
    #[tokio::test]
    async fn returns_crawl_delay_from_robots_txt() {
        let server = MockServer::start().await;
        let robots_txt = "User-agent: *\nAllow: /\nCrawl-delay: 2\n";
        Mock::given(method("GET"))
            .and(path("/robots.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_string(robots_txt))
            .mount(&server)
            .await;

        let checker = test_checker();
        let url = format!("{}/page", server.uri());
        let delay = checker.crawl_delay_for(&url).await.unwrap();
        assert_eq!(delay, Some(Duration::from_secs(2)));
    }

    /// No `Crawl-Delay` directive → `crawl_delay_for` returns `None`.
    #[tokio::test]
    async fn returns_none_when_no_crawl_delay() {
        let server = MockServer::start().await;
        let robots_txt = "User-agent: *\nAllow: /\n";
        Mock::given(method("GET"))
            .and(path("/robots.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_string(robots_txt))
            .mount(&server)
            .await;

        let checker = test_checker();
        let url = format!("{}/page", server.uri());
        let delay = checker.crawl_delay_for(&url).await.unwrap();
        assert!(delay.is_none(), "no Crawl-Delay directive → None");
    }

    /// 404 robots.txt → `crawl_delay_for` returns `None`.
    #[tokio::test]
    async fn returns_none_crawl_delay_when_robots_missing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/robots.txt"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let checker = test_checker();
        let url = format!("{}/page", server.uri());
        let delay = checker.crawl_delay_for(&url).await.unwrap();
        assert!(delay.is_none(), "404 robots.txt → None crawl delay");
    }

    /// After the first fetch, robots.txt is cached: a second call for a different
    /// path on the same origin must NOT trigger another HTTP request.
    ///
    /// wiremock `.expect(1)` panics on drop if robots.txt was fetched != 1 time.
    #[tokio::test]
    async fn caches_per_origin() {
        let server = MockServer::start().await;

        let robots_txt = "User-agent: *\nAllow: /\n";

        Mock::given(method("GET"))
            .and(path("/robots.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_string(robots_txt))
            .expect(1)
            .mount(&server)
            .await;

        let checker = test_checker();
        let url1 = format!("{}/page1", server.uri());
        let url2 = format!("{}/page2", server.uri());

        assert!(checker.is_allowed(&url1).await.unwrap());
        assert!(checker.is_allowed(&url2).await.unwrap());
        // `server` drops here — wiremock verifies exactly 1 robots.txt fetch.
    }
}
