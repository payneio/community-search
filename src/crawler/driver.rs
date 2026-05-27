use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::warn;

use crate::crawler::{
    fetcher::Fetcher,
    page::{crawl_page, PageContext},
    robots::RobotsChecker,
    sitemap,
    url_class::normalize_url,
    CrawlError, CrawlResult,
};
use crate::db::Database;
use crate::index::indexer::IndexJob;

// ── Public types ──────────────────────────────────────────────────────────────

/// Configuration for a single BFS crawl run.
#[derive(Clone)]
pub struct DriverConfig {
    /// Minimum delay between successive page fetches (politeness).
    pub politeness_delay: Duration,
    /// Maximum number of pages to fetch in one run.
    pub max_pages_per_run: usize,
}

/// Aggregate statistics for a completed crawl run.
#[derive(Default)]
pub struct DriverStats {
    /// Total pages fetched successfully (HTTP response received, error free).
    pub pages_fetched: usize,
    /// Pages whose content was (re-)written to the search index.
    pub pages_indexed: usize,
    /// Pages where the server returned 304 Not Modified.
    pub pages_not_modified: usize,
    /// Pages that returned a `CrawlError` (robots-disallowed, network error, …).
    pub pages_errored: usize,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Crawl `seed_url` and all reachable in-prefix pages in BFS order.
///
/// # Behaviour
/// 1. Probe `robots.txt` once at the start to pick up any `Crawl-Delay`.
/// 2. Compute effective delay: `max(config.politeness_delay, robots Crawl-Delay)`.
/// 3. While the queue is non-empty **and** `pages_fetched < max_pages_per_run`:
///    - Pop the front URL.
///    - Call `crawl_page`; on success reset the consecutive-429 counter, update
///      stats and enqueue newly-discovered in-prefix links; on `RateLimited`
///      apply back-off (Retry-After or exponential), re-queue the URL, and abort
///      after 5 consecutive 429s; on any other error increment `pages_errored`
///      and log a warning.
///    - After each non-rate-limited iteration sleep the effective delay.
/// 4. Commit the index writer and return the stats.
#[allow(clippy::too_many_arguments)]
pub async fn crawl_target(
    seed_url: &str,
    ctx: &PageContext,
    fetcher: &Fetcher,
    robots: &RobotsChecker,
    db: &Database,
    indexer_tx: &mpsc::Sender<IndexJob>,
    config: &DriverConfig,
    now_unix: i64,
) -> CrawlResult<DriverStats> {
    let mut stats = DriverStats::default();
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();

    // Canonicalize the seed before anything else. Without this, a target
    // configured as `https://example.com` and a discovered link to
    // `https://example.com/` are written as two distinct rows in
    // `crawled_pages` even though they're the same resource.
    let seed_url = normalize_url(seed_url)
        .ok_or_else(|| CrawlError::Other(format!("seed URL is unparseable: {seed_url}")))?;

    // Fetch robots.txt once upfront to determine the effective politeness delay.
    // Errors are silently ignored (treat as no Crawl-Delay).
    let robots_delay = robots.crawl_delay_for(&seed_url).await.unwrap_or(None);
    let effective_delay = effective_delay_for_run(config.politeness_delay, robots_delay);

    visited.insert(seed_url.clone());
    queue.push_back(seed_url.clone());

    // Sitemap seeding. JS-only homepages (e.g. Substack) have no server-side
    // article links, so the BFS would otherwise terminate after one page.
    // Best-effort: any failure leaves `seeds` empty and the crawl proceeds
    // with the seed URL alone. Capped at 10k URLs per run.
    let seeds = sitemap::discover_urls(&seed_url, &ctx.url_prefix, fetcher, robots, 10_000).await;
    for s in seeds {
        if visited.insert(s.clone()) {
            queue.push_back(s);
        }
    }

    let mut consecutive_429s: u32 = 0;

    while !queue.is_empty() && stats.pages_fetched < config.max_pages_per_run {
        let url = queue.pop_front().expect("queue non-empty (checked above)");

        match crawl_page(&url, ctx, fetcher, robots, db, indexer_tx, now_unix).await {
            Ok(result) => {
                consecutive_429s = 0;
                stats.pages_fetched += 1;
                if result.indexed {
                    stats.pages_indexed += 1;
                }
                if result.not_modified {
                    stats.pages_not_modified += 1;
                }
                for link in result.in_prefix_links {
                    if !visited.contains(&link) {
                        visited.insert(link.clone());
                        queue.push_back(link);
                    }
                }
            }
            Err(CrawlError::RateLimited { retry_after }) => {
                consecutive_429s += 1;
                if consecutive_429s >= 5 {
                    warn!(
                        "aborting crawl for {}: 5 consecutive 429 responses",
                        seed_url
                    );
                    break;
                }
                let sleep_dur = match retry_after {
                    Some(d) => d.min(Duration::from_secs(120)),
                    None => rate_limit_backoff(consecutive_429s)
                        .unwrap_or(Duration::from_secs(32))
                        .min(Duration::from_secs(120)),
                };
                tokio::time::sleep(sleep_dur).await;
                // Re-queue the URL at the front so it is retried next.
                queue.push_front(url);
                // Skip the politeness delay: the back-off sleep already waited.
                continue;
            }
            Err(e) => {
                stats.pages_errored += 1;
                warn!("crawl error for {}: {}", url, e);
            }
        }

        // Politeness delay: sleep between fetches unless this was the last one.
        if !queue.is_empty() && stats.pages_fetched < config.max_pages_per_run {
            tokio::time::sleep(effective_delay).await;
        }
    }

    // No final commit here: the dedicated indexer task owns the IndexWriter
    // and commits on its own batching schedule (`BATCH_SIZE` jobs or
    // `MAX_AGE` elapsed, whichever fires first). The driver hands each
    // fresh page off via `indexer_tx` and only touches SQLite through
    // per-op `db.conn()` acquisitions inside `crawl_page`.
    Ok(stats)
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Effective politeness delay for a crawl run.
///
/// Returns the larger of `configured` and `robots_crawl_delay` (when present),
/// so we always respect whichever constraint is more conservative.
fn effective_delay_for_run(configured: Duration, robots_crawl_delay: Option<Duration>) -> Duration {
    robots_crawl_delay
        .map(|d| d.max(configured))
        .unwrap_or(configured)
}

/// Exponential back-off delay for the *n*-th consecutive 429 response.
///
/// Returns `None` when `consecutive >= 5`, signalling the caller to abort.
///
/// Sequence: 1 s, 2 s, 4 s, 8 s → abort.
fn rate_limit_backoff(consecutive: u32) -> Option<Duration> {
    if consecutive >= 5 {
        return None;
    }
    Some(Duration::from_secs(1u64 << (consecutive - 1)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tantivy::Index;
    use tokio::sync::mpsc;
    use tokio::task::LocalSet;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::crawler::{fetcher::Fetcher, page::PageContext, robots::RobotsChecker};
    use crate::db::Database;
    use crate::index::indexer::{self, CHANNEL_CAPACITY};
    use crate::index::schema;

    // ── pure-function unit tests ────────────────────────────────────────────────

    /// `rate_limit_backoff` returns exponential durations for counts 1–4
    /// and `None` (abort signal) at count 5.
    #[test]
    fn rate_limit_backoff_is_exponential_up_to_abort() {
        assert_eq!(rate_limit_backoff(1), Some(Duration::from_secs(1)));
        assert_eq!(rate_limit_backoff(2), Some(Duration::from_secs(2)));
        assert_eq!(rate_limit_backoff(3), Some(Duration::from_secs(4)));
        assert_eq!(rate_limit_backoff(4), Some(Duration::from_secs(8)));
        assert_eq!(rate_limit_backoff(5), None, "5 consecutive 429s → abort");
    }

    /// `effective_delay_for_run` returns the maximum of the configured delay and
    /// any Crawl-Delay from robots.txt, falling back to configured when absent.
    #[test]
    fn effective_delay_uses_max_of_configured_and_robots() {
        // configured > robots: keep configured
        assert_eq!(
            effective_delay_for_run(Duration::from_secs(3), Some(Duration::from_secs(2))),
            Duration::from_secs(3),
        );
        // robots > configured: use robots
        assert_eq!(
            effective_delay_for_run(Duration::from_millis(250), Some(Duration::from_secs(2))),
            Duration::from_secs(2),
        );
        // no robots delay: use configured
        assert_eq!(
            effective_delay_for_run(Duration::from_millis(250), None),
            Duration::from_millis(250),
        );
    }

    // ── integration test ────────────────────────────────────────────────────────

    /// After 5 consecutive 429 responses the driver aborts the crawl run and
    /// returns `Ok` with `pages_fetched == 0`.  `Retry-After: 0` makes the
    /// test run without any real sleeps.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn aborts_after_five_consecutive_429s() {
        let server = MockServer::start().await;

        // robots.txt: 404 → allow all, no Crawl-Delay
        Mock::given(method("GET"))
            .and(path("/robots.txt"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        // Seed page: always 429 with Retry-After: 0 (zero-duration sleep → fast test)
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
            .mount(&server)
            .await;

        let db = Database::open_in_memory().expect("in-memory db");
        // Sink consumer: this test never produces an upsert (all 429s →
        // crawl_page returns Err before reaching the index path) but we
        // still need a receiver so the channel doesn't panic on send.
        let (tx, mut rx) = mpsc::channel::<IndexJob>(CHANNEL_CAPACITY);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });

        let fetcher = Fetcher::new("TestBot/1.0", Duration::from_secs(10)).unwrap();
        let robots = RobotsChecker::new("TestBot/1.0", Duration::from_secs(10)).unwrap();
        let config = DriverConfig {
            politeness_delay: Duration::ZERO,
            max_pages_per_run: 100,
        };
        let seed_url = format!("{}/", server.uri());
        let ctx = PageContext {
            collection_id: "col1".to_string(),
            crawl_target_id: "ct1".to_string(),
            url_prefix: seed_url.clone(),
            collection_name: "Test".to_string(),
        };

        let stats = crawl_target(&seed_url, &ctx, &fetcher, &robots, &db, &tx, &config, 0)
            .await
            .expect("crawl_target should return Ok even when aborting due to 429");

        assert_eq!(stats.pages_fetched, 0, "all 429s → no successful fetches");
        assert_eq!(
            stats.pages_errored, 0,
            "rate-limited pages are not counted as errors"
        );
    }

    // ── resilience tests ───────────────────────────────────────────────────────

    /// Seed the test DB with a single `collections` + `crawl_targets` pair so
    /// that `crawled_pages` foreign-key inserts succeed. Returns the
    /// collection UUID, suitable for `PageContext.collection_id`.
    fn seed_collection_and_target(conn: &rusqlite::Connection, prefix: &str) -> String {
        conn.execute_batch(&format!(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES ('col1', 'test', '', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'); \
             INSERT INTO crawl_targets (id, collection_id, url_prefix, recrawl_interval_s, enabled, created_at) \
             VALUES ('ct1', 'col1', '{prefix}', 86400, 1, '2024-01-01T00:00:00Z');",
        ))
        .expect("seed collection+target");
        "col1".to_string()
    }

    /// Mount the standard "robots 404 + GET / returns body" mocks against
    /// the wiremock server. The body is fixed so its content_hash is stable.
    /// `set_body_raw` (not `set_body_string`) is required so the explicit
    /// `text/html` mime survives — `set_body_string` would force
    /// `text/plain` and cause the fetcher to reject the response as
    /// `NotHtml`.
    async fn mount_serves_one_html_page(server: &MockServer, body: &'static str) {
        Mock::given(method("GET"))
            .and(path("/robots.txt"))
            .respond_with(ResponseTemplate::new(404))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/html; charset=utf-8"))
            .mount(server)
            .await;
    }

    /// End-to-end happy-path: crawl_target sends one IndexJob → indexer
    /// commits Tantivy and advances `indexed_content_hash`. Exercises the
    /// full crawler → channel → indexer → journal path so a regression in
    /// any link breaks here.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn crawl_target_journals_indexed_pages_via_indexer() {
        let server = MockServer::start().await;
        mount_serves_one_html_page(&server, "<html><body>hello world</body></html>").await;

        let db = Arc::new(Database::open_in_memory().expect("in-memory db"));
        let seed_url = format!("{}/", server.uri());
        {
            let conn = db.conn();
            seed_collection_and_target(&conn, &seed_url);
        }
        let index = Arc::new(Index::create_in_ram(schema::build()));

        LocalSet::new()
            .run_until(async {
                let (tx, rx) = mpsc::channel::<IndexJob>(CHANNEL_CAPACITY);
                let (_del_tx, del_rx) = mpsc::channel::<Vec<String>>(8);
                let indexer = tokio::task::spawn_local(indexer::run(
                    rx,
                    del_rx,
                    Arc::clone(&index),
                    Arc::clone(&db),
                ));

                let fetcher = Fetcher::new("TestBot/1.0", Duration::from_secs(10)).unwrap();
                let robots = RobotsChecker::new("TestBot/1.0", Duration::from_secs(10)).unwrap();
                let config = DriverConfig {
                    politeness_delay: Duration::ZERO,
                    max_pages_per_run: 10,
                };
                let ctx = PageContext {
                    collection_id: "col1".into(),
                    crawl_target_id: "ct1".into(),
                    url_prefix: seed_url.clone(),
                    collection_name: "test".into(),
                };

                let stats = crawl_target(
                    &seed_url,
                    &ctx,
                    &fetcher,
                    &robots,
                    &db,
                    &tx,
                    &config,
                    1_700_000_000,
                )
                .await
                .expect("crawl_target Ok");
                assert_eq!(stats.pages_indexed, 1);

                // Close the channel and wait for the indexer to do its final
                // flush — this is the only deterministic way to observe the
                // journal update from a test.
                drop(tx);
                indexer.await.expect("indexer joined").expect("indexer ok");

                let conn = db.conn();
                let row = crate::db::crawled_pages::get_by_url(&conn, "col1", &seed_url)
                    .unwrap()
                    .expect("row exists");
                assert_eq!(
                    row.content_hash, row.indexed_content_hash,
                    "journal must record the same hash that was indexed"
                );
            })
            .await;
    }

    /// Two targets driven concurrently must complete in roughly *max* of
    /// their individual durations, not the *sum*. This is the load-bearing
    /// property of the parallelised scheduler: a slow target can no longer
    /// starve a fast one.
    ///
    /// Mechanism: spin up two mock origins, give one a 600 ms server delay
    /// per fetch, leave the other instant. Drive both `crawl_target` calls
    /// inside the same `LocalSet` via `tokio::join!`. Assert the wall-clock
    /// is closer to one of them than to their sum.
    ///
    /// If a regression makes the driver effectively serial (e.g. a global
    /// mutex re-introduced on the index path that holds across awaits) the
    /// elapsed time creeps toward the sum and the assertion fails.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn two_crawl_targets_progress_concurrently() {
        use std::time::Instant;

        let slow = MockServer::start().await;
        let fast = MockServer::start().await;
        for srv in [&slow, &fast] {
            Mock::given(method("GET"))
                .and(path("/robots.txt"))
                .respond_with(ResponseTemplate::new(404))
                .mount(srv)
                .await;
        }
        // Slow mock: each fetch waits ~600 ms server-side.
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(
                        "<html><body>slow page</body></html>",
                        "text/html; charset=utf-8",
                    )
                    .set_delay(Duration::from_millis(600)),
            )
            .mount(&slow)
            .await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "<html><body>fast page</body></html>",
                "text/html; charset=utf-8",
            ))
            .mount(&fast)
            .await;

        let db = Arc::new(Database::open_in_memory().expect("db"));
        let slow_url = format!("{}/", slow.uri());
        let fast_url = format!("{}/", fast.uri());
        {
            let conn = db.conn();
            conn.execute_batch(&format!(
                "INSERT INTO collections (id, name, description, created_at, updated_at) \
                 VALUES ('col1', 'test', '', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'); \
                 INSERT INTO crawl_targets (id, collection_id, url_prefix, recrawl_interval_s, enabled, created_at) \
                 VALUES ('slow', 'col1', '{slow_url}', 86400, 1, '2024-01-01T00:00:00Z'), \
                        ('fast', 'col1', '{fast_url}', 86400, 1, '2024-01-01T00:00:00Z');",
            ))
            .unwrap();
        }

        // Sink consumer — this test measures wall-clock, not the journal.
        let (tx, mut rx) = mpsc::channel::<IndexJob>(CHANNEL_CAPACITY);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });

        LocalSet::new()
            .run_until(async move {
                let fetcher = Fetcher::new("TestBot/1.0", Duration::from_secs(10)).unwrap();
                let robots = RobotsChecker::new("TestBot/1.0", Duration::from_secs(10)).unwrap();
                let config = Arc::new(DriverConfig {
                    politeness_delay: Duration::ZERO,
                    max_pages_per_run: 1,
                });
                let mk_ctx = |id: &str, prefix: String| PageContext {
                    collection_id: "col1".into(),
                    crawl_target_id: id.into(),
                    url_prefix: prefix,
                    collection_name: "test".into(),
                };

                let started = Instant::now();
                let fetcher = Arc::new(fetcher);
                let robots = Arc::new(robots);

                let task_slow = {
                    let db = Arc::clone(&db);
                    let tx = tx.clone();
                    let fetcher = Arc::clone(&fetcher);
                    let robots = Arc::clone(&robots);
                    let config = Arc::clone(&config);
                    let seed = slow_url.clone();
                    tokio::task::spawn_local(async move {
                        crawl_target(
                            &seed,
                            &mk_ctx("slow", seed.clone()),
                            &fetcher,
                            &robots,
                            &db,
                            &tx,
                            &config,
                            1_700_000_000,
                        )
                        .await
                    })
                };
                let task_fast = {
                    let db = Arc::clone(&db);
                    let tx = tx.clone();
                    let fetcher = Arc::clone(&fetcher);
                    let robots = Arc::clone(&robots);
                    let config = Arc::clone(&config);
                    let seed = fast_url.clone();
                    tokio::task::spawn_local(async move {
                        crawl_target(
                            &seed,
                            &mk_ctx("fast", seed.clone()),
                            &fetcher,
                            &robots,
                            &db,
                            &tx,
                            &config,
                            1_700_000_000,
                        )
                        .await
                    })
                };

                let (slow_res, fast_res) = tokio::join!(task_slow, task_fast);
                slow_res.unwrap().unwrap();
                fast_res.unwrap().unwrap();
                let elapsed = started.elapsed();

                // Concurrent: ~600 ms (the slow leg dominates). Serial would
                // be ~1200 ms. Give plenty of headroom for CI jitter — we
                // only need to rule out the serial regime.
                assert!(
                    elapsed < Duration::from_millis(1000),
                    "two concurrent crawls should finish in ~max(durations); \
                     elapsed={elapsed:?} suggests serial execution",
                );
            })
            .await;
    }

    /// A home page with no server-rendered `<a>` links would otherwise yield
    /// a single-page crawl (the JS-only Substack failure mode). With sitemap
    /// seeding, the driver should still discover the article URLs via
    /// `/sitemap.xml` and fetch them in the same run.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn sitemap_seeds_bfs_when_home_has_no_links() {
        let server = MockServer::start().await;

        // robots.txt: 404 → no Sitemap: directive, no Crawl-Delay
        Mock::given(method("GET"))
            .and(path("/robots.txt"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        // Home page: a Substack-style fallback with zero outgoing links.
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "<html><body>enable javascript</body></html>",
                "text/html; charset=utf-8",
            ))
            .mount(&server)
            .await;

        // Sitemap lists two pages we'd otherwise never reach.
        let seed_uri = server.uri();
        let sitemap_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url><loc>{seed_uri}/a</loc></url>
              <url><loc>{seed_uri}/b</loc></url>
            </urlset>"#,
        );
        Mock::given(method("GET"))
            .and(path("/sitemap.xml"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(sitemap_xml, "application/xml; charset=utf-8"),
            )
            .mount(&server)
            .await;

        // The two article pages.
        for p in ["/a", "/b"] {
            Mock::given(method("GET"))
                .and(path(p))
                .respond_with(ResponseTemplate::new(200).set_body_raw(
                    "<html><body>article body</body></html>",
                    "text/html; charset=utf-8",
                ))
                .mount(&server)
                .await;
        }

        let db = Arc::new(Database::open_in_memory().expect("in-memory db"));
        let seed_url = format!("{}/", server.uri());
        {
            let conn = db.conn();
            seed_collection_and_target(&conn, &seed_url);
        }
        let index = Arc::new(Index::create_in_ram(schema::build()));

        LocalSet::new()
            .run_until(async {
                let (tx, rx) = mpsc::channel::<IndexJob>(CHANNEL_CAPACITY);
                let (_del_tx, del_rx) = mpsc::channel::<Vec<String>>(8);
                let indexer = tokio::task::spawn_local(indexer::run(
                    rx,
                    del_rx,
                    Arc::clone(&index),
                    Arc::clone(&db),
                ));

                let fetcher = Fetcher::new("TestBot/1.0", Duration::from_secs(10)).unwrap();
                let robots = RobotsChecker::new("TestBot/1.0", Duration::from_secs(10)).unwrap();
                let config = DriverConfig {
                    politeness_delay: Duration::ZERO,
                    max_pages_per_run: 10,
                };
                let ctx = PageContext {
                    collection_id: "col1".into(),
                    crawl_target_id: "ct1".into(),
                    url_prefix: seed_url.clone(),
                    collection_name: "test".into(),
                };

                let stats = crawl_target(
                    &seed_url,
                    &ctx,
                    &fetcher,
                    &robots,
                    &db,
                    &tx,
                    &config,
                    1_700_000_000,
                )
                .await
                .expect("crawl_target Ok");

                drop(tx);
                indexer.await.expect("indexer joined").expect("indexer ok");

                assert_eq!(
                    stats.pages_indexed, 3,
                    "home + 2 sitemap URLs must all be indexed; without sitemap seeding only the home would be reached"
                );
            })
            .await;
    }

    /// **The core resilience guarantee.** A row that was previously fetched
    /// but whose `indexed_content_hash` is NULL (simulating a crash between
    /// the SQLite write and the Tantivy commit) must be re-indexed on the
    /// next crawl, even when the body is byte-identical to the prior fetch.
    ///
    /// If this test fails the divergence bug from the production incident
    /// is back: pages would stay forever invisible to search.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn pages_with_null_indexed_hash_are_reindexed_on_next_crawl() {
        let server = MockServer::start().await;
        mount_serves_one_html_page(&server, "<html><body>survivor</body></html>").await;

        let db = Arc::new(Database::open_in_memory().expect("in-memory db"));
        let seed_url = format!("{}/", server.uri());
        {
            let conn = db.conn();
            seed_collection_and_target(&conn, &seed_url);
            // Pre-existing crash-survivor row: content_hash present but
            // `indexed_content_hash` is NULL because the previous Tantivy
            // commit was never reached. The hash is intentionally bogus —
            // the guard must compare against `indexed_content_hash`, not
            // `content_hash`, so a matching content_hash wouldn't help.
            crate::db::crawled_pages::upsert(
                &conn,
                &crate::db::crawled_pages::CrawledPageRow {
                    id: 0,
                    collection_id: "col1".into(),
                    crawl_target_id: "ct1".into(),
                    url: seed_url.clone(),
                    etag: None,
                    last_modified: None,
                    content_hash: Some("stale-precrash-hash".into()),
                    last_status: Some(200),
                    last_crawled_at: 1_600_000_000,
                    indexed_content_hash: None,
                },
            )
            .expect("seed crash-survivor row");
        }
        let index = Arc::new(Index::create_in_ram(schema::build()));

        LocalSet::new()
            .run_until(async {
                let (tx, rx) = mpsc::channel::<IndexJob>(CHANNEL_CAPACITY);
                let (_del_tx, del_rx) = mpsc::channel::<Vec<String>>(8);
                let indexer = tokio::task::spawn_local(indexer::run(
                    rx,
                    del_rx,
                    Arc::clone(&index),
                    Arc::clone(&db),
                ));

                let fetcher = Fetcher::new("TestBot/1.0", Duration::from_secs(10)).unwrap();
                let robots = RobotsChecker::new("TestBot/1.0", Duration::from_secs(10)).unwrap();
                let config = DriverConfig {
                    politeness_delay: Duration::ZERO,
                    max_pages_per_run: 10,
                };
                let ctx = PageContext {
                    collection_id: "col1".into(),
                    crawl_target_id: "ct1".into(),
                    url_prefix: seed_url.clone(),
                    collection_name: "test".into(),
                };

                let stats = crawl_target(
                    &seed_url,
                    &ctx,
                    &fetcher,
                    &robots,
                    &db,
                    &tx,
                    &config,
                    1_700_000_000,
                )
                .await
                .expect("crawl_target Ok");
                assert_eq!(
                    stats.pages_indexed, 1,
                    "crash-survivor row must be re-indexed, not skipped"
                );

                drop(tx);
                indexer.await.expect("indexer joined").expect("indexer ok");

                let conn = db.conn();
                let row = crate::db::crawled_pages::get_by_url(&conn, "col1", &seed_url)
                    .unwrap()
                    .expect("row");
                let indexed = row
                    .indexed_content_hash
                    .expect("indexed_content_hash now set");
                assert_eq!(
                    Some(indexed.clone()),
                    row.content_hash,
                    "journal must catch up to current content_hash"
                );
                assert_ne!(
                    indexed, "stale-precrash-hash",
                    "the new content_hash must overwrite the stale one"
                );
            })
            .await;
    }
}
