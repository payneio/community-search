use std::sync::Arc;
use std::time::Duration;

use community_search::crawler::driver::{crawl_target, DriverConfig};
use community_search::crawler::fetcher::Fetcher;
use community_search::crawler::page::{crawl_page, PageContext};
use community_search::crawler::robots::RobotsChecker;
use community_search::crawler::scheduler::{list_due_targets, run_one_target, TaskCtx};
use community_search::db::crawled_pages;
use community_search::db::outlink_hosts;
use community_search::db::Database;
use community_search::index::indexer::{IndexJob, CHANNEL_CAPACITY};
use tokio::sync::mpsc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ── helpers ──────────────────────────────────────────────────────────────────

/// Seed a collection (id='1') and crawl_target (id='1', collection_id='1') so
/// that FK constraints on crawled_pages and outlink_suggestions are satisfied.
fn seed_collection_and_target(conn: &rusqlite::Connection, url_prefix: &str) {
    conn.execute_batch(&format!(
        "INSERT INTO collections (id, name, description, created_at, updated_at) \
         VALUES ('1', 'test-collection', '', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'); \
         INSERT INTO crawl_targets (id, collection_id, url_prefix, recrawl_interval_s, enabled, created_at) \
         VALUES ('1', '1', '{url_prefix}', 86400, 1, '2024-01-01T00:00:00Z');",
    ))
    .expect("seed_collection_and_target failed");
}

fn make_fetcher() -> Fetcher {
    Fetcher::new("test-agent/1.0", Duration::from_secs(10)).unwrap()
}

fn make_robots() -> RobotsChecker {
    RobotsChecker::new("test-agent/1.0", Duration::from_secs(10)).unwrap()
}

/// Sender of a sink-only indexer channel: spawns a consumer that drains and
/// drops every job. Tests that don't care about index-side state use this so
/// they don't need a full Tantivy + indexer task wired up.
fn spawn_sink_indexer() -> mpsc::Sender<IndexJob> {
    let (tx, mut rx) = mpsc::channel::<IndexJob>(CHANNEL_CAPACITY);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    tx
}

// ── integration tests ─────────────────────────────────────────────────────────

/// First crawl of a fresh page:
/// - robots.txt is missing (404) → allow
/// - page returns 200 with etag "v1" and HTML containing one in-prefix link and
///   one external outlink
/// - result.indexed == true
/// - result.in_prefix_links has exactly 1 entry
/// - outlink_suggestions count == 1
/// - crawled_pages row has etag "v1" and last_status 200
#[tokio::test]
#[allow(clippy::await_holding_lock)] // conn guard held intentionally; single-task test
async fn end_to_end_single_page_indexes_and_records_outlink() {
    let server = MockServer::start().await;
    let base = server.uri();

    // robots.txt → 404 (allow all)
    Mock::given(method("GET"))
        .and(path("/robots.txt"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    // Page HTML: one in-prefix link, one external outlink
    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head><title>Article A</title></head>
<body>
  <p>Content here</p>
  <a href="{base}/articles/b">In-prefix link</a>
  <a href="https://external.example.com/page">External outlink</a>
</body>
</html>"#,
        base = base
    );

    Mock::given(method("GET"))
        .and(path("/articles/a"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(html.as_str(), "text/html; charset=utf-8")
                .insert_header("etag", "v1"),
        )
        .mount(&server)
        .await;

    // Set up DB and index
    let db = Database::open_in_memory().expect("open in-memory DB");
    let url_prefix = format!("{base}/articles/");
    {
        let conn = db.conn();
        seed_collection_and_target(&conn, &url_prefix);
    }

    let tx = spawn_sink_indexer();
    let inflight = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

    let fetcher = make_fetcher();
    let robots = make_robots();

    let page_url = format!("{base}/articles/a");
    let ctx = PageContext {
        collection_id: "1".to_string(),
        crawl_target_id: "1".to_string(),
        url_prefix,
        collection_name: "test-collection".to_string(),
    };

    let result = crawl_page(&page_url, &ctx, &fetcher, &robots, &db, &tx, &inflight, 1_000_000)
        .await
        .expect("crawl_page should succeed");

    assert!(result.indexed, "page should be indexed on first crawl");
    assert!(
        !result.not_modified,
        "first crawl should not be not_modified"
    );
    assert_eq!(
        result.in_prefix_links.len(),
        1,
        "should have exactly 1 in-prefix link, got: {:?}",
        result.in_prefix_links
    );

    let conn = db.conn();
    let outlink_count = outlink_hosts::count_for_collection(&conn, "1")
        .expect("count_for_collection should succeed");
    assert_eq!(
        outlink_count, 1,
        "should have exactly 1 outlink host recorded"
    );

    let row = crawled_pages::get_by_url(&conn, "1", &page_url)
        .expect("get_by_url should succeed")
        .expect("crawled_pages row should exist");

    assert_eq!(
        row.etag,
        Some("v1".to_string()),
        "etag should be 'v1', got: {:?}",
        row.etag
    );
    assert_eq!(
        row.last_status,
        Some(200),
        "last_status should be 200, got: {:?}",
        row.last_status
    );
}

/// Second crawl when the server returns 304 Not Modified:
/// - First call returns 200 with etag "v1" (via up_to_n_times(1))
/// - Second call returns 304
/// - r1.indexed == true
/// - r2.not_modified == true
/// - r2.indexed == false
#[tokio::test]
#[allow(clippy::await_holding_lock)] // conn guard held intentionally; single-task test
async fn second_crawl_with_304_skips_indexing() {
    let server = MockServer::start().await;
    let base = server.uri();

    // robots.txt → 404 (allow all)
    Mock::given(method("GET"))
        .and(path("/robots.txt"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let html = r#"<!DOCTYPE html>
<html>
<head><title>Article A</title></head>
<body><p>Content here</p></body>
</html>"#;

    // Mount the 200 mock FIRST so it is checked first.  wiremock evaluates
    // mocks in registration order; up_to_n_times(1) exhausts it after one
    // use so subsequent requests fall through to the 304 fallback below.
    Mock::given(method("GET"))
        .and(path("/articles/a"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(html, "text/html; charset=utf-8")
                .insert_header("etag", "v1"),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Mount the 304 fallback SECOND — wiremock reaches it only after the
    // 200 mock above has been exhausted.
    Mock::given(method("GET"))
        .and(path("/articles/a"))
        .respond_with(ResponseTemplate::new(304))
        .mount(&server)
        .await;

    // Set up DB and index
    let db = Database::open_in_memory().expect("open in-memory DB");
    let url_prefix = format!("{base}/articles/");
    {
        let conn = db.conn();
        seed_collection_and_target(&conn, &url_prefix);
    }

    let tx = spawn_sink_indexer();
    let inflight = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

    let fetcher = make_fetcher();
    let robots = make_robots();

    let page_url = format!("{base}/articles/a");
    let ctx = PageContext {
        collection_id: "1".to_string(),
        crawl_target_id: "1".to_string(),
        url_prefix,
        collection_name: "test-collection".to_string(),
    };

    // First crawl — should index the page and store etag "v1"
    let r1 = crawl_page(&page_url, &ctx, &fetcher, &robots, &db, &tx, &inflight, 1_000_000)
        .await
        .expect("first crawl_page should succeed");

    // Mark the row as durably indexed so the next-crawl skip-guard fires.
    // In production this happens automatically when the indexer task
    // commits; in this test we set it directly.
    {
        let conn = db.conn();
        conn.execute(
            "UPDATE crawled_pages SET indexed_content_hash = content_hash WHERE url = ?1",
            rusqlite::params![&page_url],
        )
        .expect("mark indexed");
    }

    // Second crawl — server returns 304, should skip re-indexing
    let r2 = crawl_page(&page_url, &ctx, &fetcher, &robots, &db, &tx, &inflight, 2_000_000)
        .await
        .expect("second crawl_page should succeed");

    assert!(r1.indexed, "first crawl should index the page");
    assert!(!r1.not_modified, "first crawl should not be not_modified");

    assert!(r2.not_modified, "second crawl should be not_modified");
    assert!(!r2.indexed, "second crawl should NOT re-index the page");
    assert!(
        r2.in_prefix_links.is_empty(),
        "304 response should yield no in-prefix links"
    );
}

/// BFS driver crawls seed page and follows all in-prefix links discovered.
///
/// Setup:
/// - /articles/  → 200 HTML with 2 in-prefix links (/articles/a, /articles/b)
///                 + 1 out-of-prefix external outlink
/// - /articles/a → 200 HTML (no further in-prefix links)
/// - /articles/b → 200 HTML (no further in-prefix links)
/// - /robots.txt → 404 (allow all)
///
/// Expected: stats.pages_fetched == 3, pages_indexed == 3, pages_errored == 0
///
/// Regression: commenting out the `for link in result.in_prefix_links` BFS
/// expansion loop in driver.rs drops pages_fetched to 1 (only seed crawled).
#[tokio::test]
#[allow(clippy::await_holding_lock)] // conn guard held intentionally; single-task test
async fn driver_crawls_multiple_pages_in_prefix() {
    let server = MockServer::start().await;
    let base = server.uri();

    // robots.txt → 404 (allow all)
    Mock::given(method("GET"))
        .and(path("/robots.txt"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    // Seed page: /articles/ has 2 in-prefix links + 1 external outlink
    let index_html = format!(
        r#"<!DOCTYPE html>
<html>
<head><title>Articles Index</title></head>
<body>
  <a href="{base}/articles/a">Article A</a>
  <a href="{base}/articles/b">Article B</a>
  <a href="https://external.example.com/page">External outlink</a>
</body>
</html>"#,
        base = base
    );
    Mock::given(method("GET"))
        .and(path("/articles/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(index_html.as_str(), "text/html; charset=utf-8"),
        )
        .mount(&server)
        .await;

    // /articles/a — leaf page, no further links
    let html_a = r#"<!DOCTYPE html>
<html><head><title>Article A</title></head>
<body><p>Content of article A.</p></body>
</html>"#;
    Mock::given(method("GET"))
        .and(path("/articles/a"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(html_a, "text/html; charset=utf-8"))
        .mount(&server)
        .await;

    // /articles/b — leaf page, no further links
    let html_b = r#"<!DOCTYPE html>
<html><head><title>Article B</title></head>
<body><p>Content of article B.</p></body>
</html>"#;
    Mock::given(method("GET"))
        .and(path("/articles/b"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(html_b, "text/html; charset=utf-8"))
        .mount(&server)
        .await;

    // Set up DB and index
    let db = Database::open_in_memory().expect("open in-memory DB");
    let url_prefix = format!("{base}/articles/");
    {
        let conn = db.conn();
        seed_collection_and_target(&conn, &url_prefix);
    }

    let tx = spawn_sink_indexer();
    let inflight = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

    let fetcher = make_fetcher();
    let robots = make_robots();

    let seed_url = format!("{base}/articles/");
    let ctx = PageContext {
        collection_id: "1".to_string(),
        crawl_target_id: "1".to_string(),
        url_prefix,
        collection_name: "test-collection".to_string(),
    };

    let config = DriverConfig {
        politeness_delay: Duration::from_millis(1),
        max_pages_per_run: 10,
    };

    let stats = crawl_target(
        &seed_url, &ctx, &fetcher, &robots, &db, &tx, &inflight, &config, 1_000_000,
    )
    .await
    .expect("crawl_target should succeed");

    assert_eq!(
        stats.pages_fetched, 3,
        "BFS should fetch seed + 2 in-prefix pages = 3 total"
    );
    assert_eq!(
        stats.pages_indexed, 3,
        "all 3 pages should be indexed (fresh content)"
    );
    assert_eq!(stats.pages_errored, 0, "should have no crawl errors");
}

/// After `run_one_target` completes, `crawl_targets.last_crawled_at` must be
/// updated to `now_unix` (2_000_000 in this test).
///
/// Setup:
/// - `/robots.txt` → 404  (allow all)
/// - `/articles/`  → 200 minimal HTML
/// - crawl_target seeded with `last_crawled_at = NULL` → always due
#[tokio::test]
async fn scheduler_run_one_target_updates_last_crawled_at() {
    let server = MockServer::start().await;
    let base = server.uri();

    // robots.txt → 404 (allow-all)
    Mock::given(method("GET"))
        .and(path("/robots.txt"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    // /articles/ → minimal 200 HTML
    let html = r#"<!DOCTYPE html>
<html><head><title>Articles</title></head><body><p>Content</p></body></html>"#;
    Mock::given(method("GET"))
        .and(path("/articles/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(html, "text/html; charset=utf-8"))
        .mount(&server)
        .await;

    // Set up DB
    let db = Arc::new(Database::open_in_memory().expect("open in-memory DB"));
    let url_prefix = format!("{base}/articles/");

    {
        let conn = db.conn();
        seed_collection_and_target(&conn, &url_prefix);
    }

    let now: i64 = 2_000_000;

    let due_targets = {
        let conn = db.conn();
        list_due_targets(&conn, now).expect("list_due_targets")
    };
    assert_eq!(due_targets.len(), 1, "seeded target should be due");

    // Build a TaskCtx with a sink indexer — this test cares only about
    // last_crawled_at, not the index state.
    let ctx = TaskCtx {
        fetcher: Arc::new(make_fetcher()),
        robots: Arc::new(make_robots()),
        db: Arc::clone(&db),
        indexer_tx: spawn_sink_indexer(),
        indexing_inflight: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        driver_config: Arc::new(DriverConfig {
            politeness_delay: Duration::from_millis(0),
            max_pages_per_run: 10,
        }),
    };

    run_one_target(&due_targets[0], &ctx, now)
        .await
        .expect("run_one_target should succeed");

    // Verify last_crawled_at was updated to now_unix
    {
        let conn = db.conn();
        let last_crawled_at: i64 = conn
            .query_row(
                "SELECT CAST(last_crawled_at AS INTEGER) FROM crawl_targets WHERE id = '1'",
                [],
                |row| row.get(0),
            )
            .expect("query last_crawled_at");
        assert_eq!(
            last_crawled_at, now,
            "last_crawled_at must equal now_unix={now} after run_one_target"
        );
    }
}
