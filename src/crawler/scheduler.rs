use std::cell::RefCell;
use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, LocalSet};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::crawler::{
    driver::{crawl_target, DriverConfig},
    fetcher::Fetcher,
    page::PageContext,
    robots::RobotsChecker,
    CrawlResult,
};
use crate::db::Database;
use crate::index::indexer::{self, IndexJob, CHANNEL_CAPACITY};
use crate::index::size::{check_capacity, index_dir_size_bytes};

// ── Public types ────────────────────────────────────────────────────────────

/// A crawl target that is due for re-crawling.
pub struct DueTarget {
    pub id: String,
    pub collection_id: String,
    pub collection_name: String,
    pub url_prefix: String,
    pub recrawl_interval_secs: i64,
    /// Per-target politeness-delay override in seconds. `None` means use
    /// the global default from DriverConfig.
    pub crawl_delay_secs: Option<i64>,
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Returns all enabled crawl targets that are due for crawling as of `now_unix`.
///
/// A target is due if `last_crawled_at IS NULL` or
/// `last_crawled_at + recrawl_interval_s <= now_unix`.
///
/// Regression: removing the `OR ... + recrawl_interval_s <= ?1` condition would
/// cause `list_due_excludes_recently_crawled_targets` to wrongly include a
/// target that was crawled less than one interval ago.
pub fn list_due_targets(conn: &rusqlite::Connection, now_unix: i64) -> CrawlResult<Vec<DueTarget>> {
    const SQL: &str = "
        SELECT ct.id,
               ct.collection_id,
               c.name,
               ct.url_prefix,
               ct.recrawl_interval_s,
               ct.crawl_delay_secs
        FROM crawl_targets ct
        JOIN collections c ON c.id = ct.collection_id
        WHERE ct.enabled = 1
          AND (ct.last_crawled_at IS NULL
               OR ct.last_crawled_at + ct.recrawl_interval_s <= ?1)
    ";

    let mut stmt = conn.prepare(SQL)?;
    let targets = stmt
        .query_map([now_unix], |row| {
            Ok(DueTarget {
                id: row.get(0)?,
                collection_id: row.get(1)?,
                collection_name: row.get(2)?,
                url_prefix: row.get(3)?,
                recrawl_interval_secs: row.get(4)?,
                crawl_delay_secs: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(targets)
}

/// Shared, cheaply-cloneable handle bundling everything a per-target task
/// needs at runtime. Kept Arc so spawning a fresh task is just a handful
/// of refcount bumps. Exposed for integration tests that drive
/// [`run_one_target`] directly.
#[derive(Clone)]
pub struct TaskCtx {
    pub fetcher: Arc<Fetcher>,
    pub robots: Arc<RobotsChecker>,
    pub db: Arc<Database>,
    pub indexer_tx: mpsc::Sender<IndexJob>,
    pub driver_config: Arc<DriverConfig>,
}

/// Run the BFS driver for a single due target, then update `last_crawled_at`.
///
/// The driver takes `&Database` (not a long-held `&Connection`) so each DB
/// op locks the connection mutex only for its own duration; this lets
/// multiple concurrent `run_one_target` tasks make progress on the same
/// single-thread runtime. A regression that re-introduces a long-held
/// `MutexGuard` here would silently re-serialise the scheduler.
pub async fn run_one_target(target: &DueTarget, ctx: &TaskCtx, now_unix: i64) -> CrawlResult<()> {
    let page_ctx = PageContext {
        collection_id: target.collection_id.clone(),
        crawl_target_id: target.id.clone(),
        url_prefix: target.url_prefix.clone(),
        collection_name: target.collection_name.clone(),
    };

    // Per-target override: clone the DriverConfig and replace its
    // politeness_delay. effective_delay_for_run will still take max() with
    // robots.txt Crawl-Delay, so the override raises the floor but can't
    // violate robots.txt.
    let driver_config: Arc<DriverConfig> = match target.crawl_delay_secs {
        Some(secs) if secs > 0 => {
            let mut cfg = (*ctx.driver_config).clone();
            cfg.politeness_delay = std::time::Duration::from_secs(secs as u64);
            Arc::new(cfg)
        }
        _ => Arc::clone(&ctx.driver_config),
    };

    let stats = crawl_target(
        &target.url_prefix,
        &page_ctx,
        &ctx.fetcher,
        &ctx.robots,
        &ctx.db,
        &ctx.indexer_tx,
        &driver_config,
        now_unix,
    )
    .await?;

    {
        let conn = ctx.db.conn();
        conn.execute(
            "UPDATE crawl_targets SET last_crawled_at = ?1 WHERE id = ?2",
            rusqlite::params![now_unix, target.id],
        )?;
    }

    info!(
        target_id = %target.id,
        url_prefix = %target.url_prefix,
        pages_fetched = stats.pages_fetched,
        pages_indexed = stats.pages_indexed,
        pages_not_modified = stats.pages_not_modified,
        pages_errored = stats.pages_errored,
        "crawl target complete"
    );

    Ok(())
}

// ── Scheduler ───────────────────────────────────────────────────────────────

/// Background crawl scheduler.
pub struct Scheduler {
    pub config: Arc<Config>,
    pub db: Arc<Database>,
    pub index: Arc<tantivy::Index>,
    pub index_path: PathBuf,
}

impl Scheduler {
    /// Spawn the scheduler as a background OS thread.
    ///
    /// The thread hosts a `current_thread` Tokio runtime with a `LocalSet`,
    /// and on it runs *three* kinds of work cooperatively:
    ///
    /// 1. The **indexer task** — sole owner of the Tantivy `IndexWriter`.
    ///    Drains the upsert channel and the admin-side delete channel;
    ///    commits in batches.
    /// 2. The **ticker** — every `tick`, lists due targets and spawns one
    ///    task per target that isn't already running.
    /// 3. Zero or more **per-target tasks** — each runs a full
    ///    `crawl_target` BFS. They share `db`, `fetcher`, `robots`, and the
    ///    indexer `Sender` via `TaskCtx` clones.
    ///
    /// `indexer_delete_rx` is the receiving end of the admin-side delete
    /// channel; its `Sender` lives in `AppState` and is used by the
    /// crawl-target Remove handler to drop the corresponding documents
    /// from Tantivy after the DB cascade.
    ///
    /// All futures are kept on this one OS thread because each task holds a
    /// `MutexGuard<rusqlite::Connection>` across `.await` boundaries
    /// (`Connection` is `!Sync`). `LocalSet` + `spawn_local` give us
    /// concurrency without the `Send` constraint.
    ///
    /// Returns a sentinel `JoinHandle<()>` that never completes; the real
    /// scheduler loop lives on the spawned OS thread.
    pub fn spawn(
        self,
        tick: Duration,
        indexer_delete_rx: mpsc::Receiver<Vec<String>>,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name("crawl-scheduler".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("crawl-scheduler tokio runtime");
                rt.block_on(async move {
                    let local = LocalSet::new();
                    local
                        .run_until(self.run_loop(tick, indexer_delete_rx))
                        .await;
                });
            })
            .expect("spawn crawl-scheduler thread");

        // Sentinel: the real work runs on the OS thread above.
        tokio::spawn(std::future::pending())
    }

    /// Main scheduler loop. Owns the indexer channel sender for the lifetime
    /// of the process so the indexer's `rx.recv()` never returns `None`
    /// during normal operation. Spawns the indexer task once on the
    /// surrounding `LocalSet`; ticks every `tick` to dispatch crawls.
    async fn run_loop(self, tick: Duration, indexer_delete_rx: mpsc::Receiver<Vec<String>>) {
        let (tx, rx) = mpsc::channel::<IndexJob>(CHANNEL_CAPACITY);
        let db_for_indexer = Arc::clone(&self.db);
        let index_for_indexer = Arc::clone(&self.index);

        // Spawn the indexer on the surrounding LocalSet. It exits when the
        // upsert channel closes; we hold `tx` for the loop's lifetime so
        // this only happens at process teardown.
        let _indexer = tokio::task::spawn_local(async move {
            if let Err(e) = indexer::run(
                rx,
                indexer_delete_rx,
                index_for_indexer,
                db_for_indexer,
            )
            .await
            {
                error!("indexer task exited with error: {e}");
            }
        });

        let ctx = TaskCtx {
            fetcher: Arc::new(self.build_fetcher()),
            robots: Arc::new(self.build_robots()),
            db: Arc::clone(&self.db),
            indexer_tx: tx,
            driver_config: Arc::new(DriverConfig {
                politeness_delay: Duration::from_millis(self.config.crawler_politeness_delay_ms),
                max_pages_per_run: 1000,
            }),
        };

        // Targets currently in-flight, keyed by `crawl_targets.id`. Lives on
        // the single scheduler thread, so `Rc<RefCell<_>>` is the right tool
        // — no need for the Arc/Mutex tax.
        let running: Rc<RefCell<HashSet<String>>> = Rc::new(RefCell::new(HashSet::new()));

        let mut interval = tokio::time::interval(tick);
        loop {
            interval.tick().await;
            // Panic isolation: a panic in `tick_once` (e.g. inside an HTML
            // parser, a third-party crate, an `expect()`) must not kill this
            // thread — the HTTP server would keep serving but the crawler
            // would silently go dark.
            match AssertUnwindSafe(self.tick_once(&ctx, &running))
                .catch_unwind()
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!("scheduler tick error: {}", e),
                Err(panic) => {
                    let msg = panic_message(&panic);
                    error!("scheduler tick panicked: {msg} — continuing");
                }
            }
        }
    }

    /// One ticker iteration: enumerate due targets, spawn a task per target
    /// that isn't already running. Returns immediately after dispatching —
    /// long crawls never block the next tick from firing.
    async fn tick_once(
        &self,
        ctx: &TaskCtx,
        running: &Rc<RefCell<HashSet<String>>>,
    ) -> CrawlResult<()> {
        let now_unix = chrono::Utc::now().timestamp();

        let used = index_dir_size_bytes(&self.index_path).unwrap_or(0);
        if let Err(e) = check_capacity(used, self.config.max_index_bytes) {
            warn!("index capacity exceeded, skipping crawl tick: {}", e);
            return Ok(());
        }

        let due_targets = {
            let conn = self.db.conn();
            list_due_targets(&conn, now_unix)?
        };

        for target in due_targets {
            // Skip if a previous tick is still crawling this target.
            // Cheap: O(1) and avoids any spawn overhead.
            if running.borrow().contains(&target.id) {
                continue;
            }
            running.borrow_mut().insert(target.id.clone());

            let ctx = ctx.clone();
            let running = Rc::clone(running);
            tokio::task::spawn_local(async move {
                let id = target.id.clone();
                let result = AssertUnwindSafe(run_one_target(&target, &ctx, now_unix))
                    .catch_unwind()
                    .await;
                running.borrow_mut().remove(&id);
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => warn!(target_id = %id, "crawl target failed: {e}"),
                    Err(p) => {
                        error!(
                            target_id = %id,
                            "crawl target panicked: {} — continuing",
                            panic_message(&p),
                        );
                    }
                }
            });
        }

        Ok(())
    }

    fn build_fetcher(&self) -> Fetcher {
        Fetcher::new(
            &self.config.crawler_user_agent,
            Duration::from_millis(self.config.crawler_request_timeout_ms),
        )
        .expect("fetcher builds with valid user-agent and timeout")
    }

    fn build_robots(&self) -> RobotsChecker {
        RobotsChecker::new(
            &self.config.crawler_user_agent,
            Duration::from_millis(self.config.crawler_request_timeout_ms),
        )
        .expect("robots checker builds with valid user-agent and timeout")
    }
}

/// Best-effort extraction of a panic message from the boxed payload returned
/// by `catch_unwind`. Handles the two common cases — `String` and `&str` —
/// and falls back to a generic note for anything else.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else {
        "non-string panic payload".to_string()
    }
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    fn seed_collection(conn: &rusqlite::Connection) {
        conn.execute_batch(
            "INSERT INTO collections (id, name, description, created_at, updated_at)
             VALUES ('1', 'test-collection', '', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z');",
        )
        .expect("seed collection failed");
    }

    fn insert_target(
        conn: &rusqlite::Connection,
        id: &str,
        last_crawled_at: Option<i64>,
        recrawl_interval_s: i64,
        enabled: i64,
    ) {
        match last_crawled_at {
            Some(ts) => conn
                .execute(
                    "INSERT INTO crawl_targets \
                     (id, collection_id, url_prefix, recrawl_interval_s, last_crawled_at, enabled, created_at)
                     VALUES (?1, '1', 'https://example.com/', ?2, ?3, ?4, '2024-01-01T00:00:00Z')",
                    rusqlite::params![id, recrawl_interval_s, ts, enabled],
                )
                .expect("insert target with ts failed"),
            None => conn
                .execute(
                    "INSERT INTO crawl_targets \
                     (id, collection_id, url_prefix, recrawl_interval_s, last_crawled_at, enabled, created_at)
                     VALUES (?1, '1', 'https://example.com/', ?2, NULL, ?3, '2024-01-01T00:00:00Z')",
                    rusqlite::params![id, recrawl_interval_s, enabled],
                )
                .expect("insert target with null ts failed"),
        };
    }

    /// Targets with `last_crawled_at IS NULL` are always due, regardless of
    /// `recrawl_interval_s`.
    #[test]
    fn list_due_returns_never_crawled_targets() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();
        seed_collection(&conn);
        insert_target(&conn, "1", None, 86400, 1);

        let due = list_due_targets(&conn, 1_000_000).expect("list_due_targets");
        assert_eq!(due.len(), 1, "target with NULL last_crawled_at must be due");
        assert_eq!(due[0].id, "1");
    }

    /// Targets crawled recently (last + interval > now) must NOT be returned.
    ///
    /// Regression: removing the time-based condition from the WHERE clause would
    /// cause this test to fail because the recently-crawled target would appear.
    #[test]
    fn list_due_excludes_recently_crawled_targets() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();
        seed_collection(&conn);
        // last=950_000, interval=86400 → next due at 1_036_400 > now(1_000_000)
        insert_target(&conn, "1", Some(950_000), 86400, 1);

        let due = list_due_targets(&conn, 1_000_000).expect("list_due_targets");
        assert!(
            due.is_empty(),
            "recently crawled target must not be due (next_due=1_036_400 > now=1_000_000)"
        );
    }

    /// Targets where `last + interval <= now` (stale) must be returned.
    #[test]
    fn list_due_includes_stale_targets() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();
        seed_collection(&conn);
        // last=800_000, interval=86400 → next due at 886_400 <= now(1_000_000) → DUE
        insert_target(&conn, "1", Some(800_000), 86400, 1);

        let due = list_due_targets(&conn, 1_000_000).expect("list_due_targets");
        assert_eq!(
            due.len(),
            1,
            "stale target must be due (next_due=886_400 <= now=1_000_000)"
        );
    }

    /// Targets with `enabled = 0` must never be returned, even if overdue.
    #[test]
    fn list_due_excludes_disabled_targets() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();
        seed_collection(&conn);
        insert_target(&conn, "1", None, 86400, 0); // enabled=0

        let due = list_due_targets(&conn, 1_000_000).expect("list_due_targets");
        assert!(due.is_empty(), "disabled target must never be due");
    }

    /// The scheduler loop must survive a panicking `run_once`. We can't
    /// drive `Scheduler::spawn` directly without standing up a full
    /// crawler, so this test exercises the same `catch_unwind` boundary
    /// against a synthetic future that panics on the first poll. If the
    /// boundary is removed, the test will unwind out of `block_on` and
    /// fail. With the boundary in place we observe the panic payload as
    /// a value, then continue.
    #[test]
    fn catch_unwind_isolates_panics_from_the_scheduler_loop() {
        use futures::FutureExt;
        use std::panic::AssertUnwindSafe;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");

        let outcome: Result<(), String> = rt.block_on(async {
            let fut = AssertUnwindSafe(async {
                panic!("simulated tick panic");
            });
            match fut.catch_unwind().await {
                Ok(()) => Err("expected panic, got Ok".to_string()),
                Err(payload) => {
                    let msg = panic_message(&*payload);
                    assert!(msg.contains("simulated tick panic"), "msg = {msg:?}");
                    Ok(())
                }
            }
        });
        outcome.expect("catch_unwind must convert panic into Err, not unwind further");
    }
}
