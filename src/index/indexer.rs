//! Single dedicated task that owns the Tantivy `IndexWriter`.
//!
//! Crawler tasks send `IndexJob`s over an mpsc channel. This task drains
//! them, upserts to the in-memory writer buffer, and commits when either
//! `BATCH_SIZE` jobs have queued OR `MAX_AGE` has elapsed since the last
//! commit — whichever fires first. Each commit returns the journal of
//! durable upserts, which we apply to `crawled_pages.indexed_content_hash`
//! before considering the work complete.
//!
//! Single-writer is a Tantivy invariant. Funneling upserts through one task
//! preserves that invariant while letting any number of crawler tasks run
//! in parallel. The bounded channel doubles as a backpressure mechanism:
//! when commits stall, fetchers block on `tx.send().await`, which in turn
//! slows the crawl — the polite response in every direction.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::warn;

use crate::crawler::error::{CrawlError, CrawlResult};
use crate::db::crawled_pages::{mark_indexed_batch, IndexedEntry};
use crate::db::Database;
use crate::index::writer::{Document, IndexWriter};

/// Number of buffered upserts that triggers an immediate commit.
pub const BATCH_SIZE: usize = 50;

/// Maximum age of the oldest buffered upsert before a forced commit. Without
/// this, a slow trickle (e.g. a near-idle scheduler) would leave work
/// uncommitted indefinitely and search results would lag behind crawls.
pub const MAX_AGE: Duration = Duration::from_secs(5);

/// Bounded mpsc capacity. Sized to absorb a full batch plus a small burst
/// without blocking fast crawlers; smaller would couple crawl throughput
/// too tightly to commit cadence.
pub const CHANNEL_CAPACITY: usize = 256;

/// One page ready for indexing, sent from a crawler task to the indexer.
///
/// Carries both the Tantivy document fields and the journal metadata
/// (`collection_id` + `content_hash`) so the indexer can mark
/// `crawled_pages.indexed_content_hash` after the commit succeeds.
#[derive(Debug, Clone)]
pub struct IndexJob {
    pub collection_name: String,
    pub url: String,
    pub title: String,
    pub body: String,
    pub indexed_at: i64,
    pub collection_id: String,
    pub content_hash: String,
}

impl IndexJob {
    fn as_document(&self) -> Document<'_> {
        Document {
            collection: &self.collection_name,
            url: &self.url,
            title: &self.title,
            body: &self.body,
            indexed_at: self.indexed_at,
            collection_id: &self.collection_id,
            content_hash: &self.content_hash,
        }
    }
}

/// Drive the indexer until the upsert channel is closed.
///
/// Holds the `IndexWriter` for the entire lifetime; the writer is not
/// `Sync`, so the indexer must be the only place it lives.
///
/// Two input channels: `rx` carries [`IndexJob`] upserts from crawler tasks,
/// and `delete_rx` carries batches of URLs to remove (admin-initiated, when
/// a crawl target is deleted). Deletes force an immediate commit so the
/// admin-visible effect is prompt; upserts batch under the usual size/age
/// triggers.
///
/// Shutdown is gated on `rx`: when the upsert channel closes, the indexer
/// flushes and exits even if `delete_rx` is still open. The Tantivy writer
/// only allows a single owner, so callers must not spawn a second indexer
/// against the same index.
///
/// `await_holding_lock` is silenced for the same reason as the scheduler:
/// this task runs on a single-threaded runtime where holding a
/// `MutexGuard<Connection>` across `.await` cannot deadlock.
#[allow(clippy::await_holding_lock)]
pub async fn run(
    mut rx: mpsc::Receiver<IndexJob>,
    mut delete_rx: mpsc::Receiver<Vec<String>>,
    index: Arc<tantivy::Index>,
    db: Arc<Database>,
    inflight: Arc<AtomicI64>,
) -> CrawlResult<()> {
    let mut writer = IndexWriter::open(&index)?;
    let mut pending: usize = 0;
    let mut deadline = Instant::now() + MAX_AGE;
    let mut delete_open = true;

    loop {
        tokio::select! {
            // Prefer draining queued work over firing the timer when both
            // are ready. Without `biased`, a heavy queue plus a hot timer
            // could starve fetchers in pathological cases.
            biased;

            msg = rx.recv() => match msg {
                Some(job) => {
                    writer.upsert(&job.as_document())?;
                    pending += 1;
                    if pending >= BATCH_SIZE {
                        flush(&mut writer, &db, &inflight)?;
                        pending = 0;
                        deadline = Instant::now() + MAX_AGE;
                    }
                }
                None => {
                    // All upsert senders dropped — graceful shutdown. Final
                    // flush so any tail of work is durable before we exit.
                    flush(&mut writer, &db, &inflight)?;
                    return Ok(());
                }
            },

            // Conditional arm: once `delete_rx` closes, never poll it again.
            // Without the guard, `recv()` on a closed channel returns `None`
            // immediately and the select would spin.
            msg = delete_rx.recv(), if delete_open => match msg {
                Some(urls) => {
                    for u in &urls {
                        writer.delete_url(u)?;
                    }
                    // Admin deletes are user-visible; flush immediately so
                    // the next search no longer returns the removed docs.
                    // This also drains any buffered upserts in the same
                    // commit — the journal still advances correctly.
                    flush(&mut writer, &db, &inflight)?;
                    pending = 0;
                    deadline = Instant::now() + MAX_AGE;
                }
                None => {
                    delete_open = false;
                }
            },

            _ = tokio::time::sleep_until(deadline) => {
                if pending > 0 {
                    flush(&mut writer, &db, &inflight)?;
                    pending = 0;
                }
                deadline = Instant::now() + MAX_AGE;
            }
        }
    }
}

/// Commit the Tantivy writer and apply the returned journal to
/// `crawled_pages.indexed_content_hash`. A failure to journal is logged but
/// does NOT abort the indexer — the index is already durable, and the
/// next crawl will redundantly re-upsert the affected URLs (Tantivy
/// `upsert` is idempotent by URL term-delete).
fn flush(writer: &mut IndexWriter, db: &Database, inflight: &AtomicI64) -> CrawlResult<()> {
    let journal = writer.commit()?;
    // Every journal entry corresponds to one earlier `indexer_upsert_tx.send`
    // (which incremented `inflight`). Decrement here, after Tantivy has
    // committed, so the counter reaches zero exactly when the index is
    // durably caught up. Use saturating_sub semantics via `fetch_sub` —
    // even if a future change miscounts, we don't want it going negative
    // and panicking under cast.
    if !journal.is_empty() {
        inflight.fetch_sub(journal.len() as i64, Ordering::Relaxed);
    }
    if journal.is_empty() {
        return Ok(());
    }
    let conn = db.conn();
    let entries: Vec<IndexedEntry<'_>> = journal
        .iter()
        .map(|j| IndexedEntry {
            collection_id: &j.collection_id,
            url: &j.url,
            content_hash: &j.content_hash,
        })
        .collect();
    if let Err(e) = mark_indexed_batch(&conn, &entries) {
        warn!(
            commit_size = journal.len(),
            "Tantivy commit succeeded but journal update failed: {e} — \
             affected pages will be redundantly re-indexed on next crawl",
        );
        return Err(CrawlError::Other(e.to_string()));
    }
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;
    use tantivy::Index;
    use tokio::task::LocalSet;

    fn mk_job(url: &str, hash: &str) -> IndexJob {
        IndexJob {
            collection_name: "test".into(),
            url: url.into(),
            title: "t".into(),
            body: "b".into(),
            indexed_at: 1_700_000_000,
            collection_id: "col1".into(),
            content_hash: hash.into(),
        }
    }

    fn seed(db: &Database) {
        let conn = db.conn();
        conn.execute_batch(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES ('col1', 't', '', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'); \
             INSERT INTO crawl_targets (id, collection_id, url_prefix, recrawl_interval_s, enabled, created_at) \
             VALUES ('ct1', 'col1', 'https://example.com/', 86400, 1, '2024-01-01T00:00:00Z');",
        )
        .unwrap();
        for i in 0..(BATCH_SIZE + 3) {
            conn.execute(
                "INSERT INTO crawled_pages \
                 (collection_id, crawl_target_id, url, content_hash, last_status, last_crawled_at) \
                 VALUES ('col1', 'ct1', ?1, ?2, 200, 1)",
                rusqlite::params![format!("https://example.com/{i}"), format!("h{i}")],
            )
            .unwrap();
        }
    }

    /// Sending exactly `BATCH_SIZE` jobs must trigger a commit, marking the
    /// journal for those rows. The remaining jobs stay buffered until the
    /// next size or time trigger.
    #[test]
    fn commits_at_batch_size_threshold() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let local = LocalSet::new();
            local
                .run_until(async {
                    let db = Arc::new(Database::open_in_memory().unwrap());
                    seed(&db);
                    let index = Arc::new(Index::create_in_ram(schema::build()));
                    let (tx, rx) = mpsc::channel::<IndexJob>(CHANNEL_CAPACITY);
                    let (_del_tx, del_rx) = mpsc::channel::<Vec<String>>(8);

                    let db_for_task = Arc::clone(&db);
                    let idx_for_task = Arc::clone(&index);
                    let handle =
                        tokio::task::spawn_local(run(rx, del_rx, idx_for_task, db_for_task, std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0))));

                    for i in 0..BATCH_SIZE {
                        tx.send(mk_job(&format!("https://example.com/{i}"), &format!("h{i}")))
                            .await
                            .unwrap();
                    }
                    // Give the indexer a chance to drain and commit.
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    let n: i64 = {
                        let conn = db.conn();
                        conn.query_row(
                            "SELECT COUNT(*) FROM crawled_pages WHERE indexed_content_hash IS NOT NULL",
                            [],
                            |r| r.get(0),
                        )
                        .unwrap()
                    };
                    assert_eq!(
                        n, BATCH_SIZE as i64,
                        "exactly BATCH_SIZE rows must be journaled after the size-trigger commit"
                    );

                    drop(tx);
                    handle.await.unwrap().unwrap();
                })
                .await;
        });
    }

    /// A small batch that never reaches BATCH_SIZE must still get committed
    /// by the time-based trigger after `MAX_AGE`.
    #[test]
    fn commits_at_max_age_when_under_batch_size() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let local = LocalSet::new();
            local
                .run_until(async {
                    let db = Arc::new(Database::open_in_memory().unwrap());
                    seed(&db);
                    let index = Arc::new(Index::create_in_ram(schema::build()));
                    let (tx, rx) = mpsc::channel::<IndexJob>(CHANNEL_CAPACITY);
                    let (_del_tx, del_rx) = mpsc::channel::<Vec<String>>(8);

                    let db_for_task = Arc::clone(&db);
                    let idx_for_task = Arc::clone(&index);
                    let handle =
                        tokio::task::spawn_local(run(rx, del_rx, idx_for_task, db_for_task, std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0))));

                    // Three jobs — well under BATCH_SIZE.
                    for i in 0..3 {
                        tx.send(mk_job(&format!("https://example.com/{i}"), &format!("h{i}")))
                            .await
                            .unwrap();
                    }
                    // Wait longer than MAX_AGE so the timer fires.
                    tokio::time::sleep(MAX_AGE + Duration::from_millis(200)).await;

                    let n: i64 = {
                        let conn = db.conn();
                        conn.query_row(
                            "SELECT COUNT(*) FROM crawled_pages WHERE indexed_content_hash IS NOT NULL",
                            [],
                            |r| r.get(0),
                        )
                        .unwrap()
                    };
                    assert_eq!(
                        n, 3,
                        "time-based trigger must commit small batches; got {n}"
                    );

                    drop(tx);
                    handle.await.unwrap().unwrap();
                })
                .await;
        });
    }

    /// Dropping all senders triggers a final flush so no work is silently
    /// lost on graceful shutdown.
    #[test]
    fn final_flush_on_channel_close() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let local = LocalSet::new();
            local
                .run_until(async {
                    let db = Arc::new(Database::open_in_memory().unwrap());
                    seed(&db);
                    let index = Arc::new(Index::create_in_ram(schema::build()));
                    let (tx, rx) = mpsc::channel::<IndexJob>(CHANNEL_CAPACITY);
                    let (_del_tx, del_rx) = mpsc::channel::<Vec<String>>(8);

                    let db_for_task = Arc::clone(&db);
                    let idx_for_task = Arc::clone(&index);
                    let handle =
                        tokio::task::spawn_local(run(rx, del_rx, idx_for_task, db_for_task, std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0))));

                    tx.send(mk_job("https://example.com/0", "h0")).await.unwrap();
                    tx.send(mk_job("https://example.com/1", "h1")).await.unwrap();
                    drop(tx);

                    handle.await.unwrap().unwrap();

                    let conn = db.conn();
                    let n: i64 = conn
                        .query_row(
                            "SELECT COUNT(*) FROM crawled_pages WHERE indexed_content_hash IS NOT NULL",
                            [],
                            |r| r.get(0),
                        )
                        .unwrap();
                    assert_eq!(n, 2, "final flush must commit the tail before exit");
                })
                .await;
        });
    }

    /// A delete batch sent through `delete_rx` must remove the matching
    /// documents from the index and the effect must be immediately
    /// committed (no waiting for the next batch/age trigger).
    #[test]
    fn delete_batch_removes_docs_and_commits_immediately() {
        use tantivy::{
            collector::Count, query::TermQuery, schema::IndexRecordOption, ReloadPolicy, Term,
        };

        fn count_for_url(index: &tantivy::Index, url: &str) -> usize {
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()
                .unwrap();
            reader.reload().unwrap();
            let f_url = index.schema().get_field("url").unwrap();
            reader
                .searcher()
                .search(
                    &TermQuery::new(Term::from_field_text(f_url, url), IndexRecordOption::Basic),
                    &Count,
                )
                .unwrap()
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let local = LocalSet::new();
            local
                .run_until(async {
                    let db = Arc::new(Database::open_in_memory().unwrap());
                    seed(&db);
                    let index = Arc::new(Index::create_in_ram(schema::build()));
                    let (tx, rx) = mpsc::channel::<IndexJob>(CHANNEL_CAPACITY);
                    let (del_tx, del_rx) = mpsc::channel::<Vec<String>>(8);

                    let db_for_task = Arc::clone(&db);
                    let idx_for_task = Arc::clone(&index);
                    let handle = tokio::task::spawn_local(run(
                        rx,
                        del_rx,
                        idx_for_task,
                        db_for_task,
                        std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
                    ));

                    // Index two pages and let the time trigger commit them.
                    tx.send(mk_job("https://example.com/0", "h0"))
                        .await
                        .unwrap();
                    tx.send(mk_job("https://example.com/1", "h1"))
                        .await
                        .unwrap();
                    tokio::time::sleep(MAX_AGE + Duration::from_millis(200)).await;

                    assert_eq!(
                        count_for_url(&index, "https://example.com/0"),
                        1,
                        "doc 0 must be indexed before delete"
                    );
                    assert_eq!(
                        count_for_url(&index, "https://example.com/1"),
                        1,
                        "doc 1 must be indexed before delete"
                    );

                    // Delete doc 0; doc 1 must remain.
                    del_tx
                        .send(vec!["https://example.com/0".into()])
                        .await
                        .unwrap();
                    // Give the indexer a tick to drain the delete and commit.
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    assert_eq!(
                        count_for_url(&index, "https://example.com/0"),
                        0,
                        "deleted doc must be gone after admin delete batch"
                    );
                    assert_eq!(
                        count_for_url(&index, "https://example.com/1"),
                        1,
                        "other docs must be untouched by the delete batch"
                    );

                    drop(tx);
                    drop(del_tx);
                    handle.await.unwrap().unwrap();
                })
                .await;
        });
    }
}
