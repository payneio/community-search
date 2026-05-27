use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, Row};

// ── Struct ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrawledPageRow {
    pub id: i64,
    pub collection_id: String,
    pub crawl_target_id: String,
    pub url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_hash: Option<String>,
    pub last_status: Option<i64>,
    pub last_crawled_at: i64,
    /// Content hash of the document that was most recently committed to the
    /// Tantivy index for this URL. NULL means "no committed version yet" —
    /// the next crawl will re-index even if `content_hash` matches.
    pub indexed_content_hash: Option<String>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Look up a single crawled page by collection and URL.
/// Returns `None` if no matching row exists.
pub fn get_by_url(
    conn: &Connection,
    collection_id: &str,
    url: &str,
) -> Result<Option<CrawledPageRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, collection_id, crawl_target_id, url, etag, last_modified, \
         content_hash, last_status, last_crawled_at, indexed_content_hash \
         FROM crawled_pages WHERE collection_id = ?1 AND url = ?2",
    )?;

    let result = stmt
        .query_row(rusqlite::params![collection_id, url], row_to_struct)
        .optional()?;

    Ok(result)
}

/// Insert or update a crawled page row.
///
/// If a row with the same `(collection_id, url)` already exists the non-key
/// fields are updated in place (keeping the same `id`). Returns the `id` of
/// the affected row.
pub fn upsert(conn: &Connection, row: &CrawledPageRow) -> Result<i64> {
    conn.execute(
        "INSERT INTO crawled_pages \
         (collection_id, crawl_target_id, url, etag, last_modified, content_hash, last_status, last_crawled_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
         ON CONFLICT(collection_id, url) DO UPDATE SET \
         crawl_target_id  = excluded.crawl_target_id, \
         etag             = excluded.etag, \
         last_modified    = excluded.last_modified, \
         content_hash     = excluded.content_hash, \
         last_status      = excluded.last_status, \
         last_crawled_at  = excluded.last_crawled_at",
        rusqlite::params![
            row.collection_id,
            row.crawl_target_id,
            row.url,
            row.etag,
            row.last_modified,
            row.content_hash,
            row.last_status,
            row.last_crawled_at,
        ],
    )?;

    let id: i64 = conn.query_row(
        "SELECT id FROM crawled_pages WHERE collection_id = ?1 AND url = ?2",
        rusqlite::params![row.collection_id, row.url],
        |r| r.get(0),
    )?;

    Ok(id)
}

/// Return all crawled pages belonging to a given crawl target, ordered by id.
pub fn list_by_target(conn: &Connection, crawl_target_id: &str) -> Result<Vec<CrawledPageRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, collection_id, crawl_target_id, url, etag, last_modified, \
         content_hash, last_status, last_crawled_at, indexed_content_hash \
         FROM crawled_pages WHERE crawl_target_id = ?1 ORDER BY id ASC",
    )?;

    let rows = stmt.query_map(rusqlite::params![crawl_target_id], row_to_struct)?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// One entry in a batch passed to [`mark_indexed_batch`].
pub struct IndexedEntry<'a> {
    pub collection_id: &'a str,
    pub url: &'a str,
    pub content_hash: &'a str,
}

/// Mark a batch of crawled-page rows as durably indexed.
///
/// Sets `indexed_content_hash` to the provided hash for each
/// `(collection_id, url)` in `entries`, in a single SQLite transaction.
/// Called by `IndexWriter::commit` *after* the Tantivy commit succeeds so
/// that a crash before this returns leaves the rows with the old (or NULL)
/// `indexed_content_hash`, forcing a re-index next time.
pub fn mark_indexed_batch(conn: &Connection, entries: &[IndexedEntry<'_>]) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "UPDATE crawled_pages \
             SET indexed_content_hash = ?1 \
             WHERE collection_id = ?2 AND url = ?3",
        )?;
        for e in entries {
            stmt.execute(rusqlite::params![e.content_hash, e.collection_id, e.url])?;
        }
    }
    tx.commit()?;
    Ok(())
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn row_to_struct(row: &Row) -> rusqlite::Result<CrawledPageRow> {
    Ok(CrawledPageRow {
        id: row.get(0)?,
        collection_id: row.get(1)?,
        crawl_target_id: row.get(2)?,
        url: row.get(3)?,
        etag: row.get(4)?,
        last_modified: row.get(5)?,
        content_hash: row.get(6)?,
        last_status: row.get(7)?,
        last_crawled_at: row.get(8)?,
        indexed_content_hash: row.get(9)?,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    /// Insert a collection (name='c1') and a crawl_target (prefix='https://example.com/')
    /// using integer-compatible string ids so that FK checks against these TEXT PRIMARY KEY
    /// columns succeed when child rows use `collection_id = 1` / `crawl_target_id = 1`.
    ///
    /// Returns `(collection_id, crawl_target_id)` as i64 values suitable for use in
    /// `CrawledPageRow`.
    fn seed_collection_and_target(conn: &Connection) -> (String, String) {
        conn.execute_batch(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES ('1', 'c1', '', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z');
             INSERT INTO crawl_targets (id, collection_id, url_prefix, recrawl_interval_s, enabled, created_at) \
             VALUES ('1', '1', 'https://example.com/', 86400, 1, '2024-01-01T00:00:00Z');",
        )
        .expect("seed_collection_and_target failed");

        ("1".to_string(), "1".to_string())
    }

    #[test]
    fn upsert_inserts_then_updates() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();
        let (collection_id, crawl_target_id) = seed_collection_and_target(&conn);

        let page = CrawledPageRow {
            id: 0, // ignored on insert; assigned by DB
            collection_id: collection_id.clone(),
            crawl_target_id,
            url: "https://example.com/page1".to_string(),
            etag: Some("etag-v1".to_string()),
            last_modified: None,
            content_hash: None,
            last_status: Some(200),
            last_crawled_at: 1_000,
            indexed_content_hash: None,
        };

        let id1 = upsert(&conn, &page).expect("first upsert");
        assert!(
            id1 > 0,
            "expected a positive id after first insert, got {id1}"
        );

        // Second upsert: same url, different etag and timestamp.
        let page2 = CrawledPageRow {
            etag: Some("etag-v2".to_string()),
            last_crawled_at: 2_000,
            ..page.clone()
        };
        let id2 = upsert(&conn, &page2).expect("second upsert");

        assert_eq!(id1, id2, "conflict should preserve the original row id");

        // Confirm the updated fields are visible via get_by_url.
        let fetched = get_by_url(&conn, &collection_id, "https://example.com/page1")
            .expect("get_by_url")
            .expect("row should exist");

        assert_eq!(fetched.etag, Some("etag-v2".to_string()));
        assert_eq!(fetched.last_crawled_at, 2_000);
        assert_eq!(fetched.id, id1);
    }

    #[test]
    fn list_by_target_returns_only_matching_rows() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();
        let (collection_id, crawl_target_id) = seed_collection_and_target(&conn);

        let make_page = |url: &str| CrawledPageRow {
            id: 0,
            collection_id: collection_id.clone(),
            crawl_target_id: crawl_target_id.clone(),
            url: url.to_string(),
            etag: None,
            last_modified: None,
            content_hash: None,
            last_status: Some(200),
            last_crawled_at: 1_000,
            indexed_content_hash: None,
        };

        upsert(&conn, &make_page("https://example.com/a")).expect("upsert a");
        upsert(&conn, &make_page("https://example.com/b")).expect("upsert b");

        let rows = list_by_target(&conn, &crawl_target_id).expect("list_by_target");
        assert_eq!(
            rows.len(),
            2,
            "expected 2 rows for the target, got {}",
            rows.len()
        );
    }

    #[test]
    fn get_by_url_returns_none_when_missing() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();
        let (collection_id, _) = seed_collection_and_target(&conn);

        let result = get_by_url(&conn, &collection_id, "https://example.com/nonexistent")
            .expect("get_by_url");
        assert!(
            result.is_none(),
            "expected None for missing url, got Some(...)"
        );
    }

    /// `mark_indexed_batch` sets `indexed_content_hash` only for the rows
    /// named in the batch, in a single transaction, leaving other rows
    /// untouched.
    #[test]
    fn mark_indexed_batch_updates_only_listed_rows() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();
        let (col, tgt) = seed_collection_and_target(&conn);

        let mk = |url: &str, hash: &str| CrawledPageRow {
            id: 0,
            collection_id: col.clone(),
            crawl_target_id: tgt.clone(),
            url: url.to_string(),
            etag: None,
            last_modified: None,
            content_hash: Some(hash.to_string()),
            last_status: Some(200),
            last_crawled_at: 1_000,
            indexed_content_hash: None,
        };
        upsert(&conn, &mk("https://example.com/a", "ha")).unwrap();
        upsert(&conn, &mk("https://example.com/b", "hb")).unwrap();
        upsert(&conn, &mk("https://example.com/c", "hc")).unwrap();

        mark_indexed_batch(
            &conn,
            &[
                IndexedEntry {
                    collection_id: &col,
                    url: "https://example.com/a",
                    content_hash: "ha",
                },
                IndexedEntry {
                    collection_id: &col,
                    url: "https://example.com/c",
                    content_hash: "hc",
                },
            ],
        )
        .expect("mark_indexed_batch");

        let a = get_by_url(&conn, &col, "https://example.com/a")
            .unwrap()
            .unwrap();
        let b = get_by_url(&conn, &col, "https://example.com/b")
            .unwrap()
            .unwrap();
        let c = get_by_url(&conn, &col, "https://example.com/c")
            .unwrap()
            .unwrap();
        assert_eq!(a.indexed_content_hash.as_deref(), Some("ha"));
        assert_eq!(b.indexed_content_hash, None, "b was not in batch");
        assert_eq!(c.indexed_content_hash.as_deref(), Some("hc"));
    }
}
