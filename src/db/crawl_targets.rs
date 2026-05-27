use anyhow::Result;
use chrono::Utc;
use rusqlite::Connection;
use serde::Serialize;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Admin API response struct
// ---------------------------------------------------------------------------

/// Lightweight crawl-target representation returned by the admin CRUD API.
///
/// Uses the SQLite `rowid` as a stable integer `id` and `collection_id` so
/// that callers can refer to records with simple numeric identifiers.
#[derive(Debug, Serialize)]
pub struct CrawlTargetRecord {
    pub id: i64,
    pub collection_id: i64,
    pub url_prefix: String,
    pub recrawl_interval_secs: i64,
}

/// Row returned by `list()` for display in the admin UI.
///
/// Joins to `collections` so the UI can show the collection name without a
/// second round-trip.
#[derive(Debug, Serialize)]
pub struct CrawlTargetListItem {
    pub id: i64,
    pub collection_id: i64,
    pub collection_name: String,
    pub url_prefix: String,
    pub recrawl_interval_secs: i64,
    pub enabled: bool,
    /// Unix epoch seconds. Migration 015 normalized the column to INTEGER.
    pub last_crawled_at: Option<i64>,
    pub created_at: String,
    /// True when at least one crawled_pages row for this target has
    /// `last_status = 429`. Best-effort signal — clears the next time the
    /// crawler successfully updates that row to a non-429 status.
    pub rate_limited: bool,
    /// Count of crawled_pages rows for this target whose most-recent
    /// `last_status` is a non-2xx, non-429 error (4xx/5xx). Surfaces dead
    /// or broken URLs in the admin UI; 429s are reported separately via
    /// [`rate_limited`].
    pub error_pages_count: i64,
    /// Per-target politeness-delay override in seconds. `None` means fall
    /// back to the global default + robots.txt as before; `Some(n)` raises
    /// the floor to `max(n seconds, robots.txt Crawl-Delay)`.
    pub crawl_delay_secs: Option<i64>,
}

// ---------------------------------------------------------------------------
// Admin CRUD functions (operate on `&Connection`, use rowids for id fields)
// ---------------------------------------------------------------------------

/// Insert a new crawl target and return its record.
///
/// `collection_rowid` is the SQLite rowid of the parent collection (the integer
/// id returned by the admin collections endpoint).  The function resolves the
/// UUID `collections.id` internally before inserting into `crawl_targets`.
///
/// Both `recrawl_interval_secs` and the legacy `recrawl_interval_s` columns
/// are set to `recrawl_interval_secs` so that the crawler scheduler, which
/// reads `recrawl_interval_s`, continues to work correctly.
pub fn add(
    conn: &Connection,
    collection_rowid: i64,
    url_prefix: &str,
    recrawl_interval_secs: i64,
) -> Result<CrawlTargetRecord> {
    // Resolve the UUID for the given rowid.
    let collection_uuid: String = conn.query_row(
        "SELECT id FROM collections WHERE rowid = ?1",
        rusqlite::params![collection_rowid],
        |row| row.get(0),
    )?;

    let target_uuid = Uuid::new_v4().to_string();
    let ts = Utc::now().to_rfc3339();

    conn.execute(
        "INSERT INTO crawl_targets \
         (id, collection_id, url_prefix, recrawl_interval_s, recrawl_interval_secs, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?4, ?5)",
        rusqlite::params![
            target_uuid,
            collection_uuid,
            url_prefix,
            recrawl_interval_secs,
            ts
        ],
    )?;

    let rowid = conn.last_insert_rowid();

    Ok(CrawlTargetRecord {
        id: rowid,
        collection_id: collection_rowid,
        url_prefix: url_prefix.to_string(),
        recrawl_interval_secs,
    })
}

/// List all crawl targets, joined to their parent collection.
///
/// Ordered by collection name, then `created_at`, so the UI presents related
/// targets together in stable order.
pub fn list(conn: &Connection) -> Result<Vec<CrawlTargetListItem>> {
    // The `rate_limited` flag is computed via an EXISTS subquery against
    // crawled_pages. The idx_crawled_pages_target index keeps this O(log n)
    // per target. Note the join key is `ct.id` (UUID) not `ct.rowid`.
    // Two correlated subqueries on crawled_pages: one EXISTS for the 429
    // flag, one COUNT for non-2xx non-429 errors. Both ride the
    // idx_crawled_pages_target index so cost is O(log n) per target.
    let mut stmt = conn.prepare(
        "SELECT ct.rowid, c.rowid, c.name, ct.url_prefix, ct.recrawl_interval_secs, \
                ct.enabled, ct.last_crawled_at, ct.created_at, \
                EXISTS ( \
                    SELECT 1 FROM crawled_pages cp \
                    WHERE cp.crawl_target_id = ct.id AND cp.last_status = 429 \
                ) AS rate_limited, \
                ( \
                    SELECT COUNT(*) FROM crawled_pages cp \
                    WHERE cp.crawl_target_id = ct.id \
                      AND cp.last_status >= 400 \
                      AND cp.last_status != 429 \
                ) AS error_pages_count, \
                ct.crawl_delay_secs \
         FROM crawl_targets ct \
         JOIN collections c ON c.id = ct.collection_id \
         ORDER BY c.name, ct.created_at",
    )?;

    let rows = stmt
        .query_map([], |row| {
            Ok(CrawlTargetListItem {
                id: row.get(0)?,
                collection_id: row.get(1)?,
                collection_name: row.get(2)?,
                url_prefix: row.get(3)?,
                recrawl_interval_secs: row.get(4)?,
                enabled: row.get::<_, i64>(5)? != 0,
                last_crawled_at: row.get(6)?,
                created_at: row.get(7)?,
                rate_limited: row.get::<_, i64>(8)? != 0,
                error_pages_count: row.get(9)?,
                crawl_delay_secs: row.get(10)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(rows)
}

/// Set or clear the per-target politeness-delay override.
///
/// `secs = Some(n)` → store the override (the crawler uses
/// `max(n, robots.txt Crawl-Delay)`).
/// `secs = None`    → clear the override (fall back to the global default).
///
/// Returns `true` if a row was updated, `false` if no such target exists.
pub fn set_crawl_delay(conn: &Connection, id: i64, secs: Option<i64>) -> Result<bool> {
    let rows = conn.execute(
        "UPDATE crawl_targets SET crawl_delay_secs = ?1 WHERE rowid = ?2",
        rusqlite::params![secs, id],
    )?;
    Ok(rows > 0)
}

/// Delete the crawl target identified by its rowid.
///
/// Returns `true` if a row was removed, `false` if no such target existed.
///
/// Note: `crawled_pages.crawl_target_id REFERENCES crawl_targets(id) ON
/// DELETE CASCADE`, so this also wipes the matching rows in `crawled_pages`.
/// The Tantivy index is *not* touched here; callers that want the deleted
/// pages to disappear from search must first read their URLs via
/// [`list_page_urls`] and forward them to the indexer's delete channel.
pub fn remove(conn: &Connection, id: i64) -> Result<bool> {
    let rows = conn.execute(
        "DELETE FROM crawl_targets WHERE rowid = ?1",
        rusqlite::params![id],
    )?;
    Ok(rows > 0)
}

/// Return the URLs of every `crawled_pages` row that belongs to the crawl
/// target identified by its rowid. Used by the admin Remove handler to
/// build a delete batch for the search index *before* the FK cascade fires.
///
/// Returns an empty vector if the target does not exist or has no pages.
pub fn list_page_urls(conn: &Connection, rowid: i64) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT cp.url \
         FROM crawled_pages cp \
         JOIN crawl_targets ct ON ct.id = cp.crawl_target_id \
         WHERE ct.rowid = ?1",
    )?;
    let urls = stmt
        .query_map(rusqlite::params![rowid], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(urls)
}

/// Count all crawl targets (enabled and disabled).
///
/// Used by the admin status endpoint to report the total number of configured
/// crawl targets regardless of state.
pub fn count_all(conn: &Connection) -> Result<i64> {
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM crawl_targets", [], |row| row.get(0))?;
    Ok(count)
}

/// Update `recrawl_interval_secs` (and the legacy `recrawl_interval_s`) for
/// the crawl target identified by its rowid.
///
/// Returns `true` if a row was updated, `false` if no such target existed.
pub fn set_interval(conn: &Connection, id: i64, recrawl_interval_secs: i64) -> Result<bool> {
    let rows = conn.execute(
        "UPDATE crawl_targets \
         SET recrawl_interval_secs = ?1, recrawl_interval_s = ?1 \
         WHERE rowid = ?2",
        rusqlite::params![recrawl_interval_secs, id],
    )?;
    Ok(rows > 0)
}
