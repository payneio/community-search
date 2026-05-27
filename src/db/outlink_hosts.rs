use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Cap on how many example URLs we store per host. Enough for an admin to
/// understand what pages link out without bloating the row.
const MAX_EXAMPLES: usize = 5;

// ── Types ─────────────────────────────────────────────────────────────────────

/// One example link to a host: which page mentioned it and with what anchor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlinkExample {
    pub source_url: String,
    pub target_url: String,
    pub link_text: String,
}

/// A row from outlink_host_suggestions as returned to the admin UI.
#[derive(Debug, Serialize)]
pub struct OutlinkHostRow {
    /// SQLite rowid (used by promote/dismiss endpoints).
    pub id: i64,
    pub host: String,
    pub link_count: i64,
    /// Parsed examples_json. Capped at [`MAX_EXAMPLES`].
    pub examples: Vec<OutlinkExample>,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
}

/// Internal struct used when fetching a host row for promote/dismiss.
pub struct OutlinkHostRecord {
    pub rowid: i64,
    pub collection_id: String,
    pub host: String,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Record one observed link to `host` from the given crawl.
///
/// - First sighting → INSERT a new pending row with one example.
/// - Repeat sighting on a pending row → increment `link_count`, bump
///   `last_seen_at`, merge the example into `examples_json` (capped at
///   [`MAX_EXAMPLES`], no duplicates).
/// - Row already promoted or dismissed → no-op. This is what makes
///   dismissal sticky: the crawler stops adding noise for that host
///   without needing a separate blacklist table.
pub fn record_hit(
    conn: &Connection,
    collection_id: &str,
    host: &str,
    example: &OutlinkExample,
    now_unix: i64,
) -> Result<()> {
    let existing: Option<(i64, String, String)> = conn
        .query_row(
            "SELECT rowid, status, examples_json \
             FROM outlink_host_suggestions \
             WHERE collection_id = ?1 AND host = ?2",
            rusqlite::params![collection_id, host],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;

    match existing {
        Some((rowid, status, examples_json)) => {
            if status != "pending" {
                return Ok(());
            }
            let merged = merge_example(&examples_json, example);
            conn.execute(
                "UPDATE outlink_host_suggestions \
                 SET link_count = link_count + 1, \
                     last_seen_at = ?1, \
                     examples_json = ?2 \
                 WHERE rowid = ?3",
                rusqlite::params![now_unix, merged, rowid],
            )?;
        }
        None => {
            let id = Uuid::new_v4().to_string();
            let examples_json = serde_json::to_string(&[example])?;
            conn.execute(
                "INSERT INTO outlink_host_suggestions \
                 (id, collection_id, host, link_count, examples_json, \
                  first_seen_at, last_seen_at, status) \
                 VALUES (?1, ?2, ?3, 1, ?4, ?5, ?5, 'pending')",
                rusqlite::params![id, collection_id, host, examples_json, now_unix],
            )?;
        }
    }
    Ok(())
}

/// Return the number of host rows stored for a collection (any status).
/// Used by tests and stats; not on the hot path.
pub fn count_for_collection(conn: &Connection, collection_id: &str) -> Result<i64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM outlink_host_suggestions WHERE collection_id = ?1",
        rusqlite::params![collection_id],
        |row| row.get(0),
    )?;
    Ok(count)
}

/// List pending host rows for a collection, busiest first.
///
/// Sort order — `link_count DESC, last_seen_at DESC` — surfaces the highest-
/// signal review candidates first: hosts linked many times across many crawls.
///
/// `collection_id` is the SQLite rowid of the parent collection (for parity
/// with the older outlinks API). `limit` is clamped to [1, 500]; default 50.
pub fn list_pending(
    conn: &Connection,
    collection_id: i64,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<OutlinkHostRow>> {
    let limit = limit.unwrap_or(50).clamp(1, 500);
    let offset = offset.unwrap_or(0).max(0);

    let collection_uuid: String = conn.query_row(
        "SELECT id FROM collections WHERE rowid = ?1",
        rusqlite::params![collection_id],
        |row| row.get(0),
    )?;

    let mut stmt = conn.prepare(
        "SELECT rowid, host, link_count, examples_json, first_seen_at, last_seen_at \
         FROM outlink_host_suggestions \
         WHERE collection_id = ?1 AND status = 'pending' \
         ORDER BY link_count DESC, last_seen_at DESC \
         LIMIT ?2 OFFSET ?3",
    )?;

    let rows = stmt.query_map(
        rusqlite::params![collection_uuid, limit, offset],
        |row| {
            let examples_json: String = row.get(3)?;
            let examples: Vec<OutlinkExample> =
                serde_json::from_str(&examples_json).unwrap_or_default();
            Ok(OutlinkHostRow {
                id: row.get(0)?,
                host: row.get(1)?,
                link_count: row.get(2)?,
                examples,
                first_seen_at: row.get(4)?,
                last_seen_at: row.get(5)?,
            })
        },
    )?;

    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Fetch a single host row by its SQLite rowid. Returns `None` if not found.
pub fn find(conn: &Connection, id: i64) -> Result<Option<OutlinkHostRecord>> {
    conn.query_row(
        "SELECT rowid, collection_id, host \
         FROM outlink_host_suggestions WHERE rowid = ?1",
        rusqlite::params![id],
        |row| {
            Ok(OutlinkHostRecord {
                rowid: row.get(0)?,
                collection_id: row.get(1)?,
                host: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// Mark a host as promoted, recording the crawl target that was created.
pub fn mark_promoted(conn: &Connection, id: i64, target_id: i64) -> Result<bool> {
    let rows = conn.execute(
        "UPDATE outlink_host_suggestions \
         SET status = 'promoted', promoted_target_id = ?1 \
         WHERE rowid = ?2",
        rusqlite::params![target_id, id],
    )?;
    Ok(rows > 0)
}

/// Mark a host as dismissed. Future `record_hit` calls for the same
/// (collection, host) will short-circuit — this is the persistent blacklist.
pub fn mark_dismissed(conn: &Connection, id: i64) -> Result<bool> {
    let rows = conn.execute(
        "UPDATE outlink_host_suggestions SET status = 'dismissed' WHERE rowid = ?1",
        rusqlite::params![id],
    )?;
    Ok(rows > 0)
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Merge `new_example` into the existing examples JSON array.
///
/// - Capped at [`MAX_EXAMPLES`] (older entries kept; new arrivals dropped
///   once full — first-seen wins, so the array stabilises and writes
///   become cheap on hot hosts).
/// - Duplicate `(source_url, target_url)` pairs are not re-added.
/// - On malformed JSON, falls back to a single-element array containing
///   only the new example, so a corrupt row self-heals.
fn merge_example(existing_json: &str, new_example: &OutlinkExample) -> String {
    let mut examples: Vec<OutlinkExample> =
        serde_json::from_str(existing_json).unwrap_or_default();

    if examples.len() >= MAX_EXAMPLES {
        return existing_json.to_string();
    }

    let dup = examples.iter().any(|e| {
        e.source_url == new_example.source_url && e.target_url == new_example.target_url
    });
    if dup {
        return existing_json.to_string();
    }

    examples.push(new_example.clone());
    serde_json::to_string(&examples).unwrap_or_else(|_| existing_json.to_string())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    fn seed_collection(conn: &Connection) -> String {
        conn.execute_batch(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES ('c1', 'test', '', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z');",
        )
        .expect("seed collection");
        "c1".to_string()
    }

    fn example(source: &str, target: &str, text: &str) -> OutlinkExample {
        OutlinkExample {
            source_url: source.into(),
            target_url: target.into(),
            link_text: text.into(),
        }
    }

    #[test]
    fn first_hit_inserts_row_with_count_one() {
        let db = Database::open_in_memory().expect("open db");
        let conn = db.conn();
        let col = seed_collection(&conn);

        record_hit(
            &conn,
            &col,
            "example.com",
            &example("https://src/", "https://example.com/a", "anchor"),
            100,
        )
        .expect("record_hit");

        let rows = list_pending(&conn, 1, None, None).expect("list_pending");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].host, "example.com");
        assert_eq!(rows[0].link_count, 1);
        assert_eq!(rows[0].examples.len(), 1);
        assert_eq!(rows[0].first_seen_at, 100);
        assert_eq!(rows[0].last_seen_at, 100);
    }

    #[test]
    fn second_hit_bumps_count_and_appends_example() {
        let db = Database::open_in_memory().expect("open db");
        let conn = db.conn();
        let col = seed_collection(&conn);

        record_hit(
            &conn,
            &col,
            "example.com",
            &example("https://src/a", "https://example.com/1", "one"),
            100,
        )
        .expect("first hit");
        record_hit(
            &conn,
            &col,
            "example.com",
            &example("https://src/b", "https://example.com/2", "two"),
            200,
        )
        .expect("second hit");

        let rows = list_pending(&conn, 1, None, None).expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].link_count, 2);
        assert_eq!(rows[0].examples.len(), 2);
        assert_eq!(rows[0].last_seen_at, 200);
        assert_eq!(rows[0].first_seen_at, 100, "first_seen_at must not move");
    }

    #[test]
    fn examples_capped_at_max() {
        let db = Database::open_in_memory().expect("open db");
        let conn = db.conn();
        let col = seed_collection(&conn);

        // Insert MAX_EXAMPLES + 3 distinct examples.
        for i in 0..MAX_EXAMPLES + 3 {
            record_hit(
                &conn,
                &col,
                "example.com",
                &example(
                    &format!("https://src/{i}"),
                    &format!("https://example.com/{i}"),
                    "anchor",
                ),
                100 + i as i64,
            )
            .expect("hit");
        }

        let rows = list_pending(&conn, 1, None, None).expect("list");
        assert_eq!(rows[0].examples.len(), MAX_EXAMPLES);
        assert_eq!(rows[0].link_count, (MAX_EXAMPLES + 3) as i64);
    }

    #[test]
    fn duplicate_example_does_not_grow_array() {
        let db = Database::open_in_memory().expect("open db");
        let conn = db.conn();
        let col = seed_collection(&conn);

        let ex = example("https://src/x", "https://example.com/x", "same");
        record_hit(&conn, &col, "example.com", &ex, 100).expect("h1");
        record_hit(&conn, &col, "example.com", &ex, 200).expect("h2");

        let rows = list_pending(&conn, 1, None, None).expect("list");
        assert_eq!(rows[0].link_count, 2, "count still bumps");
        assert_eq!(rows[0].examples.len(), 1, "examples must not duplicate");
    }

    #[test]
    fn dismissed_host_short_circuits_future_hits() {
        let db = Database::open_in_memory().expect("open db");
        let conn = db.conn();
        let col = seed_collection(&conn);

        record_hit(
            &conn,
            &col,
            "example.com",
            &example("https://src/a", "https://example.com/1", "t"),
            100,
        )
        .expect("h1");

        let rowid = list_pending(&conn, 1, None, None).expect("list")[0].id;
        assert!(mark_dismissed(&conn, rowid).expect("dismiss"));

        // Subsequent hits must not touch link_count or examples.
        record_hit(
            &conn,
            &col,
            "example.com",
            &example("https://src/b", "https://example.com/2", "t"),
            200,
        )
        .expect("h2 no-op");

        // Row is no longer in pending list.
        assert!(list_pending(&conn, 1, None, None).expect("list").is_empty());

        // Raw row inspection: link_count still 1.
        let (count, examples_json): (i64, String) = conn
            .query_row(
                "SELECT link_count, examples_json FROM outlink_host_suggestions \
                 WHERE collection_id = ?1 AND host = ?2",
                rusqlite::params![col, "example.com"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("read row");
        assert_eq!(count, 1, "dismissed row's count must not bump");
        let parsed: Vec<OutlinkExample> = serde_json::from_str(&examples_json).unwrap();
        assert_eq!(parsed.len(), 1, "dismissed row's examples must not grow");
    }

    #[test]
    fn promoted_host_also_short_circuits() {
        let db = Database::open_in_memory().expect("open db");
        let conn = db.conn();
        let col = seed_collection(&conn);

        record_hit(
            &conn,
            &col,
            "example.com",
            &example("https://src/", "https://example.com/", "t"),
            100,
        )
        .expect("h1");
        let rowid = list_pending(&conn, 1, None, None).expect("list")[0].id;
        assert!(mark_promoted(&conn, rowid, 42).expect("promote"));

        record_hit(
            &conn,
            &col,
            "example.com",
            &example("https://src/b", "https://example.com/b", "t"),
            200,
        )
        .expect("post-promote hit no-op");

        let count: i64 = conn
            .query_row(
                "SELECT link_count FROM outlink_host_suggestions \
                 WHERE collection_id = ?1 AND host = ?2",
                rusqlite::params![col, "example.com"],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(count, 1);
    }

    #[test]
    fn list_orders_by_link_count_desc() {
        let db = Database::open_in_memory().expect("open db");
        let conn = db.conn();
        let col = seed_collection(&conn);

        // a: 1 hit, b: 3 hits, c: 2 hits  → expected order b, c, a
        record_hit(&conn, &col, "a.com", &example("s", "https://a.com/", "t"), 100).unwrap();
        for i in 0..3 {
            record_hit(
                &conn,
                &col,
                "b.com",
                &example(&format!("s{i}"), &format!("https://b.com/{i}"), "t"),
                100 + i,
            )
            .unwrap();
        }
        for i in 0..2 {
            record_hit(
                &conn,
                &col,
                "c.com",
                &example(&format!("s{i}"), &format!("https://c.com/{i}"), "t"),
                100 + i,
            )
            .unwrap();
        }

        let rows = list_pending(&conn, 1, None, None).expect("list");
        let hosts: Vec<&str> = rows.iter().map(|r| r.host.as_str()).collect();
        assert_eq!(hosts, vec!["b.com", "c.com", "a.com"]);
    }
}
