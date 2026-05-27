//! Discovered-engine repository — CRUD over the `discovered_engines` table.

use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredEngine {
    pub url: String,
    pub name: String,
    pub description: String,
    pub first_seen: i64,
    pub last_seen: i64,
}

/// Insert or update a discovered engine.
///
/// `first_seen` is preserved across upserts — the original discovery timestamp
/// is never overwritten.  `last_seen` is advanced to the maximum of the stored
/// value and the incoming value.
pub fn upsert(conn: &Connection, e: &DiscoveredEngine) -> Result<()> {
    conn.execute(
        "INSERT INTO discovered_engines (url, name, description, first_seen, last_seen)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(url) DO UPDATE SET
           name        = excluded.name,
           description = excluded.description,
           last_seen   = MAX(last_seen, excluded.last_seen)",
        rusqlite::params![e.url, e.name, e.description, e.first_seen, e.last_seen],
    )?;
    Ok(())
}

/// Return all discovered engines ordered by `last_seen DESC`.
pub fn list(conn: &Connection) -> Result<Vec<DiscoveredEngine>> {
    let mut stmt = conn.prepare(
        "SELECT url, name, description, first_seen, last_seen
         FROM discovered_engines
         ORDER BY last_seen DESC",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(DiscoveredEngine {
            url: r.get(0)?,
            name: r.get(1)?,
            description: r.get(2)?,
            first_seen: r.get(3)?,
            last_seen: r.get(4)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Return the engine with the given URL, or `None` if not present.
pub fn get(conn: &Connection, url: &str) -> Result<Option<DiscoveredEngine>> {
    let mut stmt = conn.prepare(
        "SELECT url, name, description, first_seen, last_seen
         FROM discovered_engines
         WHERE url = ?1",
    )?;
    let mut rows = stmt.query_map(rusqlite::params![url], |r| {
        Ok(DiscoveredEngine {
            url: r.get(0)?,
            name: r.get(1)?,
            description: r.get(2)?,
            first_seen: r.get(3)?,
            last_seen: r.get(4)?,
        })
    })?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Delete the engine with the given URL and return the number of rows deleted.
pub fn remove(conn: &Connection, url: &str) -> Result<usize> {
    let n = conn.execute(
        "DELETE FROM discovered_engines WHERE url = ?1",
        rusqlite::params![url],
    )?;
    Ok(n)
}

/// Ensures the engine's own URL is present in the discovered list.
/// Idempotent. Preserves the original `first_seen`.
///
/// Delegates to [`upsert`]; the `rusqlite::Result` return type is preserved for
/// compatibility with call sites that use it directly with `?` in non-anyhow contexts.
pub fn ensure_self_entry(
    conn: &Connection,
    self_url: &str,
    name: &str,
    description: &str,
    now: i64,
) -> Result<()> {
    upsert(
        conn,
        &DiscoveredEngine {
            url: self_url.to_string(),
            name: name.to_string(),
            description: description.to_string(),
            first_seen: now,
            last_seen: now,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_in_memory;
    use rusqlite::Connection;

    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        crate::db::run_migrations(&conn).unwrap();
        conn
    }

    fn engine(url: &str, ts: i64) -> DiscoveredEngine {
        DiscoveredEngine {
            url: url.to_string(),
            name: format!("Engine {url}"),
            description: String::new(),
            first_seen: ts,
            last_seen: ts,
        }
    }

    #[test]
    fn upsert_then_get_returns_engine() {
        let conn = fresh_db();
        let e = engine("https://a", 10);
        upsert(&conn, &e).unwrap();
        let stored = get(&conn, "https://a")
            .unwrap()
            .expect("engine should exist");
        assert_eq!(stored.url, "https://a");
        assert_eq!(stored.first_seen, 10);
    }

    #[test]
    fn upsert_twice_updates_last_seen_keeps_first_seen() {
        let conn = fresh_db();
        // First upsert at ts=10
        upsert(&conn, &engine("https://a", 10)).unwrap();
        // Second upsert with first_seen=50, last_seen=50
        let e2 = DiscoveredEngine {
            url: "https://a".to_string(),
            name: "Engine https://a".to_string(),
            description: String::new(),
            first_seen: 50,
            last_seen: 50,
        };
        upsert(&conn, &e2).unwrap();
        let stored = get(&conn, "https://a")
            .unwrap()
            .expect("engine should exist");
        assert_eq!(stored.first_seen, 10, "first_seen should be preserved");
        assert_eq!(stored.last_seen, 50, "last_seen should be updated");
    }

    #[test]
    fn list_orders_by_last_seen_desc() {
        let conn = fresh_db();
        upsert(&conn, &engine("https://a", 10)).unwrap();
        upsert(&conn, &engine("https://b", 30)).unwrap();
        upsert(&conn, &engine("https://c", 20)).unwrap();
        let engines = list(&conn).unwrap();
        assert_eq!(engines.len(), 3);
        assert_eq!(engines[0].url, "https://b"); // last_seen=30
        assert_eq!(engines[1].url, "https://c"); // last_seen=20
        assert_eq!(engines[2].url, "https://a"); // last_seen=10
    }

    #[test]
    fn remove_deletes_row() {
        let conn = fresh_db();
        upsert(&conn, &engine("https://a", 10)).unwrap();
        let n = remove(&conn, "https://a").unwrap();
        assert_eq!(n, 1);
        let result = get(&conn, "https://a").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn ensure_self_entry_inserts_when_missing() {
        let db = open_in_memory().unwrap();
        let conn = db.connection();
        ensure_self_entry(&conn, "https://me.example.com", "Me", "My engine", 1234).unwrap();
        let got = get(&conn, "https://me.example.com").unwrap().unwrap();
        assert_eq!(got.name, "Me");
        assert_eq!(got.first_seen, 1234);
        assert_eq!(got.last_seen, 1234);
    }

    #[test]
    fn ensure_self_entry_updates_metadata_but_not_first_seen() {
        let db = open_in_memory().unwrap();
        let conn = db.connection();
        ensure_self_entry(&conn, "https://me.example.com", "Me", "v1", 1000).unwrap();
        ensure_self_entry(&conn, "https://me.example.com", "Me-Renamed", "v2", 2000).unwrap();
        let got = get(&conn, "https://me.example.com").unwrap().unwrap();
        assert_eq!(got.name, "Me-Renamed");
        assert_eq!(got.description, "v2");
        assert_eq!(got.first_seen, 1000, "first_seen preserved");
        assert_eq!(got.last_seen, 2000);
    }
}
