use anyhow::Result;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Row};
use serde::Serialize;
use uuid::Uuid;

use crate::db::models::Collection;
use crate::db::Database;

// ---------------------------------------------------------------------------
// Admin API response struct
// ---------------------------------------------------------------------------

/// Lightweight collection representation returned by the admin CRUD API.
///
/// Uses the SQLite `rowid` as a stable integer `id` so that callers can
/// refer to collections with a simple numeric identifier rather than a UUID
/// string.
#[derive(Debug, Serialize)]
pub struct CollectionRecord {
    pub id: i64,
    pub name: String,
    pub description: String,
}

// ---------------------------------------------------------------------------
// Thin wrappers for the admin API (operate on `&Connection`, return rowid)
// ---------------------------------------------------------------------------

/// Insert a new collection and return it with its SQLite rowid as the integer id.
pub fn create_item(conn: &Connection, name: &str, description: &str) -> Result<CollectionRecord> {
    let uuid = Uuid::new_v4().to_string();
    let ts = Utc::now().to_rfc3339();

    conn.execute(
        "INSERT INTO collections (id, name, description, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?4)",
        rusqlite::params![uuid, name, description, ts],
    )?;

    let rowid = conn.last_insert_rowid();
    let record = conn.query_row(
        "SELECT rowid, name, description FROM collections WHERE rowid = ?1",
        rusqlite::params![rowid],
        |row| {
            Ok(CollectionRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
            })
        },
    )?;
    Ok(record)
}

/// Update the name and description of the collection identified by its rowid.
///
/// Returns `Some(CollectionRecord)` if the row existed, `None` otherwise.
pub fn update_item(
    conn: &Connection,
    id: i64,
    name: &str,
    description: &str,
) -> Result<Option<CollectionRecord>> {
    let ts = Utc::now().to_rfc3339();
    let rows = conn.execute(
        "UPDATE collections SET name=?1, description=?2, updated_at=?3 WHERE rowid=?4",
        rusqlite::params![name, description, ts, id],
    )?;

    if rows == 0 {
        return Ok(None);
    }

    let record = conn.query_row(
        "SELECT rowid, name, description FROM collections WHERE rowid = ?1",
        rusqlite::params![id],
        |row| {
            Ok(CollectionRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
            })
        },
    )?;
    Ok(Some(record))
}

/// List all collections, returned with their SQLite rowid as the integer id.
///
/// Ordered by name ascending for stable display.
pub fn list_items(conn: &Connection) -> Result<Vec<CollectionRecord>> {
    let mut stmt =
        conn.prepare("SELECT rowid, name, description FROM collections ORDER BY name ASC")?;
    let rows = stmt.query_map([], |row| {
        Ok(CollectionRecord {
            id: row.get(0)?,
            name: row.get(1)?,
            description: row.get(2)?,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Delete the collection identified by its rowid.
///
/// Returns `true` if a row was removed, `false` if no such collection existed.
pub fn delete_item(conn: &Connection, id: i64) -> Result<bool> {
    let rows = conn.execute(
        "DELETE FROM collections WHERE rowid = ?1",
        rusqlite::params![id],
    )?;
    Ok(rows > 0)
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Insert a new collection and return the freshly fetched row.
pub fn create(db: &Database, name: &str, description: &str) -> Result<Collection> {
    let id = Uuid::new_v4().to_string();
    let ts = now_iso();

    db.conn().execute(
        "INSERT INTO collections (id, name, description, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?4)",
        rusqlite::params![id, name, description, ts],
    )?;

    let col =
        get_by_id(db, &id)?.ok_or_else(|| anyhow::anyhow!("collection not found after insert"))?;
    Ok(col)
}

/// Return the collection with the given id, or `None` if it does not exist.
pub fn get_by_id(db: &Database, id: &str) -> Result<Option<Collection>> {
    let conn = db.conn();
    let mut stmt = conn.prepare(
        "SELECT id, name, description, created_at, updated_at \
         FROM collections WHERE id = ?1",
    )?;

    let result = stmt
        .query_row(rusqlite::params![id], row_to_collection)
        .optional()?;

    Ok(result)
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn row_to_collection(row: &Row) -> rusqlite::Result<Collection> {
    let created_at_s: String = row.get(3)?;
    let updated_at_s: String = row.get(4)?;

    Ok(Collection {
        id: row.get(0)?,
        name: row.get(1)?,
        description: row.get(2)?,
        created_at: parse_ts(&created_at_s)?,
        updated_at: parse_ts(&updated_at_s)?,
    })
}

fn parse_ts(s: &str) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })
}

fn now_iso() -> String {
    Utc::now().to_rfc3339()
}

/// Return all collections ordered by name ascending.
pub fn list(db: &Database) -> Result<Vec<Collection>> {
    let conn = db.conn();
    let mut stmt = conn.prepare(
        "SELECT id, name, description, created_at, updated_at \
         FROM collections ORDER BY name ASC",
    )?;

    let rows = stmt.query_map([], row_to_collection)?;
    let mut cols = Vec::new();
    for row in rows {
        cols.push(row?);
    }
    Ok(cols)
}

/// Update the name, description, and updated_at of an existing collection.
/// Returns an error if no collection with the given id exists.
pub fn update(db: &Database, id: &str, name: &str, description: &str) -> Result<()> {
    let ts = now_iso();

    let rows = db.conn().execute(
        "UPDATE collections SET name=?1, description=?2, updated_at=?3 WHERE id=?4",
        rusqlite::params![name, description, ts, id],
    )?;

    if rows == 0 {
        anyhow::bail!("collection not found: {id}");
    }
    Ok(())
}

/// Delete the collection with the given id.
/// Returns true if a row was removed, false if no such collection existed.
pub fn delete(db: &Database, id: &str) -> Result<bool> {
    let rows = db
        .conn()
        .execute("DELETE FROM collections WHERE id=?1", rusqlite::params![id])?;
    Ok(rows > 0)
}

/// Return `true` if a collection with the given SQLite `rowid` exists.
///
/// Used by the admin crawl-targets endpoint to validate `collection_id`
/// before inserting a new crawl target.
pub fn exists(conn: &Connection, id: i64) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM collections WHERE rowid = ?1",
        rusqlite::params![id],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}
