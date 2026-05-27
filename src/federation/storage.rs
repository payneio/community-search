//! Peer storage: thin wrappers over node_peers / collection_peers tables.

use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePeer {
    pub id: i64,
    pub url: String,
    pub name: Option<String>,
    pub enabled: bool,
    pub consecutive_failures: i64,
    pub last_response_ms: Option<i64>,
    pub last_checked_at: Option<i64>,
    pub disabled_at: Option<i64>,
    pub created_at: i64,
}

/// Insert a new node peer and return its auto-assigned row ID.
pub fn insert_node_peer(conn: &Connection, url: &str, name: Option<&str>) -> Result<i64> {
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        "INSERT INTO node_peers (url, name, enabled, consecutive_failures, created_at)
         VALUES (?1, ?2, 1, 0, ?3)",
        rusqlite::params![url, name, now],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Delete a node peer by its row ID.
pub fn delete_node_peer(conn: &Connection, id: i64) -> Result<()> {
    conn.execute(
        "DELETE FROM node_peers WHERE id = ?1",
        rusqlite::params![id],
    )?;
    Ok(())
}

/// Enable or disable a node peer.
///
/// When `enabled` is `true`: sets `enabled=1`, clears `disabled_at`, and resets
/// `consecutive_failures` to 0.
/// When `enabled` is `false`: sets `enabled=0` and records `disabled_at` as now.
pub fn set_node_peer_enabled(conn: &Connection, id: i64, enabled: bool) -> Result<()> {
    if enabled {
        conn.execute(
            "UPDATE node_peers SET enabled = 1, disabled_at = NULL, consecutive_failures = 0 WHERE id = ?1",
            rusqlite::params![id],
        )?;
    } else {
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "UPDATE node_peers SET enabled = 0, disabled_at = ?1 WHERE id = ?2",
            rusqlite::params![now, id],
        )?;
    }
    Ok(())
}

/// Fetch a single node peer by its row ID, returning `None` if not found.
pub fn get_node_peer(conn: &Connection, id: i64) -> Result<Option<NodePeer>> {
    let mut stmt = conn.prepare(
        "SELECT id, url, name, enabled, consecutive_failures, last_response_ms,
                last_checked_at, disabled_at, created_at
         FROM node_peers WHERE id = ?1",
    )?;
    let mut rows = stmt.query_map(rusqlite::params![id], |r| {
        Ok(NodePeer {
            id: r.get(0)?,
            url: r.get(1)?,
            name: r.get(2)?,
            enabled: r.get::<_, i64>(3)? != 0,
            consecutive_failures: r.get(4)?,
            last_response_ms: r.get(5)?,
            last_checked_at: r.get(6)?,
            disabled_at: r.get(7)?,
            created_at: r.get(8)?,
        })
    })?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// CollectionPeer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionPeer {
    pub id: i64,
    pub local_collection: String,
    pub node_peer_id: i64,
    pub remote_collection: String,
    pub source_weight: f32,
    pub enabled: bool,
    pub created_at: i64,
}

/// Insert a new collection peer mapping and return its auto-assigned row ID.
pub fn insert_collection_peer(
    conn: &Connection,
    local_collection: &str,
    node_peer_id: i64,
    remote_collection: &str,
    source_weight: f32,
) -> Result<i64> {
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        "INSERT INTO collection_peers
             (local_collection, node_peer_id, remote_collection, source_weight, enabled, created_at)
         VALUES (?1, ?2, ?3, ?4, 1, ?5)",
        rusqlite::params![
            local_collection,
            node_peer_id,
            remote_collection,
            source_weight,
            now
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// List collection peers, optionally filtered by `local_collection`, ordered by
/// creation time ascending.
pub fn list_collection_peers(
    conn: &Connection,
    local_filter: Option<&str>,
) -> Result<Vec<CollectionPeer>> {
    let map_row = |r: &rusqlite::Row<'_>| {
        Ok(CollectionPeer {
            id: r.get(0)?,
            local_collection: r.get(1)?,
            node_peer_id: r.get(2)?,
            remote_collection: r.get(3)?,
            source_weight: r.get(4)?,
            enabled: r.get::<_, i64>(5)? != 0,
            created_at: r.get(6)?,
        })
    };

    if let Some(filter) = local_filter {
        let mut stmt = conn.prepare(
            "SELECT id, local_collection, node_peer_id, remote_collection, source_weight,
                    enabled, created_at
             FROM collection_peers
             WHERE local_collection = ?1
             ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map(rusqlite::params![filter], map_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    } else {
        let mut stmt = conn.prepare(
            "SELECT id, local_collection, node_peer_id, remote_collection, source_weight,
                    enabled, created_at
             FROM collection_peers
             ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], map_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

/// Delete a collection peer by its row ID.
pub fn delete_collection_peer(conn: &Connection, id: i64) -> Result<()> {
    conn.execute(
        "DELETE FROM collection_peers WHERE id = ?1",
        rusqlite::params![id],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------

/// Return all node peers ordered by creation time (oldest first).
pub fn list_node_peers(conn: &Connection) -> Result<Vec<NodePeer>> {
    let mut stmt = conn.prepare(
        "SELECT id, url, name, enabled, consecutive_failures, last_response_ms,
                last_checked_at, disabled_at, created_at
         FROM node_peers ORDER BY created_at ASC",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(NodePeer {
            id: r.get(0)?,
            url: r.get(1)?,
            name: r.get(2)?,
            enabled: r.get::<_, i64>(3)? != 0,
            consecutive_failures: r.get(4)?,
            last_response_ms: r.get(5)?,
            last_checked_at: r.get(6)?,
            disabled_at: r.get(7)?,
            created_at: r.get(8)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        crate::db::run_migrations(&conn).unwrap();
        conn
    }

    #[test]
    fn insert_and_list_node_peer() {
        let conn = fresh_db();
        let id = insert_node_peer(&conn, "https://peer.example", Some("Peer A")).unwrap();
        let peers = list_node_peers(&conn).unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, id);
        assert_eq!(peers[0].url, "https://peer.example");
        assert!(peers[0].enabled);
    }

    #[test]
    fn delete_node_peer_removes_row() {
        let conn = fresh_db();
        let id = insert_node_peer(&conn, "https://peer.example", None).unwrap();
        assert_eq!(list_node_peers(&conn).unwrap().len(), 1);

        delete_node_peer(&conn, id).unwrap();

        assert_eq!(list_node_peers(&conn).unwrap().len(), 0);
    }

    #[test]
    fn set_enabled_toggles_flag() {
        let conn = fresh_db();
        let id = insert_node_peer(&conn, "https://peer.example", None).unwrap();

        // Disable: should set disabled_at and leave enabled=false
        set_node_peer_enabled(&conn, id, false).unwrap();
        let peer = get_node_peer(&conn, id)
            .unwrap()
            .expect("peer should exist");
        assert!(!peer.enabled);
        assert!(
            peer.disabled_at.is_some(),
            "disabled_at should be set when disabling"
        );

        // Re-enable: should clear disabled_at and reset consecutive_failures
        // First bump consecutive_failures to check it gets reset
        conn.execute(
            "UPDATE node_peers SET consecutive_failures = 5 WHERE id = ?1",
            rusqlite::params![id],
        )
        .unwrap();

        set_node_peer_enabled(&conn, id, true).unwrap();
        let peer = get_node_peer(&conn, id)
            .unwrap()
            .expect("peer should exist");
        assert!(peer.enabled);
        assert!(
            peer.disabled_at.is_none(),
            "disabled_at should be cleared when re-enabling"
        );
        assert_eq!(
            peer.consecutive_failures, 0,
            "consecutive_failures should be reset to 0"
        );
    }

    #[test]
    fn get_node_peer_returns_none_for_missing() {
        let conn = fresh_db();
        let result = get_node_peer(&conn, 9999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn insert_and_list_collection_peers() {
        let conn = fresh_db();
        let node_id = insert_node_peer(&conn, "https://peer.example", Some("Peer A")).unwrap();

        let cp_id = insert_collection_peer(&conn, "local-col", node_id, "remote-col", 1.0).unwrap();

        // List with no filter returns all
        let all = list_collection_peers(&conn, None).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, cp_id);
        assert_eq!(all[0].local_collection, "local-col");
        assert_eq!(all[0].node_peer_id, node_id);
        assert_eq!(all[0].remote_collection, "remote-col");
        assert_eq!(all[0].source_weight, 1.0);
        assert!(all[0].enabled);

        // List with matching local filter returns the row
        let filtered = list_collection_peers(&conn, Some("local-col")).unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, cp_id);

        // List with non-matching filter returns empty
        let empty = list_collection_peers(&conn, Some("other-col")).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn cascade_delete_when_node_removed() {
        let conn = fresh_db();
        // SQLite requires explicit PRAGMA to enforce FK constraints
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        let node_id = insert_node_peer(&conn, "https://peer.example", None).unwrap();
        insert_collection_peer(&conn, "local-col", node_id, "remote-col", 1.0).unwrap();

        // Verify collection peer exists
        let before = list_collection_peers(&conn, None).unwrap();
        assert_eq!(before.len(), 1);

        // Delete the node; cascade should remove the collection peer
        delete_node_peer(&conn, node_id).unwrap();

        let after = list_collection_peers(&conn, None).unwrap();
        assert!(
            after.is_empty(),
            "collection_peers should be empty after node deletion (FK cascade)"
        );
    }
}
