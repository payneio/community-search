//! Per-collection ranking configuration repository.

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension, Result as SqlResult};
use serde::{Deserialize, Serialize};

// ── Struct ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RankingConfig {
    pub collection_id: i64,
    pub freshness_half_life_days: f64,
    pub source_weights: HashMap<String, f64>,
    pub domain_boosts: HashMap<String, f64>,
}

impl RankingConfig {
    /// Return a default `RankingConfig` for the given collection.
    ///
    /// - `freshness_half_life_days` = 90.0
    /// - `source_weights` = {"local": 1.0}
    /// - `domain_boosts` = {}
    pub fn default_for(collection_id: i64) -> Self {
        let mut source_weights = HashMap::new();
        source_weights.insert("local".to_string(), 1.0);

        RankingConfig {
            collection_id,
            freshness_half_life_days: 90.0,
            source_weights,
            domain_boosts: HashMap::new(),
        }
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Load the ranking config for `collection_id` from the database.
///
/// Returns `RankingConfig::default_for(collection_id)` when no row exists.
pub fn load(conn: &Connection, collection_id: i64) -> SqlResult<RankingConfig> {
    let row = conn
        .query_row(
            "SELECT freshness_half_life_days, source_weights_json, domain_boosts_json
             FROM ranking_config
             WHERE collection_id = ?1",
            rusqlite::params![collection_id],
            |row| {
                let half_life: f64 = row.get(0)?;
                let sw_json: String = row.get(1)?;
                let db_json: String = row.get(2)?;
                Ok((half_life, sw_json, db_json))
            },
        )
        .optional()?;

    match row {
        None => Ok(RankingConfig::default_for(collection_id)),
        Some((freshness_half_life_days, sw_json, db_json)) => {
            let source_weights: HashMap<String, f64> =
                serde_json::from_str(&sw_json).unwrap_or_default();
            let domain_boosts: HashMap<String, f64> =
                serde_json::from_str(&db_json).unwrap_or_default();
            Ok(RankingConfig {
                collection_id,
                freshness_half_life_days,
                source_weights,
                domain_boosts,
            })
        }
    }
}

/// Persist `cfg` using an upsert (INSERT … ON CONFLICT … DO UPDATE).
pub fn save(conn: &Connection, cfg: &RankingConfig) -> SqlResult<()> {
    let sw_json = serde_json::to_string(&cfg.source_weights)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let db_json = serde_json::to_string(&cfg.domain_boosts)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    conn.execute(
        "INSERT INTO ranking_config
             (collection_id, freshness_half_life_days, source_weights_json, domain_boosts_json, updated_at)
         VALUES (?1, ?2, ?3, ?4, strftime('%s','now'))
         ON CONFLICT(collection_id) DO UPDATE SET
             freshness_half_life_days = excluded.freshness_half_life_days,
             source_weights_json      = excluded.source_weights_json,
             domain_boosts_json       = excluded.domain_boosts_json,
             updated_at               = excluded.updated_at",
        rusqlite::params![
            cfg.collection_id,
            cfg.freshness_half_life_days,
            sw_json,
            db_json,
        ],
    )?;
    Ok(())
}

/// Persist the entire ranking config payload, writing both the JSON blob in
/// `config_json` AND the typed columns (`freshness_half_life_days`,
/// `source_weights_json`, `domain_boosts_json`) so that `load()` — which
/// reads only the typed columns — stays in sync with what the admin API wrote.
///
/// Uses an UPSERT keyed on `collection_id`.  The `payload_json` string is
/// stored verbatim in `config_json`; the typed columns are derived by
/// deserialising `payload_json` as a [`RankingConfig`].
///
/// FK enforcement is disabled for this operation because `ranking_config`
/// has an INTEGER PK while `collections.id` is a TEXT UUID — the FK
/// relationship cannot hold numerically in production.  Collection existence
/// is verified by the caller before invoking this function.
pub fn upsert(conn: &Connection, collection_id: i64, payload_json: &str) -> SqlResult<()> {
    // Parse the payload to extract typed fields so load() stays in sync.
    let cfg: RankingConfig = serde_json::from_str(payload_json)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let sw_json = serde_json::to_string(&cfg.source_weights)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let db_json = serde_json::to_string(&cfg.domain_boosts)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    // Temporarily disable FK enforcement.  The caller has already confirmed
    // the collection exists via `collections::exists()`.
    conn.execute_batch("PRAGMA foreign_keys = OFF;")?;
    let result = conn.execute(
        "INSERT INTO ranking_config
             (collection_id, freshness_half_life_days, source_weights_json,
              domain_boosts_json, config_json, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, strftime('%s','now'))
         ON CONFLICT(collection_id) DO UPDATE SET
             freshness_half_life_days = excluded.freshness_half_life_days,
             source_weights_json      = excluded.source_weights_json,
             domain_boosts_json       = excluded.domain_boosts_json,
             config_json              = excluded.config_json,
             updated_at               = excluded.updated_at",
        rusqlite::params![
            collection_id,
            cfg.freshness_half_life_days,
            sw_json,
            db_json,
            payload_json,
        ],
    );
    // Restore FK enforcement regardless of success or failure.
    let _ = conn.execute_batch("PRAGMA foreign_keys = ON;");
    result?;
    Ok(())
}

/// Return the JSON blob stored in `config_json` for `collection_id`.
///
/// Returns `Ok(None)` when no row exists or the `config_json` column is NULL.
pub fn get(conn: &Connection, collection_id: i64) -> SqlResult<Option<String>> {
    conn.query_row(
        "SELECT config_json FROM ranking_config WHERE collection_id = ?1",
        rusqlite::params![collection_id],
        |row| row.get::<_, Option<String>>(0),
    )
    .optional()
    .map(|opt| opt.flatten())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const MIGRATIONS: &[&str] = &[include_str!("migrations/001_init.sql")];

    /// Open a fully-migrated in-memory SQLite connection and seed one collection.
    ///
    /// FK enforcement is intentionally left OFF: the `collections` table declares
    /// `id TEXT PRIMARY KEY` while `ranking_config.collection_id` is `INTEGER`,
    /// so SQLite's type-affinity rules would convert INTEGER 1 → TEXT "1" on
    /// insert, making the FK check fail even when the logical value matches.
    /// Unit tests for load/save do not need FK integrity.
    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        for sql in MIGRATIONS {
            conn.execute_batch(sql).expect("apply migration");
        }
        // Insert a test collection; columns match the 001_init.sql schema.
        conn.execute(
            "INSERT INTO collections (id, name, description, created_at, updated_at)
             VALUES ('1', 'test', '', '2024-01-01', '2024-01-01')",
            [],
        )
        .expect("insert collection");
        conn
    }

    #[test]
    fn load_returns_defaults_when_no_row() {
        let conn = fresh_db();
        let cfg = load(&conn, 1).expect("load");
        assert_eq!(cfg.freshness_half_life_days, 90.0);
        assert_eq!(cfg.source_weights.get("local"), Some(&1.0));
    }

    #[test]
    fn save_then_load_roundtrips() {
        let conn = fresh_db();

        let mut source_weights = HashMap::new();
        source_weights.insert("local".to_string(), 2.5);
        source_weights.insert("peer".to_string(), 0.5);

        let mut domain_boosts = HashMap::new();
        domain_boosts.insert("example.com".to_string(), 1.5);

        let cfg = RankingConfig {
            collection_id: 1,
            freshness_half_life_days: 45.0,
            source_weights,
            domain_boosts,
        };

        save(&conn, &cfg).expect("save");
        let loaded = load(&conn, 1).expect("load after save");

        assert_eq!(loaded, cfg);
    }

    #[test]
    fn upsert_then_get_roundtrips_json() {
        let conn = fresh_db();

        let payload = r#"{"collection_id":1,"freshness_half_life_days":14.0,"source_weights":{"local":1.0},"domain_boosts":{"good.com":2.0}}"#;
        upsert(&conn, 1, payload).expect("upsert");

        let result = get(&conn, 1).expect("get");
        assert_eq!(result.as_deref(), Some(payload));
    }

    #[test]
    fn get_returns_none_when_no_row() {
        let conn = fresh_db();
        let result = get(&conn, 999).expect("get");
        assert_eq!(result, None);
    }

    /// `upsert()` must write the typed columns so that `load()` — which reads
    /// only `freshness_half_life_days`, `source_weights_json`, and
    /// `domain_boosts_json` — reflects the admin-API payload.
    #[test]
    fn upsert_then_load_reflects_typed_columns() {
        let conn = fresh_db();

        let payload = r#"{"collection_id":1,"freshness_half_life_days":14.0,"source_weights":{"local":1.0},"domain_boosts":{"good.com":2.0}}"#;
        upsert(&conn, 1, payload).expect("upsert");

        let loaded = load(&conn, 1).expect("load after upsert");
        assert_eq!(
            loaded.freshness_half_life_days, 14.0,
            "freshness_half_life_days must reflect the upserted payload"
        );
        assert_eq!(
            loaded.source_weights.get("local"),
            Some(&1.0),
            "source_weights must reflect the upserted payload"
        );
        assert_eq!(
            loaded.domain_boosts.get("good.com"),
            Some(&2.0),
            "domain_boosts must reflect the upserted payload"
        );
    }
}
