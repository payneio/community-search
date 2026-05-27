pub mod collections;
pub mod crawl_targets;
pub mod crawled_pages;
mod migrations;
pub mod models;
pub mod outlink_hosts;
pub mod ranking_config;
pub mod settings;

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use anyhow::Result;
use rusqlite::Connection;

/// Row struct for the crawled_pages table.
#[derive(Debug, Clone)]
pub struct CrawledPage {
    pub id: i64,
    pub collection_id: i64,
    pub crawl_target_id: i64,
    pub url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_hash: Option<String>,
    pub last_status: Option<i64>,
    pub last_crawled_at: i64,
}

/// Apply all migrations in order.
///
/// Most migration statements use `CREATE TABLE IF NOT EXISTS` and are
/// inherently idempotent.  The only exception is `ALTER TABLE … ADD COLUMN`,
/// which SQLite rejects if the column already exists.  We ignore the
/// resulting "duplicate column name" error so that opening an existing
/// database a second time (as tested by `opens_existing_database_idempotently`)
/// succeeds cleanly.
pub fn run_migrations(conn: &Connection) -> Result<()> {
    for sql in migrations::MIGRATIONS {
        if let Err(e) = conn.execute_batch(sql) {
            // A "duplicate column name" error means an ALTER TABLE ADD COLUMN
            // was already applied on a previous open.  Treat it as a no-op so
            // migrations remain idempotent on re-opens.
            if !e.to_string().contains("duplicate column name") {
                return Err(e.into());
            }
        }
    }
    Ok(())
}

pub struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        // Create parent directories if a non-empty parent exists
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let conn = Connection::open(path)?;

        // Apply PRAGMAs: WAL journal mode, foreign keys ON, synchronous NORMAL
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA synchronous = NORMAL;",
        )?;

        // Run all migrations (includes the Phase 1 schema from 001_init.sql).
        // This supersedes the legacy `schema::apply` call so that tables added
        // in later migrations (e.g. `app_config` from 004) are available.
        run_migrations(&conn)?;

        Ok(Database {
            conn: Mutex::new(conn),
        })
    }

    /// Open a transient in-memory database with all migrations applied.
    /// Intended for tests and tooling that need a fully initialised schema
    /// without a persistent file.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;

        conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        run_migrations(&conn)?;

        Ok(Database {
            conn: Mutex::new(conn),
        })
    }

    pub fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().expect("database mutex poisoned")
    }

    /// Alias for [`conn`][Self::conn].
    pub fn connection(&self) -> MutexGuard<'_, Connection> {
        self.conn()
    }
}

/// Open a transient in-memory [`Database`] with all migrations applied.
///
/// Thin free-function wrapper around [`Database::open_in_memory`] for
/// ergonomic use in integration tests:
/// ```rust,ignore
/// use community_search::db::open_in_memory;
/// let db = open_in_memory().unwrap();
/// ```
pub fn open_in_memory() -> Result<Database> {
    Database::open_in_memory()
}

#[cfg(test)]
mod ranking_config_tests {
    use super::*;

    #[test]
    fn ranking_config_table_exists_after_migrations() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();

        // Verify the table exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='ranking_config'",
                [],
                |row| row.get(0),
            )
            .expect("query sqlite_master");
        assert!(
            count > 0,
            "ranking_config table should exist after migrations"
        );

        // Verify all required columns are present
        let required_columns = [
            "collection_id",
            "freshness_half_life_days",
            "source_weights_json",
            "domain_boosts_json",
            "updated_at",
        ];
        for col in &required_columns {
            let col_count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('ranking_config') WHERE name=?1",
                    [col],
                    |row| row.get(0),
                )
                .expect("query pragma_table_info");
            assert!(
                col_count > 0,
                "column '{}' should exist in ranking_config",
                col
            );
        }
    }
}

#[cfg(test)]
mod crawled_pages_tests {
    use super::*;

    #[test]
    fn crawled_pages_table_exists_after_migration() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='crawled_pages'",
                [],
                |row| row.get(0),
            )
            .expect("query sqlite_master");
        assert!(
            count > 0,
            "crawled_pages table should exist after migration"
        );
    }
}

#[cfg(test)]
mod migration_004_tests {
    use super::*;

    #[test]
    fn migration_004_creates_admin_token_row_in_app_config() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM app_config WHERE key = 'admin_token'",
                [],
                |row| row.get(0),
            )
            .expect("query app_config for admin_token row");
        assert_eq!(
            count, 1,
            "admin_token row should exist in app_config after migrations"
        );
    }

    #[test]
    fn migration_004_creates_rate_limit_state_table() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='rate_limit_state'",
                [],
                |row| row.get(0),
            )
            .expect("query sqlite_master for rate_limit_state");
        assert!(
            count > 0,
            "rate_limit_state table should exist after migrations"
        );

        // Verify the new ip-keyed schema (not the old kind/key composite key)
        let ip_col_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('rate_limit_state') WHERE name='ip'",
                [],
                |row| row.get(0),
            )
            .expect("query pragma_table_info for ip column");
        assert!(
            ip_col_count > 0,
            "rate_limit_state should have 'ip' column after migration 004"
        );
    }

    #[test]
    fn migration_004_creates_auth_attempts_table() {
        let db = Database::open_in_memory().expect("open in-memory db");
        let conn = db.conn();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='auth_attempts'",
                [],
                |row| row.get(0),
            )
            .expect("query sqlite_master for auth_attempts");
        assert!(
            count > 0,
            "auth_attempts table should exist after migrations"
        );
    }
}
