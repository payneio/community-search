//! Repository for the `auth_attempts` table.
//!
//! Tracks per-IP failed authentication attempts and enforces a 15-minute
//! lockout after [`MAX_FAILURES`] consecutive failures.
//!
//! The lockout is checked **before** token validation so that even a correct
//! token is rejected while an IP is locked out.

use anyhow::Result;

use crate::api::public::SharedDb;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Number of consecutive failures before an IP is locked out.
pub const MAX_FAILURES: u32 = 5;

/// Lockout duration in seconds (15 minutes).
pub const LOCKOUT_SECONDS: i64 = 15 * 60; // 900

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Current auth-attempt state for a single IP address.
#[derive(Debug, Clone, PartialEq)]
pub struct AttemptState {
    /// Number of consecutive failed authentication attempts.
    pub failed_count: u32,
    /// Unix timestamp (seconds) until which the IP is locked out.
    /// `0` means no active lockout.
    pub lockout_until: i64,
}

// ---------------------------------------------------------------------------
// Repository functions
// ---------------------------------------------------------------------------

/// Return the current [`AttemptState`] for `ip`.
///
/// If no row exists, returns `AttemptState { failed_count: 0, lockout_until: 0 }`.
pub fn current_state(db: &SharedDb, ip: &str, _now: i64) -> Result<AttemptState> {
    let conn = db.lock().expect("db mutex poisoned");
    match conn.query_row(
        "SELECT failed_count, lockout_until FROM auth_attempts WHERE ip = ?1",
        rusqlite::params![ip],
        |row| {
            Ok(AttemptState {
                failed_count: row.get::<_, u32>(0)?,
                lockout_until: row.get::<_, i64>(1)?,
            })
        },
    ) {
        Ok(state) => Ok(state),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(AttemptState {
            failed_count: 0,
            lockout_until: 0,
        }),
        Err(e) => Err(e.into()),
    }
}

/// Record a failed authentication attempt for `ip`.
///
/// Uses an UPSERT that:
/// - Increments `failed_count`
/// - Sets `lockout_until = now + LOCKOUT_SECONDS` when `failed_count >= MAX_FAILURES`
///
/// Returns the new [`AttemptState`].
pub fn record_failure(db: &SharedDb, ip: &str, now: i64) -> Result<AttemptState> {
    let lockout_at = now + LOCKOUT_SECONDS;
    let max = MAX_FAILURES as i64;

    {
        let conn = db.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO auth_attempts (ip, failed_count, lockout_until, last_attempt_at)
             VALUES (?1, 1, CASE WHEN 1 >= ?2 THEN ?3 ELSE 0 END, ?4)
             ON CONFLICT(ip) DO UPDATE SET
               failed_count     = failed_count + 1,
               lockout_until    = CASE WHEN failed_count + 1 >= ?2 THEN ?3
                                       ELSE lockout_until END,
               last_attempt_at  = ?4",
            rusqlite::params![ip, max, lockout_at, now],
        )?;

        let state = conn.query_row(
            "SELECT failed_count, lockout_until FROM auth_attempts WHERE ip = ?1",
            rusqlite::params![ip],
            |row| {
                Ok(AttemptState {
                    failed_count: row.get::<_, u32>(0)?,
                    lockout_until: row.get::<_, i64>(1)?,
                })
            },
        )?;
        Ok(state)
    }
}

/// Record a successful authentication for `ip`.
///
/// Deletes the `auth_attempts` row, resetting the failed count to zero.
pub fn record_success(db: &SharedDb, ip: &str) -> Result<()> {
    let conn = db.lock().expect("db mutex poisoned");
    conn.execute(
        "DELETE FROM auth_attempts WHERE ip = ?1",
        rusqlite::params![ip],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::sync::{Arc, Mutex};

    /// Build an in-memory DB with just the `auth_attempts` table.
    fn test_db() -> SharedDb {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS auth_attempts (
                ip              TEXT    PRIMARY KEY,
                failed_count    INTEGER DEFAULT 0,
                lockout_until   INTEGER DEFAULT 0,
                last_attempt_at INTEGER DEFAULT 0
            );",
        )
        .expect("create auth_attempts table");
        Arc::new(Mutex::new(conn))
    }

    /// Five consecutive failures must set `lockout_until` to `now + LOCKOUT_SECONDS`.
    #[test]
    fn five_failures_set_lockout() {
        let db = test_db();
        let now: i64 = 1_000_000;

        let mut state = AttemptState {
            failed_count: 0,
            lockout_until: 0,
        };
        for _ in 0..5 {
            state = record_failure(&db, "1.2.3.4", now).unwrap();
        }

        assert_eq!(
            state.failed_count, 5,
            "failed_count must be 5 after 5 failures"
        );
        assert!(
            state.lockout_until > now,
            "lockout_until must be in the future after 5 failures, got {}",
            state.lockout_until
        );
        assert_eq!(
            state.lockout_until,
            now + LOCKOUT_SECONDS,
            "lockout_until must equal now + LOCKOUT_SECONDS"
        );
    }

    /// Four failures must NOT yet trigger a lockout.
    #[test]
    fn four_failures_no_lockout() {
        let db = test_db();
        let now: i64 = 1_000_000;

        let mut state = AttemptState {
            failed_count: 0,
            lockout_until: 0,
        };
        for _ in 0..4 {
            state = record_failure(&db, "1.2.3.4", now).unwrap();
        }

        assert_eq!(state.failed_count, 4);
        assert_eq!(
            state.lockout_until, 0,
            "lockout_until must still be 0 after only 4 failures"
        );
    }

    /// `record_success` must delete the row, resetting both counters.
    #[test]
    fn success_clears_attempts() {
        let db = test_db();
        let now: i64 = 1_000_000;

        // Record 3 failures.
        for _ in 0..3 {
            record_failure(&db, "1.2.3.4", now).unwrap();
        }

        // A successful auth must clear the state.
        record_success(&db, "1.2.3.4").unwrap();

        let state = current_state(&db, "1.2.3.4", now).unwrap();
        assert_eq!(
            state.failed_count, 0,
            "failed_count must be 0 after record_success"
        );
        assert_eq!(
            state.lockout_until, 0,
            "lockout_until must be 0 after record_success"
        );
    }

    /// `current_state` for an unknown IP must return zeros (no error).
    #[test]
    fn unknown_ip_returns_zero_state() {
        let db = test_db();
        let now: i64 = 1_000_000;
        let state = current_state(&db, "9.9.9.9", now).unwrap();
        assert_eq!(state.failed_count, 0);
        assert_eq!(state.lockout_until, 0);
    }
}
