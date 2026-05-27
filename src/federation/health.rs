//! Peer health tracking and auto-disable/re-enable.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use rusqlite::Connection;

use crate::federation::peer::PeerClient;
use crate::federation::storage::{list_node_peers, set_node_peer_enabled};

/// Number of consecutive failures before a peer is automatically disabled.
pub const FAILURE_THRESHOLD: i64 = 5;

/// Record a health-check result for a node peer.
///
/// On success: resets `consecutive_failures` to 0 and writes
/// `last_response_ms` and `last_checked_at`.
///
/// On failure: increments `consecutive_failures` and updates
/// `last_checked_at`.  If the failure count reaches [`FAILURE_THRESHOLD`]
/// and the peer is currently enabled, the peer is disabled (`enabled=0`,
/// `disabled_at=now`) and a `peer_disabled` admin notification is inserted.
pub fn record_result(
    conn: &Connection,
    node_peer_id: i64,
    success: bool,
    response_ms: Option<i64>,
) -> Result<()> {
    let now = chrono::Utc::now().timestamp();

    if success {
        conn.execute(
            "UPDATE node_peers
             SET consecutive_failures = 0,
                 last_response_ms     = ?1,
                 last_checked_at      = ?2
             WHERE id = ?3",
            rusqlite::params![response_ms, now, node_peer_id],
        )?;
    } else {
        conn.execute(
            "UPDATE node_peers
             SET consecutive_failures = consecutive_failures + 1,
                 last_checked_at      = ?1
             WHERE id = ?2",
            rusqlite::params![now, node_peer_id],
        )?;

        // Check whether the threshold is now reached for an enabled peer.
        let (consecutive_failures, enabled): (i64, i64) = conn.query_row(
            "SELECT consecutive_failures, enabled FROM node_peers WHERE id = ?1",
            rusqlite::params![node_peer_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;

        if consecutive_failures >= FAILURE_THRESHOLD && enabled != 0 {
            conn.execute(
                "UPDATE node_peers
                 SET enabled     = 0,
                     disabled_at = ?1
                 WHERE id = ?2",
                rusqlite::params![now, node_peer_id],
            )?;

            let message = format!(
                "Peer {node_peer_id} auto-disabled after {consecutive_failures} consecutive failures"
            );
            insert_notification(conn, "peer_disabled", &message)?;
        }
    }

    Ok(())
}

/// Insert an admin notification row.
pub fn insert_notification(conn: &Connection, kind: &str, message: &str) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        "INSERT INTO admin_notifications (kind, message, created_at) VALUES (?1, ?2, ?3)",
        rusqlite::params![kind, message, now],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Background health checker — re-enable recovered peers
// ---------------------------------------------------------------------------

/// Summary of a single health-check sweep over all currently-disabled peers.
#[derive(Debug, Default, PartialEq)]
pub struct HealthCheckOutcome {
    /// IDs of peers that were re-enabled because their health check passed.
    pub re_enabled: Vec<i64>,
    /// IDs of peers whose health check still failed; they remain disabled.
    pub still_down: Vec<i64>,
}

/// Run one health-check sweep: for every disabled peer, issue an HTTP probe.
/// On success the peer is re-enabled and a `peer_re_enabled` notification is
/// recorded.  On failure the peer stays disabled.
///
/// Returns a [`HealthCheckOutcome`] summarising the results.
pub async fn run_health_check_once(
    conn: &Connection,
    client: &dyn PeerClient,
) -> Result<HealthCheckOutcome> {
    let disabled_peers: Vec<_> = list_node_peers(conn)?
        .into_iter()
        .filter(|p| !p.enabled)
        .collect();

    let mut outcome = HealthCheckOutcome::default();

    for p in disabled_peers {
        let ok = client.health_check(&p.url).await.unwrap_or(false);
        if ok {
            set_node_peer_enabled(conn, p.id, true)?;
            let msg = format!("Peer {} re-enabled after successful health check", p.id);
            insert_notification(conn, "peer_re_enabled", &msg)?;
            outcome.re_enabled.push(p.id);
        } else {
            outcome.still_down.push(p.id);
        }
    }

    Ok(outcome)
}

/// Spawn a background task that calls [`run_health_check_once`] on every tick.
///
/// `MissedTickBehavior::Skip` is used so that a slow sweep does not cause
/// back-to-back ticks to pile up.  Errors are logged with `tracing::warn` but
/// do not terminate the task.
///
/// Returns the [`tokio::task::JoinHandle`] so callers can abort or await it.
pub fn spawn_health_check_task(
    db: Arc<Mutex<Connection>>,
    client: Arc<dyn PeerClient>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            // Collect disabled peers while holding the lock, then release it
            // before any async operations so we never hold a MutexGuard across
            // an await point (Connection is !Sync, so &Connection is !Send).
            let disabled_peers = {
                match db.lock() {
                    Ok(conn) => match list_node_peers(&conn) {
                        Ok(peers) => peers.into_iter().filter(|p| !p.enabled).collect::<Vec<_>>(),
                        Err(e) => {
                            tracing::warn!("health check: failed to list peers: {e}");
                            continue;
                        }
                    },
                    Err(e) => {
                        tracing::warn!("health check: db lock poisoned: {e}");
                        continue;
                    }
                }
            };
            // MutexGuard dropped here — safe to await.

            for p in disabled_peers {
                // HTTP probe — no lock held.
                let ok = client.health_check(&p.url).await.unwrap_or(false);

                if ok {
                    // Re-acquire lock for the DB writes, drop before next await.
                    match db.lock() {
                        Ok(conn) => {
                            if let Err(e) = set_node_peer_enabled(&conn, p.id, true) {
                                tracing::warn!("health check: re-enable peer {} failed: {e}", p.id);
                            } else {
                                let msg = format!(
                                    "Peer {} re-enabled after successful health check",
                                    p.id
                                );
                                if let Err(e) = insert_notification(&conn, "peer_re_enabled", &msg)
                                {
                                    tracing::warn!("health check: insert notification failed: {e}");
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("health check: db lock poisoned during update: {e}");
                        }
                    }
                    // MutexGuard dropped — safe for next iteration's await.
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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

    /// Insert a minimal node_peers row and return its id.
    fn insert_test_peer(conn: &Connection) -> i64 {
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO node_peers (url, enabled, consecutive_failures, created_at)
             VALUES ('https://test.example', 1, 0, ?1)",
            rusqlite::params![now],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Insert a disabled peer at the given URL and return its id.
    fn insert_disabled_peer(conn: &Connection, url: &str) -> i64 {
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO node_peers (url, enabled, consecutive_failures, disabled_at, created_at)
             VALUES (?1, 0, 5, ?2, ?2)",
            rusqlite::params![url, now],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    // --- new tests for run_health_check_once ---

    #[tokio::test]
    async fn re_enables_disabled_peer_when_check_succeeds() {
        use crate::federation::peer::HttpPeerClient;
        use crate::federation::storage::get_node_peer;
        use std::time::Duration;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/collections"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocol_version": "1.0",
                "collections": []
            })))
            .mount(&server)
            .await;

        let conn = fresh_db();
        let id = insert_disabled_peer(&conn, &server.uri());

        let client = HttpPeerClient::new(Duration::from_secs(5)).unwrap();
        let outcome = run_health_check_once(&conn, &client).await.unwrap();

        assert_eq!(
            outcome.re_enabled,
            vec![id],
            "re_enabled should contain the peer id"
        );
        assert!(outcome.still_down.is_empty(), "still_down should be empty");

        let peer = get_node_peer(&conn, id).unwrap().unwrap();
        assert!(peer.enabled, "peer must be re-enabled");

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM admin_notifications WHERE kind = 'peer_re_enabled'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 1,
            "exactly one peer_re_enabled notification must exist"
        );
    }

    #[tokio::test]
    async fn keeps_peer_disabled_when_check_fails() {
        use crate::federation::peer::HttpPeerClient;
        use crate::federation::storage::get_node_peer;
        use std::time::Duration;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/collections"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let conn = fresh_db();
        let id = insert_disabled_peer(&conn, &server.uri());

        let client = HttpPeerClient::new(Duration::from_secs(5)).unwrap();
        let outcome = run_health_check_once(&conn, &client).await.unwrap();

        assert!(
            outcome.re_enabled.is_empty(),
            "re_enabled should be empty on failure"
        );
        assert_eq!(
            outcome.still_down,
            vec![id],
            "still_down should contain the peer id"
        );

        let peer = get_node_peer(&conn, id).unwrap().unwrap();
        assert!(!peer.enabled, "peer must remain disabled");
    }

    /// A success call after a failure must reset `consecutive_failures` to 0,
    /// write the supplied `last_response_ms`, and leave the peer enabled.
    #[test]
    fn success_resets_failures_and_records_response_time() {
        let conn = fresh_db();
        let peer_id = insert_test_peer(&conn);

        // Record one failure first so consecutive_failures > 0.
        record_result(&conn, peer_id, false, None).unwrap();

        // Now record a success with a 42 ms response time.
        record_result(&conn, peer_id, true, Some(42)).unwrap();

        let (consecutive, last_response_ms, enabled): (i64, Option<i64>, i64) = conn
            .query_row(
                "SELECT consecutive_failures, last_response_ms, enabled
                 FROM node_peers WHERE id = ?1",
                rusqlite::params![peer_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();

        assert_eq!(consecutive, 0, "consecutive_failures must be reset to 0");
        assert_eq!(last_response_ms, Some(42), "last_response_ms must be 42");
        assert_eq!(enabled, 1, "peer must still be enabled");
    }

    /// After exactly FAILURE_THRESHOLD failures the peer must be disabled and
    /// exactly one `peer_disabled` admin notification must be present.
    #[test]
    fn auto_disables_at_threshold_and_writes_notification() {
        let conn = fresh_db();
        let peer_id = insert_test_peer(&conn);

        // Record FAILURE_THRESHOLD failures.
        for _ in 0..FAILURE_THRESHOLD {
            record_result(&conn, peer_id, false, None).unwrap();
        }

        let (enabled, disabled_at): (i64, Option<i64>) = conn
            .query_row(
                "SELECT enabled, disabled_at FROM node_peers WHERE id = ?1",
                rusqlite::params![peer_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();

        assert_eq!(
            enabled, 0,
            "peer must be disabled after FAILURE_THRESHOLD failures"
        );
        assert!(disabled_at.is_some(), "disabled_at must be set");

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM admin_notifications WHERE kind = 'peer_disabled'",
                [],
                |r| r.get(0),
            )
            .unwrap();

        assert_eq!(
            count, 1,
            "exactly one peer_disabled notification must be inserted"
        );
    }
}
