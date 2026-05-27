//! Gossip merge logic and outbound gossip client for discovered engines.
//!
//! Pure, deterministic function that merges two lists of discovered engines
//! by URL, applying gossip semantics:
//!
//! - Union by URL.
//! - For URLs in both lists: keep `MIN(first_seen)`.  If the incoming entry
//!   has a newer-or-equal `last_seen`, its `name`/`description` win.  The
//!   `last_seen` for any entry that appeared on the incoming side is then
//!   advanced to `max(last_seen, now)`.
//! - For URLs only in local: left untouched — `last_seen` is **not** advanced.
//! - For URLs only in incoming: inserted as-is with `last_seen` advanced to
//!   `max(last_seen, now)`.
//! - Final order: `last_seen DESC`, then `url ASC` (deterministic).
//!
//! This function is pure — no database access.

use crate::api::gossip::{EngineSummary, ExchangeRequest, ExchangeResponse};
use crate::federation::discovered::{self, DiscoveredEngine};
use crate::protocol::{check_compatibility, Compatibility, PROTOCOL_VERSION};
use rusqlite::Connection;
use std::collections::HashMap;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error and outcome types
// ---------------------------------------------------------------------------

/// Errors that can occur during an outbound gossip exchange.
#[derive(Debug, Error)]
pub enum GossipError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    /// Wraps database errors as strings (discovered::list/upsert return anyhow::Error).
    #[error("DB error: {0}")]
    Db(String),
    #[error("incompatible protocol version: {0}")]
    Version(String),
    #[error("peer returned status {0}")]
    Status(u16),
}

impl From<rusqlite::Error> for GossipError {
    fn from(e: rusqlite::Error) -> Self {
        GossipError::Db(e.to_string())
    }
}

/// Counts of engines sent and received in a gossip exchange.
#[derive(Debug)]
pub struct ExchangeOutcome {
    pub sent_count: usize,
    pub received_count: usize,
}

// ---------------------------------------------------------------------------
// Outbound gossip client
// ---------------------------------------------------------------------------

/// Exchange engine lists with a remote peer.
///
/// POSTs our local discovered list to `peer_url/api/gossip/exchange`, validates
/// the peer's `protocol_version`, and merges + persists the returned engine list.
///
/// # Errors
///
/// Returns [`GossipError::Version`] if the peer speaks an incompatible major
/// version, [`GossipError::Status`] for non-2xx HTTP responses, and
/// [`GossipError::Http`] / [`GossipError::Db`] for transport or database errors.
pub async fn exchange_with_peer(
    client: &reqwest::Client,
    conn: &Connection,
    peer_url: &str,
) -> Result<ExchangeOutcome, GossipError> {
    // ── Snapshot the local list ───────────────────────────────────────────
    let local = discovered::list(conn).map_err(|e| GossipError::Db(e.to_string()))?;
    let our_payload = ExchangeRequest {
        protocol_version: PROTOCOL_VERSION.to_string(),
        engines: local
            .iter()
            .map(|e| EngineSummary {
                url: e.url.clone(),
                name: e.name.clone(),
                description: e.description.clone(),
            })
            .collect(),
    };
    let sent_count = our_payload.engines.len();

    // ── HTTP exchange ─────────────────────────────────────────────────────
    let url = format!("{}/api/gossip/exchange", peer_url.trim_end_matches('/'));
    let resp = client.post(&url).json(&our_payload).send().await?;
    if !resp.status().is_success() {
        return Err(GossipError::Status(resp.status().as_u16()));
    }
    let body: ExchangeResponse = resp.json().await?;

    // ── Protocol version check ────────────────────────────────────────────
    match check_compatibility(&body.protocol_version) {
        Compatibility::Incompatible => {
            return Err(GossipError::Version(format!(
                "peer {peer_url} speaks {} (ours {PROTOCOL_VERSION})",
                body.protocol_version
            )))
        }
        Compatibility::MinorMismatch => {
            tracing::warn!(
                peer = peer_url,
                their = %body.protocol_version,
                "minor protocol version mismatch"
            );
        }
        Compatibility::Compatible => {}
    }

    // ── Merge and persist ─────────────────────────────────────────────────
    let now = chrono::Utc::now().timestamp();
    let incoming: Vec<DiscoveredEngine> = body
        .engines
        .iter()
        .map(|s| DiscoveredEngine {
            url: s.url.clone(),
            name: s.name.clone(),
            description: s.description.clone(),
            first_seen: now,
            last_seen: now,
        })
        .collect();
    let received_count = incoming.len();
    let merged = merge_engine_lists(local, incoming, now);
    for e in &merged {
        discovered::upsert(conn, e).map_err(|e| GossipError::Db(e.to_string()))?;
    }

    Ok(ExchangeOutcome {
        sent_count,
        received_count,
    })
}

/// Spawn a non-blocking background task that performs an immediate gossip
/// exchange with a single peer URL.
///
/// This function works around the `!Send` restriction of
/// `std::sync::MutexGuard<Connection>` by never holding the guard across
/// an `.await` point.  It follows the same split-lock pattern used by the
/// peer health-check task:
///
/// 1. Lock → snapshot the local engine list → drop the guard.
/// 2. Async HTTP exchange (no lock held).
/// 3. Lock → persist the merged engine list → drop the guard.
///
/// Errors are logged at the appropriate level but do **not** propagate —
/// the caller's HTTP response is always sent first.
pub fn spawn_gossip_exchange_with_peer(
    client: reqwest::Client,
    db: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    peer_url: String,
) {
    tokio::spawn(async move {
        // ── 1. Snapshot local list while holding the lock (sync, no await) ──
        let local_list = {
            match db.lock() {
                Ok(conn) => match discovered::list(&conn) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(
                            peer = %peer_url,
                            error = %e,
                            "initial gossip: failed to read local engine list"
                        );
                        return;
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        peer = %peer_url,
                        error = %e,
                        "initial gossip: db mutex poisoned when reading engine list"
                    );
                    return;
                }
            }
            // MutexGuard dropped here — safe to .await
        };

        // ── 2. Build our outbound payload (no lock held) ───────────────────
        let our_payload = ExchangeRequest {
            protocol_version: PROTOCOL_VERSION.to_string(),
            engines: local_list
                .iter()
                .map(|e| EngineSummary {
                    url: e.url.clone(),
                    name: e.name.clone(),
                    description: e.description.clone(),
                })
                .collect(),
        };
        let sent_count = our_payload.engines.len();

        // ── 3. Async HTTP exchange (no lock held) ──────────────────────────
        let url = format!("{}/api/gossip/exchange", peer_url.trim_end_matches('/'));
        let resp = match client.post(&url).json(&our_payload).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    peer = %peer_url,
                    error = %e,
                    "initial gossip exchange failed (HTTP send)"
                );
                return;
            }
        };

        if !resp.status().is_success() {
            tracing::warn!(
                peer = %peer_url,
                status = %resp.status(),
                "initial gossip exchange failed (non-2xx)"
            );
            return;
        }

        let body: ExchangeResponse = match resp.json().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    peer = %peer_url,
                    error = %e,
                    "initial gossip exchange failed (parse response)"
                );
                return;
            }
        };

        // ── 4. Protocol version check (no lock held) ───────────────────────
        match check_compatibility(&body.protocol_version) {
            Compatibility::Incompatible => {
                tracing::warn!(
                    peer = %peer_url,
                    their = %body.protocol_version,
                    "initial gossip: incompatible protocol version, skipping merge"
                );
                return;
            }
            Compatibility::MinorMismatch => {
                tracing::warn!(
                    peer = %peer_url,
                    their = %body.protocol_version,
                    "initial gossip: minor protocol version mismatch"
                );
            }
            Compatibility::Compatible => {}
        }

        // ── 5. Merge engine lists (no lock held) ───────────────────────────
        let now = chrono::Utc::now().timestamp();
        let incoming: Vec<DiscoveredEngine> = body
            .engines
            .iter()
            .map(|s| DiscoveredEngine {
                url: s.url.clone(),
                name: s.name.clone(),
                description: s.description.clone(),
                first_seen: now,
                last_seen: now,
            })
            .collect();
        let received_count = incoming.len();
        let merged = merge_engine_lists(local_list, incoming, now);

        // ── 6. Persist merged list (re-acquire lock, no await inside) ──────
        match db.lock() {
            Ok(conn) => {
                for e in &merged {
                    if let Err(e) = discovered::upsert(&conn, e) {
                        tracing::warn!(
                            peer = %peer_url,
                            error = %e,
                            "initial gossip: failed to upsert engine"
                        );
                    }
                }
                // MutexGuard dropped at end of block
            }
            Err(e) => {
                tracing::warn!(
                    peer = %peer_url,
                    error = %e,
                    "initial gossip: db mutex poisoned when persisting merged list"
                );
                return;
            }
        }

        tracing::info!(
            peer = %peer_url,
            sent = sent_count,
            received = received_count,
            "initial gossip exchange completed"
        );
    });
}

// ---------------------------------------------------------------------------
// Periodic sync
// ---------------------------------------------------------------------------

/// Counts of gossip exchanges attempted and succeeded in a sync round.
pub struct SyncResult {
    pub attempted: usize,
    pub succeeded: usize,
}

/// Perform a single gossip sync round with all enabled node peers.
///
/// Iterates every enabled node peer, calls [`exchange_with_peer`] for each,
/// and returns a [`SyncResult`] with the total attempt and success counts.
///
/// # Send-ness
///
/// This function holds `&Connection` across `.await` points and is therefore
/// `!Send`.  It is intended for direct use in tests (single-threaded tokio
/// runtime).  For background use, see [`run_periodic_sync_loop`].
pub async fn run_periodic_sync_once(
    client: &reqwest::Client,
    conn: &rusqlite::Connection,
) -> SyncResult {
    let peers = match crate::federation::storage::list_node_peers(conn) {
        Ok(all) => all.into_iter().filter(|p| p.enabled).collect::<Vec<_>>(),
        Err(e) => {
            tracing::error!(error = %e, "periodic sync: failed to list peers");
            return SyncResult {
                attempted: 0,
                succeeded: 0,
            };
        }
    };

    let mut succeeded = 0;
    let attempted = peers.len();

    for peer in &peers {
        match exchange_with_peer(client, conn, &peer.url).await {
            Ok(outcome) => {
                tracing::info!(
                    peer = %peer.url,
                    sent = outcome.sent_count,
                    received = outcome.received_count,
                    "periodic gossip sync"
                );
                succeeded += 1;
            }
            Err(e) => tracing::warn!(peer = %peer.url, error = %e, "gossip sync failed"),
        }
    }

    SyncResult {
        attempted,
        succeeded,
    }
}

/// Background loop that periodically gossips with all enabled node peers.
///
/// Skips the immediate first tick (starts after a full `interval` delay) and
/// uses [`MissedTickBehavior::Skip`] so that slow sync rounds do not trigger
/// catch-up bursts.
///
/// Uses the split-lock pattern (lock → sync work → drop guard → await) so the
/// returned future is `Send` and can be passed directly to [`tokio::spawn`].
pub async fn run_periodic_sync_loop(
    client: reqwest::Client,
    db: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    interval: std::time::Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await; // skip immediate first tick

    loop {
        ticker.tick().await;

        // Collect enabled peer URLs while holding the lock (sync, no await).
        // The MutexGuard is dropped at the end of this block — before the next
        // await point — so the future remains Send.
        let peer_urls: Vec<String> = {
            match db.lock() {
                Ok(conn) => crate::federation::storage::list_node_peers(&conn)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|p| p.enabled)
                    .map(|p| p.url.clone())
                    .collect(),
                Err(e) => {
                    tracing::error!(error = %e, "periodic sync: db mutex poisoned");
                    vec![]
                }
            }
            // MutexGuard dropped here
        };

        for url in peer_urls {
            // spawn_gossip_exchange_with_peer internally calls tokio::spawn,
            // so each peer exchange runs concurrently and does not block the loop.
            spawn_gossip_exchange_with_peer(client.clone(), db.clone(), url);
        }
    }
}

/// Merge two lists of discovered engines using gossip semantics.
///
/// See module-level documentation for the full rule set.
pub fn merge_engine_lists(
    local: Vec<DiscoveredEngine>,
    incoming: Vec<DiscoveredEngine>,
    now: i64,
) -> Vec<DiscoveredEngine> {
    // Build a map keyed by URL, seeded from the local list.
    let mut map: HashMap<String, DiscoveredEngine> =
        local.into_iter().map(|e| (e.url.clone(), e)).collect();

    for inc in incoming {
        let url = inc.url.clone();
        if let Some(existing) = map.get_mut(&url) {
            // URL present in both lists.
            // Rule: keep the oldest first_seen.
            existing.first_seen = existing.first_seen.min(inc.first_seen);
            // Rule: if incoming has a newer (or equal) last_seen, it wins for
            // name/description/last_seen.
            if inc.last_seen >= existing.last_seen {
                existing.name = inc.name;
                existing.description = inc.description;
                existing.last_seen = inc.last_seen;
            }
            // Rule: advance last_seen to now for any entry on the incoming side.
            existing.last_seen = existing.last_seen.max(now);
        } else {
            // URL only in incoming — insert with last_seen advanced to now.
            map.insert(
                url,
                DiscoveredEngine {
                    last_seen: inc.last_seen.max(now),
                    ..inc
                },
            );
        }
    }

    // Collect and sort: last_seen DESC, url ASC.
    let mut result: Vec<DiscoveredEngine> = map.into_values().collect();
    result.sort_by(|a, b| {
        b.last_seen
            .cmp(&a.last_seen)
            .then_with(|| a.url.cmp(&b.url))
    });
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn engine(url: &str, first_seen: i64, last_seen: i64) -> DiscoveredEngine {
        DiscoveredEngine {
            url: url.to_string(),
            name: format!("Engine {url}"),
            description: String::new(),
            first_seen,
            last_seen,
        }
    }

    fn engine_named(
        url: &str,
        name: &str,
        description: &str,
        first_seen: i64,
        last_seen: i64,
    ) -> DiscoveredEngine {
        DiscoveredEngine {
            url: url.to_string(),
            name: name.to_string(),
            description: description.to_string(),
            first_seen,
            last_seen,
        }
    }

    /// 1. Disjoint lists: the result must contain all URLs from both sides.
    #[test]
    fn merge_disjoint_lists_unions_both() {
        let local = vec![engine("https://a", 10, 10)];
        let incoming = vec![engine("https://b", 20, 20)];
        let result = merge_engine_lists(local, incoming, 100);
        let urls: Vec<&str> = result.iter().map(|e| e.url.as_str()).collect();
        assert!(urls.contains(&"https://a"), "missing https://a: {urls:?}");
        assert!(urls.contains(&"https://b"), "missing https://b: {urls:?}");
        assert_eq!(result.len(), 2);
    }

    /// 2. Overlapping URL: first_seen must be the minimum of both sides.
    #[test]
    fn merge_overlapping_lists_keeps_older_first_seen() {
        // local a: first_seen=10, last_seen=10
        // incoming a: first_seen=5, last_seen=30
        // expected first_seen = MIN(10, 5) = 5
        let local = vec![engine("https://a", 10, 10)];
        let incoming = vec![engine("https://a", 5, 30)];
        let result = merge_engine_lists(local, incoming, 100);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].first_seen, 5);
    }

    /// 3. Overlapping URL on incoming side advances last_seen to now.
    #[test]
    fn merge_advances_last_seen_to_now_on_contact() {
        // local a: last_seen=10
        // incoming a: last_seen=20
        // now=999 → result last_seen = max(20, 999) = 999
        let local = vec![engine("https://a", 10, 10)];
        let incoming = vec![engine("https://a", 10, 20)];
        let result = merge_engine_lists(local, incoming, 999);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].last_seen, 999);
    }

    /// 4. Empty incoming: local entries must be returned untouched (last_seen unchanged).
    #[test]
    fn merge_empty_incoming_returns_local_with_unchanged_last_seen() {
        let local = vec![engine("https://a", 10, 10)];
        let result = merge_engine_lists(local, vec![], 999);
        assert_eq!(result.len(), 1);
        // last_seen must NOT be advanced to now — the entry was not contacted.
        assert_eq!(result[0].last_seen, 10);
    }

    /// 5. Empty local: incoming entry is inserted with last_seen advanced to now.
    #[test]
    fn merge_empty_local_uses_incoming() {
        let incoming = vec![engine("https://a", 10, 10)];
        let result = merge_engine_lists(vec![], incoming, 999);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].last_seen, 999);
    }

    /// 6. When incoming is newer, its name and description overwrite the local values.
    #[test]
    fn merge_newer_incoming_overwrites_name_and_description() {
        let local = vec![engine_named("https://a", "OLD", "old desc", 10, 10)];
        let incoming = vec![engine_named("https://a", "NEW", "new desc", 10, 50)];
        let result = merge_engine_lists(local, incoming, 100);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "NEW");
        assert_eq!(result[0].description, "new desc");
    }

    /// 7. When incoming is older, the local name and description are preserved.
    #[test]
    fn merge_older_incoming_does_not_overwrite_name() {
        let local = vec![engine_named("https://a", "KEEP", "keep desc", 10, 50)];
        let incoming = vec![engine_named("https://a", "OLDER", "older desc", 10, 20)];
        let result = merge_engine_lists(local, incoming, 100);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "KEEP");
        assert_eq!(result[0].description, "keep desc");
    }

    // ── Helper: open an in-memory DB with migrations applied ─────────────────

    fn fresh_conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        crate::db::run_migrations(&conn).unwrap();
        conn
    }

    // ── Test 8: sends our list and persists the peer's response ──────────────

    /// 8. exchange_with_peer: success path — sends our list, validates protocol
    ///    version 1.0, merges the peer's engine list, and returns received_count.
    #[tokio::test]
    async fn exchange_with_peer_sends_our_list_and_persists_response() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // Start a mock server that returns a compatible response.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/gossip/exchange"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocol_version": "1.0",
                "engines": [
                    {"url": "https://from-peer.example.com", "name": "Peer Engine", "description": ""}
                ]
            })))
            .mount(&server)
            .await;

        let conn = fresh_conn();
        // Seed local list so we have something to send.
        crate::federation::discovered::ensure_self_entry(
            &conn,
            "https://self.example.com",
            "Self",
            "Self Engine",
            1000,
        )
        .unwrap();

        let client = reqwest::Client::new();
        let outcome = super::exchange_with_peer(&client, &conn, &server.uri())
            .await
            .expect("exchange should succeed");

        assert_eq!(outcome.received_count, 1, "should have received 1 engine");

        let list = crate::federation::discovered::list(&conn).unwrap();
        let urls: Vec<&str> = list.iter().map(|e| e.url.as_str()).collect();
        assert!(
            urls.iter().any(|u| u.contains("from-peer.example.com")),
            "from-peer.example.com should be in discovered list: {urls:?}"
        );
    }

    // ── Test 9: rejects incompatible major version ────────────────────────────

    /// 9. exchange_with_peer: returns GossipError::Version when peer responds
    ///    with a different major version (2.0 vs our 1.0).
    #[tokio::test]
    async fn exchange_with_peer_rejects_major_version_mismatch() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/gossip/exchange"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocol_version": "2.0",
                "engines": []
            })))
            .mount(&server)
            .await;

        let conn = fresh_conn();
        crate::federation::discovered::ensure_self_entry(
            &conn,
            "https://self.example.com",
            "Self",
            "Self Engine",
            1000,
        )
        .unwrap();

        let client = reqwest::Client::new();
        let result = super::exchange_with_peer(&client, &conn, &server.uri()).await;

        assert!(result.is_err(), "should fail for incompatible version");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("version"),
            "error should mention 'version': {err}"
        );
    }
}
