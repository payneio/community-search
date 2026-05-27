//! Per-IP sliding-window rate limiter with escalating cooloff.
//!
//! Persisted in the `rate_limit_state` SQLite table (created by migration 004).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{HeaderValue, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tokio::sync::RwLock;

use crate::api::public::{AppState, SharedDb};

// ---------------------------------------------------------------------------
// Peer-IP cache
// ---------------------------------------------------------------------------

/// How long to keep the peer-IP host set cached before refreshing from the DB.
const PEER_IP_CACHE_TTL: Duration = Duration::from_secs(300); // 5 minutes

/// Cached set of host names extracted from enabled `node_peers` URLs.
///
/// Populated lazily on the first request and refreshed at most once every
/// [`PEER_IP_CACHE_TTL`].  Storing host-name strings (rather than full URLs)
/// lets the middleware compare directly against the client IP string extracted
/// by [`crate::api::auth::client_ip_pub`].
pub struct PeerIpCache {
    hosts: HashSet<String>,
    last_refreshed_at: Option<Instant>,
}

impl Default for PeerIpCache {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerIpCache {
    /// Create a new, empty (immediately stale) cache.
    pub fn new() -> Self {
        Self {
            hosts: HashSet::new(),
            last_refreshed_at: None,
        }
    }

    fn is_stale(&self) -> bool {
        match self.last_refreshed_at {
            None => true,
            Some(t) => t.elapsed() > PEER_IP_CACHE_TTL,
        }
    }

    fn refresh(&mut self, db: &SharedDb) {
        let hosts = {
            let conn = db.lock().expect("db mutex poisoned");
            load_peer_hosts_from_db(&conn)
        };
        self.hosts = hosts;
        self.last_refreshed_at = Some(Instant::now());
    }

    fn contains(&self, ip: &str) -> bool {
        self.hosts.contains(ip)
    }
}

/// Query the database for all enabled node peers, parse their URLs, and
/// return the set of host-name strings (e.g. `"127.0.0.1"`, `"peer.example"`).
fn load_peer_hosts_from_db(conn: &rusqlite::Connection) -> HashSet<String> {
    let mut hosts = HashSet::new();
    let Ok(mut stmt) = conn.prepare("SELECT url FROM node_peers WHERE enabled = 1") else {
        return hosts;
    };
    let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) else {
        return hosts;
    };
    for row in rows.flatten() {
        if let Ok(parsed) = url::Url::parse(&row) {
            if let Some(host) = parsed.host_str() {
                hosts.insert(host.to_string());
            }
        }
    }
    hosts
}

/// Returns `true` when `client_ip` matches the host extracted from any enabled
/// `node_peers.url` row.
///
/// Uses a cached set of peer hosts (`cache`) that is refreshed at most once
/// every [`PEER_IP_CACHE_TTL`] to avoid a SQLite query on every request.  A
/// double-check (`is_stale` inside the write lock) prevents redundant refreshes
/// when multiple concurrent requests race to update a stale cache.
pub async fn is_peer_ip(db: &SharedDb, client_ip: &str, cache: &Arc<RwLock<PeerIpCache>>) -> bool {
    // Fast path: read-lock, cache is fresh.
    {
        let guard = cache.read().await;
        if !guard.is_stale() {
            return guard.contains(client_ip);
        }
    }
    // Slow path: cache needs refresh — acquire write lock.
    let mut guard = cache.write().await;
    // Re-check after acquiring write lock; another task may have refreshed first.
    if guard.is_stale() {
        guard.refresh(db);
    }
    guard.contains(client_ip)
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Identifies what entity a rate-limit window is tracked against.
///
/// Different variants use distinct storage-key namespaces, so an `Ip` and a
/// `Peer` entry with the same string value are tracked **independently**.
pub enum RateLimitKey {
    /// Per client IP address (the existing namespace).
    Ip(String),
    /// Per authenticated peer / client identity (reserved for Phase 5).
    Peer(String),
}

impl RateLimitKey {
    /// Returns the string used as the primary key in `rate_limit_state`.
    ///
    /// - `Ip("1.2.3.4")`  → `"ip:1.2.3.4"`
    /// - `Peer("alice")`  → `"peer:alice"`
    fn storage_key(&self) -> String {
        match self {
            RateLimitKey::Ip(s) => format!("ip:{s}"),
            RateLimitKey::Peer(s) => format!("peer:{s}"),
        }
    }
}

/// Configuration for the sliding-window rate limiter.
#[derive(Clone)]
pub struct RateLimitConfig {
    /// Maximum number of requests allowed within `window_seconds`.
    pub limit: u32,
    /// Sliding-window duration in seconds.
    pub window_seconds: i64,
    /// Escalating cooloff durations in seconds (indexed by violation count).
    pub cooloff_ladder: Vec<i64>,
    /// Seconds of inactivity (no new violations, cooloff expired) after which
    /// the violation counter is reset to zero.
    pub clean_period_seconds: i64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            limit: 30,
            window_seconds: 60,
            cooloff_ladder: vec![60, 300, 3600],
            clean_period_seconds: 3600,
        }
    }
}

/// Rate-limiter decision for a single request.
pub enum Decision {
    /// The request is within the rate limit and may proceed.
    Allow,
    /// The IP is in a cooloff period; the request should be rejected.
    Cooloff {
        /// Seconds until the cooloff expires.
        seconds_remaining: i64,
    },
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

struct RateLimitState {
    request_log: Vec<i64>,
    violations: i64,
    cooloff_until: i64,
    last_violation_at: i64,
}

fn load_state(conn: &rusqlite::Connection, ip: &str) -> Result<RateLimitState> {
    match conn.query_row(
        "SELECT request_log, violations, cooloff_until, last_violation_at
         FROM rate_limit_state WHERE ip = ?1",
        rusqlite::params![ip],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    ) {
        Ok((log_json, violations, cooloff_until, last_violation_at)) => {
            let request_log: Vec<i64> = serde_json::from_str(&log_json).unwrap_or_default();
            Ok(RateLimitState {
                request_log,
                violations,
                cooloff_until,
                last_violation_at,
            })
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(RateLimitState {
            request_log: vec![],
            violations: 0,
            cooloff_until: 0,
            last_violation_at: 0,
        }),
        Err(e) => Err(e.into()),
    }
}

fn save_state(
    conn: &rusqlite::Connection,
    ip: &str,
    now: i64,
    state: &RateLimitState,
) -> Result<()> {
    let log_json = serde_json::to_string(&state.request_log)?;
    conn.execute(
        "INSERT INTO rate_limit_state
             (ip, request_log, violations, cooloff_until, last_violation_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(ip) DO UPDATE SET
             request_log       = ?2,
             violations        = ?3,
             cooloff_until     = ?4,
             last_violation_at = ?5,
             updated_at        = ?6",
        rusqlite::params![
            ip,
            log_json,
            state.violations,
            state.cooloff_until,
            state.last_violation_at,
            now,
        ],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Check whether the entity identified by `key` is within the rate limit and
/// record the attempt.
///
/// This is the canonical implementation: [`check_and_record`] is a thin
/// wrapper that constructs an [`RateLimitKey::Ip`] and delegates here.
///
/// Returns [`Decision::Allow`] if the request can proceed, or
/// [`Decision::Cooloff`] with the remaining seconds if the entity is throttled.
///
/// State is persisted to the `rate_limit_state` table after every call.
pub fn check_and_record_key(
    db: &SharedDb,
    key: &RateLimitKey,
    now: i64,
    cfg: &RateLimitConfig,
) -> Result<Decision> {
    let conn = db.lock().expect("db mutex poisoned");
    let storage_key = key.storage_key();

    let mut state = load_state(&conn, &storage_key)?;

    // ── Clean-period reset ────────────────────────────────────────────────────
    // If the last violation was long enough ago AND the cooloff has expired,
    // reset the violation counter so the entity starts fresh.
    if state.last_violation_at > 0
        && now - state.last_violation_at > cfg.clean_period_seconds
        && state.cooloff_until <= now
    {
        state.violations = 0;
    }

    // ── Active cooloff check ──────────────────────────────────────────────────
    if state.cooloff_until > now {
        let seconds_remaining = state.cooloff_until - now;
        save_state(&conn, &storage_key, now, &state)?;
        return Ok(Decision::Cooloff { seconds_remaining });
    }

    // ── Prune sliding window ──────────────────────────────────────────────────
    let window_start = now - cfg.window_seconds;
    state.request_log.retain(|&t| t > window_start);

    // ── Rate check ────────────────────────────────────────────────────────────
    let decision = if state.request_log.len() >= cfg.limit as usize {
        // Violation: pick cooloff from ladder (capped at last rung), escalate.
        let idx = (state.violations as usize).min(cfg.cooloff_ladder.len() - 1);
        let duration = cfg.cooloff_ladder[idx];
        state.violations += 1;
        state.cooloff_until = now + duration;
        state.last_violation_at = now;
        Decision::Cooloff {
            seconds_remaining: duration,
        }
    } else {
        // Within limit: record this timestamp.
        state.request_log.push(now);
        // Cap log at limit * 2 to prevent unbounded growth.
        let cap = (cfg.limit * 2) as usize;
        if state.request_log.len() > cap {
            let excess = state.request_log.len() - cap;
            state.request_log.drain(..excess);
        }
        Decision::Allow
    };

    save_state(&conn, &storage_key, now, &state)?;
    Ok(decision)
}

/// Check whether `ip` is within the rate limit and record the attempt.
///
/// Thin wrapper around [`check_and_record_key`] using [`RateLimitKey::Ip`].
///
/// Returns [`Decision::Allow`] if the request can proceed, or
/// [`Decision::Cooloff`] with the remaining seconds if the IP is throttled.
///
/// State is persisted to the `rate_limit_state` table after every call.
pub fn check_and_record(
    db: &SharedDb,
    ip: &str,
    now: i64,
    cfg: &RateLimitConfig,
) -> Result<Decision> {
    check_and_record_key(db, &RateLimitKey::Ip(ip.to_string()), now, cfg)
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

/// Axum middleware: enforce the per-IP rate limit on the route it is applied to.
///
/// Extracts the client IP using the same logic as the auth middleware (via
/// [`crate::api::auth::client_ip_pub`]), computes the current Unix timestamp,
/// and calls [`check_and_record`].
///
/// **Two-bucket system:** known peer node IPs (any IP whose hostname appears in
/// an enabled `node_peers.url`) use the peer-rate-limit bucket
/// (`state.peer_rate_limit_config`); all other IPs use the anonymous bucket
/// (`state.rate_limit_config`).  The peer-IP set is cached in
/// `state.peer_ip_cache` and refreshed at most once every 5 minutes.
///
/// - **Allow** → forwards the request to the next handler.
/// - **Cooloff** → returns `429 Too Many Requests` with a `Retry-After`
///   header set to the remaining cooloff duration in seconds, and the body
///   `"rate limited"`.
/// - **Err** → returns `500 Internal Server Error` with the body
///   `"rate-limit storage error"`.
pub async fn require_rate_limit(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let ip = {
        let connect_info = req.extensions().get::<ConnectInfo<SocketAddr>>();
        crate::api::auth::client_ip_pub(&req, connect_info)
    };
    let now = chrono::Utc::now().timestamp();

    // Determine whether the client IP belongs to a known peer node.
    let peer = is_peer_ip(&state.db, &ip, &state.peer_ip_cache).await;

    // Pick the appropriate bucket config and take a snapshot so the admin
    // endpoint can update it without holding the lock during DB operations.
    let cfg = if peer {
        state.peer_rate_limit_config.read().await.clone()
    } else {
        state.rate_limit_config.read().await.clone()
    };

    match check_and_record(&state.db, &ip, now, &cfg) {
        Ok(Decision::Allow) => next.run(req).await,
        Ok(Decision::Cooloff { seconds_remaining }) => {
            let mut response = (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response();
            // SAFETY: `seconds_remaining` is a non-negative integer; its decimal
            // representation contains only ASCII digits, so `from_str` always succeeds.
            if let Ok(hv) = HeaderValue::from_str(&seconds_remaining.to_string()) {
                response.headers_mut().insert("retry-after", hv);
            }
            response
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "rate-limit storage error",
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::sync::{Arc, Mutex};

    // ── Test helpers ──────────────────────────────────────────────────────────

    fn test_db() -> SharedDb {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS rate_limit_state (
                ip                TEXT    PRIMARY KEY,
                request_log       TEXT    NOT NULL DEFAULT '[]',
                violations        INTEGER DEFAULT 0,
                cooloff_until     INTEGER DEFAULT 0,
                last_violation_at INTEGER DEFAULT 0,
                updated_at        INTEGER DEFAULT 0
            );",
        )
        .expect("create rate_limit_state table");
        Arc::new(Mutex::new(conn))
    }

    /// Tiny limit so tests don't need to issue 30 requests each.
    fn small_cfg() -> RateLimitConfig {
        RateLimitConfig {
            limit: 3,
            window_seconds: 60,
            cooloff_ladder: vec![60, 300, 3600],
            clean_period_seconds: 3600,
        }
    }

    // ── 1. under_limit_allows_request ─────────────────────────────────────────

    /// Every request within the limit returns Allow.
    #[test]
    fn under_limit_allows_request() {
        let db = test_db();
        let cfg = small_cfg();
        let now: i64 = 1_000_000;
        let ip = "1.2.3.4";

        for i in 0..cfg.limit {
            let d = check_and_record(&db, ip, now, &cfg).unwrap();
            assert!(
                matches!(d, Decision::Allow),
                "request {i} (within limit of {}) must return Allow",
                cfg.limit
            );
        }
    }

    // ── 2. exceeding_limit_triggers_first_cooloff_60s ─────────────────────────

    /// The request that exceeds the limit triggers a 60 s cooloff.
    #[test]
    fn exceeding_limit_triggers_first_cooloff_60s() {
        let db = test_db();
        let cfg = small_cfg(); // limit = 3
        let now: i64 = 1_000_000;
        let ip = "1.2.3.4";

        // Saturate the window.
        for _ in 0..cfg.limit {
            check_and_record(&db, ip, now, &cfg).unwrap();
        }

        // The very next request must trigger the first rung (60 s).
        let d = check_and_record(&db, ip, now, &cfg).unwrap();
        assert!(
            matches!(
                d,
                Decision::Cooloff {
                    seconds_remaining: 60
                }
            ),
            "first violation must produce a 60 s cooloff"
        );
    }

    // ── 3. cooloff_escalates_60_300_3600 ──────────────────────────────────────

    /// Successive violations climb the ladder: 60 s → 300 s → 3 600 s → 3 600 s.
    ///
    /// Time is advanced exactly to the end of each cooloff so the clean-period
    /// (which has the same duration as the 3rd rung) does NOT fire in between.
    #[test]
    fn cooloff_escalates_60_300_3600() {
        let db = test_db();
        let cfg = small_cfg(); // limit = 3, ladder = [60, 300, 3600], clean = 3600
        let mut now: i64 = 1_000_000;
        let ip = "1.2.3.4";

        // ── 1st violation → 60 s ──────────────────────────────────────────────
        for _ in 0..cfg.limit {
            check_and_record(&db, ip, now, &cfg).unwrap();
        }
        let d = check_and_record(&db, ip, now, &cfg).unwrap();
        assert!(
            matches!(
                d,
                Decision::Cooloff {
                    seconds_remaining: 60
                }
            ),
            "1st violation must be 60 s"
        );
        // State: violations=1, cooloff_until=now+60, last_violation_at=now

        // Advance exactly to the end of the 60 s cooloff.
        // now - last_violation_at == 60, which is NOT > clean_period (3600),
        // so the violation counter is preserved.
        now += 60;

        // ── 2nd violation → 300 s ─────────────────────────────────────────────
        // Entries from the previous round are outside the new window (they were
        // recorded at now-60, which equals the window boundary — strict > means
        // they are pruned), so we can saturate again.
        for _ in 0..cfg.limit {
            check_and_record(&db, ip, now, &cfg).unwrap();
        }
        let d = check_and_record(&db, ip, now, &cfg).unwrap();
        assert!(
            matches!(
                d,
                Decision::Cooloff {
                    seconds_remaining: 300
                }
            ),
            "2nd violation must be 300 s"
        );
        // State: violations=2, cooloff_until=now+300, last_violation_at=now

        // Advance exactly to the end of the 300 s cooloff.
        // now - last_violation_at == 300, NOT > 3600 → no reset.
        now += 300;

        // ── 3rd violation → 3 600 s ───────────────────────────────────────────
        for _ in 0..cfg.limit {
            check_and_record(&db, ip, now, &cfg).unwrap();
        }
        let d = check_and_record(&db, ip, now, &cfg).unwrap();
        assert!(
            matches!(
                d,
                Decision::Cooloff {
                    seconds_remaining: 3600
                }
            ),
            "3rd violation must be 3 600 s"
        );
        // State: violations=3, cooloff_until=now+3600, last_violation_at=now

        // Advance exactly 3 600 s.
        // now - last_violation_at == 3600, which is NOT > clean_period (3600)
        // (strict >), so violations stay at 3.
        now += 3600;

        // ── 4th violation → still 3 600 s (ladder is capped) ──────────────────
        for _ in 0..cfg.limit {
            check_and_record(&db, ip, now, &cfg).unwrap();
        }
        let d = check_and_record(&db, ip, now, &cfg).unwrap();
        assert!(
            matches!(
                d,
                Decision::Cooloff {
                    seconds_remaining: 3600
                }
            ),
            "4th violation must stay at 3 600 s (ladder cap)"
        );
    }

    // ── 4. clean_period_resets_violations ─────────────────────────────────────

    /// After the clean period has elapsed (cooloff expired + > clean_period_seconds
    /// since last violation), the violation counter resets and the next request
    /// is Allow.
    #[test]
    fn clean_period_resets_violations() {
        let db = test_db();
        let cfg = small_cfg(); // limit = 3, clean_period = 3600
        let mut now: i64 = 1_000_000;
        let ip = "1.2.3.4";

        // Trigger a violation (first rung: 60 s cooloff).
        for _ in 0..cfg.limit {
            check_and_record(&db, ip, now, &cfg).unwrap();
        }
        let d = check_and_record(&db, ip, now, &cfg).unwrap();
        assert!(
            matches!(d, Decision::Cooloff { .. }),
            "setup: must be in cooloff after saturating"
        );
        // State: violations=1, cooloff_until=now+60, last_violation_at=now

        // Advance past the cooloff (60 s) AND past the clean period (3 600 s).
        // now - last_violation_at = 3 661 > 3 600 → clean period fires.
        now += 60 + 3601;

        // Violations are reset to 0; log is empty (entries 3 661 s old are
        // outside the 60 s window). First request must be Allow.
        let d = check_and_record(&db, ip, now, &cfg).unwrap();
        assert!(
            matches!(d, Decision::Allow),
            "after clean period, first request must return Allow (violations reset)"
        );
    }

    // ── 5. peer_key_is_tracked_separately_from_ip_key ─────────────────────────

    /// Hammering an Ip key to its limit must not consume quota for the same
    /// string used as a Peer key — the two namespaces are independent.
    #[test]
    fn peer_key_is_tracked_separately_from_ip_key() {
        let db = test_db();
        let cfg = small_cfg(); // limit = 3
        let now: i64 = 1_000_000;
        let addr = "5.5.5.5".to_string();

        // Saturate the Ip key (limit calls → Allow, next → Cooloff).
        for _ in 0..cfg.limit {
            check_and_record_key(&db, &RateLimitKey::Ip(addr.clone()), now, &cfg).unwrap();
        }
        // Confirm Ip key is now in cooloff.
        let d = check_and_record_key(&db, &RateLimitKey::Ip(addr.clone()), now, &cfg).unwrap();
        assert!(
            matches!(d, Decision::Cooloff { .. }),
            "Ip key should be in cooloff after exceeding limit"
        );

        // Same string as a Peer key must still be within limit → Allow.
        let d = check_and_record_key(&db, &RateLimitKey::Peer(addr.clone()), now, &cfg).unwrap();
        assert!(
            matches!(d, Decision::Allow),
            "Peer key with same address must be tracked independently and return Allow"
        );
    }

    // ── 6. per_peer_config_overrides_default ──────────────────────────────────

    /// A custom config (limit=5) is respected for Peer keys:
    /// calls 1-5 → Allow, call 6 → Cooloff.
    #[test]
    fn per_peer_config_overrides_default() {
        let db = test_db();
        let cfg = RateLimitConfig {
            limit: 5,
            window_seconds: 60,
            cooloff_ladder: vec![60, 300, 3600],
            clean_period_seconds: 3600,
        };
        let now: i64 = 1_000_000;
        let peer = "p1".to_string();

        for i in 0..cfg.limit {
            let d =
                check_and_record_key(&db, &RateLimitKey::Peer(peer.clone()), now, &cfg).unwrap();
            assert!(
                matches!(d, Decision::Allow),
                "call {} (within limit of {}) must return Allow",
                i + 1,
                cfg.limit
            );
        }

        // The 6th call must trigger a cooloff.
        let d = check_and_record_key(&db, &RateLimitKey::Peer(peer.clone()), now, &cfg).unwrap();
        assert!(
            matches!(d, Decision::Cooloff { .. }),
            "6th call must return Cooloff when limit is 5"
        );
    }
}
