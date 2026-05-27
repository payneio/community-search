use community_search::federation::gossip::run_periodic_sync_once;
use community_search::federation::storage::insert_node_peer;
use rusqlite::Connection;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Verifies that `run_periodic_sync_once` contacts every enabled node peer
/// exactly once and returns the correct attempt/success counts.
#[tokio::test]
async fn periodic_sync_exchanges_with_every_node_peer() {
    let peer1 = MockServer::start().await;
    let peer2 = MockServer::start().await;

    for p in [&peer1, &peer2] {
        Mock::given(method("POST"))
            .and(path("/api/gossip/exchange"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"protocol_version":"1.0","engines":[]})),
            )
            .expect(1)
            .mount(p)
            .await;
    }

    // Create an in-memory DB with migrations applied (raw Connection — no
    // MutexGuard involved, so &conn can be passed directly to
    // run_periodic_sync_once without holding a lock across an await).
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    community_search::db::run_migrations(&conn).unwrap();

    // Seed two enabled node peers pointing at the mock servers.
    // insert_node_peer inserts with enabled=1 by default.
    insert_node_peer(&conn, &peer1.uri(), Some("p1")).unwrap();
    insert_node_peer(&conn, &peer2.uri(), Some("p2")).unwrap();

    let client = reqwest::Client::new();
    let count = run_periodic_sync_once(&client, &conn).await;

    assert_eq!(count.attempted, 2, "should have attempted 2 peers");
    assert_eq!(count.succeeded, 2, "both peers should have succeeded");

    // Dropping the mock servers triggers wiremock's `.expect(1)` verification —
    // this panics if either exchange endpoint was NOT called exactly once.
    drop(peer1);
    drop(peer2);
}
