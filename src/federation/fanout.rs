//! Fan-out search across collection peers.

use anyhow::Result;
use futures::{stream::FuturesUnordered, Stream};
use rusqlite::Connection;
use std::sync::Arc;
use std::time::Instant;

use crate::federation::peer::PeerClient;
use crate::search::{SearchRequest, SearchResult};

/// A joined view of a collection_peer row with its parent node_peer's URL,
/// ready for use in fan-out queries.
#[derive(Debug, Clone)]
pub struct ActiveCollectionPeer {
    pub collection_peer_id: i64,
    pub node_peer_id: i64,
    pub node_peer_url: String,
    pub remote_collection: String,
    pub source_weight: f32,
    pub source_label: String,
}

/// The outcome of dispatching a search request to a single peer.
///
/// Carries both the results (or error) and the metadata needed to update peer
/// health via [`crate::federation::health::record_result`].
#[derive(Debug)]
pub struct PeerOutcome {
    pub node_peer_id: i64,
    pub source_label: String,
    pub source_weight: f32,
    pub result: Result<Vec<SearchResult>>,
    pub elapsed_ms: i64,
}

/// Dispatch search requests to all `peers` in parallel and return a stream of
/// [`PeerOutcome`] values in completion order (fastest peer first).
///
/// Uses [`FuturesUnordered`] so results are yielded as each peer responds
/// rather than waiting for the slowest peer before delivering any output.
///
/// For each peer the outgoing [`SearchRequest`] is patched with:
/// - `depth = outgoing_depth`
/// - `collection = Some(peer.remote_collection)`
///
/// Errors from individual peers are captured in [`PeerOutcome::result`] rather
/// than being propagated; the caller decides how to handle them (e.g. log and
/// call `record_result` with `success = false`).
pub fn dispatch(
    client: Arc<dyn PeerClient>,
    peers: Vec<ActiveCollectionPeer>,
    request: SearchRequest,
    outgoing_depth: u8,
) -> impl Stream<Item = PeerOutcome> + Send + 'static {
    let futs: FuturesUnordered<_> = FuturesUnordered::new();

    for peer in peers {
        let client = Arc::clone(&client);
        let mut req = request.clone();
        req.depth = outgoing_depth as u32;
        req.collection = Some(peer.remote_collection.clone());
        let label = peer.source_label.clone();
        let weight = peer.source_weight;
        let url = peer.node_peer_url.clone();
        let node_peer_id = peer.node_peer_id;

        futs.push(async move {
            let start = Instant::now();
            let result = async {
                let mut results = client.search(&url, &req).await?;
                for r in &mut results {
                    r.source = label.clone();
                    r.score *= weight;
                }
                Ok::<Vec<SearchResult>, anyhow::Error>(results)
            }
            .await;
            let elapsed_ms = start.elapsed().as_millis() as i64;
            PeerOutcome {
                node_peer_id,
                source_label: label,
                source_weight: weight,
                result,
                elapsed_ms,
            }
        });
    }

    futs
}

/// Return all enabled collection-peer/node-peer pairs for the given local
/// collection, joining only rows where both `collection_peers.enabled` and
/// `node_peers.enabled` are true.
pub fn active_collection_peers_for(
    conn: &Connection,
    local: &str,
) -> Result<Vec<ActiveCollectionPeer>> {
    let mut stmt = conn.prepare(
        "SELECT cp.id, np.id, np.url, cp.remote_collection, cp.source_weight,
                COALESCE(np.name, np.url)
         FROM collection_peers cp
         JOIN node_peers np ON np.id = cp.node_peer_id
         WHERE cp.local_collection = ?1 AND cp.enabled = 1 AND np.enabled = 1
         ORDER BY cp.id ASC",
    )?;
    let rows = stmt.query_map(rusqlite::params![local], |r| {
        let node_peer_id: i64 = r.get(1)?;
        let url: String = r.get(2)?;
        let remote: String = r.get(3)?;
        let source_label = format!("peer:{}/{}", url, remote);
        Ok(ActiveCollectionPeer {
            collection_peer_id: r.get(0)?,
            node_peer_id,
            node_peer_url: url,
            remote_collection: remote,
            source_weight: r.get(4)?,
            source_label,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::peer::HttpPeerClient;
    use crate::federation::storage::{
        insert_collection_peer, insert_node_peer, set_node_peer_enabled,
    };
    use futures::StreamExt;
    use std::sync::Arc;
    use std::time::Duration;

    fn fresh_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        crate::db::run_migrations(&conn).unwrap();
        conn
    }

    #[tokio::test]
    async fn dispatch_streams_results_per_peer() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let result_s = serde_json::json!({
            "title": "S",
            "url": "https://slow.example/1",
            "snippet_html": "",
            "source": "local",
            "timestamp": 0,
            "score": 1.0
        });
        let sse_slow = format!(
            "event: result\ndata: {}\n\nevent: complete\ndata: {{}}\n\n",
            result_s
        );

        let result_f = serde_json::json!({
            "title": "F",
            "url": "https://fast.example/1",
            "snippet_html": "",
            "source": "local",
            "timestamp": 0,
            "score": 1.0
        });
        let sse_fast = format!(
            "event: result\ndata: {}\n\nevent: complete\ndata: {{}}\n\n",
            result_f
        );

        // Slow server: 80ms delay
        let slow_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/search"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/event-stream")
                    .set_body_string(sse_slow)
                    .set_delay(Duration::from_millis(80)),
            )
            .mount(&slow_server)
            .await;

        // Fast server: no delay
        let fast_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/search"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/event-stream")
                    .set_body_string(sse_fast),
            )
            .mount(&fast_server)
            .await;

        let peers = vec![
            ActiveCollectionPeer {
                collection_peer_id: 1,
                node_peer_id: 10,
                node_peer_url: slow_server.uri(),
                remote_collection: "rust".to_string(),
                source_weight: 0.5,
                source_label: "slow".to_string(),
            },
            ActiveCollectionPeer {
                collection_peer_id: 2,
                node_peer_id: 20,
                node_peer_url: fast_server.uri(),
                remote_collection: "rust".to_string(),
                source_weight: 1.0,
                source_label: "fast".to_string(),
            },
        ];

        let client = Arc::new(HttpPeerClient::new(Duration::from_secs(5)).unwrap());

        let req = crate::search::SearchRequest {
            query: "x".to_string(),
            collection: Some("rust".to_string()),
            depth: 0,
        };

        let mut stream = dispatch(client, peers, req, 0);

        let mut labels: Vec<String> = Vec::new();
        while let Some(outcome) = stream.next().await {
            labels.push(outcome.source_label.clone());
        }

        assert_eq!(labels, vec!["fast", "slow"]);
    }

    #[tokio::test]
    async fn dispatch_applies_source_weight_to_scores() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let result_json = serde_json::json!({
            "title": "W",
            "url": "https://w.example/1",
            "snippet_html": "",
            "source": "local",
            "timestamp": 0,
            "score": 2.0
        });
        let sse_body = format!(
            "event: result\ndata: {}\n\nevent: complete\ndata: {{}}\n\n",
            result_json
        );

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/search"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/event-stream")
                    .set_body_string(sse_body),
            )
            .mount(&server)
            .await;

        let peers = vec![ActiveCollectionPeer {
            collection_peer_id: 1,
            node_peer_id: 10,
            node_peer_url: server.uri(),
            remote_collection: "rust".to_string(),
            source_weight: 0.5,
            source_label: "weighted".to_string(),
        }];

        let client = Arc::new(HttpPeerClient::new(Duration::from_secs(5)).unwrap());

        let req = crate::search::SearchRequest {
            query: "x".to_string(),
            collection: Some("rust".to_string()),
            depth: 0,
        };

        let mut stream = dispatch(client, peers, req, 0);

        let outcome = stream.next().await.unwrap();
        let results = outcome.result.unwrap();
        assert_eq!(results.len(), 1, "expected one result");
        assert!(
            (results[0].score - 1.0_f32).abs() < 1e-6,
            "expected score ≈ 1.0 (2.0 * 0.5), got {}",
            results[0].score
        );
    }

    #[test]
    fn returns_only_enabled_pairs_for_matching_collection() {
        let conn = fresh_db();

        // Two node peers
        let n1 = insert_node_peer(&conn, "https://a.example", None).unwrap();
        let n2 = insert_node_peer(&conn, "https://b.example", None).unwrap();

        // Three collection peers: two for "rust", one for "other"
        insert_collection_peer(&conn, "rust", n1, "rust", 0.8).unwrap();
        insert_collection_peer(&conn, "rust", n2, "rust", 1.0).unwrap();
        insert_collection_peer(&conn, "other", n1, "rust", 1.0).unwrap();

        // Disable n2 — its rust pair should be excluded
        set_node_peer_enabled(&conn, n2, false).unwrap();

        let results = active_collection_peers_for(&conn, "rust").unwrap();

        // Only the (rust, n1) pair should appear
        assert_eq!(
            results.len(),
            1,
            "expected exactly 1 result, got {:?}",
            results.len()
        );

        let r = &results[0];
        assert_eq!(r.node_peer_url, "https://a.example");
        assert_eq!(r.node_peer_id, n1, "node_peer_id must match n1");
        assert!(
            (r.source_weight - 0.8).abs() < 1e-6,
            "source_weight mismatch: {}",
            r.source_weight
        );
        assert_eq!(r.source_label, "peer:https://a.example/rust");
    }
}
