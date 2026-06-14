//! Peer communication trait and HTTP implementation.

use anyhow::Result;
use async_trait::async_trait;

use crate::collections::CollectionInfo;
use crate::search::{SearchRequest, SearchResult};

/// Contract for communicating with a remote community-search peer node.
///
/// The trait is the seam that lets `HttpPeerClient` be swapped for a
/// libp2p backend (or a test double) without touching the rest of the system.
#[async_trait]
pub trait PeerClient: Send + Sync {
    /// Forward a search query to the peer and collect all results.
    async fn search(&self, url: &str, query: &SearchRequest) -> Result<Vec<SearchResult>>;

    /// Retrieve the list of collections advertised by the peer.
    async fn list_collections(&self, url: &str) -> Result<Vec<CollectionInfo>>;

    /// Return `true` if the peer responds to a health probe, `false` otherwise.
    async fn health_check(&self, url: &str) -> Result<bool>;
}

use reqwest::Client;
use std::time::Duration;

pub struct HttpPeerClient {
    inner: Client,
}

impl HttpPeerClient {
    pub fn new(timeout: Duration) -> Result<Self> {
        let inner = Client::builder()
            .timeout(timeout)
            .user_agent(concat!("community-search/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self { inner })
    }
}

#[async_trait]
impl PeerClient for HttpPeerClient {
    async fn search(&self, url: &str, query: &SearchRequest) -> Result<Vec<SearchResult>> {
        use eventsource_stream::Eventsource;
        use futures::StreamExt;
        let target = format!("{}/api/search", url.trim_end_matches('/'));
        let resp = self.inner.post(&target).json(query).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("peer {} search returned status {}", url, resp.status());
        }
        let mut stream = resp.bytes_stream().eventsource();
        let mut out = Vec::new();
        while let Some(event) = stream.next().await {
            let event = event?;
            match event.event.as_str() {
                "result" => {
                    let r: SearchResult = serde_json::from_str(&event.data)?;
                    out.push(r);
                }
                "complete" => break,
                _ => {}
            }
        }
        Ok(out)
    }

    async fn list_collections(&self, url: &str) -> Result<Vec<CollectionInfo>> {
        #[derive(serde::Deserialize)]
        struct Envelope {
            collections: Vec<CollectionInfo>,
        }
        let target = format!("{}/api/collections", url.trim_end_matches('/'));
        let resp = self.inner.get(&target).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("peer {} returned status {}", url, resp.status());
        }
        let env: Envelope = resp.json().await?;
        Ok(env.collections)
    }

    async fn health_check(&self, url: &str) -> Result<bool> {
        let target = format!("{}/api/collections", url.trim_end_matches('/'));
        let resp = self.inner.get(&target).send().await;
        match resp {
            Ok(r) => Ok(r.status().is_success()),
            Err(_) => Ok(false),
        }
    }
}

impl HttpPeerClient {
    /// Search a peer and overwrite each result's `source` field with `source_label`.
    ///
    /// Peer nodes return results tagged with `source = "local"` (from their own
    /// perspective). Callers use this method to stamp results with an attributable
    /// label such as `"peer:example.com/rust"` before merging with local results.
    pub async fn search_tagged(
        &self,
        url: &str,
        query: &SearchRequest,
        source_label: &str,
    ) -> Result<Vec<SearchResult>> {
        let mut results = <Self as PeerClient>::search(self, url, query).await?;
        for r in &mut results {
            r.source = source_label.to_string();
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn http_peer_client_can_be_constructed_with_default_timeout() {
        let result = HttpPeerClient::new(Duration::from_secs(5));
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn search_consumes_sse_stream_and_returns_results() {
        let server = MockServer::start().await;

        let result_a = serde_json::json!({
            "title": "A",
            "url": "https://a.example/1",
            "snippet_html": "",
            "source": "peer",
            "timestamp": 0,
            "score": 1.0
        });
        let result_b = serde_json::json!({
            "title": "B",
            "url": "https://b.example/2",
            "snippet_html": "",
            "source": "peer",
            "timestamp": 0,
            "score": 1.0
        });

        let sse_body = format!(
            "event: result\ndata: {}\n\nevent: result\ndata: {}\n\nevent: complete\ndata: {{}}\n\n",
            result_a, result_b
        );

        Mock::given(method("POST"))
            .and(path("/api/search"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/event-stream")
                    .set_body_string(sse_body),
            )
            .mount(&server)
            .await;

        let client = HttpPeerClient::new(Duration::from_secs(5)).unwrap();
        let req = SearchRequest {
            query: "hi".to_string(),
            collection: Some("rust".to_string()),
            depth: 0,
        };
        let results = client.search(&server.uri(), &req).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].url, "https://a.example/1");
        assert_eq!(results[1].url, "https://b.example/2");
    }

    #[tokio::test]
    async fn list_collections_parses_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/collections"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocol_version": "1.0",
                "collections": [
                    {"name": "rust", "description": "Rust-related sites"},
                    {"name": "gardening", "description": null}
                ]
            })))
            .mount(&server)
            .await;

        let client = HttpPeerClient::new(Duration::from_secs(5)).unwrap();
        let cols = client.list_collections(&server.uri()).await.unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "rust");
        assert_eq!(cols[1].name, "gardening");
    }

    #[tokio::test]
    async fn list_collections_errors_on_non_200() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/collections"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let client = HttpPeerClient::new(Duration::from_secs(5)).unwrap();
        let result = client.list_collections(&server.uri()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn health_check_returns_true_on_200() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/collections"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocol_version": "1.0",
                "collections": []
            })))
            .mount(&server)
            .await;

        let client = HttpPeerClient::new(Duration::from_secs(5)).unwrap();
        let result = client.health_check(&server.uri()).await;
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn health_check_returns_false_on_500() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/collections"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let client = HttpPeerClient::new(Duration::from_secs(5)).unwrap();
        let result = client.health_check(&server.uri()).await;
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn search_overwrites_source_field_when_caller_provides_label() {
        let server = MockServer::start().await;

        let result_json = serde_json::json!({
            "title": "Example",
            "url": "https://example.com/1",
            "snippet_html": "",
            "source": "local",
            "timestamp": 0,
            "score": 1.0
        });

        let sse_body = format!(
            "event: result\ndata: {}\n\nevent: complete\ndata: {{}}\n\n",
            result_json
        );

        Mock::given(method("POST"))
            .and(path("/api/search"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/event-stream")
                    .set_body_string(sse_body),
            )
            .mount(&server)
            .await;

        let client = HttpPeerClient::new(Duration::from_secs(5)).unwrap();
        let req = SearchRequest {
            query: "rust".to_string(),
            collection: Some("rust".to_string()),
            depth: 0,
        };
        let results = client
            .search_tagged(&server.uri(), &req, "peer:example.com/rust")
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].source, "peer:example.com/rust");
    }
}
