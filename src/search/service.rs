//! SearchService orchestrating query, rank, and format.

use anyhow::Result;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

use crate::db::ranking_config::{self, RankingConfig};
use crate::index::reader::{ExportedDoc, RawHitWithSnippet, Searcher};
use crate::search::ranking::{self, ScoreInputs};
use crate::search::result::SearchResult;

// ── Struct ─────────────────────────────────────────────────────────────────

/// High-level search service combining the Tantivy index reader with
/// per-collection ranking configuration from SQLite.
pub struct SearchService {
    index: Arc<Searcher>,
    db: Arc<Mutex<Connection>>,
}

impl SearchService {
    /// Create a new `SearchService` from shared index and database handles.
    pub fn new(index: Arc<Searcher>, db: Arc<Mutex<Connection>>) -> Self {
        Self { index, db }
    }

    /// Return every stored document in the local index. Used by the admin
    /// export handler. See `Searcher::export_all_docs` for the cost profile.
    pub fn export_all_docs(&self) -> Result<Vec<ExportedDoc>> {
        self.index.export_all_docs()
    }

    /// Number of documents indexed under the given collection name.
    pub fn count_in_collection(&self, collection: &str) -> Result<u64> {
        self.index.count_in_collection(collection)
    }

    /// Search the local index, apply ranking, and return up to `limit` results.
    ///
    /// Fetches `limit * 4` raw candidates from the index, scores them with the
    /// collection-specific `RankingConfig`, sorts by score descending, and
    /// truncates to `limit`.
    pub fn local_search(
        &self,
        query: &str,
        collection: Option<&str>,
        collection_id_for_ranking: Option<i64>,
        limit: usize,
        now_secs: i64,
    ) -> Result<Vec<SearchResult>> {
        // 1. Fetch raw candidates with HTML snippets.
        let raw_hits: Vec<RawHitWithSnippet> =
            self.index
                .search_with_snippets(query, collection, limit * 4)?;

        // 2. Load ranking config for the collection (or use a default).
        let cfg: RankingConfig = match collection_id_for_ranking {
            Some(id) => {
                let conn = self.db.lock().expect("db mutex poisoned");
                ranking_config::load(&conn, id)?
            }
            None => RankingConfig::default_for(0),
        };

        // 3. Map raw hits → SearchResult, computing the final score.
        let mut results: Vec<SearchResult> = raw_hits
            .into_iter()
            .map(|h| {
                let domain = extract_domain(&h.url);
                let final_score = ranking::score(
                    &ScoreInputs {
                        base_relevance: h.bm25,
                        source: "local",
                        domain: &domain,
                        doc_timestamp_secs: h.timestamp,
                        now_secs,
                    },
                    &cfg,
                );
                SearchResult {
                    title: h.title,
                    url: h.url,
                    snippet_html: h.snippet_html,
                    source: "local".to_string(),
                    timestamp: h.timestamp,
                    score: final_score,
                }
            })
            .collect();

        // 4. Sort descending by score, then truncate to the requested limit.
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);

        Ok(results)
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Extract the lowercase hostname from a URL string.
///
/// - Strips the scheme by splitting on `"://"` (takes the right half).
/// - Takes only the first path segment (splits on `'/'`).
/// - Strips a `user@` prefix if present.
/// - Lowercases the result.
fn extract_domain(url: &str) -> String {
    // Strip scheme ("https://", "http://", etc.)
    let after_scheme = match url.split_once("://") {
        Some((_, rest)) => rest,
        None => url,
    };

    // Take only the host:port part (first segment before any '/')
    let host_and_port = after_scheme.split('/').next().unwrap_or(after_scheme);

    // Strip optional user@ prefix
    let host = host_and_port
        .split('@')
        .next_back()
        .unwrap_or(host_and_port);

    host.to_lowercase()
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_domain_strips_scheme_and_path() {
        assert_eq!(
            extract_domain("https://Example.com/foo"),
            "example.com",
            "https URL with uppercase host"
        );
        assert_eq!(
            extract_domain("http://a.b.c/x?q=1"),
            "a.b.c",
            "http URL with path+query"
        );
        assert_eq!(
            extract_domain("no-scheme.example/path"),
            "no-scheme.example",
            "URL without scheme"
        );
    }
}
