//! SSE event taxonomy for the search stream.
//!
//! Every event emitted on the `/search` SSE stream is one of three types:
//!
//! - `result`         — carries a single [`SearchResult`]
//! - `source_complete` — signals that a named source has finished
//! - `done`           — signals end-of-stream
//!
//! The `#[serde(tag = "type")]` attribute produces an internally-tagged JSON
//! object so Phase 5 peers can decode the stream with a single enum decode.

use serde::{Deserialize, Serialize};

use crate::search::result::SearchResult;

// ── Types ─────────────────────────────────────────────────────────────────────

/// A single event emitted on the SSE search stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SseEvent {
    /// A search result from any source.
    Result(SearchResult),
    /// All results from the named source have been delivered.
    SourceComplete { source: String },
    /// The search stream has ended; no more events will follow.
    Done,
}

// ── Public API ────────────────────────────────────────────────────────────────

impl SseEvent {
    /// The SSE event name (`event:` field in the text/event-stream protocol).
    pub fn name(&self) -> &'static str {
        match self {
            SseEvent::Result(_) => "result",
            SseEvent::SourceComplete { .. } => "source_complete",
            SseEvent::Done => "done",
        }
    }

    /// Serialize the event to a JSON string for the SSE `data:` field.
    ///
    /// # Panics
    ///
    /// Panics if serde_json fails to serialize — this should never happen for
    /// well-formed variants of this enum.
    pub fn data_json(&self) -> String {
        serde_json::to_string(self).expect("SseEvent serialization must not fail")
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::result::SearchResult;

    fn make_result(title: &str) -> SearchResult {
        SearchResult {
            title: title.to_string(),
            url: "http://example.com".to_string(),
            snippet_html: "<p>snippet</p>".to_string(),
            source: "test".to_string(),
            timestamp: 0,
            score: 1.0,
        }
    }

    #[test]
    fn result_event_name_and_payload() {
        let event = SseEvent::Result(make_result("t"));
        assert_eq!(event.name(), "result");
        let json = event.data_json();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "result");
        assert_eq!(v["title"], "t");
    }

    #[test]
    fn source_complete_event() {
        let event = SseEvent::SourceComplete {
            source: "local".to_string(),
        };
        assert_eq!(event.name(), "source_complete");
        let json = event.data_json();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["source"], "local");
    }

    #[test]
    fn done_event() {
        let event = SseEvent::Done;
        assert_eq!(event.name(), "done");
        let json = event.data_json();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "done");
    }
}
