//! Search result type and snippet-formatting utilities.

use serde::{Deserialize, Serialize};

// ── Types ─────────────────────────────────────────────────────────────────────

/// A single search result returned to callers.
///
/// **Wire-format stability:** field names are part of the P2P protocol; rename
/// only when bumping the protocol version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    pub title: String,
    pub url: String,
    pub snippet_html: String,
    pub source: String,
    pub timestamp: i64,
    pub score: f32,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Escape `& < > "` for safe HTML embedding.
pub fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

/// Wrap every occurrence of `needle` in `haystack` with `pre`/`post`,
/// matching case-insensitively while preserving the original casing of the
/// matched text.
///
/// Walks lowercased indices so case is preserved in output.
pub fn case_insensitive_wrap(haystack: &str, needle: &str, pre: &str, post: &str) -> String {
    if needle.is_empty() {
        return haystack.to_string();
    }

    let lower_haystack = haystack.to_lowercase();
    let lower_needle = needle.to_lowercase();
    let needle_len = lower_needle.len();

    let mut out = String::with_capacity(haystack.len() + 32);
    let mut start = 0;

    while start < lower_haystack.len() {
        match lower_haystack[start..].find(&lower_needle) {
            Some(rel_pos) => {
                let abs_pos = start + rel_pos;
                out.push_str(&haystack[start..abs_pos]);
                out.push_str(pre);
                out.push_str(&haystack[abs_pos..abs_pos + needle_len]);
                out.push_str(post);
                start = abs_pos + needle_len;
            }
            None => {
                out.push_str(&haystack[start..]);
                return out;
            }
        }
    }
    out
}

/// Build a fallback snippet by HTML-escaping `text` then wrapping each
/// non-empty query term in `<mark>…</mark>`.
///
/// Used when `SnippetGenerator` yields no fragments.
pub fn highlight_fallback(text: &str, query_terms: &[&str]) -> String {
    let mut out = html_escape(text);
    for &term in query_terms {
        if !term.is_empty() {
            out = case_insensitive_wrap(&out, term, "<mark>", "</mark>");
        }
    }
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_then_highlight() {
        // "<" is escaped to "&lt;" then "rust" is case-insensitively wrapped
        let result = highlight_fallback("Rust <3 search", &["rust"]);
        assert_eq!(result, "<mark>Rust</mark> &lt;3 search");
    }

    #[test]
    fn multiple_terms_all_marked() {
        let result = highlight_fallback("Hello World", &["hello", "world"]);
        assert_eq!(result, "<mark>Hello</mark> <mark>World</mark>");
    }

    #[test]
    fn empty_term_is_skipped() {
        // The empty string must not produce spurious <mark></mark> wrapping
        let result = highlight_fallback("Hello", &["", "hello"]);
        assert_eq!(result, "<mark>Hello</mark>");
    }

    #[test]
    fn result_serializes_as_expected_fields() {
        let r = SearchResult {
            title: "My Title".to_string(),
            url: "https://example.com".to_string(),
            snippet_html: "<mark>hello</mark> world".to_string(),
            source: "local".to_string(),
            timestamp: 1_700_000_000,
            score: 0.95,
        };
        let json: serde_json::Value = serde_json::to_value(&r).unwrap();
        let obj = json.as_object().unwrap();
        assert!(obj.contains_key("title"), "missing 'title' key");
        assert!(obj.contains_key("url"), "missing 'url' key");
        assert!(
            obj.contains_key("snippet_html"),
            "missing 'snippet_html' key"
        );
        assert!(obj.contains_key("source"), "missing 'source' key");
        assert!(obj.contains_key("timestamp"), "missing 'timestamp' key");
        assert!(obj.contains_key("score"), "missing 'score' key");
    }
}
