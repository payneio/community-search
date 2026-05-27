use anyhow::{Context, Result};
use tantivy::{
    collector::TopDocs,
    query::{AllQuery, BooleanQuery, Occur, Query, QueryParser, TermQuery},
    schema::{Field, IndexRecordOption, OwnedValue},
    snippet::SnippetGenerator,
    Index, IndexReader, ReloadPolicy, TantivyDocument, Term,
};

// ---------------------------------------------------------------------------
// Field-name constants
// Note: F_TIMESTAMP maps to "indexed_at" — the actual name chosen in Phase 2.
// ---------------------------------------------------------------------------
pub const F_TITLE: &str = "title";
pub const F_BODY: &str = "body";
pub const F_URL: &str = "url";
pub const F_COLLECTION: &str = "collection";
pub const F_TIMESTAMP: &str = "indexed_at";

/// Boost applied to the `title` field in the QueryParser. Tuned so that
/// title hits outrank body hits of similar BM25 score — without title weighting,
/// a 5-word title and a 5000-word body compete on equal footing per term.
const TITLE_BOOST: f32 = 3.0;

/// Rewrite Google-style `site:example.com` clauses to `+host:example.com`
/// so they hit the indexed `host` field and are required (MUST). The leading
/// `+` matters under default-OR semantics — otherwise `tokio site:foo` would
/// parse as `tokio OR host:foo` and let any document containing "tokio"
/// slip past the filter.
///
/// Tokens inside quoted phrases are left alone, so `"site:foo"` as a literal
/// phrase stays intact. A `site:` token must be at a word boundary to match.
fn rewrite_site_filter(q: &str) -> String {
    let mut out = String::with_capacity(q.len());
    let mut in_quote = false;
    let mut chars = q.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' {
            in_quote = !in_quote;
            out.push(c);
            continue;
        }
        if !in_quote && c == 's' {
            let at_boundary = out
                .chars()
                .last()
                .map(|p| p.is_whitespace() || p == '(' || p == '-' || p == '+')
                .unwrap_or(true);
            if at_boundary {
                let rest: String = chars.clone().take(4).collect();
                if rest == "ite:" {
                    out.push_str("+host:");
                    for _ in 0..4 {
                        chars.next();
                    }
                    continue;
                }
            }
        }
        out.push(c);
    }
    out
}

// ---------------------------------------------------------------------------
// RawHit — one result returned from the index
// ---------------------------------------------------------------------------
#[derive(Debug)]
pub struct RawHit {
    pub title: String,
    pub url: String,
    pub body: String,
    pub timestamp: i64,
    pub bm25: f32,
}

// ---------------------------------------------------------------------------
// ExportedDoc — one stored document retrieved by Searcher::export_all_docs.
// Used by the admin export endpoint to ship the full index contents off-box.
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct ExportedDoc {
    pub collection: String,
    pub url: String,
    pub title: String,
    pub body: String,
    pub indexed_at: i64,
}

// ---------------------------------------------------------------------------
// RawHitWithSnippet — one result with HTML-highlighted snippet
// ---------------------------------------------------------------------------
#[derive(Debug)]
pub struct RawHitWithSnippet {
    pub title: String,
    pub url: String,
    pub snippet_html: String,
    pub timestamp: i64,
    pub bm25: f32,
}

// ---------------------------------------------------------------------------
// Searcher — wraps Index + IndexReader + resolved Field handles
// ---------------------------------------------------------------------------
pub struct Searcher {
    index: Index,
    reader: IndexReader,
    f_title: Field,
    f_body: Field,
    f_url: Field,
    f_collection: Field,
    f_timestamp: Field,
}

impl Searcher {
    /// Open a `Searcher` against an already-built `Index`.
    /// Returns an error if any required schema field is missing.
    pub fn open(index: Index) -> Result<Self> {
        let schema = index.schema();

        let f_title = schema
            .get_field(F_TITLE)
            .with_context(|| format!("missing field '{F_TITLE}'"))?;
        let f_body = schema
            .get_field(F_BODY)
            .with_context(|| format!("missing field '{F_BODY}'"))?;
        let f_url = schema
            .get_field(F_URL)
            .with_context(|| format!("missing field '{F_URL}'"))?;
        let f_collection = schema
            .get_field(F_COLLECTION)
            .with_context(|| format!("missing field '{F_COLLECTION}'"))?;
        let f_timestamp = schema
            .get_field(F_TIMESTAMP)
            .with_context(|| format!("missing field '{F_TIMESTAMP}'"))?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .context("failed to create IndexReader")?;

        Ok(Self {
            index,
            reader,
            f_title,
            f_body,
            f_url,
            f_collection,
            f_timestamp,
        })
    }

    /// Run a BM25 query against `[title, body]`.
    ///
    /// When `collection` is `Some`, results are filtered to that collection
    /// via a MUST `TermQuery` combined with the user query in a `BooleanQuery`.
    pub fn search(
        &self,
        query_text: &str,
        collection: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RawHit>> {
        let searcher = self.reader.searcher();
        let mut parser = QueryParser::for_index(&self.index, vec![self.f_title, self.f_body]);
        parser.set_field_boost(self.f_title, TITLE_BOOST);

        let rewritten = rewrite_site_filter(query_text);
        let user_q = parser.parse_query(&rewritten)?;

        let query: Box<dyn Query> = match collection {
            Some(coll) => {
                let term = Term::from_field_text(self.f_collection, coll);
                let coll_q = TermQuery::new(term, IndexRecordOption::Basic);
                Box::new(BooleanQuery::new(vec![
                    (Occur::Must, Box::new(user_q) as Box<dyn Query>),
                    (Occur::Must, Box::new(coll_q) as Box<dyn Query>),
                ]))
            }
            None => Box::new(user_q),
        };

        let top_docs = searcher.search(&*query, &TopDocs::with_limit(limit))?;

        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, addr) in top_docs {
            let doc: TantivyDocument = searcher.doc(addr)?;
            hits.push(RawHit {
                title: first_text(&doc, self.f_title),
                url: first_text(&doc, self.f_url),
                body: first_text(&doc, self.f_body),
                timestamp: first_i64(&doc, self.f_timestamp),
                bm25: score,
            });
        }

        Ok(hits)
    }

    /// Iterate every stored document in the index and return its retrievable
    /// fields. Used by the admin export endpoint.
    ///
    /// Tantivy has no public segment iterator, so this runs an `AllQuery`
    /// with `TopDocs::with_limit(num_docs)` to fetch every doc address, then
    /// resolves each through `searcher.doc(addr)`. Suitable for the moderate
    /// indices a curated community-search instance is expected to hold; not
    /// intended for indices in the millions-of-documents range.
    pub fn export_all_docs(&self) -> Result<Vec<ExportedDoc>> {
        let searcher = self.reader.searcher();
        let total = searcher.num_docs() as usize;
        if total == 0 {
            return Ok(Vec::new());
        }
        let top = searcher.search(&AllQuery, &TopDocs::with_limit(total))?;
        let mut out = Vec::with_capacity(top.len());
        for (_, addr) in top {
            let doc: TantivyDocument = searcher.doc(addr)?;
            out.push(ExportedDoc {
                collection: first_text(&doc, self.f_collection),
                url: first_text(&doc, self.f_url),
                title: first_text(&doc, self.f_title),
                body: first_text(&doc, self.f_body),
                indexed_at: first_i64(&doc, self.f_timestamp),
            });
        }
        Ok(out)
    }

    /// Run a BM25 query against `[title, body]` and return HTML-highlighted snippets.
    ///
    /// The snippet is generated from the `body` field using Tantivy's `SnippetGenerator`.
    /// Query terms are wrapped in `<b>` tags in the returned HTML. When the snippet
    /// generator produces no excerpt (e.g. the query matched only the title), the first
    /// 220 characters of the body are returned as a plain-text fallback.
    ///
    /// When `collection` is `Some`, results are filtered to that collection.
    pub fn search_with_snippets(
        &self,
        query_text: &str,
        collection: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RawHitWithSnippet>> {
        let searcher = self.reader.searcher();
        let mut parser = QueryParser::for_index(&self.index, vec![self.f_title, self.f_body]);
        parser.set_field_boost(self.f_title, TITLE_BOOST);

        let rewritten = rewrite_site_filter(query_text);
        let user_q = parser.parse_query(&rewritten)?;

        let mut snip_gen = SnippetGenerator::create(&searcher, &*user_q, self.f_body)?;
        snip_gen.set_max_num_chars(220);

        let query: Box<dyn Query> = match collection {
            Some(coll) => {
                let term = Term::from_field_text(self.f_collection, coll);
                let coll_q = TermQuery::new(term, IndexRecordOption::Basic);
                Box::new(BooleanQuery::new(vec![
                    (Occur::Must, user_q.box_clone()),
                    (Occur::Must, Box::new(coll_q) as Box<dyn Query>),
                ]))
            }
            None => user_q,
        };

        let top_docs = searcher.search(&*query, &TopDocs::with_limit(limit))?;

        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, addr) in top_docs {
            let doc: TantivyDocument = searcher.doc(addr)?;
            let snippet = snip_gen.snippet_from_doc(&doc);
            let html: String = if snippet.is_empty() {
                first_text(&doc, self.f_body).chars().take(220).collect()
            } else {
                snippet.to_html()
            };
            hits.push(RawHitWithSnippet {
                title: first_text(&doc, self.f_title),
                url: first_text(&doc, self.f_url),
                snippet_html: html,
                timestamp: first_i64(&doc, self.f_timestamp),
                bm25: score,
            });
        }

        Ok(hits)
    }
}

// ---------------------------------------------------------------------------
// Field-value helpers
// ---------------------------------------------------------------------------

fn first_text(doc: &TantivyDocument, field: Field) -> String {
    doc.get_first(field)
        .and_then(|v| {
            if let OwnedValue::Str(s) = v {
                Some(s.clone())
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn first_i64(doc: &TantivyDocument, field: Field) -> i64 {
    doc.get_first(field)
        .and_then(|v| match v {
            OwnedValue::I64(n) => Some(*n),
            OwnedValue::Date(dt) => Some((*dt).into_timestamp_secs()),
            _ => None,
        })
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{
        schema,
        writer::{Document, IndexWriter},
    };

    /// Build an in-RAM index containing two documents:
    /// - collection "tech":    title="Rust async",         body="Tokio",      url="a.example/1"
    /// - collection "cooking": title="Cooking with Rust",  body="cast iron",  url="a.example/2"
    fn build_test_index() -> Index {
        let index = Index::create_in_ram(schema::build());
        let mut writer = IndexWriter::open(&index).unwrap();

        writer
            .upsert(&Document {
                collection: "tech",
                url: "a.example/1",
                title: "Rust async",
                body: "Tokio",
                indexed_at: 1_700_000_000,
                collection_id: "tech",
                content_hash: "",
            })
            .unwrap();

        writer
            .upsert(&Document {
                collection: "cooking",
                url: "a.example/2",
                title: "Cooking with Rust",
                body: "cast iron",
                indexed_at: 1_700_000_001,
                collection_id: "cooking",
                content_hash: "",
            })
            .unwrap();

        writer.commit().unwrap();
        index
    }

    #[test]
    fn search_filters_by_collection() {
        let index = build_test_index();
        let searcher = Searcher::open(index).unwrap();

        let hits = searcher.search("rust", Some("tech"), 10).unwrap();

        assert_eq!(hits.len(), 1, "expected exactly 1 hit in 'tech'");
        assert_eq!(hits[0].url, "a.example/1");
    }

    #[test]
    fn search_all_collections_when_none() {
        let index = build_test_index();
        let searcher = Searcher::open(index).unwrap();

        let hits = searcher.search("rust", None, 10).unwrap();

        assert_eq!(hits.len(), 2, "expected 2 hits across all collections");
    }

    #[test]
    fn no_matches_returns_empty() {
        let index = build_test_index();
        let searcher = Searcher::open(index).unwrap();

        let hits = searcher.search("zzznomatch", None, 10).unwrap();

        assert!(hits.is_empty(), "expected no hits for an unindexed term");
    }

    #[test]
    fn rewrite_site_filter_basic() {
        assert_eq!(rewrite_site_filter("site:example.com"), "+host:example.com");
        assert_eq!(
            rewrite_site_filter("rust site:example.com"),
            "rust +host:example.com"
        );
        // Quoted phrase containing the literal "site:" must be preserved.
        assert_eq!(rewrite_site_filter("\"site:foo\""), "\"site:foo\"");
        // Mid-word "site" should not be rewritten.
        assert_eq!(rewrite_site_filter("offsite:foo"), "offsite:foo");
        // No-op on empty / no-match input.
        assert_eq!(rewrite_site_filter("just words"), "just words");
    }

    #[test]
    fn site_filter_matches_only_indexed_host() {
        let index = Index::create_in_ram(schema::build());
        let mut writer = IndexWriter::open(&index).unwrap();

        writer
            .upsert(&Document {
                collection: "tech",
                url: "https://rust-lang.org/learn",
                title: "Learn Rust",
                body: "tokio async",
                indexed_at: 1_700_000_000,
                collection_id: "tech",
                content_hash: "",
            })
            .unwrap();
        writer
            .upsert(&Document {
                collection: "tech",
                url: "https://example.com/rust",
                title: "Rust elsewhere",
                body: "tokio async",
                indexed_at: 1_700_000_001,
                collection_id: "tech",
                content_hash: "",
            })
            .unwrap();
        writer.commit().unwrap();

        let searcher = Searcher::open(index).unwrap();
        let hits = searcher
            .search("tokio site:rust-lang.org", None, 10)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].url, "https://rust-lang.org/learn");
    }

    #[test]
    fn title_match_outranks_body_match() {
        let index = Index::create_in_ram(schema::build());
        let mut writer = IndexWriter::open(&index).unwrap();

        writer
            .upsert(&Document {
                collection: "tech",
                url: "https://a.example/title-hit",
                title: "Quantum entanglement",
                body: "a long article about other things",
                indexed_at: 1_700_000_000,
                collection_id: "tech",
                content_hash: "",
            })
            .unwrap();
        writer
            .upsert(&Document {
                collection: "tech",
                url: "https://a.example/body-hit",
                title: "Some article",
                body: "discusses quantum entanglement briefly",
                indexed_at: 1_700_000_001,
                collection_id: "tech",
                content_hash: "",
            })
            .unwrap();
        writer.commit().unwrap();

        let searcher = Searcher::open(index).unwrap();
        let hits = searcher.search("quantum entanglement", None, 10).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(
            hits[0].url, "https://a.example/title-hit",
            "title hit must outrank body hit with TITLE_BOOST"
        );
    }

    #[test]
    fn phrase_query_matches_adjacent_terms_only() {
        let index = Index::create_in_ram(schema::build());
        let mut writer = IndexWriter::open(&index).unwrap();

        writer
            .upsert(&Document {
                collection: "tech",
                url: "a.example/phrase-1",
                title: "Async Rust",
                body: "hello world from tokio",
                indexed_at: 1_700_000_000,
                collection_id: "tech",
                content_hash: "",
            })
            .unwrap();
        writer
            .upsert(&Document {
                collection: "tech",
                url: "a.example/phrase-2",
                title: "Other",
                body: "world hello from tokio",
                indexed_at: 1_700_000_001,
                collection_id: "tech",
                content_hash: "",
            })
            .unwrap();
        writer.commit().unwrap();

        let searcher = Searcher::open(index).unwrap();

        let hits = searcher.search("\"hello world\"", None, 10).unwrap();
        assert_eq!(
            hits.len(),
            1,
            "phrase should match only adjacent occurrence"
        );
        assert_eq!(hits[0].url, "a.example/phrase-1");
    }

    #[test]
    fn search_with_snippets_marks_query_terms() {
        let index = build_test_index();
        let searcher = Searcher::open(index).unwrap();

        let hits = searcher
            .search_with_snippets("tokio", Some("tech"), 10)
            .unwrap();

        assert_eq!(
            hits.len(),
            1,
            "expected exactly 1 hit in 'tech' for 'tokio'"
        );
        assert_eq!(hits[0].url, "a.example/1");
        assert!(
            !hits[0].snippet_html.is_empty(),
            "snippet_html should not be empty"
        );
        assert!(
            hits[0].snippet_html.to_lowercase().contains("tokio"),
            "snippet_html should contain 'tokio' but was: {:?}",
            hits[0].snippet_html
        );
    }
}
