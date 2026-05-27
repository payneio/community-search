use tantivy::{doc, schema::Field, DateTime, Index, IndexWriter as TantivyWriter, Term};
use url::Url;

use crate::crawler::error::{CrawlError, CrawlResult};

const WRITER_HEAP_BYTES: usize = 50_000_000;

pub struct IndexWriter {
    writer: TantivyWriter,
    f_collection: Field,
    f_url: Field,
    f_host: Field,
    f_title: Field,
    f_body: Field,
    f_indexed_at: Field,
    /// Pages upserted since the last successful commit. Drained on commit
    /// and returned to the caller so the DB journal can be updated only
    /// after Tantivy confirms durability.
    pending: Vec<JournalEntry>,
}

pub struct Document<'a> {
    pub collection: &'a str,
    pub url: &'a str,
    pub title: &'a str,
    pub body: &'a str,
    pub indexed_at: i64,
    /// Collection UUID (the `id` column in `collections`). Recorded in the
    /// journal so the post-commit DB update can target the right row.
    /// Not indexed by Tantivy — the `collection` *name* is what goes into
    /// the search field.
    pub collection_id: &'a str,
    /// Content fingerprint, mirrored from `crawled_pages.content_hash`.
    /// Recorded in the journal so commit can set `indexed_content_hash`
    /// to this value, marking the doc durable.
    pub content_hash: &'a str,
}

/// One pending upsert tracked by [`IndexWriter`]. Returned in a batch from
/// [`IndexWriter::commit`] *after* the Tantivy commit succeeds so the caller
/// can mark these rows as durably indexed in SQLite.
#[derive(Debug, Clone)]
pub struct JournalEntry {
    pub collection_id: String,
    pub url: String,
    pub content_hash: String,
}

impl IndexWriter {
    pub fn open(index: &Index) -> CrawlResult<Self> {
        let schema = index.schema();

        let f_collection = schema
            .get_field("collection")
            .map_err(|_| CrawlError::Other("field 'collection' not found in schema".into()))?;
        let f_url = schema
            .get_field("url")
            .map_err(|_| CrawlError::Other("field 'url' not found in schema".into()))?;
        let f_host = schema
            .get_field("host")
            .map_err(|_| CrawlError::Other("field 'host' not found in schema".into()))?;
        let f_title = schema
            .get_field("title")
            .map_err(|_| CrawlError::Other("field 'title' not found in schema".into()))?;
        let f_body = schema
            .get_field("body")
            .map_err(|_| CrawlError::Other("field 'body' not found in schema".into()))?;
        let f_indexed_at = schema
            .get_field("indexed_at")
            .map_err(|_| CrawlError::Other("field 'indexed_at' not found in schema".into()))?;

        let writer = index.writer(WRITER_HEAP_BYTES)?;

        Ok(Self {
            writer,
            f_collection,
            f_url,
            f_host,
            f_title,
            f_body,
            f_indexed_at,
            pending: Vec::new(),
        })
    }

    /// Delete any existing document for `d.url`, then add the new document.
    /// This gives upsert (replace-by-URL) semantics. Also records a journal
    /// entry that [`commit`] will return after Tantivy commits durably.
    ///
    /// [`commit`]: Self::commit
    pub fn upsert(&mut self, d: &Document) -> CrawlResult<()> {
        let url_term = Term::from_field_text(self.f_url, d.url);
        self.writer.delete_term(url_term);
        let host = Url::parse(d.url)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase()))
            .unwrap_or_default();
        self.writer.add_document(doc!(
            self.f_collection => d.collection,
            self.f_url       => d.url,
            self.f_host      => host.as_str(),
            self.f_title     => d.title,
            self.f_body      => d.body,
            self.f_indexed_at => DateTime::from_timestamp_secs(d.indexed_at),
        ))?;
        self.pending.push(JournalEntry {
            collection_id: d.collection_id.to_string(),
            url: d.url.to_string(),
            content_hash: d.content_hash.to_string(),
        });
        Ok(())
    }

    /// Remove all documents whose `url` field matches `url`.
    pub fn delete_url(&mut self, url: &str) -> CrawlResult<()> {
        let url_term = Term::from_field_text(self.f_url, url);
        self.writer.delete_term(url_term);
        Ok(())
    }

    /// Flush all pending operations to the index, returning the journal of
    /// upserts that just became durable.
    ///
    /// The Tantivy commit is performed first; only on success are the
    /// pending entries drained and returned. The caller is then responsible
    /// for marking the corresponding rows in `crawled_pages` via
    /// [`crate::db::crawled_pages::mark_indexed_batch`]. If the process
    /// crashes between this commit and that DB update the worst case is a
    /// redundant re-upsert on the next crawl — Tantivy's upsert-by-URL
    /// keeps it idempotent.
    pub fn commit(&mut self) -> CrawlResult<Vec<JournalEntry>> {
        self.writer.commit()?;
        Ok(std::mem::take(&mut self.pending))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::{
        collector::Count,
        query::TermQuery,
        schema::{
            DateOptions, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions,
            STORED, STRING,
        },
        ReloadPolicy,
    };

    /// Build the minimal schema required by IndexWriter.
    fn schema() -> Schema {
        let mut builder: SchemaBuilder = Schema::builder();
        builder.add_text_field("collection", STRING | STORED);
        builder.add_text_field("url", STRING | STORED);
        builder.add_text_field("host", STRING | STORED);
        builder.add_text_field(
            "title",
            TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer("default"))
                .set_stored(),
        );
        builder.add_text_field(
            "body",
            TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer("default"))
                .set_stored(),
        );
        let indexed_at_opts = DateOptions::default().set_indexed().set_stored().set_fast();
        builder.add_date_field("indexed_at", indexed_at_opts);
        builder.build()
    }

    /// Count documents matching `url` exactly, after forcing a reader reload.
    fn count_docs_for_url(index: &Index, url: &str) -> usize {
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        reader.reload().unwrap();
        let searcher = reader.searcher();
        let schema = index.schema();
        let f_url = schema.get_field("url").unwrap();
        let url_term = Term::from_field_text(f_url, url);
        let query = TermQuery::new(url_term, IndexRecordOption::Basic);
        searcher.search(&query, &Count).unwrap()
    }

    fn sample_doc<'a>(url: &'a str, body: &'a str) -> Document<'a> {
        Document {
            collection: "test-collection",
            url,
            title: "Test Title",
            body,
            indexed_at: 1_700_000_000,
            collection_id: "test-collection-id",
            content_hash: "deadbeef",
        }
    }

    #[test]
    fn upsert_adds_a_document() {
        let index = Index::create_in_ram(schema());
        let mut writer = IndexWriter::open(&index).unwrap();

        writer
            .upsert(&sample_doc("https://example.com/page1", "body text"))
            .unwrap();
        writer.commit().unwrap();

        assert_eq!(count_docs_for_url(&index, "https://example.com/page1"), 1);
    }

    #[test]
    fn upsert_replaces_existing_document() {
        // Upsert the same URL 3 times with different bodies.
        // After commit, exactly 1 document must exist for that URL.
        let index = Index::create_in_ram(schema());
        let mut writer = IndexWriter::open(&index).unwrap();
        let url = "https://example.com/page2";

        writer.upsert(&sample_doc(url, "first body")).unwrap();
        writer.upsert(&sample_doc(url, "second body")).unwrap();
        writer.upsert(&sample_doc(url, "third body")).unwrap();
        writer.commit().unwrap();

        // True upsert: only the latest document survives
        assert_eq!(count_docs_for_url(&index, url), 1);
    }

    /// `commit` returns the journal of all upserts since the previous commit,
    /// in upsert order, and clears the pending list afterwards.
    #[test]
    fn commit_returns_pending_journal_then_clears_it() {
        let index = Index::create_in_ram(schema());
        let mut writer = IndexWriter::open(&index).unwrap();

        writer
            .upsert(&Document {
                collection: "tech",
                url: "https://a.example/1",
                title: "t1",
                body: "b1",
                indexed_at: 1_700_000_000,
                collection_id: "col-uuid-tech",
                content_hash: "h1",
            })
            .unwrap();
        writer
            .upsert(&Document {
                collection: "tech",
                url: "https://a.example/2",
                title: "t2",
                body: "b2",
                indexed_at: 1_700_000_001,
                collection_id: "col-uuid-tech",
                content_hash: "h2",
            })
            .unwrap();

        let journal = writer.commit().unwrap();
        assert_eq!(journal.len(), 2);
        assert_eq!(journal[0].url, "https://a.example/1");
        assert_eq!(journal[0].content_hash, "h1");
        assert_eq!(journal[1].url, "https://a.example/2");
        assert_eq!(journal[1].content_hash, "h2");

        // Pending was drained: a second commit with no upserts returns empty.
        let journal2 = writer.commit().unwrap();
        assert!(
            journal2.is_empty(),
            "second commit must drain pending; got {:?}",
            journal2
        );
    }

    /// Dropping the writer without committing must NOT yield any journal —
    /// the simulated "crash before commit" path. The Document was never
    /// durably indexed; the journal would have told the caller to mark
    /// `indexed_content_hash`, and we must not.
    #[test]
    fn dropping_without_commit_loses_pending_silently() {
        let index = Index::create_in_ram(schema());
        {
            let mut writer = IndexWriter::open(&index).unwrap();
            writer
                .upsert(&sample_doc("https://example.com/uncommitted", "body"))
                .unwrap();
            // Simulate process crash: writer dropped before commit.
        }
        // Re-open and query: nothing should be findable.
        assert_eq!(
            count_docs_for_url(&index, "https://example.com/uncommitted"),
            0,
            "uncommitted doc must not be visible after writer drop"
        );
    }

    #[test]
    fn delete_url_removes_document() {
        let index = Index::create_in_ram(schema());
        let mut writer = IndexWriter::open(&index).unwrap();
        let url = "https://example.com/page3";

        writer.upsert(&sample_doc(url, "some body")).unwrap();
        writer.commit().unwrap();

        assert_eq!(
            count_docs_for_url(&index, url),
            1,
            "precondition: doc present"
        );

        writer.delete_url(url).unwrap();
        writer.commit().unwrap();

        assert_eq!(
            count_docs_for_url(&index, url),
            0,
            "doc should be gone after delete"
        );
    }
}
