use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use crate::{
    crawler::{
        fetcher::{FetchOutcome, FetchRequest, Fetcher},
        parser::{extract_canonical, extract_links, parse_text},
        robots::RobotsChecker,
        url_class::{host_of, is_blacklisted_outlink_host, is_in_prefix, normalize_url},
        CrawlError, CrawlResult,
    },
    db::{
        crawled_pages::{self, CrawledPageRow},
        outlink_hosts::{self, OutlinkExample},
        Database,
    },
    index::indexer::IndexJob,
};

// ── Public types ──────────────────────────────────────────────────────────────

/// Context required to crawl a single page within a collection.
pub struct PageContext {
    pub collection_id: String,
    pub crawl_target_id: String,
    /// URL prefix used to classify discovered links as in-prefix vs outlinks.
    pub url_prefix: String,
    /// Name of the collection — stored in the search index per document.
    pub collection_name: String,
}

/// Outcome of a single-page crawl operation.
pub struct PageResult {
    /// Absolute, normalised URLs found on the page that fall within the prefix.
    pub in_prefix_links: Vec<String>,
    /// `true` if the page was (re-)indexed in this call.
    pub indexed: bool,
    /// `true` if the server returned 304 Not Modified (content unchanged).
    pub not_modified: bool,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Crawl a single page, updating the search index and persistent crawl state.
///
/// # Steps
/// 1. Check `robots.txt` — returns `Err(RobotsDisallowed)` if disallowed.
/// 2. Load any prior crawl row (for conditional-request headers).
/// 3. Fetch with `If-None-Match` / `If-Modified-Since` if available.
/// 4. **429 Too Many Requests** — propagate `RateLimited` to the caller.
/// 5. **304 Not Modified** — update `last_crawled_at`/`last_status`, skip re-index.
/// 6. **Non-2xx status** — record the status, preserve prior cache headers.
/// 7. **Fresh** — parse, hash, conditionally index, upsert DB, classify links.
#[allow(clippy::too_many_arguments)]
pub async fn crawl_page(
    url: &str,
    ctx: &PageContext,
    fetcher: &Fetcher,
    robots: &RobotsChecker,
    db: &Database,
    indexer_tx: &mpsc::Sender<IndexJob>,
    indexing_inflight: &std::sync::atomic::AtomicI64,
    now_unix: i64,
) -> CrawlResult<PageResult> {
    // ── Step 1: Robots check ──────────────────────────────────────────────────
    if !robots.is_allowed(url).await? {
        return Err(CrawlError::RobotsDisallowed(url.to_string()));
    }

    // ── Step 2: Load prior crawl state ────────────────────────────────────────
    // Lock per-op: acquire the conn, do the read, drop the guard before any
    // `.await`. With multiple concurrent crawler tasks this is what allows
    // them to actually interleave — holding the guard across an HTTP fetch
    // would serialise everything via the shared `Mutex<Connection>`.
    let prior = {
        let conn = db.conn();
        crawled_pages::get_by_url(&conn, &ctx.collection_id, url)
            .map_err(|e| CrawlError::Other(e.to_string()))?
    };

    // ── Step 3: Fetch (with conditional headers when available) ───────────────
    let fetch_req = FetchRequest {
        url: url.to_string(),
        etag: prior.as_ref().and_then(|p| p.etag.clone()),
        last_modified: prior.as_ref().and_then(|p| p.last_modified.clone()),
    };
    let outcome = fetcher.fetch(&fetch_req).await?;

    match outcome {
        // ── Step 4: 429 Too Many Requests ─────────────────────────────────────
        // Propagate rate-limit information back to the driver so it can apply
        // appropriate back-off logic.  No DB write is performed.
        FetchOutcome::RateLimited { retry_after } => Err(CrawlError::RateLimited { retry_after }),

        // ── Step 5: 304 Not Modified ──────────────────────────────────────────
        FetchOutcome::NotModified => {
            let row = CrawledPageRow {
                id: 0,
                collection_id: ctx.collection_id.clone(),
                crawl_target_id: ctx.crawl_target_id.clone(),
                url: url.to_string(),
                // Preserve existing cache validators.
                etag: prior.as_ref().and_then(|p| p.etag.clone()),
                last_modified: prior.as_ref().and_then(|p| p.last_modified.clone()),
                content_hash: prior.as_ref().and_then(|p| p.content_hash.clone()),
                last_status: Some(304),
                last_crawled_at: now_unix,
                // No new body fetched; carry the existing journal state forward.
                indexed_content_hash: prior.as_ref().and_then(|p| p.indexed_content_hash.clone()),
            };
            {
                let conn = db.conn();
                crawled_pages::upsert(&conn, &row).map_err(|e| CrawlError::Other(e.to_string()))?;
            }

            Ok(PageResult {
                in_prefix_links: vec![],
                indexed: false,
                not_modified: true,
            })
        }

        // ── Step 6: Non-2xx status ────────────────────────────────────────────
        FetchOutcome::OtherStatus { status } => {
            let row = CrawledPageRow {
                id: 0,
                collection_id: ctx.collection_id.clone(),
                crawl_target_id: ctx.crawl_target_id.clone(),
                url: url.to_string(),
                // Preserve existing cache validators; they may still be valid.
                etag: prior.as_ref().and_then(|p| p.etag.clone()),
                last_modified: prior.as_ref().and_then(|p| p.last_modified.clone()),
                content_hash: prior.as_ref().and_then(|p| p.content_hash.clone()),
                last_status: Some(i64::from(status)),
                last_crawled_at: now_unix,
                indexed_content_hash: prior.as_ref().and_then(|p| p.indexed_content_hash.clone()),
            };
            {
                let conn = db.conn();
                crawled_pages::upsert(&conn, &row).map_err(|e| CrawlError::Other(e.to_string()))?;
            }

            Ok(PageResult {
                in_prefix_links: vec![],
                indexed: false,
                not_modified: false,
            })
        }

        // ── Step 7: Fresh 2xx HTML response ───────────────────────────────────
        FetchOutcome::Fresh {
            status,
            body,
            etag,
            last_modified,
            final_url,
        } => {
            // Parse content. Relative URLs inside the body resolve against
            // `final_url` (where the content actually came from), not the
            // requested URL — they would otherwise be wrong on redirect.
            let parsed = parse_text(&body);
            let extracted = extract_links(&body, &final_url);

            // Decide which URL this content should be indexed under. Three
            // signals collapse aliased URLs onto a single index entry:
            //   1. HTTP redirect target (`final_url`) — server's authoritative
            //      statement about where the content lives. Highest priority.
            //   2. `<link rel="canonical">` — soft hint from the page.
            //   3. Fall back to the requested URL.
            // Out-of-prefix targets are ignored (a site can't trick us into
            // indexing content we weren't authorized to crawl). The chosen
            // URL is also surfaced to the scheduler so it gets crawled
            // directly and earns its own `crawled_pages` row.
            let normalized_fetched = normalize_url(url);
            let redirect_url: Option<String> = normalize_url(&final_url)
                .filter(|c| Some(c) != normalized_fetched.as_ref())
                .filter(|c| is_in_prefix(c, &ctx.url_prefix));
            let canonical_url: Option<String> = extract_canonical(&body, &final_url)
                .and_then(|c| normalize_url(&c))
                .filter(|c| Some(c) != normalized_fetched.as_ref())
                .filter(|c| redirect_url.as_ref() != Some(c))
                .filter(|c| is_in_prefix(c, &ctx.url_prefix));
            let index_url: String = redirect_url
                .clone()
                .or_else(|| canonical_url.clone())
                .unwrap_or_else(|| url.to_string());

            // SHA-256 of (title + NUL + text) as content fingerprint
            let mut hasher = Sha256::new();
            hasher.update(parsed.title.as_bytes());
            hasher.update(b"\0");
            hasher.update(parsed.text.as_bytes());
            let content_hash = format!("{:x}", hasher.finalize());

            // Skip re-index only when the *durably committed* version of this
            // URL already matches the new content. We deliberately gate on
            // `indexed_content_hash`, not `content_hash`: a previous crawl
            // that fetched the body but crashed before the Tantivy commit
            // left `content_hash` set but `indexed_content_hash` NULL, so
            // this guard correctly re-indexes on the next run.
            let already_indexed = prior
                .as_ref()
                .and_then(|p| p.indexed_content_hash.as_deref())
                == Some(content_hash.as_str());

            if !already_indexed {
                // Hand the doc off to the dedicated indexer task. The bounded
                // mpsc applies natural backpressure: if commits stall, this
                // await blocks the crawler, which is exactly the polite thing
                // to do. A SendError means the indexer task has exited —
                // there's nothing useful we can do besides surface it.
                //
                // Increment the in-flight counter *before* the send so a
                // concurrent `/api/admin/status` snapshot can't observe an
                // already-queued job as zero-pending. The indexer's flush
                // decrements by the journal length once Tantivy commits.
                indexing_inflight.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let Err(e) = indexer_tx
                    .send(IndexJob {
                        collection_name: ctx.collection_name.clone(),
                        url: index_url.clone(),
                        title: parsed.title.clone(),
                        body: parsed.text.clone(),
                        indexed_at: now_unix,
                        collection_id: ctx.collection_id.clone(),
                        content_hash: content_hash.clone(),
                    })
                    .await
                {
                    // Send failed → job is not actually in flight; roll back.
                    indexing_inflight.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    return Err(CrawlError::Other(format!("indexer channel closed: {e}")));
                }
            }

            // Persist updated crawl state. `indexed_content_hash` is NOT
            // touched here — it advances only when `IndexWriter::commit`
            // succeeds and the caller calls `mark_indexed_batch`. Preserve
            // any prior value so the next crawl can still short-circuit if
            // the index is up-to-date.
            let row = CrawledPageRow {
                id: 0,
                collection_id: ctx.collection_id.clone(),
                crawl_target_id: ctx.crawl_target_id.clone(),
                url: url.to_string(),
                etag,
                last_modified,
                content_hash: Some(content_hash),
                last_status: Some(i64::from(status)),
                last_crawled_at: now_unix,
                indexed_content_hash: prior.as_ref().and_then(|p| p.indexed_content_hash.clone()),
            };

            // Classify discovered links into in-prefix vs outlinks, then
            // perform DB writes in a single locked scope so the conn mutex
            // is held for at most one batch of synchronous SQL — never
            // across an `.await`.
            let mut in_prefix_links = Vec::new();
            // Surface redirect target and canonical to the scheduler so each
            // gets crawled directly and earns its own `crawled_pages` row.
            // Without this, re-fetches of the alias URL would keep re-sending
            // an IndexJob for the canonical (idempotent at Tantivy, but wasteful).
            if let Some(redirect) = redirect_url.clone() {
                in_prefix_links.push(redirect);
            }
            if let Some(canon) = canonical_url.clone() {
                in_prefix_links.push(canon);
            }
            {
                let conn = db.conn();
                crawled_pages::upsert(&conn, &row).map_err(|e| CrawlError::Other(e.to_string()))?;
                for link in &extracted {
                    let Some(normalized) = normalize_url(&link.url) else {
                        continue;
                    };
                    if is_in_prefix(&normalized, &ctx.url_prefix) {
                        in_prefix_links.push(normalized);
                    } else if !is_blacklisted_outlink_host(&normalized) {
                        // Aggregate by host. record_hit itself short-circuits
                        // for hosts the admin has dismissed or promoted — so
                        // the dismissed set grows organically into a
                        // per-collection blacklist.
                        if let Some(host) = host_of(&normalized) {
                            outlink_hosts::record_hit(
                                &conn,
                                &ctx.collection_id,
                                &host,
                                &OutlinkExample {
                                    source_url: url.to_string(),
                                    target_url: normalized.clone(),
                                    link_text: link.text.clone(),
                                },
                                now_unix,
                            )
                            .map_err(|e| CrawlError::Other(e.to_string()))?;
                        }
                    }
                }
            }

            Ok(PageResult {
                in_prefix_links,
                indexed: !already_indexed,
                not_modified: false,
            })
        }
    }
}
