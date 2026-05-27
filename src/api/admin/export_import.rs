//! Admin export/import: ship the contents of one community-search instance
//! to another.
//!
//! ## Wire format (`format_version = 1`)
//!
//! A single JSON object nested by collection *name*, so that the import side
//! can remap every UUID and rowid without ambiguity. Top-level node peers
//! and collection-to-collection peering rows live alongside `collections`.
//!
//! ## Import semantics
//!
//! Additive merge keyed by name/URL:
//!   - A collection whose `name` already exists locally is skipped along
//!     with everything nested under it (crawl_targets, crawled_pages,
//!     ranking_config, outlinks, documents).
//!   - A node_peer whose `url` already exists is skipped.
//!   - A collection_peer is inserted only when its `local_collection`
//!     exists (either pre-existing or freshly imported) AND its
//!     `node_peer_url` matches a row already present (likewise either pre-
//!     existing or freshly imported).
//!
//! Documents are not written through a private `IndexWriter`; instead each
//! is sent down `AppState::indexer_upsert_tx` so the single dedicated
//! indexer task (the sole owner of the Tantivy writer) batches and commits
//! them. The DB transaction is committed *before* sending so the indexer's
//! post-commit `mark_indexed_batch` finds the matching `crawled_pages` rows.

use std::collections::HashMap;
use std::io::Read;

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::public::AppState;
use crate::index::indexer::IndexJob;

/// Gzip's two-byte magic so we can detect a gzipped import body without
/// relying on Content-Type. (Old uncompressed exports keep working.)
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Latest export format. Versioned independently from the Community Search
/// Protocol so we can evolve the dump shape without touching the peer wire.
const FORMAT_VERSION: u32 = 1;

/// Cap for the import body. The default axum limit (2 MiB) is far too small
/// for an index with non-trivial page bodies.
const IMPORT_BODY_LIMIT_BYTES: usize = 512 * 1024 * 1024; // 512 MiB

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct ExportEnvelope {
    pub format_version: u32,
    pub exported_at: String,
    pub self_url: String,
    pub collections: Vec<ExportedCollection>,
    #[serde(default)]
    pub node_peers: Vec<ExportedNodePeer>,
    #[serde(default)]
    pub collection_peers: Vec<ExportedCollectionPeer>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExportedCollection {
    pub name: String,
    pub description: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub crawl_targets: Vec<ExportedCrawlTarget>,
    #[serde(default)]
    pub ranking_config: Option<ExportedRankingConfig>,
    #[serde(default)]
    pub documents: Vec<ExportedDocument>,
    #[serde(default)]
    pub outlink_host_suggestions: Vec<ExportedOutlinkHostSuggestion>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExportedCrawlTarget {
    pub url_prefix: String,
    pub recrawl_interval_s: i64,
    pub recrawl_interval_secs: i64,
    pub enabled: i64,
    pub crawl_delay_secs: Option<i64>,
    pub created_at: String,
    #[serde(default)]
    pub crawled_pages: Vec<ExportedCrawledPage>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExportedCrawledPage {
    pub url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_hash: Option<String>,
    pub last_status: Option<i64>,
    pub last_crawled_at: i64,
    pub indexed_content_hash: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExportedRankingConfig {
    pub freshness_half_life_days: f64,
    pub source_weights_json: String,
    pub domain_boosts_json: String,
    pub config_json: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExportedDocument {
    pub url: String,
    pub title: String,
    pub body: String,
    pub indexed_at: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExportedOutlinkHostSuggestion {
    pub host: String,
    pub link_count: i64,
    pub examples_json: String,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExportedNodePeer {
    pub url: String,
    pub name: Option<String>,
    pub enabled: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExportedCollectionPeer {
    pub local_collection: String,
    pub node_peer_url: String,
    pub remote_collection: String,
    pub source_weight: f64,
    pub enabled: i64,
}

// ---------------------------------------------------------------------------
// Response of POST /api/admin/import — what got applied vs. skipped.
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Default)]
pub struct ImportReport {
    pub collections_imported: usize,
    pub collections_skipped: usize,
    pub crawl_targets_imported: usize,
    pub crawled_pages_imported: usize,
    pub documents_queued: usize,
    pub outlink_host_suggestions_imported: usize,
    pub node_peers_imported: usize,
    pub node_peers_skipped: usize,
    pub collection_peers_imported: usize,
    pub collection_peers_skipped: usize,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// GET /api/admin/export — dump everything as a gzipped JSON download.
///
/// Page bodies dominate the payload and compress 5-10×, so we always ship
/// gzipped. The download lands as `*.json.gz`; the matching import handler
/// transparently accepts either gzipped or plain JSON.
async fn handle_export(State(state): State<AppState>) -> Result<Response, (StatusCode, String)> {
    let envelope = {
        let conn = state.db.lock().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "db mutex poisoned".into(),
            )
        })?;
        build_envelope(&conn, &state.self_url, state.search.as_ref())
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    // Stream serde_json straight into the gzip encoder — avoids holding the
    // uncompressed JSON in memory at the same time as the compressed copy.
    let body = {
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        serde_json::to_writer(&mut enc, &envelope)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        enc.finish()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    let filename = format!(
        "community-search-export-{}.json.gz",
        Utc::now().format("%Y%m%d-%H%M%S")
    );

    let headers = [
        (header::CONTENT_TYPE, "application/gzip".to_string()),
        (
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        ),
    ];

    Ok((StatusCode::OK, headers, body).into_response())
}

/// POST /api/admin/import — apply an envelope produced by `handle_export`.
///
/// Accepts the body as either gzipped JSON (preferred — what
/// `handle_export` produces) or raw JSON (older exports, hand-written
/// payloads). Detection is by the gzip magic bytes, not Content-Type, so
/// curl invocations without `-H 'Content-Encoding: gzip'` still work.
async fn handle_import(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let envelope: ExportEnvelope = if body.len() >= 2 && body[..2] == GZIP_MAGIC {
        let mut dec = GzDecoder::new(&body[..]);
        let mut decoded = Vec::with_capacity(body.len() * 4);
        dec.read_to_end(&mut decoded)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("gzip decode failed: {e}")))?;
        serde_json::from_slice(&decoded).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid JSON in gzip: {e}"),
            )
        })?
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid JSON: {e}")))?
    };

    if envelope.format_version != FORMAT_VERSION {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "unsupported format_version {}: this build understands {FORMAT_VERSION}",
                envelope.format_version
            ),
        ));
    }

    let (report, jobs) = {
        let conn = state.db.lock().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "db mutex poisoned".into(),
            )
        })?;
        apply_envelope(&conn, &envelope)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    // Hand documents off to the indexer task. We deliberately do this
    // *after* releasing the DB lock so the indexer's flush (which uses a
    // separate connection) can see the freshly committed rows when it
    // applies its journal. Increment the in-flight counter on each send
    // so a concurrent /admin/status observer can't see queued imports as
    // zero-pending; the indexer decrements after Tantivy commits.
    for job in jobs {
        state
            .indexing_inflight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Err(e) = state.indexer_upsert_tx.send(job).await {
            state
                .indexing_inflight
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("indexer channel closed mid-import: {e}"),
            ));
        }
    }

    Ok(Json(report).into_response())
}

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------

fn build_envelope(
    conn: &Connection,
    self_url: &str,
    search: &crate::search::service::SearchService,
) -> anyhow::Result<ExportEnvelope> {
    // -- collections (name → uuid + rowid) ------------------------------------
    struct CollHeader {
        rowid: i64,
        uuid: String,
        name: String,
        description: String,
        created_at: String,
        updated_at: String,
    }

    let mut stmt = conn.prepare(
        "SELECT rowid, id, name, description, created_at, updated_at \
         FROM collections ORDER BY name ASC",
    )?;
    let headers: Vec<CollHeader> = stmt
        .query_map([], |row| {
            Ok(CollHeader {
                rowid: row.get(0)?,
                uuid: row.get(1)?,
                name: row.get(2)?,
                description: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    // -- documents (indexed: collection name → Vec<doc>) ----------------------
    let mut docs_by_collection: HashMap<String, Vec<ExportedDocument>> = HashMap::new();
    for d in search.export_all_docs()? {
        docs_by_collection
            .entry(d.collection)
            .or_default()
            .push(ExportedDocument {
                url: d.url,
                title: d.title,
                body: d.body,
                indexed_at: d.indexed_at,
            });
    }

    // -- per-collection nested dump ------------------------------------------
    let mut collections = Vec::with_capacity(headers.len());
    for h in headers {
        let crawl_targets = export_crawl_targets(conn, &h.uuid)?;
        let ranking_config = export_ranking_config(conn, h.rowid)?;
        let outlink_host_suggestions = export_outlinks(conn, &h.uuid)?;
        let documents = docs_by_collection.remove(&h.name).unwrap_or_default();

        collections.push(ExportedCollection {
            name: h.name,
            description: h.description,
            created_at: h.created_at,
            updated_at: h.updated_at,
            crawl_targets,
            ranking_config,
            documents,
            outlink_host_suggestions,
        });
    }

    let node_peers = export_node_peers(conn)?;
    let collection_peers = export_collection_peers(conn)?;

    Ok(ExportEnvelope {
        format_version: FORMAT_VERSION,
        exported_at: Utc::now().to_rfc3339(),
        self_url: self_url.to_string(),
        collections,
        node_peers,
        collection_peers,
    })
}

fn export_crawl_targets(
    conn: &Connection,
    collection_uuid: &str,
) -> anyhow::Result<Vec<ExportedCrawlTarget>> {
    let mut stmt = conn.prepare(
        "SELECT id, url_prefix, recrawl_interval_s, recrawl_interval_secs, \
                enabled, crawl_delay_secs, created_at \
         FROM crawl_targets WHERE collection_id = ?1 ORDER BY url_prefix",
    )?;
    let rows: Vec<(String, ExportedCrawlTarget)> = stmt
        .query_map(params![collection_uuid], |row| {
            Ok((
                row.get::<_, String>(0)?,
                ExportedCrawlTarget {
                    url_prefix: row.get(1)?,
                    recrawl_interval_s: row.get(2)?,
                    recrawl_interval_secs: row.get(3)?,
                    enabled: row.get(4)?,
                    crawl_delay_secs: row.get(5)?,
                    created_at: row.get(6)?,
                    crawled_pages: Vec::new(),
                },
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    let mut out = Vec::with_capacity(rows.len());
    for (target_uuid, mut target) in rows {
        target.crawled_pages = export_crawled_pages(conn, &target_uuid)?;
        out.push(target);
    }
    Ok(out)
}

fn export_crawled_pages(
    conn: &Connection,
    crawl_target_uuid: &str,
) -> anyhow::Result<Vec<ExportedCrawledPage>> {
    let mut stmt = conn.prepare(
        "SELECT url, etag, last_modified, content_hash, last_status, \
                last_crawled_at, indexed_content_hash \
         FROM crawled_pages WHERE crawl_target_id = ?1 ORDER BY id",
    )?;
    let rows = stmt
        .query_map(params![crawl_target_uuid], |row| {
            Ok(ExportedCrawledPage {
                url: row.get(0)?,
                etag: row.get(1)?,
                last_modified: row.get(2)?,
                content_hash: row.get(3)?,
                last_status: row.get(4)?,
                last_crawled_at: row.get(5)?,
                indexed_content_hash: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn export_ranking_config(
    conn: &Connection,
    collection_rowid: i64,
) -> anyhow::Result<Option<ExportedRankingConfig>> {
    let row = conn
        .query_row(
            "SELECT freshness_half_life_days, source_weights_json, \
                    domain_boosts_json, config_json \
             FROM ranking_config WHERE collection_id = ?1",
            params![collection_rowid],
            |r| {
                Ok(ExportedRankingConfig {
                    freshness_half_life_days: r.get(0)?,
                    source_weights_json: r.get(1)?,
                    domain_boosts_json: r.get(2)?,
                    config_json: r.get(3)?,
                })
            },
        )
        .optional()?;
    Ok(row)
}

fn export_outlinks(
    conn: &Connection,
    collection_uuid: &str,
) -> anyhow::Result<Vec<ExportedOutlinkHostSuggestion>> {
    let mut stmt = conn.prepare(
        "SELECT host, link_count, examples_json, first_seen_at, last_seen_at, status \
         FROM outlink_host_suggestions WHERE collection_id = ?1 ORDER BY host",
    )?;
    let rows = stmt
        .query_map(params![collection_uuid], |row| {
            Ok(ExportedOutlinkHostSuggestion {
                host: row.get(0)?,
                link_count: row.get(1)?,
                examples_json: row.get(2)?,
                first_seen_at: row.get(3)?,
                last_seen_at: row.get(4)?,
                status: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn export_node_peers(conn: &Connection) -> anyhow::Result<Vec<ExportedNodePeer>> {
    let mut stmt = conn.prepare("SELECT url, name, enabled FROM node_peers ORDER BY url")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(ExportedNodePeer {
                url: row.get(0)?,
                name: row.get(1)?,
                enabled: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn export_collection_peers(conn: &Connection) -> anyhow::Result<Vec<ExportedCollectionPeer>> {
    let mut stmt = conn.prepare(
        "SELECT cp.local_collection, np.url, cp.remote_collection, \
                cp.source_weight, cp.enabled \
         FROM collection_peers cp \
         JOIN node_peers np ON np.id = cp.node_peer_id \
         ORDER BY cp.local_collection, np.url",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(ExportedCollectionPeer {
                local_collection: row.get(0)?,
                node_peer_url: row.get(1)?,
                remote_collection: row.get(2)?,
                source_weight: row.get(3)?,
                enabled: row.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Import
// ---------------------------------------------------------------------------

fn apply_envelope(
    conn: &Connection,
    env: &ExportEnvelope,
) -> anyhow::Result<(ImportReport, Vec<IndexJob>)> {
    let mut report = ImportReport::default();
    let mut jobs: Vec<IndexJob> = Vec::new();

    // FK enforcement must be disabled *before* opening the transaction:
    // `PRAGMA foreign_keys` is a no-op while a transaction is open. The
    // ranking_config relation declares `collection_id INTEGER` against
    // `collections(id) TEXT`, so SQLite's type-affinity rules reject the
    // FK check even when the values are logically equivalent.
    conn.execute_batch("PRAGMA foreign_keys = OFF;")?;
    let result = apply_envelope_inner(conn, env, &mut report, &mut jobs);
    // Re-enable regardless of outcome; the connection is shared with other
    // handlers that rely on FK enforcement.
    let _ = conn.execute_batch("PRAGMA foreign_keys = ON;");
    result?;
    Ok((report, jobs))
}

fn apply_envelope_inner(
    conn: &Connection,
    env: &ExportEnvelope,
    report: &mut ImportReport,
    jobs: &mut Vec<IndexJob>,
) -> anyhow::Result<()> {
    let tx = conn.unchecked_transaction()?;

    // Track which collection names exist after import — pre-existing or
    // freshly inserted — so collection_peers can be cross-checked.
    let mut known_collection_names: std::collections::HashSet<String> =
        existing_collection_names(&tx)?;

    for c in &env.collections {
        if known_collection_names.contains(&c.name) {
            report.collections_skipped += 1;
            continue;
        }

        let now = Utc::now().to_rfc3339();
        let new_uuid = Uuid::new_v4().to_string();
        tx.execute(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                new_uuid,
                c.name,
                c.description,
                // Preserve original timestamps when present; otherwise stamp now.
                if c.created_at.is_empty() {
                    &now
                } else {
                    &c.created_at
                },
                if c.updated_at.is_empty() {
                    &now
                } else {
                    &c.updated_at
                },
            ],
        )?;
        let new_rowid: i64 = tx.query_row(
            "SELECT rowid FROM collections WHERE id = ?1",
            params![new_uuid],
            |r| r.get(0),
        )?;
        report.collections_imported += 1;
        known_collection_names.insert(c.name.clone());

        // crawl_targets + their crawled_pages
        for t in &c.crawl_targets {
            let target_uuid = Uuid::new_v4().to_string();
            tx.execute(
                "INSERT INTO crawl_targets \
                 (id, collection_id, url_prefix, recrawl_interval_s, recrawl_interval_secs, \
                  enabled, crawl_delay_secs, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    target_uuid,
                    new_uuid,
                    t.url_prefix,
                    t.recrawl_interval_s,
                    t.recrawl_interval_secs,
                    t.enabled,
                    t.crawl_delay_secs,
                    if t.created_at.is_empty() {
                        &now
                    } else {
                        &t.created_at
                    },
                ],
            )?;
            report.crawl_targets_imported += 1;

            for p in &t.crawled_pages {
                tx.execute(
                    "INSERT INTO crawled_pages \
                     (collection_id, crawl_target_id, url, etag, last_modified, \
                      content_hash, last_status, last_crawled_at, indexed_content_hash) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)",
                    params![
                        new_uuid,
                        target_uuid,
                        p.url,
                        p.etag,
                        p.last_modified,
                        p.content_hash,
                        p.last_status,
                        p.last_crawled_at,
                    ],
                )?;
                report.crawled_pages_imported += 1;
            }
        }

        // ranking_config (FK enforcement already toggled off by caller)
        if let Some(rc) = &c.ranking_config {
            tx.execute(
                "INSERT INTO ranking_config \
                 (collection_id, freshness_half_life_days, source_weights_json, \
                  domain_boosts_json, config_json, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, strftime('%s','now'))",
                params![
                    new_rowid,
                    rc.freshness_half_life_days,
                    rc.source_weights_json,
                    rc.domain_boosts_json,
                    rc.config_json,
                ],
            )?;
        }

        // outlink_host_suggestions
        for o in &c.outlink_host_suggestions {
            let id = Uuid::new_v4().to_string();
            tx.execute(
                "INSERT INTO outlink_host_suggestions \
                 (id, collection_id, host, link_count, examples_json, \
                  first_seen_at, last_seen_at, status) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    id,
                    new_uuid,
                    o.host,
                    o.link_count,
                    o.examples_json,
                    o.first_seen_at,
                    o.last_seen_at,
                    o.status,
                ],
            )?;
            report.outlink_host_suggestions_imported += 1;
        }

        // Queue documents (sent after commit).
        for d in &c.documents {
            jobs.push(IndexJob {
                collection_name: c.name.clone(),
                url: d.url.clone(),
                title: d.title.clone(),
                body: d.body.clone(),
                indexed_at: d.indexed_at,
                collection_id: new_uuid.clone(),
                content_hash: doc_content_hash(d),
            });
            report.documents_queued += 1;
        }
    }

    // Node peers (top-level, by URL).
    let mut known_peer_urls: std::collections::HashSet<String> = existing_node_peer_urls(&tx)?;
    for p in &env.node_peers {
        if known_peer_urls.contains(&p.url) {
            report.node_peers_skipped += 1;
            continue;
        }
        let now_unix = Utc::now().timestamp();
        tx.execute(
            "INSERT INTO node_peers (url, name, enabled, created_at) \
             VALUES (?1, ?2, ?3, ?4)",
            params![p.url, p.name, p.enabled, now_unix],
        )?;
        known_peer_urls.insert(p.url.clone());
        report.node_peers_imported += 1;
    }

    // Collection peers — require both local collection and node peer to exist.
    for cp in &env.collection_peers {
        if !known_collection_names.contains(&cp.local_collection)
            || !known_peer_urls.contains(&cp.node_peer_url)
        {
            report.collection_peers_skipped += 1;
            continue;
        }
        let node_peer_id: i64 = tx.query_row(
            "SELECT id FROM node_peers WHERE url = ?1",
            params![cp.node_peer_url],
            |r| r.get(0),
        )?;
        let now_unix = Utc::now().timestamp();
        let affected = tx.execute(
            "INSERT OR IGNORE INTO collection_peers \
             (local_collection, node_peer_id, remote_collection, source_weight, enabled, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                cp.local_collection,
                node_peer_id,
                cp.remote_collection,
                cp.source_weight,
                cp.enabled,
                now_unix,
            ],
        )?;
        if affected == 1 {
            report.collection_peers_imported += 1;
        } else {
            report.collection_peers_skipped += 1;
        }
    }

    tx.commit()?;
    Ok(())
}

fn existing_collection_names(
    conn: &Connection,
) -> anyhow::Result<std::collections::HashSet<String>> {
    let mut stmt = conn.prepare("SELECT name FROM collections")?;
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows.into_iter().collect())
}

fn existing_node_peer_urls(conn: &Connection) -> anyhow::Result<std::collections::HashSet<String>> {
    let mut stmt = conn.prepare("SELECT url FROM node_peers")?;
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows.into_iter().collect())
}

/// Sha256 over the document body bytes. Used as the synthetic
/// `content_hash` carried in the IndexJob so the indexer journal has
/// something deterministic to write back. The source export does not
/// preserve the per-page content_hash on the document itself — that lives
/// on the `crawled_pages` row, which may or may not be present in the
/// import for a given URL.
fn doc_content_hash(d: &ExportedDocument) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(d.body.as_bytes());
    format!("{:x}", h.finalize())
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/admin/export", get(handle_export))
        .route(
            "/api/admin/import",
            post(handle_import).layer(DefaultBodyLimit::max(IMPORT_BODY_LIMIT_BYTES)),
        )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tantivy::Index;

    use crate::index::reader::Searcher;
    use crate::index::schema;
    use crate::index::writer::{Document, IndexWriter};
    use crate::search::service::SearchService;
    use std::sync::{Arc, Mutex};

    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        crate::db::run_migrations(&conn).expect("apply migrations");
        conn
    }

    fn fresh_search() -> Arc<SearchService> {
        let conn = Connection::open_in_memory().unwrap();
        crate::db::run_migrations(&conn).unwrap();
        let db = Arc::new(Mutex::new(conn));
        let index = Index::create_in_ram(schema::build());
        let searcher = Searcher::open(index).unwrap();
        Arc::new(SearchService::new(Arc::new(searcher), db))
    }

    fn seed_source_db(conn: &Connection) -> (String, String, String) {
        // Returns (collection_uuid, target_uuid, page_url).
        let coll_uuid = "src-coll-uuid".to_string();
        conn.execute(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES (?1, 'tech', 'tech blogs', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')",
            rusqlite::params![coll_uuid],
        )
        .unwrap();
        let target_uuid = "src-target-uuid".to_string();
        conn.execute(
            "INSERT INTO crawl_targets \
             (id, collection_id, url_prefix, recrawl_interval_s, recrawl_interval_secs, enabled, created_at) \
             VALUES (?1, ?2, 'https://example.com/', 3600, 3600, 1, '2024-01-01T00:00:00Z')",
            rusqlite::params![target_uuid, coll_uuid],
        )
        .unwrap();
        let url = "https://example.com/post1".to_string();
        conn.execute(
            "INSERT INTO crawled_pages \
             (collection_id, crawl_target_id, url, content_hash, last_status, last_crawled_at, indexed_content_hash) \
             VALUES (?1, ?2, ?3, 'h1', 200, 1700000000, 'h1')",
            rusqlite::params![coll_uuid, target_uuid, url],
        )
        .unwrap();
        // ranking_config keyed on rowid
        let rowid: i64 = conn
            .query_row(
                "SELECT rowid FROM collections WHERE id = ?1",
                rusqlite::params![coll_uuid],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
        conn.execute(
            "INSERT INTO ranking_config \
             (collection_id, freshness_half_life_days, source_weights_json, domain_boosts_json, updated_at) \
             VALUES (?1, 30.0, '{\"local\":2.0}', '{\"example.com\":1.5}', 1700000000)",
            rusqlite::params![rowid],
        )
        .unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute(
            "INSERT INTO outlink_host_suggestions \
             (id, collection_id, host, link_count, examples_json, first_seen_at, last_seen_at, status) \
             VALUES ('ol1', ?1, 'other.example', 4, '[]', 1, 2, 'pending')",
            rusqlite::params![coll_uuid],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO node_peers (url, name, enabled, created_at) \
             VALUES ('https://peer.example/', 'peer', 1, 1700000000)",
            [],
        )
        .unwrap();
        let np_id: i64 = conn
            .query_row(
                "SELECT id FROM node_peers WHERE url = ?1",
                rusqlite::params!["https://peer.example/"],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute(
            "INSERT INTO collection_peers \
             (local_collection, node_peer_id, remote_collection, source_weight, enabled, created_at) \
             VALUES ('tech', ?1, 'remote-tech', 0.8, 1, 1700000000)",
            rusqlite::params![np_id],
        )
        .unwrap();
        (coll_uuid, target_uuid, url)
    }

    /// build_envelope sees every seeded relation, nested under its collection.
    #[test]
    fn build_envelope_collects_seeded_data() {
        let conn = fresh_db();
        let (_, _, page_url) = seed_source_db(&conn);

        let search = fresh_search();
        let env = build_envelope(&conn, "https://self.example/", search.as_ref()).unwrap();

        assert_eq!(env.format_version, FORMAT_VERSION);
        assert_eq!(env.self_url, "https://self.example/");
        assert_eq!(env.collections.len(), 1);
        let c = &env.collections[0];
        assert_eq!(c.name, "tech");
        assert_eq!(c.crawl_targets.len(), 1);
        assert_eq!(c.crawl_targets[0].url_prefix, "https://example.com/");
        assert_eq!(c.crawl_targets[0].crawled_pages.len(), 1);
        assert_eq!(c.crawl_targets[0].crawled_pages[0].url, page_url);
        let rc = c.ranking_config.as_ref().expect("ranking config exported");
        assert!((rc.freshness_half_life_days - 30.0).abs() < f64::EPSILON);
        assert_eq!(c.outlink_host_suggestions.len(), 1);
        assert_eq!(env.node_peers.len(), 1);
        assert_eq!(env.collection_peers.len(), 1);
        assert_eq!(
            env.collection_peers[0].node_peer_url,
            "https://peer.example/"
        );
    }

    /// apply_envelope writes every relation into a fresh DB and returns one
    /// IndexJob per exported document. New IDs are minted and the SQL
    /// graph stays consistent (FKs satisfied via name lookup).
    #[test]
    fn apply_envelope_imports_into_fresh_db() {
        let src = fresh_db();
        seed_source_db(&src);
        let search = fresh_search();
        let env = build_envelope(&src, "", search.as_ref()).unwrap();

        // Pretend the source had two documents in Tantivy.
        let env = ExportEnvelope {
            collections: env
                .collections
                .into_iter()
                .map(|mut c| {
                    c.documents = vec![
                        ExportedDocument {
                            url: "https://example.com/post1".into(),
                            title: "Post 1".into(),
                            body: "alpha bravo".into(),
                            indexed_at: 1700000000,
                        },
                        ExportedDocument {
                            url: "https://example.com/post2".into(),
                            title: "Post 2".into(),
                            body: "charlie delta".into(),
                            indexed_at: 1700000001,
                        },
                    ];
                    c
                })
                .collect(),
            ..env
        };

        let dst = fresh_db();
        let (report, jobs) = apply_envelope(&dst, &env).unwrap();

        assert_eq!(report.collections_imported, 1);
        assert_eq!(report.collections_skipped, 0);
        assert_eq!(report.crawl_targets_imported, 1);
        assert_eq!(report.crawled_pages_imported, 1);
        assert_eq!(report.documents_queued, 2);
        assert_eq!(report.outlink_host_suggestions_imported, 1);
        assert_eq!(report.node_peers_imported, 1);
        assert_eq!(report.collection_peers_imported, 1);
        assert_eq!(jobs.len(), 2);

        // All jobs use the *new* collection UUID, not the source's.
        let new_uuid: String = dst
            .query_row("SELECT id FROM collections WHERE name = 'tech'", [], |r| {
                r.get(0)
            })
            .unwrap();
        for j in &jobs {
            assert_eq!(j.collection_id, new_uuid);
            assert_eq!(j.collection_name, "tech");
        }

        // The destination DB sees everything.
        let crawl_targets: i64 = dst
            .query_row("SELECT COUNT(*) FROM crawl_targets", [], |r| r.get(0))
            .unwrap();
        assert_eq!(crawl_targets, 1);
        let pages: i64 = dst
            .query_row("SELECT COUNT(*) FROM crawled_pages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pages, 1);
        let outlinks: i64 = dst
            .query_row("SELECT COUNT(*) FROM outlink_host_suggestions", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(outlinks, 1);
        let coll_peers: i64 = dst
            .query_row("SELECT COUNT(*) FROM collection_peers", [], |r| r.get(0))
            .unwrap();
        assert_eq!(coll_peers, 1);
    }

    /// Importing into a DB that already has a collection by the same name
    /// skips the whole subtree under that name (no double-insert of
    /// crawl_targets, no doc IndexJobs for the conflicting collection).
    #[test]
    fn apply_envelope_skips_existing_collection_by_name() {
        let src = fresh_db();
        seed_source_db(&src);
        let search = fresh_search();
        let env = build_envelope(&src, "", search.as_ref()).unwrap();
        let env = ExportEnvelope {
            collections: env
                .collections
                .into_iter()
                .map(|mut c| {
                    c.documents.push(ExportedDocument {
                        url: "https://example.com/x".into(),
                        title: "x".into(),
                        body: "x".into(),
                        indexed_at: 1,
                    });
                    c
                })
                .collect(),
            ..env
        };

        let dst = fresh_db();
        // Pre-create a conflicting collection.
        dst.execute(
            "INSERT INTO collections (id, name, description, created_at, updated_at) \
             VALUES ('preexist-id', 'tech', 'pre', '2020-01-01', '2020-01-01')",
            [],
        )
        .unwrap();

        let (report, jobs) = apply_envelope(&dst, &env).unwrap();
        assert_eq!(report.collections_imported, 0);
        assert_eq!(report.collections_skipped, 1);
        assert_eq!(report.documents_queued, 0);
        assert!(jobs.is_empty(), "no docs queued when collection is skipped");

        // The pre-existing collection is untouched.
        let crawl_targets: i64 = dst
            .query_row("SELECT COUNT(*) FROM crawl_targets", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            crawl_targets, 0,
            "skipped collection contributes no targets"
        );
    }

    /// End-to-end document path: write docs through the same IndexWriter the
    /// crawler uses, export them via Searcher::export_all_docs, import the
    /// envelope into a second index by feeding the returned IndexJobs through
    /// a second writer, then assert search hits on the target side.
    #[test]
    fn documents_roundtrip_through_envelope() {
        // -- Source side: in-RAM index with two docs --------------------------
        let src_db = fresh_db();
        let (coll_uuid, _, _) = seed_source_db(&src_db);
        let src_index = Index::create_in_ram(schema::build());
        {
            let mut w = IndexWriter::open(&src_index).unwrap();
            w.upsert(&Document {
                collection: "tech",
                url: "https://example.com/post1",
                title: "Async Rust",
                body: "tokio is great",
                indexed_at: 1700000000,
                collection_id: &coll_uuid,
                content_hash: "h1",
            })
            .unwrap();
            w.upsert(&Document {
                collection: "tech",
                url: "https://example.com/post2",
                title: "Cast iron",
                body: "skillet recipes",
                indexed_at: 1700000001,
                collection_id: &coll_uuid,
                content_hash: "h2",
            })
            .unwrap();
            w.commit().unwrap();
        }
        let src_search = {
            let conn = Connection::open_in_memory().unwrap();
            crate::db::run_migrations(&conn).unwrap();
            let db = Arc::new(Mutex::new(conn));
            let searcher = Searcher::open(src_index.clone()).unwrap();
            Arc::new(SearchService::new(Arc::new(searcher), db))
        };

        let env = build_envelope(&src_db, "", src_search.as_ref()).unwrap();
        assert_eq!(env.collections[0].documents.len(), 2);

        // -- Target side: fresh DB + fresh index, drive the import ------------
        let dst_db = fresh_db();
        let (_, jobs) = apply_envelope(&dst_db, &env).unwrap();
        assert_eq!(jobs.len(), 2);

        let dst_index = Index::create_in_ram(schema::build());
        {
            let mut w = IndexWriter::open(&dst_index).unwrap();
            for j in &jobs {
                w.upsert(&Document {
                    collection: &j.collection_name,
                    url: &j.url,
                    title: &j.title,
                    body: &j.body,
                    indexed_at: j.indexed_at,
                    collection_id: &j.collection_id,
                    content_hash: &j.content_hash,
                })
                .unwrap();
            }
            w.commit().unwrap();
        }

        let dst_search = Searcher::open(dst_index).unwrap();
        let hits = dst_search.search("tokio", Some("tech"), 10).unwrap();
        assert_eq!(hits.len(), 1, "imported doc must be findable on target");
        assert_eq!(hits[0].url, "https://example.com/post1");
        let hits = dst_search.search("skillet", Some("tech"), 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].url, "https://example.com/post2");
    }

    /// The gzip path on import accepts gzipped JSON and the plain path
    /// still accepts uncompressed JSON. Detection is by magic bytes.
    #[test]
    fn import_accepts_both_gzip_and_plain_json() {
        use flate2::write::GzEncoder;
        use std::io::Write;

        let src = fresh_db();
        seed_source_db(&src);
        let search = fresh_search();
        let env = build_envelope(&src, "", search.as_ref()).unwrap();

        let plain = serde_json::to_vec(&env).unwrap();
        let gzipped = {
            let mut e = GzEncoder::new(Vec::new(), flate2::Compression::default());
            e.write_all(&plain).unwrap();
            e.finish().unwrap()
        };
        assert!(
            gzipped.len() < plain.len(),
            "gzip should shrink the payload"
        );
        assert_eq!(&gzipped[..2], &GZIP_MAGIC);

        // Round-trip both via the same detection logic the handler uses.
        for (label, bytes) in [("plain", plain), ("gzip", gzipped)] {
            let parsed: ExportEnvelope = if bytes.len() >= 2 && bytes[..2] == GZIP_MAGIC {
                let mut dec = flate2::read::GzDecoder::new(&bytes[..]);
                let mut out = Vec::new();
                dec.read_to_end(&mut out).unwrap();
                serde_json::from_slice(&out).unwrap()
            } else {
                serde_json::from_slice(&bytes).unwrap()
            };
            assert_eq!(parsed.format_version, FORMAT_VERSION, "{label} parse");
            assert_eq!(parsed.collections.len(), 1, "{label} parse");
        }
    }

    #[test]
    fn doc_content_hash_is_stable() {
        let d = ExportedDocument {
            url: "https://e.com/".into(),
            title: "t".into(),
            body: "hello world".into(),
            indexed_at: 1,
        };
        assert_eq!(doc_content_hash(&d), doc_content_hash(&d));
        let d2 = ExportedDocument {
            body: "different".into(),
            ..d
        };
        assert_ne!(
            doc_content_hash(&ExportedDocument {
                url: "https://e.com/".into(),
                title: "t".into(),
                body: "hello world".into(),
                indexed_at: 1,
            }),
            doc_content_hash(&d2)
        );
    }
}
