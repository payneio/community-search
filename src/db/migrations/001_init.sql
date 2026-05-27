-- Migration 001: full schema for community-search.
--
-- Consolidated from the original migrations 001–015. Every statement uses
-- CREATE TABLE / INDEX IF NOT EXISTS so this file is a no-op on the existing
-- database (which already ran the chain) and a single-step fresh install
-- for new ones. New schema changes should be appended as 002+, never
-- folded back into this file.
--
-- Tables defined here (final state):
--   settings                  legacy key/value store
--   app_config                runtime key/value store (admin token, etc.)
--   collections               named sets of crawled content
--   crawl_targets             URL prefixes to crawl within a collection
--   crawled_pages             per-page crawl state (ETag, content hash, ...)
--   ranking_config            per-collection ranking tuning
--   outlink_host_suggestions  host-level outlink review queue
--   node_peers                federated nodes
--   collection_peers          maps local collections to remote ones
--   discovered_engines        OpenSearch autodiscovery cache
--   rate_limit_state          per-IP rate limit + cooloff tracking
--   auth_attempts             per-IP login-failure / lockout state
--   admin_notifications       admin UI inbox

-- ── settings ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- ── app_config ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS app_config (
    key   TEXT PRIMARY KEY,
    value TEXT
);
INSERT INTO app_config (key, value) VALUES ('admin_token', NULL) ON CONFLICT DO NOTHING;

-- ── collections ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS collections (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- ── crawl_targets ────────────────────────────────────────────────────────
-- `recrawl_interval_s` is read by the scheduler; `recrawl_interval_secs` is
-- read by the admin API. Writers keep them in sync. Collapsing into one
-- column is future work.
CREATE TABLE IF NOT EXISTS crawl_targets (
    id                     TEXT PRIMARY KEY,
    collection_id          TEXT NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    url_prefix             TEXT NOT NULL,
    recrawl_interval_s     INTEGER NOT NULL DEFAULT 86400,
    recrawl_interval_secs  INTEGER NOT NULL DEFAULT 86400,
    last_crawled_at        INTEGER,
    enabled                INTEGER NOT NULL DEFAULT 1,
    created_at             TEXT NOT NULL,
    crawl_delay_secs       INTEGER,
    UNIQUE (collection_id, url_prefix)
);
CREATE INDEX IF NOT EXISTS idx_crawl_targets_collection
    ON crawl_targets (collection_id);

-- ── crawled_pages ────────────────────────────────────────────────────────
-- `indexed_content_hash` advances only after Tantivy commit succeeds, so a
-- crash between fetch and commit causes re-indexing on the next run.
CREATE TABLE IF NOT EXISTS crawled_pages (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id         TEXT    NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    crawl_target_id       TEXT    NOT NULL REFERENCES crawl_targets(id) ON DELETE CASCADE,
    url                   TEXT    NOT NULL,
    etag                  TEXT,
    last_modified         TEXT,
    content_hash          TEXT,
    last_status           INTEGER,
    last_crawled_at       INTEGER NOT NULL,
    indexed_content_hash  TEXT,
    UNIQUE (collection_id, url)
);
CREATE INDEX IF NOT EXISTS idx_crawled_pages_target ON crawled_pages (crawl_target_id);
CREATE INDEX IF NOT EXISTS idx_crawled_pages_url    ON crawled_pages (url);

-- ── ranking_config ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ranking_config (
    collection_id            INTEGER PRIMARY KEY,
    freshness_half_life_days REAL    NOT NULL DEFAULT 90.0,
    source_weights_json      TEXT    NOT NULL DEFAULT '{"local":1.0}',
    domain_boosts_json       TEXT    NOT NULL DEFAULT '{}',
    updated_at               INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    config_json              TEXT,
    FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE
);

-- ── outlink_host_suggestions ─────────────────────────────────────────────
-- Host-level review queue. Statuses: pending | promoted | dismissed.
-- The BLACKLISTED_OUTLINK_HOSTS list in crawler/url_class.rs is a cold-start
-- blacklist that this table augments via admin decisions.
CREATE TABLE IF NOT EXISTS outlink_host_suggestions (
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    host                TEXT NOT NULL,
    link_count          INTEGER NOT NULL DEFAULT 1,
    examples_json       TEXT NOT NULL DEFAULT '[]',
    first_seen_at       INTEGER NOT NULL,
    last_seen_at        INTEGER NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    promoted_target_id  INTEGER,
    UNIQUE (collection_id, host)
);
CREATE INDEX IF NOT EXISTS idx_outlink_hosts_collection_status
    ON outlink_host_suggestions (collection_id, status);

-- ── node_peers ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS node_peers (
    id                   INTEGER PRIMARY KEY,
    url                  TEXT NOT NULL UNIQUE,
    name                 TEXT,
    enabled              INTEGER NOT NULL DEFAULT 1,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_response_ms     INTEGER,
    last_checked_at      INTEGER,
    disabled_at          INTEGER,
    created_at           INTEGER NOT NULL
);

-- ── collection_peers ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS collection_peers (
    id                 INTEGER PRIMARY KEY,
    local_collection   TEXT NOT NULL,
    node_peer_id       INTEGER NOT NULL
        REFERENCES node_peers(id) ON DELETE CASCADE,
    remote_collection  TEXT NOT NULL,
    source_weight      REAL NOT NULL DEFAULT 1.0,
    enabled            INTEGER NOT NULL DEFAULT 1,
    created_at         INTEGER NOT NULL,
    UNIQUE(local_collection, node_peer_id, remote_collection)
);
CREATE INDEX IF NOT EXISTS idx_coll_peers_local ON collection_peers (local_collection);

-- ── discovered_engines ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS discovered_engines (
    url         TEXT PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    first_seen  INTEGER NOT NULL,
    last_seen   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_discovered_engines_last_seen
    ON discovered_engines (last_seen DESC);

-- ── rate_limit_state ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rate_limit_state (
    ip                TEXT    PRIMARY KEY,
    request_log       TEXT    NOT NULL DEFAULT '[]',
    violations        INTEGER DEFAULT 0,
    cooloff_until     INTEGER DEFAULT 0,
    last_violation_at INTEGER DEFAULT 0,
    updated_at        INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_rate_limit_cooloff ON rate_limit_state (cooloff_until);

-- ── auth_attempts ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS auth_attempts (
    ip              TEXT    PRIMARY KEY,
    failed_count    INTEGER DEFAULT 0,
    lockout_until   INTEGER DEFAULT 0,
    last_attempt_at INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_auth_lockout ON auth_attempts (lockout_until);

-- ── admin_notifications ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS admin_notifications (
    id         INTEGER PRIMARY KEY,
    kind       TEXT    NOT NULL,
    message    TEXT    NOT NULL,
    created_at INTEGER NOT NULL,
    read_at    INTEGER
);
CREATE INDEX IF NOT EXISTS idx_admin_notifications_unread
    ON admin_notifications(created_at) WHERE read_at IS NULL;
