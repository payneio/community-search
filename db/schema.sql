-- Phase 5 federation schema.
-- Used by federation storage tests via include_str!("../../db/schema.sql").

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

CREATE TABLE IF NOT EXISTS admin_notifications (
    id         INTEGER PRIMARY KEY,
    kind       TEXT    NOT NULL,
    message    TEXT    NOT NULL,
    created_at INTEGER NOT NULL,
    read_at    INTEGER
);

CREATE INDEX IF NOT EXISTS idx_admin_notifications_unread
    ON admin_notifications(created_at) WHERE read_at IS NULL;
