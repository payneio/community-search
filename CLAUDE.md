# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Community Search — a self-hosted, federated peer-to-peer search engine. Single Rust binary, no external runtime dependencies. SQLite (rusqlite, bundled) for operational data; Tantivy for the on-disk full-text index; Axum for HTTP; embedded HTML/JS UI served from the binary via `rust-embed`.

The authoritative wire spec for peer communication is `docs/COMMUNITY_SEARCH_PROTOCOL.md` (Community Search Protocol v1.0). The product spec is `docs/spec.md`. The README covers user-facing setup.

## Common commands

```bash
cargo build --release       # release binary at ./target/release/community-search
cargo test                  # unit + integration tests (tests/ dir)
cargo test <name>           # single test by substring; e.g. cargo test gossip_periodic
cargo test --test api_test  # one integration-test binary at a time
cargo clippy
cargo fmt
```

Running locally: `./target/release/community-search`. On first run with no data dir, the binary prints a generated admin token to stdout and creates `./data/` — save it. UI is at `http://localhost:8080`, admin UI at `/admin`.

Smoke check the admin UI sections after a UI change: `./scripts/check-admin-ui.sh` (requires a running server on 8080).

## Architecture — the parts that span multiple files

**Two routers, one state.** `src/api/router.rs::build_router` composes the app from sub-routers:
- **Peer-facing routes** (`GET /api/collections`, `POST /api/search`, `POST /api/gossip/exchange`) get the `X-CommunitySearch-Version: 1.0` header injected via `middleware/peer_version.rs`. The constant lives in `src/protocol.rs` (`PROTOCOL_VERSION`).
- **UI and admin routes** (`/`, `/static/*`, `/admin`, `/health`, `/api/admin/*`) deliberately do **not** carry the version header. Admin routes are nested in `src/api/admin/` (one file per resource) and the whole admin subtree is gated by `require_admin_token` middleware.
- The rate-limit middleware is scoped via `route_layer` to **only** `POST /api/search` — do not apply it more broadly.

**Shared state lives in `AppState`** (`src/api/public.rs`). It carries the SQLite handle (`Arc<Mutex<Connection>>`), the Tantivy `Searcher`, two rate-limit buckets (anon and peer), a `RuntimeConfig` blob (persisted as JSON in `app_config` so admin edits survive restart), the `peer_client` trait object, and a plain `reqwest::Client` for outbound gossip. New cross-cutting state goes here; if you add a field, update every test helper that constructs `AppState` (see "Test infrastructure" below).

**Database schema is migration-driven.** `src/db/migrations.rs` is an ordered list — **append only, never reorder**. Each migration is a `.sql` file under `src/db/migrations/` embedded via `include_str!`. `run_migrations` swallows the SQLite "duplicate column name" error so `ALTER TABLE ADD COLUMN` migrations are idempotent on re-opens; keep that pattern when adding similar migrations. PRAGMAs set at open time: WAL mode, foreign keys ON, synchronous NORMAL.

**Federation has two distinct peering levels** (see `src/federation/`):
- **Node peering** (`node_peers` table) — server-to-server "I know you exist." Gossip exchange runs here.
- **Collection peering** (`collection_peers` table) — collection-to-collection subscription. Search fan-out runs here. Each subscription has its own source weight in the ranking formula.
- Fan-out depth is configurable (1 or 2). Depth-2 peers decrement `remaining_depth` and stop at 0 — preserve this contract when touching `federation/fanout.rs`.

**Gossip** (`src/federation/gossip.rs`) merges discovered-engine lists additively — engines are never auto-removed. Triggered (1) immediately when a node peer is added, (2) periodically (background task spawned in `main.rs`, `GOSSIP_SYNC_INTERVAL_SECS`), (3) on demand from the admin UI. The engine's own URL (`SELF_URL`) is seeded into `discovered_engines` on startup and the admin endpoint refuses to delete it.

**Crawler** is a single background task spawned in `main.rs` (`crawler::scheduler::Scheduler::spawn`, polling every 60s). It is prefix-bounded (stays under the configured URL prefix), respects `robots.txt` (hard requirement — not configurable), uses `ETag`/`Last-Modified` for smart re-crawl, and writes outlinks (links *outside* the prefix) to `outlink_suggestions` for admin review rather than following them.

**Protocol version compatibility** (`src/protocol.rs::check_compatibility`): same major + minor = `Compatible`; same major different minor = `MinorMismatch` (proceed with warning, ignore unknown fields); different major or unparseable = `Incompatible` (reject). All version comparisons go through this — don't open-code semver checks.

## Test infrastructure

Integration tests in `tests/*.rs` compile the crate as a library, so test helpers live in `src/test_support.rs` (compiled unconditionally, not behind a feature flag) so they're visible without the `testing` feature. There's also `src/testing.rs` behind `#[cfg(any(test, feature = "testing"))]` for unit-test-only helpers.

Three flavors of test harness in `test_support.rs`:
- `test_router_with_search()` / `test_router_full(token)` — in-memory SQLite + in-RAM Tantivy, drive via `tower::ServiceExt::oneshot` (no TCP).
- `test_app(token)` / `test_app_with_self_url(...)` — same, plus direct DB handle for seeding (`seed_outlink` etc.).
- `spawn_test_server()` — real TCP listener on a random port for tests that use a real `reqwest::Client`. Drops cleanly via an oneshot shutdown channel.

**When adding a field to `AppState`,** every constructor in `test_support.rs` *and* `src/api/router.rs::tests::test_state` must be updated — there's no shared builder. Use `RateLimitConfig::default()` and `RuntimeConfig::default()` as the baseline.

## Conventions worth knowing

- **Runtime-tunable settings** belong in `RuntimeConfig` (`src/lib.rs`) — these are persisted as a JSON blob under `app_config.key='runtime_config'` and the admin `PUT /api/admin/config` patches in place (only fields present in the body are updated). Startup settings that should *not* be hot-reloadable go in `Config` (`src/config.rs`) instead, loaded from env vars / `.env`.
- **Admin UI is a single embedded HTML file** at `src/ui/static/admin.html`, served at `GET /admin` *without* auth. The page reads a token from `localStorage` and adds `Authorization: Bearer` to every `/api/admin/*` call from JS. The static-files route is separate (`/static/*path` → `src/ui/static/` via `rust-embed`).
- **Admin token** is auto-generated on first run if `COMMUNITY_SEARCH_ADMIN_TOKEN` is unset and no token is stored in `app_config`. Generated tokens are 48-char alphanumeric and announced once on stdout (see `auth::token::ensure_and_announce_admin_token`). Setting the env var overwrites the stored value.
- **API handler DB connections**: the API has a *separate* `rusqlite::Connection` from the crawler scheduler (both open the same file). SQLite WAL mode allows concurrent readers across connections — do not introduce a shared mutex spanning both.
