# Community Search

## Overview

Community Search is a self-hosted, federated search engine built as a single Rust binary. An administrator curates which sites are indexed. Users search through a web interface. Multiple instances can peer with each other to form a decentralized search network.

## Motivation

Community curation motivated by privacy and control. The administrator controls the crawl list and ranking. This is a curated alternative to general-purpose search, built for communities of hobbyists and interest groups who want to search across their favorite niche sites.

## Tech Stack

- **Language**: Rust
- **Search engine**: Tantivy (full-text search, native Rust)
- **Database**: SQLite (via rusqlite) for operational data
- **Config**: `.env` file via `dotenvy` (env vars take precedence over .env file, built-in defaults as fallback)
- **Web framework**: TBD by implementer -- likely Axum or Actix-web
- **Output**: Single self-contained binary with embedded web UI

## Core Components

1. **HTTP Server** -- serves the public search UI, the REST API, and handles peer-to-peer communication
2. **Crawler** -- prefix-bounded web crawler with smart re-crawl (respects Last-Modified/ETag)
3. **Indexer** -- Tantivy-based full-text search index
4. **Peer Manager** -- handles fan-out queries to peers, progressive result merging, and gossip exchange
5. **Admin Interface** -- token-protected endpoints for managing crawl targets, peers, ranking, and reviewing outlink suggestions
6. **Rate Limiter** -- per-IP and per-peer rate limiting with escalating cooloff periods

## Data Storage

- **`.env` file / environment variables** -- main configuration. Resolution order: env var > .env file > built-in default.

  | Variable | Default | Purpose |
  |----------|---------|---------|
  | `COMMUNITY_SEARCH_BIND_ADDR` | `127.0.0.1` | Listen address |
  | `COMMUNITY_SEARCH_PORT` | `8080` | Listen port |
  | `COMMUNITY_SEARCH_DATA_DIR` | `./data` | Data directory |
  | `COMMUNITY_SEARCH_ADMIN_TOKEN` | _(auto-generated on first run)_ | Admin API token |
  | `COMMUNITY_SEARCH_INDEX_PATH` | `~/.community-search/index` | Tantivy index directory |
  | `COMMUNITY_SEARCH_MAX_INDEX_BYTES` | `10737418240` (10 GiB) | Max index size on disk |
  | `COMMUNITY_SEARCH_CRAWLER_USER_AGENT` | `community-search/0.1` | Crawler User-Agent string |
  | `COMMUNITY_SEARCH_CRAWLER_REQUEST_TIMEOUT_MS` | `30000` | Crawler HTTP timeout (ms) |
  | `COMMUNITY_SEARCH_CRAWLER_POLITENESS_DELAY_MS` | `250` | Per-domain request delay (ms) |
  | `COMMUNITY_SEARCH_CRAWLER_MAX_CONCURRENT_DOMAINS` | `4` | Max domains crawled concurrently |
  | `COMMUNITY_SEARCH_PEER_RATE_LIMIT_PER_MINUTE` | `120` | Peer search rate limit (req/min) |
  | `SELF_URL` | _(empty)_ | Public URL of this engine (for gossip self-entry) |
  | `SELF_NAME` | _(empty)_ | Display name advertised via gossip |
  | `SELF_DESCRIPTION` | _(empty)_ | Short description advertised via gossip |
  | `GOSSIP_SYNC_INTERVAL_SECS` | `86400` (1 day) | Periodic gossip sync interval |

  Note: the per-crawl-target re-crawl interval is configured as a `recrawl_interval_secs` field on each crawl target row in SQLite, not as a global environment variable.

- **SQLite** -- operational data (crawl state, peer list, discovered engines, outlink suggestions, ranking config, rate limit state, collection metadata)
- **Tantivy on-disk index** -- the search index itself (single index with collection field for logical separation)

## Collections

Collections are the primary organizational unit for content.

- Each collection has a name, description, and its own ranking configuration
- Crawl targets are added to a specific collection
- Outlink suggestions are scoped to the collection that discovered them
- Index size limits can be set per-collection (within the global max)

**Implementation:**

- Single Tantivy index with a `collection` field on every document
- Queries filter by collection (or search all collections for a cross-collection search)
- SQLite stores collection metadata, per-collection crawl targets, and per-collection ranking config

**User-facing search:**

- Search UI shows a collection picker (or "all")
- API accepts an optional `collection` parameter

## Crawler

**Seed model**: Admin provides a URL prefix (e.g., `https://example-blog.com/articles/`). The crawler indexes all pages under that prefix. Crawl targets belong to a specific collection.

**Crawl behavior:**

- Follows links within the prefix boundary
- Respects `robots.txt` per domain (hard requirement, not configurable)
- Configurable politeness delay between requests to the same domain (default: 1 second)
- Configurable concurrent crawl limit (how many domains crawled simultaneously)
- Stores page content, title, URL, and timestamp in Tantivy

**Smart re-crawl:**

- Each crawl target has a configurable re-crawl interval (default: daily)
- On re-crawl, sends `If-Modified-Since` / `If-None-Match` headers
- Only re-indexes pages that return new content (HTTP 200 vs 304)
- Runs as a background task within the binary

**Outlink discovery:**

- Any link found that points outside the current prefix is recorded in SQLite as a "suggested outlink"
- Suggestions include: source page URL, target URL, link text, first-seen timestamp
- Suggestions are scoped to the collection that discovered them
- Admin can review suggestions via the admin UI/API and one-click promote them to new crawl targets

**Crawl politeness:**

- `robots.txt` always respected (hard requirement)
- Per-domain request delay (configurable, default: 1 second)

**Index size management:**

- Configurable max index size (disk space for the Tantivy index)
- When the index approaches the limit, new crawl targets are queued but not crawled until space is freed (admin removes existing targets)
- Admin UI shows current index size vs configured max
- Existing re-crawls continue (they update in place, not grow the index) but new sites are blocked

## Search & Ranking

**Local search:**

- Tantivy handles full-text search with BM25 scoring as the base relevance signal
- Results include: title, URL, snippet (with query term highlighting), source label, timestamp

**Peer search:**

- Query is sent to all collection peers in parallel via `POST /api/search` on each peer
- Progressive merge: local results return immediately, peer results are merged as they arrive via SSE to the UI
- Fallback: if progressive merge adds too much complexity for v1, use fan-out with configurable timeout (e.g., 3 seconds default)

**Ranking knobs** (stored in SQLite, editable via admin API, per-collection):

| Knob | Description | Default |
|------|-------------|---------|
| Source weight | Per-source multiplier (local index, each peer) | Local: 1.0, Peers: 1.0 |
| Freshness decay | Time-based decay factor -- newer content scores higher | Gentle decay (configurable half-life) |
| Domain boost | Per-domain multiplier applied globally regardless of source | 1.0 (neutral) |

**Scoring formula:**

```
final_score = base_relevance * source_weight * freshness_factor * domain_boost
```

Each factor is independently tunable.

## Peer Federation

There are two distinct levels of peering.

### Node Peering (Server-to-Server)

- A server-to-server relationship: "I know about your server"
- Gossip exchange happens at this level
- Browsing another server's collection list happens at this level
- A node peer is just a known server URL
- Node peers are added manually by the admin or discovered via gossip

### Collection Peering (Collection-to-Collection)

- A collection-to-collection subscription: "My collection X includes results from your collection Y"
- Search fan-out happens at this level
- Per-peer ranking weights are actually per-collection-peer weights
- Collection peering requires node peering as a prerequisite (you must know the server to subscribe to its collections)

**Admin workflow:**

1. Add a node peer (or discover one via gossip) -- server level
2. Browse that node's collections via `GET /api/collections` -- server level
3. Subscribe one of your local collections to one of their collections -- collection level
4. Searches in your collection now include results from the subscribed remote collection

**Peer search protocol:**

- When a user searches, the local engine queries `POST /api/search` on each enabled collection peer with the search terms
- Search requests include a `remaining_depth` field to control fanout depth
- Results from all sources are scored locally using the ranking formula and merged into a single result list
- SSE streams results to the UI progressively as peers respond

**Fanout depth** -- no recursive fan-out by default, but configurable:

- `fanout_depth: 1` (default) -- search only direct peers. Peers return local results only.
- `fanout_depth: 2` -- search direct peers + their peers. When a peer receives a query with `remaining_depth > 0`, it fans out to its own peers with `remaining_depth - 1`. At `remaining_depth: 0`, return local results only.

**Peer health:**

- Track response time and failure rate per peer
- After N consecutive failures, auto-disable the peer and surface a notification to the admin
- Background health check runs periodically (e.g., once daily) against disabled peers
- If a disabled peer responds successfully, auto-re-enable it and notify the admin
- Admin can also manually disable/enable at any time

## Gossip & Discovery

**Discovery model:**

- Every engine maintains a list of "discovered engines" in SQLite (URL, name/description, first-seen timestamp, last-seen timestamp)
- Your own engine is always in your discovered list (self-entry)

**Gossip exchange:**

- When any two engines communicate via the gossip endpoint, they exchange their discovered-engine lists
- Endpoint: `POST /api/gossip/exchange` -- send your list, receive theirs, merge
- Merge is additive -- once an engine is discovered, it stays in the list (admin can manually remove entries they don't want)
- Last-seen timestamp updates on each exchange

**Discovery vs Peering:**

- Discovered engines are passive -- you know about them, but don't query them during search
- Peers are active -- your searches fan out to them
- The admin can promote any discovered engine to a node peer with one click
- The admin UI shows the full discovered-engine list as a browsable directory

**Gossip triggers:**

- On first connection to a new peer -- always exchange discovery lists when a peer is first added
- Periodic background sync -- exchange with each peer on a configurable interval (e.g., once daily, alongside the peer health check)
- Manual trigger -- admin can initiate a gossip exchange with any known engine URL on demand

## Rate Limiting & Abuse Protection

**Per-IP rate limiting (search API):**

- Configurable request limit per time window (e.g., 30 requests/minute)
- Escalating cooloff on violation: 1 min → 5 min → 1 hour
- Cooloff resets after a clean period with no violations

**Per-peer rate limiting (peer query endpoints):**

- More generous limits than anonymous search (peers are semi-trusted)
- Configurable per-peer
- Same escalating cooloff model

**Admin endpoint protection:**

- Token required for all admin endpoints
- Rate limit on failed auth attempts per IP (e.g., 5 failures → 15 min lockout)

**Crawl politeness (outbound):**

- `robots.txt` always respected (not configurable -- hard requirement)
- Per-domain request delay (configurable, default: 1 second)
- Configurable concurrent crawl limit (how many domains crawled simultaneously)

**Rate limit state** stored in SQLite so it persists across restarts.

## Admin Interface

**Admin endpoints** (all require API token in Authorization header):

| Area | Purpose |
|------|---------|
| Collections | Create/update/delete collections |
| Crawl targets | Add/remove crawl targets within a collection; configure per-site re-crawl interval |
| Outlink suggestions | List discovered outlinks per collection; promote to crawl target or dismiss |
| Node peers | Add/remove node peers (server-level) |
| Collection peers | Subscribe/unsubscribe collection peering; set per-collection-peer weight |
| Discovered engines | Browse gossip-discovered engines; promote to node peer or remove |
| Ranking config | Set source weights, freshness decay half-life, domain boosts per collection |
| Status | Current index size vs max, crawl queue status, peer health overview |
| Config | Update fanout depth, rate limits, crawl defaults |

**Admin UI:**

- Part of the embedded web UI, behind a token-gated section
- Simple, functional -- tables and forms, not a dashboard
- Same REST API that external tools would use (the UI is just a client)

**First-run experience:**

- On first launch with no existing data directory, the binary generates an admin token and prints it to stdout
- The `.env` file can also set a token explicitly (overrides the generated one)
- Data directory is created automatically with empty SQLite DB and Tantivy index

## REST API

**Public (no auth):**

| Endpoint | Purpose |
|----------|---------|
| `GET /` | Serve embedded search UI |
| `GET /health` | Service health check |
| `GET /api/collections` | List this engine's public collections (name + description) |
| `POST /api/search` | Search a collection (params: query, collection, remaining_depth) |

**Peer-to-peer (no auth, but rate-limited):**

| Endpoint | Purpose |
|----------|---------|
| `POST /api/search` | Same endpoint -- peers call it with `remaining_depth` to control fanout |
| `POST /api/gossip/exchange` | Exchange discovered-engine lists (server-level) |

**Admin (token required):**

| Endpoint | Purpose |
|----------|---------|
| `POST /api/admin/collections` | Create/update/delete collections |
| `POST /api/admin/crawl-targets` | Add/remove crawl targets within a collection |
| `GET /api/admin/outlinks` | Review suggested outlinks per collection |
| `POST /api/admin/outlinks/{id}/promote` | Promote an outlink to a crawl target |
| `POST /api/admin/nodes` | Add/remove node peers (server-level) |
| `POST /api/admin/collection-peers` | Subscribe/unsubscribe collection peering |
| `GET /api/admin/collection-peers` | List collection peer mappings |
| `PUT /api/admin/ranking` | Update ranking config per collection |
| `GET /api/admin/status` | Index size, crawl queue, peer health |
| `PUT /api/admin/config` | Update fanout depth, rate limits, crawl defaults |

**Result streaming:** Search responses use SSE (`text/event-stream`) -- local results come first, then peer results stream in as they arrive.

## Community Search Protocol

This project defines the **Community Search Protocol** -- the wire-level specification for how Community Search engines communicate with each other. The protocol document will live at `docs/COMMUNITY_SEARCH_PROTOCOL.md` as a standalone reference that another implementer could use to build a compatible engine.

**Version advertisement:**

- Every engine includes its protocol version in HTTP response headers: `X-CommunitySearch-Version: 1.0`
- The `GET /api/collections` endpoint returns the protocol version in its JSON response body
- Gossip exchange payloads include the sender's protocol version

**What the protocol specifies:**

| Area | What's specified |
|------|-----------------|
| Search | `POST /api/search` request/response format, `remaining_depth` semantics, SSE event format for streaming results |
| Gossip | `POST /api/gossip/exchange` request/response format, merge semantics (additive, last-seen update) |
| Discovery | `GET /api/collections` response format, what metadata an engine exposes publicly |
| Versioning | Version header convention, how engines handle version mismatches |

**Compatibility rules:**

- Same major version = compatible (must be able to exchange queries and gossip)
- Different major version = incompatible (reject with a clear error, don't silently fail)
- Minor version differences = backward compatible (newer features ignored by older engines)

**Protocol version:** The initial protocol version is `1.0`. The full protocol specification will be maintained in `docs/COMMUNITY_SEARCH_PROTOCOL.md`.

## Architectural Decision: Peer Communication Layer

**Chosen: HTTP REST everywhere.**

All interaction -- search queries, gossip exchange, peer discovery -- happens over HTTP REST. SSE handles progressive result streaming. One protocol to implement, debug, and secure.

**Design hedge:** All peer communication is isolated behind a trait/interface, so the peer layer is pluggable. If NAT traversal or trustless identity becomes a real pain point in the future, a libp2p implementation could be swapped in without touching the rest of the system.

**Rejected alternatives:**

- HTTP + WebSocket for peers (unnecessary connection management complexity for infrequent peer interactions)
- HTTP + libp2p for gossip/peers (massive dependency, overkill for manual peer management and simple list-merging gossip)
