# Community Search

Federated search built from community-curated indexes.

## Why

General-purpose search engines optimize for scale and engagement. Valuable knowledge increasingly lives in niche sites, independent blogs, research groups, and community-curated collections that those engines surface poorly.

Community Search takes a different approach: communities define what gets indexed, collections federate selectively across trusted peers, and trust stays local. There is no global index — only a network of intentional ones. The same retrieval surface is useful for humans looking for trustworthy sources and for AI systems that need provenance-aware retrieval instead of opaque web-scale corpora.

The longer thesis — why search needs to be rebuilt around trust and community curation — is in [community-search.md](community-search.md).

## Design

**You control the index.** Define URL prefixes (e.g. `https://example.com/blog/`); the crawler indexes everything under them and nothing outside. Collections are intentionally curated, not globally crawled. Links pointing outside the prefix are surfaced as suggestions for review rather than followed.

**Federation is selective.** Subscribe a local collection to a collection on a trusted peer. Searches fan out only to subscribed collections, and results are re-ranked locally with a per-subscription source weight.

**Discovery is separate from trust.** Engines find each other via gossip — a browsable directory builds up without any central registry — but discovered engines aren't queried until you promote them to active peers. Two distinct layers, *node peering* for discovery and *collection peering* for retrieval, keep transport, trust, and editorial scope from collapsing into each other.

**Interoperability is a protocol, not an app.** Peer communication is defined by the [Community Search Protocol](docs/COMMUNITY_SEARCH_PROTOCOL.md) — an open wire spec any implementation can speak. This repository is one implementation.

## Capabilities

- Full-text search with BM25 ranking
- Configurable ranking: per-source weights, freshness decay, per-domain boosts
- Prefix-bounded web crawler; stays within the URL prefix you specify
- Respects `robots.txt` (hard requirement, not configurable)
- Smart re-crawl using `ETag` / `Last-Modified` — only re-indexes changed pages
- Outlink discovery: links outside the prefix are surfaced as suggestions for the admin to review
- Collections for organizing indexes, each with independent ranking config
- Federation with configurable fan-out depth (1 or 2 hops)
- Gossip-based engine discovery (`POST /api/gossip/exchange`)
- Community Search Protocol v1.0 — documented wire spec for interoperability
- Search results streamed via SSE as peers respond
- Admin API and embedded web UI (no separate frontend deploy)
- Per-IP and per-peer rate limiting with escalating cooloffs
- Single binary, no external runtime dependencies — SQLite and Tantivy are embedded

## Quick Start

**Prerequisites:** Rust toolchain (stable).

```bash
git clone <this repo>
cd community-search
cargo build --release
./target/release/community-search
```

On first run with no existing data directory, the binary prints the generated admin token to stdout and creates `./data/` automatically. Save that token — it's what you'll use for all admin API calls.

- Search UI: http://localhost:8080
- Admin UI: http://localhost:8080/admin

### Key environment variables

| Variable | Default | Purpose |
|---|---|---|
| `COMMUNITY_SEARCH_BIND_ADDR` | `127.0.0.1` | Listen address |
| `COMMUNITY_SEARCH_PORT` | `8080` | Listen port |
| `COMMUNITY_SEARCH_DATA_DIR` | `./data` | Data directory (SQLite + index) |
| `COMMUNITY_SEARCH_ADMIN_TOKEN` | _(auto-generated)_ | Admin API token |
| `COMMUNITY_SEARCH_INDEX_PATH` | `~/.community-search/index` | Tantivy index directory |
| `COMMUNITY_SEARCH_MAX_INDEX_BYTES` | `10737418240` (10 GiB) | Max index size on disk |
| `SELF_URL` | _(empty)_ | Public URL of this engine, used in gossip |
| `SELF_NAME` | _(empty)_ | Display name advertised via gossip |
| `SELF_DESCRIPTION` | _(empty)_ | Short description advertised via gossip |

## Configuration

Put a `.env` file in the working directory. Environment variables override `.env` values; built-in defaults apply for anything not set.

```dotenv
COMMUNITY_SEARCH_BIND_ADDR=0.0.0.0
COMMUNITY_SEARCH_PORT=8080
COMMUNITY_SEARCH_DATA_DIR=./data
COMMUNITY_SEARCH_ADMIN_TOKEN=your-secret-token

# Index
COMMUNITY_SEARCH_INDEX_PATH=./data/index
COMMUNITY_SEARCH_MAX_INDEX_BYTES=10737418240

# Crawler
COMMUNITY_SEARCH_CRAWLER_USER_AGENT=community-search/0.1
COMMUNITY_SEARCH_CRAWLER_REQUEST_TIMEOUT_MS=30000
COMMUNITY_SEARCH_CRAWLER_POLITENESS_DELAY_MS=250
COMMUNITY_SEARCH_CRAWLER_MAX_CONCURRENT_DOMAINS=4

# Rate limiting
COMMUNITY_SEARCH_PEER_RATE_LIMIT_PER_MINUTE=120

# Gossip identity (set these if you want other engines to find you)
SELF_URL=https://search.example.com
SELF_NAME=Example Community Search
SELF_DESCRIPTION=Indexing niche hobby sites since 2025

# Gossip sync interval
GOSSIP_SYNC_INTERVAL_SECS=86400
```

## Usage

All admin endpoints require `Authorization: Bearer <token>`.

### Create a collection

```bash
curl -s -X POST http://localhost:8080/api/admin/collections \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "hobby-blogs", "description": "Curated hobby and maker blogs"}'
```

### Add a crawl target

```bash
curl -s -X POST http://localhost:8080/api/admin/crawl-targets \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "collection_id": 1,
    "url_prefix": "https://example-blog.com/posts/",
    "recrawl_interval_secs": 86400
  }'
```

The crawler will index all pages under the prefix and queue periodic re-crawls.

### Search

The web UI at http://localhost:8080 includes a collection picker and streams results as peers respond. For scripts and tools, `GET /api/search` returns a single JSON document:

```bash
curl -s "http://localhost:8080/api/search?q=mechanical+keyboards&collection=hobby-blogs"
# {"protocol_version":"1.0","results":[ ... ],"duration_ms":12}
```

| Query param  | Default | Notes                                                             |
|--------------|---------|-------------------------------------------------------------------|
| `q`          | —       | Required. The search query (`query` also accepted).               |
| `collection` | all     | Optional collection name to scope the search.                     |
| `depth`      | `0`     | `0` = this engine only; `1`–`2` also query federated peers (slower). |

The browser-facing `POST /api/search` streams the same results as Server-Sent Events; it also returns this JSON shape when called with `Accept: application/json`.

### Machine & agent access

- **`GET /opensearch.xml`** — an OpenSearch descriptor, so browsers can add this engine as a search provider. Set `SELF_URL` for absolute URLs.
- **`GET /robots.txt`** — steers crawlers to the homepage and away from result/API pages.
- **`POST /mcp`** — a [Model Context Protocol](https://modelcontextprotocol.io) server exposing a `search` tool, so LLM agents (ChatGPT, Claude, etc.) can query the index directly:

```bash
curl -s -X POST http://localhost:8080/mcp -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call",
       "params":{"name":"search","arguments":{"query":"mechanical keyboards"}}}'
```

### Add a node peer

```bash
curl -s -X POST http://localhost:8080/api/admin/nodes \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"url": "https://search.friend.example.com"}'
```

Adding a node peer triggers an immediate gossip exchange.

### Subscribe a collection to a peer collection

```bash
curl -s -X POST http://localhost:8080/api/admin/collection-peers \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "local_collection_id": 1,
    "peer_url": "https://search.friend.example.com",
    "remote_collection_name": "their-collection"
  }'
```

Searches in `hobby-blogs` will now include results from the peer's collection.

## Federation

There are two distinct levels of peering.

**Node peering** is a server-to-server relationship: "I know your server exists." Gossip exchange happens here. You can browse a node peer's collection list. Adding a node peer is a prerequisite for collection peering.

**Collection peering** is a collection-to-collection subscription: "My collection X includes results from your collection Y." Search fan-out happens here. Each collection-peer relationship has its own configurable source weight in the ranking formula.

**Fan-out depth** is configurable:
- `fanout_depth: 1` (default) — query only direct collection peers, which return local results only.
- `fanout_depth: 2` — direct peers also fan out to their peers. Controlled by the `depth` field in the search request; peers decrement it and stop at 0.

Results from all sources are re-ranked locally using the scoring formula:

```
final_score = base_relevance × source_weight × freshness_factor × domain_boost
```

Each factor is tunable per collection via the admin API or UI.

## Gossip & Discovery

Every engine maintains a table of discovered engines (URL, name, description, first-seen, last-seen). Your own engine is always in the table if `SELF_URL` is set.

When two engines communicate via `POST /api/gossip/exchange`, they swap their full discovery tables and merge additively — engines are never removed automatically. The last-seen timestamp updates on each exchange.

Gossip triggers:
1. Immediately when a new node peer is added.
2. Periodically in the background (`GOSSIP_SYNC_INTERVAL_SECS`, default daily).
3. On demand from the admin UI.

Discovered engines are passive — you see them but don't query them during search. Promote a discovered engine to a node peer (one click in the admin UI) to make it active.

## Community Search Protocol

The **Community Search Protocol v1.0** is an open interoperability layer for federated retrieval, covering search fan-out, gossip exchange, and engine discovery. This engine is one implementation; any system that speaks the protocol can participate in the network. Every response includes `X-CommunitySearch-Version: 1.0`.

Full specification: [docs/COMMUNITY_SEARCH_PROTOCOL.md](docs/COMMUNITY_SEARCH_PROTOCOL.md)

Compatibility rules: same major version = compatible; different major version = rejected with a clear error.

## Architecture

Single Rust binary. No external runtime dependencies.

- **HTTP server**: Axum, serving the public search UI, REST API, and peer endpoints
- **Search index**: Tantivy (embedded, on-disk BM25 full-text index with a `collection` field for logical separation)
- **Operational data**: SQLite via rusqlite (crawl state, peer list, discovered engines, ranking config, rate limit state, collection metadata)
- **Web UI**: Embedded in the binary, served from `/`; the admin section is the same UI behind a token gate
- **Crawler**: Background task within the binary; prefix-bounded, respects `robots.txt`, uses `ETag`/`Last-Modified` for smart re-crawl
- **Peer communication**: HTTP REST everywhere; SSE for streaming search results

## Development

```bash
# Run tests
cargo test

# Lint
cargo clippy

# Format
cargo fmt
```

## License

This project is licensed under the [GNU Affero General Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.html). See [LICENSE](LICENSE) for the full text.
