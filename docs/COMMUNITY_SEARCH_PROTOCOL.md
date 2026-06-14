# Community Search Protocol — Version 1.0

The Community Search Protocol is the wire-level specification for how
independent search engines communicate to form a federated, decentralised
search network.

This document is the authoritative reference. Any engine that implements the
endpoints and behaviours below — regardless of language or storage backend —
can interoperate with Community Search and other Community-Search-compatible
engines.

## 1. Overview

A Community Search engine exposes three peer-facing endpoints over HTTPS:

| Endpoint                       | Purpose                                       |
|--------------------------------|-----------------------------------------------|
| `GET  /api/collections`        | List public collections + advertise version   |
| `POST /api/search`             | Federated search with bounded fan-out depth   |
| `POST /api/gossip/exchange`    | Exchange discovered-engine lists              |

All peer-facing responses MUST include the `X-CommunitySearch-Version` HTTP
header identifying the protocol version the engine speaks.

## 2. Versioning

- **Current version:** `1.0`
- **Header:** `X-CommunitySearch-Version: <major>.<minor>`
- **Body:** `GET /api/collections` and `POST /api/gossip/exchange` MUST include
  a top-level `"protocol_version"` field in their JSON response.

### Compatibility rules

- **Same major version:** engines MUST be able to exchange queries and gossip.
- **Different major version:** engines MUST reject the exchange with a clear
  error response and not silently fall through.
- **Different minor version (same major):** engines MUST treat the exchange as
  backward compatible. Newer fields not understood by the receiver MUST be
  ignored. Engines SHOULD log a warning.

Rejection of a major-version mismatch on `POST /api/gossip/exchange` returns
HTTP `400 Bad Request` with a JSON body:

```json
{ "error": "incompatible protocol version: theirs=2.0 ours=1.0" }
```

## 3. Endpoint: `GET /api/collections`

Returns the list of collections this engine exposes publicly.

### Response — `200 OK`

```json
{
  "protocol_version": "1.0",
  "collections": [
    {
      "name": "rust-blogs",
      "description": "Hand-curated Rust community blogs",
      "doc_count": 12453
    }
  ]
}
```

| Field              | Type    | Required | Notes                                  |
|--------------------|---------|----------|----------------------------------------|
| protocol_version   | string  | yes      | `<major>.<minor>`                      |
| collections        | array   | yes      | May be empty                           |
| collections[].name        | string  | yes      | Unique within the engine        |
| collections[].description | string  | yes      | May be empty                    |
| collections[].doc_count   | integer | no       | Approximate; for UI display     |

## 4. Endpoint: `POST /api/search`

Federated search. Called by users and by other engines (fan-out).

### Request

```json
{
  "query": "tokio async runtime",
  "collection": "rust-blogs",
  "depth": 0,
  "limit": 25
}
```

| Field             | Type    | Required | Notes                                                  |
|-------------------|---------|----------|--------------------------------------------------------|
| query             | string  | yes      | Free-text query, BM25-friendly                         |
| collection        | string  | no       | Omit or empty string = search all public collections   |
| depth             | integer | yes      | See §4.1                                               |
| limit             | integer | no       | Max results per source; default 25                     |

> The field was named `remaining_depth` prior to v1.0; engines SHOULD accept it
> as a deprecated alias for `depth` on input.

### 4.1 `depth` semantics

- The originating client (or upstream peer) sets `depth` to `fanout_depth`
  (e.g. `1` for direct peers only).
- An engine that receives a request with `depth > 0` MAY fan out to its own
  collection peers with `depth - 1`.
- At `depth == 0` the engine MUST return only its local results;
  it MUST NOT fan out further.
- Engines MUST NOT increase `depth` when forwarding.

### 4.2 Response — `text/event-stream`

Search responses are streamed using Server-Sent Events. Each event is one of:

- `event: result` — a single result row, JSON-encoded in the `data:` field.
- `event: done`   — terminator with summary metadata.
- `event: error`  — recoverable error from one source; the stream MAY continue.

```
event: result
data: {"source":"local","collection":"rust-blogs","title":"Async Rust","url":"https://example.com/post","snippet":"...","score":12.5,"timestamp":1717000000}

event: result
data: {"source":"https://peer.example.com","collection":"rust-blogs","title":"Tokio Internals","url":"https://peer.example.com/p","snippet":"...","score":9.8,"timestamp":1716900000}

event: done
data: {"total_sources":3,"completed_sources":3,"duration_ms":845}
```

| Result field   | Type    | Required | Notes                                       |
|----------------|---------|----------|---------------------------------------------|
| source         | string  | yes      | `"local"` or peer engine URL                |
| collection     | string  | yes      |                                             |
| title          | string  | yes      |                                             |
| url            | string  | yes      |                                             |
| snippet        | string  | yes      | Plain text; may include `<mark>` highlights |
| score          | number  | yes      | Engine-local relevance score                |
| timestamp      | integer | no       | Unix seconds; last-modified of the doc      |

Result merging across sources is the *receiver's* responsibility. Each source's
scores are local to that source — receivers normalise / apply per-source
weights according to their own ranking config.

### 4.3 Non-streaming fallback

Engines that cannot implement SSE MAY accept `Accept: application/json` and
return a single JSON object:

```json
{
  "protocol_version": "1.0",
  "results": [ /* same shape as `event: result` data */ ],
  "duration_ms": 845
}
```

SSE is the canonical form and is REQUIRED for full compatibility.

> Non-normative: engines MAY additionally expose a `GET /api/search` convenience
> endpoint (query string in, this same JSON object out) and an MCP server for
> human/agent consumers. These are outside the federation contract; see the
> README and `docs/spec.md`.

## 5. Endpoint: `POST /api/gossip/exchange`

Exchange "discovered engine" lists. Each engine maintains a passive directory
of other engines it knows about, including its own self-entry. Gossip merges
two such directories on contact.

### Request

```json
{
  "protocol_version": "1.0",
  "engines": [
    {
      "url": "https://search1.example.com",
      "name": "Search 1",
      "description": "Rust community search"
    },
    {
      "url": "https://search2.example.com",
      "name": "Search 2",
      "description": "Go community search"
    }
  ]
}
```

### Response — `200 OK`

```json
{
  "protocol_version": "1.0",
  "engines": [
    {
      "url": "https://search3.example.com",
      "name": "Search 3",
      "description": "Python community search"
    },
    {
      "url": "https://search1.example.com",
      "name": "Search 1",
      "description": "Rust community search"
    }
  ]
}
```

| Field                | Type    | Required | Notes                          |
|----------------------|---------|----------|--------------------------------|
| protocol_version     | string  | yes      |                                |
| engines              | array   | yes      | May be empty                   |
| engines[].url        | string  | yes      | Absolute URL, no trailing slash|
| engines[].name       | string  | no       | Defaults to empty string       |
| engines[].description| string  | no       | Defaults to empty string       |

### 5.1 Merge semantics

When engine A sends its list to engine B:

1. B unions the two lists by `url`.
2. For each URL present on both sides, B keeps the **earliest** `first_seen`
   it knows about and the **latest** `last_seen` (and updates `last_seen` to
   "now" on contact).
3. `name` and `description` from the newer side (by `last_seen`) overwrite the
   older side. Ties favour the existing local value.
4. The merge is **additive**: an engine, once discovered, stays in the list
   until an operator explicitly removes it.
5. Each engine's response to a gossip request SHOULD reflect the *pre-merge*
   state — so the sender learns the union of both lists.

### 5.2 Self-entry

Every engine MUST include itself in its own discovered-engine list. The
self-entry is created on first startup and refreshed (its `last_seen`
advanced) on every startup. Operators MUST NOT be able to remove the
self-entry through normal admin operations.

### 5.3 Gossip triggers

Implementations SHOULD perform gossip exchanges in these situations:

- **On first peer connection**: when a new node peer is added by the
  operator, exchange immediately.
- **Periodic sync**: exchange with each known node peer on a configurable
  interval (e.g. once per day).
- **Manual trigger**: operators can invoke a gossip exchange with any
  reachable URL on demand.

### 5.4 Discovery vs. peering

A *discovered engine* is passive — it is known but not queried during search.
A *node peer* is an engine the operator has explicitly added; only node peers
that are subscribed at the collection level participate in search fan-out.
Discovered engines can be promoted to node peers; this is an operator
decision, not an automatic one.

## 6. Rate Limiting

Engines SHOULD apply per-peer rate limits to all peer-facing endpoints.
Recommended posture:

- `POST /api/search`: more generous than anonymous user search; for example
  60 requests/minute per peer with escalating cooloff on violation.
- `POST /api/gossip/exchange`: low volume; e.g. 6 requests/hour per peer.
- Violations should escalate (1 min → 5 min → 1 hour cooloff) and reset after
  a clean window.

## 7. Transport & Security

- All peer-facing endpoints SHOULD be served over HTTPS in production.
- Engines MUST NOT trust peer-supplied content beyond using it for ranked
  display. In particular, snippets from peers MUST be treated as untrusted
  HTML and escaped or sanitised before rendering.
- No authentication is required for peer endpoints. Trust is established by
  the operator's manual peer-add step plus rate limiting.

## 8. Compatibility Matrix

| Our version | Their version | Behaviour                              |
|-------------|---------------|----------------------------------------|
| 1.0         | 1.0           | Compatible — full exchange             |
| 1.0         | 1.x  (x>0)    | Backward compatible — ignore unknowns  |
| 1.0         | 2.x           | Incompatible — reject with `400`       |
| 1.0         | 0.x           | Incompatible — reject with `400`       |
| 1.0         | malformed     | Incompatible — reject with `400`       |

## 9. Conformance

A conforming Community Search Protocol 1.0 engine MUST:

1. Serve `GET /api/collections` returning a body with `protocol_version: "1.0"`.
2. Serve `POST /api/search` and accept the `depth` field (and its deprecated
   `remaining_depth` alias).
3. Serve `POST /api/gossip/exchange` with the merge semantics in §5.1.
4. Include the `X-CommunitySearch-Version: 1.0` header on every peer-facing
   response.
5. Reject gossip exchanges with a different major protocol version.
6. Maintain a self-entry in its discovered-engine list.

A conforming engine SHOULD:

- Serve `POST /api/search` results as SSE.
- Implement the three gossip triggers in §5.3.
- Apply per-peer rate limits per §6.

## 10. Changelog

| Version | Date       | Notes                          |
|---------|------------|--------------------------------|
| 1.0     | (TBD)      | Initial release                |

## 11. Reference Implementation

The Community Search project at <https://github.com/[org]/community-search>
is the reference implementation for this protocol. Disagreements between this
document and the reference implementation SHOULD be resolved in favour of
this document; bugs in the reference implementation are not protocol changes.
