//! Minimal Model Context Protocol (MCP) server over Streamable HTTP.
//!
//! Exposes this engine's search as an MCP `search` tool so LLM agents
//! (ChatGPT, Claude, and other MCP clients) can query the curated index with a
//! typed tool call instead of scraping HTML. Speaks JSON-RPC 2.0 at
//! `POST /mcp` and replies with a single JSON object — the Streamable HTTP
//! spec permits this for request/response interactions that need no
//! server-initiated streaming, which keeps the implementation simple.
//!
//! This is a hand-rolled, dependency-free implementation of the handful of
//! methods a simple connector uses (`initialize`, `tools/list`, `tools/call`,
//! `ping`, and the `notifications/*` notifications). It is intentionally
//! stateless — no `Mcp-Session-Id` handshake — which is sufficient for the
//! search use case.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};

use crate::api::public::{collect_search, AppState, MAX_FANOUT_DEPTH};

/// MCP protocol revision this server implements.
const MCP_PROTOCOL_VERSION: &str = "2025-06-18";

/// `POST /mcp` — JSON-RPC 2.0 dispatch for the MCP Streamable HTTP transport.
pub async fn mcp_handler(State(state): State<AppState>, body: Json<Value>) -> Response {
    let req = body.0;

    // Batching was removed in MCP 2025-06-18; reject arrays explicitly.
    if req.is_array() {
        return Json(rpc_error(
            Value::Null,
            -32600,
            "batch requests are not supported",
        ))
        .into_response();
    }

    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");

    match method {
        "initialize" => Json(rpc_result(id, initialize_result())).into_response(),
        // Notifications carry no `id` and MUST NOT receive a response body.
        m if m.starts_with("notifications/") => StatusCode::ACCEPTED.into_response(),
        "ping" => Json(rpc_result(id, json!({}))).into_response(),
        "tools/list" => Json(rpc_result(id, tools_list())).into_response(),
        "tools/call" => tools_call(&state, id, req.get("params")).await,
        "" => Json(rpc_error(id, -32600, "missing `method`")).into_response(),
        other => Json(rpc_error(id, -32601, &format!("method not found: {other}"))).into_response(),
    }
}

/// `initialize` result: advertise the tools capability and server identity.
fn initialize_result() -> Value {
    json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": {
            "name": "community-search",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "instructions": "Use the `search` tool to query this Community Search engine's curated, federated index.",
    })
}

/// `tools/list` result: the single `search` tool and its input schema.
fn tools_list() -> Value {
    json!({
        "tools": [{
            "name": "search",
            "title": "Search",
            "description": "Full-text search over this Community Search engine's curated index. \
                            Returns ranked results, each with title, url, snippet (HTML with <mark> \
                            highlights), source, and score.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query."
                    },
                    "collection": {
                        "type": "string",
                        "description": "Optional collection name to scope the search."
                    },
                    "depth": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": MAX_FANOUT_DEPTH,
                        "default": 0,
                        "description": "Federation fan-out depth. 0 = this engine only; \
                                        1-2 = also query federated peers (slower)."
                    }
                },
                "required": ["query"]
            }
        }]
    })
}

/// `tools/call` for the `search` tool.
async fn tools_call(state: &AppState, id: Value, params: Option<&Value>) -> Response {
    let name = params.and_then(|p| p.get("name")).and_then(Value::as_str);
    if name != Some("search") {
        return Json(rpc_error(
            id,
            -32602,
            &format!("unknown tool: {}", name.unwrap_or("<none>")),
        ))
        .into_response();
    }

    let args = params.and_then(|p| p.get("arguments"));
    let query = args
        .and_then(|a| a.get("query"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    if query.is_empty() {
        return Json(rpc_error(id, -32602, "missing required argument: `query`")).into_response();
    }
    let collection = args
        .and_then(|a| a.get("collection"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let depth = args
        .and_then(|a| a.get("depth"))
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .min(MAX_FANOUT_DEPTH as u64) as u32;

    let results = collect_search(state, query, collection, depth).await;
    let count = results.len();
    let structured = json!({ "results": results, "count": count });
    // Tool results carry a human/agent-readable `content` block; the parsed
    // form is mirrored in `structuredContent` for clients that consume it.
    let text = serde_json::to_string_pretty(&structured).unwrap_or_else(|_| "{}".to_string());

    Json(rpc_result(
        id,
        json!({
            "content": [ { "type": "text", "text": text } ],
            "structuredContent": structured,
            "isError": false,
        }),
    ))
    .into_response()
}

/// Build a JSON-RPC 2.0 success envelope.
fn rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// Build a JSON-RPC 2.0 error envelope.
fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request, routing::post, Router};
    use tower::ServiceExt;

    use crate::api::public::AppState;

    fn app() -> Router {
        Router::new()
            .route("/mcp", post(mcp_handler))
            .with_state(AppState::for_tests_with_token("test-admin-token"))
    }

    async fn call(body: Value) -> Value {
        let res = app()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn initialize_advertises_tools_capability() {
        let v = call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "protocolVersion": "2025-06-18", "capabilities": {} }
        }))
        .await;
        assert_eq!(v["id"], 1);
        assert_eq!(v["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert!(v["result"]["capabilities"]["tools"].is_object());
        assert_eq!(v["result"]["serverInfo"]["name"], "community-search");
    }

    #[tokio::test]
    async fn tools_list_exposes_search() {
        let v = call(json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" })).await;
        let tools = v["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "search");
        assert!(tools[0]["inputSchema"]["properties"]["query"].is_object());
    }

    #[tokio::test]
    async fn tools_call_search_returns_structured_content() {
        // Empty index → zero results, but the envelope must be well-formed.
        let v = call(json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "search", "arguments": { "query": "anything" } }
        }))
        .await;
        assert_eq!(v["id"], 3);
        assert_eq!(v["result"]["isError"], false);
        assert_eq!(v["result"]["structuredContent"]["count"], 0);
        assert!(v["result"]["content"][0]["text"].is_string());
    }

    #[tokio::test]
    async fn tools_call_missing_query_is_invalid_params() {
        let v = call(json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": { "name": "search", "arguments": {} }
        }))
        .await;
        assert_eq!(v["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn unknown_method_is_method_not_found() {
        let v = call(json!({ "jsonrpc": "2.0", "id": 5, "method": "bogus/method" })).await;
        assert_eq!(v["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn notification_gets_no_body() {
        let res = app()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);
    }
}
