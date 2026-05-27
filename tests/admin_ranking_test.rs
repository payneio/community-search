/// Integration tests for PUT /api/admin/ranking and GET /api/admin/ranking.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token";

// ---------------------------------------------------------------------------
// Request helpers
// ---------------------------------------------------------------------------

fn auth_put(uri: &str, json_body: &str) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/json")
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .body(Body::from(json_body.to_string()))
        .unwrap()
}

fn auth_get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .body(Body::empty())
        .unwrap()
}

fn auth_post(uri: &str, json_body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .body(Body::from(json_body.to_string()))
        .unwrap()
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// PUT ranking for a real collection, then GET it back — all fields preserved.
#[tokio::test]
async fn put_ranking_persists_and_round_trips() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    // Step 1 — create a collection
    let resp = app
        .clone()
        .oneshot(auth_post(
            "/api/admin/collections",
            r#"{"action":"create","name":"rank-test","description":"test"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "create collection must return 200"
    );
    let body = body_json(resp).await;
    let col_id = body["id"].as_i64().expect("collection id must be i64");

    // Step 2 — PUT ranking config
    let put_payload = serde_json::json!({
        "collection_id": col_id,
        "source_weights": {"local": 1.0, "peer": 0.5},
        "freshness_half_life_days": 14.0,
        "domain_boosts": {"good.com": 2.0}
    })
    .to_string();

    let resp = app
        .clone()
        .oneshot(auth_put("/api/admin/ranking", &put_payload))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "PUT ranking must return 200");

    // Step 3 — GET ranking config and verify round-trip
    let resp = app
        .clone()
        .oneshot(auth_get(&format!(
            "/api/admin/ranking?collection_id={col_id}"
        )))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "GET ranking must return 200");

    let body = body_json(resp).await;
    assert_eq!(
        body["collection_id"].as_i64(),
        Some(col_id),
        "collection_id must round-trip"
    );
    assert_eq!(
        body["freshness_half_life_days"].as_f64(),
        Some(14.0),
        "freshness_half_life_days must round-trip"
    );
    assert_eq!(
        body["domain_boosts"]["good.com"].as_f64(),
        Some(2.0),
        "domain_boosts[good.com] must round-trip"
    );
    assert_eq!(
        body["source_weights"]["local"].as_f64(),
        Some(1.0),
        "source_weights[local] must round-trip"
    );
    assert_eq!(
        body["source_weights"]["peer"].as_f64(),
        Some(0.5),
        "source_weights[peer] must round-trip"
    );
}

/// PUT ranking for a collection that does not exist must return 404.
#[tokio::test]
async fn put_ranking_for_nonexistent_collection_is_404() {
    let app = community_search::test_support::test_router_full(ADMIN_TOKEN);

    let put_payload = serde_json::json!({
        "collection_id": 999999,
        "source_weights": {"local": 1.0},
        "freshness_half_life_days": 30.0,
        "domain_boosts": {}
    })
    .to_string();

    let resp = app
        .oneshot(auth_put("/api/admin/ranking", &put_payload))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "PUT ranking for nonexistent collection must return 404"
    );
}
