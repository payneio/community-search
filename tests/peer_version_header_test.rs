use axum::http::StatusCode;
use community_search::test_support::spawn_test_server;

#[tokio::test]
async fn peer_facing_responses_include_version_header() {
    let app = spawn_test_server().await;
    for path in ["/api/collections", "/api/search", "/api/gossip/exchange"] {
        let resp = if path == "/api/collections" {
            app.client
                .get(format!("{}{}", app.base_url, path))
                .send()
                .await
                .unwrap()
        } else {
            app.client
                .post(format!("{}{}", app.base_url, path))
                .json(&serde_json::json!({"query": "", "engines": [], "protocol_version": "1.0"}))
                .send()
                .await
                .unwrap()
        };
        assert_ne!(resp.status(), StatusCode::NOT_FOUND);
        let hdr = resp
            .headers()
            .get("X-CommunitySearch-Version")
            .unwrap_or_else(|| panic!("{path}: missing header"));
        assert_eq!(hdr, "1.0");
    }
}

#[tokio::test]
async fn root_ui_does_not_advertise_peer_version() {
    let app = spawn_test_server().await;
    let resp = app
        .client
        .get(format!("{}/", app.base_url))
        .send()
        .await
        .unwrap();
    assert!(resp.headers().get("X-CommunitySearch-Version").is_none());
}
