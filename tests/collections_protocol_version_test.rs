use community_search::test_support::spawn_test_server;

/// GET /api/collections must return a JSON object with:
///   - protocol_version: "1.0"
///   - collections: an array
#[tokio::test]
async fn collections_response_includes_protocol_version() {
    let app = spawn_test_server().await;
    let resp = app
        .client
        .get(format!("{}/api/collections", app.base_url))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["protocol_version"], "1.0");
    assert!(body["collections"].is_array());
}
