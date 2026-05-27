use community_search::db::Database;
use tempfile::TempDir;

#[test]
fn opens_new_database_creating_parent_directory() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("nested").join("data.sqlite");

    let _db = Database::open(&db_path).expect("should open database");

    assert!(db_path.exists(), "database file should exist at path");
}

#[test]
fn opens_existing_database_idempotently() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("data.sqlite");

    // First open — creates the file
    {
        let _db = Database::open(&db_path).expect("first open should succeed");
    }

    // Second open — file already exists
    let _db = Database::open(&db_path).expect("second open should succeed (idempotent)");
}

#[test]
fn database_enables_foreign_keys() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("fk_test.sqlite");

    let db = Database::open(&db_path).expect("should open database");
    let conn = db.conn();

    let fk_enabled: i64 = conn
        .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
        .expect("should query foreign_keys pragma");

    assert_eq!(fk_enabled, 1, "foreign_keys should be ON (1)");
}

// ── Phase 2 collection CRUD tests ────────────────────────────────────────

fn open_tmp_db() -> (tempfile::TempDir, community_search::db::Database) {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.sqlite");
    let db = Database::open(&db_path).expect("should open database");
    (tmp, db)
}

#[test]
fn create_collection_then_get_by_id() {
    use community_search::db::collections;

    let (_tmp, db) = open_tmp_db();

    let col = collections::create(&db, "Recipes", "A collection of recipes")
        .expect("should create collection");

    // id is a non-empty uuid string
    assert!(!col.id.is_empty(), "id should be non-empty");
    assert_eq!(col.name, "Recipes");
    assert_eq!(col.description, "A collection of recipes");
    // created_at == updated_at on creation
    assert_eq!(
        col.created_at, col.updated_at,
        "created_at and updated_at should match on creation"
    );

    // get_by_id returns Some with equal fields
    let fetched = collections::get_by_id(&db, &col.id)
        .expect("should query collection")
        .expect("collection should exist");

    assert_eq!(fetched.id, col.id);
    assert_eq!(fetched.name, col.name);
    assert_eq!(fetched.description, col.description);
    assert_eq!(fetched.created_at, col.created_at);
    assert_eq!(fetched.updated_at, col.updated_at);
}

#[test]
fn get_nonexistent_collection_returns_none() {
    use community_search::db::collections;

    let (_tmp, db) = open_tmp_db();

    let result = collections::get_by_id(&db, "does-not-exist").expect("query should not error");

    assert!(result.is_none(), "should return None for nonexistent id");
}

#[test]
fn create_duplicate_name_fails() {
    use community_search::db::collections;

    let (_tmp, db) = open_tmp_db();

    collections::create(&db, "Recipes", "First").expect("first create should succeed");

    let err = collections::create(&db, "Recipes", "Second")
        .expect_err("second create with same name should fail");

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("unique") || msg.contains("constraint"),
        "error message '{}' should mention unique or constraint",
        msg
    );
}

// ── Phase 1 schema tests ──────────────────────────────────────────────────────

fn all_table_names(db: &Database) -> Vec<String> {
    let conn = db.conn();
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .expect("should prepare sqlite_master query");
    stmt.query_map([], |row| row.get::<_, String>(0))
        .expect("should query table names")
        .map(|r| r.expect("should read table name"))
        .collect()
}

#[test]
fn schema_creates_all_phase1_tables() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("schema_test.sqlite");

    let db = Database::open(&db_path).expect("should open database");

    let tables = all_table_names(&db);

    let expected = vec![
        "collection_peers",
        "collections",
        "crawl_targets",
        "discovered_engines",
        "node_peers",
        "outlink_host_suggestions",
        "ranking_config",
        "rate_limit_state",
        "settings",
    ];

    for table in &expected {
        assert!(
            tables.contains(&table.to_string()),
            "missing table: {}",
            table
        );
    }
}

#[test]
fn schema_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("idempotent_test.sqlite");

    // First open — creates DB and applies schema
    {
        let _db = Database::open(&db_path).expect("first open should succeed");
    }

    // Second open — re-applies schema, should not error or duplicate tables
    let db = Database::open(&db_path).expect("second open should succeed (schema idempotent)");

    let tables = all_table_names(&db);

    // Each table should appear exactly once
    let expected = vec![
        "collection_peers",
        "collections",
        "crawl_targets",
        "discovered_engines",
        "node_peers",
        "outlink_host_suggestions",
        "ranking_config",
        "rate_limit_state",
        "settings",
    ];

    for table in &expected {
        let count = tables.iter().filter(|t| t.as_str() == *table).count();
        assert_eq!(count, 1, "table {} should appear exactly once", table);
    }
}

// ── Phase 3 collection list/update/delete tests ──────────────────────────────

#[test]
fn list_returns_all_collections_alphabetically() {
    use community_search::db::collections;

    let (_tmp, db) = open_tmp_db();

    collections::create(&db, "zeta", "z").expect("create zeta");
    collections::create(&db, "alpha", "a").expect("create alpha");
    collections::create(&db, "mango", "m").expect("create mango");

    let list = collections::list(&db).expect("list should succeed");
    let names: Vec<&str> = list.iter().map(|c| c.name.as_str()).collect();

    assert_eq!(names, vec!["alpha", "mango", "zeta"]);
}

#[test]
fn update_collection_changes_fields_and_updated_at() {
    use community_search::db::collections;
    use std::thread;
    use std::time::Duration;

    let (_tmp, db) = open_tmp_db();

    let original =
        collections::create(&db, "Original", "Original desc").expect("create should succeed");

    thread::sleep(Duration::from_millis(1100));

    collections::update(&db, &original.id, "Updated", "Updated desc")
        .expect("update should succeed");

    let updated = collections::get_by_id(&db, &original.id)
        .expect("query should succeed")
        .expect("collection should still exist");

    assert_eq!(updated.name, "Updated");
    assert_eq!(updated.description, "Updated desc");
    assert!(
        updated.updated_at > original.updated_at,
        "updated_at should be newer after update"
    );
    assert_eq!(
        updated.created_at, original.created_at,
        "created_at should not change"
    );
}

#[test]
fn update_nonexistent_collection_returns_error() {
    use community_search::db::collections;

    let (_tmp, db) = open_tmp_db();

    let err = collections::update(&db, "no-such-id", "Name", "Desc")
        .expect_err("update on missing id should fail");

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not found"),
        "error message '{}' should contain 'not found'",
        msg
    );
}

#[test]
fn delete_removes_the_collection() {
    use community_search::db::collections;

    let (_tmp, db) = open_tmp_db();

    let col = collections::create(&db, "ToDelete", "going away").expect("create should succeed");

    let removed = collections::delete(&db, &col.id).expect("delete should not error");
    assert!(removed, "delete should return true when row existed");

    let fetched = collections::get_by_id(&db, &col.id).expect("get_by_id should not error");
    assert!(fetched.is_none(), "collection should be gone after delete");
}

#[test]
fn delete_returns_false_for_nonexistent_id() {
    use community_search::db::collections;

    let (_tmp, db) = open_tmp_db();

    let removed = collections::delete(&db, "no-such-id").expect("delete should not error");
    assert!(
        !removed,
        "delete should return false when row did not exist"
    );
}

// ── Phase 9 admin token / settings tests ────────────────────────────────────

#[test]
fn ensure_admin_token_generates_on_first_call_and_persists() {
    use community_search::db::settings;

    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("settings_test.sqlite");

    let (token, generated) = {
        let db = Database::open(&db_path).expect("should open database");
        settings::ensure_admin_token(&db, None).expect("should generate admin token")
    };

    assert!(generated, "first call should return generated=true");
    assert!(
        token.len() >= 32,
        "token should be at least 32 chars, got {}",
        token.len()
    );

    // Re-open the same database
    let db2 = Database::open(&db_path).expect("should re-open database");
    let (token2, generated2) =
        settings::ensure_admin_token(&db2, None).expect("should read persisted admin token");

    assert!(!generated2, "second call should return generated=false");
    assert_eq!(token, token2, "re-opened DB should return same token");
}

#[test]
fn explicit_admin_token_overrides_generated() {
    use community_search::db::settings;

    let (_tmp, db) = open_tmp_db();

    let (token, generated) = settings::ensure_admin_token(&db, Some("explicit-secret"))
        .expect("should set explicit admin token");

    assert_eq!(token, "explicit-secret");
    assert!(!generated, "explicit token should return generated=false");

    let (token2, generated2) =
        settings::ensure_admin_token(&db, None).expect("should read persisted admin token");

    assert_eq!(token2, "explicit-secret");
    assert!(!generated2, "subsequent call should return generated=false");
}

#[test]
fn explicit_admin_token_replaces_previously_persisted_value() {
    use community_search::db::settings;

    let (_tmp, db) = open_tmp_db();

    // First auto-generate a token
    let (_, generated) =
        settings::ensure_admin_token(&db, None).expect("should generate admin token");
    assert!(generated, "first call should return generated=true");

    // Override with explicit value
    let (token, generated) =
        settings::ensure_admin_token(&db, Some("override")).expect("should override admin token");

    assert_eq!(token, "override");
    assert!(
        !generated,
        "explicit override should return generated=false"
    );

    // Subsequent call without explicit should return the overridden value
    let (token2, generated2) =
        settings::ensure_admin_token(&db, None).expect("should read persisted admin token");
    assert_eq!(token2, "override");
    assert!(!generated2);
}
