use community_search::db::open_in_memory;

#[test]
fn discovered_engines_table_exists_with_expected_columns() {
    let db = open_in_memory().expect("open in-memory db");
    let conn = db.connection();
    let cols: Vec<(String, String)> = conn
        .prepare("PRAGMA table_info(discovered_engines)")
        .unwrap()
        .query_map([], |row: &rusqlite::Row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert!(!cols.is_empty(), "discovered_engines table should exist");
    let names: Vec<&str> = cols
        .iter()
        .map(|(n, _t): &(String, String)| n.as_str())
        .collect();
    for expected in ["url", "name", "description", "first_seen", "last_seen"] {
        assert!(
            names.contains(&expected),
            "missing column {expected}; have {names:?}"
        );
    }
}

#[test]
fn discovered_engines_url_is_primary_key_unique() {
    let db = open_in_memory().expect("open in-memory db");
    let conn = db.connection();
    conn.execute(
        "INSERT INTO discovered_engines (url, name, description, first_seen, last_seen) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params!["https://a.example.com", "A", "desc", 100i64, 100i64],
    )
    .unwrap();
    let err = conn
        .execute(
            "INSERT INTO discovered_engines (url, name, description, first_seen, last_seen) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["https://a.example.com", "A2", "desc2", 200i64, 200i64],
        )
        .unwrap_err();
    assert!(
        format!("{err}").to_lowercase().contains("unique")
            || format!("{err}").to_lowercase().contains("constraint"),
        "expected unique/constraint error, got: {err}"
    );
}
