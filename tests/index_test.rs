use community_search::index;
use community_search::index::schema::DocFields;
use tantivy::doc;
use tempfile::TempDir;

#[test]
fn opens_new_index_creating_directory() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("tantivy");

    // Directory should not exist yet
    assert!(
        !index_path.exists(),
        "tantivy dir should not exist before open_or_create"
    );

    let _index = index::open_or_create(&index_path).expect("should create new index");

    // Directory and meta.json should now exist
    assert!(
        index_path.exists(),
        "tantivy dir should exist after open_or_create"
    );
    assert!(
        index_path.join("meta.json").exists(),
        "meta.json should exist after open_or_create"
    );
}

#[test]
fn reopens_existing_index_without_clobbering() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("tantivy");

    // Create index and write one document
    {
        let index = index::open_or_create(&index_path).expect("should create index");
        let schema = index.schema();
        let fields = DocFields::from_schema(&schema);

        let mut writer = index.writer(50_000_000).expect("should create writer");
        writer
            .add_document(doc!(
                fields.collection => "recipes",
                fields.url => "https://example.com/a",
                fields.title => "A title",
                fields.body => "Some body content"
            ))
            .expect("should add document");
        writer.commit().expect("should commit");
    }

    // Reopen the index — must NOT clobber existing data
    {
        let index = index::open_or_create(&index_path).expect("should reopen index");
        let reader = index.reader().expect("should get reader");
        let searcher = reader.searcher();
        assert_eq!(
            searcher.num_docs(),
            1,
            "should have exactly 1 document after reopen"
        );
    }
}

#[test]
fn schema_has_required_fields() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("tantivy");

    let index = index::open_or_create(&index_path).expect("should create index");
    let schema = index.schema();

    for field_name in &["collection", "url", "title", "body", "indexed_at"] {
        assert!(
            schema.get_field(field_name).is_ok(),
            "schema should have field '{}'",
            field_name
        );
    }
}
