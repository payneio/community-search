use tantivy::schema::{
    DateOptions, Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions,
    STORED, STRING,
};

pub struct DocFields {
    pub collection: Field,
    pub url: Field,
    pub host: Field,
    pub title: Field,
    pub body: Field,
    pub indexed_at: Field,
}

impl DocFields {
    pub fn from_schema(schema: &Schema) -> Self {
        Self {
            collection: schema
                .get_field("collection")
                .expect("field 'collection' not found"),
            url: schema.get_field("url").expect("field 'url' not found"),
            host: schema.get_field("host").expect("field 'host' not found"),
            title: schema.get_field("title").expect("field 'title' not found"),
            body: schema.get_field("body").expect("field 'body' not found"),
            indexed_at: schema
                .get_field("indexed_at")
                .expect("field 'indexed_at' not found"),
        }
    }
}

pub fn build() -> Schema {
    let mut builder: SchemaBuilder = Schema::builder();

    // collection — STRING | STORED (whole-string tokenization, for filtering)
    builder.add_text_field("collection", STRING | STORED);

    // url — STRING | STORED (exact string, no tokenization)
    builder.add_text_field("url", STRING | STORED);

    // host — STRING | STORED (whole-hostname, for `site:example.com` filtering)
    builder.add_text_field("host", STRING | STORED);

    // title — TEXT | STORED (standard tokenizer for full-text search)
    // Positions are required so quoted phrase queries (e.g. `"hello world"`) can match.
    builder.add_text_field(
        "title",
        TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored(),
    );

    // body — TEXT | STORED (standard tokenizer for full-text search)
    // Positions are required so quoted phrase queries (e.g. `"hello world"`) can match.
    builder.add_text_field(
        "body",
        TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored(),
    );

    // indexed_at — date/time, INDEXED | STORED | FAST (Unix epoch seconds; FAST for freshness-decay)
    let indexed_at_opts = DateOptions::default().set_indexed().set_stored().set_fast();
    builder.add_date_field("indexed_at", indexed_at_opts);

    builder.build()
}
