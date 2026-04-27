CREATE TABLE IF NOT EXISTS schema_catalog_tables (
    table_name VARCHAR(255) PRIMARY KEY,
    layer VARCHAR(50) NOT NULL,
    object_type VARCHAR(20) NOT NULL,
    domain VARCHAR(100) NOT NULL DEFAULT 'general',
    description TEXT NOT NULL DEFAULT '',
    grain TEXT,
    preferred_for_llm BOOLEAN NOT NULL DEFAULT FALSE,
    is_sensitive BOOLEAN NOT NULL DEFAULT FALSE,
    source_of_truth VARCHAR(50) NOT NULL DEFAULT 'seed',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS schema_catalog_columns (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL REFERENCES schema_catalog_tables(table_name) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    data_type VARCHAR(100) NOT NULL,
    ordinal_position INTEGER NOT NULL DEFAULT 0,
    description TEXT NOT NULL DEFAULT '',
    semantic_role VARCHAR(100),
    is_primary_key BOOLEAN NOT NULL DEFAULT FALSE,
    is_filter_key BOOLEAN NOT NULL DEFAULT FALSE,
    is_time_key BOOLEAN NOT NULL DEFAULT FALSE,
    is_measure BOOLEAN NOT NULL DEFAULT FALSE,
    is_sensitive BOOLEAN NOT NULL DEFAULT FALSE,
    example_values_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    UNIQUE(table_name, column_name)
);

CREATE TABLE IF NOT EXISTS schema_catalog_relationships (
    id BIGSERIAL PRIMARY KEY,
    from_table VARCHAR(255) NOT NULL REFERENCES schema_catalog_tables(table_name) ON DELETE CASCADE,
    to_table VARCHAR(255) NOT NULL REFERENCES schema_catalog_tables(table_name) ON DELETE CASCADE,
    relationship_type VARCHAR(50) NOT NULL,
    physical_fk BOOLEAN NOT NULL DEFAULT FALSE,
    join_expression TEXT NOT NULL,
    confidence VARCHAR(50) NOT NULL DEFAULT 'logical',
    description TEXT NOT NULL DEFAULT '',
    from_columns_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    to_columns_json JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS schema_catalog_examples (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL REFERENCES schema_catalog_tables(table_name) ON DELETE CASCADE,
    use_case VARCHAR(100) NOT NULL,
    question TEXT NOT NULL,
    sql_template TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_schema_catalog_tables_layer
    ON schema_catalog_tables(layer, preferred_for_llm DESC, table_name);

CREATE INDEX IF NOT EXISTS idx_schema_catalog_columns_table
    ON schema_catalog_columns(table_name, ordinal_position);

CREATE INDEX IF NOT EXISTS idx_schema_catalog_relationships_from
    ON schema_catalog_relationships(from_table, to_table);

CREATE INDEX IF NOT EXISTS idx_schema_catalog_relationships_to
    ON schema_catalog_relationships(to_table, from_table);

CREATE INDEX IF NOT EXISTS idx_schema_catalog_examples_table
    ON schema_catalog_examples(table_name, use_case);
