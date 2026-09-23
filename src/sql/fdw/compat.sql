-- Compatibility entry points for the original Oracle-specific names.
-- Both helpers serve all FDW connectors.  Preserve argument names and defaults
-- so existing positional, named, and default-argument calls keep working.

CREATE OR REPLACE FUNCTION synchdb_materialize_ora_metadata(
    p_source_schema name,
    p_dest_schema   name,
    p_on_exists     text DEFAULT 'replace'
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM synchdb_materialize_metadata(
        p_source_schema, p_dest_schema, p_on_exists
    );
END;
$$;

COMMENT ON FUNCTION synchdb_materialize_ora_metadata(name, name, text) IS
   'compatibility alias for synchdb_materialize_metadata';

CREATE OR REPLACE FUNCTION synchdb_create_ora_stage_fts(
    p_connector_name        name,
    p_desired_db            name,
    p_desired_schema        name,
    p_stage_schema          name DEFAULT 'ora_stage'::name,
    p_server_name           name DEFAULT 'oracle'::name,
    p_lower_names           boolean DEFAULT true,
    p_on_exists             text DEFAULT 'replace',
    p_offset                text DEFAULT NULL,
    p_source_schema         name DEFAULT 'ora_obj'::name,
    p_snapshot_tables       text DEFAULT NULL,
    p_write_dbz_schema_info boolean DEFAULT false,
    p_case_strategy         text DEFAULT 'asis'
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM synchdb_create_stage_fts(
        p_connector_name, p_desired_db, p_desired_schema, p_stage_schema,
        p_server_name, p_lower_names, p_on_exists, p_offset, p_source_schema,
        p_snapshot_tables, p_write_dbz_schema_info, p_case_strategy
    );
END;
$$;

COMMENT ON FUNCTION synchdb_create_ora_stage_fts(name, name, name, name, name, boolean, text, text, name, text, boolean, text) IS
   'compatibility alias for synchdb_create_stage_fts';
