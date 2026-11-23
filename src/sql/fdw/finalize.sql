-- Cleanup authority is the exact manifest returned by the coordinator.
CREATE OR REPLACE FUNCTION synchdb_finalize_initial_snapshot(p_resources jsonb)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE v_schema name;
BEGIN
    SELECT n.nspname INTO STRICT v_schema FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='synchdb';
    EXECUTE pg_catalog.format('SELECT %I.synchdb_bootstrap_cleanup($1)',v_schema) USING p_resources;
END;
$$;

CREATE OR REPLACE FUNCTION synchdb_finalize_initial_snapshot(
    p_source_schema name,p_stage_schema name,p_connector_name name,p_meta_schema name
) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'Name-only snapshot finalization is not supported'
        USING ERRCODE='0A000',HINT='The bootstrap coordinator cleans only objects in its exact OID resource manifest.';
END;
$$;
