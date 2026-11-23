CREATE OR REPLACE FUNCTION synchdb_do_schema_sync(
    p_connector_name  name,               -- e.g. 'oracleconn'
    p_source_schema   name,               -- e.g. 'ora_obj'   (FDW objects + current_scn FT)
    p_stage_schema    name,               -- e.g. 'ora_stage' (staging foreign tables)
    p_dest_schema     name,               -- e.g. 'dst_stage' (materialized tables)
    p_lookup_db       text,               -- e.g. 'free'
    p_lookup_schema   name,               -- e.g. 'dbzuser'
    p_lower_names     boolean DEFAULT true,
    p_on_exists       text    DEFAULT 'error',    -- only 'error' until target plans support adoption/replacement
    p_offset          text    DEFAULT null,          -- >0 to force a specific SCN; else auto-read
    p_snapshot_tables text    DEFAULT null,
    p_use_subtx         boolean DEFAULT true,
    p_write_schema_hist boolean DEFAULT false,
    p_case_strategy  text DEFAULT 'asis'
)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    v_schema name;
    v_request jsonb;
    v_selection jsonb := NULL;
    v_tables jsonb := '[]'::jsonb;
    v_parts text[];
    v_item text;
    v_result jsonb;
BEGIN
    IF p_on_exists IS NULL OR p_use_subtx IS NULL THEN
        RAISE EXCEPTION 'Bootstrap target and failure policies must be explicit' USING ERRCODE='22023';
    END IF;
    IF p_snapshot_tables IS NOT NULL THEN
        IF btrim(p_snapshot_tables)='' THEN
            RAISE EXCEPTION 'Snapshot selection is explicitly empty' USING ERRCODE='22023';
        END IF;
        FOREACH v_item IN ARRAY regexp_split_to_array(p_snapshot_tables,'\s*,\s*') LOOP
            v_parts:=parse_ident(v_item,true);
            IF cardinality(v_parts)=3 THEN
                v_tables:=v_tables||jsonb_build_array(jsonb_build_object('db',v_parts[1],'schema',v_parts[2],'table',v_parts[3]));
            ELSIF cardinality(v_parts)=2 THEN
                v_tables:=v_tables||jsonb_build_array(jsonb_build_object('db',v_parts[1],'schema',p_lookup_schema::text,'table',v_parts[2]));
            ELSE
                RAISE EXCEPTION 'Invalid explicit snapshot table identity: %',v_item USING ERRCODE='22023';
            END IF;
        END LOOP;
        v_selection:=jsonb_build_object('kind','tables','tables',v_tables);
    END IF;
    v_request:=jsonb_build_object('version',1,'connector_name',p_connector_name::text,
        'mode','schemasync','dest_schema',p_dest_schema::text,
        'case_strategy',coalesce(nullif(lower(p_case_strategy),''),CASE WHEN p_lower_names THEN 'lower' ELSE 'asis' END),
        'target_policy',p_on_exists,'failure_policy',CASE WHEN p_use_subtx THEN 'continue_tables' ELSE 'fail_fast' END,
        'offset',p_offset,'write_schema_history',p_write_schema_hist,'selection',v_selection);
    SELECT n.nspname INTO STRICT v_schema FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='synchdb';
    EXECUTE pg_catalog.format('SELECT %I.synchdb_bootstrap($1)',v_schema)
      INTO STRICT v_result USING v_request;
    RETURN v_result::text;
END;
$$;
