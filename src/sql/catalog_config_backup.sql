-- Portable configuration is separate from database backup and CDC recovery.
-- This format carries caller-visible configuration and encrypted envelopes,
-- never grants, RLS policies, accepted execution roles, physical OIDs or offsets.
CREATE OR REPLACE FUNCTION synchdb_export_configuration()
RETURNS pg_catalog.jsonb
LANGUAGE plpgsql VOLATILE SECURITY INVOKER
SET search_path = pg_catalog
AS $synchdb_export_configuration$
DECLARE
    v_extension pg_catalog.oid;
    v_namespace pg_catalog.oid;
    v_schema pg_catalog.name;
    v_tables pg_catalog.text[] := ARRAY['synchdb_conninfo','synchdb_objmap'];
    v_relations pg_catalog.oid[] := ARRAY[]::pg_catalog.oid[];
    v_relation pg_catalog.oid;
    v_table pg_catalog.text;
    v_lock pg_catalog.text := 'LOCK TABLE ';
    v_limit pg_catalog.int4 := pg_catalog.current_setting('max_identifier_length')::pg_catalog.int4;
    v_valid pg_catalog.bool;
    v_result pg_catalog.jsonb;
    i pg_catalog.int4;
BEGIN
    SELECT e.oid, e.extnamespace, n.nspname
      INTO STRICT v_extension, v_namespace, v_schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'synchdb';
    FOREACH v_table IN ARRAY v_tables LOOP
        SELECT c.oid INTO v_relation
          FROM pg_catalog.pg_class c
         WHERE c.relnamespace = v_namespace AND c.relname::pg_catalog.text = v_table
           AND c.relkind = 'r' AND NOT c.relispartition
           AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h
                            WHERE h.inhrelid = c.oid OR h.inhparent = c.oid)
           AND EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                        WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                          AND d.objid = c.oid AND d.objsubid = 0
                          AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                          AND d.refobjid = v_extension AND d.deptype = 'e');
        IF v_relation IS NULL THEN
            RAISE EXCEPTION 'Configuration catalog is not an ordinary synchdb extension table'
                USING ERRCODE = '55000';
        END IF;
        v_relations := pg_catalog.array_append(v_relations, v_relation);
        IF pg_catalog.array_length(v_relations, 1) > 1 THEN v_lock := v_lock || ', '; END IF;
        v_lock := v_lock || pg_catalog.format('ONLY %I.%I', v_schema, v_table);
    END LOOP;
    -- The caller must have its own table privileges; neither this lock nor the
    -- data query changes the effective role or disables row security.
    EXECUTE v_lock || ' IN ACCESS SHARE MODE';
    FOR i IN 1..pg_catalog.array_length(v_tables, 1) LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_extension e
            JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
            JOIN pg_catalog.pg_class c ON c.relnamespace = e.extnamespace
            JOIN pg_catalog.pg_depend d
              ON d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
             AND d.objid = c.oid AND d.objsubid = 0
             AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
             AND d.refobjid = e.oid AND d.deptype = 'e'
           WHERE e.oid = v_extension AND e.extname = 'synchdb'
             AND e.extnamespace = v_namespace AND n.nspname = v_schema
             AND c.oid = v_relations[i] AND c.relname::pg_catalog.text = v_tables[i]
             AND c.relkind = 'r' AND NOT c.relispartition
             AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h
                              WHERE h.inhrelid = c.oid OR h.inhparent = c.oid)
        ) THEN
            RAISE EXCEPTION 'Configuration catalog changed while acquiring its lock'
                USING ERRCODE = '55000';
        END IF;
    END LOOP;
    -- Both materialized inputs and all their validation use one statement
    -- snapshot. RLS filters each input before it is aggregated or validated.
    EXECUTE pg_catalog.format($query$
        WITH connectors AS MATERIALIZED (
            SELECT name, connector_id, config_revision, data FROM %1$I.synchdb_conninfo
        ), mappings AS MATERIALIZED (
            SELECT name, objtype, enabled, srcobj, dstobj FROM %1$I.synchdb_objmap
        )
        SELECT pg_catalog.jsonb_build_object(
            'format', 'synchdb-configuration', 'version', 1,
            'connectors', (SELECT COALESCE(pg_catalog.jsonb_agg(
                pg_catalog.jsonb_build_object('name', c.name, 'connector_id', c.connector_id,
                    'config_revision', c.config_revision, 'data', c.data)
                ORDER BY c.name COLLATE pg_catalog."C"), '[]'::pg_catalog.jsonb) FROM connectors c),
            'mappings', (SELECT COALESCE(pg_catalog.jsonb_agg(
                pg_catalog.jsonb_build_object('name', m.name, 'objtype', m.objtype,
                    'enabled', m.enabled, 'srcobj', m.srcobj, 'dstobj', m.dstobj)
                ORDER BY m.name::pg_catalog.text COLLATE pg_catalog."C",
                    m.objtype::pg_catalog.text COLLATE pg_catalog."C", m.srcobj COLLATE pg_catalog."C"),
                '[]'::pg_catalog.jsonb) FROM mappings m)),
            (SELECT COALESCE(pg_catalog.bool_and((
                c.name <> '' AND pg_catalog.octet_length(c.name) <= $1
                AND c.connector_id <> '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid
                AND c.config_revision > 0 AND pg_catalog.jsonb_typeof(c.data) = 'object'
            ) IS TRUE), true) FROM connectors c)
            AND (SELECT COALESCE(pg_catalog.bool_and((
                m.name::pg_catalog.text <> '' AND pg_catalog.octet_length(m.name::pg_catalog.text) <= $1
                AND m.objtype::pg_catalog.text IN ('table','column','datatype','transform')
                AND m.srcobj <> '' AND pg_catalog.octet_length(m.srcobj) <= 2400
            ) IS TRUE), true) FROM mappings m)
    $query$, v_schema) INTO v_result, v_valid USING v_limit;
    IF v_valid IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'Caller-visible configuration contains a row outside the portable format'
            USING ERRCODE = '22023';
    END IF;
    RETURN v_result;
END;
$synchdb_export_configuration$;

CREATE OR REPLACE FUNCTION synchdb_import_configuration(p_configuration pg_catalog.jsonb)
RETURNS pg_catalog.int8
LANGUAGE plpgsql VOLATILE SECURITY INVOKER
SET search_path = pg_catalog
AS $synchdb_import_configuration$
DECLARE
    v_extension pg_catalog.oid;
    v_namespace pg_catalog.oid;
    v_schema pg_catalog.name;
    v_tables pg_catalog.text[] := ARRAY[
        'synchdb_conninfo','synchdb_objmap','synchdb_attribute',
        'synchdb_control_state','synchdb_control_commands','synchdb_artifacts',
        'synchdb_bootstrap_targets','synchdb_bootstrap_runs'];
    v_relations pg_catalog.oid[] := ARRAY[]::pg_catalog.oid[];
    v_relation pg_catalog.oid;
    v_table pg_catalog.text;
    v_lock pg_catalog.text := 'LOCK TABLE ';
    v_empty_query pg_catalog.text := 'SELECT ';
    v_nonempty pg_catalog.bool;
    v_row pg_catalog.jsonb;
    v_keys pg_catalog.int8;
    v_revision pg_catalog.text;
    v_limit pg_catalog.int4 := pg_catalog.current_setting('max_identifier_length')::pg_catalog.int4;
    v_expected pg_catalog.int8;
    v_inserted pg_catalog.int8;
    i pg_catalog.int4;
BEGIN
    -- Check the effective execution role, including SET ROLE, rather than the
    -- session's original authenticated role. Import grants no extra authority.
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                    WHERE rolname = CURRENT_USER AND rolsuper) THEN
        RAISE EXCEPTION 'Configuration import requires a superuser'
            USING ERRCODE = '42501';
    END IF;
    -- Under a transaction-wide old snapshot an empty check could miss rows
    -- committed before these locks. VOLATILE queries in READ COMMITTED obtain
    -- a fresh snapshot after all competing catalog writers have been excluded.
    IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'Configuration import requires READ COMMITTED isolation'
            USING ERRCODE = '0A000';
    END IF;
    IF p_configuration IS NULL OR pg_catalog.jsonb_typeof(p_configuration) <> 'object' THEN
        RAISE EXCEPTION 'Expected a portable configuration object' USING ERRCODE = '22023';
    END IF;
    SELECT pg_catalog.count(*) INTO v_keys FROM pg_catalog.jsonb_object_keys(p_configuration);
    IF v_keys <> 4 OR NOT (p_configuration ?& ARRAY['format','version','connectors','mappings'])
       OR pg_catalog.jsonb_typeof(p_configuration->'format') IS DISTINCT FROM 'string'
       OR p_configuration->>'format' IS DISTINCT FROM 'synchdb-configuration'
       OR pg_catalog.jsonb_typeof(p_configuration->'version') IS DISTINCT FROM 'number'
       OR p_configuration->>'version' IS DISTINCT FROM '1'
       OR pg_catalog.jsonb_typeof(p_configuration->'connectors') IS DISTINCT FROM 'array'
       OR pg_catalog.jsonb_typeof(p_configuration->'mappings') IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'Invalid portable configuration envelope or version'
            USING ERRCODE = '22023';
    END IF;
    SELECT e.oid, e.extnamespace, n.nspname
      INTO STRICT v_extension, v_namespace, v_schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'synchdb';
    FOREACH v_table IN ARRAY v_tables LOOP
        SELECT c.oid INTO v_relation
          FROM pg_catalog.pg_class c
         WHERE c.relnamespace = v_namespace AND c.relname::pg_catalog.text = v_table
           AND c.relkind = 'r' AND NOT c.relispartition
           AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h
                            WHERE h.inhrelid = c.oid OR h.inhparent = c.oid)
           AND EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                        WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                          AND d.objid = c.oid AND d.objsubid = 0
                          AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                          AND d.refobjid = v_extension AND d.deptype = 'e');
        IF v_relation IS NULL THEN
            RAISE EXCEPTION 'Configuration import requires ordinary synchdb extension catalogs'
                USING ERRCODE = '55000';
        END IF;
        v_relations := pg_catalog.array_append(v_relations, v_relation);
        IF pg_catalog.array_length(v_relations, 1) > 1 THEN
            v_lock := v_lock || ', ';
            v_empty_query := v_empty_query || ' OR ';
        END IF;
        v_lock := v_lock || pg_catalog.format('ONLY %I.%I', v_schema, v_table);
        v_empty_query := v_empty_query || pg_catalog.format('EXISTS (SELECT 1 FROM ONLY %I.%I)', v_schema, v_table);
    END LOOP;
    -- Do not wait while holding part of this set: controller reads and control
    -- submissions use different catalog lock orders. Retry a lock conflict in
    -- a new transaction; no configuration has been written at that point.
    EXECUTE v_lock || ' IN ACCESS EXCLUSIVE MODE NOWAIT';
    FOR i IN 1..pg_catalog.array_length(v_tables, 1) LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_extension e
            JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
            JOIN pg_catalog.pg_class c ON c.relnamespace = e.extnamespace
            JOIN pg_catalog.pg_depend d
              ON d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
             AND d.objid = c.oid AND d.objsubid = 0
             AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
             AND d.refobjid = e.oid AND d.deptype = 'e'
           WHERE e.oid = v_extension AND e.extname = 'synchdb'
             AND e.extnamespace = v_namespace AND n.nspname = v_schema
             AND c.oid = v_relations[i] AND c.relname::pg_catalog.text = v_tables[i]
             AND c.relkind = 'r' AND NOT c.relispartition
             AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h
                              WHERE h.inhrelid = c.oid OR h.inhparent = c.oid)
        ) THEN
            RAISE EXCEPTION 'Configuration catalog changed while acquiring its lock'
                USING ERRCODE = '55000';
        END IF;
    END LOOP;
    EXECUTE v_empty_query INTO v_nonempty;
    IF v_nonempty THEN
        RAISE EXCEPTION 'Configuration import requires all eight synchdb catalogs to be empty'
            USING ERRCODE = '55000';
    END IF;
    -- Validate before typed conversion: jsonb_to_recordset otherwise accepts
    -- string-to-number coercion and NAME silently truncates overlong input.
    -- jsonb has already normalized object keys; this is not a duplicate-key
    -- check of the original JSON text supplied by a client.
    FOR v_row IN SELECT value FROM pg_catalog.jsonb_array_elements(p_configuration->'connectors') LOOP
        IF pg_catalog.jsonb_typeof(v_row) IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'Invalid portable connector row' USING ERRCODE = '22023';
        END IF;
        SELECT pg_catalog.count(*) INTO v_keys FROM pg_catalog.jsonb_object_keys(v_row);
        IF v_keys <> 4 OR NOT (v_row ?& ARRAY['name','connector_id','config_revision','data'])
           OR pg_catalog.jsonb_typeof(v_row->'name') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_row->'connector_id') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_row->'config_revision') IS DISTINCT FROM 'number'
           OR pg_catalog.jsonb_typeof(v_row->'data') IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'Invalid portable connector fields' USING ERRCODE = '22023';
        END IF;
        v_revision := v_row->>'config_revision';
        IF v_row->>'name' = '' OR pg_catalog.octet_length(v_row->>'name') > v_limit
           OR (v_row->>'connector_id') COLLATE pg_catalog."C" !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           OR v_row->>'connector_id' = '00000000-0000-0000-0000-000000000000'
           OR v_revision COLLATE pg_catalog."C" !~ '^[1-9][0-9]*$'
           OR pg_catalog.length(v_revision) > 19
           OR (pg_catalog.length(v_revision) = 19 AND v_revision COLLATE pg_catalog."C" > '9223372036854775807' COLLATE pg_catalog."C") THEN
            RAISE EXCEPTION 'Portable connector identity or revision is outside its supported range'
                USING ERRCODE = '22023';
        END IF;
    END LOOP;
    FOR v_row IN SELECT value FROM pg_catalog.jsonb_array_elements(p_configuration->'mappings') LOOP
        IF pg_catalog.jsonb_typeof(v_row) IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'Invalid portable mapping row' USING ERRCODE = '22023';
        END IF;
        SELECT pg_catalog.count(*) INTO v_keys FROM pg_catalog.jsonb_object_keys(v_row);
        IF v_keys <> 5 OR NOT (v_row ?& ARRAY['name','objtype','enabled','srcobj','dstobj'])
           OR pg_catalog.jsonb_typeof(v_row->'name') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_row->'objtype') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_row->'enabled') NOT IN ('boolean','null')
           OR pg_catalog.jsonb_typeof(v_row->'srcobj') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_row->'dstobj') NOT IN ('string','null') THEN
            RAISE EXCEPTION 'Invalid portable mapping fields' USING ERRCODE = '22023';
        END IF;
        IF v_row->>'name' = '' OR pg_catalog.octet_length(v_row->>'name') > v_limit
           OR pg_catalog.octet_length(v_row->>'objtype') > v_limit
           OR v_row->>'objtype' NOT IN ('table','column','datatype','transform')
           OR v_row->>'srcobj' = '' OR pg_catalog.octet_length(v_row->>'srcobj') > 2400 THEN
            RAISE EXCEPTION 'Portable mapping identifier is outside its supported range'
                USING ERRCODE = '22023';
        END IF;
        -- No connector FK is invented here. An RLS-filtered export may contain
        -- mappings whose connector was invisible to that caller. enabled and
        -- dstobj retain the catalog's nullable-field semantics.
    END LOOP;
    v_expected := pg_catalog.jsonb_array_length(p_configuration->'connectors');
    EXECUTE pg_catalog.format($insert$
        INSERT INTO %I.synchdb_conninfo(name,isactive,data,connector_id,config_revision)
        SELECT c.name, false, c.data, c.connector_id, c.config_revision
          FROM pg_catalog.jsonb_to_recordset($1) AS c(
              name pg_catalog.text, connector_id pg_catalog.uuid,
              config_revision pg_catalog.int8, data pg_catalog.jsonb)
    $insert$, v_schema) USING p_configuration->'connectors';
    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    IF v_inserted <> v_expected THEN
        RAISE EXCEPTION 'Configuration import did not insert every connector row' USING ERRCODE = '55000';
    END IF;
    EXECUTE pg_catalog.format($insert$
        INSERT INTO %I.synchdb_objmap(name,objtype,enabled,srcobj,dstobj)
        SELECT m.name, m.objtype, m.enabled, m.srcobj, m.dstobj
          FROM pg_catalog.jsonb_to_recordset($1) AS m(
              name pg_catalog.name, objtype pg_catalog.name, enabled pg_catalog.bool,
              srcobj pg_catalog.text, dstobj pg_catalog.text)
    $insert$, v_schema) USING p_configuration->'mappings';
    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    IF v_inserted <> pg_catalog.jsonb_array_length(p_configuration->'mappings') THEN
        RAISE EXCEPTION 'Configuration import did not insert every mapping row' USING ERRCODE = '55000';
    END IF;
    RETURN v_expected;
END;
$synchdb_import_configuration$;

REVOKE ALL ON FUNCTION synchdb_export_configuration(), synchdb_import_configuration(pg_catalog.jsonb)
    FROM PUBLIC;
