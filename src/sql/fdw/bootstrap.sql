CREATE OR REPLACE FUNCTION synchdb_bootstrap_normalize_name(text,text) RETURNS text
AS 'MODULE_PATHNAME','synchdb_bootstrap_normalize_name' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION synchdb_bootstrap_type(text,text,jsonb,jsonb) RETURNS text
AS 'MODULE_PATHNAME','synchdb_bootstrap_type' LANGUAGE C VOLATILE PARALLEL UNSAFE;
CREATE OR REPLACE FUNCTION synchdb_bootstrap_value(text,text) RETURNS record
AS 'MODULE_PATHNAME','synchdb_bootstrap_value' LANGUAGE C VOLATILE PARALLEL UNSAFE;
CREATE OR REPLACE FUNCTION synchdb_bootstrap_source_id(text,text,text,text) RETURNS text
AS 'MODULE_PATHNAME','synchdb_bootstrap_source_id' LANGUAGE C IMMUTABLE PARALLEL SAFE;
CREATE OR REPLACE FUNCTION synchdb_bootstrap_column_id(text,text) RETURNS text
AS 'MODULE_PATHNAME','synchdb_bootstrap_column_id' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION synchdb_bootstrap_validate_transform(text) RETURNS void
AS 'MODULE_PATHNAME','synchdb_bootstrap_validate_transform' LANGUAGE C VOLATILE PARALLEL UNSAFE;
CREATE OR REPLACE FUNCTION synchdb_source_consistency_contract(text,text,text) RETURNS jsonb
AS 'MODULE_PATHNAME','synchdb_source_consistency_contract' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- A target bootstrap is not a CDC handoff.  The result intentionally contains
-- no runnable offset/history and does not claim a synchronized remote session.
CREATE OR REPLACE FUNCTION synchdb_bootstrap_relation_descriptor(p_relation oid)
RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog
SET quote_all_identifiers = on AS $sql$
    SELECT jsonb_build_object(
        'kind', c.relkind, 'persistence', c.relpersistence,
        'access_method', c.relam, 'tablespace', c.reltablespace,
        'options', to_jsonb(c.reloptions), 'replica_identity', c.relreplident,
        'columns', coalesce((SELECT jsonb_agg(jsonb_build_object(
            'number', a.attnum, 'name', a.attname, 'type', a.atttypid,
            'typmod', a.atttypmod, 'collation', a.attcollation,
            'not_null', a.attnotnull, 'identity', a.attidentity,
            'generated', a.attgenerated, 'dropped', a.attisdropped,
            'default', pg_get_expr(d.adbin, d.adrelid)) ORDER BY a.attnum)
            FROM pg_attribute a LEFT JOIN pg_attrdef d
              ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE a.attrelid = c.oid AND a.attnum > 0), '[]'::jsonb),
        'constraints', coalesce((SELECT jsonb_agg(jsonb_build_object(
            'name', k.conname, 'definition', pg_get_constraintdef(k.oid, false),
            'validated', k.convalidated) ORDER BY k.conname)
            FROM pg_constraint k WHERE k.conrelid = c.oid), '[]'::jsonb),
        'indexes', coalesce((SELECT jsonb_agg(jsonb_build_object(
            'definition', pg_get_indexdef(i.indexrelid), 'valid', i.indisvalid,
            'ready', i.indisready, 'live', i.indislive) ORDER BY i.indexrelid)
            FROM pg_index i WHERE i.indrelid = c.oid), '[]'::jsonb))
      FROM pg_class c WHERE c.oid = p_relation
$sql$;

CREATE OR REPLACE FUNCTION synchdb_bootstrap_check_replaceable(p_relation oid)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $fn$
DECLARE
    r record;
    v_dependency text;
BEGIN
    SELECT * INTO STRICT r FROM pg_class WHERE oid = p_relation;
    IF r.relkind <> 'r' OR r.relam <> (SELECT oid FROM pg_am WHERE amname='heap' AND amtype='t')
       OR r.relispartition OR r.relrowsecurity OR r.relforcerowsecurity
       OR r.relacl IS NOT NULL OR r.relowner <> (current_user::regrole)::oid
       OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = r.oid OR inhparent = r.oid)
       OR EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = r.oid AND NOT tgisinternal)
       OR EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = r.oid)
       OR EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = r.oid AND attacl IS NOT NULL)
       OR EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = r.oid
                   AND contype NOT IN ('p', 'n')) THEN
        RAISE EXCEPTION 'Target % cannot be safely replaced', p_relation::regclass
            USING ERRCODE = '0A000',
                  DETAIL = 'Replacement supports an owned ordinary table with default ACLs and no custom triggers, policies, inheritance, or additional constraints.';
    END IF;
    SELECT pg_describe_object(d.classid, d.objid, d.objsubid)
      INTO v_dependency FROM pg_depend d
     WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = r.oid
       AND NOT (d.classid = 'pg_class'::regclass AND
                (d.objid = r.reltoastrelid OR EXISTS
                 (SELECT 1 FROM pg_index i WHERE i.indexrelid = d.objid AND i.indrelid = r.oid)))
       AND NOT (d.classid = 'pg_type'::regclass AND d.objid = r.reltype)
       AND NOT (d.classid = 'pg_constraint'::regclass AND EXISTS
                (SELECT 1 FROM pg_constraint k WHERE k.oid = d.objid AND k.conrelid = r.oid))
       AND NOT (d.classid = 'pg_attrdef'::regclass AND EXISTS
                (SELECT 1 FROM pg_attrdef a WHERE a.oid = d.objid AND a.adrelid = r.oid))
     LIMIT 1;
    IF v_dependency IS NULL THEN
        SELECT pg_describe_object(d.classid, d.objid, d.objsubid)
          INTO v_dependency FROM pg_depend d
         WHERE d.refclassid = 'pg_type'::regclass AND d.refobjid = r.reltype
           AND NOT (d.classid = 'pg_type'::regclass AND EXISTS
                    (SELECT 1 FROM pg_type t WHERE t.oid = d.objid AND t.typelem = r.reltype))
         LIMIT 1;
    END IF;
    IF v_dependency IS NOT NULL THEN
        RAISE EXCEPTION 'Target % has an unsupported dependency', p_relation::regclass
            USING ERRCODE = '2BP01', DETAIL = v_dependency;
    END IF;
END;
$fn$;

-- Names are merely diagnostics; OID, owner, namespace and kind are the cleanup
-- authority.  All listed objects are created and removed in this transaction.
CREATE OR REPLACE FUNCTION synchdb_bootstrap_resources(p_schemas name[], p_server oid)
RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog AS $sql$
    SELECT coalesce(jsonb_agg(value ORDER BY ordering, object_oid), '[]'::jsonb)
      FROM (
        SELECT 1 AS ordering, n.oid AS object_oid, jsonb_build_object(
            'kind', 'schema', 'oid', n.oid, 'owner', n.nspowner, 'name', n.nspname) AS value
          FROM pg_namespace n WHERE n.nspname = ANY(p_schemas)
        UNION ALL
        SELECT 2, c.oid, jsonb_build_object('kind', 'relation', 'oid', c.oid,
            'owner', c.relowner, 'namespace_oid', c.relnamespace,
            'schema', n.nspname, 'name', c.relname, 'relkind', c.relkind)
          FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = ANY(p_schemas)
        UNION ALL
        SELECT 3, s.oid, jsonb_build_object('kind', 'server', 'oid', s.oid,
            'owner', s.srvowner, 'name', s.srvname)
          FROM pg_foreign_server s WHERE s.oid = p_server
        UNION ALL
        -- The underlying catalog protects credentials from ordinary roles.
        -- Its public view exposes the same identity OIDs without requiring
        -- access to mapping options or granting catalog-table privileges.
        SELECT 4, u.umid, jsonb_build_object('kind', 'user_mapping', 'oid', u.umid,
            'server_oid', u.srvid, 'user_oid', u.umuser)
          FROM pg_user_mappings u WHERE u.srvid = p_server
      ) objects
$sql$;

CREATE OR REPLACE FUNCTION synchdb_bootstrap_cleanup(p_resources jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $fn$
DECLARE
    r jsonb;
    c record;
    v_server name;
BEGIN
    IF jsonb_typeof(p_resources) <> 'array' THEN
        RAISE EXCEPTION 'Invalid bootstrap resource manifest' USING ERRCODE = '22023';
    END IF;
    -- Check the whole manifest before deleting any object.  Indexes/row types
    -- owned by a table disappear with that exact table, never by CASCADE.
    FOR r IN SELECT value FROM jsonb_array_elements(p_resources) LOOP
        IF r->>'kind' = 'schema' THEN
            IF NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.oid = (r->>'oid')::oid
                            AND n.nspname = r->>'name' AND n.nspowner = (r->>'owner')::oid) THEN
                RAISE EXCEPTION 'Bootstrap schema resource changed' USING ERRCODE = '55000';
            END IF;
            IF EXISTS (SELECT 1 FROM pg_class x WHERE x.relnamespace = (r->>'oid')::oid
                        AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_resources) saved
                                         WHERE saved->>'kind' = 'relation' AND (saved->>'oid')::oid = x.oid)) THEN
                RAISE EXCEPTION 'Bootstrap workspace contains an unowned relation' USING ERRCODE = '55000';
            END IF;
        ELSIF r->>'kind' = 'relation' THEN
            IF NOT EXISTS (SELECT 1 FROM pg_class x WHERE x.oid = (r->>'oid')::oid
                            AND x.relnamespace = (r->>'namespace_oid')::oid
                            AND x.relname = r->>'name' AND x.relkind::text = r->>'relkind'
                            AND x.relowner = (r->>'owner')::oid) THEN
                RAISE EXCEPTION 'Bootstrap relation resource changed' USING ERRCODE = '55000';
            END IF;
        ELSIF r->>'kind' = 'server' THEN
            IF NOT EXISTS (SELECT 1 FROM pg_foreign_server s WHERE s.oid = (r->>'oid')::oid
                            AND s.srvname = r->>'name' AND s.srvowner = (r->>'owner')::oid) THEN
                RAISE EXCEPTION 'Bootstrap server resource changed' USING ERRCODE = '55000';
            END IF;
        ELSIF r->>'kind' = 'user_mapping' THEN
            IF NOT EXISTS (SELECT 1 FROM pg_user_mappings u WHERE u.umid = (r->>'oid')::oid
                            AND u.srvid = (r->>'server_oid')::oid AND u.umuser = (r->>'user_oid')::oid) THEN
                RAISE EXCEPTION 'Bootstrap user-mapping resource changed' USING ERRCODE = '55000';
            END IF;
        ELSE
            RAISE EXCEPTION 'Unknown bootstrap resource kind' USING ERRCODE = '22023';
        END IF;
    END LOOP;
    FOR r IN SELECT value FROM jsonb_array_elements(p_resources)
              WHERE value->>'kind' = 'relation'
              ORDER BY CASE value->>'relkind' WHEN 'v' THEN 0 WHEN 'm' THEN 1 ELSE 2 END,
                       (value->>'oid')::oid DESC LOOP
        IF r->>'relkind' IN ('i', 'I', 'S', 't') THEN CONTINUE; END IF;
        IF r->>'relkind' NOT IN ('v', 'm', 'r', 'f') THEN
            RAISE EXCEPTION 'Unsupported bootstrap resource relation kind' USING ERRCODE = '0A000';
        END IF;
        EXECUTE format('DROP %s %I.%I', CASE r->>'relkind'
            WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW'
            WHEN 'f' THEN 'FOREIGN TABLE' ELSE 'TABLE' END, r->>'schema', r->>'name');
    END LOOP;
    FOR r IN SELECT value FROM jsonb_array_elements(p_resources) WHERE value->>'kind' = 'schema' LOOP
        EXECUTE format('DROP SCHEMA %I', r->>'name');
    END LOOP;
    FOR r IN SELECT value FROM jsonb_array_elements(p_resources) WHERE value->>'kind' = 'user_mapping' LOOP
        SELECT srvname INTO STRICT v_server FROM pg_foreign_server WHERE oid = (r->>'server_oid')::oid;
        EXECUTE format('DROP USER MAPPING FOR %s SERVER %I',
            CASE WHEN (r->>'user_oid')::oid = 0 THEN 'PUBLIC'
                 ELSE quote_ident(pg_get_userbyid((r->>'user_oid')::oid)) END, v_server);
    END LOOP;
    FOR r IN SELECT value FROM jsonb_array_elements(p_resources) WHERE value->>'kind' = 'server' LOOP
        EXECUTE format('DROP SERVER %I', r->>'name');
    END LOOP;
END;
$fn$;

CREATE OR REPLACE FUNCTION synchdb_bootstrap(p_request jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE
    v_extension oid;
    v_ext name;
    v_name text;
    v_connector_id uuid;
    v_revision bigint;
    v_config jsonb;
    v_connector text;
    v_mode text;
    v_failure text;
    v_policy text;
    v_case text;
    v_copy boolean;
    v_generation uuid;
    v_prefix text;
    v_source name;
    v_stage name;
    v_meta name;
    v_load name;
    v_dest text;
    v_db text;
    v_owner text;
    v_server name;
    v_server_oid oid;
    v_selection jsonb;
    v_requested jsonb := '[]'::jsonb;
    v_selected jsonb := '[]'::jsonb;
    v_inventory jsonb;
    v_mappings jsonb;
    v_plan jsonb;
    v_table jsonb;
    v_tables jsonb := '[]'::jsonb;
    v_result jsonb;
    v_results jsonb := '[]'::jsonb;
    v_resources jsonb;
    v_artifact_intents jsonb;
    v_candidate jsonb := NULL;
    v_boundary text := 'UNSYNCHRONIZED_FDW_READ';
    v_read_contract jsonb;
    v_scn numeric;
    v_source_id jsonb;
    v_source_key text;
    v_column jsonb;
    v_columns jsonb;
    v_pk jsonb;
    v_rule jsonb;
    v_parts text[];
    v_dst_parts text[];
    v_src_parts text[];
    v_old_oid oid;
    v_new_oid oid;
    v_old_descriptor jsonb;
    v_descriptor jsonb;
    v_old_ledger jsonb;
    v_target_schema text;
    v_target_name text;
    v_local_name text;
    v_source_name text;
    v_target_column text;
    v_type_sql text;
    v_type_oid oid;
    v_transform text;
    v_ddl text;
    v_dst_list text;
    v_src_list text;
    v_laterals text;
    v_pk_list text;
    v_rows bigint;
    v_total_rows bigint := 0;
    v_success integer := 0;
    v_failed integer := 0;
    v_stage_name text;
    v_state text;
    v_message text;
    v_detail text;
    v_count integer;
    r record;
BEGIN
    IF p_request IS NULL OR jsonb_typeof(p_request) <> 'object'
       OR p_request->'version' IS DISTINCT FROM '1'::jsonb THEN
        RAISE EXCEPTION 'Expected bootstrap request version 1' USING ERRCODE = '22023';
    END IF;
    IF p_request ?| ARRAY['cdc_eligible','cdc_resume_material','source_read_boundary','consistency_proof'] THEN
        RAISE EXCEPTION 'Bootstrap callers cannot supply source consistency or CDC handoff proof'
            USING ERRCODE = '22023';
    END IF;
    v_mode := p_request->>'mode';
    IF v_mode IN ('initial', 'no_data', 'always') THEN
        RAISE EXCEPTION 'FDW bootstrap cannot establish a verified CDC handoff for mode %', v_mode
            USING ERRCODE = '0A000', HINT = 'Use initial_only or schemasync; source-consistent CDC handoff requires a provider-specific protocol.';
    ELSIF v_mode IS NULL OR v_mode NOT IN ('initial_only', 'schemasync') THEN
        RAISE EXCEPTION 'Unsupported FDW bootstrap mode: %', v_mode USING ERRCODE = '22023';
    END IF;
    v_copy := v_mode = 'initial_only';
    v_failure := coalesce(p_request->>'failure_policy', 'fail_fast');
    v_policy := coalesce(p_request->>'target_policy', 'error');
    v_case := coalesce(p_request->>'case_strategy', 'asis');
    IF v_failure NOT IN ('fail_fast', 'continue_tables') OR v_case NOT IN ('lower', 'upper', 'asis') THEN
        RAISE EXCEPTION 'Invalid bootstrap failure policy or case strategy' USING ERRCODE = '22023';
    END IF;
    IF v_policy IN ('validate_existing', 'skip', 'drop') THEN
        RAISE EXCEPTION 'Bootstrap target policy % is unsupported', v_policy USING ERRCODE = '0A000';
    ELSIF v_policy NOT IN ('error', 'replace') THEN
        RAISE EXCEPTION 'Invalid bootstrap target policy %', v_policy USING ERRCODE = '22023';
    END IF;
    IF (p_request ? 'offset' AND p_request->'offset' <> 'null'::jsonb)
       OR coalesce((p_request->>'write_schema_history')::boolean, false) THEN
        RAISE EXCEPTION 'Bootstrap offset overrides and schema-history publication require a verified artifact handoff'
            USING ERRCODE = '0A000';
    END IF;
    v_name := p_request->>'connector_name';
    IF v_name IS NULL OR btrim(v_name) = '' OR octet_length(v_name)>current_setting('max_identifier_length')::int THEN
        RAISE EXCEPTION 'Bootstrap connector name must be nonempty' USING ERRCODE = '22023';
    END IF;
    SELECT e.oid, n.nspname INTO STRICT v_extension, v_ext
      FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'synchdb';
    PERFORM set_config('search_path', format('pg_catalog,%I,pg_temp', v_ext), true);
    -- The connector row is frozen for this local transaction; expressions and
    -- mapping rows are copied below and never rediscovered during execution.
    SELECT connector_id, config_revision, data INTO STRICT v_connector_id, v_revision, v_config
      FROM synchdb_conninfo WHERE name COLLATE "C" = v_name COLLATE "C" FOR SHARE;
    IF p_request ? 'connector_id' AND (p_request->>'connector_id')::uuid IS DISTINCT FROM v_connector_id THEN
        RAISE EXCEPTION 'Bootstrap connector incarnation changed' USING ERRCODE = '55000';
    END IF;
    IF p_request ? 'config_revision' AND (p_request->>'config_revision')::bigint IS DISTINCT FROM v_revision THEN
        RAISE EXCEPTION 'Bootstrap connector configuration changed' USING ERRCODE = '55000';
    END IF;
    v_connector := lower(v_config->>'connector');
    IF v_connector IS NULL OR v_connector NOT IN ('oracle', 'olr', 'postgres', 'mysql', 'sqlserver') THEN
        RAISE EXCEPTION 'Unsupported FDW connector %', v_connector USING ERRCODE = '0A000';
    END IF;
    v_read_contract := synchdb_source_consistency_contract(v_connector,'fdw',v_mode);
    v_db := v_config->>'srcdb';
    IF v_connector IN ('oracle', 'olr') AND strpos(v_db, '/') > 0 THEN v_db := split_part(v_db, '/', 2); END IF;
    v_owner := nullif(nullif(v_config->>'srcschema', ''), 'null');
    IF v_owner IS NULL AND v_connector = 'mysql' THEN v_owner := v_db; END IF;
    IF v_owner IS NULL OR v_db IS NULL OR v_db = '' THEN
        RAISE EXCEPTION 'FDW bootstrap requires one explicit source database and schema'
            USING ERRCODE = '22023';
    END IF;
    v_dest := coalesce(p_request->>'dest_schema', v_db);
    IF v_dest = '' OR octet_length(v_dest) > current_setting('max_identifier_length')::int THEN
        RAISE EXCEPTION 'Invalid bootstrap destination schema' USING ERRCODE = '22023';
    END IF;
    v_generation := coalesce((p_request->>'generation')::uuid, gen_random_uuid());
    IF EXISTS (SELECT 1 FROM synchdb_bootstrap_runs WHERE generation=v_generation) THEN
        RAISE EXCEPTION 'Bootstrap generation % already has a committed result',v_generation USING ERRCODE='23505';
    END IF;
    v_prefix := 'sdb_b_' || replace(v_generation::text, '-', '');
    v_source := (v_prefix || '_src')::name;
    v_stage := (v_prefix || '_ft')::name;
    v_meta := (v_prefix || '_meta')::name;
    v_load := (v_prefix || '_load')::name;
    v_server := (v_prefix || '_srv')::name;
    IF v_dest = ANY(ARRAY[v_source::text, v_stage::text, v_meta::text, v_load::text]) THEN
        RAISE EXCEPTION 'Bootstrap destination aliases a private workspace' USING ERRCODE = '22023';
    END IF;
    -- Read the mapping catalog once.  No helper executes live mapping rows.
    SELECT coalesce(jsonb_agg(jsonb_build_object('kind', objtype, 'source', srcobj, 'destination', dstobj)
                              ORDER BY objtype, srcobj), '[]'::jsonb)
      INTO v_mappings FROM synchdb_objmap WHERE name::text COLLATE "C" = v_name COLLATE "C" AND enabled;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_mappings) m
               WHERE m->>'kind' NOT IN ('table','column','datatype','transform')
                  OR m->>'source' IS NULL OR btrim(m->>'source')=''
                  OR m->>'destination' IS NULL OR btrim(m->>'destination')='') THEN
        RAISE EXCEPTION 'Invalid enabled bootstrap mapping rule' USING ERRCODE='22023';
    END IF;
    EXECUTE format('CREATE SCHEMA %I', v_source);
    EXECUTE format('CREATE SCHEMA %I', v_stage);
    EXECUTE format('CREATE SCHEMA %I', v_meta);
    EXECUTE format('CREATE SCHEMA %I', v_load);
    v_server := synchdb_prepare_initial_snapshot(v_name, v_server);
    SELECT oid INTO STRICT v_server_oid FROM pg_foreign_server WHERE srvname = v_server;
    CASE v_connector
      WHEN 'oracle', 'olr' THEN PERFORM synchdb_create_oraviews(v_server, v_source, '{"case":"original"}'::jsonb);
      WHEN 'postgres' THEN PERFORM synchdb_create_pg_objs(v_server, v_source, '{"case":"original"}'::jsonb);
      WHEN 'mysql' THEN PERFORM synchdb_create_mysql_objs(v_server, v_source, '{"case":"original"}'::jsonb);
      WHEN 'sqlserver' THEN PERFORM synchdb_create_sqlserver_objs(v_server, v_source, '{"case":"original"}'::jsonb);
    END CASE;
    PERFORM synchdb_materialize_metadata(v_source, v_meta, 'error');
    EXECUTE format('SELECT coalesce(jsonb_agg(jsonb_build_object(''db'',$1,''schema'',"schema",''table'',table_name)
                       ORDER BY "schema",table_name),''[]''::jsonb)
                    FROM (SELECT DISTINCT "schema",table_name FROM %I.columns WHERE "schema"=$2) inventory', v_meta)
      INTO v_inventory USING v_db, v_owner;
    v_selection := p_request->'selection';
    IF v_selection IS NULL OR v_selection = 'null'::jsonb THEN
        v_selection := coalesce(nullif(nullif(v_config->'snapshottable', 'null'::jsonb), '"null"'::jsonb),
                                nullif(nullif(v_config->'table', 'null'::jsonb), '"null"'::jsonb));
    END IF;
    IF v_selection IS NULL OR v_selection = 'null'::jsonb
       OR (jsonb_typeof(v_selection) = 'object' AND v_selection->>'kind' = 'all') THEN
        v_selected := v_inventory;
    ELSE
        IF jsonb_typeof(v_selection) = 'object' AND v_selection->>'kind' = 'tables' THEN
            v_requested := v_selection->'tables';
        ELSE
            IF jsonb_typeof(v_selection) = 'string' THEN
                v_message := v_selection #>> '{}';
                IF left(v_message, 5) = 'file:' THEN
                    v_selection := pg_read_file(substr(v_message, 6))::jsonb->'snapshot_table_list';
                ELSE
                    v_selection := to_jsonb(regexp_split_to_array(v_message, '\s*,\s*'));
                END IF;
            END IF;
            IF jsonb_typeof(v_selection) <> 'array' THEN
                RAISE EXCEPTION 'Unsupported FDW table selection' USING ERRCODE = '22023';
            END IF;
            FOR v_rule IN SELECT value FROM jsonb_array_elements(v_selection) LOOP
                IF jsonb_typeof(v_rule) <> 'string' THEN
                    RAISE EXCEPTION 'FDW table selection entries must be strings or structured identities' USING ERRCODE = '22023';
                END IF;
                v_message := v_rule #>> '{}';
                IF v_message ~ '[*+?()\[\]|^$\\]' THEN
                    RAISE EXCEPTION 'FDW table selection does not accept connector regular expressions' USING ERRCODE = '0A000';
                END IF;
                v_parts := parse_ident(v_message, true);
                IF cardinality(v_parts) = 3 THEN
                    v_source_id := jsonb_build_object('db',v_parts[1],'schema',v_parts[2],'table',v_parts[3]);
                ELSIF cardinality(v_parts) = 2 AND v_connector = 'mysql' THEN
                    v_source_id := jsonb_build_object('db',v_parts[1],'schema',v_owner,'table',v_parts[2]);
                ELSIF cardinality(v_parts) = 2 THEN
                    v_source_id := jsonb_build_object('db',v_db,'schema',v_parts[1],'table',v_parts[2]);
                ELSE
                    RAISE EXCEPTION 'Invalid FDW table identity: %', v_message USING ERRCODE = '22023';
                END IF;
                SELECT count(*), (jsonb_agg(value)->0) INTO v_count, v_source_id
                  FROM jsonb_array_elements(v_inventory)
                 WHERE lower(value->>'db') = lower(v_source_id->>'db')
                   AND lower(value->>'schema') = lower(v_source_id->>'schema')
                   AND lower(value->>'table') = lower(v_source_id->>'table');
                IF v_count <> 1 THEN
                    RAISE EXCEPTION 'FDW table selection has no unique match: %', v_message USING ERRCODE = '22023';
                END IF;
                v_requested := v_requested || jsonb_build_array(v_source_id);
            END LOOP;
        END IF;
        IF v_requested IS NULL OR jsonb_typeof(v_requested) <> 'array' OR jsonb_array_length(v_requested) = 0 THEN
            RAISE EXCEPTION 'FDW table selection is explicitly empty' USING ERRCODE = '22023';
        END IF;
        FOR v_source_id IN SELECT value FROM jsonb_array_elements(v_requested) LOOP
            IF jsonb_typeof(v_source_id) <> 'object' OR v_source_id->>'db' IS DISTINCT FROM v_db
               OR v_source_id->>'schema' IS DISTINCT FROM v_owner
               OR NOT v_inventory @> jsonb_build_array(v_source_id) THEN
                RAISE EXCEPTION 'FDW table selection is outside the exact configured source scope: %', v_source_id USING ERRCODE = '22023';
            END IF;
            IF NOT v_selected @> jsonb_build_array(v_source_id) THEN
                v_selected := v_selected || jsonb_build_array(v_source_id);
            END IF;
        END LOOP;
    END IF;
    IF v_connector IN ('oracle', 'olr') AND v_copy THEN
        PERFORM synchdb_create_current_scn_ft(v_meta, v_server);
        EXECUTE format('SELECT current_scn FROM %I.current_scn', v_meta) INTO STRICT v_scn;
        IF v_scn IS NULL OR v_scn <= 0 OR v_scn <> trunc(v_scn) THEN
            RAISE EXCEPTION 'Invalid Oracle data-read SCN' USING ERRCODE = '22000';
        END IF;
        v_candidate := jsonb_build_object('scn', v_scn);
        v_boundary := 'ORACLE_AS_OF_SCN_DATA_ONLY';
    ELSIF NOT v_copy THEN
        v_boundary := 'METADATA_ONLY';
    END IF;
    PERFORM synchdb_create_stage_fts_plan(v_name, v_db, v_owner,
        v_stage, v_server, v_case = 'lower', 'error', v_candidate::text,
        v_meta, NULL::text, false, v_case, v_selected, false);
    -- Freeze all final target identities, columns and PKs before local target
    -- DDL or copying.  A malformed/colliding plan is a global preflight error.
    FOR v_source_id IN SELECT value FROM jsonb_array_elements(v_selected) LOOP
        v_source_key:=synchdb_bootstrap_source_id(v_source_id->>'db',v_source_id->>'schema',v_source_id->>'table',v_connector);
        v_local_name := synchdb_bootstrap_normalize_name(v_source_id->>'table',v_case);
        IF octet_length(v_local_name) > current_setting('max_identifier_length')::int THEN
            RAISE EXCEPTION 'Normalized FDW table name is too long' USING ERRCODE = '22023';
        END IF;
        v_target_schema := v_dest;
        v_target_name := v_local_name;
        v_columns := '[]'::jsonb;
        v_pk := '[]'::jsonb;
        v_src_parts := CASE WHEN v_connector = 'mysql'
            THEN ARRAY[v_db,v_source_id->>'table'] ELSE ARRAY[v_db,v_owner,v_source_id->>'table'] END;
        FOR v_rule IN SELECT value FROM jsonb_array_elements(v_mappings) WHERE value->>'kind' = 'table' LOOP
            IF v_rule->>'source' = v_source_key THEN
                v_dst_parts := parse_ident(v_rule->>'destination', true);
                IF cardinality(v_dst_parts) = 1 THEN v_target_schema := 'public'; v_target_name := v_dst_parts[1];
                ELSIF cardinality(v_dst_parts) = 2 THEN v_target_schema := v_dst_parts[1]; v_target_name := v_dst_parts[2];
                ELSE RAISE EXCEPTION 'Invalid FDW table mapping destination' USING ERRCODE = '22023'; END IF;
            END IF;
        END LOOP;
        IF v_target_schema = ANY(ARRAY[v_source::text,v_stage::text,v_meta::text,v_load::text])
           OR v_target_schema = 'pg_catalog' OR left(v_target_schema, 3) = 'pg_'
           OR octet_length(v_target_name) > current_setting('max_identifier_length')::int
           OR octet_length(v_target_schema) > current_setting('max_identifier_length')::int THEN
            RAISE EXCEPTION 'Invalid or reserved FDW final target identity' USING ERRCODE = '22023';
        END IF;
        IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_tables) t
                    WHERE t->>'target_schema' = v_target_schema AND t->>'target_name' = v_target_name) THEN
            RAISE EXCEPTION 'FDW table mappings collide at %.%', v_target_schema, v_target_name USING ERRCODE = '22023';
        END IF;
        SELECT c.oid INTO STRICT v_new_oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = v_stage AND c.relname = v_local_name AND c.relkind = 'f';
        FOR r IN EXECUTE format('SELECT c.*, a.attname::text AS local_name, a.atttypid, a.atttypmod, a.attnotnull
                 FROM (SELECT c.*, row_number() OVER (ORDER BY c.position) AS local_position
                         FROM %I.columns c WHERE c."schema"=$2 AND c.table_name=$3) c
                 JOIN pg_attribute a ON a.attrelid = $1 AND a.attnum = c.local_position
                WHERE c."schema" = $2 AND c.table_name = $3 AND NOT a.attisdropped ORDER BY c.position', v_meta)
                 USING v_new_oid, v_owner, v_source_id->>'table' LOOP
            v_source_name := r.local_name;
            v_target_column := v_source_name;
            v_type_sql := synchdb_bootstrap_type(v_connector,v_source_key,
                jsonb_build_object('name',r.column_name,'type_name',r.type_name,
                    'physical_type',format_type(r.atttypid,r.atttypmod),
                    'length',CASE WHEN coalesce(nullif(r.length,0),r.precision,-1)>=2147483647 THEN -1
                                  ELSE coalesce(nullif(r.length,0),r.precision,-1) END,
                    'scale',r.scale,'auto_incremented',false),v_mappings);
            v_type_oid := to_regtype(v_type_sql);
            v_transform := NULL;
            FOR v_rule IN SELECT value FROM jsonb_array_elements(v_mappings)
                           WHERE value->>'kind' IN ('column','transform') LOOP
                IF v_rule->>'source' <> synchdb_bootstrap_column_id(v_source_key,r.column_name::text) THEN CONTINUE; END IF;
                CASE v_rule->>'kind'
                  WHEN 'column' THEN v_target_column := v_rule->>'destination';
                  WHEN 'transform' THEN v_transform := v_rule->>'destination';
                END CASE;
            END LOOP;
            IF v_target_column IS NULL OR v_target_column = ''
               OR octet_length(v_target_column) > current_setting('max_identifier_length')::int
               OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_columns) x WHERE x->>'target' = v_target_column) THEN
                RAISE EXCEPTION 'Invalid or colliding FDW target column' USING ERRCODE = '22023';
            END IF;
            v_column := jsonb_build_object('position', r.position, 'source', r.column_name,
                'source_key',synchdb_bootstrap_column_id(v_source_key,r.column_name::text),
                'local_source', v_source_name, 'source_type', r.type_name, 'target', v_target_column,
                'type_sql', v_type_sql, 'type_oid', v_type_oid, 'not_null', r.attnotnull,
                'transform', v_transform);
            PERFORM synchdb_bootstrap_validate_transform(v_transform);
            v_columns := v_columns || jsonb_build_array(v_column);
        END LOOP;
        FOR v_rule IN SELECT value FROM jsonb_array_elements(v_mappings)
                       WHERE value->>'kind' IN ('column','datatype','transform') LOOP
            IF left((CASE WHEN v_rule->>'kind'='datatype' THEN split_part(v_rule->>'source','|',1) ELSE v_rule->>'source' END),length(v_source_key)+1)
                  = v_source_key||'.'
               AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(v_columns) c
                   WHERE (CASE WHEN v_rule->>'kind'='datatype' THEN split_part(v_rule->>'source','|',1) ELSE v_rule->>'source' END)=synchdb_bootstrap_column_id(v_source_key,c->>'source')) THEN
                RAISE EXCEPTION 'FDW mapping refers to an unknown selected source column: %',v_rule->>'source' USING ERRCODE='42703';
            END IF;
        END LOOP;
        IF jsonb_array_length(v_columns) = 0 THEN
            RAISE EXCEPTION 'FDW selected table has no validated columns' USING ERRCODE = '22000';
        END IF;
        FOR r IN EXECUTE format('SELECT column_name FROM %I.keys WHERE "schema"=$1 AND table_name=$2 AND is_primary ORDER BY position', v_meta)
                 USING v_owner, v_source_id->>'table' LOOP
            SELECT x->>'target' INTO STRICT v_target_column FROM jsonb_array_elements(v_columns) x WHERE x->>'source' = r.column_name;
            v_pk := v_pk || jsonb_build_array(v_target_column);
        END LOOP;
        v_old_oid := to_regclass(format('%I.%I', v_target_schema, v_target_name));
        v_old_descriptor := NULL;
        IF v_old_oid IS NOT NULL THEN
            IF v_policy = 'error' THEN
                RAISE EXCEPTION 'Bootstrap final target %.% already exists', v_target_schema, v_target_name USING ERRCODE = '42P07';
            END IF;
            SELECT to_jsonb(t) INTO v_old_ledger FROM synchdb_bootstrap_targets t
             WHERE target_oid = v_old_oid AND connector_id = v_connector_id AND source_identity = v_source_id;
            v_old_descriptor := synchdb_bootstrap_relation_descriptor(v_old_oid);
            IF v_old_ledger IS NULL OR (v_old_ledger->>'target_owner')::oid <> (current_user::regrole)::oid
               OR v_old_ledger->>'target_schema' IS DISTINCT FROM v_target_schema
               OR v_old_ledger->>'target_name' IS DISTINCT FROM v_target_name
               OR v_old_ledger->'descriptor' IS DISTINCT FROM v_old_descriptor THEN
                RAISE EXCEPTION 'Bootstrap final target %.% is unowned or has changed', v_target_schema, v_target_name USING ERRCODE = '55000';
            END IF;
            PERFORM synchdb_bootstrap_check_replaceable(v_old_oid);
        END IF;
        v_tables := v_tables || jsonb_build_array(jsonb_build_object('source',v_source_id,'source_key',v_source_key,
            'stage_oid', v_new_oid, 'local_name',v_local_name,'target_schema',v_target_schema,
            'target_name',v_target_name,'columns',v_columns,'primary_key',v_pk,
            'expected_old_oid',v_old_oid,'expected_old_descriptor',v_old_descriptor));
    END LOOP;
    v_artifact_intents := jsonb_build_array(jsonb_build_object('kind','HISTORY','provider',v_connector,
        'required_for_future_cdc',v_connector IN ('oracle','mysql','sqlserver'),
        'publish',false,'relation_oid',NULL,'disposition','UNPUBLISHED'));
    v_plan := jsonb_build_object('version',1,'generation',v_generation,'connector_id',v_connector_id,
        'connector_name',v_name,'config_revision',v_revision,'provider',v_connector,
        'config_fingerprint',md5(v_config::text),'selection_fingerprint',md5(v_selected::text),
        'mapping_fingerprint',md5(v_mappings::text),'schema_fingerprint',md5(v_tables::text),
        'copy_data',v_copy,'after_bootstrap',CASE WHEN v_copy THEN 'STOP' ELSE 'PAUSE' END,
        'failure_policy',v_failure,'target_policy',v_policy,'case_strategy',v_case,
        'selection',v_selected,'mapping_snapshot',v_mappings,'tables',v_tables,'artifact_intents',v_artifact_intents,
        'source_read_policy',v_boundary,'candidate_boundary',v_candidate,
        'source_consistency',v_read_contract,
        'metadata_consistent_with_data',false,'cdc_protocol_verified',false);
    -- Capture preparation resources before executing user expressions.  A
    -- callback cannot smuggle a preexisting relation into cleanup by moving it
    -- into a workspace; the manifest rejects any later unplanned object.
    v_resources := synchdb_bootstrap_resources(ARRAY[v_source,v_stage,v_meta,v_load],v_server_oid);
    -- A table unit owns every local action, including binding and publication.
    FOR v_table IN SELECT value FROM jsonb_array_elements(v_tables) LOOP
        v_source_id := v_table->'source';
        v_source_key := v_table->>'source_key';
        v_target_schema := v_table->>'target_schema';
        v_target_name := v_table->>'target_name';
        v_local_name := v_table->>'local_name';
        v_rows := 0;
        BEGIN
            v_stage_name := 'CREATE';
            SELECT string_agg(format('%I %s%s', x->>'target', x->>'type_sql',
                CASE WHEN (x->>'not_null')::boolean THEN ' NOT NULL' ELSE '' END),', ' ORDER BY ord)
              INTO v_ddl FROM jsonb_array_elements(v_table->'columns') WITH ORDINALITY f(x,ord);
            EXECUTE format('CREATE TABLE %I.%I (%s) USING heap',v_load,v_local_name,v_ddl);
            v_new_oid := to_regclass(format('%I.%I',v_load,v_local_name));
            v_stage_name := 'PRIMARY_KEY';
            SELECT string_agg(quote_ident(value #>> '{}'),', ' ORDER BY ord) INTO v_pk_list
              FROM jsonb_array_elements(v_table->'primary_key') WITH ORDINALITY f(value,ord);
            IF v_pk_list IS NOT NULL THEN
                EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I PRIMARY KEY (%s)',
                    v_load,v_local_name,v_prefix || '_pk_' || (v_success+v_failed)::text,v_pk_list);
            END IF;
            v_stage_name := 'COPY';
            IF v_copy THEN
                SELECT string_agg(quote_ident(x->>'target'),', ' ORDER BY ord),
                       string_agg(format('_sdb_value_%s.v',ord),', ' ORDER BY ord),
                       string_agg(format(' CROSS JOIN LATERAL %I.synchdb_bootstrap_value(s.%I::text,%L) AS _sdb_value_%s(v %s)',
                           v_ext,x->>'local_source',x->>'transform',ord,x->>'type_sql'),' ' ORDER BY ord)
                  INTO v_dst_list,v_src_list,v_laterals
                  FROM jsonb_array_elements(v_table->'columns') WITH ORDINALITY f(x,ord);
                EXECUTE format('INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I s %s',
                    v_load,v_local_name,v_dst_list,v_src_list,v_stage,v_local_name,v_laterals);
                GET DIAGNOSTICS v_rows = ROW_COUNT;
            END IF;
            v_stage_name := 'PUBLISH';
            v_old_oid := (v_table->>'expected_old_oid')::oid;
            EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I',v_target_schema);
            IF v_old_oid IS NOT NULL THEN
                EXECUTE format('LOCK TABLE ONLY %I.%I IN ACCESS EXCLUSIVE MODE',v_target_schema,v_target_name);
                IF to_regclass(format('%I.%I',v_target_schema,v_target_name)) IS DISTINCT FROM v_old_oid
                   OR synchdb_bootstrap_relation_descriptor(v_old_oid) IS DISTINCT FROM v_table->'expected_old_descriptor'
                   OR NOT EXISTS (SELECT 1 FROM synchdb_bootstrap_targets t WHERE t.target_oid = v_old_oid
                       AND t.connector_id = v_connector_id AND t.source_identity = v_source_id
                       AND t.target_owner = (current_user::regrole)::oid
                       AND t.target_schema = v_target_schema AND t.target_name = v_target_name
                       AND t.descriptor = v_table->'expected_old_descriptor') THEN
                    RAISE EXCEPTION 'Bootstrap replacement target changed before publication' USING ERRCODE = '55000';
                END IF;
                PERFORM synchdb_bootstrap_check_replaceable(v_old_oid);
                -- The old object remains locked. DROP RESTRICT can never remove
                -- a dependent object; any failure rolls this complete unit back.
                EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',v_target_schema,v_target_name,
                    v_prefix || '_old_' || v_old_oid::text);
            ELSIF to_regclass(format('%I.%I',v_target_schema,v_target_name)) IS NOT NULL THEN
                RAISE EXCEPTION 'Bootstrap target appeared before publication' USING ERRCODE = '42P07';
            END IF;
            IF v_local_name <> v_target_name THEN
                EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',v_load,v_local_name,v_target_name);
            END IF;
            EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I',v_load,v_target_name,v_target_schema);
            v_stage_name := 'BIND';
            DELETE FROM synchdb_attribute WHERE name::text COLLATE "C" = v_name COLLATE "C" AND type::text = v_connector
                AND ext_tbname = v_source_key COLLATE "C";
            INSERT INTO synchdb_attribute(name,type,attrelid,attnum,ext_tbname,ext_attname,ext_atttypename)
                SELECT v_name::name,v_connector::name,v_new_oid,ord::smallint,
                    v_source_key,
                    x->>'source',lower(x->>'source_type')
                  FROM jsonb_array_elements(v_table->'columns') WITH ORDINALITY f(x,ord);
            GET DIAGNOSTICS v_count = ROW_COUNT;
            IF v_count <> jsonb_array_length(v_table->'columns') THEN
                RAISE EXCEPTION 'Bootstrap attribute publication did not bind every selected column' USING ERRCODE='55000';
            END IF;
            IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_table->'columns') WITH ORDINALITY f(x,ord)
                WHERE NOT EXISTS (SELECT 1 FROM synchdb_attribute a
                    WHERE a.name::text COLLATE "C"=v_name COLLATE "C" AND a.type::text=v_connector
                      AND a.attrelid=v_new_oid AND a.attnum=ord
                      AND a.ext_tbname=v_source_key COLLATE "C"
                      AND a.ext_attname=x->>'source' AND a.ext_atttypename=lower(x->>'source_type'))) THEN
                RAISE EXCEPTION 'Bootstrap attribute publication changed the required binding' USING ERRCODE='55000';
            END IF;
            v_stage_name := 'OWNERSHIP';
            IF v_old_oid IS NOT NULL THEN
                EXECUTE format('DROP TABLE %I.%I',v_target_schema,v_prefix || '_old_' || v_old_oid::text);
                DELETE FROM synchdb_bootstrap_targets WHERE target_oid = v_old_oid;
                GET DIAGNOSTICS v_count = ROW_COUNT;
                IF v_count <> 1 THEN
                    RAISE EXCEPTION 'Bootstrap ownership changed during replacement' USING ERRCODE='55000';
                END IF;
            END IF;
            v_descriptor := synchdb_bootstrap_relation_descriptor(v_new_oid);
            INSERT INTO synchdb_bootstrap_targets(target_oid,connector_id,generation,source_identity,
                target_schema,target_name,target_owner,descriptor,descriptor_hash,plan)
                VALUES(v_new_oid,v_connector_id,v_generation,v_source_id,v_target_schema,v_target_name,
                       (current_user::regrole)::oid,v_descriptor,md5(v_descriptor::text),v_plan);
            GET DIAGNOSTICS v_count = ROW_COUNT;
            IF v_count <> 1 THEN
                RAISE EXCEPTION 'Bootstrap target ownership was not published' USING ERRCODE='55000';
            END IF;
            IF NOT EXISTS (SELECT 1 FROM synchdb_bootstrap_targets t
                WHERE t.target_oid=v_new_oid AND t.connector_id=v_connector_id
                  AND t.generation=v_generation AND t.source_identity=v_source_id
                  AND t.target_schema=v_target_schema AND t.target_name=v_target_name
                  AND t.target_owner=(current_user::regrole)::oid AND t.descriptor=v_descriptor
                  AND t.descriptor_hash=md5(v_descriptor::text) AND t.plan=v_plan) THEN
                RAISE EXCEPTION 'Bootstrap ownership publication changed the required ledger entry' USING ERRCODE='55000';
            END IF;
            v_results := v_results || jsonb_build_array(jsonb_build_object('source',v_source_id,
                'target_schema',v_target_schema,'target_name',v_target_name,'target_oid',v_new_oid,
                'status','COMPLETE','stage','PUBLISHED','rows',v_rows,'sqlstate',NULL));
            v_success := v_success + 1;
            v_total_rows := v_total_rows + v_rows;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE,v_message = MESSAGE_TEXT,v_detail = PG_EXCEPTION_DETAIL;
            IF v_failure = 'fail_fast' THEN RAISE; END IF;
            v_failed := v_failed + 1;
            v_results := v_results || jsonb_build_array(jsonb_build_object('source',v_source_id,
                'target_schema',v_target_schema,'target_name',v_target_name,'target_oid',NULL,
                'status','FAILED','stage',v_stage_name,'rows',0,'sqlstate',v_state,'message',v_message,'detail',v_detail));
        END;
    END LOOP;
    PERFORM synchdb_bootstrap_cleanup(v_resources);
    v_result := jsonb_build_object('version',1,'status',CASE WHEN v_failed = 0 THEN 'COMPLETE'
             WHEN v_success > 0 THEN 'PARTIAL' ELSE 'FAILED' END,
        'generation',v_generation,'copy_data',v_copy,
        'after_bootstrap',CASE WHEN v_copy THEN 'STOP' ELSE 'PAUSE' END,
        'cdc_eligible',false,'cdc_resume_material',NULL,
        'retry_disposition',CASE WHEN v_failed = 0 THEN 'NONE' ELSE 'RESTART_BOOTSTRAP' END,
        'source_read_boundary',jsonb_build_object('kind',v_boundary,'candidate',v_candidate,'cdc_verified',false),
        'source_consistency',v_read_contract,
        'plan',v_plan,'tables',v_results,'resources',v_resources,
        'resources_disposition','CLEANED','rows',v_total_rows,
        'artifact_intents',v_artifact_intents);
    INSERT INTO synchdb_bootstrap_runs(generation,connector_id,config_revision,result)
      VALUES(v_generation,v_connector_id,v_revision,v_result);
    GET DIAGNOSTICS v_count=ROW_COUNT;
    IF v_count<>1 OR NOT EXISTS (SELECT 1 FROM synchdb_bootstrap_runs saved_run
        WHERE saved_run.generation=v_generation AND saved_run.connector_id=v_connector_id
          AND saved_run.config_revision=v_revision AND saved_run.result=v_result) THEN
        RAISE EXCEPTION 'Bootstrap structured result was not persisted' USING ERRCODE='55000';
    END IF;
    RETURN v_result;
END;
$fn$;
