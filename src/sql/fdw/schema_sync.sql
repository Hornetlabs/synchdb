CREATE OR REPLACE FUNCTION synchdb_do_schema_sync(
    p_connector_name  name,               -- e.g. 'oracleconn'
    p_secret          text,               -- master key for decrypting connector password
    p_source_schema   name,               -- e.g. 'ora_obj'   (FDW objects + current_scn FT)
    p_stage_schema    name,               -- e.g. 'ora_stage' (staging foreign tables)
    p_dest_schema     name,               -- e.g. 'dst_stage' (materialized tables)
    p_lookup_db       text,               -- e.g. 'free'
    p_lookup_schema   name,               -- e.g. 'dbzuser'
    p_lower_names     boolean DEFAULT true,
    p_on_exists       text    DEFAULT 'replace',  -- 'replace' | 'drop' | 'skip'
    p_offset          text    DEFAULT null,          -- >0 to force a specific SCN; else auto-read
    p_snapshot_tables text    DEFAULT null,
    p_use_subtx         boolean DEFAULT true,
    p_write_schema_hist boolean DEFAULT false,
    p_case_strategy  text DEFAULT 'asis'
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_scn           numeric;    -- used by oracle connector
    v_case_json     jsonb;
    v_offset_json   jsonb;
    v_server_name   text;       -- will be set by synchdb_prepare_initial_snapshot()
    v_meta_schema   name;
    v_connector     text;

    v_binlog_file   text;       -- used by mysql connector
    v_binlog_pos    bigint;     -- used by mysql connector
    v_server_id     text;       -- used by mysql connector

    v_lsn           text;       -- used by postgres connector

    v_ss_lsn        text;       -- used by sqlserver connector (commit_lsn, Debezium hex-group format)

    v_ret           text;       -- return value (connector-specific offset string)
BEGIN
    SELECT lower(data->>'connector')
      INTO v_connector
    FROM synchdb_conninfo
    WHERE name = p_connector_name;

        -- quick check on p_offset and turn it to json
    IF p_offset IS NOT NULL AND btrim(p_offset) <> '' THEN
      v_offset_json := p_offset::jsonb;
    ELSE
      v_offset_json := NULL;
    END IF;

    IF v_connector IS NULL OR v_connector = '' THEN
        RAISE EXCEPTION 'synchdb_do_initial_snapshot(%): data->>connector is missing/empty in synchdb_conninfo',
                        p_connector_name;
    END IF;

    PERFORM synchdb_set_snapstats(
        p_connector_name,
        0::bigint,
        0::bigint,
        (extract(epoch from clock_timestamp()) * 1000)::bigint,
        0::bigint
    );

    v_server_name := synchdb_prepare_initial_snapshot(p_connector_name, p_secret);
    RAISE NOTICE 'Using FDW server "%" for connector "%"', v_server_name, p_connector_name;

    -- Build the case option JSON for synchdb_create_oraviews / *_objs
    v_case_json := jsonb_build_object(
        'case', CASE WHEN p_lower_names THEN 'lower' ELSE 'original' END
    );

    -- Decide metadata schema name up-front
    v_meta_schema := ('metaschema_' || p_connector_name)::name;
    RAISE NOTICE 'Step 1.5c: Materialize foreign object views to %', v_meta_schema;

    -- Create foreign object views based on connector type
    IF v_connector IN ('oracle', 'olr') THEN
        RAISE NOTICE 'Step 1: Creating Oracle FDW views for schema "%" using server "%"',
                     p_source_schema, v_server_name;
        PERFORM synchdb_create_oraviews(v_server_name, p_source_schema, v_case_json);

    ELSIF v_connector = 'mysql' THEN
        RAISE NOTICE 'Step 1: Creating MySQL FDW object views for schema "%" using server "%"',
                     p_source_schema, v_server_name;
        PERFORM synchdb_create_mysql_objs(v_server_name, p_source_schema, v_case_json);

    ELSIF v_connector = 'postgres' THEN
        RAISE NOTICE 'Step 1: Creating PostgreSQL FDW object views for schema "%" using server "%"',
                     p_source_schema, v_server_name;
        PERFORM synchdb_create_pg_objs(v_server_name, p_source_schema, v_case_json);

    ELSIF v_connector = 'sqlserver' THEN
        RAISE NOTICE 'Step 1: Creating SQL Server FDW object views for schema "%" using server "%"',
                     p_source_schema, v_server_name;
        PERFORM synchdb_create_sqlserver_objs(v_server_name, p_source_schema, v_case_json);

    ELSE
        RAISE EXCEPTION 'Unsupported connector type: %', v_connector;
    END IF;

    RAISE NOTICE 'Materializing foreign object views to %', v_meta_schema;
    PERFORM synchdb_materialize_metadata(p_source_schema, v_meta_schema, p_on_exists);
    ----------------------------------------------------------------------
    -- Obtain cutoff offset values (SCN, binlog, LSN) based on connector
    ----------------------------------------------------------------------
    IF v_connector IN ('oracle', 'olr') THEN
        RAISE NOTICE 'Step 2: Creating/ensuring current_scn foreign table exists in schema "%" using server "%"',
                     v_meta_schema, v_server_name;
        PERFORM synchdb_create_current_scn_ft(v_meta_schema, v_server_name);

        RAISE NOTICE 'Step 3: Reading current SCN value from %.current_scn', v_meta_schema;
        EXECUTE format('SELECT current_scn FROM %I.current_scn', v_meta_schema)
           INTO v_scn;

        -- overwrite if needed
        IF v_offset_json IS NOT NULL AND v_offset_json ? 'scn' THEN
                  v_scn := (v_offset_json->>'scn')::numeric;
        END IF;

        RAISE NOTICE 'Using SCN value % for snapshot', v_scn;

        RAISE NOTICE 'Step 4: Creating staging foreign tables in schema "%"', p_stage_schema;
        PERFORM synchdb_create_stage_fts(
            p_connector_name,
            p_lookup_db,
            p_lookup_schema,
            p_stage_schema,
            v_server_name,
            p_lower_names,
            p_on_exists,
            json_build_object('scn', v_scn)::text,
            v_meta_schema,
            p_snapshot_tables,
            p_write_schema_hist,
            p_case_strategy
        );

    ELSIF v_connector = 'mysql' THEN
        RAISE NOTICE 'Step 2: Creating/ensuring current binlog pos foreign table exists in schema "%" using server "%"',
                     v_meta_schema, v_server_name;
        PERFORM synchdb_create_current_binlog_pos_ft(v_meta_schema, v_server_name);

        RAISE NOTICE 'Step 3: Reading current binlog pos and server id values from %.log_status and %.global_variables',
                     v_meta_schema, v_meta_schema;

        EXECUTE format(
            'SELECT (local::jsonb ->> ''binary_log_file'') AS binlog_file,
                    ((local::jsonb ->> ''binary_log_position'')::bigint) AS binlog_pos
               FROM %I.log_status
              LIMIT 1',
            v_meta_schema
        )
        INTO v_binlog_file, v_binlog_pos;

        EXECUTE format(
            'SELECT variable_value
               FROM %I.global_variables
              WHERE variable_name = ''server_id''
              LIMIT 1',
            v_meta_schema
        )
        INTO v_server_id;

        IF v_binlog_file IS NULL THEN
            RAISE EXCEPTION 'Unable to read current binlog file from %.log_status', v_meta_schema;
        END IF;

        IF v_binlog_pos IS NULL THEN
            RAISE EXCEPTION 'Unable to read current binlog pos from %.log_status', v_meta_schema;
        END IF;

        IF v_server_id IS NULL THEN
            RAISE EXCEPTION 'Unable to read server_id from %.global_variables', v_meta_schema;
        END IF;

                -- overwrite if needed
        IF v_offset_json IS NOT NULL THEN
              v_binlog_file := v_offset_json->>'file';
          v_binlog_pos  := (v_offset_json->>'pos')::bigint;
        END IF;

        RAISE NOTICE 'Using binlog file %, pos %, server id % for snapshot',
                     v_binlog_file, v_binlog_pos, v_server_id;

        RAISE NOTICE 'Step 4: Creating staging foreign tables in schema "%"', p_stage_schema;
        PERFORM synchdb_create_stage_fts(
            p_connector_name,
            p_lookup_db,
            p_lookup_schema,
            p_stage_schema,
            v_server_name,
            p_lower_names,
            p_on_exists,
            json_build_object('file', v_binlog_file, 'pos', v_binlog_pos, 'ts_sec', floor(extract(epoch from clock_timestamp())))::text,
            v_meta_schema,
            p_snapshot_tables,
            p_write_schema_hist,
            p_case_strategy
        );

    ELSIF v_connector = 'postgres' THEN
        RAISE NOTICE 'Step 2: Creating/ensuring current lsn foreign table exists in schema "%" using server "%"',
                     v_meta_schema, v_server_name;
        PERFORM synchdb_create_current_lsn_ft(v_meta_schema, v_server_name);

        RAISE NOTICE 'Step 3: Reading current lsn value from %.wal_lsn', v_meta_schema;
        --EXECUTE format('SELECT wal_lsn FROM %I.wal_lsn', v_meta_schema)
        EXECUTE format('SELECT (wal_lsn::pg_lsn - ''0/0''::pg_lsn)::bigint FROM %I.wal_lsn', v_meta_schema)
           INTO v_lsn;

        IF v_lsn IS NULL THEN
            RAISE EXCEPTION 'Unable to read wal_lsn from %.wal_lsn', v_meta_schema;
        END IF;

                -- overwrite if needed
                IF v_offset_json IS NOT NULL THEN
          v_lsn  := v_offset_json->>'lsn';
        END IF;

        RAISE NOTICE 'Using LSN % for snapshot', v_lsn;

        RAISE NOTICE 'Step 4: Creating staging foreign tables in schema "%"', p_stage_schema;
        PERFORM synchdb_create_stage_fts(
            p_connector_name,
            p_lookup_db,
            p_lookup_schema,
            p_stage_schema,
            v_server_name,
            p_lower_names,
            p_on_exists,
            json_build_object('lsn', v_lsn)::text,
            v_meta_schema,
            p_snapshot_tables,
            p_write_schema_hist,
            p_case_strategy
        );

    ELSIF v_connector = 'sqlserver' THEN
        RAISE NOTICE 'Step 2: Creating/ensuring current lsn foreign table exists in schema "%" using server "%"',
                     v_meta_schema, v_server_name;
        PERFORM synchdb_create_current_sqlserver_lsn_ft(v_meta_schema, v_server_name);

        RAISE NOTICE 'Step 3: Reading current commit_lsn value from %.current_lsn', v_meta_schema;
        EXECUTE format('SELECT max_lsn FROM %I.current_lsn', v_meta_schema)
           INTO v_ss_lsn;

        IF v_ss_lsn IS NULL THEN
            RAISE EXCEPTION 'Unable to read max_lsn from %.current_lsn', v_meta_schema;
        END IF;

        -- overwrite if needed
        IF v_offset_json IS NOT NULL AND v_offset_json ? 'commit_lsn' THEN
          v_ss_lsn := v_offset_json->>'commit_lsn';
        END IF;

        RAISE NOTICE 'Using commit_lsn % for snapshot', v_ss_lsn;

        RAISE NOTICE 'Step 4: Creating staging foreign tables in schema "%"', p_stage_schema;
        PERFORM synchdb_create_stage_fts(
            p_connector_name,
            p_lookup_db,
            p_lookup_schema,
            p_stage_schema,
            v_server_name,
            p_lower_names,
            p_on_exists,
            json_build_object('commit_lsn', v_ss_lsn)::text,
            v_meta_schema,
            p_snapshot_tables,
            p_write_schema_hist,
            p_case_strategy
        );

    ELSE
        RAISE EXCEPTION 'Unsupported connector type: %', v_connector;
    END IF;

    RAISE NOTICE 'Step 5: Materializing staging foreign tables from "%" to "%"', p_stage_schema, p_dest_schema;
    PERFORM synchdb_materialize_schema(p_connector_name, p_stage_schema, p_dest_schema, p_on_exists);

    RAISE NOTICE 'Step 6: Migrating primary keys from metadata schema "%" to "%"', p_source_schema, p_dest_schema;
    PERFORM synchdb_migrate_primary_keys(p_source_schema, p_dest_schema, p_case_strategy);

    RAISE NOTICE 'Step 7: Applying column mappings to schema "%" using objmap %.% for connector %',
                 p_dest_schema, p_lookup_db, p_lookup_schema, p_connector_name;
    PERFORM synchdb_apply_column_mappings(p_dest_schema, p_connector_name, p_lookup_db, p_lookup_schema, p_case_strategy);

    RAISE NOTICE 'Step 8: Migrating data with transforms from "%" to "%" (lookup: %.% for connector %)',
                 p_stage_schema, p_dest_schema, p_lookup_db, p_lookup_schema, p_connector_name;

    RAISE NOTICE 'Step 9: Applying table mappings on destination schema "%" (lookup: %.% for connector %)',
                 p_dest_schema, p_lookup_db, p_lookup_schema, p_connector_name;
    PERFORM synchdb_apply_table_mappings(p_dest_schema, p_connector_name, p_lookup_db, p_lookup_schema, p_case_strategy);

    -- Connector-specific completion notice + return value
    IF v_connector IN ('oracle', 'olr') THEN
        RAISE NOTICE 'Initial snapshot completed successfully at SCN %', v_scn;
        v_ret := v_scn::text;
    ELSIF v_connector = 'mysql' THEN
        RAISE NOTICE 'Initial snapshot completed successfully at binlog %, pos %, server_id %',
                     v_binlog_file, v_binlog_pos, v_server_id;
        v_ret := format('%s;%s;%s', v_binlog_file, v_binlog_pos, v_server_id);
    ELSIF v_connector = 'postgres' THEN
        RAISE NOTICE 'Initial snapshot completed successfully at LSN %', v_lsn;
        v_ret := v_lsn;
    ELSIF v_connector = 'sqlserver' THEN
        RAISE NOTICE 'Initial snapshot completed successfully at commit_lsn %', v_ss_lsn;
        v_ret := v_ss_lsn;
    ELSE
        v_ret := NULL;
    END IF;

    PERFORM synchdb_finalize_initial_snapshot(p_source_schema, p_stage_schema, p_connector_name, v_meta_schema);

    PERFORM synchdb_set_snapstats(
        p_connector_name,
        0::bigint,
        0::bigint,
        0::bigint,
        (extract(epoch from clock_timestamp()) * 1000)::bigint
    );

    RETURN v_ret;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'synchdb_do_schema_sync() failed: % [%]', SQLERRM, SQLSTATE
            USING HINT = 'Check NOTICE logs above to identify which step failed.';
END;
$$;
COMMENT ON FUNCTION synchdb_do_schema_sync(name, text, name, name, name, text, name, boolean, text, text, text, boolean, boolean, text) IS
   'perform schema only sync procedure + transforms using a oracle_fdw server - no data migration';

