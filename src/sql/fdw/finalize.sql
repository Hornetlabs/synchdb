CREATE OR REPLACE FUNCTION synchdb_finalize_initial_snapshot(
    p_source_schema  name,              -- e.g. 'ora_obj'
    p_stage_schema   name,              -- e.g. 'ora_stage'
    p_connector_name name,              -- e.g. 'ora19cconn'
    p_meta_schema    name               -- e.g. 'ora_meta'
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_connector text;           -- 'oracle' | 'olr' | 'mysql' | 'postgres' | 'sqlserver' (lowercased)
    v_server    text;           -- e.g. '<connector>_oracle', '<connector>_mysql', '<connector>_postgres'
    r           record;

    -- error table handling
    v_err_tbl_ident  text;      -- synchdb_fdw_snapshot_errors_<sanitized connector>
    v_err_tbl_exists boolean;
    v_err_count      bigint;
BEGIN
    ----------------------------------------------------------------------
    -- 0) Determine connector type and server name from synchdb_conninfo
    ----------------------------------------------------------------------
    SELECT lower(data->>'connector')
      INTO v_connector
    FROM synchdb_conninfo
    WHERE name = p_connector_name;

    IF v_connector IS NULL OR v_connector = '' THEN
        RAISE EXCEPTION 'synchdb_finalize_initial_snapshot(%): data->>connector is missing/empty in synchdb_conninfo',
                        p_connector_name;
    END IF;

    -- server naming convention: <connector_name>_<connector_type>
    v_server := format('%s_%s', p_connector_name, v_connector);

    ----------------------------------------------------------------------
    -- 1) Drop stage schema (often contains FTs that depend on the FDW)
    ----------------------------------------------------------------------
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = p_stage_schema) THEN
        RAISE NOTICE 'Dropping stage schema "%" CASCADE', p_stage_schema;
        EXECUTE format('DROP SCHEMA %I CASCADE', p_stage_schema);
    ELSE
        RAISE NOTICE 'Stage schema "%" does not exist, skipping', p_stage_schema;
    END IF;

    ----------------------------------------------------------------------
    -- 2) Drop source schema (FDW objects)
    ----------------------------------------------------------------------
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = p_source_schema) THEN
        RAISE NOTICE 'Dropping source schema "%" CASCADE', p_source_schema;
        EXECUTE format('DROP SCHEMA %I CASCADE', p_source_schema);
    ELSE
        RAISE NOTICE 'Source schema "%" does not exist, skipping', p_source_schema;
    END IF;

    ----------------------------------------------------------------------
    -- 3) Drop metadata schema (materialized FDW objects)
    ----------------------------------------------------------------------
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = p_meta_schema) THEN
        RAISE NOTICE 'Dropping metadata schema "%" CASCADE', p_meta_schema;
        EXECUTE format('DROP SCHEMA %I CASCADE', p_meta_schema);
    ELSE
        RAISE NOTICE 'Metadata schema "%" does not exist, skipping', p_meta_schema;
    END IF;

    ----------------------------------------------------------------------
    -- 4) Drop user mappings for server <connector>_<type> (if the server exists)
    ----------------------------------------------------------------------
    IF EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = v_server) THEN
        FOR r IN
            SELECT um.usename
            FROM pg_user_mappings um
            JOIN pg_foreign_server fs ON fs.oid = um.srvid
            WHERE fs.srvname = v_server
        LOOP
            IF r.usename = 'PUBLIC' THEN
                RAISE NOTICE 'Dropping USER MAPPING FOR PUBLIC on server "%"', v_server;
                EXECUTE format('DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER %I', v_server);
            ELSE
                RAISE NOTICE 'Dropping USER MAPPING FOR "%" on server "%"', r.usename, v_server;
                EXECUTE format('DROP USER MAPPING IF EXISTS FOR %I SERVER %I', r.usename, v_server);
            END IF;
        END LOOP;

        ------------------------------------------------------------------
        -- 5) Drop the FDW server itself
        ------------------------------------------------------------------
        RAISE NOTICE 'Dropping SERVER "%" CASCADE', v_server;
        EXECUTE format('DROP SERVER IF EXISTS %I CASCADE', v_server);
    ELSE
        RAISE NOTICE 'Server "%" does not exist, skipping user mappings and server drop', v_server;
    END IF;

    ----------------------------------------------------------------------
    -- 6) Per-connector error table housekeeping
    --    If table doesn't exist  -> do nothing
    --    If exists and empty     -> drop it
    --    If exists and non-empty -> keep it (do nothing)
    ----------------------------------------------------------------------
    v_err_tbl_ident :=
        'synchdb_fdw_snapshot_errors_'
        || regexp_replace(lower(p_connector_name::text), '[^a-z0-9_]', '_', 'g');

    SELECT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname = v_err_tbl_ident
    )
    INTO v_err_tbl_exists;

    IF v_err_tbl_exists THEN
        EXECUTE format('SELECT count(*) FROM %I.%I', 'public', v_err_tbl_ident)
        INTO v_err_count;

        IF COALESCE(v_err_count, 0) = 0 THEN
            RAISE NOTICE 'Dropping empty error table %.% (all snapshot errors resolved)',
                         'public', v_err_tbl_ident;
            EXECUTE format('DROP TABLE %I.%I', 'public', v_err_tbl_ident);
        ELSE
            RAISE NOTICE 'Error table %.% retained with % outstanding item(s)',
                         'public', v_err_tbl_ident, v_err_count;
        END IF;
    END IF;
END;
$$;

COMMENT ON FUNCTION synchdb_finalize_initial_snapshot(name, name, name, name) IS
   'finalize initial snapshot by cleaning up resources and objects';

