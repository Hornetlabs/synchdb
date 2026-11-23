-- Candidate-only source positions; these never authorize a CDC handoff.

CREATE OR REPLACE FUNCTION synchdb_create_current_binlog_pos_ft(
    p_schema name,   -- e.g. 'mysql_stage'
    p_server name    -- e.g. 'mysql_server'
) RETURNS void
LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT
SET search_path = pg_catalog
AS $synchdb_create_current_binlog_pos_ft$
DECLARE
    v_schema name := p_schema;
    v_server name := p_server;
BEGIN
    ----------------------------------------------------------------------
    -- Sanity check: foreign server must exist
    ----------------------------------------------------------------------
    PERFORM 1
    FROM pg_foreign_server
    WHERE srvname = v_server;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'synchdb_create_current_binlog_pos_ft: foreign server "%" does not exist', v_server;
    END IF;

    ----------------------------------------------------------------------
    -- Ensure target schema exists
    ----------------------------------------------------------------------
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);

    ----------------------------------------------------------------------
    -- performance_schema.global_variables -> <schema>.global_variables
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.global_variables (
            variable_name  text,
            variable_value text
        )
        SERVER %2$I
        OPTIONS (
            dbname     'performance_schema',
            table_name 'global_variables'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- performance_schema.log_status -> <schema>.log_status
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.log_status (
            server_uuid     text,
            local           text,   -- JSON document as text
            replication     text,   -- JSON
            storage_engines text    -- JSON
        )
        SERVER %2$I
        OPTIONS (
            dbname     'performance_schema',
            table_name 'log_status'
        )
    $SQL$, v_schema, v_server);

    RETURN;
END;
$synchdb_create_current_binlog_pos_ft$;

COMMENT ON FUNCTION synchdb_create_current_binlog_pos_ft(name, name) IS
   'Read diagnostic MySQL binlog coordinates; this does not establish a locked source transaction or authorize a CDC seed';

CREATE OR REPLACE FUNCTION synchdb_create_current_sqlserver_lsn_ft(
    p_schema name,  -- e.g. 'sqlserver_obj'
    p_server name   -- e.g. '<connector_name>_sqlserver'
) RETURNS void
LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT
SET search_path = pg_catalog
AS $synchdb_create_current_sqlserver_lsn_ft$
DECLARE
  /*
   * sys.fn_cdc_get_max_lsn() returns a varbinary(10). Format it here as Debezium's
   * Lsn.toString() hex-group representation ("%08x:%08x:%04x"). This is the CDC
   * capture high watermark, not the latest business commit or an FDW read-view
   * boundary. Capture lag can put it behind ordinary table reads; it must never
   * independently authorize a snapshot-to-CDC seed.
   */
  v_subqry text := $q$
     SELECT LOWER(STUFF(STUFF(
        CONVERT(varchar(20), sys.fn_cdc_get_max_lsn(), 2),
        9, 0, ':'), 18, 0, ':')) AS [max_lsn]
  $q$;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = p_server) THEN
    RAISE EXCEPTION 'Foreign server "%" does not exist', p_server
      USING HINT = 'Create it first: CREATE SERVER ... FOREIGN DATA WRAPPER tds_fdw OPTIONS(...);';
  END IF;
  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_schema);
  EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.%I', p_schema, 'current_lsn');
  EXECUTE format(
    'CREATE FOREIGN TABLE %I.%I (max_lsn text) ' ||
    'SERVER %I OPTIONS (query %L)',
    p_schema, 'current_lsn', p_server, v_subqry
  );
  RAISE NOTICE 'Recreated foreign table %.% on server %', p_schema, 'current_lsn', p_server;
END;
$synchdb_create_current_sqlserver_lsn_ft$;

COMMENT ON FUNCTION synchdb_create_current_sqlserver_lsn_ft(name, name) IS
   'Read the captured CDC high-water LSN as a diagnostic candidate; capture lag and the FDW read view are not coordinated';

CREATE OR REPLACE FUNCTION synchdb_create_current_scn_ft(
    p_schema name,   -- e.g. 'ora_obj'
    p_server name    -- e.g. 'oracle'
) RETURNS void
LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT
SET search_path = pg_catalog
AS $$
DECLARE
  v_subqry text := '(SELECT current_scn FROM v$database)';
BEGIN
  -- Ensure the foreign server exists
  IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = p_server) THEN
    RAISE EXCEPTION 'Foreign server "%" does not exist', p_server
      USING HINT = 'Create it first: CREATE SERVER ... FOREIGN DATA WRAPPER oracle_fdw OPTIONS(...);';
  END IF;

  -- Ensure target schema
  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_schema);

  -- Always recreate the FT
  EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.%I', p_schema, 'current_scn');

  EXECUTE format(
    'CREATE FOREIGN TABLE %I.%I (current_scn numeric) ' ||
    'SERVER %I OPTIONS ("table" %L, readonly ''true'')',
    p_schema, 'current_scn', p_server, v_subqry
  );

  RAISE NOTICE 'Recreated foreign table %.% on server %',
               p_schema, 'current_scn', p_server;
END;
$$;

COMMENT ON FUNCTION synchdb_create_current_scn_ft(name, name) IS
   'Read an Oracle AS OF SCN data-view candidate; a scalar SCN is not complete LogMiner recovery material';
