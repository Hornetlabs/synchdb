-----------------------------------------------------------------------------------------------------------------
    -- SQLSERVER: functions to map SQL Server objects to PostgreSQL via FDW (tds_fdw)
-----------------------------------------------------------------------------------------------------------------
CREATE FUNCTION synchdb_create_sqlserver_objs(
   server      name,
   schema      name    DEFAULT NAME 'public',
   options     jsonb   DEFAULT NULL
) RETURNS void
   LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT SET search_path = pg_catalog AS
$synchdb_create_sqlserver_objs$
DECLARE
   old_msglevel text;
BEGIN
   /* remember old setting */
   old_msglevel := current_setting('client_min_messages');

   /* make the output less verbose */
   SET LOCAL client_min_messages = warning;

   EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema);

   /*
    * SynchDB only needs "tables", "columns" and "keys" to construct the destination
    * tables (same minimal contract as the mysql/oracle builders above) - see
    * doc/docs/en/architecture/fdw_based_snapshot.md. SQL Server implements the ANSI
    * INFORMATION_SCHEMA views, so these are built directly against them via tds_fdw's
    * "query" pass-through option, rather than importing raw catalog tables first.
    */

   /*
    * NOTE: tds_fdw defaults to matching result columns to local foreign-table
    * columns BY NAME (option "match_column_names", default true), and the match
    * is case-sensitive. Every query below therefore aliases each result column
    * to the exact (lowercase) local column name.
    */

   /* tables */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.tables CASCADE', schema);
   EXECUTE format($SQL$
      CREATE FOREIGN TABLE %1$I.tables (
         schema     text,
         table_name text
      ) SERVER %2$I OPTIONS (query $query$
         SELECT t.TABLE_SCHEMA AS [schema], t.TABLE_NAME AS [table_name]
         FROM INFORMATION_SCHEMA.TABLES t
         JOIN sys.schemas s ON s.name = t.TABLE_SCHEMA
         JOIN sys.tables st ON st.schema_id = s.schema_id AND st.name = t.TABLE_NAME
         WHERE t.TABLE_TYPE = 'BASE TABLE'
           AND st.is_ms_shipped = 0
      $query$);
      COMMENT ON FOREIGN TABLE %1$I.tables IS 'SQL Server tables (via tds_fdw)';
   $SQL$, schema, server);

   /* columns */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.columns CASCADE', schema);
   EXECUTE format($SQL$
      CREATE FOREIGN TABLE %1$I.columns (
         schema        text,
         table_name    text,
         column_name   text,
         position      integer,
         type_name     text,
         length        integer,
         precision     integer,
         scale         integer,
         nullable      boolean,
         default_value text
      ) SERVER %2$I OPTIONS (query $query$
         SELECT c.TABLE_SCHEMA AS [schema],
                c.TABLE_NAME AS [table_name],
                c.COLUMN_NAME AS [column_name],
                CAST(c.ORDINAL_POSITION AS int) AS [position],
                c.DATA_TYPE AS [type_name],
                CAST(c.CHARACTER_MAXIMUM_LENGTH AS int) AS [length],
                CAST(c.NUMERIC_PRECISION AS int) AS [precision],
                CAST(c.NUMERIC_SCALE AS int) AS [scale],
                CASE WHEN c.IS_NULLABLE = 'YES' THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS [nullable],
                c.COLUMN_DEFAULT AS [default_value]
         FROM INFORMATION_SCHEMA.COLUMNS c
         JOIN INFORMATION_SCHEMA.TABLES t
           ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
         JOIN sys.schemas s ON s.name = t.TABLE_SCHEMA
         JOIN sys.tables st ON st.schema_id = s.schema_id AND st.name = t.TABLE_NAME
         WHERE t.TABLE_TYPE = 'BASE TABLE'
           AND st.is_ms_shipped = 0
      $query$);
      COMMENT ON FOREIGN TABLE %1$I.columns IS 'columns of SQL Server tables (via tds_fdw)';
   $SQL$, schema, server);

   /* keys (primary and unique) */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.keys CASCADE', schema);
   EXECUTE format($SQL$
      CREATE FOREIGN TABLE %1$I.keys (
         schema          text,
         table_name      text,
         constraint_name text,
         "deferrable"    boolean,
         deferred        boolean,
         column_name     text,
         position        integer,
         is_primary      boolean
      ) SERVER %2$I OPTIONS (query $query$
         SELECT tc.TABLE_SCHEMA AS [schema],
                tc.TABLE_NAME AS [table_name],
                tc.CONSTRAINT_NAME AS [constraint_name],
                CAST(0 AS bit) AS [deferrable],
                CAST(0 AS bit) AS [deferred],
                kcu.COLUMN_NAME AS [column_name],
                CAST(kcu.ORDINAL_POSITION AS int) AS [position],
                CASE WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS [is_primary]
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
         JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
           ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
          AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         JOIN sys.schemas s ON s.name = tc.TABLE_SCHEMA
         JOIN sys.tables st ON st.schema_id = s.schema_id AND st.name = tc.TABLE_NAME
         WHERE tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
           AND st.is_ms_shipped = 0
      $query$);
      COMMENT ON FOREIGN TABLE %1$I.keys IS 'SQL Server primary/unique key columns (via tds_fdw)';
   $SQL$, schema, server);

   /* restore old setting */
   PERFORM set_config('client_min_messages', old_msglevel, true);

   RETURN;
END;
$synchdb_create_sqlserver_objs$;

COMMENT ON FUNCTION synchdb_create_sqlserver_objs(name, name, jsonb) IS
   'create SQL Server foreign tables (tables/columns/keys) via tds_fdw for FDW-based snapshot';

CREATE OR REPLACE FUNCTION synchdb_create_current_sqlserver_lsn_ft(
    p_schema name,  -- e.g. 'sqlserver_obj'
    p_server name   -- e.g. '<connector_name>_sqlserver'
) RETURNS void
LANGUAGE plpgsql
AS $synchdb_create_current_sqlserver_lsn_ft$
DECLARE
  /*
   * sys.fn_cdc_get_max_lsn() returns a varbinary(10). Format it here as Debezium's
   * Lsn.toString() hex-group representation ("%08x:%08x:%04x") so the value returned
   * by this foreign table can be used directly as the commit_lsn offset - this exact
   * shape is already used/observed for the non-FDW SQL Server connector in this
   * project (see doc/docs/en/monitoring/state_view.md).
   */
  v_subqry text := $q$
     SELECT LOWER(
        CONVERT(varchar(8), CONVERT(varbinary(4), SUBSTRING(sys.fn_cdc_get_max_lsn(), 1, 4)), 2)
        + ':' +
        CONVERT(varchar(8), CONVERT(varbinary(4), SUBSTRING(sys.fn_cdc_get_max_lsn(), 5, 4)), 2)
        + ':' +
        CONVERT(varchar(4), CONVERT(varbinary(2), SUBSTRING(sys.fn_cdc_get_max_lsn(), 9, 2)), 2)
     ) AS [max_lsn]
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
   'create SQL Server foreign table to obtain the current CDC max LSN (formatted as Debezium''s Lsn string)';

