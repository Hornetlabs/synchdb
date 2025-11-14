--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION synchdb" to load this file. \quit
 
CREATE OR REPLACE FUNCTION synchdb_start_engine_bgw(name) RETURNS int
AS '$libdir/synchdb', 'synchdb_start_engine_bgw'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_start_engine_bgw(name, name) RETURNS int
AS '$libdir/synchdb', 'synchdb_start_engine_bgw_snapshot_mode'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_stop_engine_bgw(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_get_state() RETURNS SETOF record
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE VIEW synchdb_state_view AS SELECT * FROM synchdb_get_state() AS (name text, connector_type text, pid int, stage text, state text, err text, last_dbz_offset text);

CREATE OR REPLACE FUNCTION synchdb_pause_engine(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_resume_engine(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_set_offset(name, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_conninfo(name, text, int, text, text, text, text, text, text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_restart_connector(name, name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_log_jvm_meminfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_get_stats() RETURNS SETOF record
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_reset_stats(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE VIEW synchdb_genstats AS
SELECT
  name,
  bad_events,
  total_events,
  batches_done,
  average_batch_size,
  first_src_ts,
  first_pg_ts,
  last_src_ts,
  last_pg_ts
FROM synchdb_get_stats() AS (
  name               text,
  ddls               bigint,
  dmls               bigint,
  creates            bigint,
  updates            bigint,
  deletes            bigint,
  txs                bigint,
  truncates          bigint,
  bad_events         bigint,
  total_events       bigint,
  batches_done       bigint,
  average_batch_size bigint,
  first_src_ts       bigint,
  first_pg_ts        bigint,
  last_src_ts        bigint,
  last_pg_ts         bigint,
  tables             bigint,
  rows               bigint,
  snapshot_begin_ts  bigint,
  snapshot_end_ts    bigint
);

CREATE OR REPLACE VIEW synchdb_snapstats AS
SELECT
  name,
  tables,
  rows,
  snapshot_begin_ts,
  snapshot_end_ts
FROM synchdb_get_stats() AS (
  name               text,
  ddls               bigint,
  dmls               bigint,
  creates            bigint,
  updates            bigint,
  deletes            bigint,
  txs                bigint,
  truncates          bigint,
  bad_events         bigint,
  total_events       bigint,
  batches_done       bigint,
  average_batch_size bigint,
  first_src_ts       bigint,
  first_pg_ts        bigint,
  last_src_ts        bigint,
  last_pg_ts         bigint,
  tables             bigint,
  rows               bigint,
  snapshot_begin_ts  bigint,
  snapshot_end_ts    bigint
);

CREATE OR REPLACE VIEW synchdb_cdcstats AS
SELECT
  name,
  ddls,
  dmls,
  creates,
  updates,
  deletes,
  txs,
  truncates
FROM synchdb_get_stats() AS (
  name               text,
  ddls               bigint,
  dmls               bigint,
  creates            bigint,
  updates            bigint,
  deletes            bigint,
  txs                bigint,
  truncates          bigint,
  bad_events         bigint,
  total_events       bigint,
  batches_done       bigint,
  average_batch_size bigint,
  first_src_ts       bigint,
  first_pg_ts        bigint,
  last_src_ts        bigint,
  last_pg_ts         bigint,
  tables             bigint,
  rows               bigint,
  snapshot_begin_ts  bigint,
  snapshot_end_ts    bigint
);

CREATE TABLE IF NOT EXISTS synchdb_conninfo(name TEXT PRIMARY KEY, isactive BOOL, data JSONB);

CREATE TABLE IF NOT EXISTS synchdb_attribute (
    name name,
    type name,
    attrelid oid,
    attnum smallint,
    ext_tbname name,
    ext_attname name,
    ext_atttypename name,
    PRIMARY KEY (name, type, attrelid, attnum)
);

CREATE OR REPLACE FUNCTION synchdb_add_objmap(name, name, name, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_reload_objmap(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE TABLE IF NOT EXISTS synchdb_objmap (
    name name,
    objtype name,
    enabled bool,
    srcobj name,
    dstobj text,
    PRIMARY KEY (name, objtype, srcobj)
);

CREATE VIEW synchdb_att_view AS
    SELECT
        name,
        type,
        synchdb_attribute.attnum,
        ext_tbname,
        (SELECT n.nspname || '.' || c.relname AS table_full_name FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.oid=pg_attribute.attrelid) AS pg_tbname,
        synchdb_attribute.ext_attname,
        pg_attribute.attname AS pg_attname,
        synchdb_attribute.ext_atttypename,
        format_type(pg_attribute.atttypid, NULL) AS pg_atttypename,
        (SELECT dstobj FROM synchdb_objmap WHERE synchdb_objmap.objtype='transform' AND synchdb_objmap.enabled=true AND synchdb_objmap.srcobj = synchdb_attribute.ext_tbname || '.' || synchdb_attribute.ext_attname) AS transform
    FROM synchdb_attribute
    LEFT JOIN pg_attribute
    ON synchdb_attribute.attrelid = pg_attribute.attrelid
    AND synchdb_attribute.attnum = pg_attribute.attnum
    ORDER BY (name, type, ext_tbname, synchdb_attribute.attnum);

CREATE OR REPLACE FUNCTION synchdb_add_extra_conninfo(name, name, text, text, text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_extra_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_objmap(name, name, name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_jmx_conninfo(name, text, int, text, int, bool, text, text, bool, text, text, text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_jmx_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_jmx_exporter_conninfo(name, text, int, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_jmx_exporter_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_olr_conninfo(name, text, int, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_olr_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_infinispan(name, name, int) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_infinispan(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_translate_datatype(name, name, int, int, int) RETURNS text
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_set_snapstats(name, bigint, bigint, bigint, bigint) RETURNS void
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION read_snapshot_table_list(file_uri text)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    file_path text;
    file_content text;
    json_data jsonb;
    table_list text;
BEGIN
    -- strip the leading "file:" prefix
    IF position('file:' IN file_uri) = 1 THEN
        file_path := substr(file_uri, 6);
    ELSE
        RAISE EXCEPTION 'Invalid file URI format: % (must start with file:)', file_uri;
    END IF;

    -- read the file content
    file_content := pg_read_file(file_path);

    -- parse it as JSONB
    json_data := file_content::jsonb;

    -- concatenate the array elements into a comma-separated list
    SELECT string_agg(value::text, ',')
    INTO table_list
    FROM jsonb_array_elements_text(json_data->'snapshot_table_list');

    RETURN table_list;
END;
$$;

/* added for oracle_fdw based initial snapshot */

CREATE OR REPLACE FUNCTION synchdb_prepare_initial_snapshot(
    p_connector_name name,
    p_master_key     text DEFAULT NULL      -- prefer supplying via GUC or parameter, not hardcoding
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_hostname   text;
    v_port       int;
    v_service    text;   -- from data->>'srcdb'
    v_user       text;
    v_pwd        text;   -- decrypted password
    v_server     text := format('%s_oracle', p_connector_name);
    v_dbserver   text;   -- oracle_fdw "dbserver" option, e.g. //host:1521/SERVICE
    v_key        text;
BEGIN
    ----------------------------------------------------------------------
    -- 0) Determine the master key WITHOUT hardcoding
    --    Priority: explicit param > GUC synchdb.master_key
    ----------------------------------------------------------------------
	v_key := p_master_key;

    IF v_key IS NULL OR v_key = '' THEN
        RAISE EXCEPTION 'Master key not provided.';
    END IF;

    ----------------------------------------------------------------------
    -- 1) Ensure required extensions exist (oracle_fdw and pgcrypto)
    ----------------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'oracle_fdw') THEN
        RAISE NOTICE 'oracle_fdw not found; attempting CREATE EXTENSION';
        BEGIN
            EXECUTE 'CREATE EXTENSION oracle_fdw';
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'Failed to install oracle_fdw: % [%]', SQLERRM, SQLSTATE
                USING HINT = 'Install oracle_fdw (and Oracle client libs) as a superuser, then retry.';
        END;
    END IF;

    ----------------------------------------------------------------------
    -- 2) Fetch connector info and DECRYPT password
    ----------------------------------------------------------------------
    SELECT
        data->>'hostname',
        NULLIF(data->>'port','')::int,
        lower(data->>'srcdb'),          -- service/SID
        data->>'user',
        pgp_sym_decrypt((data->>'pwd')::bytea, v_key)
    INTO v_hostname, v_port, v_service, v_user, v_pwd
    FROM synchdb_conninfo
    WHERE name = p_connector_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'No row in synchdb_conninfo for connector name %', p_connector_name;
    END IF;

    IF v_hostname IS NULL OR v_hostname = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data.hostname is missing', p_connector_name;
    END IF;
    IF v_service IS NULL OR v_service = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data.srcdb (service/SID) is missing', p_connector_name;
    END IF;
    IF v_user IS NULL OR v_user = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data.user is missing', p_connector_name;
    END IF;
    IF v_pwd IS NULL OR v_pwd = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: decrypted password is empty or invalid; check key and ciphertext', p_connector_name;
    END IF;

    v_port := COALESCE(v_port, 1521);
    v_dbserver := format('//%s:%s/%s', v_hostname, v_port, v_service);

    RAISE NOTICE 'Using dbserver=%', v_dbserver;

    ----------------------------------------------------------------------
    -- 3) Recreate FDW server and user mapping with fresh info
    ----------------------------------------------------------------------
    EXECUTE format('DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER %I', v_server);
    EXECUTE format('DROP SERVER IF EXISTS %I CASCADE', v_server);

    EXECUTE format(
        'CREATE SERVER %I FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver %L)',
        v_server, v_dbserver
    );

    EXECUTE format(
        'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (user %L, password %L)',
        v_server, v_user, v_pwd
    );

    RAISE NOTICE 'Created server % and user mapping for CURRENT_USER', v_server;

    RETURN v_server;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'synchdb_prepare_initial_snapshot(%) failed: % [%]',
                        p_connector_name, SQLERRM, SQLSTATE
            USING HINT = 'Verify oracle_fdw/pgcrypto are installed and synchdb_conninfo JSON fields (hostname, port, srcdb, user, pwd) are valid; also ensure the master key is correct.';
END;
$$;

COMMENT ON FUNCTION synchdb_prepare_initial_snapshot(name, text) IS
   'check oracle_fdw and prepare foreign server and user mapping objects';

CREATE FUNCTION synchdb_create_oraviews(
   server      name,
   schema      name    DEFAULT NAME 'public',
   options     jsonb   DEFAULT NULL
) RETURNS void
   LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT SET search_path = pg_catalog AS
$$DECLARE
   old_msglevel text;
   v_max_long   integer := 32767;

   sys_schemas text :=
      E'''''ANONYMOUS'''', ''''APEX_PUBLIC_USER'''', ''''APEX_030200'''', ''''APEX_040000'''',\n'
      '         ''''APEX_050000'''', ''''APPQOSSYS'''', ''''AUDSYS'''', ''''AURORA$JIS$UTILITY$'''',\n'
      '         ''''AURORA$ORB$UNAUTHENTICATED'''', ''''CTXSYS'''', ''''DBSFWUSER'''', ''''DBSNMP'''',\n'
      '         ''''DIP'''', ''''DMSYS'''', ''''DVSYS'''', ''''DVF'''', ''''EXFSYS'''',\n'
      '         ''''FLOWS_30000'''', ''''FLOWS_FILES'''', ''''GDOSYS'''', ''''GGSYS'''',\n'
      '         ''''GSMADMIN_INTERNAL'''', ''''GSMCATUSER'''', ''''GSMUSER'''', ''''LBACSYS'''',\n'
      '         ''''MDDATA'''', ''''MDSYS'''', ''''MGMT_VIEW'''', ''''ODM'''', ''''ODM_MTR'''',\n'
      '         ''''OJVMSYS'''', ''''OLAPSYS'''', ''''ORACLE_OCM'''', ''''ORDDATA'''',\n'
      '         ''''ORDPLUGINS'''', ''''ORDSYS'''', ''''OSE$HTTP$ADMIN'''', ''''OUTLN'''',\n'
      '         ''''PDBADMIN'''', ''''REMOTE_SCHEDULER_AGENT'''', ''''SI_INFORMTN_SCHEMA'''',\n'
      '         ''''SPATIAL_WFS_ADMIN_USR'''', ''''SPATIAL_CSW_ADMIN_USR'''', ''''SPATIAL_WFS_ADMIN_USR'''',\n'
      '         ''''SYS'''', ''''SYS$UMF'''', ''''SYSBACKUP'''', ''''SYSDG'''', ''''SYSKM'''',\n'
      '         ''''SYSMAN'''', ''''SYSRAC'''', ''''SYSTEM'''', ''''TRACESRV'''',\n'
      '         ''''MTSSYS'''', ''''OASPUBLIC'''', ''''OLAPSYS'''', ''''OWBSYS'''', ''''OWBSYS_AUDIT'''',\n'
      '         ''''PERFSTAT'''', ''''WEBSYS'''', ''''WK_PROXY'''', ''''WKSYS'''', ''''WK_TEST'''',\n'
      '         ''''WMSYS'''', ''''XDB'''', ''''XS$NULL''''';

   tables_sql text := E'CREATE FOREIGN TABLE %I.tables (\n'
      '   schema     text NOT NULL,\n'
      '   table_name text NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT owner,\n'
         '       table_name\n'
         'FROM dba_tables\n'
         'WHERE temporary = ''''N''''\n'
         '  AND secondary = ''''N''''\n'
         '  AND nested    = ''''NO''''\n'
         '  AND dropped   = ''''NO''''\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT owner, mview_name\n'
         '             FROM dba_mviews)\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT log_owner, log_table\n'
         '             FROM dba_mview_logs)\n'
         '  AND owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   columns_sql text := E'CREATE FOREIGN TABLE %I.columns (\n'
      '   schema        text    NOT NULL,\n'
      '   table_name    text    NOT NULL,\n'
      '   column_name   text    NOT NULL,\n'
      '   position      integer NOT NULL,\n'
      '   type_name     text    NOT NULL,\n'
      '   length        integer NOT NULL,\n'
      '   precision     integer,\n'
      '   scale         integer,\n'
      '   nullable      boolean NOT NULL,\n'
      '   default_value text\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT col.owner,\n'
         '       col.table_name,\n'
         '       col.column_name,\n'
         '       col.column_id,\n'
         '       CASE WHEN col.data_type_owner IS NULL\n'
         '            THEN col.data_type\n'
         '            ELSE col.data_type_owner || ''''.'''' || col.data_type\n'
         '       END data_type,\n'
         '       col.char_length,\n'
         '       col.data_precision,\n'
         '       col.data_scale,\n'
         '       CASE WHEN col.nullable = ''''Y'''' THEN 1 ELSE 0 END AS nullable,\n'
         '       col.data_default\n'
         'FROM dba_tab_columns col\n'
         '   JOIN (SELECT owner, table_name\n'
         '            FROM dba_tables\n'
         '            WHERE owner NOT IN (' || sys_schemas || E')\n'
         '              AND temporary = ''''N''''\n'
         '              AND secondary = ''''N''''\n'
         '              AND nested    = ''''NO''''\n'
         '              AND dropped   = ''''NO''''\n'
         '         UNION SELECT owner, view_name\n'
         '            FROM dba_views\n'
         '            WHERE owner NOT IN (' || sys_schemas || E')\n'
         '        ) tab\n'
         '      ON tab.owner = col.owner AND tab.table_name = col.table_name\n'
         'WHERE (col.owner, col.table_name)\n'
         '     NOT IN (SELECT owner, mview_name\n'
         '             FROM dba_mviews)\n'
         '  AND (col.owner, col.table_name)\n'
         '     NOT IN (SELECT log_owner, log_table\n'
         '             FROM dba_mview_logs)\n'
         '  AND col.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   checks_sql text := E'CREATE FOREIGN TABLE %I.checks (\n'
      '   schema          text    NOT NULL,\n'
      '   table_name      text    NOT NULL,\n'
      '   constraint_name text    NOT NULL,\n'
      '   "deferrable"    boolean NOT NULL,\n'
      '   deferred        boolean NOT NULL,\n'
      '   condition       text    NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT con.owner,\n'
         '       con.table_name,\n'
         '       con.constraint_name,\n'
         '       CASE WHEN con.deferrable = ''''DEFERRABLE'''' THEN 1 ELSE 0 END deferrable,\n'
         '       CASE WHEN con.deferred   = ''''DEFERRED''''   THEN 1 ELSE 0 END deferred,\n'
         '       con.search_condition\n'
         'FROM dba_constraints con\n'
         '   JOIN dba_tables tab\n'
         '      ON tab.owner = con.owner AND tab.table_name = con.table_name\n'
         'WHERE tab.temporary = ''''N''''\n'
         '  AND tab.secondary = ''''N''''\n'
         '  AND tab.nested    = ''''NO''''\n'
         '  AND tab.dropped   = ''''NO''''\n'
         '  AND con.constraint_type = ''''C''''\n'
         '  AND con.status          = ''''ENABLED''''\n'
         '  AND con.validated       = ''''VALIDATED''''\n'
         '  AND con.invalid         IS NULL\n'
         '  AND con.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   foreign_keys_sql text := E'CREATE FOREIGN TABLE %I.foreign_keys (\n'
      '   schema          text    NOT NULL,\n'
      '   table_name      text    NOT NULL,\n'
      '   constraint_name text    NOT NULL,\n'
      '   "deferrable"    boolean NOT NULL,\n'
      '   deferred        boolean NOT NULL,\n'
      '   delete_rule     text    NOT NULL,\n'
      '   column_name     text    NOT NULL,\n'
      '   position        integer NOT NULL,\n'
      '   remote_schema   text    NOT NULL,\n'
      '   remote_table    text    NOT NULL,\n'
      '   remote_column   text    NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT con.owner,\n'
         '       con.table_name,\n'
         '       con.constraint_name,\n'
         '       CASE WHEN con.deferrable = ''''DEFERRABLE'''' THEN 1 ELSE 0 END deferrable,\n'
         '       CASE WHEN con.deferred   = ''''DEFERRED''''   THEN 1 ELSE 0 END deferred,\n'
         '       con.delete_rule,\n'
         '       col.column_name,\n'
         '       col.position,\n'
         '       r_col.owner AS remote_schema,\n'
         '       r_col.table_name AS remote_table,\n'
         '       r_col.column_name AS remote_column\n'
         'FROM dba_constraints con\n'
         '   JOIN dba_cons_columns col\n'
         '      ON con.owner = col.owner AND con.table_name = col.table_name AND con.constraint_name = col.constraint_name\n'
         '   JOIN dba_cons_columns r_col\n'
         '      ON con.r_owner = r_col.owner AND con.r_constraint_name = r_col.constraint_name AND col.position = r_col.position\n'
         'WHERE con.constraint_type = ''''R''''\n'
         '  AND con.status          = ''''ENABLED''''\n'
         '  AND con.validated       = ''''VALIDATED''''\n'
         '  AND con.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   keys_sql text := E'CREATE FOREIGN TABLE %I.keys (\n'
      '   schema          text    NOT NULL,\n'
      '   table_name      text    NOT NULL,\n'
      '   constraint_name text    NOT NULL,\n'
      '   "deferrable"    boolean NOT NULL,\n'
      '   deferred        boolean NOT NULL,\n'
      '   column_name     text    NOT NULL,\n'
      '   position        integer NOT NULL,\n'
      '   is_primary      boolean NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT con.owner,\n'
         '       con.table_name,\n'
         '       con.constraint_name,\n'
         '       CASE WHEN deferrable = ''''DEFERRABLE'''' THEN 1 ELSE 0 END deferrable,\n'
         '       CASE WHEN deferred   = ''''DEFERRED''''   THEN 1 ELSE 0 END deferred,\n'
         '       col.column_name,\n'
         '       col.position,\n'
         '       CASE WHEN con.constraint_type = ''''P'''' THEN 1 ELSE 0 END is_primary\n'
         'FROM dba_tables tab\n'
         '   JOIN dba_constraints con\n'
         '      ON tab.owner = con.owner AND tab.table_name = con.table_name\n'
         '   JOIN dba_cons_columns col\n'
         '      ON con.owner = col.owner AND con.table_name = col.table_name AND con.constraint_name = col.constraint_name\n'
         'WHERE (con.owner, con.table_name)\n'
         '     NOT IN (SELECT owner, mview_name\n'
         '             FROM dba_mviews)\n'
         '  AND con.constraint_type IN (''''P'''', ''''U'''')\n'
         '  AND con.status    = ''''ENABLED''''\n'
         '  AND con.validated = ''''VALIDATED''''\n'
         '  AND tab.temporary = ''''N''''\n'
         '  AND tab.secondary = ''''N''''\n'
         '  AND tab.nested    = ''''NO''''\n'
         '  AND tab.dropped   = ''''NO''''\n'
         '  AND con.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   views_sql text := E'CREATE FOREIGN TABLE %I.views (\n'
      '   schema     text NOT NULL,\n'
      '   view_name  text NOT NULL,\n'
      '   definition text NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT owner,\n'
         '       view_name,\n'
         '       text\n'
         'FROM dba_views\n'
         'WHERE owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   func_src_sql text := E'CREATE FOREIGN TABLE %I.func_src (\n'
      '   schema        text    NOT NULL,\n'
      '   function_name text    NOT NULL,\n'
      '   is_procedure  boolean NOT NULL,\n'
      '   line_number   integer NOT NULL,\n'
      '   line          text    NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT pro.owner,\n'
         '       pro.object_name,\n'
         '       CASE WHEN pro.object_type = ''''PROCEDURE'''' THEN 1 ELSE 0 END is_procedure,\n'
         '       src.line,\n'
         '       src.text\n'
         'FROM dba_procedures pro\n'
         '   JOIN dba_source src\n'
         '      ON pro.owner = src.owner\n'
         '         AND pro.object_name = src.name\n'
         '         AND pro.object_type = src.type\n'
         'WHERE pro.object_type IN (''''FUNCTION'''', ''''PROCEDURE'''')\n'
         '  AND pro.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   functions_sql text := E'CREATE VIEW %I.functions AS\n'
      'SELECT schema,\n'
      '       function_name,\n'
      '       is_procedure,\n'
      '       string_agg(line, TEXT '''' ORDER BY line_number) AS source\n'
      'FROM %I.func_src\n'
      'GROUP BY schema, function_name, is_procedure';

   sequences_sql text := E'CREATE FOREIGN TABLE %I.sequences (\n'
      '   schema        text        NOT NULL,\n'
      '   sequence_name text        NOT NULL,\n'
      '   min_value     numeric(28),\n'
      '   max_value     numeric(28),\n'
      '   increment_by  numeric(28) NOT NULL,\n'
      '   cyclical      boolean     NOT NULL,\n'
      '   cache_size    integer     NOT NULL,\n'
      '   last_value    numeric(28) NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT sequence_owner,\n'
         '       sequence_name,\n'
         '       min_value,\n'
         '       max_value,\n'
         '       increment_by,\n'
         '       CASE WHEN cycle_flag = ''''Y'''' THEN 1 ELSE 0 END cyclical,\n'
         '       cache_size,\n'
         '       last_number\n'
         'FROM dba_sequences\n'
         'WHERE sequence_owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   index_exp_sql text := E'CREATE FOREIGN TABLE %I.index_exp (\n'
      '   schema         text    NOT NULL,\n'
      '   table_name     text    NOT NULL,\n'
      '   index_name     text    NOT NULL,\n'
      '   uniqueness     boolean NOT NULL,\n'
      '   position       integer NOT NULL,\n'
      '   descend        boolean NOT NULL,\n'
      '   col_name       text    NOT NULL,\n'
      '   col_expression text\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT ic.table_owner,\n'
         '       ic.table_name,\n'
         '       ic.index_name,\n'
         '       CASE WHEN i.uniqueness = ''''UNIQUE'''' THEN 1 ELSE 0 END uniqueness,\n'
         '       ic.column_position,\n'
         '       CASE WHEN ic.descend = ''''DESC'''' THEN 1 ELSE 0 END descend,\n'
         '       ic.column_name,\n'
         '       ie.column_expression\n'
         'FROM dba_indexes i,\n'
         '     dba_tables t,\n'
         '     dba_ind_columns ic,\n'
         '     dba_ind_expressions ie\n'
         'WHERE i.table_owner      = t.owner\n'
         '  AND i.table_name       = t.table_name\n'
         '  AND i.owner            = ic.index_owner\n'
         '  AND i.index_name       = ic.index_name\n'
         '  AND i.table_owner      = ic.table_owner\n'
         '  AND i.table_name       = ic.table_name\n'
         '  AND ic.index_owner     = ie.index_owner(+)\n'
         '  AND ic.index_name      = ie.index_name(+)\n'
         '  AND ic.table_owner     = ie.table_owner(+)\n'
         '  AND ic.table_name      = ie.table_name(+)\n'
         '  AND ic.column_position = ie.column_position(+)\n'
         '  AND t.temporary        = ''''N''''\n'
         '  AND t.secondary        = ''''N''''\n'
         '  AND t.nested           = ''''NO''''\n'
         '  AND t.dropped          = ''''NO''''\n'
         '  AND i.index_type NOT IN (''''LOB'''', ''''DOMAIN'''')\n'
         '  AND coalesce(i.dropped, ''''NO'''') = ''''NO''''\n'
         '  AND NOT EXISTS (SELECT 1  /* exclude constraint indexes */\n'
         '                  FROM dba_constraints c\n'
         '                  WHERE c.owner = i.table_owner\n'
         '                    AND c.table_name = i.table_name\n'
         '                    AND COALESCE(c.index_owner, i.owner) = i.owner\n'
         '                    AND c.index_name = i.index_name)\n'
         '  AND NOT EXISTS (SELECT 1  /* exclude materialized views */\n'
         '                  FROM dba_mviews m\n'
         '                  WHERE m.owner = i.table_owner\n'
         '                    AND m.mview_name = i.table_name)\n'
         '  AND NOT EXISTS (SELECT 1  /* exclude materialized views logs */\n'
         '                  FROM dba_mview_logs ml\n'
         '                  WHERE ml.log_owner = i.table_owner\n'
         '                    AND ml.log_table = i.table_name)\n'
         '  AND ic.table_owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   index_columns_sql text := E'CREATE VIEW %I.index_columns AS\n'
      'SELECT schema,\n'
      '       table_name,\n'
      '       index_name,\n'
      '       position,\n'
      '       descend,\n'
      '       col_expression IS NOT NULL\n'
      '          AND (NOT descend OR col_expression !~ ''^"[^"]*"$'') AS is_expression,\n'
      '       coalesce(\n'
      '          CASE WHEN descend AND col_expression ~ ''^"[^"]*"$''\n'
      '               THEN replace (col_expression, ''"'', '''')\n'
      '               ELSE col_expression\n'
      '          END,\n'
      '          col_name) AS column_name\n'
      'FROM %I.index_exp';

   indexes_sql text := E'CREATE VIEW %I.indexes AS\n'
      'SELECT DISTINCT\n'
      '       schema,\n'
      '       table_name,\n'
      '       index_name,\n'
      '       uniqueness\n'
      'FROM %I.index_exp';

   partition_cols_sql text := E'CREATE FOREIGN TABLE %I.partition_columns (\n'
      '   schema          text NOT NULL,\n'
      '   table_name      text NOT NULL,\n'
      '   partition_name  text NOT NULL,\n'
      '   column_name     text NOT NULL,\n'
      '   column_position integer NOT NULL,\n'
      '   type            text NOT NULL,\n'
      '   position        integer NOT NULL,\n'
      '   values          text\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT part.table_owner,\n'
         '       part.table_name,\n'
         '       part.partition_name,\n'
         '       cols.column_name,\n'
         '       cols.column_position,\n'
         '       info.partitioning_type,\n'
         '       part.partition_position,\n'
         '       part.high_value\n'
         'FROM dba_tab_partitions part\n'
         '   JOIN dba_part_tables info\n'
         '      ON part.table_owner = info.owner\n'
         '         AND part.table_name = info.table_name\n'
         '   JOIN dba_part_key_columns cols\n'
         '      ON part.table_owner = cols.owner\n'
         '         AND part.table_name = cols.name\n'
         'WHERE part.table_owner NOT IN (' || sys_schemas || E')\n'
         '   AND cols.object_type = ''''TABLE''''\n'
         '   AND info.partitioning_type IN (''''LIST'''', ''''RANGE'''', ''''HASH'''')\n'
      ')'', max_long ''%s'', readonly ''true'')';

   partitions_sql text := E'CREATE VIEW %1$I.partitions AS\n'
      'WITH catalog AS (\n'
      '   SELECT schema,\n'
      '          table_name,\n'
      '          partition_name,\n'
      '          string_agg(column_name, TEXT '', '' ORDER BY column_position) AS key,\n'
      '          type, position, values,\n'
      '          count(column_name) AS colcount\n'
      '   FROM %1$I.partition_columns\n'
      '   GROUP BY schema, table_name, partition_name,\n'
      '         type, position, values\n'
      '), list_partitions AS (\n'
      '   SELECT schema, table_name, partition_name, type, key,\n'
      '      coalesce(values = ''DEFAULT'', FALSE) AS is_default,\n'
      '      string_to_array(values, '','') AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''LIST''\n'
      '     AND colcount = 1\n'
      '), range_partitions AS (\n'
      '   SELECT schema, table_name, partition_name, type, key, FALSE,\n'
      '      ARRAY[\n'
      '         lag(values, 1, ''MINVALUE'')\n'
      '            OVER (PARTITION BY schema, table_name\n'
      '                  ORDER BY position),\n'
      '         values\n'
      '      ] AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''RANGE''\n'
      '     AND colcount = 1\n'
      '), hash_partitions AS (\n'
      '   SELECT schema, table_name, partition_name, type, key, FALSE,\n'
      '      ARRAY[(position - 1)::text] AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''HASH''\n'
      ')\n'
      'SELECT * FROM list_partitions\n'
      'UNION ALL SELECT * FROM range_partitions\n'
      'UNION ALL SELECT * FROM hash_partitions';

   subpartition_cols_sql text := E'CREATE FOREIGN TABLE %I.subpartition_columns (\n'
      '   schema            text NOT NULL,\n'
      '   table_name        text NOT NULL,\n'
      '   partition_name    text NOT NULL,\n'
      '   subpartition_name text NOT NULL,\n'
      '   column_name       text NOT NULL,\n'
      '   column_position   integer NOT NULL,\n'
      '   type              text NOT NULL,\n'
      '   position          integer NOT NULL,\n'
      '   values            text\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT part.table_owner,\n'
         '       part.table_name,\n'
         '       part.partition_name,\n'
         '       part.subpartition_name,\n'
         '       cols.column_name,\n'
         '       cols.column_position,\n'
         '       info.subpartitioning_type,\n'
         '       part.subpartition_position,\n'
         '       part.high_value\n'
         'FROM dba_tab_subpartitions part\n'
         '   JOIN dba_part_tables info\n'
         '      ON part.table_owner = info.owner\n'
         '         AND part.table_name = info.table_name\n'
         '   JOIN dba_subpart_key_columns cols\n'
         '      ON part.table_owner = cols.owner\n'
         '         AND part.table_name = cols.name\n'
         'WHERE part.table_owner NOT IN (' || sys_schemas || E')\n'
         '   AND cols.object_type = ''''TABLE''''\n'
         '   AND info.partitioning_type IN (''''LIST'''', ''''RANGE'''', ''''HASH'''')\n'
         '   AND info.subpartitioning_type IN (''''LIST'''', ''''RANGE'''', ''''HASH'''')\n'
      ')'', max_long ''%s'', readonly ''true'')';

   subpartitions_sql text := E'CREATE VIEW %1$I.subpartitions AS\n'
      'WITH catalog AS (\n'
      '   SELECT schema,\n'
      '          table_name,\n'
      '          partition_name,\n'
      '          subpartition_name,\n'
      '          string_agg(column_name, TEXT '', '' ORDER BY column_position) AS key,\n'
      '          type, position, values,\n'
      '          count(column_name) AS colcount\n'
      '   FROM %1$I.subpartition_columns \n'
      '   GROUP BY schema, table_name, partition_name, subpartition_name,\n'
      '            type, position, values'
      '), list_subpartitions AS (\n'
      '   SELECT schema, table_name, partition_name, subpartition_name, type, key,\n'
      '      coalesce(values = ''DEFAULT'', FALSE) AS is_default,\n'
      '      string_to_array(values, '','') AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''LIST''\n'
      '     AND colcount = 1\n'
      '), range_subpartitions AS (\n'
      '   SELECT schema, table_name, partition_name, subpartition_name, type, key, FALSE,\n'
      '      ARRAY[\n'
      '         lag(values, 1, ''MINVALUE'')\n'
      '            OVER (PARTITION BY schema, table_name, partition_name\n'
      '                  ORDER BY position),\n'
      '         values\n'
      '      ] AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''RANGE''\n'
      '     AND colcount = 1\n'
      '), hash_subpartitions AS (\n'
      '   SELECT schema, table_name, partition_name, subpartition_name, type, key, FALSE,\n'
      '      ARRAY[(position - 1)::text] AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''HASH'''
      ')\n'
      'SELECT * FROM list_subpartitions\n'
      'UNION ALL SELECT * FROM range_subpartitions\n'
      'UNION ALL SELECT * FROM hash_subpartitions';

   schemas_sql text := E'CREATE FOREIGN TABLE %I.schemas (\n'
      '   schema text NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT username\n'
         'FROM dba_users\n'
         'WHERE username NOT IN( ' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   trig_sql text := E'CREATE FOREIGN TABLE %I.trig (\n'
      '   schema            text NOT NULL,\n'
      '   table_name        text NOT NULL,\n'
      '   trigger_name      text NOT NULL,\n'
      '   trigger_type      text NOT NULL,\n'
      '   triggering_event  text NOT NULL,\n'
      '   when_clause       text,\n'
      '   referencing_names text NOT NULL,\n'
      '   trigger_body      text NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT table_owner,\n'
         '       table_name,\n'
         '       trigger_name,\n'
         '       trigger_type,\n'
         '       triggering_event,\n'
         '       when_clause,\n'
         '       referencing_names,\n'
         '       trigger_body\n'
         'FROM dba_triggers\n'
         'WHERE table_owner NOT IN( ' || sys_schemas || E')\n'
         '  AND base_object_type IN (''''TABLE'''', ''''VIEW'''')\n'
         '  AND status = ''''ENABLED''''\n'
         '  AND crossedition = ''''NO''''\n'
         '  AND trigger_type <> ''''COMPOUND'''''
      ')'', max_long ''%s'', readonly ''true'')';

   triggers_sql text := E'CREATE VIEW %I.triggers AS\n'
      'SELECT schema,\n'
      '       table_name,\n'
      '       trigger_name,\n'
      '       CASE WHEN trigger_type LIKE ''BEFORE %%''\n'
      '            THEN ''BEFORE''\n'
      '            WHEN trigger_type LIKE ''AFTER %%''\n'
      '            THEN ''AFTER''\n'
      '            ELSE trigger_type\n'
      '       END AS trigger_type,\n'
      '       triggering_event,\n'
      '       trigger_type LIKE ''%%EACH ROW'' AS for_each_row,\n'
      '       when_clause,\n'
      '       referencing_names,\n'
      '       trigger_body\n'
      'FROM %I.trig';

   pack_src_sql text := E'CREATE FOREIGN TABLE %I.pack_src (\n'
      '   schema       text    NOT NULL,\n'
      '   package_name text    NOT NULL,\n'
      '   src_type     text    NOT NULL,\n'
      '   line_number  integer NOT NULL,\n'
      '   line         text    NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT pro.owner,\n'
         '       pro.object_name,\n'
         '       src.type,\n'
         '       src.line,\n'
         '       src.text\n'
         'FROM dba_procedures pro\n'
         '   JOIN dba_source src\n'
         '      ON pro.owner = src.owner\n'
         '         AND pro.object_name = src.name\n'
         'WHERE pro.object_type = ''''PACKAGE''''\n'
         '  AND src.type IN (''''PACKAGE'''', ''''PACKAGE BODY'''')\n'
         '  AND procedure_name IS NULL\n'
         '  AND pro.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   packages_sql text := E'CREATE VIEW %I.packages AS\n'
      'SELECT schema,\n'
      '       package_name,\n'
      '       src_type = ''PACKAGE BODY'' AS is_body,\n'
      '       string_agg(line, TEXT '''' ORDER BY line_number) AS source\n'
      'FROM %I.pack_src\n'
      'GROUP BY schema, package_name, src_type';

   table_privs_sql text := E'CREATE FOREIGN TABLE %I.table_privs (\n'
      '   schema     text    NOT NULL,\n'
      '   table_name text    NOT NULL,\n'
      '   privilege  text    NOT NULL,\n'
      '   grantor    text    NOT NULL,\n'
      '   grantee    text    NOT NULL,\n'
      '   grantable  boolean NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT owner,\n'
         '       table_name,\n'
         '       p.privilege,\n'
         '       p.grantor,\n'
         '       p.grantee,\n'
         '       CASE WHEN p.grantable = ''''YES'''' THEN 1 ELSE 0 END grantable\n'
         'FROM dba_tab_privs p\n'
         '   JOIN dba_tables t USING (owner, table_name)\n'
         'WHERE t.temporary = ''''N''''\n'
         '  AND t.secondary = ''''N''''\n'
         '  AND t.nested    = ''''NO''''\n'
         '  AND coalesce(t.dropped, ''''NO'''') = ''''NO''''\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT owner, mview_name\n'
         '             FROM dba_mviews)\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT log_owner, log_table\n'
         '             FROM dba_mview_logs)\n'
         '  AND owner NOT IN (' || sys_schemas || E')\n'
         '  AND p.grantor NOT IN (' || sys_schemas || E')\n'
         '  AND p.grantee NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   column_privs_sql text := E'CREATE FOREIGN TABLE %I.column_privs (\n'
      '   schema      text    NOT NULL,\n'
      '   table_name  text    NOT NULL,\n'
      '   column_name text    NOT NULL,\n'
      '   privilege   text    NOT NULL,\n'
      '   grantor     text    NOT NULL,\n'
      '   grantee     text    NOT NULL,\n'
      '   grantable   boolean NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT owner,\n'
         '       table_name,\n'
         '       c.column_name,\n'
         '       c.privilege,\n'
         '       c.grantor,\n'
         '       c.grantee,\n'
         '       CASE WHEN c.grantable = ''''YES'''' THEN 1 ELSE 0 END grantable\n'
         'FROM dba_col_privs c\n'
         '   JOIN dba_tables t USING (owner, table_name)\n'
         'WHERE t.temporary = ''''N''''\n'
         '  AND t.secondary = ''''N''''\n'
         '  AND t.nested    = ''''NO''''\n'
         '  AND t.dropped   = ''''NO''''\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT owner, mview_name\n'
         '             FROM dba_mviews)\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT log_owner, log_table\n'
         '             FROM dba_mview_logs)\n'
         '  AND owner NOT IN (' || sys_schemas || E')\n'
         '  AND c.grantor NOT IN (' || sys_schemas || E')\n'
         '  AND c.grantee NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   segments_sql text := E'CREATE FOREIGN TABLE %I.segments (\n'
      '   schema       text   NOT NULL,\n'
      '   segment_name text   NOT NULL,\n'
      '   segment_type text   NOT NULL,\n'
      '   bytes        bigint NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT owner,\n'
         '       segment_name,\n'
         '       segment_type,\n'
         '       bytes\n'
         'FROM dba_segments\n'
         'WHERE owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   migration_cost_estimate_sql text := E'CREATE VIEW %I.migration_cost_estimate AS\n'
      '   SELECT schema,\n'
      '          ''tables''::text   AS task_type,\n'
      '          count(*)::bigint AS task_content,\n'
      '          ''count''::text    AS task_unit,\n'
      '          ceil(count(*) / 10.0)::integer AS migration_hours\n'
      '   FROM %I.tables\n'
      '   GROUP BY schema\n'
      'UNION ALL\n'
      '   SELECT t.schema,\n'
      '          ''data_migration''::text,\n'
      '          sum(bytes)::bigint,\n'
      '          ''bytes''::text,\n'
      '          ceil(sum(bytes::float8) / 26843545600.0)::integer\n'
      '   FROM %I.segments AS s\n'
      '      JOIN %I.tables AS t\n'
      '         ON s.schema = t.schema\n'
      '            AND s.segment_name = t.table_name\n'
      '   WHERE s.segment_type = ''TABLE''\n'
      '   GROUP BY t.schema\n'
      'UNION ALL\n'
      '   SELECT schema,\n'
      '          ''functions'',\n'
      '          coalesce(sum(octet_length(source)), 0),\n'
      '          ''characters''::text,\n'
      '          ceil(coalesce(sum(octet_length(source)), 0) / 512.0)::integer\n'
      '   FROM %I.functions\n'
      '   GROUP BY schema\n'
      'UNION ALL\n'
      '   SELECT schema,\n'
      '          ''triggers'',\n'
      '          coalesce(sum(octet_length(trigger_body)), 0),\n'
      '          ''characters''::text,\n'
      '          ceil(coalesce(sum(octet_length(trigger_body)), 0) / 512.0)::integer\n'
      '   FROM %I.triggers\n'
      '   GROUP BY schema\n'
      'UNION ALL\n'
      '   SELECT schema,\n'
      '          ''packages'',\n'
      '          coalesce(sum(octet_length(source)), 0),\n'
      '          ''characters''::text,\n'
      '          ceil(coalesce(sum(octet_length(source)), 0) / 512.0)::integer\n'
      '   FROM %I.packages\n'
      '   WHERE is_body\n'
      '   GROUP BY schema\n'
      'UNION ALL\n'
      '   SELECT schema,\n'
      '          ''views'',\n'
      '          coalesce(sum(octet_length(definition)), 0),\n'
      '          ''characters''::text,\n'
      '          ceil(coalesce(sum(octet_length(definition)), 0) / 512.0)::integer\n'
      '   FROM %I.views\n'
      '   GROUP BY schema';

   test_error_sql text := E'CREATE TABLE %I.test_error (\n'
      '   log_time   timestamp with time zone NOT NULL DEFAULT current_timestamp,\n'
      '   schema     name                     NOT NULL,\n'
      '   table_name name                     NOT NULL,\n'
      '   rowid      text                     NOT NULL,\n'
      '   message    text                     NOT NULL,\n'
      '   PRIMARY KEY (schema, table_name, log_time, rowid)\n'
      ')';

   test_error_stats_sql text := E'CREATE TABLE %I.test_error_stats (\n'
      '   log_time   timestamp with time zone NOT NULL,\n'
      '   schema     name                     NOT NULL,\n'
      '   table_name name                     NOT NULL,\n'
      '   errcount   bigint                   NOT NULL,\n'
      '   PRIMARY KEY (schema, table_name, log_time)\n'
      ')';

BEGIN
   /* remember old setting */
   old_msglevel := current_setting('client_min_messages');
   /* make the output less verbose */
   SET LOCAL client_min_messages = warning;

   IF options ? 'max_long' THEN
      v_max_long := (options->>'max_long')::integer;
   END IF;

   /* CREATE SCHEMA IF NEEDED */
   EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema);

   /* tables */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.tables', schema);
   EXECUTE format(tables_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.tables IS ''Oracle tables on foreign server "%I"''', schema, server);
   /* columns */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.columns', schema);
   EXECUTE format(columns_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.columns IS ''columns of Oracle tables and views on foreign server "%I"''', schema, server);
   /* checks */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.checks', schema);
   EXECUTE format(checks_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.checks IS ''Oracle check constraints on foreign server "%I"''', schema, server);
   /* foreign_keys */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.foreign_keys', schema);
   EXECUTE format(foreign_keys_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.foreign_keys IS ''Oracle foreign key columns on foreign server "%I"''', schema, server);
   /* keys */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.keys', schema);
   EXECUTE format(keys_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.keys IS ''Oracle primary and unique key columns on foreign server "%I"''', schema, server);
   /* views */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.views', schema);
   EXECUTE format(views_sql, schema, server, v_max_long);
   /* func_src and functions */
   EXECUTE format('DROP VIEW IF EXISTS %I.functions', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.func_src', schema);
   EXECUTE format(func_src_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.func_src IS ''source lines for Oracle functions and procedures on foreign server "%I"''', schema, server);
   EXECUTE format(functions_sql, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.functions IS ''Oracle functions and procedures on foreign server "%I"''', schema, server);
   /* sequences */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.sequences', schema);
   EXECUTE format(sequences_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.sequences IS ''Oracle sequences on foreign server "%I"''', schema, server);
   /* index_exp and index_columns */
   EXECUTE format('DROP VIEW IF EXISTS %I.index_columns', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.index_exp', schema);
   EXECUTE format(index_exp_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.index_exp IS ''Oracle index columns on foreign server "%I"''', schema, server);
   EXECUTE format(index_columns_sql, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.index_columns IS ''Oracle index columns on foreign server "%I"''', schema, server);
   /* indexes */
   EXECUTE format('DROP VIEW IF EXISTS %I.indexes', schema);
   EXECUTE format(indexes_sql, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.indexes IS ''Oracle indexes on foreign server "%I"''', schema, server);
   /* partitions and subpartitions */
   EXECUTE format('DROP VIEW IF EXISTS %I.partitions', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.partition_columns', schema);
   EXECUTE format(partition_cols_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.partition_columns IS ''Oracle partition columns on foreign server "%I"''', schema, server);
   EXECUTE format(partitions_sql, schema);
   EXECUTE format('COMMENT ON VIEW %I.partitions IS ''Oracle partitions on foreign server "%I"''', schema, server);
   EXECUTE format('DROP VIEW IF EXISTS %I.subpartitions', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.subpartition_columns', schema);
   EXECUTE format(subpartition_cols_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.subpartition_columns IS ''Oracle subpartition columns on foreign server "%I"''', schema, server);
   EXECUTE format(subpartitions_sql, schema);
   EXECUTE format('COMMENT ON VIEW %I.subpartitions IS ''Oracle subpartitions on foreign server "%I"''', schema, server);
   /* schemas */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.schemas', schema);
   EXECUTE format(schemas_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.schemas IS ''Oracle schemas on foreign server "%I"''', schema, server);
   /* trig and triggers */
   EXECUTE format('DROP VIEW IF EXISTS %I.triggers', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.trig', schema);
   EXECUTE format(trig_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.trig IS ''Oracle triggers on foreign server "%I"''', schema, server);
   EXECUTE format(triggers_sql, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.triggers IS ''Oracle triggers on foreign server "%I"''', schema, server);
   /* pack_src and packages */
   EXECUTE format('DROP VIEW IF EXISTS %I.packages', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.pack_src', schema);
   EXECUTE format(pack_src_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.pack_src IS ''Oracle package source lines on foreign server "%I"''', schema, server);
   EXECUTE format(packages_sql, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.packages IS ''Oracle packages on foreign server "%I"''', schema, server);
   /* table_privs */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.table_privs', schema);
   EXECUTE format(table_privs_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.table_privs IS ''Privileges on Oracle tables on foreign server "%I"''', schema, server);
   /* column_privs */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.column_privs', schema);
   EXECUTE format(column_privs_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.column_privs IS ''Privileges on Oracle table columns on foreign server "%I"''', schema, server);
   /* segments */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.segments', schema);
   EXECUTE format(segments_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.segments IS ''Size of Oracle objects on foreign server "%I"''', schema, server);
   /* migration_cost_estimate */
   EXECUTE format('DROP VIEW IF EXISTS %I.migration_cost_estimate', schema);
   EXECUTE format(migration_cost_estimate_sql, schema, schema, schema, schema, schema, schema, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.migration_cost_estimate IS ''Estimate of the migration costs per schema and object type''', schema);
   /* test_error */
   EXECUTE format('DROP TABLE IF EXISTS %I.test_error', schema);
   EXECUTE format(test_error_sql, schema);
   EXECUTE format('COMMENT ON TABLE %I.test_error IS ''Errors from the last run of "oracle_migrate_test_data"''', schema);
   /* test_error_stats */
   EXECUTE format('DROP TABLE IF EXISTS %I.test_error_stats', schema);
   EXECUTE format(test_error_stats_sql, schema);
   EXECUTE format('COMMENT ON TABLE %I.test_error_stats IS ''Cumulative errors from previous runs of "oracle_migrate_test_data"''', schema);

   /* reset client_min_messages */
   EXECUTE 'SET LOCAL client_min_messages = ' || old_msglevel;
END;$$;

COMMENT ON FUNCTION synchdb_create_oraviews(name, name, jsonb) IS
   'create Oracle foreign tables for the metadata of a foreign server';

CREATE OR REPLACE FUNCTION synchdb_materialize_ora_metadata(
    p_source_schema name,          -- e.g. 'ora_obj' (must exist; contains FTs)
    p_dest_schema   name,          -- destination schema to hold local materialized copies
    p_on_exists     text DEFAULT 'replace'  -- 'replace' | 'skip' | 'error'
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rname  text;
    rels   text[] := ARRAY['tables','columns','keys'];  -- materialize just these
    src_ok boolean;
    dst_ok boolean;
    rel_exists boolean;
BEGIN
    -- 1) Validate source schema exists
    SELECT EXISTS (
        SELECT 1 FROM pg_namespace WHERE nspname = p_source_schema::text
    ) INTO src_ok;

    IF NOT src_ok THEN
        RAISE EXCEPTION 'Source schema % does not exist', p_source_schema;
    END IF;

    -- 2) Ensure destination schema exists (create if needed)
    SELECT EXISTS (
        SELECT 1 FROM pg_namespace WHERE nspname = p_dest_schema::text
    ) INTO dst_ok;

    IF NOT dst_ok THEN
        EXECUTE format('CREATE SCHEMA %I', p_dest_schema);
    END IF;

    -- 3) Loop through the three metadata relations
    FOREACH rname IN ARRAY rels LOOP
        -- 3a) Make sure the source relation exists (as a table/foreign table/view)
        SELECT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = p_source_schema::text
              AND c.relname = rname
              AND c.relkind IN ('r','v','f','m')  -- table, view, foreign table, matview
        ) INTO rel_exists;

        IF NOT rel_exists THEN
            RAISE NOTICE 'Skipping %.% (not found in source)', p_source_schema, rname;
            CONTINUE;
        END IF;

        -- 3b) Handle destination existence policy
        SELECT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = p_dest_schema::text
              AND c.relname = rname
              AND c.relkind IN ('r','m','f','v')
        ) INTO rel_exists;

        IF rel_exists THEN
            IF p_on_exists = 'replace' THEN
                EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', p_dest_schema, rname);
                -- If it was a view/FT/mview, DROP TABLE IF EXISTS won't catch; drop any object kind:
                BEGIN
                    EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', p_dest_schema, rname);
                EXCEPTION WHEN undefined_table THEN END;
                BEGIN
                    EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %I.%I CASCADE', p_dest_schema, rname);
                EXCEPTION WHEN undefined_table THEN END;
                BEGIN
                    EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.%I CASCADE', p_dest_schema, rname);
                EXCEPTION WHEN undefined_table THEN END;
            ELSIF p_on_exists = 'skip' THEN
                RAISE NOTICE 'Skipping %.% (already exists and on_exists=skip)', p_dest_schema, rname;
                CONTINUE;
            ELSE
                RAISE EXCEPTION 'Destination %.% already exists (on_exists=%)', p_dest_schema, rname, p_on_exists;
            END IF;
        END IF;

        -- 3c) Materialize as UNLOGGED table (structure+data)
        EXECUTE format(
            'CREATE UNLOGGED TABLE %I.%I AS TABLE %I.%I',
            p_dest_schema, rname, p_source_schema, rname
        );

        RAISE NOTICE 'Materialized %.% -> %.% (UNLOGGED)', p_source_schema, rname, p_dest_schema, rname;

        -- 3d) Add helpful indexes if the expected columns exist
        -- For tables(schema, table_name)
        IF rname = 'tables' THEN
            PERFORM 1 FROM information_schema.columns
             WHERE table_schema = p_dest_schema::text
               AND table_name   = 'tables'
               AND column_name  = 'schema';
            IF FOUND THEN
                PERFORM 1 FROM information_schema.columns
                 WHERE table_schema = p_dest_schema::text
                   AND table_name   = 'tables'
                   AND column_name  = 'table_name';
                IF FOUND THEN
                    EXECUTE format(
                        'CREATE INDEX ON %I.%I(("schema"), table_name)',
                        p_dest_schema, rname
                    );
                END IF;
            END IF;
        END IF;

        -- For columns(schema, table_name, position)
        IF rname = 'columns' THEN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = p_dest_schema::text AND table_name='columns'
                  AND column_name IN ('schema','table_name','position')
                GROUP BY table_schema, table_name
                HAVING count(*) = 3
            ) THEN
                EXECUTE format(
                    'CREATE INDEX ON %I.%I(("schema"), table_name, position)',
                    p_dest_schema, rname
                );
            END IF;
        END IF;

        -- For keys(schema, table_name[, constraint_name])
        IF rname = 'keys' THEN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = p_dest_schema::text AND table_name='keys'
                  AND column_name IN ('schema','table_name')
                GROUP BY table_schema, table_name
                HAVING count(*) = 2
            ) THEN
                EXECUTE format(
                    'CREATE INDEX ON %I.%I(("schema"), table_name)',
                    p_dest_schema, rname
                );
            END IF;

            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = p_dest_schema::text AND table_name='keys'
                  AND column_name = 'constraint_name'
            ) THEN
                EXECUTE format(
                    'CREATE INDEX ON %I.%I((constraint_name))',
                    p_dest_schema, rname
                );
            END IF;
        END IF;

    END LOOP;

    -- 4) Analyze for better local planning
    EXECUTE format('ANALYZE %I.tables',  p_dest_schema);
    EXECUTE format('ANALYZE %I.columns', p_dest_schema);
    EXECUTE format('ANALYZE %I.keys',    p_dest_schema);

    RAISE NOTICE 'Materialization complete into schema %', p_dest_schema;
END;
$$;

COMMENT ON FUNCTION synchdb_materialize_ora_metadata(name, name, text) IS
   'create a metadata schema with selected ora_obj foreign tables materialized';

CREATE OR REPLACE FUNCTION synchdb_create_current_scn_ft(
    p_schema name,   -- e.g. 'ora_obj'
    p_server name    -- e.g. 'oracle'
) RETURNS void
LANGUAGE plpgsql
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
   'create Oracle foreign tables for current scn';

CREATE OR REPLACE FUNCTION synchdb_create_ora_stage_fts(
    p_connector_name        name,                         -- connector name for attribute rows
    p_desired_db            name,                         -- db token to match entries in snapshottable
    p_desired_schema        name,                         -- schema to match when entry includes schema
    p_stage_schema          name DEFAULT 'ora_stage'::name, -- target schema in Postgres
    p_server_name           name DEFAULT 'oracle'::name,  -- oracle_fdw server name
    p_lower_names           boolean DEFAULT true,         -- lower-case PG table/column names
    p_on_exists             text DEFAULT 'replace',       -- 'replace' | 'drop' | 'skip'
    p_scn                   numeric DEFAULT 0,            -- >0 => use AS OF SCN subquery
    p_source_schema         name DEFAULT 'ora_obj'::name, -- metadata source schema (…columns)
    p_snapshot_tables       text DEFAULT NULL,            -- CSV of db.schema.table; when set, bypass conninfo
    p_write_dbz_schema_info boolean DEFAULT false         -- when true, store Debezium schema-history JSON
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  has_oracle_fdw boolean;
  r              RECORD;
  v_tbl_pg       text;
  v_cols_sql     text;
  v_sel_list     text;
  v_exists       boolean;
  v_subquery     text;

  v_conn_type    name;      -- from synchdb_conninfo.data->>'connector' (conninfo mode)
  v_srcdb        text;      -- default db token (conninfo mode)

  v_snapcfg      jsonb;     -- raw data->'snapshottable' (conninfo mode)
  v_snaptext     text;      -- comma-separated list (normalized)
  v_use_filter   boolean := false;
  v_type         text;

  base_sql       text;

  use_explicit   boolean := (p_snapshot_tables IS NOT NULL AND btrim(p_snapshot_tables) <> '');
  v_ext_db       text;      -- per-table db token for ext_tbname when explicit list is used
  v_tbl_name_l   text;
  v_relid        oid;

  -- JSON construction vars
  v_pk_json      jsonb;
  v_cols_json    jsonb;
  v_table_obj    jsonb;
  v_change_obj   jsonb;
  v_msg_json     jsonb;
  v_ts_ms        bigint;

  -- per-connector schema-history table name (sanitized)
  v_schema_tbl   text;

  -- error table vars (existence check only)
  v_err_tbl_ident  text;
  v_err_tbl_exists boolean := false;
BEGIN
  -- Ensure oracle_fdw is available
  SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname='oracle_fdw')
      OR EXISTS (SELECT 1 FROM pg_foreign_data_wrapper WHERE fdwname='oracle_fdw')
    INTO has_oracle_fdw;
  IF NOT has_oracle_fdw THEN
    RAISE EXCEPTION 'oracle_fdw is not installed. Run: CREATE EXTENSION oracle_fdw;';
  END IF;

  ----------------------------------------------------------------------
  -- Prepare per-connector schema-history table (if requested)
  ----------------------------------------------------------------------
  IF p_write_dbz_schema_info THEN
    v_schema_tbl :=
      format('schema_history_%s',
             regexp_replace(lower(p_connector_name::text), '[^a-z0-9_]', '_', 'g'));

    -- single-column table: line TEXT
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS %I.%I (
         line  text NOT NULL
       )',
      'public', v_schema_tbl
    );

    -- truncate at start so each run produces a clean set
    EXECUTE format('TRUNCATE TABLE %I.%I', 'public', v_schema_tbl);
  END IF;

  ----------------------------------------------------------------------
  -- Per-connector error table identifier (for cleanup in retry mode)
  ----------------------------------------------------------------------
  v_err_tbl_ident :=
      'synchdb_fdw_snapshot_errors_'
      || regexp_replace(lower(p_connector_name::text), '[^a-z0-9_]', '_', 'g');

  IF use_explicit THEN
    -- Check if the error table exists; used later for cleanup
    SELECT EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND c.relname = v_err_tbl_ident
    ) INTO v_err_tbl_exists;
  END IF;

  ----------------------------------------------------------------------
  -- Build tmp_snap_list in one of two modes:
  --   A) Explicit list mode (p_snapshot_tables provided)
  --   B) Conninfo+snapshottable mode (legacy/default)
  ----------------------------------------------------------------------
  CREATE TEMP TABLE IF NOT EXISTS tmp_snap_list (
    db    text NOT NULL,
    schem text,     -- NULL => item didn’t specify schema
    tbl   text NOT NULL
  ) ON COMMIT DROP;
  TRUNCATE tmp_snap_list;

  IF use_explicit THEN
    -- Mode A: parse CSV of db.schema.table triplets (enforce 3-part)
    INSERT INTO tmp_snap_list(db, schem, tbl)
    SELECT lower(parts[1]) AS db,
           lower(parts[2]) AS schem,
           lower(parts[3]) AS tbl
    FROM (
      SELECT regexp_split_to_array(trim(x), '\.') AS parts
      FROM regexp_split_to_table(p_snapshot_tables, '\s*,\s*') AS t(x)
      WHERE trim(x) <> ''
    ) s
    WHERE array_length(parts,1) = 3;

    v_use_filter := EXISTS (SELECT 1 FROM tmp_snap_list);

  ELSE
    -- Mode B: legacy behavior using synchdb_conninfo + data->snapshottable
    SELECT (data->>'connector')::name,
           lower(data->>'srcdb'),
           data->'snapshottable'
      INTO v_conn_type, v_srcdb, v_snapcfg
    FROM synchdb_conninfo
    WHERE name = p_connector_name;

    IF v_conn_type IS NULL OR v_conn_type = ''::name THEN
      RAISE EXCEPTION 'synchdb_conninfo[%]: data->>connector is missing/empty', p_connector_name;
    END IF;
    IF v_srcdb IS NULL OR v_srcdb = '' THEN
      RAISE EXCEPTION 'synchdb_conninfo[%]: data->>srcdb is missing/empty', p_connector_name;
    END IF;

    IF v_snapcfg IS NULL OR v_snapcfg::text = 'null' THEN
      v_use_filter := false;  -- migrate all (but we'll still restrict by p_desired_schema below)
    ELSE
      v_type := jsonb_typeof(v_snapcfg);

      -- Build v_snaptext as a single comma-separated list, regardless of input shape
      IF v_type = 'string' THEN
        v_snaptext := trim(both '"' from v_snapcfg::text);
        IF position('file:' IN v_snaptext) = 1 THEN
          v_snaptext := read_snapshot_table_list(v_snaptext);
        END IF;
      ELSIF v_type = 'array' THEN
        SELECT string_agg(x, ',')
          INTO v_snaptext
        FROM jsonb_array_elements_text(v_snapcfg) AS t(x);
      ELSE
        RAISE EXCEPTION 'synchdb_conninfo[%.data->snapshottable] must be string or array, got %',
                        p_connector_name, v_type;
      END IF;

      -- Parse CSV into tmp_snap_list allowing "db.tbl" or "db.schema.tbl"
      INSERT INTO tmp_snap_list(db, schem, tbl)
      SELECT
        lower((parts)[1]) AS db,
        CASE WHEN array_length(parts,1) = 3 THEN lower((parts)[2]) ELSE NULL END AS schem,
        CASE WHEN array_length(parts,1) = 3 THEN lower((parts)[3])
             WHEN array_length(parts,1) = 2 THEN lower((parts)[2])
             ELSE NULL END AS tbl
      FROM (
        SELECT regexp_split_to_array(trim(x), '\.') AS parts
        FROM regexp_split_to_table(v_snaptext, '\s*,\s*') AS t(x)
        WHERE trim(x) <> ''
      ) s
      WHERE array_length(parts,1) IN (2,3);

      -- Keep only rows that match desired DB (legacy behavior)
      DELETE FROM tmp_snap_list WHERE db <> lower(p_desired_db);

      v_use_filter := EXISTS (SELECT 1 FROM tmp_snap_list);
    END IF;
  END IF;

  -- ensure stage schema
  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_stage_schema);

  ----------------------------------------------------------------------
  -- Base table list from metadata; now RESTRICT to p_desired_schema
  ----------------------------------------------------------------------
  base_sql := format(
    'SELECT "schema" AS ora_owner, table_name
       FROM %1$I.columns
      WHERE upper("schema") = upper(%2$L)
	    AND lower(table_name) <> ''log_mining_flush''
      GROUP BY "schema", table_name',
    p_source_schema,
    p_desired_schema::text
  );

  IF v_use_filter THEN
    IF use_explicit THEN
      -- Match by table and (optionally) owner from the explicit list; still within desired schema
      base_sql := base_sql || '
        HAVING EXISTS (
          SELECT 1
            FROM tmp_snap_list f
           WHERE lower(table_name) = f.tbl
             AND (f.schem IS NULL OR lower("schema") = f.schem)
        )';
    ELSE
      -- Legacy gating: require DB == p_desired_db and (optionally) exact schema match in tmp list
      base_sql := base_sql || format(
        ' HAVING EXISTS (
             SELECT 1
               FROM tmp_snap_list f
              WHERE f.db  = %L
                AND lower(table_name) = f.tbl
                AND (f.schem IS NULL OR f.schem = %L)
          )',
        lower(p_desired_db),
        lower(p_desired_schema)
      );
    END IF;
  END IF;

  ----------------------------------------------------------------------
  -- Create (or replace) the stage foreign tables
  ----------------------------------------------------------------------
  FOR r IN EXECUTE base_sql || ' ORDER BY ora_owner, table_name'
  LOOP
    -- decide PG table name
    v_tbl_pg := CASE WHEN p_lower_names THEN lower(r.table_name) ELSE r.table_name END;

    -- does the FT already exist?
    SELECT EXISTS (
      SELECT 1
      FROM pg_foreign_table ft
      JOIN pg_class        c ON c.oid = ft.ftrelid
      JOIN pg_namespace    n ON n.oid = c.relnamespace
      WHERE n.nspname = p_stage_schema
        AND c.relname = v_tbl_pg
    ) INTO v_exists;

    IF v_exists THEN
      IF p_on_exists IN ('replace','drop') THEN
        EXECUTE format('DROP FOREIGN TABLE %I.%I', p_stage_schema, v_tbl_pg);
      ELSIF p_on_exists = 'skip' THEN
        RAISE NOTICE 'Skipping %.% (already exists)', p_stage_schema, v_tbl_pg;
        CONTINUE;
      ELSE
        RAISE EXCEPTION 'Unknown p_on_exists value: % (use replace|drop|skip)', p_on_exists;
      END IF;
    END IF;

    -- Build PG column list using translator
    EXECUTE format(
      'SELECT string_agg(
                quote_ident(%s) || '' '' ||
                synchdb_translate_datatype(''oracle'', lower(c.type_name),
                                           COALESCE(c.length, -1),
                                           COALESCE(c.scale, -1),
                                           COALESCE(c.precision, -1)) ||
                CASE WHEN c.nullable IS FALSE THEN '' NOT NULL'' ELSE '''' END,
                '', '' ORDER BY c.position)
         FROM %I.columns c
        WHERE c."schema" = %L
          AND c.table_name = %L',
      CASE WHEN p_lower_names THEN 'lower(c.column_name)' ELSE 'c.column_name' END,
      p_source_schema, r.ora_owner, r.table_name
    )
    INTO v_cols_sql;

    IF v_cols_sql IS NULL THEN
      RAISE NOTICE 'No columns found for %.% — skipping', r.ora_owner, r.table_name;
      CONTINUE;
    END IF;

    -- Create the staging FT (snapshot or not)
    IF p_scn IS NOT NULL AND p_scn > 0 THEN
      EXECUTE format(
        'SELECT string_agg(quote_ident(c.column_name), '', '' ORDER BY c.position)
           FROM %I.columns c
          WHERE c."schema" = %L
            AND c.table_name = %L',
        p_source_schema, r.ora_owner, r.table_name
      )
      INTO v_sel_list;

      v_subquery := format('(SELECT %s FROM %I.%I AS OF SCN %s)',
                           COALESCE(v_sel_list, '*'),
                           r.ora_owner, r.table_name, p_scn::text);

      EXECUTE format(
        'CREATE FOREIGN TABLE %I.%I (%s) SERVER %I OPTIONS ("table" %L)',
        p_stage_schema, v_tbl_pg, v_cols_sql, p_server_name, v_subquery
      );

      RAISE NOTICE 'Created FT %.% at SCN % -> %.% on %',
                   p_stage_schema, v_tbl_pg, p_scn::text, r.ora_owner, r.table_name, p_server_name;
    ELSE
      EXECUTE format(
        'CREATE FOREIGN TABLE %I.%I (%s) SERVER %I OPTIONS (schema %L, "table" %L)',
        p_stage_schema, v_tbl_pg, v_cols_sql, p_server_name, r.ora_owner, r.table_name
      );

      RAISE NOTICE 'Created FT %.% -> %.% on %',
                   p_stage_schema, v_tbl_pg, r.ora_owner, r.table_name, p_server_name;
    END IF;

    -- Look up the OID of the just-created foreign table (relkind = 'f')
    EXECUTE format(
      'SELECT c.oid
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %L
          AND c.relname = %L
          AND c.relkind = ''f''
        LIMIT 1',
      p_stage_schema, v_tbl_pg
    )
    INTO STRICT v_relid;

    -- Determine db token for ext_tbname:
    IF use_explicit THEN
      SELECT f.db
        INTO v_ext_db
      FROM tmp_snap_list f
      WHERE lower(f.tbl) = lower(r.table_name)
        AND (f.schem IS NULL OR f.schem = lower(r.ora_owner))
      LIMIT 1;
      IF v_ext_db IS NULL THEN
        -- Fallback (shouldn't happen if filter matched); use p_desired_db
        v_ext_db := lower(p_desired_db::text);
      END IF;
    ELSE
      v_ext_db := v_srcdb;  -- from conninfo
    END IF;

    -- Build fully-qualified external table name: db.schema.table (all lower)
    v_tbl_name_l := lower(r.table_name);

    -- Refresh synchdb_attribute rows for this table
    DELETE FROM public.synchdb_attribute
     WHERE name       = p_connector_name
       AND type       = COALESCE(v_conn_type, 'oracle'::name)
       AND ext_tbname = format('%s.%s.%s', v_ext_db, lower(r.ora_owner), v_tbl_name_l)::name;

    EXECUTE format($ins$
      INSERT INTO public.synchdb_attribute
          (name, type, attrelid, attnum, ext_tbname,    ext_attname,           ext_atttypename)
      SELECT
          %L::name,
          %L::name,
          %s::oid,
          c.position::smallint,
          %L::name,
          lower(c.column_name)::name,
          lower(c.type_name)::name
      FROM %I.columns c
      WHERE c."schema"   = %L
        AND c.table_name = %L
      ORDER BY c.position
    $ins$, p_connector_name, COALESCE(v_conn_type, 'oracle'::name), v_relid::text,
          format('%s.%s.%s', v_ext_db, lower(r.ora_owner), v_tbl_name_l),
          p_source_schema, r.ora_owner, r.table_name);

    RAISE NOTICE 'Recorded % Oracle columns for %.% into synchdb_attribute (attrelid=%) as %',
                 (SELECT count(*)
                    FROM public.synchdb_attribute
                   WHERE name = p_connector_name
                     AND type = COALESCE(v_conn_type, 'oracle'::name)
                     AND ext_tbname = format('%s.%s.%s', v_ext_db, lower(r.ora_owner), v_tbl_name_l)::name),
                 r.ora_owner, r.table_name, v_relid::text,
                 format('%s.%s.%s', v_ext_db, lower(r.ora_owner), v_tbl_name_l);

    ------------------------------------------------------------------
    -- Build, log, and store Debezium schema-history JSON (if enabled)
    ------------------------------------------------------------------
    IF p_write_dbz_schema_info THEN
      -- current time in ms
      v_ts_ms := FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000);

      -- primary key column names as JSON array
      EXECUTE format(
        'SELECT COALESCE(jsonb_agg(k.column_name ORDER BY k.position), ''[]''::jsonb)
           FROM %I.keys k
          WHERE k."schema" = %L
            AND k.table_name = %L
            AND k.is_primary IS TRUE',
        p_source_schema, r.ora_owner, r.table_name
      )
      INTO v_pk_json;

      -- columns JSON array
      EXECUTE format($SQL$
        SELECT jsonb_agg(
                 jsonb_build_object(
                   'name',            c.column_name,
                   'jdbcType',        oracle_type_to_jdbc(lower(c.type_name)),
                   'typeName',        upper(c.type_name),
                   'typeExpression',  upper(c.type_name),
                   'charsetName',     NULL,
                   'length',          COALESCE(c.length, 0),
                   'position',        c.position,
                   'optional',        COALESCE(c.nullable, TRUE),
                   'autoIncremented', FALSE,
                   'generated',       FALSE,
                   'comment',         NULL,
                   'hasDefaultValue', (c.default_value IS NOT NULL),
                   'enumValues',      '[]'::jsonb
                 )
                 ORDER BY c.position
               )
          FROM %1$I.columns c
         WHERE c."schema" = %2$L
           AND c.table_name = %3$L
      $SQL$, p_source_schema, r.ora_owner, r.table_name)
      INTO v_cols_json;

      v_table_obj := jsonb_build_object(
        'defaultCharsetName', NULL,
        'primaryKeyColumnNames', COALESCE(v_pk_json, '[]'::jsonb),
        'columns', COALESCE(v_cols_json, '[]'::jsonb)
      );

      v_change_obj := jsonb_build_object(
        'type', 'CREATE',
        'id', format('"%s"."%s"."%s"',
                     upper(p_desired_db::text),
                     upper(p_desired_schema::text),
                     upper(r.table_name)),
        'table', v_table_obj,
        'comment', NULL
      );

      v_msg_json := jsonb_build_object(
        'source',   jsonb_build_object('server', 'synchdb-connector'),
        'position', jsonb_build_object(
                       'snapshot_scn',       p_scn::text,
                       'snapshot',           TRUE,
                       'scn',                p_scn::text,
                       'snapshot_completed', TRUE
                     ),
        'ts_ms', v_ts_ms,
        'databaseName', upper(p_desired_db::text),
        'schemaName',   upper(p_desired_schema::text),
        'ddl', '',
        'tableChanges', jsonb_build_array(v_change_obj)
      );

      -- log for visibility
      -- RAISE WARNING '%', v_msg_json::text;

      -- store one JSON line per table
      EXECUTE format(
        'INSERT INTO %I.%I(line) VALUES ($1)',
        'public', v_schema_tbl
      )
      USING v_msg_json::text;
    END IF;

  END LOOP;

  ----------------------------------------------------------------------
  -- Snapshot-retry cleanup (unchanged):
  ----------------------------------------------------------------------
  IF use_explicit AND v_err_tbl_exists THEN
    -- requested_full
    CREATE TEMP TABLE IF NOT EXISTS tmp_requested_full(fullname text PRIMARY KEY) ON COMMIT DROP;
    TRUNCATE tmp_requested_full;
    INSERT INTO tmp_requested_full(fullname)
    SELECT format('%s.%s.%s', db, schem, tbl)
    FROM tmp_snap_list;

    -- existing_full (join requested list to metadata to ensure db token matches)
    CREATE TEMP TABLE IF NOT EXISTS tmp_existing_full(fullname text PRIMARY KEY) ON COMMIT DROP;
    TRUNCATE tmp_existing_full;

    EXECUTE format($SQL$
      INSERT INTO tmp_existing_full(fullname)
      SELECT format('%%s.%%s.%%s', f.db, lower(c."schema"), lower(c.table_name))
        FROM %1$I.columns c
        JOIN tmp_snap_list f
          ON lower(c.table_name) = f.tbl
         AND (f.schem IS NULL OR lower(c."schema") = f.schem)
      GROUP BY f.db, c."schema", c.table_name
    $SQL$, p_source_schema);

    -- prune: anything requested but not existing anymore
    EXECUTE format($SQL$
      DELETE FROM %1$I.%2$I e
       WHERE e.connector_name = $1
         AND e.tbl IN (
               SELECT r.fullname
                 FROM tmp_requested_full r
            EXCEPT
               SELECT x.fullname
                 FROM tmp_existing_full x
             )
    $SQL$, 'public', v_err_tbl_ident)
    USING p_connector_name;

    RAISE NOTICE 'Snapshot-retry cleanup: pruned stale errors for tables dropped in Oracle (if any).';
  END IF;

END;
$$;

COMMENT ON FUNCTION synchdb_create_ora_stage_fts(name, name, name, name, name, boolean, text, numeric, name, text, boolean) IS
   'create a staging schema, migrate table schemas with translated data types';

CREATE OR REPLACE FUNCTION synchdb_materialize_schema(
    p_connector_name name,            -- connector name for stats
    p_src_schema     name,            -- e.g. 'ora_stage'
    p_dst_schema     name,            -- e.g. 'psql_stage'
    p_on_exists      text DEFAULT 'skip'  -- 'skip' | 'replace'
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r           record;
  v_exists    boolean;
  v_ft_oid    oid;
  v_dst_oid   oid;
  v_upd_count bigint;
BEGIN
  -- ensure destination schema exists
  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_dst_schema);

  FOR r IN
    SELECT c.relname AS tbl
    FROM   pg_foreign_table ft
    JOIN   pg_class        c ON c.oid = ft.ftrelid
    JOIN   pg_namespace    n ON n.oid = c.relnamespace
    WHERE  n.nspname = p_src_schema
  LOOP
    SELECT EXISTS (
      SELECT 1
      FROM pg_class c2
      JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
      WHERE n2.nspname = p_dst_schema
        AND c2.relname = r.tbl
        AND c2.relkind = 'r'
    ) INTO v_exists;

    IF v_exists THEN
      IF p_on_exists = 'skip' THEN
        RAISE NOTICE 'Skipping %.% (already exists)', p_dst_schema, r.tbl;
        CONTINUE;
      ELSIF p_on_exists = 'replace' THEN
        EXECUTE format('DROP TABLE %I.%I CASCADE', p_dst_schema, r.tbl);
      ELSE
        RAISE EXCEPTION 'Unknown p_on_exists value: %, use "skip" or "replace"', p_on_exists;
      END IF;
    END IF;

    -- create empty materialized table with same columns
    EXECUTE format(
      'CREATE TABLE %I.%I AS TABLE %I.%I WITH NO DATA',
      p_dst_schema, r.tbl, p_src_schema, r.tbl
    );

    RAISE NOTICE 'Created %I.%I from %I.%I', r.tbl, p_dst_schema, r.tbl, p_src_schema;

    -- record stats after successful materialization
    PERFORM synchdb_set_snapstats(p_connector_name, 1::bigint, 0::bigint, 0::bigint, 0::bigint);

    ----------------------------------------------------------------
    -- Lookup source/destination OIDs
    ----------------------------------------------------------------
    SELECT c.oid
      INTO v_ft_oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = p_src_schema
      AND c.relname = r.tbl
      AND c.relkind = 'f'
    LIMIT 1;

    IF v_ft_oid IS NULL THEN
      RAISE NOTICE 'Could not find foreign table OID for %.% (skipping attrelid update)',
                   p_src_schema, r.tbl;
      CONTINUE;
    END IF;

    SELECT c.oid
      INTO v_dst_oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = p_dst_schema
      AND c.relname = r.tbl
      AND c.relkind = 'r'
    LIMIT 1;

    IF v_dst_oid IS NULL THEN
      RAISE NOTICE 'Could not find destination table OID for %.% (skipping attrelid update)',
                   p_dst_schema, r.tbl;
      CONTINUE;
    END IF;

    ----------------------------------------------------------------
    -- Update synchdb_attribute
    ----------------------------------------------------------------
    UPDATE public.synchdb_attribute
       SET attrelid = v_dst_oid
     WHERE attrelid = v_ft_oid;

    GET DIAGNOSTICS v_upd_count = ROW_COUNT;

    RAISE NOTICE 'Updated synchdb_attribute: % rows (attrelid % -> % for table %)',
                 v_upd_count, v_ft_oid::text, v_dst_oid::text, r.tbl;
  END LOOP;
END;
$$;

COMMENT ON FUNCTION synchdb_materialize_schema(name, name, name, text) IS
   'materialize table schema from staging to destination schema';

CREATE OR REPLACE FUNCTION synchdb_migrate_primary_keys(
    p_oraobj_schema name,   -- e.g. 'ora_obj'  (holds table "keys")
    p_dst_schema    name    -- e.g. 'psql_stage'
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r record;
  v_exists  boolean;
  v_cname   text;
  v_sql     text;
BEGIN
  -- Iterate PK definitions from <p_oraobj_schema>.keys
  FOR r IN
    EXECUTE format($q$
      SELECT
        lower(table_name) AS tbl,
        string_agg(quote_ident(lower(column_name)), ', ' ORDER BY position) AS cols
      FROM %I.keys
      WHERE is_primary = true
      GROUP BY table_name
    $q$, p_oraobj_schema)
  LOOP
    -- Only act if destination table exists
    SELECT EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = p_dst_schema
        AND c.relname  = r.tbl
        AND c.relkind  = 'r'
    ) INTO v_exists;

    IF NOT v_exists THEN
      RAISE NOTICE 'Skipping %.%: table not found', p_dst_schema, r.tbl;
      CONTINUE;
    END IF;

    -- Build constraint name (<= 63 chars)
    v_cname := left(r.tbl, 55) || '_pkey';

    v_sql := format(
      'ALTER TABLE %I.%I ADD CONSTRAINT %I PRIMARY KEY (%s)',
      p_dst_schema, r.tbl, v_cname, r.cols
    );

    BEGIN
      EXECUTE v_sql;
      RAISE NOTICE 'Added primary key % on %.%', v_cname, p_dst_schema, r.tbl;

    EXCEPTION
      WHEN SQLSTATE '42P16' THEN  -- multiple primary keys not allowed
        RAISE NOTICE 'Skipping %.%: a primary key already exists', p_dst_schema, r.tbl;
      WHEN duplicate_object THEN   -- constraint name already exists
        RAISE NOTICE 'Skipping %.%: constraint % already exists', p_dst_schema, r.tbl, v_cname;
      WHEN OTHERS THEN
        RAISE WARNING 'Failed to add PK on %.%: %', p_dst_schema, r.tbl, SQLERRM;
    END;
  END LOOP;
END;
$$;

COMMENT ON FUNCTION synchdb_migrate_primary_keys(name, name) IS
   'migrate primary keys from staging to destination schema';

CREATE OR REPLACE FUNCTION synchdb_apply_column_mappings(
    p_src_schema      name,             -- e.g. 'psql_stage'
    p_connector_name  name,             -- connector name to filter objmap
    p_desired_db      name,             -- e.g. 'free' or 'wrongdb'
    p_desired_schema  name DEFAULT NULL -- optional: e.g. 'dbzuser'; NULL = don't enforce
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  -- RENAME pass vars
  r          RECORD;
  parts      text[];
  s_db       text;
  s_schema   text;
  s_table    text;
  s_col      text;

  dst_parts  text[];
  d_col      text;

  -- DATATYPE pass vars
  rdt        RECORD;
  dt_parts   text[];
  dt_db      text;
  dt_schema  text;
  dt_table   text;
  dt_col     text;
  target_col text;   -- column name to ALTER TYPE (post-rename if applicable)

  dt_spec    text;   -- raw 'dstobj' from datatype row, e.g. 'varchar|128'
  dt_name    text;   -- left side of '|'
  dt_len_txt text;   -- right side of '|'
  dt_len     int;
  dtype_sql  text;   -- rendered SQL type, e.g. 'varchar(128)' or 'text'
BEGIN
  ---------------------------------------------------------------------------
  -- Pass 1: COLUMN RENAME mappings (unchanged)
  ---------------------------------------------------------------------------
  FOR r IN
    SELECT m.srcobj, m.dstobj
    FROM synchdb_objmap AS m
    WHERE m.objtype = 'column'
      AND m.enabled
      AND m.name = p_connector_name
      AND m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''
  LOOP
    parts := regexp_split_to_array(lower(r.srcobj), '\.');
    s_db := NULL; s_schema := NULL; s_table := NULL; s_col := NULL;

    IF array_length(parts,1) = 4 THEN
      s_db := parts[1]; s_schema := parts[2]; s_table := parts[3]; s_col := parts[4];
    ELSIF array_length(parts,1) = 3 THEN
      s_db := parts[1]; s_table := parts[2]; s_col := parts[3];
    ELSE
      CONTINUE;
    END IF;

    IF p_desired_db IS NOT NULL AND s_db IS DISTINCT FROM lower(p_desired_db) THEN
      CONTINUE;
    END IF;

    IF p_desired_schema IS NOT NULL
       AND s_schema IS NOT NULL
       AND s_schema <> lower(p_desired_schema) THEN
      CONTINUE;
    END IF;

    dst_parts := regexp_split_to_array(lower(r.dstobj), '\.');
    d_col := dst_parts[array_length(dst_parts,1)];

    IF NOT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
        AND table_name   = s_table
        AND column_name  = s_col
    ) THEN
      RAISE NOTICE 'Skipping %.%: source column % not found', p_src_schema, s_table, s_col;
      CONTINUE;
    END IF;

    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
        AND table_name   = s_table
        AND column_name  = d_col
    ) THEN
      RAISE NOTICE 'Skipping %.%: destination column % already exists', p_src_schema, s_table, d_col;
      CONTINUE;
    END IF;

    EXECUTE format('ALTER TABLE %I.%I RENAME COLUMN %I TO %I',
                   p_src_schema, s_table, s_col, d_col);

    RAISE NOTICE 'Renamed %.%: % -> %', p_src_schema, s_table, s_col, d_col;
  END LOOP;

  ---------------------------------------------------------------------------
  -- Pass 2: DATATYPE mappings with 'type|length' grammar in dstobj
  -- - Find target column name (renamed if a COLUMN mapping exists for same srcobj)
  -- - Render type as: length=0 -> "type", else "type(length)"
  ---------------------------------------------------------------------------
  FOR rdt IN
    SELECT m.srcobj, m.dstobj
    FROM synchdb_objmap AS m
    WHERE m.objtype = 'datatype'
      AND m.enabled
      AND m.name = p_connector_name
      AND m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''
  LOOP
    -- Parse srcobj => db(.schema).table.column
    dt_parts := regexp_split_to_array(lower(rdt.srcobj), '\.');
    dt_db := NULL; dt_schema := NULL; dt_table := NULL; dt_col := NULL;

    IF array_length(dt_parts,1) = 4 THEN
      dt_db := dt_parts[1]; dt_schema := dt_parts[2]; dt_table := dt_parts[3]; dt_col := dt_parts[4];
    ELSIF array_length(dt_parts,1) = 3 THEN
      dt_db := dt_parts[1]; dt_table := dt_parts[2]; dt_col := dt_parts[3];
    ELSE
      CONTINUE;
    END IF;

    IF p_desired_db IS NOT NULL AND dt_db IS DISTINCT FROM lower(p_desired_db) THEN
      CONTINUE;
    END IF;

    IF p_desired_schema IS NOT NULL
       AND dt_schema IS NOT NULL
       AND dt_schema <> lower(p_desired_schema) THEN
      CONTINUE;
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = p_src_schema
        AND table_name   = dt_table
    ) THEN
      RAISE NOTICE 'Skipping %.%: table not found for datatype mapping', p_src_schema, dt_table;
      CONTINUE;
    END IF;

    -- If a COLUMN mapping exists for same srcobj, use its dst column name
    SELECT (regexp_split_to_array(lower(m2.dstobj), '\.'))[
             array_length(regexp_split_to_array(lower(m2.dstobj), '\.'), 1)
           ]
    INTO target_col
    FROM synchdb_objmap m2
    WHERE m2.objtype = 'column'
      AND m2.enabled
      AND m2.name = p_connector_name
      AND lower(m2.srcobj) = lower(rdt.srcobj)
    LIMIT 1;

    IF target_col IS NULL OR btrim(target_col) = '' THEN
      target_col := dt_col;  -- no rename mapping; use original name
    END IF;

    -- Ensure target column exists
    IF NOT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
        AND table_name   = dt_table
        AND column_name  = target_col
    ) THEN
      RAISE NOTICE 'Skipping %.%: target column % not found for datatype mapping', p_src_schema, dt_table, target_col;
      CONTINUE;
    END IF;

    -- Parse datatype spec 'type|len'
    dt_spec    := rdt.dstobj;
    dt_name    := btrim(split_part(lower(dt_spec), '|', 1));
    dt_len_txt := btrim(split_part(dt_spec, '|', 2));
    dt_len     := COALESCE(NULLIF(dt_len_txt, '')::int, 0);

    IF dt_name IS NULL OR dt_name = '' THEN
      RAISE NOTICE 'Skipping %.%: invalid datatype spec (empty type) for column %', p_src_schema, dt_table, target_col;
      CONTINUE;
    END IF;

    IF dt_len > 0 THEN
      dtype_sql := format('%s(%s)', dt_name, dt_len);
    ELSE
      dtype_sql := dt_name;
    END IF;

    BEGIN
      EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I TYPE %s',
                     p_src_schema, dt_table, target_col, dtype_sql);
      RAISE NOTICE 'Altered datatype %.%: % TYPE %', p_src_schema, dt_table, target_col, dtype_sql;
    EXCEPTION
      WHEN others THEN
        -- If the cast is not binary-coercible, a USING clause may be needed.
        RAISE NOTICE 'Failed to alter datatype for %.% column % to %: %',
                     p_src_schema, dt_table, target_col, dtype_sql, SQLERRM;
    END;
  END LOOP;
END;
$$;

COMMENT ON FUNCTION synchdb_apply_column_mappings(name, name, name, name) IS
   'transform column name mappings based on synchdb_objmap';

CREATE OR REPLACE FUNCTION synchdb_migrate_data_with_transforms(
    p_src_schema        name,                 -- e.g. 'ora_stage'
    p_connector_name    name,                 -- connector for stats + objmap filter
    p_dst_schema        name,                 -- e.g. 'psql_stage'
    p_desired_db        name,                 -- e.g. 'free'
    p_scn               numeric,              -- SCN used during this snapshot run
    p_desired_schema    name DEFAULT NULL,    -- e.g. 'dbzuser'
    p_do_truncate       boolean DEFAULT false,
    p_rows_per_tick     integer DEFAULT 0,    -- 0/NULL = no batching; >0 = stats every N rows
    p_continue_on_error boolean DEFAULT true,
    p_batch_subxact     boolean DEFAULT true  -- only used when batching
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r                 record;
  dst_list          text;
  src_list          text;
  v_rows            bigint;
  v_total           bigint;
  v_off             bigint;
  ins_sql           text;

  err_state         text;
  err_msg           text;

  v_started_at      timestamptz := now();

  -- per-connector error table name, created lazily on first failure
  v_err_tbl_ident   text;       -- 'synchdb_fdw_snapshot_errors_<connector>'
  v_err_tbl_created boolean := false;

  v_tbl_display     text;       -- db[.schema].table display name

  v_err_tbl_exists  boolean;    -- for conditional delete without creating the table
  v_err_count       bigint;     -- summary count at end

  v_any_batch_failed boolean;   -- tracks data failures in batching mode
BEGIN
  -- sanitize connector name to identifier suffix
  v_err_tbl_ident :=
      'synchdb_fdw_snapshot_errors_'
      || regexp_replace(lower(p_connector_name::text), '[^a-z0-9_]', '_', 'g');

  -- helper to check if the per-connector error table already exists (in public)
  WITH tbl AS (
    SELECT EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND c.relname = v_err_tbl_ident
    ) AS exists
  )
  SELECT exists INTO v_err_tbl_exists FROM tbl;

  FOR r IN
    SELECT lower(c.relname) AS tbl
    FROM   pg_foreign_table ft
    JOIN   pg_class        c ON c.oid = ft.ftrelid
    JOIN   pg_namespace    n ON n.oid = c.relnamespace
    WHERE  n.nspname = p_src_schema
      AND  EXISTS (
             SELECT 1
             FROM pg_class c2
             JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
             WHERE n2.nspname = p_dst_schema
               AND lower(c2.relname) = lower(c.relname)
               AND c2.relkind = 'r'
           )
  LOOP
    v_tbl_display :=
      CASE
        WHEN p_desired_schema IS NULL OR btrim(p_desired_schema::text) = ''
          THEN lower(p_desired_db::text) || '.' || r.tbl
        ELSE lower(p_desired_db::text) || '.' || lower(p_desired_schema::text) || '.' || r.tbl
      END;

    v_any_batch_failed := false;

    BEGIN
      -- Build column/expr lists
      WITH dst_cols AS (
        SELECT c.ordinal_position, lower(c.column_name) AS dst_col
        FROM information_schema.columns c
        WHERE c.table_schema = p_dst_schema
          AND c.table_name   = r.tbl
      ),
      colmap AS (
        SELECT
          lower(split_part(m.srcobj,'.',2)) AS tbl,
          lower(split_part(m.srcobj,'.',3)) AS src_col,
          (regexp_split_to_array(lower(m.dstobj), '\.'))[
            array_length(regexp_split_to_array(lower(m.dstobj), '\.'),1)
          ] AS dst_col
        FROM synchdb_objmap AS m
        WHERE m.objtype='column' AND m.enabled
          AND m.name = p_connector_name
          AND lower(split_part(m.srcobj,'.',1)) = lower(p_src_schema)
          AND m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''

        UNION ALL

        SELECT
          CASE WHEN array_length(arr,1)=4 THEN arr[3]
               WHEN array_length(arr,1)=3 THEN arr[2] END AS tbl,
          arr[array_length(arr,1)]                        AS src_col,
          (regexp_split_to_array(lower(m2.dstobj), '\.'))[
            array_length(regexp_split_to_array(lower(m2.dstobj), '\.'),1)
          ] AS dst_col
        FROM (
          SELECT regexp_split_to_array(lower(srcobj), '\.') AS arr, dstobj
          FROM synchdb_objmap
          WHERE objtype='column' AND enabled
            AND name = p_connector_name
            AND dstobj IS NOT NULL AND btrim(dstobj) <> ''
        ) m2
        WHERE arr[1] = lower(p_desired_db)
          AND (
            p_desired_schema IS NULL OR p_desired_schema = ''
            OR array_length(arr,1)=3
            OR arr[2] = lower(p_desired_schema)
          )
      ),
      col_map AS (
        SELECT d.ordinal_position,
               d.dst_col,
               COALESCE(
                 (SELECT cm.src_col
                    FROM colmap cm
                   WHERE cm.tbl = r.tbl AND cm.dst_col = d.dst_col
                   LIMIT 1),
                 d.dst_col
               ) AS src_col
        FROM dst_cols d
      ),
      src_presence AS (
        SELECT lower(table_name) AS tbl, lower(column_name) AS src_col
        FROM information_schema.columns
        WHERE table_schema = p_src_schema
      ),
      tmap AS (
        SELECT
          lower(split_part(mt.srcobj,'.',2)) AS tbl,
          lower(split_part(mt.srcobj,'.',3)) AS src_col,
          mt.dstobj                          AS expr
        FROM synchdb_objmap AS mt
        WHERE mt.objtype='transform' AND mt.enabled
          AND mt.name = p_connector_name
          AND lower(split_part(mt.srcobj,'.',1)) = lower(p_src_schema)

        UNION ALL

        SELECT
          CASE WHEN array_length(arr,1)=4 THEN arr[3]
               WHEN array_length(arr,1)=3 THEN arr[2] END AS tbl,
          arr[array_length(arr,1)]                        AS src_col,
          t.dstobj                                        AS expr
        FROM (
          SELECT regexp_split_to_array(lower(srcobj), '\.') AS arr, dstobj
          FROM synchdb_objmap
          WHERE objtype='transform' AND enabled
            AND name = p_connector_name
            AND dstobj IS NOT NULL AND btrim(dstobj) <> ''
        ) t
        WHERE arr[1] = lower(p_desired_db)
          AND (
            p_desired_schema IS NULL OR p_desired_schema = ''
            OR array_length(arr,1)=3
            OR arr[2] = lower(p_desired_schema)
          )
      ),
      exprs AS (
        SELECT
          m.ordinal_position,
          m.dst_col,
          CASE
            WHEN NOT EXISTS (
              SELECT 1 FROM src_presence sp
               WHERE sp.tbl = r.tbl AND sp.src_col = m.src_col
            )
              THEN 'NULL'
            ELSE COALESCE(
                   (SELECT replace(tt.expr, '%d', quote_ident(m.src_col))
                      FROM tmap tt
                     WHERE tt.tbl = r.tbl AND tt.src_col = m.src_col
                     LIMIT 1),
                   quote_ident(m.src_col)
                 )
          END AS src_expr
        FROM col_map m
      )
      SELECT
        string_agg(quote_ident(dst_col), ', ' ORDER BY ordinal_position),
        string_agg(src_expr,              ', ' ORDER BY ordinal_position)
      INTO dst_list, src_list
      FROM exprs;

      IF dst_list IS NULL OR src_list IS NULL THEN
        RAISE NOTICE 'Skipping %.%: no column metadata', p_dst_schema, r.tbl;
        RETURN;
      END IF;

      -- Optional truncate
      IF p_do_truncate THEN
        BEGIN
          EXECUTE format('TRUNCATE %I.%I', p_dst_schema, r.tbl);
        EXCEPTION WHEN OTHERS THEN
          -- record failure (UPSERT)
          IF NOT v_err_tbl_created THEN
            EXECUTE format(
              'CREATE TABLE IF NOT EXISTS %I.%I (
                 connector_name name        NOT NULL,
                 tbl            text        NOT NULL,
                 err_state      text        NOT NULL,
                 err_msg        text        NOT NULL,
                 scn            numeric     NOT NULL,
                 ts             timestamptz NOT NULL DEFAULT now(),
                 CONSTRAINT %I UNIQUE (connector_name, tbl)
               )',
              'public', v_err_tbl_ident, ('uq_'||v_err_tbl_ident)
            );
            -- after creation, mark both flags
            v_err_tbl_created := true;
            v_err_tbl_exists  := true;
          END IF;

          GET STACKED DIAGNOSTICS err_state = RETURNED_SQLSTATE,
                                  err_msg   = MESSAGE_TEXT;

          EXECUTE format(
            'INSERT INTO %I.%I (connector_name, tbl, err_state, err_msg, scn)
             VALUES ($1,$2,$3,$4,$5)
             ON CONFLICT (connector_name, tbl)
             DO UPDATE SET err_state = EXCLUDED.err_state,
                           err_msg   = EXCLUDED.err_msg,
                           scn       = EXCLUDED.scn,
                           ts        = now()',
            'public', v_err_tbl_ident
          )
          USING p_connector_name, v_tbl_display, err_state, err_msg, p_scn;

          IF NOT p_continue_on_error THEN
            RAISE;
          ELSE
            RAISE WARNING 'TRUNCATE %.% failed: % [%]', p_dst_schema, r.tbl, err_msg, err_state;
          END IF;
        END;
      END IF;

      -- Insert path
      IF COALESCE(p_rows_per_tick,0) <= 0 THEN
        EXECUTE format(
          'INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I',
          p_dst_schema, r.tbl, dst_list, src_list, p_src_schema, r.tbl
        );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        PERFORM synchdb_set_snapstats(p_connector_name, 0::bigint, v_rows::bigint, 0::bigint, 0::bigint);
        RAISE NOTICE 'Loaded %.% from %.% (rows=%)', p_dst_schema, r.tbl, p_src_schema, r.tbl, v_rows;

        -- success for the whole table (non-batch): delete error row if table exists
        IF v_err_tbl_exists THEN
          EXECUTE format(
            'DELETE FROM %I.%I WHERE connector_name = $1 AND tbl = $2',
            'public', v_err_tbl_ident
          )
          USING p_connector_name, v_tbl_display;
        END IF;

      ELSE
        -- Batching path
        EXECUTE format(
          'SELECT count(*) FROM (SELECT %s FROM %I.%I) q',
          src_list, p_src_schema, r.tbl
        )
        INTO v_total;

        v_off := 0;
        WHILE v_off < v_total LOOP
          ins_sql := format(
            $sql$
            INSERT INTO %I.%I (%s)
            SELECT %s
            FROM (
              SELECT %s, row_number() OVER () AS rn
              FROM %I.%I
            ) t
            WHERE t.rn > %s AND t.rn <= %s
            $sql$,
            p_dst_schema, r.tbl,
            dst_list,
            src_list,
            src_list,
            p_src_schema, r.tbl,
            v_off, v_off + p_rows_per_tick
          );

          IF p_batch_subxact THEN
            BEGIN
              EXECUTE ins_sql;
              GET DIAGNOSTICS v_rows = ROW_COUNT;
            EXCEPTION WHEN OTHERS THEN
              v_any_batch_failed := true;

              IF NOT v_err_tbl_created THEN
                EXECUTE format(
                  'CREATE TABLE IF NOT EXISTS %I.%I (
                     connector_name name        NOT NULL,
                     tbl            text        NOT NULL,
                     err_state      text        NOT NULL,
                     err_msg        text        NOT NULL,
                     scn            numeric     NOT NULL,
                     ts             timestamptz NOT NULL DEFAULT now(),
                     CONSTRAINT %I UNIQUE (connector_name, tbl)
                   )',
                  'public', v_err_tbl_ident, ('uq_'||v_err_tbl_ident)
                );
                v_err_tbl_created := true;
                v_err_tbl_exists  := true;
              END IF;

              GET STACKED DIAGNOSTICS err_state = RETURNED_SQLSTATE,
                                      err_msg   = MESSAGE_TEXT;

              EXECUTE format(
                'INSERT INTO %I.%I (connector_name, tbl, err_state, err_msg, scn)
                 VALUES ($1,$2,$3,$4,$5)
                 ON CONFLICT (connector_name, tbl)
                 DO UPDATE SET err_state = EXCLUDED.err_state,
                               err_msg   = EXCLUDED.err_msg,
                               scn       = EXCLUDED.scn,
                               ts        = now()',
                'public', v_err_tbl_ident
              )
              USING p_connector_name, v_tbl_display, err_state, err_msg, p_scn;

              IF NOT p_continue_on_error THEN
                RAISE;
              ELSE
                RAISE WARNING 'Batch insert %.% rows (%-%) failed: % [%]',
                              p_dst_schema, r.tbl, v_off+1, v_off+p_rows_per_tick, err_msg, err_state;
                v_rows := 0;
              END IF;
            END;
          ELSE
            EXECUTE ins_sql;
            GET DIAGNOSTICS v_rows = ROW_COUNT;
          END IF;

          IF v_rows > 0 THEN
            PERFORM synchdb_set_snapstats(p_connector_name, 0::bigint, v_rows::bigint, 0::bigint, 0::bigint);
            RAISE NOTICE 'Loaded batch: %.% (+% rows, offset % / %)',
                         p_dst_schema, r.tbl, v_rows, v_off, v_total;
          END IF;

          v_off := v_off + p_rows_per_tick;
        END LOOP;

        -- If no batch failed, delete error row for this table (if the table exists)
        IF NOT v_any_batch_failed AND v_err_tbl_exists THEN
          EXECUTE format(
            'DELETE FROM %I.%I WHERE connector_name = $1 AND tbl = $2',
            'public', v_err_tbl_ident
          )
          USING p_connector_name, v_tbl_display;
        END IF;
      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        -- record failure (UPSERT)
        IF NOT v_err_tbl_created THEN
          EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I (
               connector_name name        NOT NULL,
               tbl            text        NOT NULL,
               err_state      text        NOT NULL,
               err_msg        text        NOT NULL,
               scn            numeric     NOT NULL,
               ts             timestamptz NOT NULL DEFAULT now(),
               CONSTRAINT %I UNIQUE (connector_name, tbl)
             )',
            'public', v_err_tbl_ident, ('uq_'||v_err_tbl_ident)
          );
          v_err_tbl_created := true;
          v_err_tbl_exists  := true;
        END IF;

        GET STACKED DIAGNOSTICS err_state = RETURNED_SQLSTATE,
                                err_msg   = MESSAGE_TEXT;

        EXECUTE format(
          'INSERT INTO %I.%I (connector_name, tbl, err_state, err_msg, scn)
           VALUES ($1,$2,$3,$4,$5)
           ON CONFLICT (connector_name, tbl)
           DO UPDATE SET err_state = EXCLUDED.err_state,
                         err_msg   = EXCLUDED.err_msg,
                         scn       = EXCLUDED.scn,
                         ts        = now()',
          'public', v_err_tbl_ident
        )
        USING p_connector_name, v_tbl_display, err_state, err_msg, p_scn;

        IF NOT p_continue_on_error THEN
          RAISE;
        ELSE
          RAISE WARNING 'Table %.% failed, continuing: % [%]',
                        p_dst_schema, r.tbl, err_msg, err_state;
        END IF;
    END; -- end per-table subtransaction
  END LOOP;

  -- Final summary (only if we created the error table this run OR it pre-existed)
  IF v_err_tbl_created OR v_err_tbl_exists THEN
    EXECUTE format(
      'SELECT count(*)
         FROM %I.%I
        WHERE connector_name = $1
          AND ts >= $2',
      'public', v_err_tbl_ident
    )
    INTO v_err_count
    USING p_connector_name, v_started_at;

    RAISE NOTICE 'Migration finished. Errors recorded this run: %', v_err_count;
  ELSE
    RAISE NOTICE 'Migration finished. No errors recorded this run.';
  END IF;
END;
$$;

COMMENT ON FUNCTION synchdb_migrate_data_with_transforms(name, name, name, name, numeric, name, boolean, int, boolean, boolean) IS
   'migrate data while applying transform expressions if available - sub-transaction mode';

CREATE OR REPLACE FUNCTION synchdb_migrate_data_with_transforms_nosubs(
    p_src_schema      name,                 -- e.g. 'ora_stage'
    p_connector_name  name,                 -- connector for stats + objmap filter
    p_dst_schema      name,                 -- e.g. 'psql_stage'
    p_desired_db      name,                 -- e.g. 'free'
    p_desired_schema  name DEFAULT NULL,    -- e.g. 'dbzuser'
    p_do_truncate     boolean DEFAULT false,
    p_rows_per_tick   integer DEFAULT 0     -- 0/NULL = no batching; >0 = call stats every N rows
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r           record;
  dst_list    text;
  src_list    text;
  v_rows      bigint;
  v_total     bigint;
  v_off       bigint;
  -- helpers for batching
  base_select text;
  ins_sql     text;
BEGIN
  -- Iterate all source FTs that have a same-named real table in the destination schema
  FOR r IN
    SELECT lower(c.relname) AS tbl
    FROM   pg_foreign_table ft
    JOIN   pg_class        c ON c.oid = ft.ftrelid
    JOIN   pg_namespace    n ON n.oid = c.relnamespace
    WHERE  n.nspname = p_src_schema
      AND  EXISTS (
             SELECT 1
             FROM pg_class c2
             JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
             WHERE n2.nspname = p_dst_schema
               AND lower(c2.relname) = lower(c.relname)
               AND c2.relkind = 'r'
           )
  LOOP
    -- Build per-table column lists (destination names and source expressions)
    WITH dst_cols AS (
      SELECT c.ordinal_position, lower(c.column_name) AS dst_col
      FROM information_schema.columns c
      WHERE c.table_schema = p_dst_schema
        AND c.table_name   = r.tbl
    ),
    colmap AS (
      -- A) schema-qualified rules: p_src_schema.table.column -> <dst column>
      SELECT
        lower(split_part(m.srcobj,'.',2)) AS tbl,
        lower(split_part(m.srcobj,'.',3)) AS src_col,
        (regexp_split_to_array(lower(m.dstobj), '\.'))[
          array_length(regexp_split_to_array(lower(m.dstobj), '\.'),1)
        ] AS dst_col
      FROM synchdb_objmap AS m
      WHERE m.objtype='column' AND m.enabled
        AND m.name = p_connector_name
        AND lower(split_part(m.srcobj,'.',1)) = lower(p_src_schema)
        AND m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''

      UNION ALL

      -- B) db-prefixed rules: db.schema.table.column OR db.table.column -> <dst column>
      SELECT
        CASE WHEN array_length(arr,1)=4 THEN arr[3]
             WHEN array_length(arr,1)=3 THEN arr[2] END AS tbl,
        arr[array_length(arr,1)]                        AS src_col,
        (regexp_split_to_array(lower(m2.dstobj), '\.'))[
          array_length(regexp_split_to_array(lower(m2.dstobj), '\.'),1)
        ] AS dst_col
      FROM (
        SELECT regexp_split_to_array(lower(srcobj), '\.') AS arr, dstobj
        FROM synchdb_objmap
        WHERE objtype='column' AND enabled
          AND name = p_connector_name
          AND dstobj IS NOT NULL AND btrim(dstobj) <> ''
      ) m2
      WHERE arr[1] = lower(p_desired_db)
        AND (
          p_desired_schema IS NULL OR p_desired_schema = ''
          OR array_length(arr,1)=3
          OR arr[2] = lower(p_desired_schema)
        )
    ),
    col_map AS (
      SELECT d.ordinal_position,
             d.dst_col,
             COALESCE(
               (SELECT cm.src_col FROM colmap cm WHERE cm.tbl = r.tbl AND cm.dst_col = d.dst_col LIMIT 1),
               d.dst_col
             ) AS src_col
      FROM dst_cols d
    ),
    src_presence AS (
      SELECT lower(table_name) AS tbl, lower(column_name) AS src_col
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
    ),
    tmap AS (
      -- A) schema-qualified
      SELECT
        lower(split_part(mt.srcobj,'.',2)) AS tbl,
        lower(split_part(mt.srcobj,'.',3)) AS src_col,
        mt.dstobj                          AS expr
      FROM synchdb_objmap AS mt
      WHERE mt.objtype='transform' AND mt.enabled
        AND mt.name = p_connector_name
        AND lower(split_part(mt.srcobj,'.',1)) = lower(p_src_schema)

      UNION ALL
      -- B) db-prefixed
      SELECT
        CASE WHEN array_length(arr,1)=4 THEN arr[3]
             WHEN array_length(arr,1)=3 THEN arr[2] END AS tbl,
        arr[array_length(arr,1)]                        AS src_col,
        t.dstobj                                        AS expr
      FROM (
        SELECT regexp_split_to_array(lower(srcobj), '\.') AS arr, dstobj
        FROM synchdb_objmap
        WHERE objtype='transform' AND enabled
          AND name = p_connector_name
          AND dstobj IS NOT NULL AND btrim(dstobj) <> ''
      ) t
      WHERE arr[1] = lower(p_desired_db)
        AND (
          p_desired_schema IS NULL OR p_desired_schema = ''
          OR array_length(arr,1)=3
          OR arr[2] = lower(p_desired_schema)
        )
    ),
    exprs AS (
      SELECT
        m.ordinal_position,
        m.dst_col,
        CASE
          WHEN NOT EXISTS (
            SELECT 1 FROM src_presence sp WHERE sp.tbl = r.tbl AND sp.src_col = m.src_col
          )
            THEN 'NULL'
          ELSE COALESCE(
                 (SELECT replace(tt.expr, '%d', quote_ident(m.src_col))
                  FROM tmap tt
                  WHERE tt.tbl = r.tbl AND tt.src_col = m.src_col
                  LIMIT 1),
                 quote_ident(m.src_col)
               )
        END AS src_expr
      FROM col_map m
    )
    SELECT
      string_agg(quote_ident(dst_col), ', ' ORDER BY ordinal_position),
      string_agg(src_expr,              ', ' ORDER BY ordinal_position)
    INTO dst_list, src_list
    FROM exprs;

    IF dst_list IS NULL OR src_list IS NULL THEN
      RAISE NOTICE 'Skipping %.%: no column metadata', p_dst_schema, r.tbl;
      CONTINUE;
    END IF;

    IF p_do_truncate THEN
      EXECUTE format('TRUNCATE %I.%I', p_dst_schema, r.tbl);
    END IF;

    -- === No batching: one-shot insert, then report actual row count ===
    IF COALESCE(p_rows_per_tick, 0) <= 0 THEN
      EXECUTE format(
        'INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I',
        p_dst_schema, r.tbl, dst_list, src_list, p_src_schema, r.tbl
      );
      GET DIAGNOSTICS v_rows = ROW_COUNT;
      -- increment rows counter once for this table
      PERFORM synchdb_set_snapstats(p_connector_name, 0::bigint, v_rows::bigint, 0::bigint, 0::bigint);
      RAISE NOTICE 'Loaded %.% from %.% (rows=%)', p_dst_schema, r.tbl, p_src_schema, r.tbl, v_rows;

    ELSE
      -- === Batching mode: insert in chunks; call stats after each chunk ===
      -- 1) Count total rows to process using the same SELECT list
      EXECUTE format(
        'SELECT count(*) FROM (SELECT %s FROM %I.%I) q',
        src_list, p_src_schema, r.tbl
      )
      INTO v_total;

      v_off := 0;
      WHILE v_off < v_total LOOP
        ins_sql := format(
          $sql$
          INSERT INTO %I.%I (%s)
          SELECT %s
          FROM (
            SELECT %s, row_number() OVER () AS rn
            FROM %I.%I
          ) t
          WHERE t.rn > %s AND t.rn <= %s
          $sql$,
          p_dst_schema, r.tbl,
          dst_list,
          src_list,
          src_list,
          p_src_schema, r.tbl,
          v_off, v_off + p_rows_per_tick
        );

        EXECUTE ins_sql;
        GET DIAGNOSTICS v_rows = ROW_COUNT;

        IF v_rows > 0 THEN
          PERFORM synchdb_set_snapstats(p_connector_name, 0::bigint, v_rows::bigint, 0::bigint, 0::bigint);
          RAISE NOTICE 'Loaded batch: %.% (+% rows, offset % / %)',
                       p_dst_schema, r.tbl, v_rows, v_off, v_total;
        END IF;

        v_off := v_off + p_rows_per_tick;
        -- Optional: exit early if no rows inserted (safety)
        IF v_rows = 0 THEN
          EXIT;
        END IF;
      END LOOP;
    END IF;

  END LOOP;
END;
$$;

COMMENT ON FUNCTION synchdb_migrate_data_with_transforms_nosubs(name, name, name, name, name, boolean, int) IS
   'migrate data while applying transform expressions if available - all or nothing mode';

CREATE OR REPLACE FUNCTION synchdb_apply_table_mappings(
    p_src_schema      name,               -- e.g. 'psql_stage'
    p_connector_name  name,               -- filter objmap rows by connector name
    p_desired_db      name,               -- e.g. 'free' (must match the db token in srcobj)
    p_desired_schema  name DEFAULT NULL   -- e.g. 'dbzuser' (only enforced if srcobj includes a schema)
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r RECORD;

  parts    text[];
  s_db     text;
  s_schema text;
  s_table  text;

  d_schema text;
  d_table  text;
BEGIN
  FOR r IN
    SELECT m.srcobj, m.dstobj
    FROM   synchdb_objmap AS m
    WHERE  m.objtype = 'table'
      AND  m.enabled
      AND  m.name = p_connector_name
      AND  m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''
  LOOP
    -- Parse srcobj as db(.schema).table
    parts := regexp_split_to_array(lower(r.srcobj), '\.');
    s_db := NULL; s_schema := NULL; s_table := NULL;

    IF array_length(parts,1) = 3 THEN
      s_db := parts[1]; s_schema := parts[2]; s_table := parts[3];
    ELSIF array_length(parts,1) = 2 THEN
      s_db := parts[1]; s_table := parts[2];         -- no schema segment
    ELSE
      CONTINUE;                                      -- unsupported form
    END IF;

    -- Require matching database
    IF p_desired_db IS NOT NULL AND s_db IS DISTINCT FROM lower(p_desired_db) THEN
      CONTINUE;
    END IF;

    -- Only enforce desired schema if a schema segment exists in srcobj
    IF p_desired_schema IS NOT NULL
       AND s_schema IS NOT NULL
       AND s_schema <> lower(p_desired_schema) THEN
      CONTINUE;
    END IF;

    -- Parse destination: "schema.table" or just "table" (defaults to public)
    IF strpos(r.dstobj, '.') > 0 THEN
      d_schema := lower(split_part(r.dstobj, '.', 1));
      d_table  := lower(split_part(r.dstobj, '.', 2));
    ELSE
      d_schema := 'public';
      d_table  := lower(r.dstobj);
    END IF;

    -- Only act if the source table exists in p_src_schema
    IF NOT EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = p_src_schema
        AND c.relkind = 'r'
        AND lower(c.relname) = s_table
    ) THEN
      CONTINUE;
    END IF;

    -- Ensure destination schema
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', d_schema);

    -- Skip if final destination already exists
    IF EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = d_schema
        AND c.relkind = 'r'
        AND lower(c.relname) = d_table
    ) THEN
      RAISE NOTICE 'Skipping % -> %.%: destination exists', s_table, d_schema, d_table;
      CONTINUE;
    END IF;

    -- Same-schema rename vs. cross-schema move (rename first to avoid collisions)
    IF p_src_schema = d_schema THEN
      IF s_table <> d_table THEN
        EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', p_src_schema, s_table, d_table);
      END IF;
    ELSE
      IF s_table <> d_table THEN
        EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', p_src_schema, s_table, d_table);
        EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I', p_src_schema, d_table, d_schema);
      ELSE
        EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I', p_src_schema, s_table, d_schema);
      END IF;
    END IF;
  END LOOP;
END;
$$;

COMMENT ON FUNCTION synchdb_apply_table_mappings(name, name, name, name) IS
   'transform table names according to synchdb_objmap';

CREATE OR REPLACE FUNCTION synchdb_finalize_initial_snapshot(
    p_source_schema  name,              -- e.g. 'ora_obj'
    p_stage_schema   name,              -- e.g. 'ora_stage'
    p_connector_name name,              -- e.g. 'ora19cconn'  -> server 'ora19cconn_oracle'
	p_meta_schema	 name
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_server  text := format('%s_oracle', p_connector_name);
    r         record;

	-- error table handling
    v_err_tbl_ident    text;      -- synchdb_fdw_snapshot_errors_<sanitized connector>
    v_err_tbl_exists   boolean;
    v_err_count        bigint;
BEGIN
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
    -- 4) Drop user mappings for server <connector>_oracle (if the server exists)
    ----------------------------------------------------------------------
    IF EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = v_server) THEN
        FOR r IN
            SELECT um.usename
            FROM pg_user_mappings um
            JOIN pg_foreign_server fs ON fs.oid = um.srvid
            WHERE fs.srvname = v_server
        LOOP
            IF r.usename = 'PUBLIC' THEN
                RAISE NOTICE 'Dropping USER MAPPING FOR PUBLIC on server "%" ', v_server;
                EXECUTE format('DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER %I', v_server);
            ELSE
                RAISE NOTICE 'Dropping USER MAPPING FOR "%" on server "%" ', r.usename, v_server;
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
    ELSE
        -- not present: nothing to do
        NULL;
    END IF;
END;
$$;

COMMENT ON FUNCTION synchdb_finalize_initial_snapshot(name, name, name, name) IS
   'finalize initial snapshot by cleaning up resources and objects';

CREATE OR REPLACE FUNCTION synchdb_do_initial_snapshot(
    p_connector_name  name,               -- e.g. 'oracleconn'
    p_secret          text,               -- master key for decrypting connector password
    p_source_schema   name,               -- e.g. 'ora_obj'   (FDW objects + current_scn FT)
    p_stage_schema    name,               -- e.g. 'ora_stage' (staging foreign tables)
    p_dest_schema     name,               -- e.g. 'dst_stage' (materialized tables)
    p_lookup_db       text,               -- e.g. 'free'
    p_lookup_schema   name,               -- e.g. 'dbzuser'
    p_lower_names     boolean DEFAULT true,
    p_on_exists       text    DEFAULT 'replace',  -- 'replace' | 'drop' | 'skip'
    p_scn             numeric DEFAULT 0,          -- >0 to force a specific SCN; else auto-read
	p_snapshot_tables text    DEFAULT null,
	p_use_subtx         boolean DEFAULT true,
	p_write_schema_hist boolean	DEFAULT false
)
RETURNS numeric
LANGUAGE plpgsql
AS $$
DECLARE
    v_scn           numeric;
    v_case_json     jsonb;
    v_effective_scn numeric;
    v_server_name   text;   -- will be set by synchdb_prepare_initial_snapshot()
	v_meta_schema 	name;
BEGIN
    ----------------------------------------------------------------------
    -- Step 0: Prepare FDW server & user mapping from connector metadata
    --         (returns the created server name, e.g. '<connector>_oracle')
    ----------------------------------------------------------------------

	PERFORM synchdb_set_snapstats(p_connector_name, 0::bigint, 0::bigint,
                                 (extract(epoch from clock_timestamp()) * 1000)::bigint, 0::bigint);

    v_server_name := synchdb_prepare_initial_snapshot(p_connector_name, p_secret);
    RAISE NOTICE 'Using FDW server "%" for connector "%"', v_server_name, p_connector_name;

    -- Build the case option JSON for synchdb_create_oraviews
    v_case_json := jsonb_build_object(
        'case', CASE WHEN p_lower_names THEN 'lower' ELSE 'original' END
    );

    RAISE NOTICE 'Step 1: Creating Oracle FDW views for schema "%" using server "%"',
                 p_source_schema, v_server_name;
    PERFORM synchdb_create_oraviews(v_server_name, p_source_schema, v_case_json);

	RAISE NOTICE 'Step 1.5: Materialize foreign object views to %', v_meta_schema;
	v_meta_schema := ('metaschema_' || p_connector_name)::name;
	PERFORM synchdb_materialize_ora_metadata(p_source_schema, v_meta_schema, p_on_exists);

    RAISE NOTICE 'Step 2: Creating/ensuring current_scn foreign table exists in schema "%" using server "%"',
                 v_meta_schema, v_server_name;
    PERFORM synchdb_create_current_scn_ft(v_meta_schema, v_server_name);

    RAISE NOTICE 'Step 3: Reading current SCN value from %.current_scn', v_meta_schema;
    EXECUTE format('SELECT current_scn FROM %I.current_scn', v_meta_schema)
       INTO v_scn;

    IF (p_scn IS NULL OR p_scn <= 0) THEN
        IF v_scn IS NULL THEN
            RAISE EXCEPTION 'Unable to read current SCN from %.current_scn', v_meta_schema;
        END IF;
        v_effective_scn := v_scn;
    ELSE
        v_effective_scn := p_scn;
    END IF;

    RAISE NOTICE 'Using SCN value % for snapshot', v_effective_scn;

    RAISE NOTICE 'Step 4: Creating staging foreign tables in schema "%"', p_stage_schema;
    PERFORM synchdb_create_ora_stage_fts(
		p_connector_name,
		p_lookup_db,
		p_lookup_schema,
        p_stage_schema,
        v_server_name,
        p_lower_names,
        p_on_exists,
        v_effective_scn,
        v_meta_schema,
		p_snapshot_tables,
		p_write_schema_hist
    );

    RAISE NOTICE 'Step 5: Materializing staging foreign tables from "%"" to "%"', p_stage_schema, p_dest_schema;
    PERFORM synchdb_materialize_schema(p_connector_name, p_stage_schema, p_dest_schema, p_on_exists);

    RAISE NOTICE 'Step 6: Migrating primary keys from metadata schema "%" to "%"', p_source_schema, p_dest_schema;
    PERFORM synchdb_migrate_primary_keys(p_source_schema, p_dest_schema);

    RAISE NOTICE 'Step 7: Applying column mappings to schema "%" using objmap %.% for connector %',
                 p_dest_schema, p_lookup_db, p_lookup_schema, p_connector_name;
    PERFORM synchdb_apply_column_mappings(p_dest_schema, p_connector_name, p_lookup_db, p_lookup_schema);

    RAISE NOTICE 'Step 8: Migrating data with transforms from "%" to "%" (lookup: %.% for connector %)',
                 p_stage_schema, p_dest_schema, p_lookup_db, p_lookup_schema, p_connector_name;

    IF p_use_subtx THEN
		-- With per-table subtransactions
        PERFORM synchdb_migrate_data_with_transforms(
            p_stage_schema,
		    p_connector_name,
		    p_dest_schema,
		    p_lookup_db,
		    v_effective_scn,
		    p_lookup_schema,
		    false, 0,
		    true,
		    true
        );
    ELSE
        -- All-or-nothing (no subtransactions)
        PERFORM synchdb_migrate_data_with_transforms_nosubs(
            p_stage_schema,
		    p_connector_name,
		    p_dest_schema,
		    p_lookup_db,
		    p_lookup_schema,
		    false,
		    0
	    );
    END IF;

    RAISE NOTICE 'Step 9: Applying table mappings on destination schema "%" (lookup: %.% for connector %)',
                 p_dest_schema, p_lookup_db, p_lookup_schema, p_connector_name;
    PERFORM synchdb_apply_table_mappings(p_dest_schema, p_connector_name, p_lookup_db, p_lookup_schema);

    RAISE NOTICE 'Initial snapshot completed successfully at SCN %', v_effective_scn;

    PERFORM synchdb_finalize_initial_snapshot(p_source_schema, p_stage_schema, p_connector_name, v_meta_schema);

    PERFORM synchdb_set_snapstats(p_connector_name, 0::bigint, 0::bigint, 0::bigint,
                                  (extract(epoch from clock_timestamp()) * 1000)::bigint);
    RETURN v_effective_scn;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'synchdb_do_initial_snapshot() failed: % [%]', SQLERRM, SQLSTATE
            USING HINT = 'Check NOTICE logs above to identify which step failed.';
END;
$$;

COMMENT ON FUNCTION synchdb_do_initial_snapshot(name, text, name, name, name, text, name, boolean, text, numeric, text, boolean, boolean) IS
   'perform initial snapshot procedure + data transforms using a oracle_fdw server';

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
    p_scn             numeric DEFAULT 0,          -- >0 to force a specific SCN; else auto-read
	p_snapshot_tables text    DEFAULT null,
	p_use_subtx         boolean DEFAULT true,
	p_write_schema_hist boolean	DEFAULT false
)
RETURNS numeric
LANGUAGE plpgsql
AS $$
DECLARE
    v_scn           numeric;
    v_case_json     jsonb;
    v_effective_scn numeric;
    v_server_name   text;   -- will be set by synchdb_prepare_initial_snapshot()
	v_meta_schema   name;
BEGIN
    ----------------------------------------------------------------------
    -- Step 0: Prepare FDW server & user mapping from connector metadata
    --         (returns the created server name, e.g. '<connector>_oracle')
    ----------------------------------------------------------------------
	PERFORM synchdb_set_snapstats(p_connector_name, 0::bigint, 0::bigint,
                                 (extract(epoch from clock_timestamp()) * 1000)::bigint, 0::bigint);
    
    v_server_name := synchdb_prepare_initial_snapshot(p_connector_name, p_secret);
    RAISE NOTICE 'Using FDW server "%" for connector "%"', v_server_name, p_connector_name;

    -- Build the case option JSON for synchdb_create_oraviews
    v_case_json := jsonb_build_object(
        'case', CASE WHEN p_lower_names THEN 'lower' ELSE 'original' END
    );

    RAISE NOTICE 'Step 1: Creating Oracle FDW views for schema "%" using server "%"',
                 p_source_schema, v_server_name;
    PERFORM synchdb_create_oraviews(v_server_name, p_source_schema, v_case_json);

    RAISE NOTICE 'Step 1.5: Materialize foreign object views to %', v_meta_schema;
    v_meta_schema := ('metaschema_' || p_connector_name)::name;
    PERFORM synchdb_materialize_ora_metadata(p_source_schema, v_meta_schema, p_on_exists);

    RAISE NOTICE 'Step 2: Creating/ensuring current_scn foreign table exists in schema "%" using server "%"',
                 v_meta_schema, v_server_name;
    PERFORM synchdb_create_current_scn_ft(v_meta_schema, v_server_name);

    RAISE NOTICE 'Step 3: Reading current SCN value from %.current_scn', v_meta_schema;
    EXECUTE format('SELECT current_scn FROM %I.current_scn', v_meta_schema)
       INTO v_scn;

    IF (p_scn IS NULL OR p_scn <= 0) THEN
        IF v_scn IS NULL THEN
            RAISE EXCEPTION 'Unable to read current SCN from %.current_scn', v_meta_schema;
        END IF;
        v_effective_scn := v_scn;
    ELSE
        v_effective_scn := p_scn;
    END IF;

    RAISE NOTICE 'Using SCN value % for snapshot', v_effective_scn;

    RAISE NOTICE 'Step 4: Creating staging foreign tables in schema "%"', p_stage_schema;
    PERFORM synchdb_create_ora_stage_fts(
		p_connector_name,
		p_lookup_db,
		p_lookup_schema,
		p_stage_schema,
        v_server_name,
        p_lower_names,
        p_on_exists,
        v_effective_scn,
        v_meta_schema,
		p_snapshot_tables,
		p_write_schema_hist
    );

    RAISE NOTICE 'Step 5: Materializing staging foreign tables from "%"" to "%"', p_stage_schema, p_dest_schema;
    PERFORM synchdb_materialize_schema(p_connector_name, p_stage_schema, p_dest_schema, p_on_exists);

    RAISE NOTICE 'Step 6: Migrating primary keys from metadata schema "%" to "%"', p_source_schema, p_dest_schema;
    PERFORM synchdb_migrate_primary_keys(p_source_schema, p_dest_schema);

    RAISE NOTICE 'Step 7: Applying column mappings to schema "%" using objmap %.% for connector %',
                 p_dest_schema, p_lookup_db, p_lookup_schema, p_connector_name;
    PERFORM synchdb_apply_column_mappings(p_dest_schema, p_connector_name, p_lookup_db, p_lookup_schema);

    RAISE NOTICE 'Step 8: Applying table mappings on destination schema "%" (lookup: %.% for connector %)',
                 p_dest_schema, p_lookup_db, p_lookup_schema, p_connector_name;
    PERFORM synchdb_apply_table_mappings(p_dest_schema, p_connector_name, p_lookup_db, p_lookup_schema);

    RAISE NOTICE 'schema sync completed successfully at SCN %', v_effective_scn;

    PERFORM synchdb_finalize_initial_snapshot(p_source_schema, p_stage_schema, p_connector_name, v_meta_schema);

    PERFORM synchdb_set_snapstats(p_connector_name, 0::bigint, 0::bigint, 0::bigint,
                                  (extract(epoch from clock_timestamp()) * 1000)::bigint);
    RETURN v_effective_scn;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'synchdb_do_schema_sync() failed: % [%]', SQLERRM, SQLSTATE
            USING HINT = 'Check NOTICE logs above to identify which step failed.';
END;
$$;

COMMENT ON FUNCTION synchdb_do_schema_sync(name, text, name, name, name, text, name, boolean, text, numeric, text, boolean, boolean) IS
   'perform schema only sync procedure + transforms using a oracle_fdw server - no data migration';

CREATE OR REPLACE FUNCTION oracle_type_to_jdbc(ora_type_text text)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  t    text;      -- normalized input
  base text;      -- base type token(s)
  m    text[];    -- regex match for (...) args
BEGIN
  -- Normalize: trim, compress spaces, uppercase, strip quotes
  t := regexp_replace(upper(btrim(ora_type_text)), '\s+', ' ', 'g');
  t := replace(replace(t, '"', ''), '''', '');

  -- Multi-word TZ types FIRST (match full text)
  IF t LIKE 'TIMESTAMP% WITH TIME ZONE%' THEN
    RETURN -101;     -- OracleTypes.TIMESTAMPTZ
  ELSIF t LIKE 'TIMESTAMP% WITH LOCAL TIME ZONE%' THEN
    RETURN -102;     -- OracleTypes.TIMESTAMPLTZ
  ELSIF t LIKE 'INTERVAL YEAR% TO MONTH%' THEN
    RETURN -103;     -- OracleTypes.INTERVALYM
  ELSIF t LIKE 'INTERVAL DAY% TO SECOND%' THEN
    RETURN -104;     -- OracleTypes.INTERVALDS
  END IF;

  -- Extract base token before any ( ... ) or trailing qualifiers
  m := regexp_match(t, '^([A-Z ]+)\s*\(');
  IF m IS NOT NULL THEN
    base := btrim(m[1]);
  ELSE
    base := t;
  END IF;

  -- Switch on base (single-word & simple multi-word that remain)
  CASE base
    -- Numeric family (Debezium sets NUMBER/DECIMAL/NUMERIC → Types.NUMERIC)
    WHEN 'NUMBER', 'DECIMAL', 'NUMERIC', 'INT', 'INTEGER', 'SMALLINT' THEN
      RETURN 2;          -- java.sql.Types.NUMERIC

    -- Floating family
    WHEN 'BINARY_FLOAT' THEN
      RETURN 100;        -- OracleTypes.BINARY_FLOAT
    WHEN 'BINARY_DOUBLE' THEN
      RETURN 101;        -- OracleTypes.BINARY_DOUBLE
    WHEN 'FLOAT' THEN
      RETURN 6;          -- java.sql.Types.FLOAT
    WHEN 'DOUBLE' THEN
      -- In Debezium grammar DOUBLE PRECISION maps to FLOAT (length set separately)
      RETURN 6;          -- java.sql.Types.FLOAT
    WHEN 'REAL' THEN
      RETURN 6;          -- Debezium maps REAL → FLOAT too

    -- Date/Time (Oracle JDBC reports DATE as TIMESTAMP)
    WHEN 'DATE' THEN
      RETURN 93;         -- java.sql.Types.TIMESTAMP
    WHEN 'TIMESTAMP' THEN
      RETURN 93;         -- java.sql.Types.TIMESTAMP

    -- Character family
    WHEN 'CHAR', 'CHARACTER' THEN
      RETURN 1;          -- CHAR
    WHEN 'NCHAR' THEN
      RETURN -15;        -- NCHAR
    WHEN 'VARCHAR2', 'VARCHAR' THEN
      RETURN 12;         -- VARCHAR
    WHEN 'NVARCHAR2', 'NVARCHAR' THEN
      RETURN -9;         -- NVARCHAR
	WHEN 'LONG' THEN
	  RETURN -1;         -- LONG

    -- LOBs
    WHEN 'BLOB' THEN
      RETURN 2004;       -- BLOB
    WHEN 'CLOB' THEN
      RETURN 2005;       -- CLOB
    WHEN 'NCLOB' THEN
      RETURN 2011;       -- NCLOB
    WHEN 'BFILE' THEN
      RETURN -13;        -- OracleTypes.BFILE

    -- Binary
    WHEN 'RAW' THEN
      RETURN -3;         -- OracleTypes.RAW (maps to BINARY)
    WHEN 'LONG RAW' THEN
      RETURN -4;         -- LONGVARBINARY

    -- Rowid (Debezium code sets ROWID → VARCHAR)
    WHEN 'ROWID', 'UROWID' THEN
      RETURN -8;         -- VARCHAR

    -- XML / Spatial / Other
    WHEN 'XMLTYPE' THEN
      RETURN 2009;       -- SQLXML (fall back to 1111 if your driver lacks it)
    WHEN 'SDO_GEOMETRY' THEN
      RETURN 1111;       -- OTHER

    -- 23ai BOOLEAN (if encountered)
    WHEN 'BOOLEAN' THEN
      RETURN 16;         -- java.sql.Types.BOOLEAN

    ELSE
      RETURN 1111;       -- OTHER (catch-all for unrecognized)
  END CASE;
END;
$$;

