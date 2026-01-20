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

CREATE OR REPLACE FUNCTION synchdb_translate_datatype(name, name, bigint, bigint, bigint) RETURNS text
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_set_snapstats(name, bigint, bigint, bigint, bigint) RETURNS void
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION read_snapshot_table_list(
    file_uri    text,
    p_conn_type  text,
    p_desired_db text
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    file_path    text;
    file_content text;
    json_data    jsonb;
    table_list   text;
    v_is_mysql   boolean;
BEGIN
    v_is_mysql := (lower(coalesce(p_conn_type, '')) = 'mysql');

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

    -- Build comma-separated list, normalizing entries:
    --   mysql: db.table
    --   non-mysql: schema.table => db.schema.table
    SELECT string_agg(norm, ',')
      INTO table_list
    FROM (
      SELECT
        CASE
          WHEN v_is_mysql THEN x
          ELSE
            CASE
              WHEN array_length(regexp_split_to_array(trim(x), '\.'), 1) = 2
              THEN format('%s.%s', p_desired_db, trim(x))  -- prepend db.
              ELSE trim(x)  -- keep 3-part (or anything else) as-is
            END
        END AS norm
      FROM jsonb_array_elements_text(json_data->'snapshot_table_list') AS t(x)
      WHERE trim(x) <> ''
    ) s;

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
	v_connector  text;      -- 'oracle' | 'olr' | 'mysql' (lowercased)
    v_hostname   text;
    v_port       int;
    v_service    text;   -- from data->>'srcdb'
    v_user       text;
    v_pwd        text;   -- decrypted password
    v_server     text;
    v_dbserver   text;   -- oracle_fdw "dbserver" option, e.g. //host:1521/SERVICE
    v_key        text;
BEGIN
    ----------------------------------------------------------------------
    -- 0) Fetch connector type and ensure we have a master key
    ----------------------------------------------------------------------
    SELECT lower(data->>'connector')
      INTO v_connector
    FROM synchdb_conninfo
    WHERE name = p_connector_name;
	
	IF v_connector IS NULL OR v_connector = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data->>connector is missing/empty', p_connector_name;
    END IF;
	
	v_key := p_master_key;
    IF v_key IS NULL OR v_key = '' THEN
        RAISE EXCEPTION 'Master key not provided.';
    END IF;

    ----------------------------------------------------------------------
    -- 1) Ensure required extensions exist (oracle_fdw and pgcrypto)
    ----------------------------------------------------------------------
	IF v_connector IN ('oracle','olr') THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'oracle_fdw') THEN
			RAISE NOTICE 'oracle_fdw not found; attempting CREATE EXTENSION';
			BEGIN
				EXECUTE 'CREATE EXTENSION oracle_fdw';
			EXCEPTION WHEN OTHERS THEN
				RAISE EXCEPTION 'Failed to install oracle_fdw: % [%]', SQLERRM, SQLSTATE
					USING HINT = 'Install oracle_fdw (and Oracle client libs) as a superuser, then retry.';
			END;
		END IF;
	ELSIF v_connector = 'mysql' THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'mysql_fdw') THEN
            RAISE NOTICE 'mysql_fdw not found; attempting CREATE EXTENSION';
            BEGIN
                EXECUTE 'CREATE EXTENSION mysql_fdw';
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to install mysql_fdw: % [%]', SQLERRM, SQLSTATE
                    USING HINT = 'Install mysql_fdw as a superuser, then retry.';
            END;
        END IF;
	ELSIF v_connector = 'postgres' THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgres_fdw') THEN
            RAISE NOTICE 'postgres_fdw not found; attempting CREATE EXTENSION';
            BEGIN
                EXECUTE 'CREATE EXTENSION postgres_fdw';
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to install postgres_fdw: % [%]', SQLERRM, SQLSTATE
                    USING HINT = 'Install postgres_fdw as a superuser, then retry.';
            END;
        END IF;
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
	v_server := format('%s_%s', p_connector_name, v_connector);	

    ----------------------------------------------------------------------
    -- 3) Recreate FDW server and user mapping with fresh info
    ----------------------------------------------------------------------
    EXECUTE format('DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER %I', v_server);
    EXECUTE format('DROP SERVER IF EXISTS %I CASCADE', v_server);

	IF v_connector IN ('oracle','olr') THEN
	
	    v_dbserver := format('//%s:%s/%s', v_hostname, v_port, v_service);
		EXECUTE format(
			'CREATE SERVER %I FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver %L)',
			v_server, v_dbserver
		);

		EXECUTE format(
			'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (user %L, password %L)',
			v_server, v_user, v_pwd
		);

		RAISE NOTICE 'Created server % and user mapping for CURRENT_USER', v_server;
	ELSIF v_connector = 'mysql' THEN
		EXECUTE format(
            'CREATE SERVER %I FOREIGN DATA WRAPPER mysql_fdw OPTIONS (host %L, port %L)',
            v_server, v_hostname, v_port::text
        );

        EXECUTE format(
            'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (username %L, password %L)',
            v_server, v_user, v_pwd
        );
	ELSIF v_connector = 'postgres' THEN
		EXECUTE format(
            'CREATE SERVER %I FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %L, dbname %L, port %L)',
            v_server, v_hostname, v_service, v_port::text
        );

        EXECUTE format(
            'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (user %L, password %L)',
            v_server, v_user, v_pwd
        );
	ELSE
        RAISE EXCEPTION 'Unsupported connector type: %', v_connector;
    END IF;
    RETURN v_server;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'synchdb_prepare_initial_snapshot(%) failed: % [%]',
                        p_connector_name, SQLERRM, SQLSTATE
            USING HINT = 'Verify designated FDWs are available and synchdb_conninfo JSON fields (hostname, port, srcdb, user, pwd) are valid; also ensure the master key is correct.';
END;
$$;

COMMENT ON FUNCTION synchdb_prepare_initial_snapshot(name, text) IS
   'check oracle_fdw and prepare foreign server and user mapping objects';

-----------------------------------------------------------------------------------------------------------------
    -- POSTGRESQL: functions to map PostgreSQL objects to PostgreSQL via FDW
-----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION synchdb_create_pg_objs(
   server   name,
   schema   name DEFAULT 'public',
   options  jsonb DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT
SET search_path = pg_catalog
AS $postgres_create_catalog$
DECLARE
    v_server name := server;
    v_schema name := schema;
BEGIN
    ----------------------------------------------------------------------
    -- Sanity check: foreign server must exist
    ----------------------------------------------------------------------
    PERFORM 1
    FROM pg_foreign_server
    WHERE srvname = v_server;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'postgres_create_catalog: foreign server "%" does not exist', v_server;
    END IF;

    ----------------------------------------------------------------------
    -- Ensure target schema exists
    ----------------------------------------------------------------------
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);

    ----------------------------------------------------------------------
    -- information_schema.columns -> <schema>.columns
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.columns (
            schema        text    OPTIONS (column_name 'table_schema'),
            table_name    text    OPTIONS (column_name 'table_name'),
            column_name   text    OPTIONS (column_name 'column_name'),
            position      integer OPTIONS (column_name 'ordinal_position'),
            type_name     text    OPTIONS (column_name 'data_type'),
            length        integer OPTIONS (column_name 'character_maximum_length'),
            precision     integer OPTIONS (column_name 'numeric_precision'),
            scale         integer OPTIONS (column_name 'numeric_scale'),
            nullable      text    OPTIONS (column_name 'is_nullable'),
            default_value text    OPTIONS (column_name 'column_default'),

			udt_schema    text    OPTIONS (column_name 'udt_schema'),
			udt_name      text    OPTIONS (column_name 'udt_name')
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'information_schema',
            table_name  'columns'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- information_schema.tables -> <schema>.tables
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.tables (
            schema     text OPTIONS (column_name 'table_schema'),
            table_name text OPTIONS (column_name 'table_name')
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'information_schema',
            table_name  'tables'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- pg_catalog.pg_namespace -> <schema>.namespaces
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.namespaces (
            oid      oid,
            nspname  name,
            nspowner oid,
            nspacl   aclitem[]
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'pg_catalog',
            table_name  'pg_namespace'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- pg_catalog.pg_class -> <schema>.classes
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.classes (
            oid           oid,
            relname       name,
            relnamespace  oid,
            reltype       oid,
            relowner      oid,
            relam         oid,
            relfilenode   oid,
            reltablespace oid,
            relpages      integer,
            reltuples     real,
            relkind       "char"
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'pg_catalog',
            table_name  'pg_class'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- pg_catalog.pg_attribute -> <schema>.attributes
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.attributes (
            attrelid      oid,
            attname       name,
            atttypid      oid,
            attstattarget integer,
            attlen        smallint,
            attnum        smallint,
            attndims      integer,
            attcacheoff   integer,
            atttypmod     integer,
            attbyval      boolean,
            attstorage    "char",
            attalign      "char",
            attnotnull    boolean,
            atthasdef     boolean,
            attisdropped  boolean,
            attislocal    boolean,
            attinhcount   integer,
            attcollation  oid
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'pg_catalog',
            table_name  'pg_attribute'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- pg_catalog.pg_constraint -> <schema>.constraints
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.constraints (
            oid           oid,
            conname       name,
            connamespace  oid,
            contype       "char",
            condeferrable boolean,
            condeferred   boolean,
            convalidated  boolean,
            conrelid      oid,
            contypid      oid,
            conindid      oid,
            conparentid   oid,
            confrelid     oid,
            confupdtype   "char",
            confdeltype   "char",
            confmatchtype "char",
            conislocal    boolean,
            coninhcount   integer,
            connoinherit  boolean,
            conkey        smallint[],
            confkey       smallint[],
            conpfeqop     oid[],
            conppeqop     oid[],
            conffeqop     oid[],
            conexclop     oid[],
            conbin        pg_node_tree
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'pg_catalog',
            table_name  'pg_constraint'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- keys view in <schema>.keys
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE OR REPLACE VIEW %1$I.keys AS
        SELECT
            upper(n.nspname)          AS schema,
            c.relname                 AS table_name,
            con.conname               AS constraint_name,
            con.condeferrable         AS deferrable,
            con.condeferred           AS deferred,
            a.attname                 AS column_name,
            ord.pos                   AS position,
            (con.contype = 'p')       AS is_primary
        FROM %1$I.constraints con
        JOIN %1$I.classes      c
          ON c.oid = con.conrelid
        JOIN %1$I.namespaces   n
          ON n.oid = c.relnamespace
        JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS ord(attnum, pos)
          ON true
        JOIN %1$I.attributes   a
          ON a.attrelid = c.oid
         AND a.attnum   = ord.attnum
        WHERE con.contype IN ('p')  -- primary keys only
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    $SQL$, v_schema);

    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.types (
            oid            oid,
            typname        name,
            typnamespace   oid,
            typlen         smallint,
            typbyval       boolean,
            typtype        "char",
            typcategory    "char",
            typispreferred boolean,
            typdelim       "char",
            typrelid       oid,
            typelem        oid,
            typarray       oid,
            typinput       regproc,
            typoutput      regproc,
            typreceive     regproc,
            typsend        regproc,
            typmodin       regproc,
            typmodout      regproc,
            typanalyze     regproc,
            typalign       "char",
            typstorage     "char",
            typnotnull     boolean,
            typbasetype    oid,
            typtypmod      integer,
            typndims       integer,
            typcollation   oid
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'pg_catalog',
            table_name  'pg_type'
        )
    $SQL$, v_schema, v_server);

    EXECUTE format($SQL$
        CREATE OR REPLACE VIEW %1$I.columns_resolved AS
        SELECT
          c.schema,
          c.table_name,
          c.column_name,
          c.position,
          c.type_name,
          c.length,
          c.precision,
          c.scale,
          c.nullable,
          c.default_value,
    
          -- carry-through so downstream SQL can use them
          c.udt_schema,
          c.udt_name,
    
          CASE WHEN c.type_name = 'ARRAY' THEN ns.nspname ELSE NULL END AS array_type_schema,
          CASE WHEN c.type_name = 'ARRAY' THEN t.typname  ELSE NULL END AS array_type_name,
    
          CASE WHEN c.type_name = 'ARRAY' THEN elemns.nspname ELSE NULL END AS element_type_schema,
          CASE WHEN c.type_name = 'ARRAY' THEN elem.typname   ELSE NULL END AS element_type_name
        FROM %1$I.columns c
        LEFT JOIN %1$I.namespaces ns
          ON ns.nspname = c.udt_schema
        LEFT JOIN %1$I.types t
          ON t.typname = c.udt_name
         AND t.typnamespace = ns.oid
        LEFT JOIN %1$I.types elem
          ON elem.oid = t.typelem
        LEFT JOIN %1$I.namespaces elemns
          ON elemns.oid = elem.typnamespace
    $SQL$, v_schema);	
    RETURN;
END;
$postgres_create_catalog$;

COMMENT ON FUNCTION synchdb_create_pg_objs(name, name, jsonb) IS
   'create PostgreSQL foreign tables for the metadata of a foreign server';

CREATE OR REPLACE FUNCTION synchdb_create_current_lsn_ft(
    p_schema name,  -- e.g. 'postgres_obj'
    p_server name   -- e.g. 'pgconn_postgres'
) RETURNS void
LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT
SET search_path = pg_catalog
AS $synchdb_create_current_lsn_ft$
DECLARE
    v_schema name := p_schema;
    v_server name := p_server;
BEGIN
    ----------------------------------------------------------------------
    -- 1) Validate foreign server existence
    ----------------------------------------------------------------------
    PERFORM 1 FROM pg_foreign_server WHERE srvname = v_server;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'synchdb_create_current_lsn_ft: foreign server "%" does not exist', v_server;
    END IF;

    ----------------------------------------------------------------------
    -- 2) Ensure schema exists
    ----------------------------------------------------------------------
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);

    ----------------------------------------------------------------------
    -- 3) Foreign table for WAL LSN snapshot access
    -- NOTE: The remote Postgres must define synchdb_wal_lsn view:
    --
    --   CREATE VIEW public.synchdb_wal_lsn AS
    --     SELECT pg_current_wal_lsn() AS wal_lsn;
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.wal_lsn (
            wal_lsn pg_lsn
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'public',
            table_name  'synchdb_wal_lsn'
        )
    $SQL$, v_schema, v_server);

    RETURN;
END;
$synchdb_create_current_lsn_ft$;

COMMENT ON FUNCTION synchdb_create_current_lsn_ft(name, name) IS
   'create PostgreSQL foreign tables for reading current LSN';

-----------------------------------------------------------------------------------------------------------------
    -- MYSQL: functions to map Mysql objects to PostgreSQL via FDW (originated from mysql_migrator project)
-----------------------------------------------------------------------------------------------------------------
CREATE FUNCTION synchdb_create_mysql_objs(
   server      name,
   schema      name    DEFAULT NAME 'public',
   options     jsonb   DEFAULT NULL
) RETURNS void
   LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT SET search_path = pg_catalog AS
$mysql_create_catalog$
DECLARE
   old_msglevel text;
   catalog_table varchar(64);
   catalog_tables varchar(64)[] := ARRAY[
      'SCHEMATA', 'TABLES', 'COLUMNS', 'TABLE_CONSTRAINTS', 'CHECK_CONSTRAINTS',
      'KEY_COLUMN_USAGE', 'REFERENTIAL_CONSTRAINTS', 'VIEWS', 'PARAMETERS',
      'STATISTICS', 'TABLE_PRIVILEGES', 'COLUMN_PRIVILEGES', 'PARTITIONS'
   ];
   sys_schemas text := $$ 'information_schema', 'mysql', 'performance_schema', 'sys' $$;

   /* schemas */
   schemas_sql text := $$
      CREATE OR REPLACE VIEW %1$I.schemas AS
         SELECT "SCHEMA_NAME" AS schema
         FROM %1$I."SCHEMATA"
         WHERE "SCHEMA_NAME" NOT IN (%2$s);
      COMMENT ON VIEW %1$I.schemas IS 'MySQL schemas on foreign server "%3$I"';
   $$;

   /* tables */
   tables_sql text := $$
      CREATE OR REPLACE VIEW %1$I.tables AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name
         FROM %1$I."TABLES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE';
      COMMENT ON VIEW %1$I.tables IS 'MySQL tables on foreign server "%3$I"';
   $$;

   /* columns */
   columns_sql text := $$
      CREATE OR REPLACE VIEW %1$I.columns AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            "COLUMN_NAME" AS column_name, "ORDINAL_POSITION" AS position,
			CASE
				WHEN "COLUMN_TYPE" ILIKE '%%unsigned%%' THEN "DATA_TYPE" || ' unsigned'
				ELSE "DATA_TYPE"
		    END AS type_name, "CHARACTER_MAXIMUM_LENGTH" AS length,
            coalesce("NUMERIC_PRECISION", "DATETIME_PRECISION", null) AS precision,
            "NUMERIC_SCALE" AS scale, "IS_NULLABLE"::boolean AS nullable,
            "COLUMN_DEFAULT" AS default_value
         FROM %1$I."COLUMNS"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s);
      COMMENT ON VIEW %1$I.columns IS 'columns of MySQL tables and views on foreign server "%3$I"';
   $$;

   /* checks */
   check_sql text := $$
      CREATE OR REPLACE VIEW %1$I.checks AS
         SELECT "CONSTRAINT_SCHEMA" AS schema, "TABLE_NAME" as table_name,
            "CONSTRAINT_NAME" AS constraint_name, false AS "deferrable", false AS deferred,
            "CHECK_CLAUSE" AS condition
         FROM %1$I."TABLE_CONSTRAINTS" cons
         JOIN %1$I."CHECK_CONSTRAINTS" cond USING ("CONSTRAINT_SCHEMA", "CONSTRAINT_NAME")
         WHERE "CONSTRAINT_SCHEMA" NOT IN (%2$s)
         --
         -- replace ENUM type by check constraints
         --
         UNION
         SELECT "TABLE_SCHEMA", "TABLE_NAME",
            concat_ws('_', "TABLE_NAME", "COLUMN_NAME", 'enum_chk')::character varying(64),
            false, false, format('(%%I IN %%s)', "COLUMN_NAME", substr("COLUMN_TYPE", 5))
         FROM %1$I."COLUMNS"
         JOIN %1$I."TABLES" USING ("TABLE_SCHEMA", "TABLE_NAME")
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE' AND "DATA_TYPE" = 'enum'
         --
         -- replace SET type by check constraints
         --
         UNION
         SELECT "TABLE_SCHEMA", "TABLE_NAME",
            concat_ws('_', "TABLE_NAME", "COLUMN_NAME", 'set_chk')::character varying(64),
            false, false, format(
               '(string_to_array(%%I, '','') <@ ARRAY[%%s])',
               "COLUMN_NAME", regexp_replace("COLUMN_TYPE", '^set\(([^)]*)\)$', '\1')
            )
         FROM %1$I."COLUMNS"
         JOIN %1$I."TABLES" USING ("TABLE_SCHEMA", "TABLE_NAME")
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE' AND "DATA_TYPE" = 'set';
      COMMENT ON VIEW %1$I.checks IS 'MySQL check constraints on foreign server "%3$I"';
   $$;

   /* foreign_keys */
   foreign_keys_sql text := $$
      CREATE OR REPLACE VIEW %1$I.foreign_keys AS
         SELECT "CONSTRAINT_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            "CONSTRAINT_NAME" AS constraint_name, false AS "deferrable", false AS deferred,
            "DELETE_RULE" AS delete_rule, "COLUMN_NAME" AS column_name, "ORDINAL_POSITION" AS position,
            "REFERENCED_TABLE_SCHEMA" AS remote_schema, keys."REFERENCED_TABLE_NAME" AS remote_table,
            "REFERENCED_COLUMN_NAME" AS remote_column
         FROM %1$I."KEY_COLUMN_USAGE" keys
         JOIN %1$I."REFERENTIAL_CONSTRAINTS" refs USING ("CONSTRAINT_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME")
         WHERE "CONSTRAINT_SCHEMA" NOT IN (%2$s)
         AND "REFERENCED_COLUMN_NAME" IS NOT NULL;
      COMMENT ON VIEW %1$I.foreign_keys IS 'MySQL foreign key columns on foreign server "%3$I"';
   $$;

   /* keys */
   keys_sql text := $$
      CREATE OR REPLACE VIEW %1$I.keys AS
         SELECT "CONSTRAINT_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            (CASE WHEN "CONSTRAINT_NAME" = 'PRIMARY'
               THEN concat_ws('_', "TABLE_NAME", 'pkey')
               ELSE "CONSTRAINT_NAME"
            END)::character varying(64) AS constraint_name, false AS "deferrable", false AS deferred,
            "COLUMN_NAME" AS column_name, "ORDINAL_POSITION" AS position,
            ("CONSTRAINT_TYPE" = 'PRIMARY KEY') AS is_primary
         FROM %1$I."TABLE_CONSTRAINTS" cons
         JOIN %1$I."KEY_COLUMN_USAGE" keys USING ("CONSTRAINT_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME")
         WHERE "CONSTRAINT_SCHEMA" NOT IN (%2$s)
         AND "CONSTRAINT_TYPE" IN ('PRIMARY KEY', 'UNIQUE');
      COMMENT ON VIEW %1$I.keys IS 'MySQL primary and unique key columns on foreign server "%3$I"';
   $$;

   /* views */
   views_sql text := $$
      CREATE OR REPLACE VIEW %1$I.views AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS view_name,
            "VIEW_DEFINITION" AS definition
         FROM %1$I."VIEWS"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s);
      COMMENT ON VIEW %1$I.views IS 'MySQL views on foreign server "%3$I"';
   $$;

   /* functions */
   functions_sql text := $$
      DROP FOREIGN TABLE IF EXISTS %1$I."ROUTINES" CASCADE;
      CREATE FOREIGN TABLE %1$I."ROUTINES" (
         "ROUTINE_SCHEMA" varchar(64) NOT NULL,
         "ROUTINE_NAME" varchar(64) NOT NULL,
         "ROUTINE_TYPE" varchar(10) NOT NULL,
         "DTD_IDENTIFIER" text,
         "ROUTINE_BODY" varchar(3) NOT NULL,
         "ROUTINE_DEFINITION" text,
         "EXTERNAL_LANGUAGE" varchar(64) NOT NULL,
         "IS_DETERMINISTIC" varchar(3) NOT NULL
      ) SERVER %3$I OPTIONS (dbname 'information_schema', table_name 'ROUTINES');

      CREATE OR REPLACE VIEW %1$I.functions AS
         SELECT "ROUTINE_SCHEMA" AS schema, "ROUTINE_NAME" AS function_name,
            ("ROUTINE_TYPE" = 'PROCEDURE') AS is_procedure,
            (CASE "ROUTINE_TYPE"
               WHEN 'PROCEDURE' THEN
                  concat_ws(' ',
                     'CREATE PROCEDURE', "ROUTINE_NAME", '(', parameters, ')',
                     "ROUTINE_DEFINITION"
                  )
               WHEN 'FUNCTION' THEN
                  concat_ws(' ',
                     'CREATE FUNCTION', "ROUTINE_NAME", '(', parameters, ')',
                     'RETURNS', "DTD_IDENTIFIER",
                     "ROUTINE_DEFINITION"
                  )
               END
            ) AS source
         FROM %1$I."ROUTINES" rout
         JOIN (
            SELECT "SPECIFIC_SCHEMA" AS "ROUTINE_SCHEMA", "SPECIFIC_NAME" AS "ROUTINE_NAME",
               string_agg(
                  concat_ws(' ', "PARAMETER_MODE", "PARAMETER_NAME", "DTD_IDENTIFIER"),
                  text ', ' ORDER BY "ORDINAL_POSITION"
               ) AS parameters
            FROM %1$I."PARAMETERS"
            WHERE "ORDINAL_POSITION" > 0
            GROUP BY "SPECIFIC_SCHEMA", "SPECIFIC_NAME"
         ) prms USING ("ROUTINE_SCHEMA", "ROUTINE_NAME")
         WHERE "ROUTINE_SCHEMA" NOT IN (%2$s);
      COMMENT ON VIEW %1$I.functions IS 'MySQL functions and procedures on foreign server "%3$I"';
   $$;

   /* sequences */
   sequences_sql text := $$
      CREATE OR REPLACE VIEW %1$I.sequences AS
         SELECT "TABLE_SCHEMA" AS schema, concat("TABLE_NAME", '_seq') AS sequence_name,
            1 AS "min_value", null::integer AS max_value, 1 AS increment_by, false AS cyclical,
            1 AS cache_size, "AUTO_INCREMENT" AS last_value
         FROM %1$I."TABLES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE' AND "AUTO_INCREMENT" IS NOT NULL;
      COMMENT ON VIEW %1$I.sequences IS 'MySQL sequences on foreign server "%3$I"';
   $$;

   /* index_columns */
   index_columns_sql text := $$
      CREATE OR REPLACE VIEW %1$I.index_columns AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            concat_ws('_', "TABLE_NAME", "INDEX_NAME")::character varying(64) AS index_name,
            "SEQ_IN_INDEX" AS position, (CASE WHEN "COLLATION" = 'D' THEN true ELSE false END) AS descend,
            "EXPRESSION" IS NOT NULL
               AND ("COLLATION" <> 'D' OR "EXPRESSION" !~ '^`[^`]*`$') AS is_expression,
            coalesce(
               CASE WHEN "COLLATION" = 'D' AND "EXPRESSION" !~ '^`[^`]*`$'
                  THEN replace ("EXPRESSION", '`', '''')
                  ELSE "EXPRESSION"
               END, "COLUMN_NAME")::character varying(64) AS column_name
         FROM %1$I."STATISTICS"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "INDEX_NAME" <> 'PRIMARY' AND "IS_VISIBLE"::boolean; -- prior to MySQL v8
      COMMENT ON VIEW %1$I.index_columns IS 'MySQL index columns on foreign server "%3$I"';
   $$;

   /* indexes */
   indexes_sql text := $$
      CREATE OR REPLACE VIEW %1$I.indexes AS
         SELECT DISTINCT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            concat_ws('_', "TABLE_NAME", "INDEX_NAME")::character varying(64) AS index_name,
            "INDEX_TYPE" AS index_type, ("NON_UNIQUE" = 0) AS uniqueness, null::text AS where_clause
         FROM %1$I."STATISTICS"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "INDEX_NAME" <> 'PRIMARY' AND "IS_VISIBLE"::boolean; -- prior to MySQL v8
      COMMENT ON VIEW %1$I.indexes IS 'MySQL indexes on foreign server "%3$I"';
   $$;

   /* partitions */
   partitions_sql text := $$
      CREATE OR REPLACE VIEW %1$I.partitions AS
         WITH catalog AS (
            SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
                  "PARTITION_NAME" AS partition_name, "PARTITION_METHOD" AS type,
                  trim('`' FROM "PARTITION_EXPRESSION") AS key,
                  "PARTITION_ORDINAL_POSITION" AS position,
                  "PARTITION_DESCRIPTION" AS values
               FROM %1$I."PARTITIONS"
               WHERE "TABLE_SCHEMA" NOT IN (%2$s) AND "PARTITION_NAME" IS NOT NULL
               AND ("SUBPARTITION_ORDINAL_POSITION" IS NULL OR "SUBPARTITION_ORDINAL_POSITION" = 1)
         ), list_partitions AS (
            -- retrieves values[any, ...]
            SELECT schema, table_name, partition_name, type, key,
               (values IS NULL) AS is_default,
               string_to_array(values, ',') AS values
            FROM catalog WHERE type = 'LIST'
         ), range_partitions AS (
            -- retrieves values[lower_bound, upper_bound]
            SELECT schema, table_name, partition_name, type, key, false,
               ARRAY[
                  lag(values, 1, 'MINVALUE')
                     OVER (PARTITION BY schema, table_name ORDER BY position),
                  values
               ] AS values
            FROM catalog WHERE type = 'RANGE'
         ), hash_partitions AS (
            -- retrieves values[modulus, remainder]
            SELECT schema, table_name, partition_name, type, key, false,
               ARRAY[(position - 1)::text] AS values
            FROM catalog WHERE type = 'HASH'
         )
         SELECT * FROM list_partitions
         UNION SELECT * FROM range_partitions
         UNION SELECT * FROM hash_partitions;
      COMMENT ON VIEW %1$I.partitions IS 'MySQL partitions on foreign server "%3$I"';
   $$;

   /* subpartitions */
   subpartitions_sql text := $$
      CREATE OR REPLACE VIEW %1$I.subpartitions AS
         WITH catalog AS (
            SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
               "PARTITION_NAME" AS partition_name, "SUBPARTITION_NAME" AS subpartition_name,
               "SUBPARTITION_METHOD" AS type,
               trim('`' FROM "SUBPARTITION_EXPRESSION") AS key,
               "SUBPARTITION_ORDINAL_POSITION" AS position
            FROM %1$I."PARTITIONS"
            WHERE "TABLE_SCHEMA" NOT IN (%2$s)
            AND "PARTITION_NAME" IS NOT NULL AND "SUBPARTITION_NAME" IS NOT NULL
         )
         -- MySQL only supports HASH subpartition method
         SELECT schema, table_name, partition_name, subpartition_name, type, key,
            false AS is_default, ARRAY[(position - 1)::text] AS values
         FROM catalog WHERE type = 'HASH';
      COMMENT ON VIEW %1$I.partitions IS 'MySQL subpartitions on foreign server "%3$I"';
   $$;

   /* triggers */
   triggers_sql text := $$
      DROP FOREIGN TABLE IF EXISTS %1$I."TRIGGERS" CASCADE;
      CREATE FOREIGN TABLE %1$I."TRIGGERS" (
         "TRIGGER_SCHEMA" varchar(64) NOT NULL,
         "TRIGGER_NAME" varchar(64) NOT NULL,
         "EVENT_MANIPULATION" varchar(6) NOT NULL,
         "EVENT_OBJECT_TABLE" varchar(64) NOT NULL,
         "ACTION_STATEMENT" text NOT NULL,
         "ACTION_ORIENTATION" varchar(3) NOT NULL,
         "ACTION_TIMING" varchar(6) NOT NULL
      ) SERVER %3$I OPTIONS (dbname 'information_schema', table_name 'TRIGGERS');

      CREATE OR REPLACE VIEW %1$I.triggers AS
         SELECT "TRIGGER_SCHEMA" AS schema, "EVENT_OBJECT_TABLE" AS table_name,
            "TRIGGER_NAME" AS trigger_name, "ACTION_TIMING" AS trigger_type,
            "EVENT_MANIPULATION" AS triggering_event,
            ("ACTION_ORIENTATION" = 'ROW') AS for_each_row, null AS when_clause,
            'REFERENCING NEW AS NEW OLD AS OLD' AS referencing_names,
            "ACTION_STATEMENT" AS trigger_body
         FROM %1$I."TRIGGERS"
         WHERE "TRIGGER_SCHEMA" NOT IN (%2$s);
      COMMENT ON VIEW %1$I.triggers IS 'MySQL triggers on foreign server "%3$I"';
   $$;

   /* table_privs */
   table_privs_sql text := $$
      CREATE OR REPLACE VIEW %1$I.table_privs AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            "PRIVILEGE_TYPE" AS privilege, 'root'::varchar(292) AS grantor,
            "GRANTEE" as grantee, "IS_GRANTABLE"::boolean AS grantable
         FROM %1$I."TABLE_PRIVILEGES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s) AND "GRANTEE" !~* 'root';
      COMMENT ON VIEW %1$I.table_privs IS 'Privileges on MySQL tables on foreign server "%3$I"';
   $$;

   /* column_privs */
   column_privs_sql text := $$
      CREATE OR REPLACE VIEW %1$I.column_privs AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            "COLUMN_NAME" AS column_name, "PRIVILEGE_TYPE" AS privilege,
            'root'::varchar(292) AS grantor, "GRANTEE" AS grantee, "IS_GRANTABLE"::boolean AS grantable
         FROM %1$I."COLUMN_PRIVILEGES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s) AND "GRANTEE" !~* 'root';
      COMMENT ON VIEW %1$I.column_privs IS 'Privileges on MySQL table columns on foreign server "%3$I"';
   $$;

   /* segments */
   segments_sql text := $$
      DROP FOREIGN TABLE IF EXISTS %1$I.innodb_index_stats CASCADE;
      CREATE FOREIGN TABLE %1$I.innodb_index_stats (
         database_name varchar(64) NOT NULL,
         table_name varchar(64) NOT NULL,
         index_name varchar(64) NOT NULL,
         stat_value bigint NOT NULL,
         stat_description varchar(1024) NOT NULL
      ) SERVER %3$I OPTIONS (dbname 'mysql', table_name 'innodb_index_stats');

      CREATE OR REPLACE VIEW %1$I.segments AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS segment_name,
            'TABLE' AS segment_type, "DATA_LENGTH" AS bytes
         FROM %1$I."TABLES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE'
         UNION
         SELECT "TABLE_SCHEMA", "TABLE_NAME" segment_name,
            'INDEX' AS segment_name, "INDEX_LENGTH" AS bytes
         FROM %1$I."TABLES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE' AND "ENGINE" = 'MyISAM'
         UNION
         SELECT database_name, index_name, 'INDEX' AS segment_type,
            sum(stat_value) * 16384 AS bytes
         FROM %1$I.innodb_index_stats
         WHERE database_name NOT IN (%2$s)
         AND index_name <> 'PRIMARY'
         AND stat_description LIKE 'Number of pages in the index'
         GROUP BY database_name, index_name;
      COMMENT ON VIEW %1$I.segments IS 'Size of MySQL objects on foreign server "%3$I"';
   $$;

   /* migration_cost_estimate */
   migration_cost_estimate_sql text := $$
      CREATE VIEW %1$I.migration_cost_estimate AS
         SELECT schema, 'tables'::text AS task_type, count(*)::bigint AS task_content,
            'count'::text AS task_unit, ceil(count(*) / 10.0)::integer AS migration_hours
         FROM %1$I.tables GROUP BY schema
         UNION ALL
         SELECT t.schema, 'data_migration'::text, sum(bytes)::bigint,
            'bytes'::text, ceil(sum(bytes::float8) / 26843545600.0)::integer
         FROM %1$I.segments AS s
         JOIN %1$I.tables AS t ON s.schema = t.schema AND s.segment_name = t.table_name
         WHERE s.segment_type = 'TABLE'
         GROUP BY t.schema
         UNION ALL
         SELECT schema, 'functions'::text, coalesce(sum(octet_length(source)), 0),
            'characters'::text, ceil(coalesce(sum(octet_length(source)), 0) / 512.0)::integer
         FROM %1$I.functions GROUP BY schema
         UNION ALL
         SELECT schema, 'triggers'::text, coalesce(sum(octet_length(trigger_body)), 0),
            'characters'::text, ceil(coalesce(sum(octet_length(trigger_body)), 0) / 512.0)::integer
         FROM %1$I.triggers GROUP BY schema
         UNION ALL
         SELECT schema, 'views'::text, coalesce(sum(octet_length(definition)), 0),
            'characters'::text, ceil(coalesce(sum(octet_length(definition)), 0) / 512.0)::integer
         FROM %1$I.views GROUP BY schema;
      COMMENT ON VIEW %1$I.migration_cost_estimate IS 'Estimate of the migration costs per schema and object type';
   $$;

BEGIN
   /* remember old setting */
   old_msglevel := current_setting('client_min_messages');

   /* make the output less verbose */
   SET LOCAL client_min_messages = warning;
   
/* CREATE SCHEMA IF NEEDED */
   EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema);

   /* refresh catalog foreign tables */
   FOREACH catalog_table IN ARRAY catalog_tables
   LOOP
      EXECUTE format(
         $$ DROP FOREIGN TABLE IF EXISTS %1$I.%2$I CASCADE $$,
         schema, catalog_table
      );

      EXECUTE format(
         $$ IMPORT FOREIGN SCHEMA information_schema
         LIMIT TO (%3$I)
         FROM SERVER %1$I INTO %2$I
         OPTIONS (import_enum_as_text 'true') $$,
         server, schema, catalog_table
      );
   END LOOP;


   /* create views with predefined column names needed by db_migrator */
   EXECUTE format(schemas_sql, schema, sys_schemas, server);
   EXECUTE format(tables_sql, schema, sys_schemas, server);
   EXECUTE format(columns_sql, schema, sys_schemas, server);
   EXECUTE format(check_sql, schema, sys_schemas, server);
   EXECUTE format(foreign_keys_sql, schema, sys_schemas, server);
   EXECUTE format(keys_sql, schema, sys_schemas, server);
   EXECUTE format(views_sql, schema, sys_schemas, server);
   EXECUTE format(functions_sql, schema, sys_schemas, server);
   EXECUTE format(sequences_sql, schema, sys_schemas, server);
   EXECUTE format(index_columns_sql, schema, sys_schemas, server);
   EXECUTE format(indexes_sql, schema, sys_schemas, server);
   EXECUTE format(partitions_sql, schema, sys_schemas, server);
   EXECUTE format(subpartitions_sql, schema, sys_schemas, server);
   EXECUTE format(triggers_sql, schema, sys_schemas, server);
   EXECUTE format(table_privs_sql, schema, sys_schemas, server);
   EXECUTE format(column_privs_sql, schema, sys_schemas, server);
   EXECUTE format(segments_sql, schema, sys_schemas, server);
   EXECUTE format(migration_cost_estimate_sql, schema);

   /* reset client_min_messages */
   EXECUTE 'SET LOCAL client_min_messages = ' || old_msglevel;
END;
$mysql_create_catalog$;

COMMENT ON FUNCTION synchdb_create_mysql_objs(name, name, jsonb) IS
   'create MySQL foreign tables for the metadata of a foreign server';

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
   'create MySQL foreign tables to obtain binlog file, pos and server id';

-----------------------------------------------------------------------------------------------------------------
    -- ORACLE: functions to map Oracle objects to PostgreSQL via FDW (originated from oracle_migrator project)
-----------------------------------------------------------------------------------------------------------------
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
    rels   text[] := ARRAY['tables','columns','keys','columns_resolved'];  -- materialize just these
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

		IF rname = 'columns_resolved' THEN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = p_dest_schema::text AND table_name='columns_resolved'
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

		IF rname = 'columns_resolved' THEN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = p_dest_schema::text AND table_name='columns_resolved'
                  AND column_name IN ('schema','table_name','position')
                GROUP BY table_schema, table_name
                HAVING count(*) = 3
            ) THEN
                EXECUTE format('CREATE INDEX ON %I.columns_resolved(("schema"), table_name, position)', p_dest_schema);
            END IF;
        END IF;

    END LOOP;

    -- 4) Analyze for better local planning
    EXECUTE format('ANALYZE %I.tables',  p_dest_schema);
    EXECUTE format('ANALYZE %I.columns', p_dest_schema);
    EXECUTE format('ANALYZE %I.keys',    p_dest_schema);

	IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname = p_dest_schema::text AND c.relname='columns_resolved' AND c.relkind='r'
    ) THEN
        EXECUTE format('ANALYZE %I.columns_resolved', p_dest_schema);
    END IF;

    RAISE NOTICE 'Materialization complete into schema %', p_dest_schema;
END;
$$;

COMMENT ON FUNCTION synchdb_materialize_ora_metadata(name, name, text) IS
   'create a metadata schema with selected ora_obj foreign tables materialized';

CREATE OR REPLACE FUNCTION synchdb_create_ora_stage_fts(
    p_connector_name        name,                         -- connector name for attribute rows
    p_desired_db            name,                         -- db token to match entries in snapshottable
    p_desired_schema        name,                         -- schema to match when entry includes schema
    p_stage_schema          name DEFAULT 'ora_stage'::name, -- target schema in Postgres
    p_server_name           name DEFAULT 'oracle'::name,  -- oracle_fdw server name
    p_lower_names           boolean DEFAULT true,         -- legacy: lower-case PG table/column names
    p_on_exists             text DEFAULT 'replace',       -- 'replace' | 'drop' | 'skip'
    p_offset                text DEFAULT NULL,            -- offset like scn, binlog pos..etc
    p_source_schema         name DEFAULT 'ora_obj'::name, -- metadata source schema (…columns)
    p_snapshot_tables       text DEFAULT NULL,            -- CSV of db.schema.table; when set, bypass conninfo
    p_write_dbz_schema_info boolean DEFAULT false,        -- when true, store Debezium schema-history JSON
    p_case_strategy         text DEFAULT 'asis'           -- 'upper' | 'lower' | 'asis' (table/column casing)
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
  v_tbl_display text;
  err_state         text;
  err_msg           text;
  err_detail        text;
  err_context       text;

  -- case strategy: 'lower' | 'upper' | 'asis'
  v_case_strategy text;
  -- expression used for column name in dynamic SQL (e.g., 'lower(c.column_name)')
  v_colname_expr  text;

  -- possible offsets
  v_offset_json jsonb;
  v_scn          numeric;    -- oracle
  v_binlog_file  text;       -- mysql
  v_binlog_pos   bigint;     -- mysql
  v_ts_sec       bigint;     -- mysql
BEGIN

  IF p_offset IS NOT NULL AND btrim(p_offset) <> '' THEN
    v_offset_json := p_offset::jsonb;
  ELSE
    v_offset_json := NULL;
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
    -- EXECUTE format('TRUNCATE TABLE %I.%I', 'public', v_schema_tbl);
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
  --  figure out v_conn_type
  ----------------------------------------------------------------------
  SELECT (data->>'connector')::name,
         data->>'srcdb',
		 COALESCE( NULLIF(data->'snapshottable', 'null'::jsonb), data->'table')
    INTO v_conn_type, v_srcdb, v_snapcfg
  FROM synchdb_conninfo
  WHERE name = p_connector_name;

  IF v_conn_type IS NULL OR v_conn_type = ''::name THEN
    RAISE EXCEPTION 'synchdb_conninfo[%]: data->>connector is missing/empty', p_connector_name;
  END IF;
  IF v_srcdb IS NULL OR v_srcdb = '' THEN
    RAISE EXCEPTION 'synchdb_conninfo[%]: data->>srcdb is missing/empty', p_connector_name;
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
	SELECT
      CASE WHEN array_length(parts,1) = 3 THEN parts[1]
           WHEN array_length(parts,1) = 2 THEN parts[1]  -- treat as db
      END AS db,
      CASE WHEN array_length(parts,1) = 3 THEN parts[2]
           WHEN array_length(parts,1) = 2 THEN NULL             -- schem unknown
      END AS schem,
      CASE WHEN array_length(parts,1) = 3 THEN parts[3]
           WHEN array_length(parts,1) = 2 THEN parts[2]
      END AS tbl
    FROM (
      SELECT regexp_split_to_array(trim(x), '\.') AS parts
      FROM regexp_split_to_table(p_snapshot_tables, '\s*,\s*') AS t(x)
      WHERE trim(x) <> ''
    ) s
    WHERE array_length(parts,1) IN (2,3);

    v_use_filter := EXISTS (SELECT 1 FROM tmp_snap_list);
  ELSE
    -- Mode B: legacy behavior using synchdb_conninfo + data->snapshottable
    IF v_snapcfg IS NULL OR v_snapcfg::text = 'null' THEN
      v_use_filter := false;  -- migrate all (but we'll still restrict by p_desired_schema below)
    ELSE
      v_type := jsonb_typeof(v_snapcfg);

      -- Build v_snaptext as a single comma-separated list, regardless of input shape
      IF v_type = 'string' THEN
        v_snaptext := trim(both '"' from v_snapcfg::text);
        IF position('file:' IN v_snaptext) = 1 THEN
          v_snaptext := read_snapshot_table_list(v_snaptext, v_conn_type::text, p_desired_db::text);
        END IF;
      ELSIF v_type = 'array' THEN
        SELECT string_agg(x, ',')
          INTO v_snaptext
        FROM jsonb_array_elements_text(v_snapcfg) AS t(x);
      ELSE
        RAISE EXCEPTION 'synchdb_conninfo[%.data->snapshottable] must be string or array, got %',
                        p_connector_name, v_type;
      END IF;

	  -- normalize
      IF lower(v_conn_type::text) <> 'mysql' THEN
        SELECT string_agg(
                 CASE
                   WHEN array_length(parts,1) = 2 THEN format('%s.%s.%s',
                                                             p_desired_db::text,
                                                             parts[1],
                                                             parts[2])
                   WHEN array_length(parts,1) = 3 THEN trim(x)  -- keep as-is if someone still provides 3 parts
                   ELSE NULL
                 END,
                 ','
               )
          INTO v_snaptext
        FROM (
           SELECT trim(x) AS x,
                 regexp_split_to_array(trim(x), '\.') AS parts
             FROM regexp_split_to_table(v_snaptext, '\s*,\s*') AS t(x)
           WHERE trim(x) <> ''
        ) s
        WHERE array_length(parts,1) IN (2,3);
      END IF;
	 
	  RAISE NOTICE 'Normalized snapshottable list: %', v_snaptext;

      -- Parse CSV into tmp_snap_list allowing "db.tbl" or "db.schema.tbl"
      INSERT INTO tmp_snap_list(db, schem, tbl)
      SELECT
        (parts)[1] AS db,
        CASE WHEN array_length(parts,1) = 3 THEN (parts)[2] ELSE NULL END AS schem,
        CASE WHEN array_length(parts,1) = 3 THEN (parts)[3]
             WHEN array_length(parts,1) = 2 THEN (parts)[2]
             ELSE NULL END AS tbl
      FROM (
        SELECT regexp_split_to_array(trim(x), '\.') AS parts
        FROM regexp_split_to_table(v_snaptext, '\s*,\s*') AS t(x)
        WHERE trim(x) <> ''
      ) s
      WHERE array_length(parts,1) IN (2,3);

      -- Keep only rows that match desired DB (legacy behavior)
      DELETE FROM tmp_snap_list WHERE db <> p_desired_db;

      v_use_filter := EXISTS (SELECT 1 FROM tmp_snap_list);
    END IF;
  END IF;

  ----------------------------------------------------------------------
  -- Resolve case strategy for table/column names
  -- p_case_strategy: 'lower' | 'upper' | 'asis'
  -- Backward compatibility:
  --   - NULL/empty: use p_lower_names (true => lower, false => asis)
  ----------------------------------------------------------------------
  IF p_case_strategy IS NULL OR btrim(p_case_strategy) = '' THEN
    IF p_lower_names THEN
      v_case_strategy := 'lower';
    ELSE
      v_case_strategy := 'asis';
    END IF;
  ELSE
    v_case_strategy := lower(btrim(p_case_strategy));
    IF v_case_strategy NOT IN ('lower','upper','asis') THEN
      RAISE EXCEPTION 'Invalid p_case_strategy: %, expected lower|upper|asis', p_case_strategy;
    END IF;
  END IF;

  -- Column name expression for dynamic SQL (Postgres side)
  IF v_case_strategy = 'lower' THEN
    v_colname_expr := 'lower(c.column_name)';
  ELSIF v_case_strategy = 'upper' THEN
    v_colname_expr := 'upper(c.column_name)';
  ELSE
    v_colname_expr := 'c.column_name';
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
            AND lower(table_name) <> ''synchdb_wal_lsn''
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
           WHERE lower(table_name) = lower(f.tbl)
             AND (f.schem IS NULL OR lower("schema") = lower(f.schem))
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

  RAISE NOTICE 'base sql = %', base_sql;
  ----------------------------------------------------------------------
  -- Create (or replace) the stage foreign tables
  ----------------------------------------------------------------------
  FOR r IN EXECUTE base_sql || ' ORDER BY ora_owner, table_name'
  LOOP

  BEGIN
	-- decide PG table name according to case strategy
    v_tbl_pg := CASE v_case_strategy
                  WHEN 'lower' THEN lower(r.table_name)
                  WHEN 'upper' THEN upper(r.table_name)
                  ELSE r.table_name
                END;

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

	IF v_conn_type = 'postgres' THEN
      EXECUTE format(
        'SELECT string_agg(
                  quote_ident(%s) || '' '' ||
                  synchdb_translate_datatype(
                      %L::name,
                      CASE
                        WHEN c.type_name = ''ARRAY'' AND c.element_type_name IS NOT NULL
                          THEN (lower(c.element_type_name) || ''[]'')::name
                        ELSE lower(c.type_name)::name
                      END,
                      COALESCE(c.length, -1)::bigint,
                      COALESCE(c.scale, -1)::bigint,
                      COALESCE(c.precision, -1)::bigint
                  ) ||
                  CASE
                    WHEN lower(coalesce(c.nullable::text, '''')) IN (''no'', ''n'', ''0'', ''false'', ''f'')
                      THEN '' NOT NULL''
                    ELSE ''''
                  END,
                  '', '' ORDER BY c.position
               )
         FROM %I.columns_resolved c
        WHERE c."schema" = %L
          AND c.table_name = %L',
        v_colname_expr,
        v_conn_type,
        p_source_schema, r.ora_owner, r.table_name
      )
      INTO v_cols_sql;

	ELSE
    -- Build PG column list using translator, honoring case strategy
    EXECUTE format(
      'SELECT string_agg(
                quote_ident(%s) || '' '' ||
                synchdb_translate_datatype(%L::name, lower(c.type_name)::name,
                                           COALESCE(c.length, -1)::bigint,
                                           COALESCE(c.scale, -1)::bigint,
                                           COALESCE(c.precision, -1)::bigint) ||
									CASE WHEN lower(coalesce(c.nullable::text, '''')) IN (''no'', ''n'', ''0'', ''false'', ''f'') THEN '' NOT NULL'' ELSE '''' END,
                '', '' ORDER BY c.position)
         FROM %I.columns c
        WHERE c."schema" = %L
          AND c.table_name = %L',
      v_colname_expr,
	  v_conn_type,
      p_source_schema, r.ora_owner, r.table_name
    )
    INTO v_cols_sql;

	END IF;

    IF v_cols_sql IS NULL THEN
      RAISE NOTICE 'No columns found for %.% — skipping', r.ora_owner, r.table_name;
      CONTINUE;
    END IF;

    -- Create the staging FT (snapshot or not)
	IF v_conn_type IN ('oracle','olr') THEN
		-- derive SCN from position JSON or fallback to 0
        IF v_offset_json IS NOT NULL AND v_offset_json ? 'scn' THEN
          v_scn := (v_offset_json->>'scn')::numeric;
	    END IF;

		IF v_scn IS NOT NULL AND v_scn > 0 THEN
		  ------------------------------------------------------------------
		  -- Build Oracle SELECT list: always double-quote column names
		  -- exactly as in metadata.
		  ------------------------------------------------------------------
		  EXECUTE format(
			'SELECT string_agg(
					 ''"'' || replace(c.column_name, ''"'', ''""'') || ''"'',
					 '', '' ORDER BY c.position)
			   FROM %I.columns c
			  WHERE c."schema"   = %L
				AND c.table_name = %L',
			p_source_schema, r.ora_owner, r.table_name
		  )
		  INTO v_sel_list;

		  ------------------------------------------------------------------
		  -- IMPORTANT: Always double-quote owner and table for Oracle,
		  -- independent of case, so "testtable" stays case-sensitive.
		  ------------------------------------------------------------------
		  v_subquery := format(
			'(SELECT %s FROM "%s"."%s" AS OF SCN %s)',
			COALESCE(v_sel_list, '*'),
			replace(r.ora_owner, '"', '""'),
			replace(r.table_name, '"', '""'),
			v_scn::text
		  );

		  EXECUTE format(
			'CREATE FOREIGN TABLE %I.%I (%s) SERVER %I OPTIONS ("table" %L)',
			p_stage_schema, v_tbl_pg, v_cols_sql, p_server_name, v_subquery
		  );

		  RAISE NOTICE 'Created FT %.% at SCN % -> %.% on %',
					   p_stage_schema, v_tbl_pg, v_scn::text, r.ora_owner, r.table_name, p_server_name;
		ELSE
		  EXECUTE format(
			'CREATE FOREIGN TABLE %I.%I (%s) SERVER %I OPTIONS (schema %L, "table" %L)',
			p_stage_schema, v_tbl_pg, v_cols_sql, p_server_name, r.ora_owner, r.table_name
		  );

		  RAISE NOTICE 'Created FT %.% -> %.% on %',
					   p_stage_schema, v_tbl_pg, r.ora_owner, r.table_name, p_server_name;
		END IF;
	ELSIF v_conn_type = 'mysql' THEN
		-- Ignore p_scn; use mysql_fdw options: dbname + table_name
		EXECUTE format(
			'CREATE FOREIGN TABLE %I.%I (%s) SERVER %I OPTIONS (dbname %L, table_name %L)',
			p_stage_schema, v_tbl_pg, v_cols_sql, p_server_name,
			lower(p_desired_schema::text), r.table_name
		);

		RAISE NOTICE 'Created MySQL FT %.% -> %.% on %',
				   p_stage_schema, v_tbl_pg, p_desired_db, r.table_name, p_server_name;
	ELSIF v_conn_type = 'postgres' THEN
          ------------------------------------------------------------------
      -- IMPORTANT for postgres_fdw:
      -- If we normalize local FT column identifiers (upper/lower),
      -- we MUST map them back to the real remote column names using
      -- column OPTIONS (column_name 'remote_col').
      ------------------------------------------------------------------
      EXECUTE format($SQL$
          SELECT string_agg(
                   (
                     quote_ident(
                       CASE
                         WHEN %L = 'lower' THEN lower(c.column_name)
                         WHEN %L = 'upper' THEN upper(c.column_name)
                         ELSE c.column_name
                       END
                     )
                     || ' ' ||
                     synchdb_translate_datatype(
                         %L::name,
                         CASE
                           WHEN lower(c.type_name) = 'array' THEN
                             CASE
                               WHEN c.element_type_name IS NOT NULL
                                 THEN (lower(c.element_type_name) || '[]')::name
                               ELSE
                                 lower(c.array_type_name)::name
                             END
                           ELSE
                             lower(c.type_name)::name
                         END,
                         COALESCE(c.length, -1)::int,
                         COALESCE(c.scale, -1)::int,
                         COALESCE(c.precision, -1)::int
                     )
                     || ' OPTIONS (column_name ' || quote_literal(c.column_name) || ')'
                     || CASE
                          WHEN lower(coalesce(c.nullable::text,'')) IN ('no','n','0','false','f')
                          THEN ' NOT NULL'
                          ELSE ''
                        END
                   ),
                   ', ' ORDER BY c.position
                 )
            FROM %I.columns_resolved c
           WHERE c."schema"   = %L
             AND c.table_name = %L
        $SQL$,
          p_case_strategy,
          p_case_strategy,
          v_conn_type,
          p_source_schema,
          r.ora_owner,
          r.table_name
      )
      INTO v_cols_sql;


      EXECUTE format(
        'CREATE FOREIGN TABLE %I.%I (%s) SERVER %I OPTIONS (schema_name %L, table_name %L)',
        p_stage_schema, v_tbl_pg, v_cols_sql, p_server_name,
        p_desired_schema::text, r.table_name
      );
	  
      RAISE NOTICE 'Created Postgres FT %.% -> %.% on %',
                   p_stage_schema, v_tbl_pg, p_desired_schema, r.table_name, p_server_name;
	
	ELSE
      RAISE EXCEPTION 'Unsupported connector type: %', v_conn_type;
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
        v_ext_db := p_desired_db::text;
      END IF;
    ELSE
      v_ext_db := v_srcdb;
    END IF;

    ------------------------------------------------------------------
    -- Build fully-qualified external table name: db.schema.table
    -- Preserve Oracle owner/table case as reported in metadata.
    ------------------------------------------------------------------
    v_tbl_name_l := r.table_name;

    -- Refresh synchdb_attribute rows for this table
	IF v_conn_type IN ('oracle','olr','postgres') THEN
	   DELETE FROM public.synchdb_attribute
		 WHERE name       = p_connector_name
		   AND type       = v_conn_type
		   AND ext_tbname = format('%s.%s.%s',
								   v_ext_db,
								   r.ora_owner,
								   v_tbl_name_l)::name;

		EXECUTE format($ins$
		  INSERT INTO public.synchdb_attribute
			  (name, type, attrelid, attnum, ext_tbname,    ext_attname,           ext_atttypename)
		  SELECT
			  %L::name,
			  %L::name,
			  %s::oid,
  			  row_number() OVER (ORDER BY c.position)::smallint,
			  %L::name,
			  c.column_name::name,
			  lower(c.type_name)::name
		  FROM %I.columns c
		  WHERE c."schema"   = %L
			AND c.table_name = %L
		  ORDER BY c.position
		$ins$, p_connector_name, v_conn_type, v_relid::text,
			  format('%s.%s.%s', v_ext_db, r.ora_owner, v_tbl_name_l),
			  p_source_schema, r.ora_owner, r.table_name);

		RAISE NOTICE 'Recorded % columns for %.% into synchdb_attribute (attrelid=%) as %',
					 (SELECT count(*)
						FROM public.synchdb_attribute
					   WHERE name = p_connector_name
						 AND type = v_conn_type
						 AND ext_tbname = format('%s.%s.%s',
												  v_ext_db,
												  r.ora_owner,
												  v_tbl_name_l)::name),
					 r.ora_owner, r.table_name, v_relid::text,
					 format('%s.%s.%s', v_ext_db, r.ora_owner, v_tbl_name_l);
					 
	 ELSIF v_conn_type = 'mysql' THEN
	 
		 DELETE FROM public.synchdb_attribute
		 WHERE name       = p_connector_name
		   AND type       = v_conn_type
		   AND ext_tbname = format('%s.%s',
								   v_ext_db,
								   v_tbl_name_l)::name;

		EXECUTE format($ins$
		  INSERT INTO public.synchdb_attribute
			  (name, type, attrelid, attnum, ext_tbname,    ext_attname,           ext_atttypename)
		  SELECT
			  %L::name,
			  %L::name,
			  %s::oid,
			  c.position::smallint,
			  %L::name,
			  c.column_name::name,
			  lower(c.type_name)::name
		  FROM %I.columns c
		  WHERE c."schema"   = %L
			AND c.table_name = %L
		  ORDER BY c.position
		$ins$, p_connector_name, v_conn_type, v_relid::text,
			  format('%s.%s', v_ext_db, v_tbl_name_l),
			  p_source_schema, r.ora_owner, r.table_name);

		RAISE NOTICE 'Recorded % columns for %.% into synchdb_attribute (attrelid=%) as %',
					 (SELECT count(*)
						FROM public.synchdb_attribute
					   WHERE name = p_connector_name
						 AND type = v_conn_type
						 AND ext_tbname = format('%s.%s',
												  v_ext_db,
												  v_tbl_name_l)::name),
					 r.ora_owner, r.table_name, v_relid::text,
					 format('%s.%s', v_ext_db, v_tbl_name_l);
					 
	 ELSE
		RAISE EXCEPTION 'Unsupported connector type: %', v_conn_type;
	END IF;
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
            AND k.table_name = %L',
        p_source_schema, r.ora_owner, r.table_name
      )
      INTO v_pk_json;

      -- columns JSON array
      EXECUTE format($SQL$
        SELECT jsonb_agg(
                 jsonb_build_object(
                   'name',            c.column_name,
                   'jdbcType',        synchdb_type_to_jdbc(%4$L::text, lower(c.type_name)),
                   'typeName',        upper(c.type_name),
                   'typeExpression',  upper(c.type_name),
                   'charsetName',     NULL,
                   'length',          CASE
                                        WHEN c.precision IS NOT NULL AND c.precision >= 0
                                        THEN c.precision
                                      ELSE
                                        COALESCE(c.length, 0)
                                      END,
                   'position',        c.position,
                   'optional',        COALESCE(c.nullable, TRUE),
                   'autoIncremented', FALSE,
                   'generated',       FALSE,
                   'comment',         NULL,
                   'hasDefaultValue', (c.default_value IS NOT NULL),
                   'enumValues',      '[]'::jsonb
                 )
                 ||
                 CASE
                   WHEN c.scale IS NOT NULL AND c.scale >= 0
                   THEN jsonb_build_object('scale', c.scale)
                   ELSE '{}'::jsonb
                 END
                 ORDER BY c.position
               )
          FROM %1$I.columns c
         WHERE c."schema" = %2$L
           AND c.table_name = %3$L
      $SQL$, p_source_schema, r.ora_owner, r.table_name, v_conn_type::text)
      INTO v_cols_json;

	  IF v_conn_type IN ('oracle','olr') THEN
        v_table_obj := jsonb_build_object(
          'defaultCharsetName', NULL,
          'primaryKeyColumnNames', COALESCE(v_pk_json, '[]'::jsonb),
          'columns', COALESCE(v_cols_json, '[]'::jsonb)
        );
		  v_change_obj := jsonb_build_object(
		  'type', 'CREATE',
		  'id', format('"%s"."%s"."%s"',
			  		 p_desired_db::text,
					 p_desired_schema::text,
					 r.table_name),
		  'table', v_table_obj,
		  'comment', NULL
	    );
        v_msg_json := jsonb_build_object(
          'source',   jsonb_build_object('server', 'synchdb-connector'),
          'position', jsonb_build_object(
                         'snapshot_scn',       v_scn::text,
                         'snapshot',           TRUE,
                         'scn',                v_scn::text,
                         'snapshot_completed', TRUE
                       ),
          'ts_ms', v_ts_ms,
          'databaseName', p_desired_db::text,
		  'schemaName',   p_desired_schema::text,
          'ddl', '',
          'tableChanges', jsonb_build_array(v_change_obj)
        );
	  ELSIF v_conn_type = 'mysql' THEN 
        v_table_obj := jsonb_build_object(
          'defaultCharsetName', 'utf8mb4',
          'primaryKeyColumnNames', COALESCE(v_pk_json, '[]'::jsonb),
          'columns', COALESCE(v_cols_json, '[]'::jsonb),
          'attributes', '[]'::jsonb
        );
		IF v_offset_json IS NOT NULL THEN
		  v_binlog_file := v_offset_json->>'file';
  		  v_binlog_pos  := (v_offset_json->>'pos')::bigint;
		  v_ts_sec      := COALESCE((v_offset_json->>'ts_sec')::bigint, 0);
		ELSE
	      v_binlog_file := 'n/a';
		  v_binlog_pos := 0;
		  v_ts_sec := 0;
		END IF;

		v_change_obj := jsonb_build_object(
		  'type', 'CREATE',
		  'id', format('"%s"."%s"',
					 p_desired_db::text,
					 r.table_name),
		  'table', v_table_obj,
		  'comment', NULL
		);
        v_msg_json := jsonb_build_object(
          'source',   jsonb_build_object('server', 'synchdb-connector'),
          'position', jsonb_build_object(
                         'ts_sec',       	v_ts_sec,
                         'file',           	v_binlog_file,
                         'pos',            	v_binlog_pos,
                         'snapshot', 		TRUE
                       ),
          'ts_ms', v_ts_ms,
          'databaseName', p_desired_db::text,
          'ddl', '',
          'tableChanges', jsonb_build_array(v_change_obj)
        );
	  ELSE
	     RAISE EXCEPTION 'Unsupported connector type: %', v_conn_type;
	  END IF;


      -- store one JSON line per table
      EXECUTE format(
        'INSERT INTO %I.%I(line) VALUES ($1)',
        'public', v_schema_tbl
      )
      USING v_msg_json::text;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'an error has occured while creating a table schema';
    EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I (
               connector_name name        NOT NULL,
               tbl            text        NOT NULL,
               err_state      text        NOT NULL,
               err_msg        text        NOT NULL,
               err_detail     text,
               err_offset     text,
               ts             timestamptz NOT NULL DEFAULT now(),
               CONSTRAINT %I UNIQUE (connector_name, tbl)
             )',
            'public', v_err_tbl_ident, ('uq_'||v_err_tbl_ident)
          );

    GET STACKED DIAGNOSTICS err_state = RETURNED_SQLSTATE,
                            err_msg   = MESSAGE_TEXT,
                            err_detail = PG_EXCEPTION_DETAIL,
							err_context= PG_EXCEPTION_CONTEXT;
    
	IF p_desired_schema IS NULL OR btrim(p_desired_schema::text) = '' THEN
      v_tbl_display :=
          p_desired_db::text || '.' || v_tbl_pg;
    ELSE
      v_tbl_display :=
          p_desired_db::text || '.'
        || p_desired_schema::text || '.'
        || v_tbl_pg;
    END IF;
    
	RAISE WARNING 'error context = %', err_context;
	EXECUTE format(
          'INSERT INTO %I.%I (connector_name, tbl, err_state, err_msg, err_detail, err_offset)
           VALUES ($1,$2,$3,$4,$5,$6)
           ON CONFLICT (connector_name, tbl)
           DO UPDATE SET err_state = EXCLUDED.err_state,
                         err_msg   = EXCLUDED.err_msg,
                         err_detail= EXCLUDED.err_detail,
                         err_offset= EXCLUDED.err_offset,
                         ts        = now()',
          'public', v_err_tbl_ident
        )
    USING p_connector_name, v_tbl_display, err_state, err_msg, err_detail, p_offset;
  END;
  END LOOP;

  ----------------------------------------------------------------------
  -- Snapshot-retry cleanup
  ----------------------------------------------------------------------
  IF use_explicit AND v_err_tbl_exists THEN
    -- requested_full
    CREATE TEMP TABLE IF NOT EXISTS tmp_requested_full(fullname text PRIMARY KEY) ON COMMIT DROP;
    TRUNCATE tmp_requested_full;

	IF v_conn_type IN ('oracle','olr','postgres') THEN
      INSERT INTO tmp_requested_full(fullname)
      SELECT format('%s.%s.%s', db, schem, tbl)
      FROM tmp_snap_list;
	ELSIF v_conn_type = 'mysql' THEN
      INSERT INTO tmp_requested_full(fullname)
      SELECT format('%s.%s', db, tbl)
      FROM tmp_snap_list;
    ELSE
      RAISE EXCEPTION 'Unsupported connector type: %', v_conn_type;
    END IF;

    -- existing_full (join requested list to metadata to ensure db token matches)
    CREATE TEMP TABLE IF NOT EXISTS tmp_existing_full(fullname text PRIMARY KEY) ON COMMIT DROP;
    TRUNCATE tmp_existing_full;

	IF v_conn_type IN ('oracle','olr','postgres') THEN
      EXECUTE format($SQL$
        INSERT INTO tmp_existing_full(fullname)
        SELECT format('%%s.%%s.%%s', f.db, c."schema", c.table_name)
          FROM %1$I.columns c
          JOIN tmp_snap_list f
            ON c.table_name = f.tbl
           AND (f.schem IS NULL OR c."schema" = f.schem)
        GROUP BY f.db, c."schema", c.table_name
      $SQL$, p_source_schema);
    ELSIF v_conn_type = 'mysql' THEN
      EXECUTE format($SQL$
        INSERT INTO tmp_existing_full(fullname)
        SELECT format('%%s.%%s', f.db, c.table_name)
          FROM %1$I.columns c
          JOIN tmp_snap_list f
            ON c.table_name = f.tbl
           AND (f.schem IS NULL OR c."schema" = f.schem)
        GROUP BY f.db, c."schema", c.table_name
      $SQL$, p_source_schema);
    ELSE
      RAISE EXCEPTION 'Unsupported connector type: %', v_conn_type;
    END IF;

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

COMMENT ON FUNCTION synchdb_create_ora_stage_fts(name, name, name, name, name, boolean, text, text, name, text, boolean, text) IS
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
    p_oraobj_schema name,          -- e.g. 'ora_obj'  (holds table "keys")
    p_dst_schema    name,          -- e.g. 'psql_stage'
    p_case_strategy text DEFAULT 'asis'  -- 'upper' | 'lower' | 'asis'
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r              record;
  v_exists       boolean;
  v_cname        text;
  v_sql          text;

  -- case strategy: 'lower' | 'upper' | 'asis'
  v_case_strategy text;
  v_tbl_expr      text;
  v_col_expr      text;
BEGIN
  ----------------------------------------------------------------------
  -- Resolve case strategy for table/column names
  -- p_case_strategy: 'lower' | 'upper' | 'asis'
  -- Backward compatible default: 'lower'
  ----------------------------------------------------------------------
  IF p_case_strategy IS NULL OR btrim(p_case_strategy) = '' THEN
    v_case_strategy := 'lower';
  ELSE
    v_case_strategy := lower(btrim(p_case_strategy));
    IF v_case_strategy NOT IN ('lower', 'upper', 'asis') THEN
      RAISE EXCEPTION 'Invalid p_case_strategy: %, expected lower|upper|asis', p_case_strategy;
    END IF;
  END IF;

  IF v_case_strategy = 'lower' THEN
    v_tbl_expr := 'lower(table_name)';
    v_col_expr := 'lower(column_name)';
  ELSIF v_case_strategy = 'upper' THEN
    v_tbl_expr := 'upper(table_name)';
    v_col_expr := 'upper(column_name)';
  ELSE
    v_tbl_expr := 'table_name';
    v_col_expr := 'column_name';
  END IF;

  ----------------------------------------------------------------------
  -- Iterate PK definitions from <p_oraobj_schema>.keys
  -- Apply the same case strategy to table and column names
  ----------------------------------------------------------------------
  FOR r IN
    EXECUTE format($q$
      SELECT
        %1$s AS tbl,
        string_agg(quote_ident(%2$s), ', ' ORDER BY position) AS cols
      FROM %3$I.keys
      WHERE is_primary = true
      GROUP BY %1$s
    $q$, v_tbl_expr, v_col_expr, p_oraobj_schema)
  LOOP
    -- Only act if destination table exists (relkind 'r' = ordinary table)
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

    -- Build constraint name (<= 63 chars), based on normalized table name
    v_cname := left(r.tbl, 55) || '_pkey';

    -- All identifiers (schema, table, constraint, columns) are quoted
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

COMMENT ON FUNCTION synchdb_migrate_primary_keys(name, name, text) IS
   'migrate primary keys from staging to destination schema';

CREATE OR REPLACE FUNCTION synchdb_apply_column_mappings(
    p_src_schema      name,             -- e.g. 'psql_stage'
    p_connector_name  name,             -- connector name to filter objmap
    p_desired_db      name,             -- e.g. 'free' or 'wrongdb'
    p_desired_schema  name DEFAULT NULL, -- optional: e.g. 'dbzuser'; NULL = don't enforce
	p_case_strategy   text DEFAULT 'asis'
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
    parts := regexp_split_to_array(r.srcobj, '\.');
    s_db := NULL; s_schema := NULL; s_table := NULL; s_col := NULL;

    IF array_length(parts,1) = 4 THEN
      s_db := parts[1]; s_schema := parts[2]; s_table := parts[3]; s_col := parts[4];
    ELSIF array_length(parts,1) = 3 THEN
      s_db := parts[1]; s_table := parts[2]; s_col := parts[3];
    ELSE
      CONTINUE;
    END IF;

	IF p_case_strategy = 'lower' THEN
      s_col := lower(s_col);
	  s_table := lower(s_table);
    ELSIF p_case_strategy = 'upper' THEN
      s_col := upper(s_col);
	  s_table := upper(s_table);
    ELSE
      -- 'asis' → leave untouched
      NULL;
    END IF;

    IF p_desired_db IS NOT NULL AND s_db IS DISTINCT FROM p_desired_db THEN
      CONTINUE;
    END IF;

    IF p_desired_schema IS NOT NULL
       AND s_schema IS NOT NULL
       AND s_schema <> p_desired_schema THEN
      CONTINUE;
    END IF;

    dst_parts := regexp_split_to_array(r.dstobj, '\.');
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
    dt_parts := regexp_split_to_array(rdt.srcobj, '\.');
    dt_db := NULL; dt_schema := NULL; dt_table := NULL; dt_col := NULL;

    IF array_length(dt_parts,1) = 4 THEN
      dt_db := dt_parts[1]; dt_schema := dt_parts[2]; dt_table := dt_parts[3]; dt_col := dt_parts[4];
    ELSIF array_length(dt_parts,1) = 3 THEN
      dt_db := dt_parts[1]; dt_table := dt_parts[2]; dt_col := dt_parts[3];
    ELSE
      CONTINUE;
    END IF;

    IF p_case_strategy = 'lower' THEN
      s_col := lower(s_col);
      s_table := lower(s_table);
    ELSIF p_case_strategy = 'upper' THEN
      s_col := upper(s_col);
      s_table := upper(s_table);
    ELSE
      -- 'asis' → leave untouched
      NULL;
    END IF;

    IF p_desired_db IS NOT NULL AND dt_db IS DISTINCT FROM p_desired_db THEN
      CONTINUE;
    END IF;

    IF p_desired_schema IS NOT NULL
       AND dt_schema IS NOT NULL
       AND dt_schema <> p_desired_schema THEN
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

COMMENT ON FUNCTION synchdb_apply_column_mappings(name, name, name, name, text) IS
   'transform column name mappings based on synchdb_objmap';

CREATE OR REPLACE FUNCTION synchdb_migrate_data_with_transforms(
    p_src_schema        name,                 -- e.g. 'ora_stage' (foreign tables)
    p_connector_name    name,                 -- connector for stats + objmap filter
    p_dst_schema        name,                 -- e.g. 'psql_stage' (real tables)
    p_desired_db        name,                 -- e.g. 'free'
    p_offset            text,                 -- offset value (lsn, scn, binlog pos..etc)
    p_case_strategy     text,                 -- 'lower' | 'upper' | 'as_is' (or others treated as as_is)
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
  err_detail        text;

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

  ----------------------------------------------------------------------
  -- Iterate all source FTs that have a same-named real table in dst
  -- NOTE: we still join by relname equality; under your flow both sides
  -- are already created with the same normalized names.
  ----------------------------------------------------------------------
  FOR r IN
    SELECT
      -- canonical table name for mapping/transform matching
      CASE
        WHEN p_case_strategy = 'lower' THEN lower(c.relname)
        WHEN p_case_strategy = 'upper' THEN upper(c.relname)
        ELSE c.relname
      END              AS tbl_canon,

      c.relname        AS src_tbl_name, -- actual FT name in p_src_schema
      c2.relname       AS dst_tbl_name  -- actual table name in p_dst_schema
    FROM   pg_foreign_table ft
    JOIN   pg_class        c  ON c.oid = ft.ftrelid
    JOIN   pg_namespace    n  ON n.oid = c.relnamespace
    JOIN   pg_class        c2 ON c2.relname = c.relname
    JOIN   pg_namespace    n2 ON n2.oid = c2.relnamespace
    WHERE  n.nspname  = p_src_schema
      AND  n2.nspname = p_dst_schema
      AND  c2.relkind = 'r'
  LOOP
    ------------------------------------------------------------------
    -- Error-table identity (display only)
    ------------------------------------------------------------------
    IF p_desired_schema IS NULL OR btrim(p_desired_schema::text) = '' THEN
      v_tbl_display := p_desired_db::text || '.' || r.src_tbl_name;
    ELSE
      v_tbl_display := p_desired_db::text || '.'
                    || p_desired_schema::text || '.'
                    || r.src_tbl_name;
    END IF;

    v_any_batch_failed := false;

    BEGIN
      ------------------------------------------------------------------
      -- Build column/expr lists.
      -- FIX: Canonicalize all objmap matching based on p_case_strategy,
      -- while still using actual identifiers for SQL.
      ------------------------------------------------------------------
      RAISE NOTICE 'Migrating table: %', v_tbl_display;

      WITH dst_cols AS (
        SELECT
          c.ordinal_position,
          -- canonical for matching
          CASE
            WHEN p_case_strategy = 'lower' THEN lower(c.column_name)
            WHEN p_case_strategy = 'upper' THEN upper(c.column_name)
            ELSE c.column_name
          END AS dst_col_canon,
          -- actual identifier for SQL
          c.column_name AS dst_col_ident
        FROM information_schema.columns c
        WHERE c.table_schema = p_dst_schema
          AND c.table_name   = r.dst_tbl_name
      ),

      colmap AS (
        ----------------------------------------------------------------
        -- COLUMN rename rules:
        -- A) schema-qualified: <src_schema>.<table>.<col>  -> <dst col>
        -- B) db-prefixed:      db[.schema].table.col       -> <dst col>
        -- FIX: canonicalize tbl/src_col/dst_col on output.
        ----------------------------------------------------------------

        -- A) schema-qualified
        SELECT
          CASE
            WHEN p_case_strategy='lower' THEN lower(split_part(m.srcobj,'.',2))
            WHEN p_case_strategy='upper' THEN upper(split_part(m.srcobj,'.',2))
            ELSE split_part(m.srcobj,'.',2)
          END AS tbl,

          CASE
            WHEN p_case_strategy='lower' THEN lower(split_part(m.srcobj,'.',3))
            WHEN p_case_strategy='upper' THEN upper(split_part(m.srcobj,'.',3))
            ELSE split_part(m.srcobj,'.',3)
          END AS src_col,

          CASE
            WHEN p_case_strategy='lower' THEN lower(
              (regexp_split_to_array(m.dstobj, '\.'))[
                array_length(regexp_split_to_array(m.dstobj, '\.'),1)
              ]
            )
            WHEN p_case_strategy='upper' THEN upper(
              (regexp_split_to_array(m.dstobj, '\.'))[
                array_length(regexp_split_to_array(m.dstobj, '\.'),1)
              ]
            )
            ELSE
              (regexp_split_to_array(m.dstobj, '\.'))[
                array_length(regexp_split_to_array(m.dstobj, '\.'),1)
              ]
          END AS dst_col
        FROM synchdb_objmap AS m
        WHERE m.objtype='column'
          AND m.enabled
          AND m.name = p_connector_name
          AND split_part(m.srcobj,'.',1) = p_src_schema
          AND m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''

        UNION ALL

        -- B) db-prefixed
        SELECT
          CASE
            WHEN p_case_strategy='lower' THEN lower(
              CASE WHEN array_length(arr,1)=4 THEN arr[3]
                   WHEN array_length(arr,1)=3 THEN arr[2] END
            )
            WHEN p_case_strategy='upper' THEN upper(
              CASE WHEN array_length(arr,1)=4 THEN arr[3]
                   WHEN array_length(arr,1)=3 THEN arr[2] END
            )
            ELSE
              CASE WHEN array_length(arr,1)=4 THEN arr[3]
                   WHEN array_length(arr,1)=3 THEN arr[2] END
          END AS tbl,

          CASE
            WHEN p_case_strategy='lower' THEN lower(arr[array_length(arr,1)])
            WHEN p_case_strategy='upper' THEN upper(arr[array_length(arr,1)])
            ELSE arr[array_length(arr,1)]
          END AS src_col,

          CASE
            WHEN p_case_strategy='lower' THEN lower(
              (regexp_split_to_array(m2.dstobj, '\.'))[
                array_length(regexp_split_to_array(m2.dstobj, '\.'),1)
              ]
            )
            WHEN p_case_strategy='upper' THEN upper(
              (regexp_split_to_array(m2.dstobj, '\.'))[
                array_length(regexp_split_to_array(m2.dstobj, '\.'),1)
              ]
            )
            ELSE
              (regexp_split_to_array(m2.dstobj, '\.'))[
                array_length(regexp_split_to_array(m2.dstobj, '\.'),1)
              ]
          END AS dst_col
        FROM (
          SELECT regexp_split_to_array(srcobj, '\.') AS arr, dstobj
          FROM synchdb_objmap
          WHERE objtype='column'
            AND enabled
            AND name = p_connector_name
            AND dstobj IS NOT NULL AND btrim(dstobj) <> ''
        ) m2
        WHERE arr[1] = p_desired_db
          AND (
            p_desired_schema IS NULL OR p_desired_schema = ''
            OR array_length(arr,1)=3
            OR arr[2] = p_desired_schema
          )
      ),

      col_map AS (
        ----------------------------------------------------------------
        -- Map destination columns -> canonical source column names
        -- using canonical matching (tbl + dst_col).
        ----------------------------------------------------------------
        SELECT
          d.ordinal_position,
          d.dst_col_ident,
          d.dst_col_canon,
          COALESCE(
            (SELECT cm.src_col
             FROM colmap cm
             WHERE cm.tbl = r.tbl_canon
               AND cm.dst_col = d.dst_col_canon
             LIMIT 1),
            d.dst_col_canon
          ) AS src_col_canon
        FROM dst_cols d
      ),

      src_presence AS (
        ----------------------------------------------------------------
        -- Source FT column inventory:
        -- FIX: canonicalize column_name for matching, but keep actual ident.
        -- DISTINCT ON avoids duplicates if case variants exist.
        ----------------------------------------------------------------
        SELECT DISTINCT ON (table_name,
                           CASE
                             WHEN p_case_strategy='lower' THEN lower(column_name)
                             WHEN p_case_strategy='upper' THEN upper(column_name)
                             ELSE column_name
                           END)
          table_name AS src_tbl_ident,
          CASE
            WHEN p_case_strategy='lower' THEN lower(column_name)
            WHEN p_case_strategy='upper' THEN upper(column_name)
            ELSE column_name
          END AS src_col_canon,
          column_name AS src_col_ident
        FROM information_schema.columns
        WHERE table_schema = p_src_schema
        ORDER BY table_name,
                 CASE
                   WHEN p_case_strategy='lower' THEN lower(column_name)
                   WHEN p_case_strategy='upper' THEN upper(column_name)
                   ELSE column_name
                 END,
                 ordinal_position
      ),

      tmap AS (
        ----------------------------------------------------------------
        -- TRANSFORM rules:
        -- A) schema-qualified: <src_schema>.<table>.<col> -> <expr>
        -- B) db-prefixed:      db[.schema].table.col     -> <expr>
        -- FIX: canonicalize tbl/src_col for matching.
        ----------------------------------------------------------------

        -- A) schema-qualified
        SELECT
          CASE
            WHEN p_case_strategy='lower' THEN lower(split_part(mt.srcobj,'.',2))
            WHEN p_case_strategy='upper' THEN upper(split_part(mt.srcobj,'.',2))
            ELSE split_part(mt.srcobj,'.',2)
          END AS tbl,

          CASE
            WHEN p_case_strategy='lower' THEN lower(split_part(mt.srcobj,'.',3))
            WHEN p_case_strategy='upper' THEN upper(split_part(mt.srcobj,'.',3))
            ELSE split_part(mt.srcobj,'.',3)
          END AS src_col,

          mt.dstobj AS expr
        FROM synchdb_objmap AS mt
        WHERE mt.objtype='transform'
          AND mt.enabled
          AND mt.name = p_connector_name
          AND split_part(mt.srcobj,'.',1) = p_src_schema
          AND mt.dstobj IS NOT NULL AND btrim(mt.dstobj) <> ''

        UNION ALL

        -- B) db-prefixed
        SELECT
          CASE
            WHEN p_case_strategy='lower' THEN lower(
              CASE WHEN array_length(arr,1)=4 THEN arr[3]
                   WHEN array_length(arr,1)=3 THEN arr[2] END
            )
            WHEN p_case_strategy='upper' THEN upper(
              CASE WHEN array_length(arr,1)=4 THEN arr[3]
                   WHEN array_length(arr,1)=3 THEN arr[2] END
            )
            ELSE
              CASE WHEN array_length(arr,1)=4 THEN arr[3]
                   WHEN array_length(arr,1)=3 THEN arr[2] END
          END AS tbl,

          CASE
            WHEN p_case_strategy='lower' THEN lower(arr[array_length(arr,1)])
            WHEN p_case_strategy='upper' THEN upper(arr[array_length(arr,1)])
            ELSE arr[array_length(arr,1)]
          END AS src_col,

          t.dstobj AS expr
        FROM (
          SELECT regexp_split_to_array(srcobj, '\.') AS arr, dstobj
          FROM synchdb_objmap
          WHERE objtype='transform'
            AND enabled
            AND name = p_connector_name
            AND dstobj IS NOT NULL AND btrim(dstobj) <> ''
        ) t
        WHERE arr[1] = p_desired_db
          AND (
            p_desired_schema IS NULL OR p_desired_schema = ''
            OR array_length(arr,1)=3
            OR arr[2] = p_desired_schema
          )
      ),

      exprs AS (
        ----------------------------------------------------------------
        -- Build source expressions using actual source column identifiers.
        -- Matching for transform uses canonicalized tbl + src_col.
        ----------------------------------------------------------------
        SELECT
          m.ordinal_position,
          m.dst_col_ident,
          CASE
            WHEN sp.src_col_ident IS NULL THEN
              'NULL'
            ELSE COALESCE(
                   (SELECT replace(tt.expr, '%d', quote_ident(sp.src_col_ident))
                    FROM tmap tt
                    WHERE tt.tbl = r.tbl_canon
                      AND tt.src_col = m.src_col_canon
                    LIMIT 1),
                   quote_ident(sp.src_col_ident)
                 )
          END AS src_expr
        FROM col_map m
        LEFT JOIN src_presence sp
          ON sp.src_tbl_ident = r.src_tbl_name   -- exact FT name
         AND sp.src_col_canon = m.src_col_canon
      )

      SELECT
        string_agg(quote_ident(dst_col_ident), ', ' ORDER BY ordinal_position),
        string_agg(src_expr,                  ', ' ORDER BY ordinal_position)
      INTO dst_list, src_list
      FROM exprs;

      IF dst_list IS NULL OR src_list IS NULL THEN
        RAISE NOTICE 'Skipping %.%: no column metadata', p_dst_schema, r.dst_tbl_name;
        CONTINUE;
      END IF;

      -- Optional debug while validating:
      -- RAISE NOTICE 'dst_list=%', dst_list;
      -- RAISE NOTICE 'src_list=%', src_list;

      ------------------------------------------------------------------
      -- Optional truncate
      ------------------------------------------------------------------
      IF p_do_truncate THEN
        BEGIN
          EXECUTE format('TRUNCATE %I.%I', p_dst_schema, r.dst_tbl_name);
        EXCEPTION WHEN OTHERS THEN
          IF NOT v_err_tbl_created THEN
            EXECUTE format(
              'CREATE TABLE IF NOT EXISTS %I.%I (
                 connector_name name        NOT NULL,
                 tbl            text        NOT NULL,
                 err_state      text        NOT NULL,
                 err_msg        text        NOT NULL,
                 err_detail     text,
                 err_offset     text,
                 ts             timestamptz NOT NULL DEFAULT now(),
                 CONSTRAINT %I UNIQUE (connector_name, tbl)
               )',
              'public', v_err_tbl_ident, ('uq_'||v_err_tbl_ident)
            );
            v_err_tbl_created := true;
            v_err_tbl_exists  := true;
          END IF;

          GET STACKED DIAGNOSTICS err_state  = RETURNED_SQLSTATE,
                                  err_msg    = MESSAGE_TEXT,
                                  err_detail = PG_EXCEPTION_DETAIL;

          EXECUTE format(
            'INSERT INTO %I.%I (connector_name, tbl, err_state, err_msg, err_detail, err_offset)
             VALUES ($1,$2,$3,$4,$5,$6)
             ON CONFLICT (connector_name, tbl)
             DO UPDATE SET err_state  = EXCLUDED.err_state,
                           err_msg    = EXCLUDED.err_msg,
                           err_detail = EXCLUDED.err_detail,
                           err_offset = EXCLUDED.err_offset,
                           ts         = now()',
            'public', v_err_tbl_ident
          )
          USING p_connector_name, v_tbl_display, err_state, err_msg, err_detail, p_offset;

          IF NOT p_continue_on_error THEN
            RAISE;
          ELSE
            RAISE WARNING 'TRUNCATE %.% failed: % [%]', p_dst_schema, r.dst_tbl_name, err_msg, err_state;
          END IF;
        END;
      END IF;

      ------------------------------------------------------------------
      -- Insert path (no batching)
      ------------------------------------------------------------------
      IF COALESCE(p_rows_per_tick,0) <= 0 THEN
        EXECUTE format(
          'INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I',
          p_dst_schema, r.dst_tbl_name,
          dst_list,
          src_list,
          p_src_schema, r.src_tbl_name
        );

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        PERFORM synchdb_set_snapstats(p_connector_name, 0::bigint, v_rows::bigint, 0::bigint, 0::bigint);

        RAISE NOTICE 'Loaded %.% from %.% (rows=%)',
                     p_dst_schema, r.dst_tbl_name,
                     p_src_schema, r.src_tbl_name,
                     v_rows;

        -- success for whole table: delete error row if table exists
        IF v_err_tbl_exists THEN
          EXECUTE format(
            'DELETE FROM %I.%I WHERE connector_name = $1 AND tbl = $2',
            'public', v_err_tbl_ident
          )
          USING p_connector_name, v_tbl_display;
        END IF;

      ELSE
        ----------------------------------------------------------------
        -- Batching path (unchanged)
        ----------------------------------------------------------------
        EXECUTE format(
          'SELECT count(*) FROM (SELECT %s FROM %I.%I) q',
          src_list, p_src_schema, r.src_tbl_name
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
            p_dst_schema, r.dst_tbl_name,
            dst_list,
            src_list,
            src_list,
            p_src_schema, r.src_tbl_name,
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
                     err_detail     text,
                     err_offset     text,
                     ts             timestamptz NOT NULL DEFAULT now(),
                     CONSTRAINT %I UNIQUE (connector_name, tbl)
                   )',
                  'public', v_err_tbl_ident, ('uq_'||v_err_tbl_ident)
                );
                v_err_tbl_created := true;
                v_err_tbl_exists  := true;
              END IF;

              GET STACKED DIAGNOSTICS err_state  = RETURNED_SQLSTATE,
                                      err_msg    = MESSAGE_TEXT,
                                      err_detail = PG_EXCEPTION_DETAIL;

              EXECUTE format(
                'INSERT INTO %I.%I (connector_name, tbl, err_state, err_msg, err_detail, err_offset)
                 VALUES ($1,$2,$3,$4,$5,$6)
                 ON CONFLICT (connector_name, tbl)
                 DO UPDATE SET err_state  = EXCLUDED.err_state,
                               err_msg    = EXCLUDED.err_msg,
                               err_detail = EXCLUDED.err_detail,
                               err_offset = EXCLUDED.err_offset,
                               ts         = now()',
                'public', v_err_tbl_ident
              )
              USING p_connector_name, v_tbl_display, err_state, err_msg, err_detail, p_offset;

              IF NOT p_continue_on_error THEN
                RAISE;
              ELSE
                RAISE WARNING 'Batch insert %.% rows (%-%) failed: % [%]',
                              p_dst_schema, r.dst_tbl_name,
                              v_off+1, v_off+p_rows_per_tick,
                              err_msg, err_state;
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
                         p_dst_schema, r.dst_tbl_name, v_rows, v_off, v_total;
          END IF;

          v_off := v_off + p_rows_per_tick;
        END LOOP;

        -- If no batch failed, delete error row for this table (if it exists)
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
        RAISE WARNING 'An error has occurred while migrating a table (%).', v_tbl_display;

        -- record failure (UPSERT) for this table
        IF NOT v_err_tbl_created THEN
          EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I (
               connector_name name        NOT NULL,
               tbl            text        NOT NULL,
               err_state      text        NOT NULL,
               err_msg        text        NOT NULL,
               err_detail     text,
               err_offset     text,
               ts             timestamptz NOT NULL DEFAULT now(),
               CONSTRAINT %I UNIQUE (connector_name, tbl)
             )',
            'public', v_err_tbl_ident, ('uq_'||v_err_tbl_ident)
          );
          v_err_tbl_created := true;
          v_err_tbl_exists  := true;
        END IF;

        GET STACKED DIAGNOSTICS err_state  = RETURNED_SQLSTATE,
                                err_msg    = MESSAGE_TEXT,
                                err_detail = PG_EXCEPTION_DETAIL;

        EXECUTE format(
          'INSERT INTO %I.%I (connector_name, tbl, err_state, err_msg, err_detail, err_offset)
           VALUES ($1,$2,$3,$4,$5,$6)
           ON CONFLICT (connector_name, tbl)
           DO UPDATE SET err_state  = EXCLUDED.err_state,
                         err_msg    = EXCLUDED.err_msg,
                         err_detail = EXCLUDED.err_detail,
                         err_offset = EXCLUDED.err_offset,
                         ts         = now()',
          'public', v_err_tbl_ident
        )
        USING p_connector_name, v_tbl_display, err_state, err_msg, err_detail, p_offset;

        IF NOT p_continue_on_error THEN
          RAISE;
        ELSE
          RAISE WARNING 'Table %.% failed, continuing: % [%]',
                        p_dst_schema, r.dst_tbl_name, err_msg, err_state;
        END IF;
    END; -- end per-table block
  END LOOP;

  ----------------------------------------------------------------------
  -- Final summary (only if we created the error table this run OR it pre-existed)
  ----------------------------------------------------------------------
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

COMMENT ON FUNCTION synchdb_migrate_data_with_transforms(name, name, name, name, text, text, name, boolean, int, boolean, boolean) IS
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
  ins_sql     text;
BEGIN
  ----------------------------------------------------------------------
  -- Iterate all source FTs that have a same-named real table
  -- in the destination schema.
  --
  -- tbl_canon (lowercase) is only for objmap lookups; src_tbl_name
  -- and dst_tbl_name preserve the exact relname for SQL and column
  -- metadata, so case-variants stay distinct.
  ----------------------------------------------------------------------
  FOR r IN
    SELECT
      lower(c.relname) AS tbl_canon,    -- canonical (lower) name for objmap
      c.relname        AS src_tbl_name, -- actual source FT name
      c2.relname       AS dst_tbl_name  -- actual destination table name
    FROM   pg_foreign_table ft
    JOIN   pg_class        c  ON c.oid = ft.ftrelid
    JOIN   pg_namespace    n  ON n.oid = c.relnamespace
    JOIN   pg_class        c2 ON c2.relname = c.relname    -- *** exact match ***
    JOIN   pg_namespace    n2 ON n2.oid = c2.relnamespace
    WHERE  n.nspname  = p_src_schema
      AND  n2.nspname = p_dst_schema
      AND  c2.relkind = 'r'
  LOOP
    ------------------------------------------------------------------
    -- Build per-table column lists:
    --   - dst_cols: canonical + actual destination column names
    --   - colmap/tmap: mapping & transforms from synchdb_objmap
    --   - src_presence: canonical + actual source column names,
    --                   keyed by the *exact* FT name, with at most
    --                   one row per (table, lower(column_name)).
    ------------------------------------------------------------------
    WITH dst_cols AS (
      SELECT
        c.ordinal_position,
        lower(c.column_name) AS dst_col_canon,
        c.column_name        AS dst_col_ident
      FROM information_schema.columns c
      WHERE c.table_schema = p_dst_schema
        AND c.table_name   = r.dst_tbl_name
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
      -- Map destination columns (by canonical name) to canonical source column names
      SELECT
        d.ordinal_position,
        d.dst_col_ident,
        d.dst_col_canon,
        COALESCE(
          (SELECT cm.src_col
             FROM colmap cm
            WHERE cm.tbl = r.tbl_canon
              AND cm.dst_col = d.dst_col_canon
            LIMIT 1),
          d.dst_col_canon
        ) AS src_col_canon
      FROM dst_cols d
    ),
    src_presence AS (
      ----------------------------------------------------------------
      -- FIX: collapse duplicate source columns per (table, canon name)
      -- so A/a variants become exactly ONE mapping for canon 'a'.
      -- Still keyed by *exact* FT name in the join.
      ----------------------------------------------------------------
      SELECT DISTINCT ON (table_name, lower(column_name))
             table_name         AS src_tbl_ident,
             lower(column_name) AS src_col_canon,
             column_name        AS src_col_ident
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
      ORDER BY table_name, lower(column_name), ordinal_position
    ),
    tmap AS (
      -- Transform rules (canonical src_col but free-form dst expr with %d placeholder)
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
      -- Build source expressions using *actual* source column identifiers,
      -- falling back to NULL when the source column does not exist.
      SELECT
        m.ordinal_position,
        m.dst_col_ident,
        CASE
          WHEN sp.src_col_ident IS NULL THEN
            'NULL'
          ELSE COALESCE(
                 (SELECT replace(tt.expr, '%d', quote_ident(sp.src_col_ident))
                    FROM tmap tt
                   WHERE tt.tbl = r.tbl_canon
                     AND tt.src_col = m.src_col_canon
                   LIMIT 1),
                 quote_ident(sp.src_col_ident)
               )
        END AS src_expr
      FROM col_map m
      LEFT JOIN src_presence sp
        ON sp.src_tbl_ident  = r.src_tbl_name   -- *** exact FT name ***
       AND sp.src_col_canon = m.src_col_canon
    )
    SELECT
      string_agg(quote_ident(dst_col_ident), ', ' ORDER BY ordinal_position),
      string_agg(src_expr,                  ', ' ORDER BY ordinal_position)
    INTO dst_list, src_list
    FROM exprs;

    IF dst_list IS NULL OR src_list IS NULL THEN
      RAISE NOTICE 'Skipping %.%: no column metadata', p_dst_schema, r.dst_tbl_name;
      CONTINUE;
    END IF;

    IF p_do_truncate THEN
      EXECUTE format('TRUNCATE %I.%I', p_dst_schema, r.dst_tbl_name);
    END IF;

    ------------------------------------------------------------------
    -- No batching: one-shot insert, then report actual row count
    ------------------------------------------------------------------
    IF COALESCE(p_rows_per_tick, 0) <= 0 THEN
      EXECUTE format(
        'INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I',
        p_dst_schema, r.dst_tbl_name,
        dst_list,
        src_list,
        p_src_schema, r.src_tbl_name
      );
      GET DIAGNOSTICS v_rows = ROW_COUNT;

      PERFORM synchdb_set_snapstats(p_connector_name, 0::bigint, v_rows::bigint, 0::bigint, 0::bigint);
      RAISE NOTICE 'Loaded %.% from %.% (rows=%)',
                   p_dst_schema, r.dst_tbl_name,
                   p_src_schema, r.src_tbl_name,
                   v_rows;

    ELSE
      ----------------------------------------------------------------
      -- Batching mode: insert in chunks; call stats after each chunk
      ----------------------------------------------------------------
      EXECUTE format(
        'SELECT count(*) FROM (SELECT %s FROM %I.%I) q',
        src_list, p_src_schema, r.src_tbl_name
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
          p_dst_schema, r.dst_tbl_name,
          dst_list,
          src_list,
          src_list,
          p_src_schema, r.src_tbl_name,
          v_off, v_off + p_rows_per_tick
        );

        EXECUTE ins_sql;
        GET DIAGNOSTICS v_rows = ROW_COUNT;

        IF v_rows > 0 THEN
          PERFORM synchdb_set_snapstats(p_connector_name, 0::bigint, v_rows::bigint, 0::bigint, 0::bigint);
          RAISE NOTICE 'Loaded batch: %.% (+% rows, offset % / %)',
                       p_dst_schema, r.dst_tbl_name, v_rows, v_off, v_total;
        END IF;

        v_off := v_off + p_rows_per_tick;
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
    p_desired_schema  name DEFAULT NULL,   -- e.g. 'dbzuser' (only enforced if srcobj includes a schema)
	p_case_strategy   text DEFAULT 'asis'
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
    parts := regexp_split_to_array(r.srcobj, '\.');
    s_db := NULL; s_schema := NULL; s_table := NULL;

    IF array_length(parts,1) = 3 THEN
      s_db := parts[1]; s_schema := parts[2]; s_table := parts[3];
    ELSIF array_length(parts,1) = 2 THEN
      s_db := parts[1]; s_table := parts[2];         -- no schema segment
    ELSE
      CONTINUE;                                      -- unsupported form
    END IF;

    IF p_case_strategy = 'lower' THEN
      s_table := lower(s_table);
    ELSIF p_case_strategy = 'upper' THEN
      s_table := upper(s_table);
    ELSE
      -- 'asis' → leave untouched
      NULL;
    END IF;

    -- Require matching database
    IF p_desired_db IS NOT NULL AND s_db IS DISTINCT FROM p_desired_db THEN
      CONTINUE;
    END IF;

    -- Only enforce desired schema if a schema segment exists in srcobj
    IF p_desired_schema IS NOT NULL
       AND s_schema IS NOT NULL
       AND s_schema <> p_desired_schema THEN
      CONTINUE;
    END IF;

    -- Parse destination: "schema.table" or just "table" (defaults to public)
    IF strpos(r.dstobj, '.') > 0 THEN
      d_schema := split_part(r.dstobj, '.', 1);
      d_table  := split_part(r.dstobj, '.', 2);
    ELSE
      d_schema := 'public';
      d_table  := r.dstobj;
    END IF;

    -- Only act if the source table exists in p_src_schema
    IF NOT EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = p_src_schema
        AND c.relkind = 'r'
        AND c.relname = s_table
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
        AND c.relname = d_table
    ) THEN
      RAISE NOTICE 'dropping % -> %.%: destination exists', s_table, d_schema, d_table;
      EXECUTE format('DROP TABLE %I.%I', d_schema, d_table);
      -- CONTINUE;
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

COMMENT ON FUNCTION synchdb_apply_table_mappings(name, name, name, name, text) IS
   'transform table names according to synchdb_objmap';

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
    v_connector text;           -- 'oracle' | 'olr' | 'mysql' | 'postgres' (lowercased)
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
    RAISE NOTICE 'Step 1.5: Materialize foreign object views to %', v_meta_schema;

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

    ELSE
        RAISE EXCEPTION 'Unsupported connector type: %', v_connector;
    END IF;

    RAISE NOTICE 'Step 1.5: Materializing foreign object views to %', v_meta_schema;
    PERFORM synchdb_materialize_ora_metadata(p_source_schema, v_meta_schema, p_on_exists);

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
        PERFORM synchdb_create_ora_stage_fts(
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
        PERFORM synchdb_create_ora_stage_fts(
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
        PERFORM synchdb_create_ora_stage_fts(
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

    IF p_use_subtx THEN

        -- With per-table subtransactions
        IF v_connector IN ('oracle', 'olr') THEN
            PERFORM synchdb_migrate_data_with_transforms(
                p_stage_schema,
                p_connector_name,
                p_dest_schema,
                p_lookup_db,
                json_build_object('scn', v_scn)::text,
				p_case_strategy,
                p_lookup_schema,
                false, 0,
                true,
                true
            );
        ELSIF v_connector = 'mysql' THEN
            PERFORM synchdb_migrate_data_with_transforms(
                p_stage_schema,
                p_connector_name,
                p_dest_schema,
                p_lookup_db,
                json_build_object('file', v_binlog_file, 'pos', v_binlog_pos, 'ts_sec', floor(extract(epoch from clock_timestamp())))::text,
				p_case_strategy,
                null,
                false, 0,
                true,
                true
            );
        ELSIF v_connector = 'postgres' THEN
            PERFORM synchdb_migrate_data_with_transforms(
                p_stage_schema,
                p_connector_name,
                p_dest_schema,
                p_lookup_db,
                json_build_object('lsn', v_lsn)::text,
				p_case_strategy,
                p_lookup_schema,
                false, 0,
                true,
                true
            );
        ELSE
            RAISE EXCEPTION 'Unsupported connector type: %', v_connector;
        END IF;

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
        RAISE EXCEPTION 'synchdb_do_initial_snapshot() failed: % [%]', SQLERRM, SQLSTATE
            USING HINT = 'Check NOTICE logs above to identify which step failed.';
END;
$$;

COMMENT ON FUNCTION synchdb_do_initial_snapshot(name, text, name, name, name, text, name, boolean, text, text, text, boolean, boolean, text) IS
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
    RAISE NOTICE 'Step 1.5: Materialize foreign object views to %', v_meta_schema;

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

    ELSE
        RAISE EXCEPTION 'Unsupported connector type: %', v_connector;
    END IF;

    RAISE NOTICE 'Step 1.5: Materializing foreign object views to %', v_meta_schema;
    PERFORM synchdb_materialize_ora_metadata(p_source_schema, v_meta_schema, p_on_exists);

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
        PERFORM synchdb_create_ora_stage_fts(
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
        PERFORM synchdb_create_ora_stage_fts(
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
        PERFORM synchdb_create_ora_stage_fts(
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

CREATE OR REPLACE FUNCTION mysql_type_to_jdbc(mysql_type_text text)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  t           text;     -- normalized input
  base        text;     -- base type token(s) without length/precision
  m           text[];   -- regex match for (...) args
  is_unsigned boolean := false;
BEGIN
  -- Normalize: trim, compress spaces, uppercase, strip quotes/backticks
  t := regexp_replace(upper(btrim(mysql_type_text)), '\s+', ' ', 'g');
  t := replace(replace(t, '"', ''), '''', '');
  t := replace(t, '`', '');

  -- Detect UNSIGNED flag (doesn't usually affect jdbcType, but matters for BIGINT)
  IF t LIKE '% UNSIGNED' THEN
    is_unsigned := true;
    t := regexp_replace(t, '\s+UNSIGNED$', '');
  END IF;

  -- Extract base token before any ( ... ) or trailing qualifiers
  m := regexp_match(t, '^([A-Z0-9_ ]+)\s*\(');
  IF m IS NOT NULL THEN
    base := btrim(m[1]);
  ELSE
    base := t;
  END IF;

  /*
   * Map to java.sql.Types (int) in a Debezium-like way.
   *
   * Reference (commonly used values):
   *   CHAR            -> 1
   *   VARCHAR/TEXT    -> 12
   *   BIT             -> -7
   *   TINYINT/SMALLINT-> 5
   *   INTEGER         -> 4
   *   BIGINT          -> -5 (or 3 when UNSIGNED to avoid overflow)
   *   NUMERIC/DECIMAL -> 3
   *   REAL/FLOAT      -> 6
   *   DOUBLE          -> 8
   *   DATE            -> 91
   *   TIME            -> 92
   *   TIMESTAMP       -> 2014 (timestamp with time zone, as in your example)
   *   BINARY          -> -2
   *   VARBINARY       -> -3
   *   BLOB*           -> 2004
   *   JSON/GEOMETRY   -> 1111 (OTHER)
   */

  CASE base

    ------------------------------------------------------------------
    -- Exact numeric / decimal family
    ------------------------------------------------------------------
    WHEN 'DECIMAL', 'NUMERIC', 'FIXED' THEN
      RETURN 3;       -- DECIMAL (signed or unsigned; length/scale handled elsewhere)

    ------------------------------------------------------------------
    -- Float / double family
    ------------------------------------------------------------------
    WHEN 'DOUBLE', 'DOUBLE PRECISION' THEN
      RETURN 8;       -- DOUBLE
    WHEN 'REAL', 'FLOAT' THEN
      RETURN 6;       -- FLOAT

    ------------------------------------------------------------------
    -- Integer family
    ------------------------------------------------------------------
    WHEN 'BIGINT' THEN
      -- Debezium uses DECIMAL for BIGINT UNSIGNED to avoid overflow.
      IF is_unsigned THEN
        RETURN 3;     -- DECIMAL
      ELSE
        RETURN -5;    -- BIGINT
      END IF;

    WHEN 'INT', 'INTEGER', 'MEDIUMINT' THEN
      RETURN 4;       -- INTEGER

    WHEN 'SMALLINT', 'TINYINT', 'YEAR' THEN
      -- Your example shows TINYINT and YEAR as int/short-ish types
      RETURN 5;       -- SMALLINT

    ------------------------------------------------------------------
    -- Bit / boolean
    ------------------------------------------------------------------
    WHEN 'BIT' THEN
      RETURN -7;      -- BIT

    WHEN 'BOOL', 'BOOLEAN' THEN
      RETURN 16;      -- BOOLEAN (not from your example, but sane to support)

    ------------------------------------------------------------------
    -- Character / text
    ------------------------------------------------------------------
    WHEN 'CHAR', 'NCHAR' THEN
      RETURN 1;       -- CHAR

    WHEN 'VARCHAR', 'NVARCHAR',
         'TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT',
         'ENUM', 'SET' THEN
      -- Debezium maps all these to VARCHAR-ish
      RETURN 12;      -- VARCHAR

    ------------------------------------------------------------------
    -- Date / time
    ------------------------------------------------------------------
    WHEN 'DATE' THEN
      RETURN 91;      -- DATE

    WHEN 'TIME' THEN
      RETURN 92;      -- TIME (with or without fractional seconds; length handled separately)

    WHEN 'DATETIME' THEN
      RETURN 93;      -- TIMESTAMP

    WHEN 'TIMESTAMP' THEN
      -- In your example, TIMESTAMP maps to 2014 (TIMESTAMP_WITH_TIMEZONE)
      RETURN 2014;

    ------------------------------------------------------------------
    -- Binary / blobs
    ------------------------------------------------------------------
    WHEN 'BINARY' THEN
      RETURN -2;      -- BINARY

    WHEN 'VARBINARY' THEN
      RETURN -3;      -- VARBINARY

    WHEN 'TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB' THEN
      RETURN 2004;    -- BLOB

    ------------------------------------------------------------------
    -- JSON and spatial types
    ------------------------------------------------------------------
    WHEN 'JSON' THEN
      RETURN 1111;    -- OTHER

    WHEN 'GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON',
         'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON',
         'GEOMETRYCOLLECTION' THEN
      RETURN 1111;    -- OTHER

    ------------------------------------------------------------------
    -- Fallback
    ------------------------------------------------------------------
    ELSE
      RETURN 1111;    -- OTHER (catch-all)
  END CASE;
END;
$$;

CREATE OR REPLACE FUNCTION synchdb_type_to_jdbc(
    p_connector text,
    p_type_text text
)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
    v_conn text := lower(p_connector);
BEGIN
    CASE v_conn
        WHEN 'oracle', 'olr' THEN
            RETURN oracle_type_to_jdbc(p_type_text);

        WHEN 'mysql' THEN
            RETURN mysql_type_to_jdbc(p_type_text);
        ELSE
            RAISE EXCEPTION
                'synchdb_type_to_jdbc: unsupported connector type % for type %',
                p_connector, p_type_text;
    END CASE;
END;
$$;

