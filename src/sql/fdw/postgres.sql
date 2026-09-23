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

