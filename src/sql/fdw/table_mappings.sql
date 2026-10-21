CREATE OR REPLACE FUNCTION synchdb_apply_table_mappings(
    p_src_schema      name,               -- e.g. 'psql_stage'
    p_connector_name  name,               -- filter objmap rows by connector name
    p_desired_db      name,               -- e.g. 'free' (must match the db token in srcobj)
    p_desired_schema  name,                -- only enforced if srcobj includes a schema
    p_case_strategy   text,
    p_table_names     name[]
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
  dst_parts text[];
  v_error_table text;
  v_failed boolean;
BEGIN
  -- Scope names refer to source tables before a mapping renames or moves them.
  IF p_table_names IS NOT NULL AND cardinality(p_table_names) = 0 THEN
    RETURN;
  END IF;
  v_error_table := 'synchdb_fdw_snapshot_errors_'
      || regexp_replace(lower(p_connector_name::text), '[^a-z0-9_]', '_', 'g');

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

    IF p_table_names IS NOT NULL
       AND (s_table::name = ANY(p_table_names)) IS NOT TRUE THEN
      CONTINUE;
    END IF;

    -- Parse destination: "schema.table" or just "table" (defaults to public)
    dst_parts := regexp_split_to_array(r.dstobj, '\.');
    IF cardinality(dst_parts) = 2 THEN
      d_schema := dst_parts[1];
      d_table := dst_parts[2];
    ELSIF cardinality(dst_parts) = 1 THEN
      d_schema := 'public';
      d_table  := r.dstobj;
    ELSE
      RAISE EXCEPTION 'Invalid table mapping destination: %', r.dstobj USING ERRCODE = '22023';
    END IF;
    IF btrim(d_schema) = '' OR btrim(d_table) = ''
       OR octet_length(d_schema) > current_setting('max_identifier_length')::int
       OR octet_length(d_table) > current_setting('max_identifier_length')::int THEN
      RAISE EXCEPTION 'Invalid or overlong table mapping destination: %', r.dstobj
        USING ERRCODE = '22023';
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

    -- Failed copies must not publish an empty or incomplete target.
    IF to_regclass(format('public.%I', v_error_table)) IS NOT NULL THEN
      EXECUTE format(
        'SELECT EXISTS (SELECT 1 FROM public.%I
          WHERE connector_name = $1
            AND CASE $3 WHEN ''lower'' THEN lower(split_part(tbl, ''.'', -1))
                        WHEN ''upper'' THEN upper(split_part(tbl, ''.'', -1))
                        ELSE split_part(tbl, ''.'', -1) END = $2)',
        v_error_table)
        INTO v_failed USING p_connector_name, s_table, p_case_strategy;
      IF v_failed THEN
        RAISE NOTICE 'Skipping mapping of %.%: snapshot has unresolved errors',
                     p_src_schema, s_table;
        CONTINUE;
      END IF;
    END IF;

    -- An identity mapping is already satisfied, including its existing table.
    IF p_src_schema = d_schema AND s_table = d_table THEN
      CONTINUE;
    END IF;

    -- Ensure destination schema
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', d_schema);

    -- Mapping never grants permission to overwrite an existing relation.
    IF EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = d_schema
        AND c.relname = d_table
    ) THEN
      RAISE EXCEPTION 'Cannot map %.% to %.%: destination already exists',
                      p_src_schema, s_table, d_schema, d_table
        USING ERRCODE = '42P07';
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

COMMENT ON FUNCTION synchdb_apply_table_mappings(name, name, name, name, text, name[]) IS
   'transform table mappings for the supplied materialized source table names; NULL means all tables';

CREATE OR REPLACE FUNCTION synchdb_apply_table_mappings(
    p_src_schema name,
    p_connector_name name,
    p_desired_db name,
    p_desired_schema name DEFAULT NULL,
    p_case_strategy text DEFAULT 'asis'
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM synchdb_apply_table_mappings(p_src_schema, p_connector_name,
                                       p_desired_db, p_desired_schema,
                                       p_case_strategy, NULL::name[]);
END;
$$;

COMMENT ON FUNCTION synchdb_apply_table_mappings(name, name, name, name, text) IS
   'transform table names according to synchdb_objmap';
