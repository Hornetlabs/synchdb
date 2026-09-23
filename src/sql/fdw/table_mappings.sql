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

