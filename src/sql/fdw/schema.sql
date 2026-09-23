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

