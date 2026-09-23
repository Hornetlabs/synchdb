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

