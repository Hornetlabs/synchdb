CREATE OR REPLACE FUNCTION synchdb_migrate_data_with_transforms(
    p_src_schema        name,                 -- e.g. 'ora_stage' (foreign tables)
    p_connector_name    name,                 -- connector for stats + objmap filter
    p_dst_schema        name,                 -- e.g. 'psql_stage' (real tables)
    p_desired_db        name,                 -- e.g. 'free'
    p_offset            text,                 -- offset value (lsn, scn, binlog pos..etc)
    p_case_strategy     text,                 -- 'lower' | 'upper' | 'asis' ('as_is' is an alias)
    p_desired_schema    name DEFAULT NULL,    -- e.g. 'dbzuser'
    p_do_truncate       boolean DEFAULT false,
    p_rows_per_tick     integer DEFAULT 0,    -- <=0/NULL = one statement; positive values are unsupported
    p_continue_on_error boolean DEFAULT true,
    p_batch_subxact     boolean DEFAULT true  -- retained for signature compatibility; batching is unsupported
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r                 record;
  dst_list          text;
  src_list          text;
  v_rows            bigint;

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

  v_case_strategy   text;
  v_ambiguous_columns text;
BEGIN
  -- Repeated unordered scans cannot partition a foreign table reliably.
  -- Reject the legacy mode before any table-level handler or write.
  IF p_rows_per_tick > 0 THEN
    RAISE EXCEPTION USING
      ERRCODE = '0A000',
      MESSAGE = 'Batched FDW data migration is not supported',
      HINT = 'Use p_rows_per_tick = 0 for a single source scan.';
  END IF;

  v_case_strategy := COALESCE(NULLIF(lower(btrim(p_case_strategy)), ''), 'asis');
  IF v_case_strategy = 'as_is' THEN
    v_case_strategy := 'asis';
  END IF;
  IF v_case_strategy NOT IN ('lower', 'upper', 'asis') THEN
    RAISE EXCEPTION 'Invalid p_case_strategy: %, expected lower|upper|asis', p_case_strategy;
  END IF;

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
        WHEN v_case_strategy = 'lower' THEN lower(c.relname)
        WHEN v_case_strategy = 'upper' THEN upper(c.relname)
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

    BEGIN
      ------------------------------------------------------------------
      -- Build column/expr lists.
      -- Match source rules using the requested case strategy and preserve
      -- actual source/destination identifiers when generating SQL.
      ------------------------------------------------------------------
      RAISE NOTICE 'Migrating table: %', v_tbl_display;

      -- Source-name folding must not rewrite an explicit destination identifier.
      -- Reject an ambiguous source inventory instead of choosing its first column.
      SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
      INTO v_ambiguous_columns
      FROM (
        SELECT column_name, ordinal_position,
               count(*) OVER (
                 PARTITION BY CASE v_case_strategy
                   WHEN 'lower' THEN lower(column_name)
                   WHEN 'upper' THEN upper(column_name)
                   ELSE column_name
                 END
               ) AS matches
        FROM information_schema.columns
        WHERE table_schema = p_src_schema
          AND table_name = r.src_tbl_name
      ) AS source_columns
      WHERE matches > 1;

      IF v_ambiguous_columns IS NOT NULL THEN
        RAISE EXCEPTION 'Ambiguous source columns in %.% under case strategy %: %',
                        p_src_schema, r.src_tbl_name, v_case_strategy, v_ambiguous_columns;
      END IF;

      WITH dst_cols AS (
        SELECT c.ordinal_position,
               CASE v_case_strategy
                 WHEN 'lower' THEN lower(c.column_name)
                 WHEN 'upper' THEN upper(c.column_name)
                 ELSE c.column_name
               END AS dst_col_canon,
               c.column_name AS dst_col_ident
        FROM information_schema.columns c
        WHERE c.table_schema = p_dst_schema
          AND c.table_name = r.dst_tbl_name
      ),
      rules AS (
        SELECT m.objtype,
               regexp_split_to_array(m.srcobj, '\.') AS src_parts,
               regexp_split_to_array(m.dstobj, '\.') AS dst_parts,
               m.dstobj
        FROM synchdb_objmap m
        WHERE m.objtype IN ('column', 'transform')
          AND m.enabled
          AND m.name = p_connector_name
          AND m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''
      ),
      scoped_rules AS (
        SELECT objtype,
               CASE v_case_strategy
                 WHEN 'lower' THEN lower(src_parts[array_length(src_parts, 1) - 1])
                 WHEN 'upper' THEN upper(src_parts[array_length(src_parts, 1) - 1])
                 ELSE src_parts[array_length(src_parts, 1) - 1]
               END AS tbl,
               CASE v_case_strategy
                 WHEN 'lower' THEN lower(src_parts[array_length(src_parts, 1)])
                 WHEN 'upper' THEN upper(src_parts[array_length(src_parts, 1)])
                 ELSE src_parts[array_length(src_parts, 1)]
               END AS src_col,
               dst_parts[array_length(dst_parts, 1)] AS dst_col_ident,
               dstobj AS expr
        FROM rules
        WHERE (
          array_length(src_parts, 1) = 3
          AND (src_parts[1] = p_src_schema::text OR src_parts[1] = p_desired_db::text)
        ) OR (
          array_length(src_parts, 1) = 4
          AND src_parts[1] = p_desired_db::text
          AND (p_desired_schema IS NULL OR p_desired_schema = ''
               OR src_parts[2] = p_desired_schema::text)
        )
      ),
      col_map AS (
        SELECT d.ordinal_position,
               d.dst_col_ident,
               COALESCE(
                 -- A scalar subquery rejects multiple matching rename rules.
                 (SELECT cm.src_col
                  FROM scoped_rules cm
                  WHERE cm.objtype = 'column'
                    AND cm.tbl = r.tbl_canon
                    AND cm.dst_col_ident = d.dst_col_ident),
                 d.dst_col_canon
               ) AS src_col_canon
        FROM dst_cols d
      ),
      src_presence AS (
        SELECT CASE v_case_strategy
                 WHEN 'lower' THEN lower(column_name)
                 WHEN 'upper' THEN upper(column_name)
                 ELSE column_name
               END AS src_col_canon,
               column_name AS src_col_ident
        FROM information_schema.columns
        WHERE table_schema = p_src_schema
          AND table_name = r.src_tbl_name
      ),
      exprs AS (
        SELECT m.ordinal_position,
               m.dst_col_ident,
               CASE
                 WHEN sp.src_col_ident IS NULL THEN 'NULL'
                 ELSE COALESCE(
                   -- A scalar subquery also rejects ambiguous transform rules.
                   (SELECT replace(t.expr, '%d', quote_ident(sp.src_col_ident))
                    FROM scoped_rules t
                    WHERE t.objtype = 'transform'
                      AND t.tbl = r.tbl_canon
                      AND t.src_col = m.src_col_canon),
                   quote_ident(sp.src_col_ident)
                 )
               END AS src_expr
        FROM col_map m
        LEFT JOIN src_presence sp ON sp.src_col_canon = m.src_col_canon
      )
      SELECT string_agg(quote_ident(dst_col_ident), ', ' ORDER BY ordinal_position),
             string_agg(src_expr, ', ' ORDER BY ordinal_position)
      INTO dst_list, src_list
      FROM exprs;

      IF dst_list IS NULL OR src_list IS NULL THEN
        RAISE NOTICE 'Skipping %.%: no column metadata', p_dst_schema, r.dst_tbl_name;
        CONTINUE;
      END IF;

      -- A failed TRUNCATE must roll back this table and reach the common
      -- error handler. Continuing here would append to the old contents.
      IF p_do_truncate THEN
        EXECUTE format('TRUNCATE %I.%I', p_dst_schema, r.dst_tbl_name);
      END IF;

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

      -- Only a successful whole-table copy clears its previous error.
      IF v_err_tbl_exists THEN
        EXECUTE format(
          'DELETE FROM %I.%I WHERE connector_name = $1 AND tbl = $2',
          'public', v_err_tbl_ident
        )
        USING p_connector_name, v_tbl_display;
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
    p_desired_schema  name,                 -- e.g. 'dbzuser'
    p_do_truncate     boolean,
    p_rows_per_tick   integer,              -- <=0/NULL = one statement; positive values are unsupported
    p_case_strategy   text                  -- 'lower' | 'upper' | 'asis' ('as_is' is an alias)
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r           record;
  dst_list    text;
  src_list    text;
  v_rows      bigint;
  v_case_strategy text;
  v_ambiguous_columns text;
BEGIN
  -- Repeated unordered scans cannot partition a foreign table reliably.
  -- Reject the legacy mode before any table-level handler or write.
  IF p_rows_per_tick > 0 THEN
    RAISE EXCEPTION USING
      ERRCODE = '0A000',
      MESSAGE = 'Batched FDW data migration is not supported',
      HINT = 'Use p_rows_per_tick = 0 for a single source scan.';
  END IF;

  v_case_strategy := COALESCE(NULLIF(lower(btrim(p_case_strategy)), ''), 'asis');
  IF v_case_strategy = 'as_is' THEN
    v_case_strategy := 'asis';
  END IF;
  IF v_case_strategy NOT IN ('lower', 'upper', 'asis') THEN
    RAISE EXCEPTION 'Invalid p_case_strategy: %, expected lower|upper|asis', p_case_strategy;
  END IF;

  ----------------------------------------------------------------------
  -- Iterate all source FTs that have a same-named real table
  -- in the destination schema.
  --
  -- tbl_canon follows the requested source-name strategy; src_tbl_name
  -- and dst_tbl_name preserve the exact relname for SQL and column
  -- metadata, so case-variants stay distinct.
  ----------------------------------------------------------------------
  FOR r IN
    SELECT
      CASE v_case_strategy
        WHEN 'lower' THEN lower(c.relname)
        WHEN 'upper' THEN upper(c.relname)
        ELSE c.relname
      END              AS tbl_canon,
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
    --                   keyed by the exact FT name; case collisions fail.
    ------------------------------------------------------------------
    -- Source-name folding must not rewrite an explicit destination identifier.
    -- Reject an ambiguous source inventory instead of choosing its first column.
    SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
    INTO v_ambiguous_columns
    FROM (
      SELECT column_name, ordinal_position,
             count(*) OVER (
               PARTITION BY CASE v_case_strategy
                 WHEN 'lower' THEN lower(column_name)
                 WHEN 'upper' THEN upper(column_name)
                 ELSE column_name
               END
             ) AS matches
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
        AND table_name = r.src_tbl_name
    ) AS source_columns
    WHERE matches > 1;

    IF v_ambiguous_columns IS NOT NULL THEN
      RAISE EXCEPTION 'Ambiguous source columns in %.% under case strategy %: %',
                      p_src_schema, r.src_tbl_name, v_case_strategy, v_ambiguous_columns;
    END IF;

    WITH dst_cols AS (
      SELECT c.ordinal_position,
             CASE v_case_strategy
               WHEN 'lower' THEN lower(c.column_name)
               WHEN 'upper' THEN upper(c.column_name)
               ELSE c.column_name
             END AS dst_col_canon,
             c.column_name AS dst_col_ident
      FROM information_schema.columns c
      WHERE c.table_schema = p_dst_schema
        AND c.table_name = r.dst_tbl_name
    ),
    rules AS (
      SELECT m.objtype,
             regexp_split_to_array(m.srcobj, '\.') AS src_parts,
             regexp_split_to_array(m.dstobj, '\.') AS dst_parts,
             m.dstobj
      FROM synchdb_objmap m
      WHERE m.objtype IN ('column', 'transform')
        AND m.enabled
        AND m.name = p_connector_name
        AND m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''
    ),
    scoped_rules AS (
      SELECT objtype,
             CASE v_case_strategy
               WHEN 'lower' THEN lower(src_parts[array_length(src_parts, 1) - 1])
               WHEN 'upper' THEN upper(src_parts[array_length(src_parts, 1) - 1])
               ELSE src_parts[array_length(src_parts, 1) - 1]
             END AS tbl,
             CASE v_case_strategy
               WHEN 'lower' THEN lower(src_parts[array_length(src_parts, 1)])
               WHEN 'upper' THEN upper(src_parts[array_length(src_parts, 1)])
               ELSE src_parts[array_length(src_parts, 1)]
             END AS src_col,
             dst_parts[array_length(dst_parts, 1)] AS dst_col_ident,
             dstobj AS expr
      FROM rules
      WHERE (
        array_length(src_parts, 1) = 3
        AND (src_parts[1] = p_src_schema::text OR src_parts[1] = p_desired_db::text)
      ) OR (
        array_length(src_parts, 1) = 4
        AND src_parts[1] = p_desired_db::text
        AND (p_desired_schema IS NULL OR p_desired_schema = ''
             OR src_parts[2] = p_desired_schema::text)
      )
    ),
    col_map AS (
      SELECT d.ordinal_position,
             d.dst_col_ident,
             COALESCE(
               -- A scalar subquery rejects multiple matching rename rules.
               (SELECT cm.src_col
                FROM scoped_rules cm
                WHERE cm.objtype = 'column'
                  AND cm.tbl = r.tbl_canon
                  AND cm.dst_col_ident = d.dst_col_ident),
               d.dst_col_canon
             ) AS src_col_canon
      FROM dst_cols d
    ),
    src_presence AS (
      SELECT CASE v_case_strategy
               WHEN 'lower' THEN lower(column_name)
               WHEN 'upper' THEN upper(column_name)
               ELSE column_name
             END AS src_col_canon,
             column_name AS src_col_ident
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
        AND table_name = r.src_tbl_name
    ),
    exprs AS (
      SELECT m.ordinal_position,
             m.dst_col_ident,
             CASE
               WHEN sp.src_col_ident IS NULL THEN 'NULL'
               ELSE COALESCE(
                 -- A scalar subquery also rejects ambiguous transform rules.
                 (SELECT replace(t.expr, '%d', quote_ident(sp.src_col_ident))
                  FROM scoped_rules t
                  WHERE t.objtype = 'transform'
                    AND t.tbl = r.tbl_canon
                    AND t.src_col = m.src_col_canon),
                 quote_ident(sp.src_col_ident)
               )
             END AS src_expr
      FROM col_map m
      LEFT JOIN src_presence sp ON sp.src_col_canon = m.src_col_canon
    )
    SELECT string_agg(quote_ident(dst_col_ident), ', ' ORDER BY ordinal_position),
           string_agg(src_expr, ', ' ORDER BY ordinal_position)
    INTO dst_list, src_list
    FROM exprs;

    IF dst_list IS NULL OR src_list IS NULL THEN
      RAISE NOTICE 'Skipping %.%: no column metadata', p_dst_schema, r.dst_tbl_name;
      CONTINUE;
    END IF;

    IF p_do_truncate THEN
      EXECUTE format('TRUNCATE %I.%I', p_dst_schema, r.dst_tbl_name);
    END IF;

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

  END LOOP;
END;
$$;

COMMENT ON FUNCTION synchdb_migrate_data_with_transforms_nosubs(name, name, name, name, name, boolean, int, text) IS
   'migrate data with source-name case strategy and transform expressions - all or nothing mode';

-- Preserve the historical seven-argument entry point and its lower-case matching.
CREATE OR REPLACE FUNCTION synchdb_migrate_data_with_transforms_nosubs(
    p_src_schema      name,
    p_connector_name  name,
    p_dst_schema      name,
    p_desired_db      name,
    p_desired_schema  name DEFAULT NULL,
    p_do_truncate     boolean DEFAULT false,
    p_rows_per_tick   integer DEFAULT 0
) RETURNS void
LANGUAGE sql
AS $$
  SELECT synchdb_migrate_data_with_transforms_nosubs(
    $1, $2, $3, $4, $5, $6, $7, 'lower'::text
  );
$$;

COMMENT ON FUNCTION synchdb_migrate_data_with_transforms_nosubs(name, name, name, name, name, boolean, int) IS
   'compatibility wrapper for lower-case source matching - all or nothing mode';
