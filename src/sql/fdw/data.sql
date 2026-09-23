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

