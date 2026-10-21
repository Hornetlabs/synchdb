CREATE OR REPLACE FUNCTION synchdb_create_stage_fts_plan(
    p_connector_name        text,                         -- connector name for attribute rows
    p_desired_db            text,                         -- db token to match entries in snapshottable
    p_desired_schema        text,                         -- schema to match when entry includes schema
    p_stage_schema          name, -- target schema in Postgres
    p_server_name           name,  -- oracle_fdw server name
    p_lower_names           boolean,         -- legacy: lower-case PG table/column names
    p_on_exists             text,         -- 'error' | 'replace' | 'drop' | 'skip'
    p_offset                text,            -- offset like scn, binlog pos..etc
    p_source_schema         name, -- metadata source schema (…columns)
    p_snapshot_tables       text,            -- CSV of db.schema.table; when set, bypass conninfo
    p_write_dbz_schema_info boolean,        -- when true, store Debezium schema-history JSON
    p_case_strategy         text, -- 'upper' | 'lower' | 'asis' (table/column casing)
    p_frozen_tables        jsonb,
    p_record_attributes    boolean
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

  use_explicit   boolean := (p_snapshot_tables IS NOT NULL);
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
  v_table_name_expr text;
  v_extension_schema name;

  -- possible offsets
  v_offset_json jsonb;
  v_bad_selection text;
  v_collision text;
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
		 COALESCE(NULLIF(NULLIF(data->'snapshottable', 'null'::jsonb), '"null"'::jsonb),
                  NULLIF(NULLIF(data->'table', 'null'::jsonb), '"null"'::jsonb))
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
  IF p_frozen_tables IS NULL THEN
  CREATE TEMP TABLE IF NOT EXISTS tmp_snap_list (
    db    text NOT NULL,
    schem text,     -- NULL => item didn’t specify schema
    tbl   text NOT NULL
  ) ON COMMIT DROP;
  TRUNCATE tmp_snap_list;
  END IF;

  -- The legacy string sentinel means no configured filter, not an empty list.
  IF v_snapcfg = '"null"'::jsonb THEN
    v_snapcfg := NULL;
  END IF;

  IF p_frozen_tables IS NOT NULL THEN
    IF jsonb_typeof(p_frozen_tables) <> 'array' THEN
      RAISE EXCEPTION 'Invalid frozen FDW table selection' USING ERRCODE = '22023';
    END IF;
    v_use_filter := false;
  ELSIF use_explicit THEN
    v_snaptext := p_snapshot_tables;
    v_use_filter := true;
  ELSIF v_snapcfg IS NOT NULL AND v_snapcfg <> 'null'::jsonb THEN
    v_use_filter := true;
    v_type := jsonb_typeof(v_snapcfg);
    IF v_type = 'string' THEN
      v_snaptext := v_snapcfg #>> '{}';
      IF position('file:' IN v_snaptext) = 1 THEN
        v_snaptext := read_snapshot_table_list(v_snaptext, v_conn_type::text, p_desired_db::text);
      END IF;
    ELSIF v_type = 'array' THEN
      IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_snapcfg) e
                 WHERE jsonb_typeof(e) <> 'string') THEN
        RAISE EXCEPTION 'Snapshot table list must contain only strings';
      END IF;
      SELECT string_agg(x, ',') INTO v_snaptext
        FROM jsonb_array_elements_text(v_snapcfg) AS t(x);
    ELSE
      RAISE EXCEPTION 'synchdb_conninfo[%.data->snapshottable] must be string or array, got %',
                      p_connector_name, v_type;
    END IF;
  END IF;

  IF v_use_filter THEN
    IF v_snaptext IS NULL OR btrim(v_snaptext) = '' THEN
      RAISE EXCEPTION 'Snapshot table selection is explicitly empty';
    END IF;

    SELECT x INTO v_bad_selection
      FROM (SELECT x, regexp_split_to_array(btrim(x), '\.') AS parts
              FROM regexp_split_to_table(v_snaptext, '\s*,\s*') t(x)) s
     WHERE array_length(parts, 1) NOT IN (2, 3)
        OR EXISTS (SELECT 1 FROM unnest(parts) p WHERE btrim(p) = '')
     LIMIT 1;
    IF FOUND THEN
      RAISE EXCEPTION 'Invalid snapshot table identifier: %', v_bad_selection;
    END IF;

    INSERT INTO tmp_snap_list(db, schem, tbl)
    SELECT DISTINCT
      CASE WHEN NOT use_explicit AND v_conn_type <> 'mysql' AND array_length(parts, 1) = 2
           THEN p_desired_db::text ELSE parts[1] END,
      CASE WHEN array_length(parts, 1) = 3 THEN parts[2]
           WHEN NOT use_explicit AND v_conn_type <> 'mysql' THEN parts[1] END,
      parts[array_length(parts, 1)]
      FROM (SELECT regexp_split_to_array(btrim(x), '\.') AS parts
              FROM regexp_split_to_table(v_snaptext, '\s*,\s*') t(x)) s;

    IF EXISTS (SELECT 1 FROM tmp_snap_list WHERE lower(db) <> lower(p_desired_db)) THEN
      RAISE EXCEPTION 'Snapshot selection contains a database outside the configured scope %', p_desired_db;
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

  IF p_frozen_tables IS NOT NULL THEN
    SELECT n.nspname INTO STRICT v_extension_schema FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='synchdb';
    v_colname_expr:=format('%I.synchdb_bootstrap_normalize_name(c.column_name,%L)',v_extension_schema,v_case_strategy);
    v_table_name_expr:=format('%I.synchdb_bootstrap_normalize_name(table_name,%L)',v_extension_schema,v_case_strategy);
  ELSE
    v_table_name_expr:=format('CASE %L WHEN ''lower'' THEN lower(table_name) WHEN ''upper'' THEN upper(table_name) ELSE table_name END',v_case_strategy);
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

  IF p_frozen_tables IS NOT NULL THEN
    base_sql := format(
      'SELECT c."schema" AS ora_owner, c.table_name FROM %I.columns c
        WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(%L::jsonb) f
                       WHERE f->>''db''=%L AND f->>''schema''=c."schema" AND f->>''table''=c.table_name)
        GROUP BY c."schema",c.table_name', p_source_schema,p_frozen_tables::text,p_desired_db::text);
  END IF;

  IF v_use_filter THEN
      base_sql := base_sql || '
        HAVING EXISTS (
          SELECT 1
            FROM tmp_snap_list f
           WHERE lower(table_name) = lower(f.tbl)
             AND (f.schem IS NULL OR lower("schema") = lower(f.schem))
        )';

    EXECUTE format(
      'SELECT f.db || ''.'' || coalesce(f.schem || ''.'', '''') || f.tbl
         FROM tmp_snap_list f
        WHERE (SELECT count(*) FROM (%s) selected
                WHERE lower(selected.table_name) = lower(f.tbl)
                  AND (f.schem IS NULL OR lower(selected.ora_owner) = lower(f.schem))) <> 1
        LIMIT 1', base_sql)
      INTO v_bad_selection;
    IF v_bad_selection IS NOT NULL THEN
      RAISE EXCEPTION 'Snapshot table selection has no unique match in the configured schema: %', v_bad_selection;
    END IF;
  END IF;

  -- Reject normalization collisions before replacing any existing stage FT.
  EXECUTE format(
    'SELECT normalized FROM
       (SELECT (%s)::name AS normalized
          FROM (%s) selected) names
      GROUP BY normalized HAVING count(*) > 1 LIMIT 1',
    v_table_name_expr, base_sql) INTO v_collision;
  IF v_collision IS NOT NULL THEN
    RAISE EXCEPTION 'Snapshot table names collide after normalization: %', v_collision;
  END IF;

  EXECUTE format(
    'SELECT c."schema" || ''.'' || c.table_name || ''.'' || (%s)::name
       FROM %I.columns c JOIN (%s) selected
         ON c."schema" = selected.ora_owner AND c.table_name = selected.table_name
      GROUP BY c."schema", c.table_name, (%s)::name
     HAVING count(*) > 1 LIMIT 1',
    v_colname_expr, p_source_schema, base_sql, v_colname_expr) INTO v_collision;
  IF v_collision IS NOT NULL THEN
    RAISE EXCEPTION 'Snapshot column names collide after normalization: %', v_collision;
  END IF;

  RAISE NOTICE 'base sql = %', base_sql;
  ----------------------------------------------------------------------
  -- Create (or replace) the stage foreign tables
  ----------------------------------------------------------------------
  FOR r IN EXECUTE base_sql || ' ORDER BY ora_owner, table_name'
  LOOP

  BEGIN
	-- decide PG table name according to case strategy
    IF p_frozen_tables IS NOT NULL THEN
        EXECUTE format('SELECT %I.synchdb_bootstrap_normalize_name($1,$2)',v_extension_schema)
          INTO STRICT v_tbl_pg USING r.table_name,v_case_strategy;
    ELSE
        v_tbl_pg := CASE v_case_strategy WHEN 'lower' THEN lower(r.table_name)
                    WHEN 'upper' THEN upper(r.table_name) ELSE r.table_name END;
    END IF;

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
      ELSIF p_on_exists = 'error' THEN
        RAISE EXCEPTION 'Stage foreign table %.% already exists', p_stage_schema, v_tbl_pg
          USING ERRCODE = '42P07';
      ELSE
        RAISE EXCEPTION 'Unknown p_on_exists value: % (use error|replace|drop|skip)', p_on_exists;
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

	ELSIF v_conn_type = 'sqlserver' THEN
	  -- tds_fdw matches columns by name and does so case-sensitively.  Keep
	  -- normalized local names while mapping every column to its exact remote
	  -- SQL Server identifier.
	  EXECUTE format(
	    'SELECT string_agg(
	              quote_ident(%s) || '' '' ||
	              synchdb_translate_datatype(%L::name, lower(c.type_name)::name,
	                                         COALESCE(c.length, -1)::bigint,
	                                         COALESCE(c.scale, -1)::bigint,
	                                         COALESCE(c.precision, -1)::bigint) ||
	              '' OPTIONS (column_name '' || quote_literal(c.column_name) || '')'' ||
	              CASE WHEN lower(coalesce(c.nullable::text, '''')) IN (''no'', ''n'', ''0'', ''false'', ''f'')
	                   THEN '' NOT NULL'' ELSE '''' END,
	              '', '' ORDER BY c.position)
	       FROM %I.columns c
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
      RAISE EXCEPTION 'No columns found for selected source %.%', r.ora_owner, r.table_name USING ERRCODE = '22000';
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
	ELSIF v_conn_type = 'sqlserver' THEN
		/*
		 * DB-Library renders SQL Server DATE values using a locale-dependent
		 * datetime string (for example "Jan 16 2016 12:00:00:AM"), which is
		 * not valid PostgreSQL date input.  Use a projection query so DATE
		 * columns arrive in unambiguous ISO 8601 form.  Quote every remote
		 * identifier with SQL Server brackets at the same time.
		 */
		EXECUTE format(
		  'SELECT string_agg(
		            CASE WHEN lower(c.type_name) = ''date'' THEN
		                   ''CONVERT(char(10), ['' || replace(c.column_name, '']'', '']]'') ||
		                   ''], 23) AS ['' || replace(c.column_name, '']'', '']]'') || '']''
		                 ELSE
		                   ''['' || replace(c.column_name, '']'', '']]'') || '']''
		            END,
		            '', '' ORDER BY c.position)
		     FROM %I.columns c
		    WHERE c."schema" = %L
		      AND c.table_name = %L',
		  p_source_schema, r.ora_owner, r.table_name
		)
		INTO v_sel_list;

		v_subquery := format(
		  'SELECT %s FROM [%s].[%s]',
		  v_sel_list,
		  replace(r.ora_owner, ']', ']]'),
		  replace(r.table_name, ']', ']]')
		);

		EXECUTE format(
			'CREATE FOREIGN TABLE %I.%I (%s) SERVER %I OPTIONS (query %L)',
			p_stage_schema, v_tbl_pg, v_cols_sql, p_server_name, v_subquery
		);

		RAISE NOTICE 'Created SQL Server FT %.% -> %.% on %',
				   p_stage_schema, v_tbl_pg, r.ora_owner, r.table_name, p_server_name;
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
                     quote_ident(%s)
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
          v_colname_expr,
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
    IF p_frozen_tables IS NOT NULL THEN
      v_ext_db := p_desired_db::text;
    ELSIF use_explicit THEN
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
      -- Oracle/OLR CDB/PDB: srcdb may be "CDB/PDB" but Debezium emits events
      -- keyed on the CDB name only (database.dbname); strip the PDB suffix so
      -- ext_tbname matches the Debezium event topic.
      IF position('/' IN v_srcdb) > 0 THEN
        v_ext_db := split_part(v_srcdb, '/', 2);
      ELSE
        v_ext_db := v_srcdb;
      END IF;
    END IF;

    ------------------------------------------------------------------
    -- Build fully-qualified external table name: db.schema.table
    -- Preserve Oracle owner/table case as reported in metadata.
    ------------------------------------------------------------------
    v_tbl_name_l := r.table_name;

    -- Planning must not publish bindings to temporary foreign table OIDs.
    IF p_record_attributes THEN
    -- Refresh synchdb_attribute rows for this table
	IF v_conn_type IN ('oracle','olr','postgres','sqlserver') THEN
	   DELETE FROM public.synchdb_attribute
		 WHERE name       = p_connector_name
		   AND type       = v_conn_type
		   AND ext_tbname = format('%s.%s.%s',
								   v_ext_db,
								   r.ora_owner,
								   v_tbl_name_l) COLLATE pg_catalog."C";

		EXECUTE format($ins$
		  INSERT INTO public.synchdb_attribute
			  (name, type, attrelid, attnum, ext_tbname,    ext_attname,           ext_atttypename)
		  SELECT
			  %L::name,
			  %L::name,
			  %s::oid,
  			  row_number() OVER (ORDER BY c.position)::smallint,
			  %L::text,
			  c.column_name::text,
			  lower(c.type_name)::text
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
												  v_tbl_name_l) COLLATE pg_catalog."C"),
					 r.ora_owner, r.table_name, v_relid::text,
					 format('%s.%s.%s', v_ext_db, r.ora_owner, v_tbl_name_l);
					 
	 ELSIF v_conn_type = 'mysql' THEN
	 
		 DELETE FROM public.synchdb_attribute
		 WHERE name       = p_connector_name
		   AND type       = v_conn_type
		   AND ext_tbname = format('%s.%s',
								   v_ext_db,
								   v_tbl_name_l) COLLATE pg_catalog."C";

		EXECUTE format($ins$
		  INSERT INTO public.synchdb_attribute
			  (name, type, attrelid, attnum, ext_tbname,    ext_attname,           ext_atttypename)
		  SELECT
			  %L::name,
			  %L::name,
			  %s::oid,
			  c.position::smallint,
			  %L::text,
			  c.column_name::text,
			  lower(c.type_name)::text
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
												  v_tbl_name_l) COLLATE pg_catalog."C"),
					 r.ora_owner, r.table_name, v_relid::text,
					 format('%s.%s', v_ext_db, v_tbl_name_l);
					 
	 ELSE
		RAISE EXCEPTION 'Unsupported connector type: %', v_conn_type;
	END IF;
    ------------------------------------------------------------------
    END IF; -- p_record_attributes

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
              r.ora_owner,    -- using origin name instead of transformed name
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
          'databaseName', r.ora_owner,
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
    IF p_frozen_tables IS NOT NULL THEN RAISE; END IF;
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

	IF v_conn_type IN ('oracle','olr','postgres','sqlserver') THEN
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

	IF v_conn_type IN ('oracle','olr','postgres','sqlserver') THEN
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

COMMENT ON FUNCTION synchdb_create_stage_fts_plan(text, text, text, name, name, boolean, text, text, name, text, boolean, text, jsonb, boolean) IS
   'create a staging schema, migrate table schemas with translated data types';

-- Legacy callers retain their explicit list/configuration behavior.  The new
-- coordinator never rediscovers a frozen scope or writes temporary bindings.
CREATE OR REPLACE FUNCTION synchdb_create_stage_fts(
    p_connector_name        name,                         -- connector name for attribute rows
    p_desired_db            name,                         -- db token to match entries in snapshottable
    p_desired_schema        name,                         -- schema to match when entry includes schema
    p_stage_schema          name DEFAULT 'ora_stage'::name, -- target schema in Postgres
    p_server_name           name DEFAULT 'oracle'::name,  -- oracle_fdw server name
    p_lower_names           boolean DEFAULT true,         -- legacy: lower-case PG table/column names
    p_on_exists             text DEFAULT 'error',         -- 'error' | 'replace' | 'drop' | 'skip'
    p_offset                text DEFAULT NULL,            -- offset like scn, binlog pos..etc
    p_source_schema         name DEFAULT 'ora_obj'::name, -- metadata source schema (…columns)
    p_snapshot_tables       text DEFAULT NULL,            -- CSV of db.schema.table; when set, bypass conninfo
    p_write_dbz_schema_info boolean DEFAULT false,        -- when true, store Debezium schema-history JSON
    p_case_strategy         text DEFAULT 'asis'           -- 'upper' | 'lower' | 'asis' (table/column casing)
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM synchdb_create_stage_fts_plan(p_connector_name::text,p_desired_db::text,p_desired_schema::text,
        p_stage_schema,p_server_name,p_lower_names,p_on_exists,p_offset,p_source_schema,
        p_snapshot_tables,p_write_dbz_schema_info,p_case_strategy,NULL::jsonb,true);
END;
$$;
