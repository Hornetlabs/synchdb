-- Keep view replacement separate from the catalog/C API creation script so an
-- extension upgrade can update these definitions without replaying core.sql.
DO $synchdb_catalog_views$
DECLARE
    v_old_path pg_catalog.text := pg_catalog.current_setting('search_path');
    v_extension pg_catalog.oid;
    v_schema_oid pg_catalog.oid;
    v_schema pg_catalog.name;
    v_definition pg_catalog.text;
    v_view pg_catalog.oid;
    v_owner pg_catalog.oid;
    v_options pg_catalog.text;
    v_projection pg_catalog.text;
    v_source_type pg_catalog.oid;
    v_count pg_catalog.int4;
    v_matched pg_catalog.int4;
    r record;
BEGIN
    PERFORM pg_catalog.set_config('search_path', 'pg_catalog', true);
    SELECT e.oid, e.extnamespace, n.nspname
      INTO STRICT v_extension, v_schema_oid, v_schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'synchdb';

    -- Both views use complete source text for lookups and ordering. Only the
    -- compatibility view's three displayed source fields retain the old NAME
    -- result type. Use synchdb_att_view_text to inspect complete source IDs.
    v_definition := pg_catalog.format($view$
      SELECT
          a.name,
          a.type,
          a.attnum,
          a.ext_tbname::pg_catalog.text COLLATE pg_catalog."C" AS ext_tbname,
          -- This display string does not parse or quote target identifiers.
          (SELECT n.nspname::pg_catalog.text
                      OPERATOR(pg_catalog.||) '.'::pg_catalog.text
                      OPERATOR(pg_catalog.||) c.relname::pg_catalog.text
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n
               ON c.relnamespace OPERATOR(pg_catalog.=) n.oid
            WHERE c.oid OPERATOR(pg_catalog.=) p.attrelid) AS pg_tbname,
          a.ext_attname::pg_catalog.text COLLATE pg_catalog."C" AS ext_attname,
          p.attname AS pg_attname,
          a.ext_atttypename::pg_catalog.text COLLATE pg_catalog."C" AS ext_atttypename,
          pg_catalog.format_type(p.atttypid, NULL) AS pg_atttypename,
          (SELECT m.dstobj
             FROM %1$I.synchdb_objmap m
            WHERE m.name OPERATOR(pg_catalog.=) a.name
              AND m.objtype OPERATOR(pg_catalog.=) 'transform'::pg_catalog.name
              AND m.enabled IS TRUE
              AND m.srcobj::pg_catalog.text COLLATE pg_catalog."C" OPERATOR(pg_catalog.=)
                  (a.ext_tbname::pg_catalog.text
                      OPERATOR(pg_catalog.||) '.'::pg_catalog.text
                      OPERATOR(pg_catalog.||) a.ext_attname::pg_catalog.text)
                      COLLATE pg_catalog."C") AS transform
      FROM %1$I.synchdb_attribute a
      LEFT JOIN pg_catalog.pg_attribute p
        ON a.attrelid OPERATOR(pg_catalog.=) p.attrelid
       AND a.attnum OPERATOR(pg_catalog.=) p.attnum
    $view$, v_schema);

    FOR r IN SELECT * FROM (VALUES
        ('synchdb_att_view'::pg_catalog.name, 'pg_catalog.name'::pg_catalog.regtype),
        ('synchdb_att_view_text'::pg_catalog.name, 'pg_catalog.text'::pg_catalog.regtype)
    ) views(view_name, source_type)
    LOOP
        v_source_type := r.source_type;
        SELECT c.oid, c.relowner INTO v_view, v_owner
          FROM pg_catalog.pg_class c
         WHERE c.relnamespace = v_schema_oid AND c.relname = r.view_name
           AND c.relkind = 'v';
        IF v_view IS NOT NULL THEN
            IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                            WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                              AND d.objid = v_view AND d.objsubid = 0
                              AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                              AND d.refobjid = v_extension AND d.deptype = 'e') THEN
                RAISE EXCEPTION 'Attribute view % must belong to the synchdb extension', r.view_name
                    USING ERRCODE = '55000';
            END IF;
            -- Acquire the view lock without recursively locking referenced
            -- system catalogs, and keep ownership/ACL/dependencies unchanged.
            EXECUTE pg_catalog.format('ALTER VIEW %I.%I OWNER TO %I',
                                      v_schema, r.view_name, pg_catalog.pg_get_userbyid(v_owner));
            SELECT count(*), count(*) FILTER (WHERE
                       a.attname = expected.column_name AND
                       a.atttypid = expected.column_type AND a.atttypmod = -1 AND
                       (a.attname NOT IN ('ext_tbname','ext_attname','ext_atttypename') OR
                        a.attcollation = 'pg_catalog."C"'::pg_catalog.regcollation))
              INTO v_count, v_matched
              FROM pg_catalog.pg_attribute a
              LEFT JOIN (VALUES
                  (1, 'name', 'pg_catalog.name'::pg_catalog.regtype),
                  (2, 'type', 'pg_catalog.name'::pg_catalog.regtype),
                  (3, 'attnum', 'pg_catalog.int2'::pg_catalog.regtype),
                  (4, 'ext_tbname', v_source_type),
                  (5, 'pg_tbname', 'pg_catalog.text'::pg_catalog.regtype),
                  (6, 'ext_attname', v_source_type),
                  (7, 'pg_attname', 'pg_catalog.name'::pg_catalog.regtype),
                  (8, 'ext_atttypename', v_source_type),
                  (9, 'pg_atttypename', 'pg_catalog.text'::pg_catalog.regtype),
                  (10, 'transform', 'pg_catalog.text'::pg_catalog.regtype)
              ) expected(column_number, column_name, column_type)
                ON expected.column_number = a.attnum
             WHERE a.attrelid = v_view AND a.attnum > 0 AND NOT a.attisdropped;
            IF v_count <> 10 OR v_matched <> 10 THEN
                RAISE EXCEPTION 'Unexpected attribute view % column layout', r.view_name
                    USING ERRCODE = '55000';
            END IF;
            SELECT CASE WHEN count(*) = 0 THEN '' ELSE
                       ' WITH (' || string_agg(pg_catalog.format('%I = %L', o.option_name, o.option_value), ', ') || ')'
                   END INTO v_options
              FROM pg_catalog.pg_class c
              CROSS JOIN LATERAL pg_catalog.pg_options_to_table(c.reloptions) o
             WHERE c.oid = v_view;
            -- CREATE OR REPLACE must match every old column's type and
            -- collation, including default-collated format_type/transform.
            SELECT string_agg(pg_catalog.format('raw_view.%I::%s%s AS %I',
                       a.attname, pg_catalog.format_type(a.atttypid,a.atttypmod),
                       CASE WHEN a.attcollation = 0 THEN '' ELSE
                           pg_catalog.format(' COLLATE %I.%I', n.nspname, col.collname) END,
                       a.attname), ', ' ORDER BY a.attnum)
              INTO v_projection
              FROM pg_catalog.pg_attribute a
              LEFT JOIN pg_catalog.pg_collation col ON col.oid = a.attcollation
              LEFT JOIN pg_catalog.pg_namespace n ON n.oid = col.collnamespace
             WHERE a.attrelid = v_view AND a.attnum > 0 AND NOT a.attisdropped;
        ELSE
            IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c
                        WHERE c.relnamespace = v_schema_oid AND c.relname = r.view_name) THEN
                RAISE EXCEPTION 'Attribute object % must be a view', r.view_name
                    USING ERRCODE = '42809';
            END IF;
            v_options := '';
            -- Spell out the same output collations as the existing-view
            -- projection above. NAME/catalog identifier expressions carry C;
            -- format_type() and mapping destination text carry the database
            -- default. This keeps fresh and baseline-upgraded definitions
            -- identical while the existing-view branch preserves custom ACLs,
            -- dependencies, OIDs and any supported preexisting collations.
            v_projection := pg_catalog.format(
                'raw_view.name::pg_catalog.name COLLATE pg_catalog."C" AS name, '
                'raw_view.type::pg_catalog.name COLLATE pg_catalog."C" AS type, '
                'raw_view.attnum::pg_catalog.int2 AS attnum, '
                'raw_view.ext_tbname::%1$s COLLATE pg_catalog."C" AS ext_tbname, '
                'raw_view.pg_tbname::pg_catalog.text COLLATE pg_catalog."C" AS pg_tbname, '
                'raw_view.ext_attname::%1$s COLLATE pg_catalog."C" AS ext_attname, '
                'raw_view.pg_attname::pg_catalog.name COLLATE pg_catalog."C" AS pg_attname, '
                'raw_view.ext_atttypename::%1$s COLLATE pg_catalog."C" AS ext_atttypename, '
                'raw_view.pg_atttypename::pg_catalog.text COLLATE pg_catalog."default" AS pg_atttypename, '
                'raw_view.transform::pg_catalog.text COLLATE pg_catalog."default" AS transform',
                pg_catalog.format_type(v_source_type, -1));
        END IF;
        EXECUTE pg_catalog.format(
            'CREATE OR REPLACE VIEW %I.%I%s AS SELECT %s FROM (%s) AS raw_view '
            'ORDER BY (raw_view.name, raw_view.type, raw_view.ext_tbname COLLATE pg_catalog."C", raw_view.attnum)',
            v_schema, r.view_name, v_options, v_projection, v_definition);
    END LOOP;
    PERFORM pg_catalog.set_config('search_path', v_old_path, true);
END;
$synchdb_catalog_views$;
