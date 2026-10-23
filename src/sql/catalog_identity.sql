-- Fresh installation and upgrade share the same bounded, byte-exact source
-- identity contract.  Previously truncated NAME values cannot be reconstructed.
DO $synchdb_catalog_identity$
DECLARE
    v_old_path pg_catalog.text := pg_catalog.current_setting('search_path');
    v_extension pg_catalog.oid;
    v_schema_oid pg_catalog.oid;
    v_schema pg_catalog.name;
    v_attributes pg_catalog.oid;
    v_mappings pg_catalog.oid;
    v_view pg_catalog.oid;
    v_view_owner pg_catalog.oid;
    v_view_rule pg_catalog.oid;
    v_view_options pg_catalog.text;
    v_old_definition pg_catalog.text;
    v_empty_projection pg_catalog.text;
    v_restore_projection pg_catalog.text;
    v_needs_alter pg_catalog.bool := false;
    v_detail pg_catalog.text;
    v_count pg_catalog.int4;
    v_constraint pg_catalog.oid;
    v_expr pg_catalog.text;
    r record;
BEGIN
    -- Do not let caller-controlled functions, operators or types participate
    -- in catalog DDL; restore the caller's setting before leaving the block.
    PERFORM pg_catalog.set_config('search_path', 'pg_catalog', true);
    SELECT e.oid, e.extnamespace, n.nspname
      INTO STRICT v_extension, v_schema_oid, v_schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'synchdb';

    SELECT c.oid INTO STRICT v_attributes FROM pg_catalog.pg_class c
     WHERE c.relnamespace = v_schema_oid AND c.relname = 'synchdb_attribute'
       AND c.relkind = 'r';
    SELECT c.oid INTO STRICT v_mappings FROM pg_catalog.pg_class c
     WHERE c.relnamespace = v_schema_oid AND c.relname = 'synchdb_objmap'
       AND c.relkind = 'r';
    IF (SELECT count(*) FROM pg_catalog.pg_depend d
         WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
           AND d.objid IN (v_attributes, v_mappings) AND d.objsubid = 0
           AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
           AND d.refobjid = v_extension AND d.deptype = 'e') <> 2 THEN
        RAISE EXCEPTION 'Source identity catalogs must belong to the synchdb extension'
            USING ERRCODE = '55000';
    END IF;

    SELECT c.oid, c.relowner INTO v_view, v_view_owner
      FROM pg_catalog.pg_class c
     WHERE c.relnamespace = v_schema_oid AND c.relname = 'synchdb_att_view'
       AND c.relkind = 'v';
    IF v_view IS NOT NULL THEN
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                        WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                          AND d.objid = v_view AND d.objsubid = 0
                          AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                          AND d.refobjid = v_extension AND d.deptype = 'e') THEN
            RAISE EXCEPTION 'The attribute compatibility view must belong to the synchdb extension'
                USING ERRCODE = '55000';
        END IF;
        -- LOCK TABLE on a view recursively locks every referenced catalog.
        -- A no-op owner operation locks only the view, before its base tables,
        -- while preserving its ownership, ACL, options and dependent views.
        EXECUTE pg_catalog.format('ALTER VIEW %I.synchdb_att_view OWNER TO %I',
                                  v_schema, pg_catalog.pg_get_userbyid(v_view_owner));
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
                        WHERE c.oid = v_view AND c.relnamespace = v_schema_oid
                          AND c.relname = 'synchdb_att_view' AND c.relkind = 'v'
                          AND c.relowner = v_view_owner) THEN
            RAISE EXCEPTION 'Attribute compatibility view changed during identity migration'
                USING ERRCODE = '55000';
        END IF;
    ELSIF EXISTS (SELECT 1 FROM pg_catalog.pg_class c
                   WHERE c.relnamespace = v_schema_oid AND c.relname = 'synchdb_att_view') THEN
        RAISE EXCEPTION 'synchdb_att_view must be a view' USING ERRCODE = '42809';
    END IF;

    EXECUTE pg_catalog.format('LOCK TABLE ONLY %I.synchdb_attribute, ONLY %I.synchdb_objmap IN ACCESS EXCLUSIVE MODE',
                              v_schema, v_schema);
    -- Exactly four known columns may change; unexpected catalog layouts fail
    -- before the compatibility definition or any stored values are changed.
    SELECT count(*) INTO v_count
      FROM pg_catalog.pg_attribute a
      JOIN (VALUES (v_attributes, 'ext_tbname'), (v_attributes, 'ext_attname'),
                   (v_attributes, 'ext_atttypename'), (v_mappings, 'srcobj'))
           expected(relid, column_name)
        ON a.attrelid = expected.relid AND a.attname = expected.column_name
     WHERE a.attnum > 0 AND NOT a.attisdropped
       AND a.atttypid IN ('pg_catalog.name'::pg_catalog.regtype,
                         'pg_catalog.text'::pg_catalog.regtype)
       AND a.atttypmod = -1;
    IF v_count <> 4 THEN
        RAISE EXCEPTION 'Unexpected source identity catalog column layout'
            USING ERRCODE = '55000';
    END IF;
    SELECT bool_or(a.atttypid <> 'pg_catalog.text'::pg_catalog.regtype
                   OR a.attcollation <> 'pg_catalog."C"'::pg_catalog.regcollation)
      INTO v_needs_alter
      FROM pg_catalog.pg_attribute a
     WHERE (a.attrelid = v_attributes AND a.attname IN ('ext_tbname','ext_attname','ext_atttypename'))
        OR (a.attrelid = v_mappings AND a.attname = 'srcobj');

    IF v_needs_alter THEN
        IF v_view IS NOT NULL THEN
            SELECT rw.oid INTO STRICT v_view_rule FROM pg_catalog.pg_rewrite rw
             WHERE rw.ev_class = v_view AND rw.rulename = '_RETURN';
        END IF;
        -- The built-in primary key may be rebuilt automatically; ordinary
        -- column NOT NULL constraints (cataloged separately since PG18) stay
        -- in place. Other directly dependent objects need installer review;
        -- never remove or rewrite an external view, function, policy or index.
        SELECT string_agg(DISTINCT pg_catalog.format('%s depends on %s',
                   pg_catalog.pg_describe_object(d.classid,d.objid,d.objsubid),
                   pg_catalog.pg_describe_object(d.refclassid,d.refobjid,d.refobjsubid)), E'\n')
          INTO v_detail
          FROM pg_catalog.pg_depend d
          JOIN pg_catalog.pg_attribute a
            ON d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
           AND d.refobjid = a.attrelid AND d.refobjsubid = a.attnum
         WHERE ((a.attrelid = v_attributes AND a.attname IN ('ext_tbname','ext_attname','ext_atttypename'))
                OR (a.attrelid = v_mappings AND a.attname = 'srcobj'))
           AND NOT (d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass
                    AND d.objid IS NOT DISTINCT FROM v_view_rule)
           AND NOT (d.classid = 'pg_catalog.pg_constraint'::pg_catalog.regclass
                    AND EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c
                                 WHERE c.oid = d.objid AND c.conrelid = v_mappings AND c.contype = 'p'))
           AND NOT (d.classid = 'pg_catalog.pg_constraint'::pg_catalog.regclass
                    AND EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c
                                 WHERE c.oid = d.objid AND c.conrelid = a.attrelid
                                   AND c.contype = 'n' AND c.conkey = ARRAY[a.attnum]))
           AND NOT (d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                    AND EXISTS (SELECT 1 FROM pg_catalog.pg_index i
                                 WHERE i.indexrelid = d.objid AND i.indrelid = v_mappings AND i.indisprimary));
        IF v_detail IS NOT NULL THEN
            RAISE EXCEPTION 'Cannot migrate source identity columns with external dependencies'
                USING ERRCODE = '2BP01', DETAIL = v_detail,
                      HINT = 'Review and temporarily remove the listed dependencies, then retry the extension update. No objects were dropped.';
        END IF;

        IF v_view IS NOT NULL THEN
            -- Preserve all ten legacy column properties, including text
            -- collations inherited from concatenation and format_type().
            SELECT count(*) INTO v_count FROM pg_catalog.pg_attribute a
             WHERE a.attrelid = v_view AND a.attnum > 0 AND NOT a.attisdropped;
            IF v_count <> 10 THEN
                RAISE EXCEPTION 'Expected ten attribute compatibility view columns'
                    USING ERRCODE = '55000';
            END IF;
            SELECT pg_catalog.pg_get_viewdef(v_view, false) INTO v_old_definition;
            v_old_definition := pg_catalog.regexp_replace(v_old_definition, ';\s*$', '');
            SELECT CASE WHEN count(*) = 0 THEN '' ELSE
                       ' WITH (' || string_agg(pg_catalog.format('%I = %L', o.option_name, o.option_value), ', ') || ')'
                   END INTO v_view_options
              FROM pg_catalog.pg_class c
              CROSS JOIN LATERAL pg_catalog.pg_options_to_table(c.reloptions) o
             WHERE c.oid = v_view;
            SELECT string_agg(pg_catalog.format('NULL::%s%s AS %I',
                       pg_catalog.format_type(a.atttypid,a.atttypmod),
                       CASE WHEN a.attcollation = 0 THEN '' ELSE
                           pg_catalog.format(' COLLATE %I.%I', n.nspname, col.collname) END,
                       a.attname), ', ' ORDER BY a.attnum),
                   string_agg(pg_catalog.format('old_view.%I::%s%s AS %I', a.attname,
                       pg_catalog.format_type(a.atttypid,a.atttypmod),
                       CASE WHEN a.attcollation = 0 THEN '' ELSE
                           pg_catalog.format(' COLLATE %I.%I', n.nspname, col.collname) END,
                       a.attname), ', ' ORDER BY a.attnum)
              INTO v_empty_projection, v_restore_projection
              FROM pg_catalog.pg_attribute a
              LEFT JOIN pg_catalog.pg_collation col ON col.oid = a.attcollation
              LEFT JOIN pg_catalog.pg_namespace n ON n.oid = col.collnamespace
             WHERE a.attrelid = v_view AND a.attnum > 0 AND NOT a.attisdropped;
            EXECUTE pg_catalog.format('CREATE OR REPLACE VIEW %I.synchdb_att_view%s AS SELECT %s WHERE false',
                                      v_schema, v_view_options, v_empty_projection);
        END IF;

        EXECUTE pg_catalog.format('ALTER TABLE %I.synchdb_attribute '
            'ALTER COLUMN ext_tbname TYPE pg_catalog.text COLLATE pg_catalog."C" USING ext_tbname::pg_catalog.text, '
            'ALTER COLUMN ext_attname TYPE pg_catalog.text COLLATE pg_catalog."C" USING ext_attname::pg_catalog.text, '
            'ALTER COLUMN ext_atttypename TYPE pg_catalog.text COLLATE pg_catalog."C" USING ext_atttypename::pg_catalog.text', v_schema);
        EXECUTE pg_catalog.format('ALTER TABLE %I.synchdb_objmap '
            'ALTER COLUMN srcobj TYPE pg_catalog.text COLLATE pg_catalog."C" USING srcobj::pg_catalog.text', v_schema);

        IF v_view IS NOT NULL THEN
            -- This fragment is complete by itself: do not leave an empty
            -- compatibility view until the later final-definition fragment.
            EXECUTE pg_catalog.format('CREATE OR REPLACE VIEW %I.synchdb_att_view%s AS SELECT %s FROM (%s) AS old_view',
                                      v_schema, v_view_options, v_restore_projection, v_old_definition);
        END IF;
    END IF;

    -- 2400 matches SYNCHDB_SOURCE_ID_MAX_BYTES in the C catalog/lookup APIs.
    -- The source-column key includes the separator, not only each component.
    FOR r IN SELECT * FROM (VALUES
        (v_mappings, 'synchdb_objmap', 'synchdb_objmap_source_size_check', 'octet_length(srcobj) <= 2400'),
        (v_attributes, 'synchdb_attribute', 'synchdb_attribute_table_size_check', 'octet_length(ext_tbname) <= 2400'),
        (v_attributes, 'synchdb_attribute', 'synchdb_attribute_column_size_check', 'octet_length(ext_attname) <= 2400'),
        (v_attributes, 'synchdb_attribute', 'synchdb_attribute_type_size_check', 'octet_length(ext_atttypename) <= 2400'),
        (v_attributes, 'synchdb_attribute', 'synchdb_attribute_qualified_column_size_check',
         'ext_atttypename IS NULL OR octet_length(ext_tbname) + 1 + octet_length(ext_attname) <= 2400')
    ) bounds(relid, table_name, constraint_name, expression)
    LOOP
        SELECT c.oid, pg_catalog.pg_get_expr(c.conbin,c.conrelid)
          INTO v_constraint, v_expr FROM pg_catalog.pg_constraint c
         WHERE c.conrelid = r.relid AND c.conname = r.constraint_name AND c.contype = 'c';
        IF v_constraint IS NULL THEN
            EXECUTE pg_catalog.format('ALTER TABLE %I.%I ADD CONSTRAINT %I CHECK (%s)',
                                      v_schema, r.table_name, r.constraint_name, r.expression);
        ELSIF pg_catalog.regexp_replace(v_expr, '[[:space:]()]', '', 'g') <>
              pg_catalog.regexp_replace(r.expression, '[[:space:]()]', '', 'g') THEN
            RAISE EXCEPTION 'Unexpected source identity constraint %', r.constraint_name
                USING ERRCODE = '55000';
        ELSE
            EXECUTE pg_catalog.format('ALTER TABLE %I.%I VALIDATE CONSTRAINT %I',
                                      v_schema, r.table_name, r.constraint_name);
        END IF;
    END LOOP;

    -- The legacy NAME entry points retain their function OIDs and ACLs.
    -- New text overloads use separate C symbols with TEXT argument decoding.
    EXECUTE pg_catalog.format($api$
        CREATE OR REPLACE FUNCTION %I.synchdb_add_objmap(name,name,text,text) RETURNS int
        AS 'MODULE_PATHNAME', 'synchdb_add_objmap_text' LANGUAGE C VOLATILE STRICT
    $api$, v_schema);
    EXECUTE pg_catalog.format($api$
        CREATE OR REPLACE FUNCTION %I.synchdb_del_objmap(name,name,text) RETURNS int
        AS 'MODULE_PATHNAME', 'synchdb_del_objmap_text' LANGUAGE C VOLATILE STRICT
    $api$, v_schema);
    PERFORM pg_catalog.set_config('search_path', v_old_path, true);
END;
$synchdb_catalog_identity$;
