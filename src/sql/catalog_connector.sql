-- Persistent connector incarnation and configuration content version.
-- Keep the original three columns and their positions: legacy INSERT ... VALUES
-- (name,isactive,data) continues to default the two appended managed columns.
DO $synchdb_catalog_connector$
DECLARE
    v_old_path pg_catalog.text := pg_catalog.current_setting('search_path');
    v_extension pg_catalog.oid;
    v_schema_oid pg_catalog.oid;
    v_schema pg_catalog.name;
    v_relation pg_catalog.oid;
    v_function pg_catalog.oid;
    v_id_attnum pg_catalog.int2;
    v_count pg_catalog.int4;
    v_default pg_catalog.text;
    v_normalized pg_catalog.text;
    v_expression pg_catalog.text;
    v_column record;
    v_existing record;
    v_bound record;
BEGIN
    PERFORM pg_catalog.set_config('search_path', 'pg_catalog', true);
    SELECT e.oid, e.extnamespace, n.nspname
      INTO STRICT v_extension, v_schema_oid, v_schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'synchdb';
    SELECT c.oid INTO STRICT v_relation
      FROM pg_catalog.pg_class c
     WHERE c.relnamespace = v_schema_oid AND c.relname = 'synchdb_conninfo'
       AND c.relkind = 'r';
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                    WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                      AND d.objid = v_relation AND d.objsubid = 0
                      AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                      AND d.refobjid = v_extension AND d.deptype = 'e') THEN
        RAISE EXCEPTION 'Connector catalog must belong to the synchdb extension'
            USING ERRCODE = '55000';
    END IF;
    EXECUTE pg_catalog.format('LOCK TABLE ONLY %I.synchdb_conninfo IN ACCESS EXCLUSIVE MODE', v_schema);
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
                    WHERE c.oid = v_relation AND c.relnamespace = v_schema_oid
                      AND c.relname = 'synchdb_conninfo' AND c.relkind = 'r') THEN
        RAISE EXCEPTION 'Connector catalog changed during identity migration'
            USING ERRCODE = '55000';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
                WHERE i.inhrelid = v_relation OR i.inhparent = v_relation) THEN
        RAISE EXCEPTION 'Connector identity migration does not support inherited catalogs'
            USING ERRCODE = '55000';
    END IF;

    SELECT count(*) INTO v_count
      FROM pg_catalog.pg_attribute a
      JOIN (VALUES (1, 'name', 'pg_catalog.text'::pg_catalog.regtype),
                   (2, 'isactive', 'pg_catalog.bool'::pg_catalog.regtype),
                   (3, 'data', 'pg_catalog.jsonb'::pg_catalog.regtype))
           expected(column_number, column_name, column_type)
        ON a.attnum = expected.column_number AND a.attname = expected.column_name
       AND a.atttypid = expected.column_type AND a.atttypmod = -1
     WHERE a.attrelid = v_relation AND NOT a.attisdropped;
    IF v_count <> 3 THEN
        RAISE EXCEPTION 'Unexpected connector catalog column layout'
            USING ERRCODE = '55000';
    END IF;

    FOR v_column IN SELECT * FROM (VALUES
        ('connector_id', 'pg_catalog.uuid'::pg_catalog.regtype, 'pg_catalog.gen_random_uuid()'),
        ('config_revision', 'pg_catalog.int8'::pg_catalog.regtype, '1')
    ) columns(column_name, column_type, default_expression)
    LOOP
        SELECT a.* INTO v_existing FROM pg_catalog.pg_attribute a
         WHERE a.attrelid = v_relation AND a.attname = v_column.column_name
           AND a.attnum > 0 AND NOT a.attisdropped;
        IF NOT FOUND THEN
            -- The volatile UUID default assigns every existing row its own ID
            -- exactly once. The transaction's table lock also protects backfill.
            EXECUTE pg_catalog.format('ALTER TABLE %I.synchdb_conninfo ADD COLUMN %I %s DEFAULT %s',
                v_schema, v_column.column_name, pg_catalog.format_type(v_column.column_type, -1),
                v_column.default_expression);
        ELSIF v_existing.atttypid <> v_column.column_type OR v_existing.atttypmod <> -1 OR
              v_existing.attidentity <> '' OR v_existing.attgenerated <> '' THEN
            RAISE EXCEPTION 'Unexpected connector metadata column %', v_column.column_name
                USING ERRCODE = '55000';
        END IF;

        SELECT pg_catalog.pg_get_expr(d.adbin,d.adrelid) INTO v_default
          FROM pg_catalog.pg_attrdef d
          JOIN pg_catalog.pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
         WHERE a.attrelid = v_relation AND a.attname = v_column.column_name;
        v_normalized := pg_catalog.regexp_replace(v_default, '[[:space:]()]', '', 'g');
        IF v_column.column_name = 'connector_id' THEN
            IF v_normalized IS DISTINCT FROM 'gen_random_uuid' THEN
                RAISE EXCEPTION 'Unexpected connector_id default' USING ERRCODE = '55000';
            END IF;
        ELSIF v_normalized IS NULL OR v_normalized NOT IN
              ('1', '1::bigint', '''1''::bigint', '1::pg_catalog.int8', '''1''::pg_catalog.int8') THEN
            RAISE EXCEPTION 'Unexpected config_revision default' USING ERRCODE = '55000';
        END IF;
        -- Do not generate replacements for existing NULL/invalid identities.
        -- An inconsistent pre-existing catalog must fail the migration.
        EXECUTE pg_catalog.format('ALTER TABLE %I.synchdb_conninfo ALTER COLUMN %I SET NOT NULL',
                                  v_schema, v_column.column_name);
    END LOOP;

    SELECT a.attnum INTO STRICT v_id_attnum FROM pg_catalog.pg_attribute a
     WHERE a.attrelid = v_relation AND a.attname = 'connector_id' AND NOT a.attisdropped;
    SELECT c.* INTO v_existing FROM pg_catalog.pg_constraint c
     WHERE c.conrelid = v_relation AND c.conname = 'synchdb_conninfo_connector_id_key';
    IF NOT FOUND THEN
        EXECUTE pg_catalog.format('ALTER TABLE %I.synchdb_conninfo '
            'ADD CONSTRAINT synchdb_conninfo_connector_id_key UNIQUE (connector_id)', v_schema);
    ELSIF v_existing.contype <> 'u' OR v_existing.conkey <> ARRAY[v_id_attnum] OR
          v_existing.condeferrable OR v_existing.condeferred OR NOT v_existing.convalidated THEN
        RAISE EXCEPTION 'Unexpected connector identity uniqueness constraint'
            USING ERRCODE = '55000';
    END IF;

    FOR v_bound IN SELECT * FROM (VALUES
        ('synchdb_conninfo_connector_id_nonzero_check',
         'connector_id <> ''00000000-0000-0000-0000-000000000000''::uuid'),
        ('synchdb_conninfo_config_revision_positive_check', 'config_revision > 0')
    ) bounds(constraint_name, expression)
    LOOP
        SELECT c.* INTO v_existing FROM pg_catalog.pg_constraint c
         WHERE c.conrelid = v_relation AND c.conname = v_bound.constraint_name;
        IF NOT FOUND THEN
            EXECUTE pg_catalog.format('ALTER TABLE %I.synchdb_conninfo ADD CONSTRAINT %I CHECK (%s)',
                                      v_schema, v_bound.constraint_name, v_bound.expression);
        ELSE
            IF v_existing.contype <> 'c' OR v_existing.connoinherit THEN
                RAISE EXCEPTION 'Unexpected connector metadata constraint %', v_bound.constraint_name
                    USING ERRCODE = '55000';
            END IF;
            SELECT pg_catalog.pg_get_expr(c.conbin,c.conrelid) INTO STRICT v_expression
              FROM pg_catalog.pg_constraint c WHERE c.oid = v_existing.oid;
            IF pg_catalog.regexp_replace(v_expression, '[[:space:]()]', '', 'g') <>
               pg_catalog.regexp_replace(v_bound.expression, '[[:space:]()]', '', 'g') THEN
                RAISE EXCEPTION 'Unexpected connector metadata constraint %', v_bound.constraint_name
                    USING ERRCODE = '55000';
            END IF;
            EXECUTE pg_catalog.format('ALTER TABLE %I.synchdb_conninfo VALIDATE CONSTRAINT %I',
                                      v_schema, v_bound.constraint_name);
        END IF;
    END LOOP;

    IF EXISTS (SELECT 1 FROM pg_catalog.pg_proc p
                WHERE p.pronamespace = v_schema_oid AND p.proname = 'synchdb_conninfo_guard'
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                                   WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                                     AND d.objid = p.oid AND d.objsubid = 0
                                     AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                                     AND d.refobjid = v_extension AND d.deptype = 'e')) THEN
        RAISE EXCEPTION 'Connector guard function must belong to the synchdb extension'
            USING ERRCODE = '55000';
    END IF;
    SELECT p.oid INTO v_function FROM pg_catalog.pg_proc p
     WHERE p.pronamespace = v_schema_oid AND p.proname = 'synchdb_conninfo_guard' AND p.pronargs = 0;
    IF v_function IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_proc p
        WHERE p.oid = v_function AND p.prokind = 'f' AND p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype
          AND NOT p.proretset) THEN
        RAISE EXCEPTION 'Unexpected connector guard function signature' USING ERRCODE = '55000';
    END IF;

    EXECUTE pg_catalog.format($function$
        CREATE OR REPLACE FUNCTION %I.synchdb_conninfo_guard() RETURNS pg_catalog.trigger
        LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog
        AS $guard$
        BEGIN
            IF TG_LEVEL <> 'ROW' OR TG_WHEN <> 'BEFORE' OR TG_OP NOT IN ('INSERT','UPDATE') THEN
                RAISE EXCEPTION 'Connector guard requires a BEFORE INSERT or UPDATE row trigger'
                    USING ERRCODE = '0A000';
            END IF;
            IF TG_OP = 'UPDATE' THEN
                IF NEW.connector_id IS DISTINCT FROM OLD.connector_id THEN
                    RAISE EXCEPTION 'connector_id cannot be changed'
                        USING ERRCODE = '22023';
                END IF;
                IF NEW.config_revision IS DISTINCT FROM OLD.config_revision THEN
                    RAISE EXCEPTION 'config_revision is managed by the connector catalog'
                        USING ERRCODE = '22023';
                END IF;
                IF NEW.name COLLATE pg_catalog."C" IS DISTINCT FROM OLD.name COLLATE pg_catalog."C" THEN
                    RAISE EXCEPTION 'Renaming a connector is not supported'
                        USING ERRCODE = '0A000',
                              HINT = 'Create a new connector instead.';
                END IF;
                -- Serialized JSONB is canonical for object key order, but
                -- preserves numeric display scales used by Store text decoding.
                IF NEW.data::pg_catalog.text COLLATE pg_catalog."C" IS DISTINCT FROM
                   OLD.data::pg_catalog.text COLLATE pg_catalog."C" THEN
                    NEW.config_revision := OLD.config_revision + 1;
                END IF;
            END IF;
            -- INSERT defaults allocate a new identity/revision. Valid explicit
            -- values are retained for logical restoration/import; using an old
            -- ID is an explicit restoration of that logical identity, not a
            -- normal new connector. Offline artifact restore belongs to M8.
            RETURN NEW;
        END;
        $guard$
    $function$, v_schema);
    SELECT p.oid INTO STRICT v_function FROM pg_catalog.pg_proc p
     WHERE p.pronamespace = v_schema_oid AND p.proname = 'synchdb_conninfo_guard' AND p.pronargs = 0;

    SELECT t.* INTO v_existing FROM pg_catalog.pg_trigger t
     WHERE t.tgrelid = v_relation AND t.tgname = 'synchdb_conninfo_guard';
    IF FOUND THEN
        -- PostgreSQL triggers have AUTO dependency on their table and NORMAL
        -- dependency on their function, not extension membership of their own.
        -- Both referenced objects were verified as extension members above.
        IF v_existing.tgfoid <> v_function OR v_existing.tgtype <> 23 OR
           v_existing.tgisinternal OR v_existing.tgconstraint <> 0 OR
           v_existing.tgdeferrable OR v_existing.tginitdeferred OR
           v_existing.tgnargs <> 0 OR v_existing.tgattr::pg_catalog.text <> '' OR
           v_existing.tgqual IS NOT NULL OR v_existing.tgparentid <> 0 OR
           v_existing.tgoldtable IS NOT NULL OR v_existing.tgnewtable IS NOT NULL THEN
            RAISE EXCEPTION 'Unexpected connector guard trigger definition'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    EXECUTE pg_catalog.format('CREATE OR REPLACE TRIGGER synchdb_conninfo_guard '
        'BEFORE INSERT OR UPDATE ON %I.synchdb_conninfo FOR EACH ROW '
        'EXECUTE FUNCTION %I.synchdb_conninfo_guard()', v_schema, v_schema);
    EXECUTE pg_catalog.format('ALTER TABLE %I.synchdb_conninfo ENABLE ALWAYS TRIGGER synchdb_conninfo_guard',
                              v_schema);
    PERFORM pg_catalog.set_config('search_path', v_old_path, true);
END;
$synchdb_catalog_connector$;
