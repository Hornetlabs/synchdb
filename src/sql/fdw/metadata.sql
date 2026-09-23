CREATE OR REPLACE FUNCTION synchdb_materialize_metadata(
    p_source_schema name,          -- e.g. 'ora_obj' (must exist; contains FTs)
    p_dest_schema   name,          -- destination schema to hold local materialized copies
    p_on_exists     text DEFAULT 'replace'  -- 'replace' | 'skip' | 'error'
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rname  text;
    rels   text[] := ARRAY['tables','columns','keys','columns_resolved'];  -- materialize just these
    src_ok boolean;
    dst_ok boolean;
    rel_exists boolean;
BEGIN
    -- 1) Validate source schema exists
    SELECT EXISTS (
        SELECT 1 FROM pg_namespace WHERE nspname = p_source_schema::text
    ) INTO src_ok;

    IF NOT src_ok THEN
        RAISE EXCEPTION 'Source schema % does not exist', p_source_schema;
    END IF;

    -- 2) Ensure destination schema exists (create if needed)
    SELECT EXISTS (
        SELECT 1 FROM pg_namespace WHERE nspname = p_dest_schema::text
    ) INTO dst_ok;

    IF NOT dst_ok THEN
        EXECUTE format('CREATE SCHEMA %I', p_dest_schema);
    END IF;

    -- 3) Loop through the three metadata relations
    FOREACH rname IN ARRAY rels LOOP
        -- 3a) Make sure the source relation exists (as a table/foreign table/view)
        SELECT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = p_source_schema::text
              AND c.relname = rname
              AND c.relkind IN ('r','v','f','m')  -- table, view, foreign table, matview
        ) INTO rel_exists;

        IF NOT rel_exists THEN
            RAISE NOTICE 'Skipping %.% (not found in source)', p_source_schema, rname;
            CONTINUE;
        END IF;

        -- 3b) Handle destination existence policy
        SELECT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = p_dest_schema::text
              AND c.relname = rname
              AND c.relkind IN ('r','m','f','v')
        ) INTO rel_exists;

        IF rel_exists THEN
            IF p_on_exists = 'replace' THEN
                EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', p_dest_schema, rname);
                -- If it was a view/FT/mview, DROP TABLE IF EXISTS won't catch; drop any object kind:
                BEGIN
                    EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', p_dest_schema, rname);
                EXCEPTION WHEN undefined_table THEN END;
                BEGIN
                    EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %I.%I CASCADE', p_dest_schema, rname);
                EXCEPTION WHEN undefined_table THEN END;
                BEGIN
                    EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.%I CASCADE', p_dest_schema, rname);
                EXCEPTION WHEN undefined_table THEN END;
            ELSIF p_on_exists = 'skip' THEN
                RAISE NOTICE 'Skipping %.% (already exists and on_exists=skip)', p_dest_schema, rname;
                CONTINUE;
            ELSE
                RAISE EXCEPTION 'Destination %.% already exists (on_exists=%)', p_dest_schema, rname, p_on_exists;
            END IF;
        END IF;

        -- 3c) Materialize as UNLOGGED table (structure+data)
        EXECUTE format(
            'CREATE UNLOGGED TABLE %I.%I AS SELECT * FROM %I.%I',
            p_dest_schema, rname, p_source_schema, rname
        );

        RAISE NOTICE 'Materialized %.% -> %.% (UNLOGGED)', p_source_schema, rname, p_dest_schema, rname;

        -- 3d) Add helpful indexes if the expected columns exist
        -- For tables(schema, table_name)
        IF rname = 'tables' THEN
            PERFORM 1 FROM information_schema.columns
             WHERE table_schema = p_dest_schema::text
               AND table_name   = 'tables'
               AND column_name  = 'schema';
            IF FOUND THEN
                PERFORM 1 FROM information_schema.columns
                 WHERE table_schema = p_dest_schema::text
                   AND table_name   = 'tables'
                   AND column_name  = 'table_name';
                IF FOUND THEN
                    EXECUTE format(
                        'CREATE INDEX ON %I.%I(("schema"), table_name)',
                        p_dest_schema, rname
                    );
                END IF;
            END IF;
        END IF;

        -- For columns(schema, table_name, position)
        IF rname = 'columns' THEN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = p_dest_schema::text AND table_name='columns'
                  AND column_name IN ('schema','table_name','position')
                GROUP BY table_schema, table_name
                HAVING count(*) = 3
            ) THEN
                EXECUTE format(
                    'CREATE INDEX ON %I.%I(("schema"), table_name, position)',
                    p_dest_schema, rname
                );
            END IF;
        END IF;

		IF rname = 'columns_resolved' THEN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = p_dest_schema::text AND table_name='columns_resolved'
                  AND column_name IN ('schema','table_name','position')
                GROUP BY table_schema, table_name
                HAVING count(*) = 3
            ) THEN
                EXECUTE format(
                    'CREATE INDEX ON %I.%I(("schema"), table_name, position)',
                    p_dest_schema, rname
                );
            END IF;
        END IF;

        -- For keys(schema, table_name[, constraint_name])
        IF rname = 'keys' THEN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = p_dest_schema::text AND table_name='keys'
                  AND column_name IN ('schema','table_name')
                GROUP BY table_schema, table_name
                HAVING count(*) = 2
            ) THEN
                EXECUTE format(
                    'CREATE INDEX ON %I.%I(("schema"), table_name)',
                    p_dest_schema, rname
                );
            END IF;

            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = p_dest_schema::text AND table_name='keys'
                  AND column_name = 'constraint_name'
            ) THEN
                EXECUTE format(
                    'CREATE INDEX ON %I.%I((constraint_name))',
                    p_dest_schema, rname
                );
            END IF;
        END IF;

		IF rname = 'columns_resolved' THEN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = p_dest_schema::text AND table_name='columns_resolved'
                  AND column_name IN ('schema','table_name','position')
                GROUP BY table_schema, table_name
                HAVING count(*) = 3
            ) THEN
                EXECUTE format('CREATE INDEX ON %I.columns_resolved(("schema"), table_name, position)', p_dest_schema);
            END IF;
        END IF;

    END LOOP;

    -- 4) Analyze for better local planning
    EXECUTE format('ANALYZE %I.tables',  p_dest_schema);
    EXECUTE format('ANALYZE %I.columns', p_dest_schema);
    EXECUTE format('ANALYZE %I.keys',    p_dest_schema);

	IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname = p_dest_schema::text AND c.relname='columns_resolved' AND c.relkind='r'
    ) THEN
        EXECUTE format('ANALYZE %I.columns_resolved', p_dest_schema);
    END IF;

    RAISE NOTICE 'Materialization complete into schema %', p_dest_schema;
END;
$$;

COMMENT ON FUNCTION synchdb_materialize_metadata(name, name, text) IS
   'create a metadata schema with selected ora_obj foreign tables materialized';

