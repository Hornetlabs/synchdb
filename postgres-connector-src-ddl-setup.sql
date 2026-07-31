CREATE VIEW synchdb_wal_lsn AS
SELECT pg_current_wal_lsn()::pg_lsn AS wal_lsn;

CREATE OR REPLACE FUNCTION synchdb_ddl_log()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    cmd    record;
    dobj   record;

    v_relid        oid;
    v_schema_name  text;
    v_table_name   text;

    v_cols_json    jsonb;
    v_pk_cols_json jsonb;

    v_id           text;
BEGIN
    ------------------------------------------------------------------
    -- Optional: allow turning this off via GUC
    ------------------------------------------------------------------
    IF current_setting('synchdb.ddl_log_enabled', true) = 'off' THEN
        RETURN;
    END IF;

    ------------------------------------------------------------------
    -- CREATE / ALTER TABLE path (ddl_command_end)
    ------------------------------------------------------------------
    IF TG_EVENT = 'ddl_command_end' THEN

        FOR cmd IN
            SELECT
                classid,
                objid,
                objsubid,
                command_tag,
                object_type,
                schema_name,
                object_identity
            FROM pg_event_trigger_ddl_commands()
            WHERE object_type = 'table'
              AND command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'ALTER TABLE')
        LOOP
            v_relid := cmd.objid;

            IF v_relid IS NULL OR v_relid = 0 THEN
                CONTINUE;
            END IF;

            SELECT n.nspname, c.relname
              INTO v_schema_name, v_table_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.oid = v_relid;

            IF v_schema_name IS NULL OR v_table_name IS NULL THEN
                CONTINUE;
            END IF;

            -- id in form: current_database.schema.table
            v_id := current_database() || '.' || v_schema_name || '.' || v_table_name;

            ------------------------------------------------------------------
            -- Build columns JSON
            ------------------------------------------------------------------
            SELECT jsonb_agg(
                       jsonb_build_object(
                           'name',     a.attname,
                           'position', a.attnum,

                           -- typeName: strip everything from first '(' onward (inclusive)
                           'typeName',
                               regexp_replace(
                                   format_type(a.atttypid, a.atttypmod),
                                   '\(.*$',
                                   ''
                               ),

                           -- length rule:
                           -- length := char_max_length
                           -- if length is NULL and precision >= 0, length := precision
                           'length',
                               CASE
                                   WHEN information_schema._pg_char_max_length(a.atttypid, a.atttypmod) IS NOT NULL
                                   THEN information_schema._pg_char_max_length(a.atttypid, a.atttypmod)

                                   WHEN information_schema._pg_char_max_length(a.atttypid, a.atttypmod) IS NULL
                                        AND information_schema._pg_numeric_precision(a.atttypid, a.atttypmod) IS NOT NULL
                                        AND information_schema._pg_numeric_precision(a.atttypid, a.atttypmod) >= 0
                                   THEN information_schema._pg_numeric_precision(a.atttypid, a.atttypmod)

                                   ELSE NULL
                               END,

                           'precision', information_schema._pg_numeric_precision(a.atttypid, a.atttypmod),
                           'scale',     information_schema._pg_numeric_scale(a.atttypid, a.atttypmod),
                           'optional',  NOT a.attnotnull,

                           -- defaultValueExpression: strip trailing "::type"
                           'defaultValueExpression',
                               CASE
                                   WHEN ad.adbin IS NULL THEN NULL
                                   ELSE regexp_replace(
                                            pg_get_expr(ad.adbin, ad.adrelid),
                                            '::[a-zA-Z_][a-zA-Z0-9_\. ]*(\[\])?$',
                                            ''
                                        )
                               END
                       )
                       ORDER BY a.attnum
                   )
            INTO v_cols_json
            FROM pg_attribute a
            LEFT JOIN pg_attrdef ad
              ON ad.adrelid = a.attrelid
             AND ad.adnum   = a.attnum
            WHERE a.attrelid = v_relid
              AND a.attnum > 0
              AND NOT a.attisdropped;

            IF v_cols_json IS NULL THEN
                v_cols_json := '[]'::jsonb;
            END IF;

            ------------------------------------------------------------------
            -- Build primary key column list
            ------------------------------------------------------------------
            SELECT COALESCE(
                     jsonb_agg(colname ORDER BY ord),
                     '[]'::jsonb
                   )
            INTO v_pk_cols_json
            FROM (
                SELECT a.attname AS colname, key_ordinal AS ord
                FROM pg_index i
                JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, key_ordinal)
                    ON TRUE
                JOIN pg_attribute a
                  ON a.attrelid = i.indrelid
                 AND a.attnum   = k.attnum
                WHERE i.indrelid    = v_relid
                  AND i.indisprimary
                  AND a.attnum > 0
                  AND NOT a.attisdropped
            ) pk;

            ------------------------------------------------------------------
            -- Emit logical message
            ------------------------------------------------------------------
            PERFORM pg_logical_emit_message(
                true,
                'synchdb_ddl',
                jsonb_build_object(
                    'type',  cmd.command_tag,
                    'id',    v_id,
                    'table', jsonb_build_object(
                        'primaryKeyColumnNames', v_pk_cols_json,
                        'columns',              v_cols_json
                    )
                )::text
            );
        END LOOP;

    ------------------------------------------------------------------
    -- DROP TABLE path (sql_drop)
    ------------------------------------------------------------------
    ELSIF TG_EVENT = 'sql_drop' THEN

        FOR dobj IN
            SELECT schema_name, object_name
            FROM pg_event_trigger_dropped_objects()
            WHERE object_type = 'table'
        LOOP
            -- id in form: current_database.schema.table
            v_id := current_database() || '.' || dobj.schema_name || '.' || dobj.object_name;

            PERFORM pg_logical_emit_message(
                true,
                'synchdb_ddl',
                jsonb_build_object(
                    'type',  'DROP TABLE',
                    'id',    v_id,
                    'table', jsonb_build_object(
                        'primaryKeyColumnNames', '[]'::jsonb,
                        'columns',              '[]'::jsonb
                    )
                )::text
            );
        END LOOP;

    END IF;
END;
$$;

CREATE EVENT TRIGGER synchdb_ddl_log_ddl
ON ddl_command_end
EXECUTE FUNCTION synchdb_ddl_log();

CREATE EVENT TRIGGER synchdb_ddl_log_drop
ON sql_drop
WHEN TAG IN ('DROP TABLE')
EXECUTE FUNCTION synchdb_ddl_log();

