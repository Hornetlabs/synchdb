-----------------------------------------------------------------------------------------------------------------
    -- MYSQL: functions to map Mysql objects to PostgreSQL via FDW (originated from mysql_migrator project)
-----------------------------------------------------------------------------------------------------------------
CREATE FUNCTION synchdb_create_mysql_objs(
   server      name,
   schema      name    DEFAULT NAME 'public',
   options     jsonb   DEFAULT NULL
) RETURNS void
   LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT SET search_path = pg_catalog AS
$mysql_create_catalog$
DECLARE
   old_msglevel text;
   catalog_table varchar(64);
   catalog_tables varchar(64)[] := ARRAY[
      'SCHEMATA', 'TABLES', 'COLUMNS', 'TABLE_CONSTRAINTS', 'CHECK_CONSTRAINTS',
      'KEY_COLUMN_USAGE', 'REFERENTIAL_CONSTRAINTS', 'VIEWS', 'PARAMETERS',
      'STATISTICS', 'TABLE_PRIVILEGES', 'COLUMN_PRIVILEGES', 'PARTITIONS'
   ];
   sys_schemas text := $$ 'information_schema', 'mysql', 'performance_schema', 'sys' $$;

   /* schemas */
   schemas_sql text := $$
      CREATE OR REPLACE VIEW %1$I.schemas AS
         SELECT "SCHEMA_NAME" AS schema
         FROM %1$I."SCHEMATA"
         WHERE "SCHEMA_NAME" NOT IN (%2$s);
      COMMENT ON VIEW %1$I.schemas IS 'MySQL schemas on foreign server "%3$I"';
   $$;

   /* tables */
   tables_sql text := $$
      CREATE OR REPLACE VIEW %1$I.tables AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name
         FROM %1$I."TABLES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE';
      COMMENT ON VIEW %1$I.tables IS 'MySQL tables on foreign server "%3$I"';
   $$;

   /* columns */
   columns_sql text := $$
      CREATE OR REPLACE VIEW %1$I.columns AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            "COLUMN_NAME" AS column_name, "ORDINAL_POSITION" AS position,
			CASE
				WHEN "COLUMN_TYPE" ILIKE '%%unsigned%%' THEN "DATA_TYPE" || ' unsigned'
				ELSE "DATA_TYPE"
		    END AS type_name, "CHARACTER_MAXIMUM_LENGTH" AS length,
            coalesce("NUMERIC_PRECISION", "DATETIME_PRECISION", null) AS precision,
            "NUMERIC_SCALE" AS scale, "IS_NULLABLE"::boolean AS nullable,
            "COLUMN_DEFAULT" AS default_value
         FROM %1$I."COLUMNS"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s);
      COMMENT ON VIEW %1$I.columns IS 'columns of MySQL tables and views on foreign server "%3$I"';
   $$;

   /* checks */
   check_sql text := $$
      CREATE OR REPLACE VIEW %1$I.checks AS
         SELECT "CONSTRAINT_SCHEMA" AS schema, "TABLE_NAME" as table_name,
            "CONSTRAINT_NAME" AS constraint_name, false AS "deferrable", false AS deferred,
            "CHECK_CLAUSE" AS condition
         FROM %1$I."TABLE_CONSTRAINTS" cons
         JOIN %1$I."CHECK_CONSTRAINTS" cond USING ("CONSTRAINT_SCHEMA", "CONSTRAINT_NAME")
         WHERE "CONSTRAINT_SCHEMA" NOT IN (%2$s)
         --
         -- replace ENUM type by check constraints
         --
         UNION
         SELECT "TABLE_SCHEMA", "TABLE_NAME",
            concat_ws('_', "TABLE_NAME", "COLUMN_NAME", 'enum_chk')::character varying(64),
            false, false, format('(%%I IN %%s)', "COLUMN_NAME", substr("COLUMN_TYPE", 5))
         FROM %1$I."COLUMNS"
         JOIN %1$I."TABLES" USING ("TABLE_SCHEMA", "TABLE_NAME")
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE' AND "DATA_TYPE" = 'enum'
         --
         -- replace SET type by check constraints
         --
         UNION
         SELECT "TABLE_SCHEMA", "TABLE_NAME",
            concat_ws('_', "TABLE_NAME", "COLUMN_NAME", 'set_chk')::character varying(64),
            false, false, format(
               '(string_to_array(%%I, '','') <@ ARRAY[%%s])',
               "COLUMN_NAME", regexp_replace("COLUMN_TYPE", '^set\(([^)]*)\)$', '\1')
            )
         FROM %1$I."COLUMNS"
         JOIN %1$I."TABLES" USING ("TABLE_SCHEMA", "TABLE_NAME")
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE' AND "DATA_TYPE" = 'set';
      COMMENT ON VIEW %1$I.checks IS 'MySQL check constraints on foreign server "%3$I"';
   $$;

   /* foreign_keys */
   foreign_keys_sql text := $$
      CREATE OR REPLACE VIEW %1$I.foreign_keys AS
         SELECT "CONSTRAINT_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            "CONSTRAINT_NAME" AS constraint_name, false AS "deferrable", false AS deferred,
            "DELETE_RULE" AS delete_rule, "COLUMN_NAME" AS column_name, "ORDINAL_POSITION" AS position,
            "REFERENCED_TABLE_SCHEMA" AS remote_schema, keys."REFERENCED_TABLE_NAME" AS remote_table,
            "REFERENCED_COLUMN_NAME" AS remote_column
         FROM %1$I."KEY_COLUMN_USAGE" keys
         JOIN %1$I."REFERENTIAL_CONSTRAINTS" refs USING ("CONSTRAINT_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME")
         WHERE "CONSTRAINT_SCHEMA" NOT IN (%2$s)
         AND "REFERENCED_COLUMN_NAME" IS NOT NULL;
      COMMENT ON VIEW %1$I.foreign_keys IS 'MySQL foreign key columns on foreign server "%3$I"';
   $$;

   /* keys */
   keys_sql text := $$
      CREATE OR REPLACE VIEW %1$I.keys AS
         SELECT "CONSTRAINT_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            (CASE WHEN "CONSTRAINT_NAME" = 'PRIMARY'
               THEN concat_ws('_', "TABLE_NAME", 'pkey')
               ELSE "CONSTRAINT_NAME"
            END)::character varying(64) AS constraint_name, false AS "deferrable", false AS deferred,
            "COLUMN_NAME" AS column_name, "ORDINAL_POSITION" AS position,
            ("CONSTRAINT_TYPE" = 'PRIMARY KEY') AS is_primary
         FROM %1$I."TABLE_CONSTRAINTS" cons
         JOIN %1$I."KEY_COLUMN_USAGE" keys USING ("CONSTRAINT_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME")
         WHERE "CONSTRAINT_SCHEMA" NOT IN (%2$s)
         AND "CONSTRAINT_TYPE" IN ('PRIMARY KEY', 'UNIQUE');
      COMMENT ON VIEW %1$I.keys IS 'MySQL primary and unique key columns on foreign server "%3$I"';
   $$;

   /* views */
   views_sql text := $$
      CREATE OR REPLACE VIEW %1$I.views AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS view_name,
            "VIEW_DEFINITION" AS definition
         FROM %1$I."VIEWS"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s);
      COMMENT ON VIEW %1$I.views IS 'MySQL views on foreign server "%3$I"';
   $$;

   /* functions */
   functions_sql text := $$
      DROP FOREIGN TABLE IF EXISTS %1$I."ROUTINES" CASCADE;
      CREATE FOREIGN TABLE %1$I."ROUTINES" (
         "ROUTINE_SCHEMA" varchar(64) NOT NULL,
         "ROUTINE_NAME" varchar(64) NOT NULL,
         "ROUTINE_TYPE" varchar(10) NOT NULL,
         "DTD_IDENTIFIER" text,
         "ROUTINE_BODY" varchar(3) NOT NULL,
         "ROUTINE_DEFINITION" text,
         "EXTERNAL_LANGUAGE" varchar(64) NOT NULL,
         "IS_DETERMINISTIC" varchar(3) NOT NULL
      ) SERVER %3$I OPTIONS (dbname 'information_schema', table_name 'ROUTINES');

      CREATE OR REPLACE VIEW %1$I.functions AS
         SELECT "ROUTINE_SCHEMA" AS schema, "ROUTINE_NAME" AS function_name,
            ("ROUTINE_TYPE" = 'PROCEDURE') AS is_procedure,
            (CASE "ROUTINE_TYPE"
               WHEN 'PROCEDURE' THEN
                  concat_ws(' ',
                     'CREATE PROCEDURE', "ROUTINE_NAME", '(', parameters, ')',
                     "ROUTINE_DEFINITION"
                  )
               WHEN 'FUNCTION' THEN
                  concat_ws(' ',
                     'CREATE FUNCTION', "ROUTINE_NAME", '(', parameters, ')',
                     'RETURNS', "DTD_IDENTIFIER",
                     "ROUTINE_DEFINITION"
                  )
               END
            ) AS source
         FROM %1$I."ROUTINES" rout
         JOIN (
            SELECT "SPECIFIC_SCHEMA" AS "ROUTINE_SCHEMA", "SPECIFIC_NAME" AS "ROUTINE_NAME",
               string_agg(
                  concat_ws(' ', "PARAMETER_MODE", "PARAMETER_NAME", "DTD_IDENTIFIER"),
                  text ', ' ORDER BY "ORDINAL_POSITION"
               ) AS parameters
            FROM %1$I."PARAMETERS"
            WHERE "ORDINAL_POSITION" > 0
            GROUP BY "SPECIFIC_SCHEMA", "SPECIFIC_NAME"
         ) prms USING ("ROUTINE_SCHEMA", "ROUTINE_NAME")
         WHERE "ROUTINE_SCHEMA" NOT IN (%2$s);
      COMMENT ON VIEW %1$I.functions IS 'MySQL functions and procedures on foreign server "%3$I"';
   $$;

   /* sequences */
   sequences_sql text := $$
      CREATE OR REPLACE VIEW %1$I.sequences AS
         SELECT "TABLE_SCHEMA" AS schema, concat("TABLE_NAME", '_seq') AS sequence_name,
            1 AS "min_value", null::integer AS max_value, 1 AS increment_by, false AS cyclical,
            1 AS cache_size, "AUTO_INCREMENT" AS last_value
         FROM %1$I."TABLES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE' AND "AUTO_INCREMENT" IS NOT NULL;
      COMMENT ON VIEW %1$I.sequences IS 'MySQL sequences on foreign server "%3$I"';
   $$;

   /* index_columns */
   index_columns_sql text := $$
      CREATE OR REPLACE VIEW %1$I.index_columns AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            concat_ws('_', "TABLE_NAME", "INDEX_NAME")::character varying(64) AS index_name,
            "SEQ_IN_INDEX" AS position, (CASE WHEN "COLLATION" = 'D' THEN true ELSE false END) AS descend,
            "EXPRESSION" IS NOT NULL
               AND ("COLLATION" <> 'D' OR "EXPRESSION" !~ '^`[^`]*`$') AS is_expression,
            coalesce(
               CASE WHEN "COLLATION" = 'D' AND "EXPRESSION" !~ '^`[^`]*`$'
                  THEN replace ("EXPRESSION", '`', '''')
                  ELSE "EXPRESSION"
               END, "COLUMN_NAME")::character varying(64) AS column_name
         FROM %1$I."STATISTICS"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "INDEX_NAME" <> 'PRIMARY' AND "IS_VISIBLE"::boolean; -- prior to MySQL v8
      COMMENT ON VIEW %1$I.index_columns IS 'MySQL index columns on foreign server "%3$I"';
   $$;

   /* indexes */
   indexes_sql text := $$
      CREATE OR REPLACE VIEW %1$I.indexes AS
         SELECT DISTINCT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            concat_ws('_', "TABLE_NAME", "INDEX_NAME")::character varying(64) AS index_name,
            "INDEX_TYPE" AS index_type, ("NON_UNIQUE" = 0) AS uniqueness, null::text AS where_clause
         FROM %1$I."STATISTICS"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "INDEX_NAME" <> 'PRIMARY' AND "IS_VISIBLE"::boolean; -- prior to MySQL v8
      COMMENT ON VIEW %1$I.indexes IS 'MySQL indexes on foreign server "%3$I"';
   $$;

   /* partitions */
   partitions_sql text := $$
      CREATE OR REPLACE VIEW %1$I.partitions AS
         WITH catalog AS (
            SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
                  "PARTITION_NAME" AS partition_name, "PARTITION_METHOD" AS type,
                  trim('`' FROM "PARTITION_EXPRESSION") AS key,
                  "PARTITION_ORDINAL_POSITION" AS position,
                  "PARTITION_DESCRIPTION" AS values
               FROM %1$I."PARTITIONS"
               WHERE "TABLE_SCHEMA" NOT IN (%2$s) AND "PARTITION_NAME" IS NOT NULL
               AND ("SUBPARTITION_ORDINAL_POSITION" IS NULL OR "SUBPARTITION_ORDINAL_POSITION" = 1)
         ), list_partitions AS (
            -- retrieves values[any, ...]
            SELECT schema, table_name, partition_name, type, key,
               (values IS NULL) AS is_default,
               string_to_array(values, ',') AS values
            FROM catalog WHERE type = 'LIST'
         ), range_partitions AS (
            -- retrieves values[lower_bound, upper_bound]
            SELECT schema, table_name, partition_name, type, key, false,
               ARRAY[
                  lag(values, 1, 'MINVALUE')
                     OVER (PARTITION BY schema, table_name ORDER BY position),
                  values
               ] AS values
            FROM catalog WHERE type = 'RANGE'
         ), hash_partitions AS (
            -- retrieves values[modulus, remainder]
            SELECT schema, table_name, partition_name, type, key, false,
               ARRAY[(position - 1)::text] AS values
            FROM catalog WHERE type = 'HASH'
         )
         SELECT * FROM list_partitions
         UNION SELECT * FROM range_partitions
         UNION SELECT * FROM hash_partitions;
      COMMENT ON VIEW %1$I.partitions IS 'MySQL partitions on foreign server "%3$I"';
   $$;

   /* subpartitions */
   subpartitions_sql text := $$
      CREATE OR REPLACE VIEW %1$I.subpartitions AS
         WITH catalog AS (
            SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
               "PARTITION_NAME" AS partition_name, "SUBPARTITION_NAME" AS subpartition_name,
               "SUBPARTITION_METHOD" AS type,
               trim('`' FROM "SUBPARTITION_EXPRESSION") AS key,
               "SUBPARTITION_ORDINAL_POSITION" AS position
            FROM %1$I."PARTITIONS"
            WHERE "TABLE_SCHEMA" NOT IN (%2$s)
            AND "PARTITION_NAME" IS NOT NULL AND "SUBPARTITION_NAME" IS NOT NULL
         )
         -- MySQL only supports HASH subpartition method
         SELECT schema, table_name, partition_name, subpartition_name, type, key,
            false AS is_default, ARRAY[(position - 1)::text] AS values
         FROM catalog WHERE type = 'HASH';
      COMMENT ON VIEW %1$I.partitions IS 'MySQL subpartitions on foreign server "%3$I"';
   $$;

   /* triggers */
   triggers_sql text := $$
      DROP FOREIGN TABLE IF EXISTS %1$I."TRIGGERS" CASCADE;
      CREATE FOREIGN TABLE %1$I."TRIGGERS" (
         "TRIGGER_SCHEMA" varchar(64) NOT NULL,
         "TRIGGER_NAME" varchar(64) NOT NULL,
         "EVENT_MANIPULATION" varchar(6) NOT NULL,
         "EVENT_OBJECT_TABLE" varchar(64) NOT NULL,
         "ACTION_STATEMENT" text NOT NULL,
         "ACTION_ORIENTATION" varchar(3) NOT NULL,
         "ACTION_TIMING" varchar(6) NOT NULL
      ) SERVER %3$I OPTIONS (dbname 'information_schema', table_name 'TRIGGERS');

      CREATE OR REPLACE VIEW %1$I.triggers AS
         SELECT "TRIGGER_SCHEMA" AS schema, "EVENT_OBJECT_TABLE" AS table_name,
            "TRIGGER_NAME" AS trigger_name, "ACTION_TIMING" AS trigger_type,
            "EVENT_MANIPULATION" AS triggering_event,
            ("ACTION_ORIENTATION" = 'ROW') AS for_each_row, null AS when_clause,
            'REFERENCING NEW AS NEW OLD AS OLD' AS referencing_names,
            "ACTION_STATEMENT" AS trigger_body
         FROM %1$I."TRIGGERS"
         WHERE "TRIGGER_SCHEMA" NOT IN (%2$s);
      COMMENT ON VIEW %1$I.triggers IS 'MySQL triggers on foreign server "%3$I"';
   $$;

   /* table_privs */
   table_privs_sql text := $$
      CREATE OR REPLACE VIEW %1$I.table_privs AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            "PRIVILEGE_TYPE" AS privilege, 'root'::varchar(292) AS grantor,
            "GRANTEE" as grantee, "IS_GRANTABLE"::boolean AS grantable
         FROM %1$I."TABLE_PRIVILEGES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s) AND "GRANTEE" !~* 'root';
      COMMENT ON VIEW %1$I.table_privs IS 'Privileges on MySQL tables on foreign server "%3$I"';
   $$;

   /* column_privs */
   column_privs_sql text := $$
      CREATE OR REPLACE VIEW %1$I.column_privs AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS table_name,
            "COLUMN_NAME" AS column_name, "PRIVILEGE_TYPE" AS privilege,
            'root'::varchar(292) AS grantor, "GRANTEE" AS grantee, "IS_GRANTABLE"::boolean AS grantable
         FROM %1$I."COLUMN_PRIVILEGES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s) AND "GRANTEE" !~* 'root';
      COMMENT ON VIEW %1$I.column_privs IS 'Privileges on MySQL table columns on foreign server "%3$I"';
   $$;

   /* segments */
   segments_sql text := $$
      DROP FOREIGN TABLE IF EXISTS %1$I.innodb_index_stats CASCADE;
      CREATE FOREIGN TABLE %1$I.innodb_index_stats (
         database_name varchar(64) NOT NULL,
         table_name varchar(64) NOT NULL,
         index_name varchar(64) NOT NULL,
         stat_value bigint NOT NULL,
         stat_description varchar(1024) NOT NULL
      ) SERVER %3$I OPTIONS (dbname 'mysql', table_name 'innodb_index_stats');

      CREATE OR REPLACE VIEW %1$I.segments AS
         SELECT "TABLE_SCHEMA" AS schema, "TABLE_NAME" AS segment_name,
            'TABLE' AS segment_type, "DATA_LENGTH" AS bytes
         FROM %1$I."TABLES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE'
         UNION
         SELECT "TABLE_SCHEMA", "TABLE_NAME" segment_name,
            'INDEX' AS segment_name, "INDEX_LENGTH" AS bytes
         FROM %1$I."TABLES"
         WHERE "TABLE_SCHEMA" NOT IN (%2$s)
         AND "TABLE_TYPE" = 'BASE TABLE' AND "ENGINE" = 'MyISAM'
         UNION
         SELECT database_name, index_name, 'INDEX' AS segment_type,
            sum(stat_value) * 16384 AS bytes
         FROM %1$I.innodb_index_stats
         WHERE database_name NOT IN (%2$s)
         AND index_name <> 'PRIMARY'
         AND stat_description LIKE 'Number of pages in the index'
         GROUP BY database_name, index_name;
      COMMENT ON VIEW %1$I.segments IS 'Size of MySQL objects on foreign server "%3$I"';
   $$;

   /* migration_cost_estimate */
   migration_cost_estimate_sql text := $$
      CREATE VIEW %1$I.migration_cost_estimate AS
         SELECT schema, 'tables'::text AS task_type, count(*)::bigint AS task_content,
            'count'::text AS task_unit, ceil(count(*) / 10.0)::integer AS migration_hours
         FROM %1$I.tables GROUP BY schema
         UNION ALL
         SELECT t.schema, 'data_migration'::text, sum(bytes)::bigint,
            'bytes'::text, ceil(sum(bytes::float8) / 26843545600.0)::integer
         FROM %1$I.segments AS s
         JOIN %1$I.tables AS t ON s.schema = t.schema AND s.segment_name = t.table_name
         WHERE s.segment_type = 'TABLE'
         GROUP BY t.schema
         UNION ALL
         SELECT schema, 'functions'::text, coalesce(sum(octet_length(source)), 0),
            'characters'::text, ceil(coalesce(sum(octet_length(source)), 0) / 512.0)::integer
         FROM %1$I.functions GROUP BY schema
         UNION ALL
         SELECT schema, 'triggers'::text, coalesce(sum(octet_length(trigger_body)), 0),
            'characters'::text, ceil(coalesce(sum(octet_length(trigger_body)), 0) / 512.0)::integer
         FROM %1$I.triggers GROUP BY schema
         UNION ALL
         SELECT schema, 'views'::text, coalesce(sum(octet_length(definition)), 0),
            'characters'::text, ceil(coalesce(sum(octet_length(definition)), 0) / 512.0)::integer
         FROM %1$I.views GROUP BY schema;
      COMMENT ON VIEW %1$I.migration_cost_estimate IS 'Estimate of the migration costs per schema and object type';
   $$;

BEGIN
   /* remember old setting */
   old_msglevel := current_setting('client_min_messages');

   /* make the output less verbose */
   SET LOCAL client_min_messages = warning;
   
/* CREATE SCHEMA IF NEEDED */
   EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema);

   /* refresh catalog foreign tables */
   FOREACH catalog_table IN ARRAY catalog_tables
   LOOP
      EXECUTE format(
         $$ DROP FOREIGN TABLE IF EXISTS %1$I.%2$I CASCADE $$,
         schema, catalog_table
      );

      EXECUTE format(
         $$ IMPORT FOREIGN SCHEMA information_schema
         LIMIT TO (%3$I)
         FROM SERVER %1$I INTO %2$I
         OPTIONS (import_enum_as_text 'true') $$,
         server, schema, catalog_table
      );
   END LOOP;


   /* create views with predefined column names needed by db_migrator */
   EXECUTE format(schemas_sql, schema, sys_schemas, server);
   EXECUTE format(tables_sql, schema, sys_schemas, server);
   EXECUTE format(columns_sql, schema, sys_schemas, server);
   EXECUTE format(check_sql, schema, sys_schemas, server);
   EXECUTE format(foreign_keys_sql, schema, sys_schemas, server);
   EXECUTE format(keys_sql, schema, sys_schemas, server);
   EXECUTE format(views_sql, schema, sys_schemas, server);
   EXECUTE format(functions_sql, schema, sys_schemas, server);
   EXECUTE format(sequences_sql, schema, sys_schemas, server);
   EXECUTE format(index_columns_sql, schema, sys_schemas, server);
   EXECUTE format(indexes_sql, schema, sys_schemas, server);
   EXECUTE format(partitions_sql, schema, sys_schemas, server);
   EXECUTE format(subpartitions_sql, schema, sys_schemas, server);
   EXECUTE format(triggers_sql, schema, sys_schemas, server);
   EXECUTE format(table_privs_sql, schema, sys_schemas, server);
   EXECUTE format(column_privs_sql, schema, sys_schemas, server);
   EXECUTE format(segments_sql, schema, sys_schemas, server);
   EXECUTE format(migration_cost_estimate_sql, schema);

   /* reset client_min_messages */
   EXECUTE 'SET LOCAL client_min_messages = ' || old_msglevel;
END;
$mysql_create_catalog$;

COMMENT ON FUNCTION synchdb_create_mysql_objs(name, name, jsonb) IS
   'create MySQL foreign tables for the metadata of a foreign server';

CREATE OR REPLACE FUNCTION synchdb_create_current_binlog_pos_ft(
    p_schema name,   -- e.g. 'mysql_stage'
    p_server name    -- e.g. 'mysql_server'
) RETURNS void
LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT
SET search_path = pg_catalog
AS $synchdb_create_current_binlog_pos_ft$
DECLARE
    v_schema name := p_schema;
    v_server name := p_server;
BEGIN
    ----------------------------------------------------------------------
    -- Sanity check: foreign server must exist
    ----------------------------------------------------------------------
    PERFORM 1
    FROM pg_foreign_server
    WHERE srvname = v_server;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'synchdb_create_current_binlog_pos_ft: foreign server "%" does not exist', v_server;
    END IF;

    ----------------------------------------------------------------------
    -- Ensure target schema exists
    ----------------------------------------------------------------------
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);

    ----------------------------------------------------------------------
    -- performance_schema.global_variables -> <schema>.global_variables
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.global_variables (
            variable_name  text,
            variable_value text
        )
        SERVER %2$I
        OPTIONS (
            dbname     'performance_schema',
            table_name 'global_variables'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- performance_schema.log_status -> <schema>.log_status
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.log_status (
            server_uuid     text,
            local           text,   -- JSON document as text
            replication     text,   -- JSON
            storage_engines text    -- JSON
        )
        SERVER %2$I
        OPTIONS (
            dbname     'performance_schema',
            table_name 'log_status'
        )
    $SQL$, v_schema, v_server);

    RETURN;
END;
$synchdb_create_current_binlog_pos_ft$;

COMMENT ON FUNCTION synchdb_create_current_binlog_pos_ft(name, name) IS
   'create MySQL foreign tables to obtain binlog file, pos and server id';

