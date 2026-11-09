-- Read-only inventory for the 1.0 baseline and the refactored 1.1 catalogs.
-- Run through synchdb_migrate.py inspect; this is not an import format.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL search_path = pg_catalog;

-- \gset fails with ON_ERROR_STOP if the extension is not installed.
SELECT n.nspname AS ext_schema, e.extversion AS ext_version
  FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
 WHERE e.extname = 'synchdb'
\gset

WITH allowed(key) AS (
    VALUES ('hostname'), ('port'), ('user'), ('srcdb'), ('dstdb'),
           ('srcschema'), ('table'), ('snapshottable'), ('connector'),
           ('ssl_mode'), ('ssl_keystore'), ('ssl_truststore'),
           ('jmx_listenaddr'), ('jmx_port'), ('jmx_rmi_address'),
           ('jmx_rmi_port'), ('jmx_auth'), ('jmx_auth_passwdfile'),
           ('jmx_auth_accessfile'), ('jmx_ssl'), ('jmx_ssl_keystore'),
           ('jmx_ssl_truststore'), ('jmx_exporter'), ('jmx_exporter_port'),
           ('jmx_exporter_conf'), ('olr_host'), ('olr_port'), ('olr_source'),
           ('ispn_cache_type'), ('ispn_memory_type'), ('ispn_memory_size'),
           ('fdw_ssl_cert'), ('fdw_ssl_key'), ('fdw_ssl_rootcert')
), secrets(key) AS (
    VALUES ('pwd'), ('ssl_keystore_pass'), ('ssl_truststore_pass'),
           ('jmx_ssl_keystore_pass'), ('jmx_ssl_truststore_pass'),
           ('fdw_ssl_cipher')
), connectors AS (
    SELECT c.name, c.isactive,
           COALESCE((SELECT jsonb_object_agg(j.key, j.value)
                       FROM jsonb_each(c.data) j JOIN allowed a USING (key)
                      WHERE jsonb_typeof(j.value) IN
                            ('string', 'number', 'boolean', 'null')), '{}'::jsonb)
               AS configuration,
           COALESCE((SELECT jsonb_agg(s.key ORDER BY s.key)
                       FROM secrets s WHERE c.data ? s.key
                        AND c.data -> s.key <> 'null'::jsonb), '[]'::jsonb)
               AS credential_fields,
           COALESCE((SELECT jsonb_object_agg(s.key, jsonb_typeof(c.data->s.key))
                       FROM secrets s WHERE c.data ? s.key
                        AND c.data->s.key <> 'null'::jsonb), '{}'::jsonb)
               AS credential_storage,
           COALESCE((SELECT jsonb_agg(j.key ORDER BY j.key)
                       FROM jsonb_each(c.data) j
                      WHERE NOT EXISTS (SELECT 1 FROM allowed a WHERE a.key = j.key)
                        AND NOT EXISTS (SELECT 1 FROM secrets s WHERE s.key = j.key)
                         OR (EXISTS (SELECT 1 FROM allowed a WHERE a.key = j.key)
                             AND jsonb_typeof(j.value) NOT IN
                                 ('string', 'number', 'boolean', 'null'))), '[]'::jsonb)
               AS fields_requiring_manual_review
      FROM :"ext_schema".synchdb_conninfo c
), mappings AS (
    SELECT name::text, objtype::text, enabled, srcobj::text, dstobj
      FROM :"ext_schema".synchdb_objmap
), bindings AS (
    SELECT a.name::text, a.type::text, a.ext_tbname::text,
           a.ext_attname::text, a.ext_atttypename::text,
           n.nspname AS target_schema, c.relname AS target_table,
           p.attname AS target_column,
           format_type(p.atttypid, p.atttypmod) AS target_type
      FROM :"ext_schema".synchdb_attribute a
      LEFT JOIN pg_class c ON c.oid = a.attrelid
      LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
      LEFT JOIN pg_attribute p ON p.attrelid = a.attrelid
                             AND p.attnum = a.attnum AND NOT p.attisdropped
)
SELECT jsonb_build_object(
    'format', 'synchdb-migration-inventory', 'version', 1,
    'database', current_database(), 'database_user', current_user,
    'server_version', current_setting('server_version'),
    'extension_version', :'ext_version', 'extension_schema', :'ext_schema',
    'scope', 'caller-visible rows; not a backup or an import file',
    'connectors', COALESCE((SELECT jsonb_agg(to_jsonb(c) ORDER BY c.name)
                             FROM connectors c), '[]'::jsonb),
    'mappings', COALESCE((SELECT jsonb_agg(to_jsonb(m)
                               ORDER BY m.name, m.objtype, m.srcobj)
                           FROM mappings m), '[]'::jsonb),
    'target_bindings', COALESCE((SELECT jsonb_agg(to_jsonb(b)
                                      ORDER BY b.name, b.ext_tbname, b.ext_attname)
                                  FROM bindings b), '[]'::jsonb),
    'recognized_settings', COALESCE((SELECT jsonb_object_agg(name, setting)
                                      FROM pg_settings
                                     WHERE name LIKE 'synchdb.%'
                                        OR name IN ('shared_preload_libraries',
                                                    'max_worker_processes')), '{}'::jsonb)
);
COMMIT;
