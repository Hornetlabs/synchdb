-- Read-only report, valid before or after the restricted catalog upgrade.
-- No LOAD, decryption, START/STOP, source DDL, or checkpoint conversion.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL search_path = pg_catalog;
SELECT n.nspname AS ext_schema, e.extversion AS ext_version
FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace
WHERE e.extname='synchdb'
\gset
SELECT :'ext_version'='1.1' AS has_control_catalog
\gset
\if :has_control_catalog
SELECT coalesce((SELECT jsonb_agg(jsonb_build_object('connector_id',connector_id,
        'desired_state',desired_state,'actual_state',actual_state))
        FROM :"ext_schema".synchdb_control_state),'[]'::jsonb) AS control_intents,
       (SELECT count(*) FROM :"ext_schema".synchdb_control_commands
         WHERE status IN ('PENDING','EXECUTING')) AS pending_commands
\gset
\else
\set control_intents '[]'
\set pending_commands 0
\endif

WITH catalog_profile AS (
-- BEGIN SHARED CATALOG PROFILE QUERY
SELECT jsonb_build_object(
 'functions',coalesce((SELECT jsonb_agg(jsonb_build_object(
   'name',p.proname,'in_extension_schema',p.pronamespace=(SELECT extnamespace FROM pg_extension WHERE oid=(SELECT oid FROM pg_extension WHERE extname='synchdb')),
   'identity_arguments',pg_get_function_identity_arguments(p.oid),
   'arguments',pg_get_function_arguments(p.oid),'result',pg_get_function_result(p.oid),
   'kind',p.prokind,'language',l.lanname,'volatility',p.provolatile,
   'strict',p.proisstrict,'security_definer',p.prosecdef,
   'configuration',coalesce(to_jsonb(p.proconfig),'[]'::jsonb))
   ORDER BY p.proname COLLATE "C",pg_get_function_identity_arguments(p.oid) COLLATE "C")
   FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
   JOIN pg_depend d ON d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.objsubid=0
   WHERE d.refclassid='pg_extension'::regclass AND d.refobjid=(SELECT oid FROM pg_extension WHERE extname='synchdb') AND d.deptype='e'),'[]'::jsonb),
 'relations',coalesce((SELECT jsonb_agg(jsonb_build_object(
   'name',c.relname,'in_extension_schema',c.relnamespace=(SELECT extnamespace FROM pg_extension WHERE oid=(SELECT oid FROM pg_extension WHERE extname='synchdb')),
   'kind',c.relkind,
   'columns',coalesce((SELECT jsonb_agg(jsonb_build_object(
     'name',a.attname,'type',format_type(a.atttypid,a.atttypmod),'not_null',a.attnotnull,
     'default',pg_get_expr(ad.adbin,ad.adrelid)) ORDER BY a.attnum)
     FROM pg_attribute a LEFT JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum
     WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped),'[]'::jsonb),
   'constraints',coalesce((SELECT jsonb_agg(jsonb_build_object(
     'name',k.conname,'definition',pg_get_constraintdef(k.oid)) ORDER BY k.conname COLLATE "C")
     FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype<>'n'),'[]'::jsonb)) ORDER BY c.relname COLLATE "C")
   FROM pg_class c JOIN pg_depend d ON d.classid='pg_class'::regclass AND d.objid=c.oid AND d.objsubid=0
   WHERE d.refclassid='pg_extension'::regclass AND d.refobjid=(SELECT oid FROM pg_extension WHERE extname='synchdb') AND d.deptype='e'
     AND c.relkind IN ('r','v','m','f','p','S')),'[]'::jsonb)) AS profile
-- END SHARED CATALOG PROFILE QUERY
), secrets(field) AS (
    VALUES ('pwd'),('ssl_keystore_pass'),('ssl_truststore_pass'),
           ('jmx_ssl_keystore_pass'),('jmx_ssl_truststore_pass'),('fdw_ssl_cipher')
), connectors AS (
    SELECT c.name,c.isactive,c.data->>'connector' AS connector,
           coalesce(c.data->>'dstdb',current_database()) AS target_database,
           coalesce(c.data->>'ispn_cache_type','null') AS ispn_cache_type,
           c.data->>'ispn_memory_type' AS ispn_memory_type,
           c.data->>'ispn_memory_size' AS ispn_memory_size,
           CASE
             WHEN lower(c.data->>'connector') NOT IN ('mysql','oracle','sqlserver','postgres','olr')
               OR c.data->>'connector' IS NULL THEN 'UNSUPPORTED_BACKEND'
             WHEN lower(coalesce(c.data->>'ispn_cache_type','null'))<>'null' AND lower(c.data->>'connector')<>'oracle'
               THEN 'INFINISPAN_UNSUPPORTED_BACKEND'
             WHEN lower(coalesce(c.data->>'ispn_cache_type','null')) NOT IN ('null','embedded')
               THEN 'INFINISPAN_UNSUPPORTED_CACHE_TYPE'
             WHEN lower(coalesce(c.data->>'ispn_cache_type','null'))='embedded' AND
               (lower(coalesce(c.data->>'ispn_memory_type','')) NOT IN ('heap','off heap') OR
                coalesce(c.data->>'ispn_memory_size','') !~ '^[1-9][0-9]*$')
               THEN 'INFINISPAN_INVALID_CONFIGURATION'
             WHEN coalesce(c.data->>'dstdb',current_database())<>current_database()
               THEN 'TARGET_DATABASE_MISMATCH'
             WHEN position('/' IN c.name)>0
               OR position('/' IN coalesce(c.data->>'dstdb',current_database()))>0
               THEN 'INVALID_LEGACY_FILENAME'
             WHEN lower(c.data->>'connector')='postgres' AND
               octet_length(c.name||'_'||coalesce(c.data->>'dstdb',current_database())||'_synchdb_slot')>
                 current_setting('max_identifier_length')::integer
               THEN 'LEGACY_POSTGRES_REMOTE_NAME_TOO_LONG'
             WHEN position('_' IN c.name)>0
               OR position('_' IN coalesce(c.data->>'dstdb',current_database()))>0
               THEN 'FILE_IDENTITY_CHECK_REQUIRED'
             ELSE 'SERVER_FILE_AND_OFFSET_CHECK_REQUIRED'
           END AS legacy_resume_check,
           coalesce((SELECT jsonb_object_agg(s.field,jsonb_typeof(c.data->s.field))
                     FROM secrets s WHERE c.data ? s.field
                       AND c.data->s.field<>'null'::jsonb),'{}'::jsonb) AS credential_storage
      FROM :"ext_schema".synchdb_conninfo c
)
SELECT jsonb_build_object(
    'format','synchdb-upgrade-preflight','version',1,
    'database',current_database(),'database_user',current_user,
    'database_oid',(SELECT oid FROM pg_database WHERE datname=current_database()),
    'superuser',(SELECT rolsuper FROM pg_roles WHERE rolname=current_user),
    'server_version',current_setting('server_version'),
    'snapshot_engine',coalesce(current_setting('synchdb.snapshot_engine',true),'debezium'),
    'extension_version',:'ext_version','extension_schema',:'ext_schema',
    'expected_source_release','v1.4','expected_source_extension_version','1.0',
    'catalog_profile',(SELECT profile FROM catalog_profile),
    'extension_owner',(SELECT pg_get_userbyid(extowner) FROM pg_extension WHERE extname='synchdb'),
    'function_members',(SELECT count(*) FROM pg_proc p JOIN pg_depend d
        ON d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.objsubid=0
        JOIN pg_extension e ON d.refclassid='pg_extension'::regclass AND d.refobjid=e.oid
        WHERE e.extname='synchdb' AND d.deptype='e'),
    'connector_columns',(SELECT jsonb_agg(jsonb_build_object('name',attname,'type',format_type(atttypid,atttypmod)) ORDER BY attnum)
        FROM pg_attribute WHERE attrelid=pg_catalog.format('%I.synchdb_conninfo',:'ext_schema')::regclass
          AND attnum>0 AND NOT attisdropped),
    'update_path_1_0_to_1_1',(SELECT path FROM pg_extension_update_paths('synchdb')
        WHERE source='1.0' AND target='1.1'),
    'shared_preload_libraries',current_setting('shared_preload_libraries'),
    'control_intents',:'control_intents'::jsonb,'pending_commands',:'pending_commands'::integer,
    'synchdb_workers',coalesce((SELECT jsonb_agg(jsonb_build_object('pid',pid,'database',datname,'backend_type',backend_type))
        FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND backend_type ILIKE '%synchdb%'),'[]'::jsonb),
    'connectors',coalesce((SELECT jsonb_agg(to_jsonb(c) ORDER BY name) FROM connectors c),'[]'::jsonb),
    'scope','caller-visible rows; server file ownership/offset retention/data correctness are not verified',
    'ordinary_1_0_credentials','compatible; no keyring or password conversion required'
);
COMMIT;
