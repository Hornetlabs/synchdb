-- Restricted 1.0 -> 1.1 catalog upgrade. Generated; no checkpoint import.
-- Use only through ALTER EXTENSION, with workers and the server preloader off.
DO $synchdb_upgrade_guard$
DECLARE v_ext oid; v_schema name; v_catalog jsonb; v_active boolean; v_search_path text;
BEGIN
 v_search_path := current_setting('search_path');
 PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
 SELECT e.oid,n.nspname INTO STRICT v_ext,v_schema FROM pg_extension e
 -- PostgreSQL sets the target extversion before running the update script.
 -- The selected update edge and the exact old layout below gate its source.
 JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='synchdb' AND e.extversion='1.1';
 IF v_schema<>'public' THEN
  RAISE EXCEPTION 'This v1.4 upgrade supports only the public extension schema' USING ERRCODE='55000';
 END IF;
 IF EXISTS (SELECT 1 FROM unnest(regexp_split_to_array(current_setting('shared_preload_libraries'),'\s*,\s*')) x(library)
   WHERE regexp_replace(trim(both '"' from btrim(library)), '^.*[/\\]', '')
         IN ('synchdb','synchdb.so','synchdb.dll','synchdb.dylib')) THEN
  RAISE EXCEPTION 'Upgrade requires synchdb removed from shared_preload_libraries' USING ERRCODE='55000';
 END IF;
 IF EXISTS (SELECT 1 FROM pg_stat_activity
             WHERE pid<>pg_backend_pid() AND backend_type ILIKE '%synchdb%') THEN
  RAISE EXCEPTION 'Stop all SynchDB background workers before catalog upgrade' USING ERRCODE='55000';
 END IF;
 SELECT jsonb_build_object(
 'functions',coalesce((SELECT jsonb_agg(jsonb_build_object(
   'name',p.proname,'in_extension_schema',p.pronamespace=(SELECT extnamespace FROM pg_extension WHERE oid=v_ext),
   'identity_arguments',pg_get_function_identity_arguments(p.oid),
   'arguments',pg_get_function_arguments(p.oid),'result',pg_get_function_result(p.oid),
   'kind',p.prokind,'language',l.lanname,'volatility',p.provolatile,
   'strict',p.proisstrict,'security_definer',p.prosecdef,
   'configuration',coalesce(to_jsonb(p.proconfig),'[]'::jsonb))
   ORDER BY p.proname COLLATE "C",pg_get_function_identity_arguments(p.oid) COLLATE "C")
   FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
   JOIN pg_depend d ON d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.objsubid=0
   WHERE d.refclassid='pg_extension'::regclass AND d.refobjid=v_ext AND d.deptype='e'),'[]'::jsonb),
 'relations',coalesce((SELECT jsonb_agg(jsonb_build_object(
   'name',c.relname,'in_extension_schema',c.relnamespace=(SELECT extnamespace FROM pg_extension WHERE oid=v_ext),
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
   WHERE d.refclassid='pg_extension'::regclass AND d.refobjid=v_ext AND d.deptype='e'
     AND c.relkind IN ('r','v','m','f','p','S')),'[]'::jsonb)) INTO v_catalog;
 IF v_catalog IS DISTINCT FROM $synchdb_v14_profile${"functions":[{"kind":"f","name":"mysql_type_to_jdbc","result":"integer","strict":true,"language":"plpgsql","arguments":"mysql_type_text text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"mysql_type_text text","in_extension_schema":true},{"kind":"f","name":"oracle_type_to_jdbc","result":"integer","strict":true,"language":"plpgsql","arguments":"ora_type_text text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"ora_type_text text","in_extension_schema":true},{"kind":"f","name":"read_snapshot_table_list","result":"text","strict":false,"language":"plpgsql","arguments":"file_uri text, p_conn_type text, p_desired_db text","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"file_uri text, p_conn_type text, p_desired_db text","in_extension_schema":true},{"kind":"f","name":"synchdb_add_conninfo","result":"integer","strict":true,"language":"c","arguments":"name, text, integer, text, text, text, text, text, text, text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, text, integer, text, text, text, text, text, text, text","in_extension_schema":true},{"kind":"f","name":"synchdb_add_extra_conninfo","result":"integer","strict":true,"language":"c","arguments":"name, name, text, text, text, text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, name, text, text, text, text","in_extension_schema":true},{"kind":"f","name":"synchdb_add_fdw_conninfo","result":"integer","strict":true,"language":"c","arguments":"name, text, text, text, text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, text, text, text, text","in_extension_schema":true},{"kind":"f","name":"synchdb_add_infinispan","result":"integer","strict":true,"language":"c","arguments":"name, name, integer","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, name, integer","in_extension_schema":true},{"kind":"f","name":"synchdb_add_jmx_conninfo","result":"integer","strict":true,"language":"c","arguments":"name, text, integer, text, integer, boolean, text, text, boolean, text, text, text, text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, text, integer, text, integer, boolean, text, text, boolean, text, text, text, text","in_extension_schema":true},{"kind":"f","name":"synchdb_add_jmx_exporter_conninfo","result":"integer","strict":true,"language":"c","arguments":"name, text, integer, text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, text, integer, text","in_extension_schema":true},{"kind":"f","name":"synchdb_add_objmap","result":"integer","strict":true,"language":"c","arguments":"name, name, name, text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, name, name, text","in_extension_schema":true},{"kind":"f","name":"synchdb_add_olr_conninfo","result":"integer","strict":true,"language":"c","arguments":"name, text, integer, text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, text, integer, text","in_extension_schema":true},{"kind":"f","name":"synchdb_apply_column_mappings","result":"void","strict":false,"language":"plpgsql","arguments":"p_src_schema name, p_connector_name name, p_desired_db name, p_desired_schema name DEFAULT NULL::name, p_case_strategy text DEFAULT 'asis'::text","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_src_schema name, p_connector_name name, p_desired_db name, p_desired_schema name, p_case_strategy text","in_extension_schema":true},{"kind":"f","name":"synchdb_apply_table_mappings","result":"void","strict":false,"language":"plpgsql","arguments":"p_src_schema name, p_connector_name name, p_desired_db name, p_desired_schema name DEFAULT NULL::name, p_case_strategy text DEFAULT 'asis'::text","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_src_schema name, p_connector_name name, p_desired_db name, p_desired_schema name, p_case_strategy text","in_extension_schema":true},{"kind":"f","name":"synchdb_create_current_binlog_pos_ft","result":"void","strict":false,"language":"plpgsql","arguments":"p_schema name, p_server name","volatility":"v","configuration":["search_path=pg_catalog"],"security_definer":false,"identity_arguments":"p_schema name, p_server name","in_extension_schema":true},{"kind":"f","name":"synchdb_create_current_lsn_ft","result":"void","strict":false,"language":"plpgsql","arguments":"p_schema name, p_server name","volatility":"v","configuration":["search_path=pg_catalog"],"security_definer":false,"identity_arguments":"p_schema name, p_server name","in_extension_schema":true},{"kind":"f","name":"synchdb_create_current_scn_ft","result":"void","strict":false,"language":"plpgsql","arguments":"p_schema name, p_server name","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_schema name, p_server name","in_extension_schema":true},{"kind":"f","name":"synchdb_create_mysql_objs","result":"void","strict":false,"language":"plpgsql","arguments":"server name, schema name DEFAULT 'public'::name, options jsonb DEFAULT NULL::jsonb","volatility":"v","configuration":["search_path=pg_catalog"],"security_definer":false,"identity_arguments":"server name, schema name, options jsonb","in_extension_schema":true},{"kind":"f","name":"synchdb_create_ora_stage_fts","result":"void","strict":false,"language":"plpgsql","arguments":"p_connector_name name, p_desired_db name, p_desired_schema name, p_stage_schema name DEFAULT 'ora_stage'::name, p_server_name name DEFAULT 'oracle'::name, p_lower_names boolean DEFAULT true, p_on_exists text DEFAULT 'replace'::text, p_offset text DEFAULT NULL::text, p_source_schema name DEFAULT 'ora_obj'::name, p_snapshot_tables text DEFAULT NULL::text, p_write_dbz_schema_info boolean DEFAULT false, p_case_strategy text DEFAULT 'asis'::text","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_connector_name name, p_desired_db name, p_desired_schema name, p_stage_schema name, p_server_name name, p_lower_names boolean, p_on_exists text, p_offset text, p_source_schema name, p_snapshot_tables text, p_write_dbz_schema_info boolean, p_case_strategy text","in_extension_schema":true},{"kind":"f","name":"synchdb_create_oraviews","result":"void","strict":false,"language":"plpgsql","arguments":"server name, schema name DEFAULT 'public'::name, options jsonb DEFAULT NULL::jsonb","volatility":"v","configuration":["search_path=pg_catalog"],"security_definer":false,"identity_arguments":"server name, schema name, options jsonb","in_extension_schema":true},{"kind":"f","name":"synchdb_create_pg_objs","result":"void","strict":false,"language":"plpgsql","arguments":"server name, schema name DEFAULT 'public'::name, options jsonb DEFAULT NULL::jsonb","volatility":"v","configuration":["search_path=pg_catalog"],"security_definer":false,"identity_arguments":"server name, schema name, options jsonb","in_extension_schema":true},{"kind":"f","name":"synchdb_del_conninfo","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_del_extra_conninfo","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_del_fdw_conninfo","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_del_infinispan","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_del_jmx_conninfo","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_del_jmx_exporter_conninfo","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_del_objmap","result":"integer","strict":true,"language":"c","arguments":"name, name, name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, name, name","in_extension_schema":true},{"kind":"f","name":"synchdb_del_olr_conninfo","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_do_initial_snapshot","result":"text","strict":false,"language":"plpgsql","arguments":"p_connector_name name, p_secret text, p_source_schema name, p_stage_schema name, p_dest_schema name, p_lookup_db text, p_lookup_schema name, p_lower_names boolean DEFAULT true, p_on_exists text DEFAULT 'replace'::text, p_offset text DEFAULT NULL::text, p_snapshot_tables text DEFAULT NULL::text, p_use_subtx boolean DEFAULT true, p_write_schema_hist boolean DEFAULT false, p_case_strategy text DEFAULT 'asis'::text","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_connector_name name, p_secret text, p_source_schema name, p_stage_schema name, p_dest_schema name, p_lookup_db text, p_lookup_schema name, p_lower_names boolean, p_on_exists text, p_offset text, p_snapshot_tables text, p_use_subtx boolean, p_write_schema_hist boolean, p_case_strategy text","in_extension_schema":true},{"kind":"f","name":"synchdb_do_schema_sync","result":"text","strict":false,"language":"plpgsql","arguments":"p_connector_name name, p_secret text, p_source_schema name, p_stage_schema name, p_dest_schema name, p_lookup_db text, p_lookup_schema name, p_lower_names boolean DEFAULT true, p_on_exists text DEFAULT 'replace'::text, p_offset text DEFAULT NULL::text, p_snapshot_tables text DEFAULT NULL::text, p_use_subtx boolean DEFAULT true, p_write_schema_hist boolean DEFAULT false, p_case_strategy text DEFAULT 'asis'::text","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_connector_name name, p_secret text, p_source_schema name, p_stage_schema name, p_dest_schema name, p_lookup_db text, p_lookup_schema name, p_lower_names boolean, p_on_exists text, p_offset text, p_snapshot_tables text, p_use_subtx boolean, p_write_schema_hist boolean, p_case_strategy text","in_extension_schema":true},{"kind":"f","name":"synchdb_finalize_initial_snapshot","result":"void","strict":false,"language":"plpgsql","arguments":"p_source_schema name, p_stage_schema name, p_connector_name name, p_meta_schema name","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_source_schema name, p_stage_schema name, p_connector_name name, p_meta_schema name","in_extension_schema":true},{"kind":"f","name":"synchdb_get_state","result":"SETOF record","strict":true,"language":"c","arguments":"","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"","in_extension_schema":true},{"kind":"f","name":"synchdb_get_stats","result":"SETOF record","strict":true,"language":"c","arguments":"","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"","in_extension_schema":true},{"kind":"f","name":"synchdb_log_jvm_meminfo","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_materialize_ora_metadata","result":"void","strict":false,"language":"plpgsql","arguments":"p_source_schema name, p_dest_schema name, p_on_exists text DEFAULT 'replace'::text","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_source_schema name, p_dest_schema name, p_on_exists text","in_extension_schema":true},{"kind":"f","name":"synchdb_materialize_schema","result":"void","strict":false,"language":"plpgsql","arguments":"p_connector_name name, p_src_schema name, p_dst_schema name, p_on_exists text DEFAULT 'skip'::text","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_connector_name name, p_src_schema name, p_dst_schema name, p_on_exists text","in_extension_schema":true},{"kind":"f","name":"synchdb_migrate_data_with_transforms","result":"void","strict":false,"language":"plpgsql","arguments":"p_src_schema name, p_connector_name name, p_dst_schema name, p_desired_db name, p_offset text, p_case_strategy text, p_desired_schema name DEFAULT NULL::name, p_do_truncate boolean DEFAULT false, p_rows_per_tick integer DEFAULT 0, p_continue_on_error boolean DEFAULT true, p_batch_subxact boolean DEFAULT true","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_src_schema name, p_connector_name name, p_dst_schema name, p_desired_db name, p_offset text, p_case_strategy text, p_desired_schema name, p_do_truncate boolean, p_rows_per_tick integer, p_continue_on_error boolean, p_batch_subxact boolean","in_extension_schema":true},{"kind":"f","name":"synchdb_migrate_data_with_transforms_nosubs","result":"void","strict":false,"language":"plpgsql","arguments":"p_src_schema name, p_connector_name name, p_dst_schema name, p_desired_db name, p_desired_schema name DEFAULT NULL::name, p_do_truncate boolean DEFAULT false, p_rows_per_tick integer DEFAULT 0","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_src_schema name, p_connector_name name, p_dst_schema name, p_desired_db name, p_desired_schema name, p_do_truncate boolean, p_rows_per_tick integer","in_extension_schema":true},{"kind":"f","name":"synchdb_migrate_primary_keys","result":"void","strict":false,"language":"plpgsql","arguments":"p_oraobj_schema name, p_dst_schema name, p_case_strategy text DEFAULT 'asis'::text","volatility":"v","configuration":[],"security_definer":false,"identity_arguments":"p_oraobj_schema name, p_dst_schema name, p_case_strategy text","in_extension_schema":true},{"kind":"f","name":"synchdb_pause_engine","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_prepare_initial_snapshot","result":"text","strict":false,"language":"plpgsql","arguments":"p_connector_name name, p_master_key text DEFAULT NULL::text","volatility":"v","configuration":[],"security_definer":true,"identity_arguments":"p_connector_name name, p_master_key text","in_extension_schema":true},{"kind":"f","name":"synchdb_reload_objmap","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_reset_stats","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_restart_connector","result":"integer","strict":true,"language":"c","arguments":"name, name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, name","in_extension_schema":true},{"kind":"f","name":"synchdb_resume_engine","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_set_dbz_loglevel","result":"integer","strict":true,"language":"c","arguments":"name, text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, text","in_extension_schema":true},{"kind":"f","name":"synchdb_set_offset","result":"integer","strict":true,"language":"c","arguments":"name, text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, text","in_extension_schema":true},{"kind":"f","name":"synchdb_set_snapstats","result":"void","strict":true,"language":"c","arguments":"name, bigint, bigint, bigint, bigint","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, bigint, bigint, bigint, bigint","in_extension_schema":true},{"kind":"f","name":"synchdb_start_engine_bgw","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_start_engine_bgw","result":"integer","strict":true,"language":"c","arguments":"name, name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, name","in_extension_schema":true},{"kind":"f","name":"synchdb_stop_engine_bgw","result":"integer","strict":true,"language":"c","arguments":"name","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name","in_extension_schema":true},{"kind":"f","name":"synchdb_translate_datatype","result":"text","strict":true,"language":"c","arguments":"name, name, bigint, bigint, bigint","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"name, name, bigint, bigint, bigint","in_extension_schema":true},{"kind":"f","name":"synchdb_type_to_jdbc","result":"integer","strict":true,"language":"plpgsql","arguments":"p_connector text, p_type_text text","volatility":"i","configuration":[],"security_definer":false,"identity_arguments":"p_connector text, p_type_text text","in_extension_schema":true}],"relations":[{"kind":"v","name":"synchdb_att_view","columns":[{"name":"name","type":"name","default":null,"not_null":false},{"name":"type","type":"name","default":null,"not_null":false},{"name":"attnum","type":"smallint","default":null,"not_null":false},{"name":"ext_tbname","type":"name","default":null,"not_null":false},{"name":"pg_tbname","type":"text","default":null,"not_null":false},{"name":"ext_attname","type":"name","default":null,"not_null":false},{"name":"pg_attname","type":"name","default":null,"not_null":false},{"name":"ext_atttypename","type":"name","default":null,"not_null":false},{"name":"pg_atttypename","type":"text","default":null,"not_null":false},{"name":"transform","type":"text","default":null,"not_null":false}],"constraints":[],"in_extension_schema":true},{"kind":"r","name":"synchdb_attribute","columns":[{"name":"name","type":"name","default":null,"not_null":true},{"name":"type","type":"name","default":null,"not_null":true},{"name":"attrelid","type":"oid","default":null,"not_null":true},{"name":"attnum","type":"smallint","default":null,"not_null":true},{"name":"ext_tbname","type":"name","default":null,"not_null":false},{"name":"ext_attname","type":"name","default":null,"not_null":false},{"name":"ext_atttypename","type":"name","default":null,"not_null":false}],"constraints":[{"name":"synchdb_attribute_pkey","definition":"PRIMARY KEY (name, type, attrelid, attnum)"}],"in_extension_schema":true},{"kind":"v","name":"synchdb_cdcstats","columns":[{"name":"name","type":"text","default":null,"not_null":false},{"name":"ddls","type":"bigint","default":null,"not_null":false},{"name":"dmls","type":"bigint","default":null,"not_null":false},{"name":"creates","type":"bigint","default":null,"not_null":false},{"name":"updates","type":"bigint","default":null,"not_null":false},{"name":"deletes","type":"bigint","default":null,"not_null":false},{"name":"txs","type":"bigint","default":null,"not_null":false},{"name":"truncates","type":"bigint","default":null,"not_null":false}],"constraints":[],"in_extension_schema":true},{"kind":"r","name":"synchdb_conninfo","columns":[{"name":"name","type":"text","default":null,"not_null":true},{"name":"isactive","type":"boolean","default":null,"not_null":false},{"name":"data","type":"jsonb","default":null,"not_null":false}],"constraints":[{"name":"synchdb_conninfo_pkey","definition":"PRIMARY KEY (name)"}],"in_extension_schema":true},{"kind":"v","name":"synchdb_genstats","columns":[{"name":"name","type":"text","default":null,"not_null":false},{"name":"bad_events","type":"bigint","default":null,"not_null":false},{"name":"total_events","type":"bigint","default":null,"not_null":false},{"name":"batches_done","type":"bigint","default":null,"not_null":false},{"name":"average_batch_size","type":"bigint","default":null,"not_null":false},{"name":"first_src_ts","type":"bigint","default":null,"not_null":false},{"name":"first_pg_ts","type":"bigint","default":null,"not_null":false},{"name":"last_src_ts","type":"bigint","default":null,"not_null":false},{"name":"last_pg_ts","type":"bigint","default":null,"not_null":false}],"constraints":[],"in_extension_schema":true},{"kind":"r","name":"synchdb_objmap","columns":[{"name":"name","type":"name","default":null,"not_null":true},{"name":"objtype","type":"name","default":null,"not_null":true},{"name":"enabled","type":"boolean","default":null,"not_null":false},{"name":"srcobj","type":"name","default":null,"not_null":true},{"name":"dstobj","type":"text","default":null,"not_null":false}],"constraints":[{"name":"synchdb_objmap_pkey","definition":"PRIMARY KEY (name, objtype, srcobj)"}],"in_extension_schema":true},{"kind":"v","name":"synchdb_snapstats","columns":[{"name":"name","type":"text","default":null,"not_null":false},{"name":"tables","type":"bigint","default":null,"not_null":false},{"name":"rows","type":"bigint","default":null,"not_null":false},{"name":"snapshot_begin_ts","type":"bigint","default":null,"not_null":false},{"name":"snapshot_end_ts","type":"bigint","default":null,"not_null":false}],"constraints":[],"in_extension_schema":true},{"kind":"v","name":"synchdb_state_view","columns":[{"name":"name","type":"text","default":null,"not_null":false},{"name":"connector_type","type":"text","default":null,"not_null":false},{"name":"pid","type":"integer","default":null,"not_null":false},{"name":"stage","type":"text","default":null,"not_null":false},{"name":"state","type":"text","default":null,"not_null":false},{"name":"err","type":"text","default":null,"not_null":false},{"name":"last_dbz_offset","type":"text","default":null,"not_null":false}],"constraints":[],"in_extension_schema":true}]}$synchdb_v14_profile$::jsonb THEN
  RAISE EXCEPTION 'Upgrade expects tag v1.4 (SQL 1.0), with its exact 53-function and catalog profile'
   USING ERRCODE='55000',HINT='Run the bundled preflight and compare its catalog_profile with baseline-v1.4.json; do not bypass the guard.';
 END IF;
 EXECUTE format('LOCK TABLE %I.synchdb_conninfo IN ACCESS EXCLUSIVE MODE',v_schema);
 EXECUTE format('SELECT coalesce(bool_or(isactive),false) FROM %I.synchdb_conninfo',v_schema) INTO v_active;
 IF v_active THEN
  RAISE EXCEPTION 'Stop all legacy connectors before catalog upgrade' USING ERRCODE='55000';
 END IF;
 -- RESTRICT preserves outside dependencies: review any refusal instead of CASCADE.
 EXECUTE format('DROP FUNCTION %I.synchdb_prepare_initial_snapshot(name,text) RESTRICT',v_schema);
 EXECUTE format('DROP FUNCTION %I.synchdb_do_initial_snapshot(name,text,name,name,name,text,name,boolean,text,text,text,boolean,boolean,text) RESTRICT',v_schema);
 EXECUTE format('DROP FUNCTION %I.synchdb_do_schema_sync(name,text,name,name,name,text,name,boolean,text,text,text,boolean,boolean,text) RESTRICT',v_schema);
 PERFORM pg_catalog.set_config('search_path',v_search_path,true);
END;
$synchdb_upgrade_guard$;


-- Source: src/sql/core.sql; sha256=31874be8afff0ab835c82adbae8d958f56624bc4608f18c209faf6e1568ca2e2

 
CREATE OR REPLACE FUNCTION synchdb_start_engine_bgw(name) RETURNS int
AS '$libdir/synchdb', 'synchdb_start_engine_bgw'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_start_engine_bgw(name, name) RETURNS int
AS '$libdir/synchdb', 'synchdb_start_engine_bgw_snapshot_mode'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_stop_engine_bgw(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_get_state() RETURNS SETOF record
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE VIEW synchdb_state_view AS SELECT * FROM synchdb_get_state() AS (name text, connector_type text, pid int, stage text, state text, err text, last_dbz_offset text);

CREATE OR REPLACE FUNCTION synchdb_pause_engine(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_resume_engine(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_set_offset(name, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_conninfo(name, text, int, text, text, text, text, text, text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_restart_connector(name, name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_log_jvm_meminfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_set_dbz_loglevel(name, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_get_stats() RETURNS SETOF record
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_reset_stats(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE VIEW synchdb_genstats AS
SELECT
  name,
  bad_events,
  total_events,
  batches_done,
  average_batch_size,
  first_src_ts,
  first_pg_ts,
  last_src_ts,
  last_pg_ts
FROM synchdb_get_stats() AS (
  name               text,
  ddls               bigint,
  dmls               bigint,
  creates            bigint,
  updates            bigint,
  deletes            bigint,
  txs                bigint,
  truncates          bigint,
  bad_events         bigint,
  total_events       bigint,
  batches_done       bigint,
  average_batch_size bigint,
  first_src_ts       bigint,
  first_pg_ts        bigint,
  last_src_ts        bigint,
  last_pg_ts         bigint,
  tables             bigint,
  rows               bigint,
  snapshot_begin_ts  bigint,
  snapshot_end_ts    bigint
);

CREATE OR REPLACE VIEW synchdb_snapstats AS
SELECT
  name,
  tables,
  rows,
  snapshot_begin_ts,
  snapshot_end_ts
FROM synchdb_get_stats() AS (
  name               text,
  ddls               bigint,
  dmls               bigint,
  creates            bigint,
  updates            bigint,
  deletes            bigint,
  txs                bigint,
  truncates          bigint,
  bad_events         bigint,
  total_events       bigint,
  batches_done       bigint,
  average_batch_size bigint,
  first_src_ts       bigint,
  first_pg_ts        bigint,
  last_src_ts        bigint,
  last_pg_ts         bigint,
  tables             bigint,
  rows               bigint,
  snapshot_begin_ts  bigint,
  snapshot_end_ts    bigint
);

CREATE OR REPLACE VIEW synchdb_cdcstats AS
SELECT
  name,
  ddls,
  dmls,
  creates,
  updates,
  deletes,
  txs,
  truncates
FROM synchdb_get_stats() AS (
  name               text,
  ddls               bigint,
  dmls               bigint,
  creates            bigint,
  updates            bigint,
  deletes            bigint,
  txs                bigint,
  truncates          bigint,
  bad_events         bigint,
  total_events       bigint,
  batches_done       bigint,
  average_batch_size bigint,
  first_src_ts       bigint,
  first_pg_ts        bigint,
  last_src_ts        bigint,
  last_pg_ts         bigint,
  tables             bigint,
  rows               bigint,
  snapshot_begin_ts  bigint,
  snapshot_end_ts    bigint
);

CREATE TABLE IF NOT EXISTS synchdb_conninfo(name TEXT PRIMARY KEY, isactive BOOL, data JSONB);

CREATE TABLE IF NOT EXISTS synchdb_attribute (
    name name,
    type name,
    attrelid oid,
    attnum smallint,
    ext_tbname text COLLATE pg_catalog."C",
    ext_attname text COLLATE pg_catalog."C",
    ext_atttypename text COLLATE pg_catalog."C",
    PRIMARY KEY (name, type, attrelid, attnum)
);

CREATE OR REPLACE FUNCTION synchdb_add_objmap(name, name, name, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_objmap(name, name, text, text) RETURNS int
AS '$libdir/synchdb', 'synchdb_add_objmap_text'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_reload_objmap(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE TABLE IF NOT EXISTS synchdb_objmap (
    name name,
    objtype name,
    enabled bool,
    srcobj text COLLATE pg_catalog."C",
    dstobj text,
    PRIMARY KEY (name, objtype, srcobj)
);

CREATE OR REPLACE FUNCTION synchdb_add_extra_conninfo(name, name, text, text, text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_extra_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_fdw_conninfo(name, text, text, text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_fdw_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_objmap(name, name, name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_objmap(name, name, text) RETURNS int
AS '$libdir/synchdb', 'synchdb_del_objmap_text'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_jmx_conninfo(name, text, int, text, int, bool, text, text, bool, text, text, text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_jmx_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_jmx_exporter_conninfo(name, text, int, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_jmx_exporter_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_olr_conninfo(name, text, int, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_olr_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_infinispan(name, name, int) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_infinispan(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_translate_datatype(name, name, bigint, bigint, bigint) RETURNS text
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_set_snapstats(name, bigint, bigint, bigint, bigint) RETURNS void
AS '$libdir/synchdb'
LANGUAGE C VOLATILE STRICT;


-- Source: src/sql/catalog_connector.sql; sha256=672dbe57db0e710bfa54a8c4a876a482a54391de2984085aecadd1f955a57c7e

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


-- Source: src/sql/catalog_credentials.sql; sha256=a072e5f1ff3aa2d37d33e435311c0d30f0c668d0a8fd501dea3aa3f96a2410bb

-- The built-in 1.0 key is held by the credential context. These functions
-- are invoker APIs; table SELECT/RLS and pgcrypto ACLs continue to apply.
CREATE OR REPLACE FUNCTION synchdb_connector_credential(pg_catalog.text,pg_catalog.text)
RETURNS pg_catalog.text
AS '$libdir/synchdb','synchdb_connector_credential'
LANGUAGE C VOLATILE STRICT SECURITY INVOKER;
CREATE OR REPLACE FUNCTION synchdb_reencrypt_credentials()
RETURNS pg_catalog.int8
AS '$libdir/synchdb','synchdb_reencrypt_credentials'
LANGUAGE C VOLATILE SECURITY INVOKER;
REVOKE ALL ON FUNCTION synchdb_connector_credential(pg_catalog.text,pg_catalog.text),
    synchdb_reencrypt_credentials() FROM PUBLIC;


-- Source: src/sql/catalog_identity.sql; sha256=cad26016b144c1cbb7c519174670b6d63627c72a7be568f14f9d725c52003a22

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
        AS '$libdir/synchdb', 'synchdb_add_objmap_text' LANGUAGE C VOLATILE STRICT
    $api$, v_schema);
    EXECUTE pg_catalog.format($api$
        CREATE OR REPLACE FUNCTION %I.synchdb_del_objmap(name,name,text) RETURNS int
        AS '$libdir/synchdb', 'synchdb_del_objmap_text' LANGUAGE C VOLATILE STRICT
    $api$, v_schema);
    PERFORM pg_catalog.set_config('search_path', v_old_path, true);
END;
$synchdb_catalog_identity$;


-- Source: src/sql/catalog_bootstrap.sql; sha256=4b348e2973ee9f088330f025fb03335bb5c3c97c42bae40ad4198455447046b9

-- Local target ownership is committed in the same transaction as publication.
-- This is not an engine offset, an artifact generation, or a durable handoff.
CREATE TABLE IF NOT EXISTS synchdb_bootstrap_targets (
    target_oid       pg_catalog.oid PRIMARY KEY,
    connector_id     pg_catalog.uuid NOT NULL,
    generation       pg_catalog.uuid NOT NULL,
    source_identity  pg_catalog.jsonb NOT NULL,
    target_schema    pg_catalog.text COLLATE pg_catalog."C" NOT NULL,
    target_name      pg_catalog.text COLLATE pg_catalog."C" NOT NULL,
    target_owner     pg_catalog.oid NOT NULL,
    descriptor       pg_catalog.jsonb NOT NULL,
    descriptor_hash  pg_catalog.text NOT NULL,
    plan             pg_catalog.jsonb NOT NULL,
    published_at     pg_catalog.timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CHECK (pg_catalog.jsonb_typeof(source_identity) = 'object'),
    CHECK (pg_catalog.jsonb_typeof(descriptor) = 'object'),
    CHECK (pg_catalog.jsonb_typeof(plan) = 'object'),
    CHECK (connector_id <> '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid)
);

-- A committed local outcome remains inspectable after its worker exits.  This
-- journal contains no engine-owned artifact/offset ownership transition.
CREATE TABLE IF NOT EXISTS synchdb_bootstrap_runs (
    generation       pg_catalog.uuid PRIMARY KEY,
    connector_id     pg_catalog.uuid NOT NULL,
    config_revision  pg_catalog.int8 NOT NULL CHECK (config_revision > 0),
    result           pg_catalog.jsonb NOT NULL CHECK (pg_catalog.jsonb_typeof(result)='object'),
    finished_at      pg_catalog.timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp()
);

DO $synchdb_bootstrap_catalog$
DECLARE
    v_extension pg_catalog.oid;
    v_namespace pg_catalog.oid;
    v_relation pg_catalog.oid;
    v_table pg_catalog.text;
BEGIN
    SELECT e.oid, e.extnamespace INTO STRICT v_extension, v_namespace
      FROM pg_catalog.pg_extension e WHERE e.extname = 'synchdb';
    FOREACH v_table IN ARRAY ARRAY['synchdb_bootstrap_targets','synchdb_bootstrap_runs'] LOOP
    SELECT c.oid INTO STRICT v_relation FROM pg_catalog.pg_class c
     WHERE c.relnamespace = v_namespace AND c.relname = v_table
       AND c.relkind = 'r';
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                    WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                      AND d.objid = v_relation AND d.objsubid = 0
                      AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                      AND d.refobjid = v_extension AND d.deptype = 'e') THEN
        RAISE EXCEPTION 'Bootstrap target ownership catalog must belong to synchdb'
            USING ERRCODE = '55000';
    END IF;
    END LOOP;
END;
$synchdb_bootstrap_catalog$;

REVOKE ALL ON synchdb_bootstrap_targets FROM PUBLIC;

REVOKE ALL ON synchdb_bootstrap_runs FROM PUBLIC;


-- Source: src/sql/catalog_control.sql; sha256=31403a7df477d981d3afc8cc274f9f7129dd5edc028e052d12a136a5633e1dc6

-- Persistent receipts, desired state, and process-independent recovery metadata.
CREATE OR REPLACE FUNCTION synchdb_runtime_admitted(bigint) RETURNS boolean
AS '$libdir/synchdb','synchdb_runtime_admitted' LANGUAGE C VOLATILE STRICT;

DO $control_catalog$
DECLARE
    ext oid; ns oid; schema_name name; r record; relation_oid oid;
    guard_oid oid; existing_trigger record;
    old_path text := pg_catalog.current_setting('search_path');
BEGIN
    PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
    SELECT e.oid,e.extnamespace,n.nspname INTO STRICT ext,ns,schema_name
      FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace
     WHERE e.extname='synchdb';
    FOR r IN SELECT * FROM (VALUES
      ('synchdb_control_state', $definition$
        connector_id uuid PRIMARY KEY,
        connector_name name NOT NULL,
        config_revision bigint NOT NULL CHECK(config_revision>0),
        execution_role oid NOT NULL,
        runtime_incarnation bigint NOT NULL DEFAULT 0,
        desired_state text NOT NULL CHECK(desired_state IN ('RUNNING','PAUSED','STOPPED')),
        snapshot_mode text NOT NULL,
        snapshot_engine integer NOT NULL,
        schema_sync boolean NOT NULL,
        oracle_compatible boolean NOT NULL,
        run_options jsonb NOT NULL,
        retry_enabled boolean NOT NULL DEFAULT false,
        retry_attempt integer NOT NULL DEFAULT 0,
        retry_at timestamptz,
        actual_state text NOT NULL DEFAULT 'STOPPED',
        last_operation bigint NOT NULL,
        last_error text,
        last_sqlstate text,
        updated_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp()
      $definition$),
      ('synchdb_control_commands', $definition$
        operation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        connector_id uuid NOT NULL,
        requested_by oid NOT NULL,
        operation text NOT NULL CHECK(operation IN
          ('START','STOP','PAUSE','RESUME','RESTART','RELOAD','MEMDUMP','LOGLEVEL','OFFSET','RESET_STATS')),
        status text NOT NULL DEFAULT 'PENDING' CHECK(status IN
          ('PENDING','EXECUTING','SUCCEEDED','FAILED','CANCELLED')),
        config_revision bigint NOT NULL,
        payload text,
        controller_generation bigint,
        request_id bigint,
        run_generation bigint,
        sqlstate text,
        message text,
        created_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
        started_at timestamptz,
        finished_at timestamptz
      $definition$)
    ) x(relation_name, definition)
    LOOP
        SELECT c.oid INTO relation_oid FROM pg_catalog.pg_class c
         WHERE c.relnamespace=ns AND c.relname=r.relation_name;
        IF relation_oid IS NULL THEN
            EXECUTE pg_catalog.format('CREATE TABLE %I.%I (%s)',schema_name,r.relation_name,r.definition);
        ELSIF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_depend d
          WHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass AND d.objid=relation_oid
            AND d.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass
            AND d.refobjid=ext AND d.deptype='e') THEN
            RAISE EXCEPTION 'Control catalog % is not an extension member',r.relation_name USING ERRCODE='55000';
        END IF;
        EXECUTE pg_catalog.format('REVOKE ALL ON TABLE %I.%I FROM PUBLIC',schema_name,r.relation_name);
    END LOOP;
    EXECUTE pg_catalog.format('CREATE UNIQUE INDEX IF NOT EXISTS synchdb_control_one_pending '
      'ON %I.synchdb_control_commands(connector_id) WHERE status IN (''PENDING'',''EXECUTING'')',schema_name);
    -- The invoker APIs write only these two ledgers in a short trusted C
    -- scope. A table grant must never become permission to forge a worker's
    -- execution role, its accepted options, command identity or receipt.
    SELECT p.oid INTO guard_oid FROM pg_catalog.pg_proc p
     WHERE p.pronamespace=ns AND p.proname='synchdb_control_ledger_guard' AND p.pronargs=0;
    IF guard_oid IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM pg_catalog.pg_depend d
       WHERE d.classid='pg_catalog.pg_proc'::pg_catalog.regclass AND d.objid=guard_oid
         AND d.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass
         AND d.refobjid=ext AND d.deptype='e') THEN
        RAISE EXCEPTION 'Control ledger guard must belong to the synchdb extension'
            USING ERRCODE='55000';
    END IF;
    IF guard_oid IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM pg_catalog.pg_proc p WHERE p.oid=guard_oid
       AND p.prokind='f' AND p.prorettype='pg_catalog.trigger'::pg_catalog.regtype
       AND NOT p.proretset) THEN
        RAISE EXCEPTION 'Unexpected control ledger guard function signature'
            USING ERRCODE='55000';
    END IF;
    EXECUTE pg_catalog.format($function$
      CREATE OR REPLACE FUNCTION %I.synchdb_control_ledger_guard() RETURNS pg_catalog.trigger
      LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog
      AS $guard$
      BEGIN
          IF TG_LEVEL <> 'STATEMENT' OR TG_WHEN <> 'BEFORE'
             OR TG_OP NOT IN ('INSERT','UPDATE','DELETE','TRUNCATE') THEN
              RAISE EXCEPTION 'Control ledger guard requires a BEFORE statement trigger'
                  USING ERRCODE='0A000';
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                          WHERE rolname=CURRENT_USER AND rolsuper) THEN
              RAISE EXCEPTION 'Control ledgers can only be modified by the trusted control API or a superuser'
                  USING ERRCODE='42501';
          END IF;
          RETURN NULL;
      END;
      $guard$
    $function$,schema_name);
    SELECT p.oid INTO STRICT guard_oid FROM pg_catalog.pg_proc p
     WHERE p.pronamespace=ns AND p.proname='synchdb_control_ledger_guard' AND p.pronargs=0;
    FOR r IN SELECT relation_name FROM (VALUES
      ('synchdb_control_state'),('synchdb_control_commands')) x(relation_name)
    LOOP
        SELECT c.oid INTO STRICT relation_oid FROM pg_catalog.pg_class c
         WHERE c.relnamespace=ns AND c.relname=r.relation_name;
        SELECT t.* INTO existing_trigger FROM pg_catalog.pg_trigger t
         WHERE t.tgrelid=relation_oid AND t.tgname='synchdb_control_ledger_guard';
        IF FOUND AND (existing_trigger.tgfoid <> guard_oid OR existing_trigger.tgtype <> 62
          OR existing_trigger.tgisinternal OR existing_trigger.tgconstraint <> 0
          OR existing_trigger.tgdeferrable OR existing_trigger.tginitdeferred
          OR existing_trigger.tgnargs <> 0 OR existing_trigger.tgattr::pg_catalog.text <> ''
          OR existing_trigger.tgqual IS NOT NULL OR existing_trigger.tgparentid <> 0
          OR existing_trigger.tgoldtable IS NOT NULL OR existing_trigger.tgnewtable IS NOT NULL) THEN
            RAISE EXCEPTION 'Unexpected control ledger guard trigger definition'
                USING ERRCODE='55000';
        END IF;
        EXECUTE pg_catalog.format('CREATE OR REPLACE TRIGGER synchdb_control_ledger_guard '
          'BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %I.%I '
          'FOR EACH STATEMENT EXECUTE FUNCTION %I.synchdb_control_ledger_guard()',
          schema_name,r.relation_name,schema_name);
        EXECUTE pg_catalog.format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER synchdb_control_ledger_guard',
          schema_name,r.relation_name);
    END LOOP;
    -- Include never-started configurations. A worker PID is not an intent/status substitute.
    EXECUTE pg_catalog.format($view$
      CREATE OR REPLACE VIEW %1$I.synchdb_control_view WITH (security_invoker=true) AS
      SELECT c.name,c.connector_id,c.config_revision AS current_revision,
             s.config_revision AS accepted_revision,s.execution_role,s.desired_state,
             CASE WHEN s.runtime_incarnation IS NOT NULL
                       AND NOT %1$I.synchdb_runtime_admitted(s.runtime_incarnation)
                  THEN 'STOPPED' ELSE coalesce(s.actual_state,'STOPPED') END
               AS actual_state,s.retry_attempt,s.retry_at,
             s.last_operation,s.last_error,s.last_sqlstate,s.updated_at
        FROM %1$I.synchdb_conninfo c
        LEFT JOIN %1$I.synchdb_control_state s USING(connector_id)
    $view$,schema_name);
    PERFORM pg_catalog.set_config('search_path',old_path,true);
END
$control_catalog$;

CREATE OR REPLACE FUNCTION synchdb_control(name,text,text DEFAULT NULL) RETURNS bigint
AS '$libdir/synchdb','synchdb_control' LANGUAGE C VOLATILE;
CREATE OR REPLACE FUNCTION synchdb_operation(bigint,integer DEFAULT 0) RETURNS jsonb
AS '$libdir/synchdb','synchdb_operation' LANGUAGE C VOLATILE;


-- Source: src/sql/catalog_artifact.sql; sha256=595eae379170d0614e007546a70f148afeb713dd9142ba8643dbb7d757de6bfb

-- Native engine state survives worker restarts and connector configuration
-- deletion. UUID generations are separate from transient runtime attempts.
CREATE TABLE IF NOT EXISTS synchdb_artifacts (
    database_oid pg_catalog.oid NOT NULL,
    connector_id pg_catalog.uuid NOT NULL,
    generation pg_catalog.uuid NOT NULL,
    config_revision pg_catalog.int8 NOT NULL CHECK (config_revision > 0),
    source_identity pg_catalog.jsonb NOT NULL CHECK (pg_catalog.jsonb_typeof(source_identity) = 'object'),
    phase pg_catalog.text NOT NULL CHECK (phase IN ('ENGINE_OWNED','PURGE_REQUESTED','PURGING','PURGE_FAILED','PURGED')),
    active pg_catalog.bool NOT NULL,
    origin pg_catalog.text NOT NULL CHECK (origin IN ('NATIVE','LEGACY_COPY','FDW_SEED')),
    format_version pg_catalog.int4 NOT NULL DEFAULT 1 CHECK (format_version = 1),
    created_at pg_catalog.timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    updated_at pg_catalog.timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    last_error pg_catalog.text,
    last_sqlstate pg_catalog.text,
    PRIMARY KEY (database_oid, connector_id, generation),
    CHECK (NOT active OR phase = 'ENGINE_OWNED'),
    CHECK (connector_id <> '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid),
    CHECK (generation <> '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid)
);
CREATE UNIQUE INDEX IF NOT EXISTS synchdb_artifacts_active
    ON synchdb_artifacts(database_oid,connector_id) WHERE active;
CREATE INDEX IF NOT EXISTS synchdb_artifacts_purge_pending
    ON synchdb_artifacts(database_oid,created_at)
    WHERE phase IN ('PURGE_REQUESTED','PURGING');

DO $synchdb_artifact_catalog$
DECLARE
    v_extension pg_catalog.oid;
    v_namespace pg_catalog.oid;
    v_relation pg_catalog.oid;
BEGIN
    SELECT e.oid,e.extnamespace INTO STRICT v_extension,v_namespace
      FROM pg_catalog.pg_extension e WHERE e.extname='synchdb';
    SELECT c.oid INTO STRICT v_relation FROM pg_catalog.pg_class c
     WHERE c.relnamespace=v_namespace AND c.relname='synchdb_artifacts' AND c.relkind='r';
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                    WHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass
                      AND d.objid=v_relation AND d.objsubid=0
                      AND d.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass
                      AND d.refobjid=v_extension AND d.deptype='e') THEN
        RAISE EXCEPTION 'Artifact catalog must belong to synchdb' USING ERRCODE='55000';
    END IF;
END;
$synchdb_artifact_catalog$;
REVOKE ALL ON synchdb_artifacts FROM PUBLIC;

-- The replay coordinate commits in the same WAL transaction as source data.
-- Zero snapshot_scn is PREPARED, except for explicitly imported legacy OLR
-- cursors whose unverified baseline has no native snapshot boundary.
CREATE TABLE synchdb_olr_positions (
    database_oid pg_catalog.oid NOT NULL,
    connector_id pg_catalog.uuid NOT NULL,
    generation pg_catalog.uuid NOT NULL,
    source_proof pg_catalog.jsonb NOT NULL,
    snapshot_scn pg_catalog.int8 NOT NULL CHECK (snapshot_scn >= 0),
    scn pg_catalog.int8 NOT NULL CHECK (scn >= 0),
    c_scn pg_catalog.int8 NOT NULL CHECK (c_scn >= 0),
    c_idx pg_catalog.int8 NOT NULL CHECK (c_idx >= 0),
    PRIMARY KEY (database_oid,connector_id,generation),
    FOREIGN KEY (database_oid,connector_id,generation)
        REFERENCES synchdb_artifacts(database_oid,connector_id,generation)
);
REVOKE ALL ON synchdb_olr_positions FROM PUBLIC;

CREATE OR REPLACE FUNCTION synchdb_artifact_manifest(pg_catalog.uuid) RETURNS pg_catalog.jsonb
AS '$libdir/synchdb','synchdb_artifact_manifest' LANGUAGE C VOLATILE STRICT;
CREATE OR REPLACE FUNCTION synchdb_artifact_reset(pg_catalog.uuid) RETURNS pg_catalog.int4
AS '$libdir/synchdb','synchdb_artifact_reset' LANGUAGE C VOLATILE STRICT;
CREATE OR REPLACE FUNCTION synchdb_artifact_request_purge(pg_catalog.uuid) RETURNS pg_catalog.int4
AS '$libdir/synchdb','synchdb_artifact_request_purge' LANGUAGE C VOLATILE STRICT;
CREATE OR REPLACE FUNCTION synchdb_artifact_migrate_legacy(pg_catalog.name) RETURNS pg_catalog.uuid
AS '$libdir/synchdb','synchdb_artifact_migrate_legacy' LANGUAGE C VOLATILE STRICT;
CREATE OR REPLACE FUNCTION synchdb_artifact_migrate_legacy(pg_catalog.name,pg_catalog.jsonb) RETURNS pg_catalog.uuid
AS '$libdir/synchdb','synchdb_artifact_migrate_legacy' LANGUAGE C VOLATILE STRICT;
REVOKE ALL ON FUNCTION synchdb_artifact_manifest(pg_catalog.uuid),
    synchdb_artifact_reset(pg_catalog.uuid),synchdb_artifact_request_purge(pg_catalog.uuid),
    synchdb_artifact_migrate_legacy(pg_catalog.name),
    synchdb_artifact_migrate_legacy(pg_catalog.name,pg_catalog.jsonb) FROM PUBLIC;


-- Source: src/sql/catalog_views.sql; sha256=329dee3f75cee450ddb84ed91a7e02ac13354c7ec909d2622eac1f2ed2ae65ed

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


-- Source: src/sql/fdw/prepare.sql; sha256=d56c566ef82646c62b6b30e2cf9740e68649bfd4169bcf0d8eea0567c7a116d4

CREATE OR REPLACE FUNCTION read_snapshot_table_list(
    file_uri    text,
    p_conn_type  text,
    p_desired_db text
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    file_path    text;
    file_content text;
    json_data    jsonb;
    table_list   text;
    v_is_mysql   boolean;
BEGIN
    v_is_mysql := (lower(coalesce(p_conn_type, '')) = 'mysql');

    -- strip the leading "file:" prefix
    IF position('file:' IN file_uri) = 1 THEN
        file_path := substr(file_uri, 6);
    ELSE
        RAISE EXCEPTION 'Invalid file URI format: % (must start with file:)', file_uri;
    END IF;

    -- read the file content
    file_content := pg_read_file(file_path);

    -- parse it as JSONB
    json_data := file_content::jsonb;

    IF jsonb_typeof(json_data->'snapshot_table_list') IS DISTINCT FROM 'array' THEN
      RAISE EXCEPTION 'Snapshot table file must contain a snapshot_table_list array';
    END IF;
    IF jsonb_array_length(json_data->'snapshot_table_list') = 0 OR EXISTS (
      SELECT 1 FROM jsonb_array_elements(json_data->'snapshot_table_list') e
       WHERE jsonb_typeof(e) <> 'string' OR btrim(e #>> '{}') = ''
    ) THEN
      RAISE EXCEPTION 'Snapshot table file must contain nonempty table identifiers';
    END IF;

    -- Build comma-separated list, normalizing entries:
    --   mysql: db.table
    --   non-mysql: schema.table => db.schema.table
    SELECT string_agg(norm, ',')
      INTO table_list
    FROM (
      SELECT
        CASE
          WHEN v_is_mysql THEN x
          ELSE
            CASE
              WHEN array_length(regexp_split_to_array(trim(x), '\.'), 1) = 2
              THEN format('%s.%s', p_desired_db, trim(x))  -- prepend db.
              ELSE trim(x)  -- keep 3-part (or anything else) as-is
            END
        END AS norm
      FROM jsonb_array_elements_text(json_data->'snapshot_table_list') AS t(x)
      WHERE trim(x) <> ''
    ) s;

    RETURN table_list;
END;
$$;

/* added for oracle_fdw based initial snapshot */

CREATE OR REPLACE FUNCTION synchdb_prepare_initial_snapshot(
    p_connector_name text,
    p_server_name    name
)
RETURNS text
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_extension_schema name;
	v_connector   text;      -- 'oracle' | 'olr' | 'mysql' | 'postgres' | 'sqlserver' (lowercased)
    v_hostname    text;
    v_port        int;
    v_srcdb       text;   -- from data->>'srcdb'
    v_service     text;   -- v_srcdb or Oracle PDB name
    v_user        text;
    v_pwd         text;   -- decrypted password
    v_server      text;
    v_dbserver    text;   -- oracle_fdw "dbserver" option, e.g. //host:1521/SERVICE
    v_ssl_mode    text;   -- ssl_mode for postgres_fdw (from extra conninfo)
    v_ssl_cert    text;   -- FDW client certificate file path
    v_ssl_key     text;   -- FDW client private key file path
    v_ssl_rootcert text;  -- FDW CA cert path; for oracle_fdw: Oracle Wallet directory
    v_ssl_cipher  text;   -- SSL cipher list (mysql_fdw only)
BEGIN
    SELECT n.nspname INTO STRICT v_extension_schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'synchdb';
    PERFORM pg_catalog.set_config(
        'search_path',
        pg_catalog.format('pg_catalog, %I, pg_temp', v_extension_schema),
        true);
    ----------------------------------------------------------------------
    -- 0) Fetch connector type. Credential keys stay inside the C provider.
    ----------------------------------------------------------------------
    SELECT lower(data->>'connector')
      INTO v_connector
    FROM synchdb_conninfo
    WHERE name = p_connector_name;
	
	IF v_connector IS NULL OR v_connector = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data->>connector is missing/empty', p_connector_name;
    END IF;
	
    ----------------------------------------------------------------------
    -- 1) Ensure required extensions exist (oracle_fdw and pgcrypto)
    ----------------------------------------------------------------------
	IF v_connector IN ('oracle','olr') THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'oracle_fdw') THEN
			RAISE NOTICE 'oracle_fdw not found; attempting CREATE EXTENSION';
			BEGIN
				EXECUTE 'CREATE EXTENSION oracle_fdw';
			EXCEPTION WHEN OTHERS THEN
				RAISE EXCEPTION 'Failed to install oracle_fdw: % [%]', SQLERRM, SQLSTATE
					USING HINT = 'Install oracle_fdw (and Oracle client libs) as a superuser, then retry.';
			END;
		END IF;
	ELSIF v_connector = 'mysql' THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'mysql_fdw') THEN
            RAISE NOTICE 'mysql_fdw not found; attempting CREATE EXTENSION';
            BEGIN
                EXECUTE 'CREATE EXTENSION mysql_fdw';
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to install mysql_fdw: % [%]', SQLERRM, SQLSTATE
                    USING HINT = 'Install mysql_fdw as a superuser, then retry.';
            END;
        END IF;
	ELSIF v_connector = 'postgres' THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgres_fdw') THEN
            RAISE NOTICE 'postgres_fdw not found; attempting CREATE EXTENSION';
            BEGIN
                EXECUTE 'CREATE EXTENSION postgres_fdw';
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to install postgres_fdw: % [%]', SQLERRM, SQLSTATE
                    USING HINT = 'Install postgres_fdw as a superuser, then retry.';
            END;
        END IF;
	ELSIF v_connector = 'sqlserver' THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'tds_fdw') THEN
            RAISE NOTICE 'tds_fdw not found; attempting CREATE EXTENSION';
            BEGIN
                EXECUTE 'CREATE EXTENSION tds_fdw';
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to install tds_fdw: % [%]', SQLERRM, SQLSTATE
                    USING HINT = 'Install tds_fdw (and FreeTDS client libs) as a superuser, then retry.';
            END;
        END IF;
	END IF;

    ----------------------------------------------------------------------
    -- 2) Fetch connector info and DECRYPT password
    ----------------------------------------------------------------------
    SELECT
        data->>'hostname',
        NULLIF(data->>'port','')::int,
        data->>'srcdb',                 -- preserve the exact source database/service
        data->>'user',
        synchdb_connector_credential(p_connector_name, 'pwd')
    INTO v_hostname, v_port, v_srcdb, v_user, v_pwd
    FROM synchdb_conninfo
    WHERE name = p_connector_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'No row in synchdb_conninfo for connector name %', p_connector_name;
    END IF;

    -- Fetch FDW TLS cert paths set via synchdb_add_fdw_conninfo (NULL when absent)
    SELECT
        NULLIF(NULLIF(data->>'ssl_mode', ''), 'null'),
        NULLIF(data->>'fdw_ssl_cert',     'null'),
        NULLIF(data->>'fdw_ssl_key',      'null'),
        NULLIF(data->>'fdw_ssl_rootcert', 'null'),
        NULLIF(synchdb_connector_credential(p_connector_name, 'fdw_ssl_cipher'), 'null')
    INTO v_ssl_mode, v_ssl_cert, v_ssl_key, v_ssl_rootcert, v_ssl_cipher
    FROM synchdb_conninfo
    WHERE name = p_connector_name;

    IF v_hostname IS NULL OR v_hostname = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data.hostname is missing', p_connector_name;
    END IF;
    IF v_srcdb IS NULL OR v_srcdb = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data.srcdb (service/SID) is missing', p_connector_name;
    END IF;
    -- Parse CDB/PDB format: "CDB/PDB"
    -- If srcdb contains a slash, the left part is the CDB service (used by Debezium in C code),
    -- and the right part is the PDB service used for the FDW connection here.
    IF position('/' IN v_srcdb) > 0 THEN
        v_service := split_part(v_srcdb, '/', 2);
        IF v_service = '' THEN
            RAISE EXCEPTION 'synchdb_conninfo[%]: data.srcdb has a trailing slash but no PDB service name (format: CDB/PDB)', p_connector_name;
        END IF;
        RAISE NOTICE 'CDB/PDB mode: srcdb=%, FDW will connect to PDB service "%"', v_srcdb, v_service;
    ELSE
        v_service := v_srcdb;
    END IF;

    IF v_user IS NULL OR v_user = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data.user is missing', p_connector_name;
    END IF;
    IF v_pwd IS NULL OR v_pwd = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: decrypted password is empty or invalid; check key and ciphertext', p_connector_name;
    END IF;

    v_port := COALESCE(v_port, 1521);
	v_server := p_server_name::text;
    IF v_server IS NULL OR btrim(v_server) = '' THEN
        RAISE EXCEPTION 'FDW server name must be nonempty' USING ERRCODE = '22023';
    END IF;

    ----------------------------------------------------------------------
    -- 3) Create a new FDW server and user mapping
    ----------------------------------------------------------------------
    -- The name alone does not prove ownership of an existing server. The
    -- CREATE below rejects collisions without deleting its dependent objects.

	IF v_connector IN ('oracle','olr') THEN
		-- oracle_fdw uses Easy Connect Plus for TLS. fdw_ssl_rootcert is treated as
		-- the Oracle Wallet directory. fdw_ssl_cert/fdw_ssl_key have no Easy Connect
		-- equivalent and must be imported into the Wallet beforehand via orapki.
		IF v_ssl_rootcert IS NOT NULL THEN
			v_dbserver := format('tcps://%s:%s/%s?wallet_location=%s&ssl_server_dn_match=yes',
			                     v_hostname, v_port, v_service, v_ssl_rootcert);
			IF v_ssl_cert IS NOT NULL OR v_ssl_key IS NOT NULL THEN
				RAISE NOTICE
					'fdw_ssl_cert/fdw_ssl_key are ignored for oracle_fdw: import them into '
					'the Wallet at % using orapki or openssl pkcs12.',
					v_ssl_rootcert;
			END IF;
		ELSE
			v_dbserver := format('//%s:%s/%s', v_hostname, v_port, v_service);
		END IF;

		EXECUTE format(
			'CREATE SERVER %I FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver %L)',
			v_server, v_dbserver
		);

		EXECUTE format(
			'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (user %L, password %L)',
			v_server, v_user, v_pwd
		);

		RAISE NOTICE 'Created server % and user mapping for CURRENT_USER', v_server;
	ELSIF v_connector = 'mysql' THEN
		EXECUTE (
            format('CREATE SERVER %I FOREIGN DATA WRAPPER mysql_fdw OPTIONS (host %L, port %L',
                   v_server, v_hostname, v_port::text)
            || CASE WHEN v_ssl_cert     IS NOT NULL THEN format(', ssl_cert %L',   v_ssl_cert)     ELSE '' END
            || CASE WHEN v_ssl_key      IS NOT NULL THEN format(', ssl_key %L',    v_ssl_key)      ELSE '' END
            || CASE WHEN v_ssl_rootcert IS NOT NULL THEN format(', ssl_ca %L',     v_ssl_rootcert) ELSE '' END
            || CASE WHEN v_ssl_cipher   IS NOT NULL THEN format(', ssl_cipher %L', v_ssl_cipher)   ELSE '' END
            || ')'
        );

        EXECUTE format(
            'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (username %L, password %L)',
            v_server, v_user, v_pwd
        );
	ELSIF v_connector = 'postgres' THEN
		EXECUTE (
            format('CREATE SERVER %I FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %L, dbname %L, port %L',
                   v_server, v_hostname, v_srcdb, v_port::text)
            || CASE WHEN v_ssl_mode     IS NOT NULL THEN format(', sslmode %L',     v_ssl_mode)     ELSE '' END
            || CASE WHEN v_ssl_cert     IS NOT NULL THEN format(', sslcert %L',     v_ssl_cert)     ELSE '' END
            || CASE WHEN v_ssl_key      IS NOT NULL THEN format(', sslkey %L',      v_ssl_key)      ELSE '' END
            || CASE WHEN v_ssl_rootcert IS NOT NULL THEN format(', sslrootcert %L', v_ssl_rootcert) ELSE '' END
            || ')'
        );

        EXECUTE format(
            'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (user %L, password %L)',
            v_server, v_user, v_pwd
        );
	ELSIF v_connector = 'sqlserver' THEN
		-- tds_fdw has no SSL-related server options; encryption (if any) is configured
		-- via FreeTDS itself (freetds.conf), not through CREATE SERVER OPTIONS.
		IF v_ssl_mode IS NOT NULL OR v_ssl_cert IS NOT NULL OR v_ssl_key IS NOT NULL OR v_ssl_rootcert IS NOT NULL THEN
			RAISE NOTICE 'fdw_ssl_* options are ignored for tds_fdw: configure TLS via FreeTDS (freetds.conf) instead.';
		END IF;

		-- Preserve the SQL Server error number/text alongside DB-Library's
		-- generic HV00L error (the default blackhole handler discards them).
		EXECUTE format(
            'CREATE SERVER %I FOREIGN DATA WRAPPER tds_fdw OPTIONS (servername %L, port %L, database %L, msg_handler ''notice'')',
            v_server, v_hostname, v_port::text, v_srcdb
        );

        EXECUTE format(
            'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (username %L, password %L)',
            v_server, v_user, v_pwd
        );
	ELSE
        RAISE EXCEPTION 'Unsupported connector type: %', v_connector;
    END IF;
    RETURN v_server;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'synchdb_prepare_initial_snapshot(%) failed: % [%]',
                        p_connector_name, SQLERRM, SQLSTATE
        USING ERRCODE = SQLSTATE,
              HINT = 'Verify designated FDWs, connector fields, credentials, and caller privileges.';
END;
$$;

COMMENT ON FUNCTION synchdb_prepare_initial_snapshot(text, name) IS
   'check oracle_fdw and prepare foreign server and user mapping objects';

-- The coordinated path supplies a generation-specific server name. Direct
-- callers can explicitly use this convenience entry point under their role.
CREATE OR REPLACE FUNCTION synchdb_prepare_initial_snapshot(
    p_connector_name name
) RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog, pg_temp AS $$
DECLARE v_schema name; v_connector text; v_result text;
BEGIN
    SELECT n.nspname INTO STRICT v_schema FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='synchdb';
    EXECUTE pg_catalog.format('SELECT lower(data->>''connector'') FROM %I.synchdb_conninfo WHERE name=$1',v_schema)
      INTO STRICT v_connector USING p_connector_name;
    EXECUTE pg_catalog.format('SELECT %I.synchdb_prepare_initial_snapshot($1::text,$2)',v_schema)
      INTO STRICT v_result USING p_connector_name,pg_catalog.format('%s_%s',p_connector_name,v_connector)::name;
    RETURN v_result;
END;
$$;


-- Source: src/sql/fdw/postgres.sql; sha256=777999535ad4cf5bc77c46b37d33e7cdae5bfea2a0d109e2b811f16f749b47fc

-----------------------------------------------------------------------------------------------------------------
    -- POSTGRESQL: functions to map PostgreSQL objects to PostgreSQL via FDW
-----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION synchdb_create_pg_objs(
   server   name,
   schema   name DEFAULT 'public',
   options  jsonb DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT
SET search_path = pg_catalog
AS $postgres_create_catalog$
DECLARE
    v_server name := server;
    v_schema name := schema;
BEGIN
    ----------------------------------------------------------------------
    -- Sanity check: foreign server must exist
    ----------------------------------------------------------------------
    PERFORM 1
    FROM pg_foreign_server
    WHERE srvname = v_server;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'postgres_create_catalog: foreign server "%" does not exist', v_server;
    END IF;

    ----------------------------------------------------------------------
    -- Ensure target schema exists
    ----------------------------------------------------------------------
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);

    ----------------------------------------------------------------------
    -- information_schema.columns -> <schema>.columns
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.columns (
            schema        text    OPTIONS (column_name 'table_schema'),
            table_name    text    OPTIONS (column_name 'table_name'),
            column_name   text    OPTIONS (column_name 'column_name'),
            position      integer OPTIONS (column_name 'ordinal_position'),
            type_name     text    OPTIONS (column_name 'data_type'),
            length        integer OPTIONS (column_name 'character_maximum_length'),
            precision     integer OPTIONS (column_name 'numeric_precision'),
            scale         integer OPTIONS (column_name 'numeric_scale'),
            nullable      text    OPTIONS (column_name 'is_nullable'),
            default_value text    OPTIONS (column_name 'column_default'),

			udt_schema    text    OPTIONS (column_name 'udt_schema'),
			udt_name      text    OPTIONS (column_name 'udt_name')
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'information_schema',
            table_name  'columns'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- information_schema.tables -> <schema>.tables
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.tables (
            schema     text OPTIONS (column_name 'table_schema'),
            table_name text OPTIONS (column_name 'table_name')
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'information_schema',
            table_name  'tables'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- pg_catalog.pg_namespace -> <schema>.namespaces
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.namespaces (
            oid      oid,
            nspname  name,
            nspowner oid,
            nspacl   aclitem[]
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'pg_catalog',
            table_name  'pg_namespace'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- pg_catalog.pg_class -> <schema>.classes
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.classes (
            oid           oid,
            relname       name,
            relnamespace  oid,
            reltype       oid,
            relowner      oid,
            relam         oid,
            relfilenode   oid,
            reltablespace oid,
            relpages      integer,
            reltuples     real,
            relkind       "char"
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'pg_catalog',
            table_name  'pg_class'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- pg_catalog.pg_attribute -> <schema>.attributes
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.attributes (
            attrelid      oid,
            attname       name,
            atttypid      oid,
            attstattarget integer,
            attlen        smallint,
            attnum        smallint,
            attndims      integer,
            attcacheoff   integer,
            atttypmod     integer,
            attbyval      boolean,
            attstorage    "char",
            attalign      "char",
            attnotnull    boolean,
            atthasdef     boolean,
            attisdropped  boolean,
            attislocal    boolean,
            attinhcount   integer,
            attcollation  oid
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'pg_catalog',
            table_name  'pg_attribute'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- pg_catalog.pg_constraint -> <schema>.constraints
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.constraints (
            oid           oid,
            conname       name,
            connamespace  oid,
            contype       "char",
            condeferrable boolean,
            condeferred   boolean,
            convalidated  boolean,
            conrelid      oid,
            contypid      oid,
            conindid      oid,
            conparentid   oid,
            confrelid     oid,
            confupdtype   "char",
            confdeltype   "char",
            confmatchtype "char",
            conislocal    boolean,
            coninhcount   integer,
            connoinherit  boolean,
            conkey        smallint[],
            confkey       smallint[],
            conpfeqop     oid[],
            conppeqop     oid[],
            conffeqop     oid[],
            conexclop     oid[],
            conbin        pg_node_tree
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'pg_catalog',
            table_name  'pg_constraint'
        )
    $SQL$, v_schema, v_server);

    ----------------------------------------------------------------------
    -- keys view in <schema>.keys
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE OR REPLACE VIEW %1$I.keys AS
        SELECT
            n.nspname                 AS schema,
            c.relname                 AS table_name,
            con.conname               AS constraint_name,
            con.condeferrable         AS deferrable,
            con.condeferred           AS deferred,
            a.attname                 AS column_name,
            ord.pos                   AS position,
            (con.contype = 'p')       AS is_primary
        FROM %1$I.constraints con
        JOIN %1$I.classes      c
          ON c.oid = con.conrelid
        JOIN %1$I.namespaces   n
          ON n.oid = c.relnamespace
        JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS ord(attnum, pos)
          ON true
        JOIN %1$I.attributes   a
          ON a.attrelid = c.oid
         AND a.attnum   = ord.attnum
        WHERE con.contype IN ('p')  -- primary keys only
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    $SQL$, v_schema);

    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.types (
            oid            oid,
            typname        name,
            typnamespace   oid,
            typlen         smallint,
            typbyval       boolean,
            typtype        "char",
            typcategory    "char",
            typispreferred boolean,
            typdelim       "char",
            typrelid       oid,
            typelem        oid,
            typarray       oid,
            typinput       regproc,
            typoutput      regproc,
            typreceive     regproc,
            typsend        regproc,
            typmodin       regproc,
            typmodout      regproc,
            typanalyze     regproc,
            typalign       "char",
            typstorage     "char",
            typnotnull     boolean,
            typbasetype    oid,
            typtypmod      integer,
            typndims       integer,
            typcollation   oid
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'pg_catalog',
            table_name  'pg_type'
        )
    $SQL$, v_schema, v_server);

    EXECUTE format($SQL$
        CREATE OR REPLACE VIEW %1$I.columns_resolved AS
        SELECT
          c.schema,
          c.table_name,
          c.column_name,
          c.position,
          c.type_name,
          c.length,
          c.precision,
          c.scale,
          c.nullable,
          c.default_value,
    
          -- carry-through so downstream SQL can use them
          c.udt_schema,
          c.udt_name,
    
          CASE WHEN c.type_name = 'ARRAY' THEN ns.nspname ELSE NULL END AS array_type_schema,
          CASE WHEN c.type_name = 'ARRAY' THEN t.typname  ELSE NULL END AS array_type_name,
    
          CASE WHEN c.type_name = 'ARRAY' THEN elemns.nspname ELSE NULL END AS element_type_schema,
          CASE WHEN c.type_name = 'ARRAY' THEN elem.typname   ELSE NULL END AS element_type_name
        FROM %1$I.columns c
        LEFT JOIN %1$I.namespaces ns
          ON ns.nspname = c.udt_schema
        LEFT JOIN %1$I.types t
          ON t.typname = c.udt_name
         AND t.typnamespace = ns.oid
        LEFT JOIN %1$I.types elem
          ON elem.oid = t.typelem
        LEFT JOIN %1$I.namespaces elemns
          ON elemns.oid = elem.typnamespace
    $SQL$, v_schema);	
    RETURN;
END;
$postgres_create_catalog$;

COMMENT ON FUNCTION synchdb_create_pg_objs(name, name, jsonb) IS
   'create PostgreSQL foreign tables for the metadata of a foreign server';

CREATE OR REPLACE FUNCTION synchdb_create_current_lsn_ft(
    p_schema name,  -- e.g. 'postgres_obj'
    p_server name   -- e.g. 'pgconn_postgres'
) RETURNS void
LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT
SET search_path = pg_catalog
AS $synchdb_create_current_lsn_ft$
DECLARE
    v_schema name := p_schema;
    v_server name := p_server;
BEGIN
    ----------------------------------------------------------------------
    -- 1) Validate foreign server existence
    ----------------------------------------------------------------------
    PERFORM 1 FROM pg_foreign_server WHERE srvname = v_server;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'synchdb_create_current_lsn_ft: foreign server "%" does not exist', v_server;
    END IF;

    ----------------------------------------------------------------------
    -- 2) Ensure schema exists
    ----------------------------------------------------------------------
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);

    ----------------------------------------------------------------------
    -- 3) Foreign table for a diagnostic WAL LSN candidate. This query does not
    -- import a replication-slot exported snapshot into the FDW connection.
    -- NOTE: The remote Postgres must define synchdb_wal_lsn view:
    --
    --   CREATE VIEW public.synchdb_wal_lsn AS
    --     SELECT pg_current_wal_lsn() AS wal_lsn;
    ----------------------------------------------------------------------
    EXECUTE format($SQL$
        CREATE FOREIGN TABLE IF NOT EXISTS %1$I.wal_lsn (
            wal_lsn pg_lsn
        )
        SERVER %2$I
        OPTIONS (
            schema_name 'public',
            table_name  'synchdb_wal_lsn'
        )
    $SQL$, v_schema, v_server);

    RETURN;
END;
$synchdb_create_current_lsn_ft$;

COMMENT ON FUNCTION synchdb_create_current_lsn_ft(name, name) IS
   'Read a diagnostic WAL LSN candidate; it does not coordinate the FDW transaction with a replication-slot snapshot';


-- Source: src/sql/fdw/mysql.sql; sha256=c65e5324c41998cb26e6475ac34f65a3714d6cd71e9bc03c492ba3c720a323d1

-----------------------------------------------------------------------------------------------------------------
    -- MYSQL: functions to map Mysql objects to PostgreSQL via FDW (originated from mysql_migrator project)
-----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION synchdb_create_mysql_objs(
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


-- Source: src/sql/fdw/sqlserver.sql; sha256=0233d4eac540769830e073c482ba01cdbccb33e6243bc2ae1fa7d3e892dd19cc

-----------------------------------------------------------------------------------------------------------------
    -- SQLSERVER: functions to map SQL Server objects to PostgreSQL via FDW (tds_fdw)
-----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION synchdb_create_sqlserver_objs(
   server      name,
   schema      name    DEFAULT NAME 'public',
   options     jsonb   DEFAULT NULL
) RETURNS void
   LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT SET search_path = pg_catalog AS
$synchdb_create_sqlserver_objs$
DECLARE
   old_msglevel text;
BEGIN
   /* remember old setting */
   old_msglevel := current_setting('client_min_messages');

   /* make the output less verbose */
   SET LOCAL client_min_messages = warning;

   EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema);

   /*
    * SynchDB only needs "tables", "columns" and "keys" to construct the destination
    * tables (same minimal contract as the mysql/oracle builders above) - see
    * doc/docs/en/architecture/fdw_based_snapshot.md. SQL Server implements the ANSI
    * INFORMATION_SCHEMA views, so these are built directly against them via tds_fdw's
    * "query" pass-through option, rather than importing raw catalog tables first.
    */

   /*
    * NOTE: tds_fdw defaults to matching result columns to local foreign-table
    * columns BY NAME (option "match_column_names", default true), and the match
    * is case-sensitive. Every query below therefore aliases each result column
    * to the exact (lowercase) local column name.
    */

   /* tables */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.tables CASCADE', schema);
   EXECUTE format($SQL$
      CREATE FOREIGN TABLE %1$I.tables (
         schema     text,
         table_name text
      ) SERVER %2$I OPTIONS (query $query$
         SELECT t.TABLE_SCHEMA AS [schema], t.TABLE_NAME AS [table_name]
         FROM INFORMATION_SCHEMA.TABLES t
         JOIN sys.schemas s ON s.name = t.TABLE_SCHEMA
         JOIN sys.tables st ON st.schema_id = s.schema_id AND st.name = t.TABLE_NAME
         WHERE t.TABLE_TYPE = 'BASE TABLE'
           AND st.is_ms_shipped = 0
      $query$);
      COMMENT ON FOREIGN TABLE %1$I.tables IS 'SQL Server tables (via tds_fdw)';
   $SQL$, schema, server);

   /* columns */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.columns CASCADE', schema);
   EXECUTE format($SQL$
      CREATE FOREIGN TABLE %1$I.columns (
         schema        text,
         table_name    text,
         column_name   text,
         position      integer,
         type_name     text,
         length        integer,
         precision     integer,
         scale         integer,
         nullable      boolean,
         default_value text
      ) SERVER %2$I OPTIONS (query $query$
         SELECT c.TABLE_SCHEMA AS [schema],
                c.TABLE_NAME AS [table_name],
                c.COLUMN_NAME AS [column_name],
                CAST(c.ORDINAL_POSITION AS int) AS [position],
                c.DATA_TYPE AS [type_name],
                CAST(c.CHARACTER_MAXIMUM_LENGTH AS int) AS [length],
                CAST(c.NUMERIC_PRECISION AS int) AS [precision],
                CAST(c.NUMERIC_SCALE AS int) AS [scale],
                CASE WHEN c.IS_NULLABLE = 'YES' THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS [nullable],
                c.COLUMN_DEFAULT AS [default_value]
         FROM INFORMATION_SCHEMA.COLUMNS c
         JOIN INFORMATION_SCHEMA.TABLES t
           ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
         JOIN sys.schemas s ON s.name = t.TABLE_SCHEMA
         JOIN sys.tables st ON st.schema_id = s.schema_id AND st.name = t.TABLE_NAME
         WHERE t.TABLE_TYPE = 'BASE TABLE'
           AND st.is_ms_shipped = 0
      $query$);
      COMMENT ON FOREIGN TABLE %1$I.columns IS 'columns of SQL Server tables (via tds_fdw)';
   $SQL$, schema, server);

   /* keys (primary and unique) */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.keys CASCADE', schema);
   EXECUTE format($SQL$
      CREATE FOREIGN TABLE %1$I.keys (
         schema          text,
         table_name      text,
         constraint_name text,
         "deferrable"    boolean,
         deferred        boolean,
         column_name     text,
         position        integer,
         is_primary      boolean
      ) SERVER %2$I OPTIONS (query $query$
         SELECT tc.TABLE_SCHEMA AS [schema],
                tc.TABLE_NAME AS [table_name],
                tc.CONSTRAINT_NAME AS [constraint_name],
                CAST(0 AS bit) AS [deferrable],
                CAST(0 AS bit) AS [deferred],
                kcu.COLUMN_NAME AS [column_name],
                CAST(kcu.ORDINAL_POSITION AS int) AS [position],
                CASE WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS [is_primary]
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
         JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
           ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
          AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         JOIN sys.schemas s ON s.name = tc.TABLE_SCHEMA
         JOIN sys.tables st ON st.schema_id = s.schema_id AND st.name = tc.TABLE_NAME
         WHERE tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
           AND st.is_ms_shipped = 0
      $query$);
      COMMENT ON FOREIGN TABLE %1$I.keys IS 'SQL Server primary/unique key columns (via tds_fdw)';
   $SQL$, schema, server);

   /* restore old setting */
   PERFORM set_config('client_min_messages', old_msglevel, true);

   RETURN;
END;
$synchdb_create_sqlserver_objs$;

COMMENT ON FUNCTION synchdb_create_sqlserver_objs(name, name, jsonb) IS
   'create SQL Server foreign tables (tables/columns/keys) via tds_fdw for FDW-based snapshot';


-- Source: src/sql/fdw/oracle.sql; sha256=76cd3990cbf17b609e608a5ae86fce3cc8a6b0332624a1fbcedea2bc28eecf03

-----------------------------------------------------------------------------------------------------------------
    -- ORACLE: functions to map Oracle objects to PostgreSQL via FDW (originated from oracle_migrator project)
-----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION synchdb_create_oraviews(
   server      name,
   schema      name    DEFAULT NAME 'public',
   options     jsonb   DEFAULT NULL
) RETURNS void
   LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT SET search_path = pg_catalog AS
$$DECLARE
   old_msglevel text;
   v_max_long   integer := 32767;

   sys_schemas text :=
      E'''''ANONYMOUS'''', ''''APEX_PUBLIC_USER'''', ''''APEX_030200'''', ''''APEX_040000'''',\n'
      '         ''''APEX_050000'''', ''''APPQOSSYS'''', ''''AUDSYS'''', ''''AURORA$JIS$UTILITY$'''',\n'
      '         ''''AURORA$ORB$UNAUTHENTICATED'''', ''''CTXSYS'''', ''''DBSFWUSER'''', ''''DBSNMP'''',\n'
      '         ''''DIP'''', ''''DMSYS'''', ''''DVSYS'''', ''''DVF'''', ''''EXFSYS'''',\n'
      '         ''''FLOWS_30000'''', ''''FLOWS_FILES'''', ''''GDOSYS'''', ''''GGSYS'''',\n'
      '         ''''GSMADMIN_INTERNAL'''', ''''GSMCATUSER'''', ''''GSMUSER'''', ''''LBACSYS'''',\n'
      '         ''''MDDATA'''', ''''MDSYS'''', ''''MGMT_VIEW'''', ''''ODM'''', ''''ODM_MTR'''',\n'
      '         ''''OJVMSYS'''', ''''OLAPSYS'''', ''''ORACLE_OCM'''', ''''ORDDATA'''',\n'
      '         ''''ORDPLUGINS'''', ''''ORDSYS'''', ''''OSE$HTTP$ADMIN'''', ''''OUTLN'''',\n'
      '         ''''PDBADMIN'''', ''''REMOTE_SCHEDULER_AGENT'''', ''''SI_INFORMTN_SCHEMA'''',\n'
      '         ''''SPATIAL_WFS_ADMIN_USR'''', ''''SPATIAL_CSW_ADMIN_USR'''', ''''SPATIAL_WFS_ADMIN_USR'''',\n'
      '         ''''SYS'''', ''''SYS$UMF'''', ''''SYSBACKUP'''', ''''SYSDG'''', ''''SYSKM'''',\n'
      '         ''''SYSMAN'''', ''''SYSRAC'''', ''''SYSTEM'''', ''''TRACESRV'''',\n'
      '         ''''MTSSYS'''', ''''OASPUBLIC'''', ''''OLAPSYS'''', ''''OWBSYS'''', ''''OWBSYS_AUDIT'''',\n'
      '         ''''PERFSTAT'''', ''''WEBSYS'''', ''''WK_PROXY'''', ''''WKSYS'''', ''''WK_TEST'''',\n'
      '         ''''WMSYS'''', ''''XDB'''', ''''XS$NULL''''';

   tables_sql text := E'CREATE FOREIGN TABLE %I.tables (\n'
      '   schema     text NOT NULL,\n'
      '   table_name text NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT owner,\n'
         '       table_name\n'
         'FROM dba_tables\n'
         'WHERE temporary = ''''N''''\n'
         '  AND secondary = ''''N''''\n'
         '  AND nested    = ''''NO''''\n'
         '  AND dropped   = ''''NO''''\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT owner, mview_name\n'
         '             FROM dba_mviews)\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT log_owner, log_table\n'
         '             FROM dba_mview_logs)\n'
         '  AND owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   columns_sql text := E'CREATE FOREIGN TABLE %I.columns (\n'
      '   schema        text    NOT NULL,\n'
      '   table_name    text    NOT NULL,\n'
      '   column_name   text    NOT NULL,\n'
      '   position      integer NOT NULL,\n'
      '   type_name     text    NOT NULL,\n'
      '   length        integer NOT NULL,\n'
      '   precision     integer,\n'
      '   scale         integer,\n'
      '   nullable      boolean NOT NULL,\n'
      '   default_value text\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT col.owner,\n'
         '       col.table_name,\n'
         '       col.column_name,\n'
         '       col.column_id,\n'
         '       CASE WHEN col.data_type_owner IS NULL\n'
         '            THEN col.data_type\n'
         '            ELSE col.data_type_owner || ''''.'''' || col.data_type\n'
         '       END data_type,\n'
         '       col.char_length,\n'
         '       col.data_precision,\n'
         '       col.data_scale,\n'
         '       CASE WHEN col.nullable = ''''Y'''' THEN 1 ELSE 0 END AS nullable,\n'
         '       col.data_default\n'
         'FROM dba_tab_columns col\n'
         '   JOIN (SELECT owner, table_name\n'
         '            FROM dba_tables\n'
         '            WHERE owner NOT IN (' || sys_schemas || E')\n'
         '              AND temporary = ''''N''''\n'
         '              AND secondary = ''''N''''\n'
         '              AND nested    = ''''NO''''\n'
         '              AND dropped   = ''''NO''''\n'
         '         UNION SELECT owner, view_name\n'
         '            FROM dba_views\n'
         '            WHERE owner NOT IN (' || sys_schemas || E')\n'
         '        ) tab\n'
         '      ON tab.owner = col.owner AND tab.table_name = col.table_name\n'
         'WHERE (col.owner, col.table_name)\n'
         '     NOT IN (SELECT owner, mview_name\n'
         '             FROM dba_mviews)\n'
         '  AND (col.owner, col.table_name)\n'
         '     NOT IN (SELECT log_owner, log_table\n'
         '             FROM dba_mview_logs)\n'
         '  AND col.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   checks_sql text := E'CREATE FOREIGN TABLE %I.checks (\n'
      '   schema          text    NOT NULL,\n'
      '   table_name      text    NOT NULL,\n'
      '   constraint_name text    NOT NULL,\n'
      '   "deferrable"    boolean NOT NULL,\n'
      '   deferred        boolean NOT NULL,\n'
      '   condition       text    NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT con.owner,\n'
         '       con.table_name,\n'
         '       con.constraint_name,\n'
         '       CASE WHEN con.deferrable = ''''DEFERRABLE'''' THEN 1 ELSE 0 END deferrable,\n'
         '       CASE WHEN con.deferred   = ''''DEFERRED''''   THEN 1 ELSE 0 END deferred,\n'
         '       con.search_condition\n'
         'FROM dba_constraints con\n'
         '   JOIN dba_tables tab\n'
         '      ON tab.owner = con.owner AND tab.table_name = con.table_name\n'
         'WHERE tab.temporary = ''''N''''\n'
         '  AND tab.secondary = ''''N''''\n'
         '  AND tab.nested    = ''''NO''''\n'
         '  AND tab.dropped   = ''''NO''''\n'
         '  AND con.constraint_type = ''''C''''\n'
         '  AND con.status          = ''''ENABLED''''\n'
         '  AND con.validated       = ''''VALIDATED''''\n'
         '  AND con.invalid         IS NULL\n'
         '  AND con.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   foreign_keys_sql text := E'CREATE FOREIGN TABLE %I.foreign_keys (\n'
      '   schema          text    NOT NULL,\n'
      '   table_name      text    NOT NULL,\n'
      '   constraint_name text    NOT NULL,\n'
      '   "deferrable"    boolean NOT NULL,\n'
      '   deferred        boolean NOT NULL,\n'
      '   delete_rule     text    NOT NULL,\n'
      '   column_name     text    NOT NULL,\n'
      '   position        integer NOT NULL,\n'
      '   remote_schema   text    NOT NULL,\n'
      '   remote_table    text    NOT NULL,\n'
      '   remote_column   text    NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT con.owner,\n'
         '       con.table_name,\n'
         '       con.constraint_name,\n'
         '       CASE WHEN con.deferrable = ''''DEFERRABLE'''' THEN 1 ELSE 0 END deferrable,\n'
         '       CASE WHEN con.deferred   = ''''DEFERRED''''   THEN 1 ELSE 0 END deferred,\n'
         '       con.delete_rule,\n'
         '       col.column_name,\n'
         '       col.position,\n'
         '       r_col.owner AS remote_schema,\n'
         '       r_col.table_name AS remote_table,\n'
         '       r_col.column_name AS remote_column\n'
         'FROM dba_constraints con\n'
         '   JOIN dba_cons_columns col\n'
         '      ON con.owner = col.owner AND con.table_name = col.table_name AND con.constraint_name = col.constraint_name\n'
         '   JOIN dba_cons_columns r_col\n'
         '      ON con.r_owner = r_col.owner AND con.r_constraint_name = r_col.constraint_name AND col.position = r_col.position\n'
         'WHERE con.constraint_type = ''''R''''\n'
         '  AND con.status          = ''''ENABLED''''\n'
         '  AND con.validated       = ''''VALIDATED''''\n'
         '  AND con.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   keys_sql text := E'CREATE FOREIGN TABLE %I.keys (\n'
      '   schema          text    NOT NULL,\n'
      '   table_name      text    NOT NULL,\n'
      '   constraint_name text    NOT NULL,\n'
      '   "deferrable"    boolean NOT NULL,\n'
      '   deferred        boolean NOT NULL,\n'
      '   column_name     text    NOT NULL,\n'
      '   position        integer NOT NULL,\n'
      '   is_primary      boolean NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT con.owner,\n'
         '       con.table_name,\n'
         '       con.constraint_name,\n'
         '       CASE WHEN deferrable = ''''DEFERRABLE'''' THEN 1 ELSE 0 END deferrable,\n'
         '       CASE WHEN deferred   = ''''DEFERRED''''   THEN 1 ELSE 0 END deferred,\n'
         '       col.column_name,\n'
         '       col.position,\n'
         '       CASE WHEN con.constraint_type = ''''P'''' THEN 1 ELSE 0 END is_primary\n'
         'FROM dba_tables tab\n'
         '   JOIN dba_constraints con\n'
         '      ON tab.owner = con.owner AND tab.table_name = con.table_name\n'
         '   JOIN dba_cons_columns col\n'
         '      ON con.owner = col.owner AND con.table_name = col.table_name AND con.constraint_name = col.constraint_name\n'
         'WHERE (con.owner, con.table_name)\n'
         '     NOT IN (SELECT owner, mview_name\n'
         '             FROM dba_mviews)\n'
         '  AND con.constraint_type IN (''''P'''', ''''U'''')\n'
         '  AND con.status    = ''''ENABLED''''\n'
         '  AND con.validated = ''''VALIDATED''''\n'
         '  AND tab.temporary = ''''N''''\n'
         '  AND tab.secondary = ''''N''''\n'
         '  AND tab.nested    = ''''NO''''\n'
         '  AND tab.dropped   = ''''NO''''\n'
         '  AND con.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   views_sql text := E'CREATE FOREIGN TABLE %I.views (\n'
      '   schema     text NOT NULL,\n'
      '   view_name  text NOT NULL,\n'
      '   definition text NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT owner,\n'
         '       view_name,\n'
         '       text\n'
         'FROM dba_views\n'
         'WHERE owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   func_src_sql text := E'CREATE FOREIGN TABLE %I.func_src (\n'
      '   schema        text    NOT NULL,\n'
      '   function_name text    NOT NULL,\n'
      '   is_procedure  boolean NOT NULL,\n'
      '   line_number   integer NOT NULL,\n'
      '   line          text    NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT pro.owner,\n'
         '       pro.object_name,\n'
         '       CASE WHEN pro.object_type = ''''PROCEDURE'''' THEN 1 ELSE 0 END is_procedure,\n'
         '       src.line,\n'
         '       src.text\n'
         'FROM dba_procedures pro\n'
         '   JOIN dba_source src\n'
         '      ON pro.owner = src.owner\n'
         '         AND pro.object_name = src.name\n'
         '         AND pro.object_type = src.type\n'
         'WHERE pro.object_type IN (''''FUNCTION'''', ''''PROCEDURE'''')\n'
         '  AND pro.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   functions_sql text := E'CREATE VIEW %I.functions AS\n'
      'SELECT schema,\n'
      '       function_name,\n'
      '       is_procedure,\n'
      '       string_agg(line, TEXT '''' ORDER BY line_number) AS source\n'
      'FROM %I.func_src\n'
      'GROUP BY schema, function_name, is_procedure';

   sequences_sql text := E'CREATE FOREIGN TABLE %I.sequences (\n'
      '   schema        text        NOT NULL,\n'
      '   sequence_name text        NOT NULL,\n'
      '   min_value     numeric(28),\n'
      '   max_value     numeric(28),\n'
      '   increment_by  numeric(28) NOT NULL,\n'
      '   cyclical      boolean     NOT NULL,\n'
      '   cache_size    integer     NOT NULL,\n'
      '   last_value    numeric(28) NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT sequence_owner,\n'
         '       sequence_name,\n'
         '       min_value,\n'
         '       max_value,\n'
         '       increment_by,\n'
         '       CASE WHEN cycle_flag = ''''Y'''' THEN 1 ELSE 0 END cyclical,\n'
         '       cache_size,\n'
         '       last_number\n'
         'FROM dba_sequences\n'
         'WHERE sequence_owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   index_exp_sql text := E'CREATE FOREIGN TABLE %I.index_exp (\n'
      '   schema         text    NOT NULL,\n'
      '   table_name     text    NOT NULL,\n'
      '   index_name     text    NOT NULL,\n'
      '   uniqueness     boolean NOT NULL,\n'
      '   position       integer NOT NULL,\n'
      '   descend        boolean NOT NULL,\n'
      '   col_name       text    NOT NULL,\n'
      '   col_expression text\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT ic.table_owner,\n'
         '       ic.table_name,\n'
         '       ic.index_name,\n'
         '       CASE WHEN i.uniqueness = ''''UNIQUE'''' THEN 1 ELSE 0 END uniqueness,\n'
         '       ic.column_position,\n'
         '       CASE WHEN ic.descend = ''''DESC'''' THEN 1 ELSE 0 END descend,\n'
         '       ic.column_name,\n'
         '       ie.column_expression\n'
         'FROM dba_indexes i,\n'
         '     dba_tables t,\n'
         '     dba_ind_columns ic,\n'
         '     dba_ind_expressions ie\n'
         'WHERE i.table_owner      = t.owner\n'
         '  AND i.table_name       = t.table_name\n'
         '  AND i.owner            = ic.index_owner\n'
         '  AND i.index_name       = ic.index_name\n'
         '  AND i.table_owner      = ic.table_owner\n'
         '  AND i.table_name       = ic.table_name\n'
         '  AND ic.index_owner     = ie.index_owner(+)\n'
         '  AND ic.index_name      = ie.index_name(+)\n'
         '  AND ic.table_owner     = ie.table_owner(+)\n'
         '  AND ic.table_name      = ie.table_name(+)\n'
         '  AND ic.column_position = ie.column_position(+)\n'
         '  AND t.temporary        = ''''N''''\n'
         '  AND t.secondary        = ''''N''''\n'
         '  AND t.nested           = ''''NO''''\n'
         '  AND t.dropped          = ''''NO''''\n'
         '  AND i.index_type NOT IN (''''LOB'''', ''''DOMAIN'''')\n'
         '  AND coalesce(i.dropped, ''''NO'''') = ''''NO''''\n'
         '  AND NOT EXISTS (SELECT 1  /* exclude constraint indexes */\n'
         '                  FROM dba_constraints c\n'
         '                  WHERE c.owner = i.table_owner\n'
         '                    AND c.table_name = i.table_name\n'
         '                    AND COALESCE(c.index_owner, i.owner) = i.owner\n'
         '                    AND c.index_name = i.index_name)\n'
         '  AND NOT EXISTS (SELECT 1  /* exclude materialized views */\n'
         '                  FROM dba_mviews m\n'
         '                  WHERE m.owner = i.table_owner\n'
         '                    AND m.mview_name = i.table_name)\n'
         '  AND NOT EXISTS (SELECT 1  /* exclude materialized views logs */\n'
         '                  FROM dba_mview_logs ml\n'
         '                  WHERE ml.log_owner = i.table_owner\n'
         '                    AND ml.log_table = i.table_name)\n'
         '  AND ic.table_owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   index_columns_sql text := E'CREATE VIEW %I.index_columns AS\n'
      'SELECT schema,\n'
      '       table_name,\n'
      '       index_name,\n'
      '       position,\n'
      '       descend,\n'
      '       col_expression IS NOT NULL\n'
      '          AND (NOT descend OR col_expression !~ ''^"[^"]*"$'') AS is_expression,\n'
      '       coalesce(\n'
      '          CASE WHEN descend AND col_expression ~ ''^"[^"]*"$''\n'
      '               THEN replace (col_expression, ''"'', '''')\n'
      '               ELSE col_expression\n'
      '          END,\n'
      '          col_name) AS column_name\n'
      'FROM %I.index_exp';

   indexes_sql text := E'CREATE VIEW %I.indexes AS\n'
      'SELECT DISTINCT\n'
      '       schema,\n'
      '       table_name,\n'
      '       index_name,\n'
      '       uniqueness\n'
      'FROM %I.index_exp';

   partition_cols_sql text := E'CREATE FOREIGN TABLE %I.partition_columns (\n'
      '   schema          text NOT NULL,\n'
      '   table_name      text NOT NULL,\n'
      '   partition_name  text NOT NULL,\n'
      '   column_name     text NOT NULL,\n'
      '   column_position integer NOT NULL,\n'
      '   type            text NOT NULL,\n'
      '   position        integer NOT NULL,\n'
      '   values          text\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT part.table_owner,\n'
         '       part.table_name,\n'
         '       part.partition_name,\n'
         '       cols.column_name,\n'
         '       cols.column_position,\n'
         '       info.partitioning_type,\n'
         '       part.partition_position,\n'
         '       part.high_value\n'
         'FROM dba_tab_partitions part\n'
         '   JOIN dba_part_tables info\n'
         '      ON part.table_owner = info.owner\n'
         '         AND part.table_name = info.table_name\n'
         '   JOIN dba_part_key_columns cols\n'
         '      ON part.table_owner = cols.owner\n'
         '         AND part.table_name = cols.name\n'
         'WHERE part.table_owner NOT IN (' || sys_schemas || E')\n'
         '   AND cols.object_type = ''''TABLE''''\n'
         '   AND info.partitioning_type IN (''''LIST'''', ''''RANGE'''', ''''HASH'''')\n'
      ')'', max_long ''%s'', readonly ''true'')';

   partitions_sql text := E'CREATE VIEW %1$I.partitions AS\n'
      'WITH catalog AS (\n'
      '   SELECT schema,\n'
      '          table_name,\n'
      '          partition_name,\n'
      '          string_agg(column_name, TEXT '', '' ORDER BY column_position) AS key,\n'
      '          type, position, values,\n'
      '          count(column_name) AS colcount\n'
      '   FROM %1$I.partition_columns\n'
      '   GROUP BY schema, table_name, partition_name,\n'
      '         type, position, values\n'
      '), list_partitions AS (\n'
      '   SELECT schema, table_name, partition_name, type, key,\n'
      '      coalesce(values = ''DEFAULT'', FALSE) AS is_default,\n'
      '      string_to_array(values, '','') AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''LIST''\n'
      '     AND colcount = 1\n'
      '), range_partitions AS (\n'
      '   SELECT schema, table_name, partition_name, type, key, FALSE,\n'
      '      ARRAY[\n'
      '         lag(values, 1, ''MINVALUE'')\n'
      '            OVER (PARTITION BY schema, table_name\n'
      '                  ORDER BY position),\n'
      '         values\n'
      '      ] AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''RANGE''\n'
      '     AND colcount = 1\n'
      '), hash_partitions AS (\n'
      '   SELECT schema, table_name, partition_name, type, key, FALSE,\n'
      '      ARRAY[(position - 1)::text] AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''HASH''\n'
      ')\n'
      'SELECT * FROM list_partitions\n'
      'UNION ALL SELECT * FROM range_partitions\n'
      'UNION ALL SELECT * FROM hash_partitions';

   subpartition_cols_sql text := E'CREATE FOREIGN TABLE %I.subpartition_columns (\n'
      '   schema            text NOT NULL,\n'
      '   table_name        text NOT NULL,\n'
      '   partition_name    text NOT NULL,\n'
      '   subpartition_name text NOT NULL,\n'
      '   column_name       text NOT NULL,\n'
      '   column_position   integer NOT NULL,\n'
      '   type              text NOT NULL,\n'
      '   position          integer NOT NULL,\n'
      '   values            text\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT part.table_owner,\n'
         '       part.table_name,\n'
         '       part.partition_name,\n'
         '       part.subpartition_name,\n'
         '       cols.column_name,\n'
         '       cols.column_position,\n'
         '       info.subpartitioning_type,\n'
         '       part.subpartition_position,\n'
         '       part.high_value\n'
         'FROM dba_tab_subpartitions part\n'
         '   JOIN dba_part_tables info\n'
         '      ON part.table_owner = info.owner\n'
         '         AND part.table_name = info.table_name\n'
         '   JOIN dba_subpart_key_columns cols\n'
         '      ON part.table_owner = cols.owner\n'
         '         AND part.table_name = cols.name\n'
         'WHERE part.table_owner NOT IN (' || sys_schemas || E')\n'
         '   AND cols.object_type = ''''TABLE''''\n'
         '   AND info.partitioning_type IN (''''LIST'''', ''''RANGE'''', ''''HASH'''')\n'
         '   AND info.subpartitioning_type IN (''''LIST'''', ''''RANGE'''', ''''HASH'''')\n'
      ')'', max_long ''%s'', readonly ''true'')';

   subpartitions_sql text := E'CREATE VIEW %1$I.subpartitions AS\n'
      'WITH catalog AS (\n'
      '   SELECT schema,\n'
      '          table_name,\n'
      '          partition_name,\n'
      '          subpartition_name,\n'
      '          string_agg(column_name, TEXT '', '' ORDER BY column_position) AS key,\n'
      '          type, position, values,\n'
      '          count(column_name) AS colcount\n'
      '   FROM %1$I.subpartition_columns \n'
      '   GROUP BY schema, table_name, partition_name, subpartition_name,\n'
      '            type, position, values'
      '), list_subpartitions AS (\n'
      '   SELECT schema, table_name, partition_name, subpartition_name, type, key,\n'
      '      coalesce(values = ''DEFAULT'', FALSE) AS is_default,\n'
      '      string_to_array(values, '','') AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''LIST''\n'
      '     AND colcount = 1\n'
      '), range_subpartitions AS (\n'
      '   SELECT schema, table_name, partition_name, subpartition_name, type, key, FALSE,\n'
      '      ARRAY[\n'
      '         lag(values, 1, ''MINVALUE'')\n'
      '            OVER (PARTITION BY schema, table_name, partition_name\n'
      '                  ORDER BY position),\n'
      '         values\n'
      '      ] AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''RANGE''\n'
      '     AND colcount = 1\n'
      '), hash_subpartitions AS (\n'
      '   SELECT schema, table_name, partition_name, subpartition_name, type, key, FALSE,\n'
      '      ARRAY[(position - 1)::text] AS values\n'
      '   FROM catalog\n'
      '   WHERE type = ''HASH'''
      ')\n'
      'SELECT * FROM list_subpartitions\n'
      'UNION ALL SELECT * FROM range_subpartitions\n'
      'UNION ALL SELECT * FROM hash_subpartitions';

   schemas_sql text := E'CREATE FOREIGN TABLE %I.schemas (\n'
      '   schema text NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT username\n'
         'FROM dba_users\n'
         'WHERE username NOT IN( ' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   trig_sql text := E'CREATE FOREIGN TABLE %I.trig (\n'
      '   schema            text NOT NULL,\n'
      '   table_name        text NOT NULL,\n'
      '   trigger_name      text NOT NULL,\n'
      '   trigger_type      text NOT NULL,\n'
      '   triggering_event  text NOT NULL,\n'
      '   when_clause       text,\n'
      '   referencing_names text NOT NULL,\n'
      '   trigger_body      text NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT table_owner,\n'
         '       table_name,\n'
         '       trigger_name,\n'
         '       trigger_type,\n'
         '       triggering_event,\n'
         '       when_clause,\n'
         '       referencing_names,\n'
         '       trigger_body\n'
         'FROM dba_triggers\n'
         'WHERE table_owner NOT IN( ' || sys_schemas || E')\n'
         '  AND base_object_type IN (''''TABLE'''', ''''VIEW'''')\n'
         '  AND status = ''''ENABLED''''\n'
         '  AND crossedition = ''''NO''''\n'
         '  AND trigger_type <> ''''COMPOUND'''''
      ')'', max_long ''%s'', readonly ''true'')';

   triggers_sql text := E'CREATE VIEW %I.triggers AS\n'
      'SELECT schema,\n'
      '       table_name,\n'
      '       trigger_name,\n'
      '       CASE WHEN trigger_type LIKE ''BEFORE %%''\n'
      '            THEN ''BEFORE''\n'
      '            WHEN trigger_type LIKE ''AFTER %%''\n'
      '            THEN ''AFTER''\n'
      '            ELSE trigger_type\n'
      '       END AS trigger_type,\n'
      '       triggering_event,\n'
      '       trigger_type LIKE ''%%EACH ROW'' AS for_each_row,\n'
      '       when_clause,\n'
      '       referencing_names,\n'
      '       trigger_body\n'
      'FROM %I.trig';

   pack_src_sql text := E'CREATE FOREIGN TABLE %I.pack_src (\n'
      '   schema       text    NOT NULL,\n'
      '   package_name text    NOT NULL,\n'
      '   src_type     text    NOT NULL,\n'
      '   line_number  integer NOT NULL,\n'
      '   line         text    NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT pro.owner,\n'
         '       pro.object_name,\n'
         '       src.type,\n'
         '       src.line,\n'
         '       src.text\n'
         'FROM dba_procedures pro\n'
         '   JOIN dba_source src\n'
         '      ON pro.owner = src.owner\n'
         '         AND pro.object_name = src.name\n'
         'WHERE pro.object_type = ''''PACKAGE''''\n'
         '  AND src.type IN (''''PACKAGE'''', ''''PACKAGE BODY'''')\n'
         '  AND procedure_name IS NULL\n'
         '  AND pro.owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   packages_sql text := E'CREATE VIEW %I.packages AS\n'
      'SELECT schema,\n'
      '       package_name,\n'
      '       src_type = ''PACKAGE BODY'' AS is_body,\n'
      '       string_agg(line, TEXT '''' ORDER BY line_number) AS source\n'
      'FROM %I.pack_src\n'
      'GROUP BY schema, package_name, src_type';

   table_privs_sql text := E'CREATE FOREIGN TABLE %I.table_privs (\n'
      '   schema     text    NOT NULL,\n'
      '   table_name text    NOT NULL,\n'
      '   privilege  text    NOT NULL,\n'
      '   grantor    text    NOT NULL,\n'
      '   grantee    text    NOT NULL,\n'
      '   grantable  boolean NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT owner,\n'
         '       table_name,\n'
         '       p.privilege,\n'
         '       p.grantor,\n'
         '       p.grantee,\n'
         '       CASE WHEN p.grantable = ''''YES'''' THEN 1 ELSE 0 END grantable\n'
         'FROM dba_tab_privs p\n'
         '   JOIN dba_tables t USING (owner, table_name)\n'
         'WHERE t.temporary = ''''N''''\n'
         '  AND t.secondary = ''''N''''\n'
         '  AND t.nested    = ''''NO''''\n'
         '  AND coalesce(t.dropped, ''''NO'''') = ''''NO''''\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT owner, mview_name\n'
         '             FROM dba_mviews)\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT log_owner, log_table\n'
         '             FROM dba_mview_logs)\n'
         '  AND owner NOT IN (' || sys_schemas || E')\n'
         '  AND p.grantor NOT IN (' || sys_schemas || E')\n'
         '  AND p.grantee NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   column_privs_sql text := E'CREATE FOREIGN TABLE %I.column_privs (\n'
      '   schema      text    NOT NULL,\n'
      '   table_name  text    NOT NULL,\n'
      '   column_name text    NOT NULL,\n'
      '   privilege   text    NOT NULL,\n'
      '   grantor     text    NOT NULL,\n'
      '   grantee     text    NOT NULL,\n'
      '   grantable   boolean NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT owner,\n'
         '       table_name,\n'
         '       c.column_name,\n'
         '       c.privilege,\n'
         '       c.grantor,\n'
         '       c.grantee,\n'
         '       CASE WHEN c.grantable = ''''YES'''' THEN 1 ELSE 0 END grantable\n'
         'FROM dba_col_privs c\n'
         '   JOIN dba_tables t USING (owner, table_name)\n'
         'WHERE t.temporary = ''''N''''\n'
         '  AND t.secondary = ''''N''''\n'
         '  AND t.nested    = ''''NO''''\n'
         '  AND t.dropped   = ''''NO''''\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT owner, mview_name\n'
         '             FROM dba_mviews)\n'
         '  AND (owner, table_name)\n'
         '     NOT IN (SELECT log_owner, log_table\n'
         '             FROM dba_mview_logs)\n'
         '  AND owner NOT IN (' || sys_schemas || E')\n'
         '  AND c.grantor NOT IN (' || sys_schemas || E')\n'
         '  AND c.grantee NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   segments_sql text := E'CREATE FOREIGN TABLE %I.segments (\n'
      '   schema       text   NOT NULL,\n'
      '   segment_name text   NOT NULL,\n'
      '   segment_type text   NOT NULL,\n'
      '   bytes        bigint NOT NULL\n'
      ') SERVER %I OPTIONS (table ''('
         'SELECT owner,\n'
         '       segment_name,\n'
         '       segment_type,\n'
         '       bytes\n'
         'FROM dba_segments\n'
         'WHERE owner NOT IN (' || sys_schemas || E')'
      ')'', max_long ''%s'', readonly ''true'')';

   migration_cost_estimate_sql text := E'CREATE VIEW %I.migration_cost_estimate AS\n'
      '   SELECT schema,\n'
      '          ''tables''::text   AS task_type,\n'
      '          count(*)::bigint AS task_content,\n'
      '          ''count''::text    AS task_unit,\n'
      '          ceil(count(*) / 10.0)::integer AS migration_hours\n'
      '   FROM %I.tables\n'
      '   GROUP BY schema\n'
      'UNION ALL\n'
      '   SELECT t.schema,\n'
      '          ''data_migration''::text,\n'
      '          sum(bytes)::bigint,\n'
      '          ''bytes''::text,\n'
      '          ceil(sum(bytes::float8) / 26843545600.0)::integer\n'
      '   FROM %I.segments AS s\n'
      '      JOIN %I.tables AS t\n'
      '         ON s.schema = t.schema\n'
      '            AND s.segment_name = t.table_name\n'
      '   WHERE s.segment_type = ''TABLE''\n'
      '   GROUP BY t.schema\n'
      'UNION ALL\n'
      '   SELECT schema,\n'
      '          ''functions'',\n'
      '          coalesce(sum(octet_length(source)), 0),\n'
      '          ''characters''::text,\n'
      '          ceil(coalesce(sum(octet_length(source)), 0) / 512.0)::integer\n'
      '   FROM %I.functions\n'
      '   GROUP BY schema\n'
      'UNION ALL\n'
      '   SELECT schema,\n'
      '          ''triggers'',\n'
      '          coalesce(sum(octet_length(trigger_body)), 0),\n'
      '          ''characters''::text,\n'
      '          ceil(coalesce(sum(octet_length(trigger_body)), 0) / 512.0)::integer\n'
      '   FROM %I.triggers\n'
      '   GROUP BY schema\n'
      'UNION ALL\n'
      '   SELECT schema,\n'
      '          ''packages'',\n'
      '          coalesce(sum(octet_length(source)), 0),\n'
      '          ''characters''::text,\n'
      '          ceil(coalesce(sum(octet_length(source)), 0) / 512.0)::integer\n'
      '   FROM %I.packages\n'
      '   WHERE is_body\n'
      '   GROUP BY schema\n'
      'UNION ALL\n'
      '   SELECT schema,\n'
      '          ''views'',\n'
      '          coalesce(sum(octet_length(definition)), 0),\n'
      '          ''characters''::text,\n'
      '          ceil(coalesce(sum(octet_length(definition)), 0) / 512.0)::integer\n'
      '   FROM %I.views\n'
      '   GROUP BY schema';

   test_error_sql text := E'CREATE TABLE %I.test_error (\n'
      '   log_time   timestamp with time zone NOT NULL DEFAULT current_timestamp,\n'
      '   schema     name                     NOT NULL,\n'
      '   table_name name                     NOT NULL,\n'
      '   rowid      text                     NOT NULL,\n'
      '   message    text                     NOT NULL,\n'
      '   PRIMARY KEY (schema, table_name, log_time, rowid)\n'
      ')';

   test_error_stats_sql text := E'CREATE TABLE %I.test_error_stats (\n'
      '   log_time   timestamp with time zone NOT NULL,\n'
      '   schema     name                     NOT NULL,\n'
      '   table_name name                     NOT NULL,\n'
      '   errcount   bigint                   NOT NULL,\n'
      '   PRIMARY KEY (schema, table_name, log_time)\n'
      ')';

BEGIN
   /* remember old setting */
   old_msglevel := current_setting('client_min_messages');
   /* make the output less verbose */
   SET LOCAL client_min_messages = warning;

   IF options ? 'max_long' THEN
      v_max_long := (options->>'max_long')::integer;
   END IF;

   /* CREATE SCHEMA IF NEEDED */
   EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema);

   /* tables */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.tables', schema);
   EXECUTE format(tables_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.tables IS ''Oracle tables on foreign server "%I"''', schema, server);
   /* columns */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.columns', schema);
   EXECUTE format(columns_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.columns IS ''columns of Oracle tables and views on foreign server "%I"''', schema, server);
   /* checks */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.checks', schema);
   EXECUTE format(checks_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.checks IS ''Oracle check constraints on foreign server "%I"''', schema, server);
   /* foreign_keys */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.foreign_keys', schema);
   EXECUTE format(foreign_keys_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.foreign_keys IS ''Oracle foreign key columns on foreign server "%I"''', schema, server);
   /* keys */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.keys', schema);
   EXECUTE format(keys_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.keys IS ''Oracle primary and unique key columns on foreign server "%I"''', schema, server);
   /* views */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.views', schema);
   EXECUTE format(views_sql, schema, server, v_max_long);
   /* func_src and functions */
   EXECUTE format('DROP VIEW IF EXISTS %I.functions', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.func_src', schema);
   EXECUTE format(func_src_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.func_src IS ''source lines for Oracle functions and procedures on foreign server "%I"''', schema, server);
   EXECUTE format(functions_sql, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.functions IS ''Oracle functions and procedures on foreign server "%I"''', schema, server);
   /* sequences */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.sequences', schema);
   EXECUTE format(sequences_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.sequences IS ''Oracle sequences on foreign server "%I"''', schema, server);
   /* index_exp and index_columns */
   EXECUTE format('DROP VIEW IF EXISTS %I.index_columns', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.index_exp', schema);
   EXECUTE format(index_exp_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.index_exp IS ''Oracle index columns on foreign server "%I"''', schema, server);
   EXECUTE format(index_columns_sql, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.index_columns IS ''Oracle index columns on foreign server "%I"''', schema, server);
   /* indexes */
   EXECUTE format('DROP VIEW IF EXISTS %I.indexes', schema);
   EXECUTE format(indexes_sql, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.indexes IS ''Oracle indexes on foreign server "%I"''', schema, server);
   /* partitions and subpartitions */
   EXECUTE format('DROP VIEW IF EXISTS %I.partitions', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.partition_columns', schema);
   EXECUTE format(partition_cols_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.partition_columns IS ''Oracle partition columns on foreign server "%I"''', schema, server);
   EXECUTE format(partitions_sql, schema);
   EXECUTE format('COMMENT ON VIEW %I.partitions IS ''Oracle partitions on foreign server "%I"''', schema, server);
   EXECUTE format('DROP VIEW IF EXISTS %I.subpartitions', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.subpartition_columns', schema);
   EXECUTE format(subpartition_cols_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.subpartition_columns IS ''Oracle subpartition columns on foreign server "%I"''', schema, server);
   EXECUTE format(subpartitions_sql, schema);
   EXECUTE format('COMMENT ON VIEW %I.subpartitions IS ''Oracle subpartitions on foreign server "%I"''', schema, server);
   /* schemas */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.schemas', schema);
   EXECUTE format(schemas_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.schemas IS ''Oracle schemas on foreign server "%I"''', schema, server);
   /* trig and triggers */
   EXECUTE format('DROP VIEW IF EXISTS %I.triggers', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.trig', schema);
   EXECUTE format(trig_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.trig IS ''Oracle triggers on foreign server "%I"''', schema, server);
   EXECUTE format(triggers_sql, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.triggers IS ''Oracle triggers on foreign server "%I"''', schema, server);
   /* pack_src and packages */
   EXECUTE format('DROP VIEW IF EXISTS %I.packages', schema);
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.pack_src', schema);
   EXECUTE format(pack_src_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.pack_src IS ''Oracle package source lines on foreign server "%I"''', schema, server);
   EXECUTE format(packages_sql, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.packages IS ''Oracle packages on foreign server "%I"''', schema, server);
   /* table_privs */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.table_privs', schema);
   EXECUTE format(table_privs_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.table_privs IS ''Privileges on Oracle tables on foreign server "%I"''', schema, server);
   /* column_privs */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.column_privs', schema);
   EXECUTE format(column_privs_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.column_privs IS ''Privileges on Oracle table columns on foreign server "%I"''', schema, server);
   /* segments */
   EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.segments', schema);
   EXECUTE format(segments_sql, schema, server, v_max_long);
   EXECUTE format('COMMENT ON FOREIGN TABLE %I.segments IS ''Size of Oracle objects on foreign server "%I"''', schema, server);
   /* migration_cost_estimate */
   EXECUTE format('DROP VIEW IF EXISTS %I.migration_cost_estimate', schema);
   EXECUTE format(migration_cost_estimate_sql, schema, schema, schema, schema, schema, schema, schema, schema);
   EXECUTE format('COMMENT ON VIEW %I.migration_cost_estimate IS ''Estimate of the migration costs per schema and object type''', schema);
   /* test_error */
   EXECUTE format('DROP TABLE IF EXISTS %I.test_error', schema);
   EXECUTE format(test_error_sql, schema);
   EXECUTE format('COMMENT ON TABLE %I.test_error IS ''Errors from the last run of "oracle_migrate_test_data"''', schema);
   /* test_error_stats */
   EXECUTE format('DROP TABLE IF EXISTS %I.test_error_stats', schema);
   EXECUTE format(test_error_stats_sql, schema);
   EXECUTE format('COMMENT ON TABLE %I.test_error_stats IS ''Cumulative errors from previous runs of "oracle_migrate_test_data"''', schema);

   /* reset client_min_messages */
   EXECUTE 'SET LOCAL client_min_messages = ' || old_msglevel;
END;$$;

COMMENT ON FUNCTION synchdb_create_oraviews(name, name, jsonb) IS
   'create Oracle foreign tables for the metadata of a foreign server';


-- Source: src/sql/fdw/source_positions.sql; sha256=d51081c1ac961c37c5113db04bf1cd182e6d0220aeee28b9fb352806b5846f64

-- Candidate-only source positions; these never authorize a CDC handoff.

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
   'Read diagnostic MySQL binlog coordinates; this does not establish a locked source transaction or authorize a CDC seed';

CREATE OR REPLACE FUNCTION synchdb_create_current_sqlserver_lsn_ft(
    p_schema name,  -- e.g. 'sqlserver_obj'
    p_server name   -- e.g. '<connector_name>_sqlserver'
) RETURNS void
LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT
SET search_path = pg_catalog
AS $synchdb_create_current_sqlserver_lsn_ft$
DECLARE
  /*
   * sys.fn_cdc_get_max_lsn() returns a varbinary(10). Format it here as Debezium's
   * Lsn.toString() hex-group representation ("%08x:%08x:%04x"). This is the CDC
   * capture high watermark, not the latest business commit or an FDW read-view
   * boundary. Capture lag can put it behind ordinary table reads; it must never
   * independently authorize a snapshot-to-CDC seed.
   */
  v_subqry text := $q$
     SELECT LOWER(STUFF(STUFF(
        CONVERT(varchar(20), sys.fn_cdc_get_max_lsn(), 2),
        9, 0, ':'), 18, 0, ':')) AS [max_lsn]
  $q$;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = p_server) THEN
    RAISE EXCEPTION 'Foreign server "%" does not exist', p_server
      USING HINT = 'Create it first: CREATE SERVER ... FOREIGN DATA WRAPPER tds_fdw OPTIONS(...);';
  END IF;
  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_schema);
  EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.%I', p_schema, 'current_lsn');
  EXECUTE format(
    'CREATE FOREIGN TABLE %I.%I (max_lsn text) ' ||
    'SERVER %I OPTIONS (query %L)',
    p_schema, 'current_lsn', p_server, v_subqry
  );
  RAISE NOTICE 'Recreated foreign table %.% on server %', p_schema, 'current_lsn', p_server;
END;
$synchdb_create_current_sqlserver_lsn_ft$;

COMMENT ON FUNCTION synchdb_create_current_sqlserver_lsn_ft(name, name) IS
   'Read the captured CDC high-water LSN as a diagnostic candidate; capture lag and the FDW read view are not coordinated';

CREATE OR REPLACE FUNCTION synchdb_create_current_scn_ft(
    p_schema name,   -- e.g. 'ora_obj'
    p_server name    -- e.g. 'oracle'
) RETURNS void
LANGUAGE plpgsql VOLATILE CALLED ON NULL INPUT
SET search_path = pg_catalog
AS $$
DECLARE
  v_subqry text := '(SELECT current_scn FROM v$database)';
BEGIN
  -- Ensure the foreign server exists
  IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = p_server) THEN
    RAISE EXCEPTION 'Foreign server "%" does not exist', p_server
      USING HINT = 'Create it first: CREATE SERVER ... FOREIGN DATA WRAPPER oracle_fdw OPTIONS(...);';
  END IF;

  -- Ensure target schema
  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_schema);

  -- Always recreate the FT
  EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.%I', p_schema, 'current_scn');

  EXECUTE format(
    'CREATE FOREIGN TABLE %I.%I (current_scn numeric) ' ||
    'SERVER %I OPTIONS ("table" %L, readonly ''true'')',
    p_schema, 'current_scn', p_server, v_subqry
  );

  RAISE NOTICE 'Recreated foreign table %.% on server %',
               p_schema, 'current_scn', p_server;
END;
$$;

COMMENT ON FUNCTION synchdb_create_current_scn_ft(name, name) IS
   'Read an Oracle AS OF SCN data-view candidate; a scalar SCN is not complete LogMiner recovery material';


-- Source: src/sql/fdw/metadata.sql; sha256=45963707e3e1c87068830973ba4d9bfd9544fdcf04ef3ea98d72f021a3dd9b3a

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



-- Source: src/sql/fdw/stage.sql; sha256=324a50a5e36388a1fd0bbc8708ceede6f3b0ae6f9ae9349afafada05a9da0262

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
  v_history_schema name;
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
    -- The frozen bootstrap keeps history inside its exact resource manifest.
    -- The compatibility wrapper retains the historical public-table location.
    v_history_schema := CASE WHEN p_record_attributes THEN 'public'::name ELSE p_stage_schema END;
    v_schema_tbl := CASE WHEN p_record_attributes THEN
      format('schema_history_%s',
             regexp_replace(lower(p_connector_name::text), '[^a-z0-9_]', '_', 'g'))
      ELSE 'schema_history' END;

    -- single-column table: line TEXT
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS %I.%I (
         line  text NOT NULL
       )',
      v_history_schema, v_schema_tbl
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
            AND lower(table_name) <> ''synchdb_olr_fence''
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
        v_history_schema, v_schema_tbl
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


-- Source: src/sql/fdw/schema.sql; sha256=12c8f3919a9173da66aa8fb817ed504b5764c6bd94cb571eb4258ab4ca2f701b

CREATE OR REPLACE FUNCTION synchdb_materialize_schema(
    p_connector_name name,            -- connector name for stats
    p_src_schema     name,            -- e.g. 'ora_stage'
    p_dst_schema     name,            -- e.g. 'psql_stage'
    p_on_exists      text DEFAULT 'error' -- only 'error' is supported for final targets
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r           record;
  v_exists    boolean;
  v_ft_oid    oid;
  v_dst_oid   oid;
  v_upd_count bigint;
BEGIN
  IF p_on_exists IN ('skip', 'replace', 'drop') THEN
    RAISE EXCEPTION 'Target policy "%" is not supported until target readiness and safe publication are available', p_on_exists
      USING ERRCODE = '0A000';
  ELSIF p_on_exists IS NULL OR p_on_exists <> 'error' THEN
    RAISE EXCEPTION 'Unknown p_on_exists value: %, use "error"', p_on_exists
      USING ERRCODE = '22023';
  END IF;
  -- ensure destination schema exists
  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_dst_schema);

  FOR r IN
    SELECT c.relname AS tbl
    FROM   pg_foreign_table ft
    JOIN   pg_class        c ON c.oid = ft.ftrelid
    JOIN   pg_namespace    n ON n.oid = c.relnamespace
    WHERE  n.nspname = p_src_schema
  LOOP
    SELECT EXISTS (
      SELECT 1
      FROM pg_class c2
      JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
      WHERE n2.nspname = p_dst_schema
        AND c2.relname = r.tbl
        AND c2.relkind = 'r'
    ) INTO v_exists;

    IF v_exists THEN
      RAISE EXCEPTION 'Destination table %.% already exists', p_dst_schema, r.tbl
        USING ERRCODE = '42P07';
    END IF;

    -- create empty materialized table with same columns
    EXECUTE format(
      'CREATE TABLE %I.%I AS TABLE %I.%I WITH NO DATA',
      p_dst_schema, r.tbl, p_src_schema, r.tbl
    );

    RAISE NOTICE 'Created %I.%I from %I.%I', r.tbl, p_dst_schema, r.tbl, p_src_schema;

    -- record stats after successful materialization
    PERFORM synchdb_set_snapstats(p_connector_name, 1::bigint, 0::bigint, 0::bigint, 0::bigint);

    ----------------------------------------------------------------
    -- Lookup source/destination OIDs
    ----------------------------------------------------------------
    SELECT c.oid
      INTO v_ft_oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = p_src_schema
      AND c.relname = r.tbl
      AND c.relkind = 'f'
    LIMIT 1;

    IF v_ft_oid IS NULL THEN
      RAISE NOTICE 'Could not find foreign table OID for %.% (skipping attrelid update)',
                   p_src_schema, r.tbl;
      CONTINUE;
    END IF;

    SELECT c.oid
      INTO v_dst_oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = p_dst_schema
      AND c.relname = r.tbl
      AND c.relkind = 'r'
    LIMIT 1;

    IF v_dst_oid IS NULL THEN
      RAISE NOTICE 'Could not find destination table OID for %.% (skipping attrelid update)',
                   p_dst_schema, r.tbl;
      CONTINUE;
    END IF;

    ----------------------------------------------------------------
    -- Update synchdb_attribute
    ----------------------------------------------------------------
    UPDATE public.synchdb_attribute
       SET attrelid = v_dst_oid
     WHERE attrelid = v_ft_oid;

    GET DIAGNOSTICS v_upd_count = ROW_COUNT;

    RAISE NOTICE 'Updated synchdb_attribute: % rows (attrelid % -> % for table %)',
                 v_upd_count, v_ft_oid::text, v_dst_oid::text, r.tbl;
  END LOOP;
END;
$$;

COMMENT ON FUNCTION synchdb_materialize_schema(name, name, name, text) IS
   'materialize table schema from staging to destination schema';

CREATE OR REPLACE FUNCTION synchdb_migrate_primary_keys(
    p_oraobj_schema name,          -- e.g. 'ora_obj'  (holds table "keys")
    p_dst_schema    name,          -- e.g. 'psql_stage'
    p_case_strategy text,
    p_source_owner name,
    p_table_names name[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r              record;
  v_exists       boolean;
  v_cname        text;
  v_sql          text;

  -- case strategy: 'lower' | 'upper' | 'asis'
  v_case_strategy text;
  v_tbl_expr      text;
  v_col_expr      text;
  v_collision    text;
BEGIN
  -- NULL retains the standalone helper's full scope; an empty scope is a no-op.
  IF p_table_names IS NOT NULL AND cardinality(p_table_names) = 0 THEN
    RETURN;
  END IF;
  ----------------------------------------------------------------------
  -- Resolve case strategy for table/column names
  -- p_case_strategy: 'lower' | 'upper' | 'asis'
  -- Backward compatible default: 'lower'
  ----------------------------------------------------------------------
  IF p_case_strategy IS NULL OR btrim(p_case_strategy) = '' THEN
    v_case_strategy := 'lower';
  ELSE
    v_case_strategy := lower(btrim(p_case_strategy));
    IF v_case_strategy NOT IN ('lower', 'upper', 'asis') THEN
      RAISE EXCEPTION 'Invalid p_case_strategy: %, expected lower|upper|asis', p_case_strategy;
    END IF;
  END IF;

  IF v_case_strategy = 'lower' THEN
    v_tbl_expr := 'lower(table_name)::name';
    v_col_expr := 'lower(column_name)';
  ELSIF v_case_strategy = 'upper' THEN
    v_tbl_expr := 'upper(table_name)::name';
    v_col_expr := 'upper(column_name)';
  ELSE
    v_tbl_expr := 'table_name::name';
    v_col_expr := 'column_name';
  END IF;

  ----------------------------------------------------------------------
  -- Iterate PK definitions from <p_oraobj_schema>.keys
  -- Apply the same case strategy to table and column names
  ----------------------------------------------------------------------
  EXECUTE format($q$
    SELECT tbl FROM (
      SELECT "schema", table_name, %1$s AS tbl FROM %2$I.keys
       WHERE is_primary = true
         AND (%3$L IS NULL OR upper("schema") = upper(%3$L))
         AND (%4$L::name[] IS NULL OR %1$s = ANY(%4$L::name[]))
       GROUP BY "schema", table_name, %1$s
    ) source_tables
    GROUP BY tbl HAVING count(*) > 1 LIMIT 1
  $q$, v_tbl_expr, p_oraobj_schema, p_source_owner, p_table_names) INTO v_collision;
  IF v_collision IS NOT NULL THEN
    RAISE EXCEPTION 'Ambiguous source schemas for primary key of table %', v_collision;
  END IF;

  FOR r IN
    EXECUTE format($q$
      SELECT
        %1$s AS tbl,
        string_agg(quote_ident(%2$s), ', ' ORDER BY position) AS cols
      FROM %3$I.keys
      WHERE is_primary = true
        AND (%4$L IS NULL OR upper("schema") = upper(%4$L))
        AND (%5$L::name[] IS NULL OR %1$s = ANY(%5$L::name[]))
      GROUP BY "schema", %1$s
    $q$, v_tbl_expr, v_col_expr, p_oraobj_schema, p_source_owner, p_table_names)
  LOOP
    -- Only act if destination table exists (relkind 'r' = ordinary table)
    SELECT EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = p_dst_schema
        AND c.relname  = r.tbl
        AND c.relkind  = 'r'
    ) INTO v_exists;

    IF NOT v_exists THEN
      RAISE NOTICE 'Skipping %.%: table not found', p_dst_schema, r.tbl;
      CONTINUE;
    END IF;

    -- Build constraint name (<= 63 chars), based on normalized table name
    v_cname := left(r.tbl, 55) || '_pkey';

    -- All identifiers (schema, table, constraint, columns) are quoted
    v_sql := format(
      'ALTER TABLE %I.%I ADD CONSTRAINT %I PRIMARY KEY (%s)',
      p_dst_schema, r.tbl, v_cname, r.cols
    );

    BEGIN
      EXECUTE v_sql;
      RAISE NOTICE 'Added primary key % on %.%', v_cname, p_dst_schema, r.tbl;

    EXCEPTION
      WHEN SQLSTATE '42P16' THEN  -- multiple primary keys not allowed
        RAISE NOTICE 'Skipping %.%: a primary key already exists', p_dst_schema, r.tbl;
      WHEN duplicate_object THEN   -- constraint name already exists
        RAISE;
      WHEN OTHERS THEN
        RAISE;
    END;
  END LOOP;
END;
$$;

COMMENT ON FUNCTION synchdb_migrate_primary_keys(name, name, text, name, name[]) IS
   'migrate primary keys for the supplied materialized table names; NULL means all tables';

CREATE OR REPLACE FUNCTION synchdb_migrate_primary_keys(
    p_oraobj_schema name,
    p_dst_schema name,
    p_case_strategy text,
    p_source_owner name
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM synchdb_migrate_primary_keys(p_oraobj_schema, p_dst_schema,
                                      p_case_strategy, p_source_owner, NULL::name[]);
END;
$$;

COMMENT ON FUNCTION synchdb_migrate_primary_keys(name, name, text, name) IS
   'migrate primary keys from staging to destination schema';

CREATE OR REPLACE FUNCTION synchdb_migrate_primary_keys(
    p_oraobj_schema name,
    p_dst_schema name,
    p_case_strategy text DEFAULT 'asis'
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM synchdb_migrate_primary_keys(p_oraobj_schema, p_dst_schema,
                                      p_case_strategy, NULL::name);
END;
$$;


-- Source: src/sql/fdw/column_mappings.sql; sha256=b52c2bbff5f80c03f02cba992d4aae17ee063861c1338337078628e116fe9de3

CREATE OR REPLACE FUNCTION synchdb_apply_column_mappings(
    p_src_schema      name,             -- e.g. 'psql_stage'
    p_connector_name  name,             -- connector name to filter objmap
    p_desired_db      name,             -- e.g. 'free' or 'wrongdb'
    p_desired_schema  name,              -- e.g. 'dbzuser'; NULL = don't enforce
    p_case_strategy   text,
    p_table_names     name[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  -- RENAME pass vars
  r          RECORD;
  parts      text[];
  s_db       text;
  s_schema   text;
  s_table    text;
  s_col      text;

  dst_parts  text[];
  d_col      text;

  -- DATATYPE pass vars
  rdt        RECORD;
  dt_parts   text[];
  dt_db      text;
  dt_schema  text;
  dt_table   text;
  dt_col     text;
  target_col text;   -- column name to ALTER TYPE (post-rename if applicable)

  dt_spec    text;   -- raw 'dstobj' from datatype row, e.g. 'varchar|128'
  dt_name    text;   -- left side of '|'
  dt_len_txt text;   -- right side of '|'
  dt_len     int;
  dtype_sql  text;   -- rendered SQL type, e.g. 'varchar(128)' or 'text'
BEGIN
  -- Scope names are actual, already normalized materialized table names.
  IF p_table_names IS NOT NULL AND cardinality(p_table_names) = 0 THEN
    RETURN;
  END IF;
  ---------------------------------------------------------------------------
  -- Pass 1: COLUMN RENAME mappings (unchanged)
  ---------------------------------------------------------------------------
  FOR r IN
    SELECT m.srcobj, m.dstobj
    FROM synchdb_objmap AS m
    WHERE m.objtype = 'column'
      AND m.enabled
      AND m.name = p_connector_name
      AND m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''
  LOOP
    parts := regexp_split_to_array(r.srcobj, '\.');
    s_db := NULL; s_schema := NULL; s_table := NULL; s_col := NULL;

    IF array_length(parts,1) = 4 THEN
      s_db := parts[1]; s_schema := parts[2]; s_table := parts[3]; s_col := parts[4];
    ELSIF array_length(parts,1) = 3 THEN
      s_db := parts[1]; s_table := parts[2]; s_col := parts[3];
    ELSE
      CONTINUE;
    END IF;

	IF p_case_strategy = 'lower' THEN
      s_col := lower(s_col);
	  s_table := lower(s_table);
    ELSIF p_case_strategy = 'upper' THEN
      s_col := upper(s_col);
	  s_table := upper(s_table);
    ELSE
      -- 'asis' → leave untouched
      NULL;
    END IF;

    IF p_desired_db IS NOT NULL AND s_db IS DISTINCT FROM p_desired_db THEN
      CONTINUE;
    END IF;

    IF p_desired_schema IS NOT NULL
       AND s_schema IS NOT NULL
       AND s_schema <> p_desired_schema THEN
      CONTINUE;
    END IF;

    IF p_table_names IS NOT NULL
       AND (s_table::name = ANY(p_table_names)) IS NOT TRUE THEN
      CONTINUE;
    END IF;

    dst_parts := regexp_split_to_array(r.dstobj, '\.');
    d_col := dst_parts[array_length(dst_parts,1)];

    IF NOT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
        AND table_name   = s_table
        AND column_name  = s_col
    ) THEN
      RAISE NOTICE 'Skipping %.%: source column % not found', p_src_schema, s_table, s_col;
      CONTINUE;
    END IF;

    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
        AND table_name   = s_table
        AND column_name  = d_col
    ) THEN
      RAISE NOTICE 'Skipping %.%: destination column % already exists', p_src_schema, s_table, d_col;
      CONTINUE;
    END IF;

    EXECUTE format('ALTER TABLE %I.%I RENAME COLUMN %I TO %I',
                   p_src_schema, s_table, s_col, d_col);

    RAISE NOTICE 'Renamed %.%: % -> %', p_src_schema, s_table, s_col, d_col;
  END LOOP;

  ---------------------------------------------------------------------------
  -- Pass 2: DATATYPE mappings with 'type|length' grammar in dstobj
  -- - Find target column name (renamed if a COLUMN mapping exists for same srcobj)
  -- - Render type as: length=0 -> "type", else "type(length)"
  ---------------------------------------------------------------------------
  FOR rdt IN
    SELECT m.srcobj, m.dstobj
    FROM synchdb_objmap AS m
    WHERE m.objtype = 'datatype'
      AND m.enabled
      AND m.name = p_connector_name
      AND m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''
  LOOP
    -- Parse srcobj => db(.schema).table.column
    dt_parts := regexp_split_to_array(rdt.srcobj, '\.');
    dt_db := NULL; dt_schema := NULL; dt_table := NULL; dt_col := NULL;

    IF array_length(dt_parts,1) = 4 THEN
      dt_db := dt_parts[1]; dt_schema := dt_parts[2]; dt_table := dt_parts[3]; dt_col := dt_parts[4];
    ELSIF array_length(dt_parts,1) = 3 THEN
      dt_db := dt_parts[1]; dt_table := dt_parts[2]; dt_col := dt_parts[3];
    ELSE
      CONTINUE;
    END IF;

    IF p_case_strategy = 'lower' THEN
      dt_col := lower(dt_col);
      dt_table := lower(dt_table);
    ELSIF p_case_strategy = 'upper' THEN
      dt_col := upper(dt_col);
      dt_table := upper(dt_table);
    ELSE
      -- 'asis' → leave untouched
      NULL;
    END IF;

    IF p_desired_db IS NOT NULL AND dt_db IS DISTINCT FROM p_desired_db THEN
      CONTINUE;
    END IF;

    IF p_desired_schema IS NOT NULL
       AND dt_schema IS NOT NULL
       AND dt_schema <> p_desired_schema THEN
      CONTINUE;
    END IF;

    IF p_table_names IS NOT NULL
       AND (dt_table::name = ANY(p_table_names)) IS NOT TRUE THEN
      CONTINUE;
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = p_src_schema
        AND table_name   = dt_table
    ) THEN
      RAISE NOTICE 'Skipping %.%: table not found for datatype mapping', p_src_schema, dt_table;
      CONTINUE;
    END IF;

    -- If a COLUMN mapping exists for same srcobj, use its dst column name
    SELECT (regexp_split_to_array(m2.dstobj, '\.'))[
             array_length(regexp_split_to_array(m2.dstobj, '\.'), 1)
           ]
    INTO target_col
    FROM synchdb_objmap m2
    WHERE m2.objtype = 'column'
      AND m2.enabled
      AND m2.name = p_connector_name
      AND m2.srcobj = rdt.srcobj
    LIMIT 1;

    IF target_col IS NULL OR btrim(target_col) = '' THEN
      target_col := dt_col;  -- no rename mapping; use original name
    END IF;

    -- Ensure target column exists
    IF NOT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
        AND table_name   = dt_table
        AND column_name  = target_col
    ) THEN
      RAISE NOTICE 'Skipping %.%: target column % not found for datatype mapping', p_src_schema, dt_table, target_col;
      CONTINUE;
    END IF;

    -- Parse datatype spec 'type|len'
    dt_spec    := rdt.dstobj;
    dt_name    := btrim(split_part(lower(dt_spec), '|', 1));
    dt_len_txt := btrim(split_part(dt_spec, '|', 2));
    dt_len     := COALESCE(NULLIF(dt_len_txt, '')::int, 0);

    IF dt_name IS NULL OR dt_name = '' THEN
      RAISE NOTICE 'Skipping %.%: invalid datatype spec (empty type) for column %', p_src_schema, dt_table, target_col;
      CONTINUE;
    END IF;

    IF dt_len > 0 THEN
      dtype_sql := format('%s(%s)', dt_name, dt_len);
    ELSE
      dtype_sql := dt_name;
    END IF;

    BEGIN
      EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I TYPE %s',
                     p_src_schema, dt_table, target_col, dtype_sql);
      RAISE NOTICE 'Altered datatype %.%: % TYPE %', p_src_schema, dt_table, target_col, dtype_sql;
    EXCEPTION
      WHEN others THEN
        -- If the cast is not binary-coercible, a USING clause may be needed.
        RAISE;
    END;
  END LOOP;
END;
$$;

COMMENT ON FUNCTION synchdb_apply_column_mappings(name, name, name, name, text, name[]) IS
   'transform column mappings for the supplied materialized table names; NULL means all tables';

CREATE OR REPLACE FUNCTION synchdb_apply_column_mappings(
    p_src_schema name,
    p_connector_name name,
    p_desired_db name,
    p_desired_schema name DEFAULT NULL,
    p_case_strategy text DEFAULT 'asis'
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM synchdb_apply_column_mappings(p_src_schema, p_connector_name,
                                        p_desired_db, p_desired_schema,
                                        p_case_strategy, NULL::name[]);
END;
$$;

COMMENT ON FUNCTION synchdb_apply_column_mappings(name, name, name, name, text) IS
   'transform column name mappings based on synchdb_objmap';


-- Source: src/sql/fdw/data.sql; sha256=2d6cf15e06e06f40b2df2eb8708f330d8b90cb92061c952872c8377208cfd4ab

CREATE OR REPLACE FUNCTION synchdb_migrate_data_with_transforms(
    p_src_schema        name,                 -- e.g. 'ora_stage' (foreign tables)
    p_connector_name    name,                 -- connector for stats + objmap filter
    p_dst_schema        name,                 -- e.g. 'psql_stage' (real tables)
    p_desired_db        name,                 -- e.g. 'free'
    p_offset            text,                 -- offset value (lsn, scn, binlog pos..etc)
    p_case_strategy     text,                 -- 'lower' | 'upper' | 'asis' ('as_is' is an alias)
    p_desired_schema    name DEFAULT NULL,    -- e.g. 'dbzuser'
    p_do_truncate       boolean DEFAULT false,
    p_rows_per_tick     integer DEFAULT 0,    -- <=0/NULL = one statement; positive values are unsupported
    p_continue_on_error boolean DEFAULT true,
    p_batch_subxact     boolean DEFAULT true  -- retained for signature compatibility; batching is unsupported
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r                 record;
  dst_list          text;
  src_list          text;
  v_rows            bigint;

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

  v_case_strategy   text;
  v_ambiguous_columns text;
BEGIN
  -- Repeated unordered scans cannot partition a foreign table reliably.
  -- Reject the legacy mode before any table-level handler or write.
  IF p_rows_per_tick > 0 THEN
    RAISE EXCEPTION USING
      ERRCODE = '0A000',
      MESSAGE = 'Batched FDW data migration is not supported',
      HINT = 'Use p_rows_per_tick = 0 for a single source scan.';
  END IF;

  v_case_strategy := COALESCE(NULLIF(lower(btrim(p_case_strategy)), ''), 'asis');
  IF v_case_strategy = 'as_is' THEN
    v_case_strategy := 'asis';
  END IF;
  IF v_case_strategy NOT IN ('lower', 'upper', 'asis') THEN
    RAISE EXCEPTION 'Invalid p_case_strategy: %, expected lower|upper|asis', p_case_strategy;
  END IF;

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
        WHEN v_case_strategy = 'lower' THEN lower(c.relname)
        WHEN v_case_strategy = 'upper' THEN upper(c.relname)
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

    BEGIN
      ------------------------------------------------------------------
      -- Build column/expr lists.
      -- Match source rules using the requested case strategy and preserve
      -- actual source/destination identifiers when generating SQL.
      ------------------------------------------------------------------
      RAISE NOTICE 'Migrating table: %', v_tbl_display;

      -- Source-name folding must not rewrite an explicit destination identifier.
      -- Reject an ambiguous source inventory instead of choosing its first column.
      SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
      INTO v_ambiguous_columns
      FROM (
        SELECT column_name, ordinal_position,
               count(*) OVER (
                 PARTITION BY CASE v_case_strategy
                   WHEN 'lower' THEN lower(column_name)
                   WHEN 'upper' THEN upper(column_name)
                   ELSE column_name
                 END
               ) AS matches
        FROM information_schema.columns
        WHERE table_schema = p_src_schema
          AND table_name = r.src_tbl_name
      ) AS source_columns
      WHERE matches > 1;

      IF v_ambiguous_columns IS NOT NULL THEN
        RAISE EXCEPTION 'Ambiguous source columns in %.% under case strategy %: %',
                        p_src_schema, r.src_tbl_name, v_case_strategy, v_ambiguous_columns;
      END IF;

      WITH dst_cols AS (
        SELECT c.ordinal_position,
               CASE v_case_strategy
                 WHEN 'lower' THEN lower(c.column_name)
                 WHEN 'upper' THEN upper(c.column_name)
                 ELSE c.column_name
               END AS dst_col_canon,
               c.column_name AS dst_col_ident
        FROM information_schema.columns c
        WHERE c.table_schema = p_dst_schema
          AND c.table_name = r.dst_tbl_name
      ),
      rules AS (
        SELECT m.objtype,
               regexp_split_to_array(m.srcobj, '\.') AS src_parts,
               regexp_split_to_array(m.dstobj, '\.') AS dst_parts,
               m.dstobj
        FROM synchdb_objmap m
        WHERE m.objtype IN ('column', 'transform')
          AND m.enabled
          AND m.name = p_connector_name
          AND m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''
      ),
      scoped_rules AS (
        SELECT objtype,
               CASE v_case_strategy
                 WHEN 'lower' THEN lower(src_parts[array_length(src_parts, 1) - 1])
                 WHEN 'upper' THEN upper(src_parts[array_length(src_parts, 1) - 1])
                 ELSE src_parts[array_length(src_parts, 1) - 1]
               END AS tbl,
               CASE v_case_strategy
                 WHEN 'lower' THEN lower(src_parts[array_length(src_parts, 1)])
                 WHEN 'upper' THEN upper(src_parts[array_length(src_parts, 1)])
                 ELSE src_parts[array_length(src_parts, 1)]
               END AS src_col,
               dst_parts[array_length(dst_parts, 1)] AS dst_col_ident,
               dstobj AS expr
        FROM rules
        WHERE (
          array_length(src_parts, 1) = 3
          AND (src_parts[1] = p_src_schema::text OR src_parts[1] = p_desired_db::text)
        ) OR (
          array_length(src_parts, 1) = 4
          AND src_parts[1] = p_desired_db::text
          AND (p_desired_schema IS NULL OR p_desired_schema = ''
               OR src_parts[2] = p_desired_schema::text)
        )
      ),
      col_map AS (
        SELECT d.ordinal_position,
               d.dst_col_ident,
               COALESCE(
                 -- A scalar subquery rejects multiple matching rename rules.
                 (SELECT cm.src_col
                  FROM scoped_rules cm
                  WHERE cm.objtype = 'column'
                    AND cm.tbl = r.tbl_canon
                    AND cm.dst_col_ident = d.dst_col_ident),
                 d.dst_col_canon
               ) AS src_col_canon
        FROM dst_cols d
      ),
      src_presence AS (
        SELECT CASE v_case_strategy
                 WHEN 'lower' THEN lower(column_name)
                 WHEN 'upper' THEN upper(column_name)
                 ELSE column_name
               END AS src_col_canon,
               column_name AS src_col_ident
        FROM information_schema.columns
        WHERE table_schema = p_src_schema
          AND table_name = r.src_tbl_name
      ),
      exprs AS (
        SELECT m.ordinal_position,
               m.dst_col_ident,
               CASE
                 WHEN sp.src_col_ident IS NULL THEN 'NULL'
                 ELSE COALESCE(
                   -- A scalar subquery also rejects ambiguous transform rules.
                   (SELECT replace(t.expr, '%d', quote_ident(sp.src_col_ident))
                    FROM scoped_rules t
                    WHERE t.objtype = 'transform'
                      AND t.tbl = r.tbl_canon
                      AND t.src_col = m.src_col_canon),
                   quote_ident(sp.src_col_ident)
                 )
               END AS src_expr
        FROM col_map m
        LEFT JOIN src_presence sp ON sp.src_col_canon = m.src_col_canon
      )
      SELECT string_agg(quote_ident(dst_col_ident), ', ' ORDER BY ordinal_position),
             string_agg(src_expr, ', ' ORDER BY ordinal_position)
      INTO dst_list, src_list
      FROM exprs;

      IF dst_list IS NULL OR src_list IS NULL THEN
        RAISE NOTICE 'Skipping %.%: no column metadata', p_dst_schema, r.dst_tbl_name;
        CONTINUE;
      END IF;

      -- A failed TRUNCATE must roll back this table and reach the common
      -- error handler. Continuing here would append to the old contents.
      IF p_do_truncate THEN
        EXECUTE format('TRUNCATE %I.%I', p_dst_schema, r.dst_tbl_name);
      END IF;

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

      -- Only a successful whole-table copy clears its previous error.
      IF v_err_tbl_exists THEN
        EXECUTE format(
          'DELETE FROM %I.%I WHERE connector_name = $1 AND tbl = $2',
          'public', v_err_tbl_ident
        )
        USING p_connector_name, v_tbl_display;
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
    p_desired_schema  name,                 -- e.g. 'dbzuser'
    p_do_truncate     boolean,
    p_rows_per_tick   integer,              -- <=0/NULL = one statement; positive values are unsupported
    p_case_strategy   text                  -- 'lower' | 'upper' | 'asis' ('as_is' is an alias)
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r           record;
  dst_list    text;
  src_list    text;
  v_rows      bigint;
  v_case_strategy text;
  v_ambiguous_columns text;
BEGIN
  -- Repeated unordered scans cannot partition a foreign table reliably.
  -- Reject the legacy mode before any table-level handler or write.
  IF p_rows_per_tick > 0 THEN
    RAISE EXCEPTION USING
      ERRCODE = '0A000',
      MESSAGE = 'Batched FDW data migration is not supported',
      HINT = 'Use p_rows_per_tick = 0 for a single source scan.';
  END IF;

  v_case_strategy := COALESCE(NULLIF(lower(btrim(p_case_strategy)), ''), 'asis');
  IF v_case_strategy = 'as_is' THEN
    v_case_strategy := 'asis';
  END IF;
  IF v_case_strategy NOT IN ('lower', 'upper', 'asis') THEN
    RAISE EXCEPTION 'Invalid p_case_strategy: %, expected lower|upper|asis', p_case_strategy;
  END IF;

  ----------------------------------------------------------------------
  -- Iterate all source FTs that have a same-named real table
  -- in the destination schema.
  --
  -- tbl_canon follows the requested source-name strategy; src_tbl_name
  -- and dst_tbl_name preserve the exact relname for SQL and column
  -- metadata, so case-variants stay distinct.
  ----------------------------------------------------------------------
  FOR r IN
    SELECT
      CASE v_case_strategy
        WHEN 'lower' THEN lower(c.relname)
        WHEN 'upper' THEN upper(c.relname)
        ELSE c.relname
      END              AS tbl_canon,
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
    --                   keyed by the exact FT name; case collisions fail.
    ------------------------------------------------------------------
    -- Source-name folding must not rewrite an explicit destination identifier.
    -- Reject an ambiguous source inventory instead of choosing its first column.
    SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
    INTO v_ambiguous_columns
    FROM (
      SELECT column_name, ordinal_position,
             count(*) OVER (
               PARTITION BY CASE v_case_strategy
                 WHEN 'lower' THEN lower(column_name)
                 WHEN 'upper' THEN upper(column_name)
                 ELSE column_name
               END
             ) AS matches
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
        AND table_name = r.src_tbl_name
    ) AS source_columns
    WHERE matches > 1;

    IF v_ambiguous_columns IS NOT NULL THEN
      RAISE EXCEPTION 'Ambiguous source columns in %.% under case strategy %: %',
                      p_src_schema, r.src_tbl_name, v_case_strategy, v_ambiguous_columns;
    END IF;

    WITH dst_cols AS (
      SELECT c.ordinal_position,
             CASE v_case_strategy
               WHEN 'lower' THEN lower(c.column_name)
               WHEN 'upper' THEN upper(c.column_name)
               ELSE c.column_name
             END AS dst_col_canon,
             c.column_name AS dst_col_ident
      FROM information_schema.columns c
      WHERE c.table_schema = p_dst_schema
        AND c.table_name = r.dst_tbl_name
    ),
    rules AS (
      SELECT m.objtype,
             regexp_split_to_array(m.srcobj, '\.') AS src_parts,
             regexp_split_to_array(m.dstobj, '\.') AS dst_parts,
             m.dstobj
      FROM synchdb_objmap m
      WHERE m.objtype IN ('column', 'transform')
        AND m.enabled
        AND m.name = p_connector_name
        AND m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''
    ),
    scoped_rules AS (
      SELECT objtype,
             CASE v_case_strategy
               WHEN 'lower' THEN lower(src_parts[array_length(src_parts, 1) - 1])
               WHEN 'upper' THEN upper(src_parts[array_length(src_parts, 1) - 1])
               ELSE src_parts[array_length(src_parts, 1) - 1]
             END AS tbl,
             CASE v_case_strategy
               WHEN 'lower' THEN lower(src_parts[array_length(src_parts, 1)])
               WHEN 'upper' THEN upper(src_parts[array_length(src_parts, 1)])
               ELSE src_parts[array_length(src_parts, 1)]
             END AS src_col,
             dst_parts[array_length(dst_parts, 1)] AS dst_col_ident,
             dstobj AS expr
      FROM rules
      WHERE (
        array_length(src_parts, 1) = 3
        AND (src_parts[1] = p_src_schema::text OR src_parts[1] = p_desired_db::text)
      ) OR (
        array_length(src_parts, 1) = 4
        AND src_parts[1] = p_desired_db::text
        AND (p_desired_schema IS NULL OR p_desired_schema = ''
             OR src_parts[2] = p_desired_schema::text)
      )
    ),
    col_map AS (
      SELECT d.ordinal_position,
             d.dst_col_ident,
             COALESCE(
               -- A scalar subquery rejects multiple matching rename rules.
               (SELECT cm.src_col
                FROM scoped_rules cm
                WHERE cm.objtype = 'column'
                  AND cm.tbl = r.tbl_canon
                  AND cm.dst_col_ident = d.dst_col_ident),
               d.dst_col_canon
             ) AS src_col_canon
      FROM dst_cols d
    ),
    src_presence AS (
      SELECT CASE v_case_strategy
               WHEN 'lower' THEN lower(column_name)
               WHEN 'upper' THEN upper(column_name)
               ELSE column_name
             END AS src_col_canon,
             column_name AS src_col_ident
      FROM information_schema.columns
      WHERE table_schema = p_src_schema
        AND table_name = r.src_tbl_name
    ),
    exprs AS (
      SELECT m.ordinal_position,
             m.dst_col_ident,
             CASE
               WHEN sp.src_col_ident IS NULL THEN 'NULL'
               ELSE COALESCE(
                 -- A scalar subquery also rejects ambiguous transform rules.
                 (SELECT replace(t.expr, '%d', quote_ident(sp.src_col_ident))
                  FROM scoped_rules t
                  WHERE t.objtype = 'transform'
                    AND t.tbl = r.tbl_canon
                    AND t.src_col = m.src_col_canon),
                 quote_ident(sp.src_col_ident)
               )
             END AS src_expr
      FROM col_map m
      LEFT JOIN src_presence sp ON sp.src_col_canon = m.src_col_canon
    )
    SELECT string_agg(quote_ident(dst_col_ident), ', ' ORDER BY ordinal_position),
           string_agg(src_expr, ', ' ORDER BY ordinal_position)
    INTO dst_list, src_list
    FROM exprs;

    IF dst_list IS NULL OR src_list IS NULL THEN
      RAISE NOTICE 'Skipping %.%: no column metadata', p_dst_schema, r.dst_tbl_name;
      CONTINUE;
    END IF;

    IF p_do_truncate THEN
      EXECUTE format('TRUNCATE %I.%I', p_dst_schema, r.dst_tbl_name);
    END IF;

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

  END LOOP;
END;
$$;

COMMENT ON FUNCTION synchdb_migrate_data_with_transforms_nosubs(name, name, name, name, name, boolean, int, text) IS
   'migrate data with source-name case strategy and transform expressions - all or nothing mode';

-- Preserve the historical seven-argument entry point and its lower-case matching.
CREATE OR REPLACE FUNCTION synchdb_migrate_data_with_transforms_nosubs(
    p_src_schema      name,
    p_connector_name  name,
    p_dst_schema      name,
    p_desired_db      name,
    p_desired_schema  name DEFAULT NULL,
    p_do_truncate     boolean DEFAULT false,
    p_rows_per_tick   integer DEFAULT 0
) RETURNS void
LANGUAGE sql
AS $$
  SELECT synchdb_migrate_data_with_transforms_nosubs(
    $1, $2, $3, $4, $5, $6, $7, 'lower'::text
  );
$$;

COMMENT ON FUNCTION synchdb_migrate_data_with_transforms_nosubs(name, name, name, name, name, boolean, int) IS
   'compatibility wrapper for lower-case source matching - all or nothing mode';


-- Source: src/sql/fdw/table_mappings.sql; sha256=4d3a1faf3429c24be955d022d4d2c1900de9da610a5e8f981542dadad8eb1f43

CREATE OR REPLACE FUNCTION synchdb_apply_table_mappings(
    p_src_schema      name,               -- e.g. 'psql_stage'
    p_connector_name  name,               -- filter objmap rows by connector name
    p_desired_db      name,               -- e.g. 'free' (must match the db token in srcobj)
    p_desired_schema  name,                -- only enforced if srcobj includes a schema
    p_case_strategy   text,
    p_table_names     name[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r RECORD;

  parts    text[];
  s_db     text;
  s_schema text;
  s_table  text;

  d_schema text;
  d_table  text;
  dst_parts text[];
  v_error_table text;
  v_failed boolean;
BEGIN
  -- Scope names refer to source tables before a mapping renames or moves them.
  IF p_table_names IS NOT NULL AND cardinality(p_table_names) = 0 THEN
    RETURN;
  END IF;
  v_error_table := 'synchdb_fdw_snapshot_errors_'
      || regexp_replace(lower(p_connector_name::text), '[^a-z0-9_]', '_', 'g');

  FOR r IN
    SELECT m.srcobj, m.dstobj
    FROM   synchdb_objmap AS m
    WHERE  m.objtype = 'table'
      AND  m.enabled
      AND  m.name = p_connector_name
      AND  m.dstobj IS NOT NULL AND btrim(m.dstobj) <> ''
  LOOP
    -- Parse srcobj as db(.schema).table
    parts := regexp_split_to_array(r.srcobj, '\.');
    s_db := NULL; s_schema := NULL; s_table := NULL;

    IF array_length(parts,1) = 3 THEN
      s_db := parts[1]; s_schema := parts[2]; s_table := parts[3];
    ELSIF array_length(parts,1) = 2 THEN
      s_db := parts[1]; s_table := parts[2];         -- no schema segment
    ELSE
      CONTINUE;                                      -- unsupported form
    END IF;

    IF p_case_strategy = 'lower' THEN
      s_table := lower(s_table);
    ELSIF p_case_strategy = 'upper' THEN
      s_table := upper(s_table);
    ELSE
      -- 'asis' → leave untouched
      NULL;
    END IF;

    -- Require matching database
    IF p_desired_db IS NOT NULL AND s_db IS DISTINCT FROM p_desired_db THEN
      CONTINUE;
    END IF;

    -- Only enforce desired schema if a schema segment exists in srcobj
    IF p_desired_schema IS NOT NULL
       AND s_schema IS NOT NULL
       AND s_schema <> p_desired_schema THEN
      CONTINUE;
    END IF;

    IF p_table_names IS NOT NULL
       AND (s_table::name = ANY(p_table_names)) IS NOT TRUE THEN
      CONTINUE;
    END IF;

    -- Parse destination: "schema.table" or just "table" (defaults to public)
    dst_parts := regexp_split_to_array(r.dstobj, '\.');
    IF cardinality(dst_parts) = 2 THEN
      d_schema := dst_parts[1];
      d_table := dst_parts[2];
    ELSIF cardinality(dst_parts) = 1 THEN
      d_schema := 'public';
      d_table  := r.dstobj;
    ELSE
      RAISE EXCEPTION 'Invalid table mapping destination: %', r.dstobj USING ERRCODE = '22023';
    END IF;
    IF btrim(d_schema) = '' OR btrim(d_table) = ''
       OR octet_length(d_schema) > current_setting('max_identifier_length')::int
       OR octet_length(d_table) > current_setting('max_identifier_length')::int THEN
      RAISE EXCEPTION 'Invalid or overlong table mapping destination: %', r.dstobj
        USING ERRCODE = '22023';
    END IF;

    -- Only act if the source table exists in p_src_schema
    IF NOT EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = p_src_schema
        AND c.relkind = 'r'
        AND c.relname = s_table
    ) THEN
      CONTINUE;
    END IF;

    -- Failed copies must not publish an empty or incomplete target.
    IF to_regclass(format('public.%I', v_error_table)) IS NOT NULL THEN
      EXECUTE format(
        'SELECT EXISTS (SELECT 1 FROM public.%I
          WHERE connector_name = $1
            AND CASE $3 WHEN ''lower'' THEN lower(split_part(tbl, ''.'', -1))
                        WHEN ''upper'' THEN upper(split_part(tbl, ''.'', -1))
                        ELSE split_part(tbl, ''.'', -1) END = $2)',
        v_error_table)
        INTO v_failed USING p_connector_name, s_table, p_case_strategy;
      IF v_failed THEN
        RAISE NOTICE 'Skipping mapping of %.%: snapshot has unresolved errors',
                     p_src_schema, s_table;
        CONTINUE;
      END IF;
    END IF;

    -- An identity mapping is already satisfied, including its existing table.
    IF p_src_schema = d_schema AND s_table = d_table THEN
      CONTINUE;
    END IF;

    -- Ensure destination schema
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', d_schema);

    -- Mapping never grants permission to overwrite an existing relation.
    IF EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = d_schema
        AND c.relname = d_table
    ) THEN
      RAISE EXCEPTION 'Cannot map %.% to %.%: destination already exists',
                      p_src_schema, s_table, d_schema, d_table
        USING ERRCODE = '42P07';
    END IF;

    -- Same-schema rename vs. cross-schema move (rename first to avoid collisions)
    IF p_src_schema = d_schema THEN
      IF s_table <> d_table THEN
        EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', p_src_schema, s_table, d_table);
      END IF;
    ELSE
      IF s_table <> d_table THEN
        EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', p_src_schema, s_table, d_table);
        EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I', p_src_schema, d_table, d_schema);
      ELSE
        EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I', p_src_schema, s_table, d_schema);
      END IF;
    END IF;
  END LOOP;
END;
$$;

COMMENT ON FUNCTION synchdb_apply_table_mappings(name, name, name, name, text, name[]) IS
   'transform table mappings for the supplied materialized source table names; NULL means all tables';

CREATE OR REPLACE FUNCTION synchdb_apply_table_mappings(
    p_src_schema name,
    p_connector_name name,
    p_desired_db name,
    p_desired_schema name DEFAULT NULL,
    p_case_strategy text DEFAULT 'asis'
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM synchdb_apply_table_mappings(p_src_schema, p_connector_name,
                                       p_desired_db, p_desired_schema,
                                       p_case_strategy, NULL::name[]);
END;
$$;

COMMENT ON FUNCTION synchdb_apply_table_mappings(name, name, name, name, text) IS
   'transform table names according to synchdb_objmap';


-- Source: src/sql/fdw/finalize.sql; sha256=03f8893f7d803bb3b08ddcce764574194d08ed3746e77ea7633f6573eea1b2fb

-- Cleanup authority is the exact manifest returned by the coordinator.
CREATE OR REPLACE FUNCTION synchdb_finalize_initial_snapshot(p_resources jsonb)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE v_schema name;
BEGIN
    SELECT n.nspname INTO STRICT v_schema FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='synchdb';
    EXECUTE pg_catalog.format('SELECT %I.synchdb_bootstrap_cleanup($1)',v_schema) USING p_resources;
END;
$$;

CREATE OR REPLACE FUNCTION synchdb_finalize_initial_snapshot(
    p_source_schema name,p_stage_schema name,p_connector_name name,p_meta_schema name
) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'Name-only snapshot finalization is not supported'
        USING ERRCODE='0A000',HINT='The bootstrap coordinator cleans only objects in its exact OID resource manifest.';
END;
$$;


-- Source: src/sql/fdw/bootstrap.sql; sha256=95e9f6c9040a1c5db4665e2debd10b3b17c8a5cb54f4467f56f1187e6a5e5695

CREATE OR REPLACE FUNCTION synchdb_bootstrap_normalize_name(text,text) RETURNS text
AS '$libdir/synchdb','synchdb_bootstrap_normalize_name' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION synchdb_bootstrap_type(text,text,jsonb,jsonb) RETURNS text
AS '$libdir/synchdb','synchdb_bootstrap_type' LANGUAGE C VOLATILE PARALLEL UNSAFE;
CREATE OR REPLACE FUNCTION synchdb_bootstrap_value(text,text) RETURNS record
AS '$libdir/synchdb','synchdb_bootstrap_value' LANGUAGE C VOLATILE PARALLEL UNSAFE;
CREATE OR REPLACE FUNCTION synchdb_bootstrap_source_id(text,text,text,text) RETURNS text
AS '$libdir/synchdb','synchdb_bootstrap_source_id' LANGUAGE C IMMUTABLE PARALLEL SAFE;
CREATE OR REPLACE FUNCTION synchdb_bootstrap_column_id(text,text) RETURNS text
AS '$libdir/synchdb','synchdb_bootstrap_column_id' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION synchdb_bootstrap_validate_transform(text) RETURNS void
AS '$libdir/synchdb','synchdb_bootstrap_validate_transform' LANGUAGE C VOLATILE PARALLEL UNSAFE;
CREATE OR REPLACE FUNCTION synchdb_source_consistency_contract(text,text,text) RETURNS jsonb
AS '$libdir/synchdb','synchdb_source_consistency_contract' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- TODO: FDW continuation is permitted with legacy candidate coordinates;
-- neither this result nor its offset/history proves a synchronized read view.
CREATE OR REPLACE FUNCTION synchdb_bootstrap_relation_descriptor(p_relation oid)
RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog
SET quote_all_identifiers = on AS $sql$
    SELECT jsonb_build_object(
        'kind', c.relkind, 'persistence', c.relpersistence,
        'access_method', c.relam, 'tablespace', c.reltablespace,
        'options', to_jsonb(c.reloptions), 'replica_identity', c.relreplident,
        'columns', coalesce((SELECT jsonb_agg(jsonb_build_object(
            'number', a.attnum, 'name', a.attname, 'type', a.atttypid,
            'typmod', a.atttypmod, 'collation', a.attcollation,
            'not_null', a.attnotnull, 'identity', a.attidentity,
            'generated', a.attgenerated, 'dropped', a.attisdropped,
            'default', pg_get_expr(d.adbin, d.adrelid)) ORDER BY a.attnum)
            FROM pg_attribute a LEFT JOIN pg_attrdef d
              ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE a.attrelid = c.oid AND a.attnum > 0), '[]'::jsonb),
        'constraints', coalesce((SELECT jsonb_agg(jsonb_build_object(
            'name', k.conname, 'definition', pg_get_constraintdef(k.oid, false),
            'validated', k.convalidated) ORDER BY k.conname)
            FROM pg_constraint k WHERE k.conrelid = c.oid), '[]'::jsonb),
        'indexes', coalesce((SELECT jsonb_agg(jsonb_build_object(
            'definition', pg_get_indexdef(i.indexrelid), 'valid', i.indisvalid,
            'ready', i.indisready, 'live', i.indislive) ORDER BY i.indexrelid)
            FROM pg_index i WHERE i.indrelid = c.oid), '[]'::jsonb))
      FROM pg_class c WHERE c.oid = p_relation
$sql$;

CREATE OR REPLACE FUNCTION synchdb_bootstrap_check_replaceable(p_relation oid)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $fn$
DECLARE
    r record;
    v_dependency text;
BEGIN
    SELECT * INTO STRICT r FROM pg_class WHERE oid = p_relation;
    IF r.relkind <> 'r' OR r.relam <> (SELECT oid FROM pg_am WHERE amname='heap' AND amtype='t')
       OR r.relispartition OR r.relrowsecurity OR r.relforcerowsecurity
       OR r.relacl IS NOT NULL OR r.relowner <> (current_user::regrole)::oid
       OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = r.oid OR inhparent = r.oid)
       OR EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = r.oid AND NOT tgisinternal)
       OR EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = r.oid)
       OR EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = r.oid AND attacl IS NOT NULL)
       OR EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = r.oid
                   AND contype NOT IN ('p', 'n')) THEN
        RAISE EXCEPTION 'Target % cannot be safely replaced', p_relation::regclass
            USING ERRCODE = '0A000',
                  DETAIL = 'Replacement supports an owned ordinary table with default ACLs and no custom triggers, policies, inheritance, or additional constraints.';
    END IF;
    SELECT pg_describe_object(d.classid, d.objid, d.objsubid)
      INTO v_dependency FROM pg_depend d
     WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = r.oid
       AND NOT (d.classid = 'pg_class'::regclass AND
                (d.objid = r.reltoastrelid OR EXISTS
                 (SELECT 1 FROM pg_index i WHERE i.indexrelid = d.objid AND i.indrelid = r.oid)))
       AND NOT (d.classid = 'pg_type'::regclass AND d.objid = r.reltype)
       AND NOT (d.classid = 'pg_constraint'::regclass AND EXISTS
                (SELECT 1 FROM pg_constraint k WHERE k.oid = d.objid AND k.conrelid = r.oid))
       AND NOT (d.classid = 'pg_attrdef'::regclass AND EXISTS
                (SELECT 1 FROM pg_attrdef a WHERE a.oid = d.objid AND a.adrelid = r.oid))
     LIMIT 1;
    IF v_dependency IS NULL THEN
        SELECT pg_describe_object(d.classid, d.objid, d.objsubid)
          INTO v_dependency FROM pg_depend d
         WHERE d.refclassid = 'pg_type'::regclass AND d.refobjid = r.reltype
           AND NOT (d.classid = 'pg_type'::regclass AND EXISTS
                    (SELECT 1 FROM pg_type t WHERE t.oid = d.objid AND t.typelem = r.reltype))
         LIMIT 1;
    END IF;
    IF v_dependency IS NOT NULL THEN
        RAISE EXCEPTION 'Target % has an unsupported dependency', p_relation::regclass
            USING ERRCODE = '2BP01', DETAIL = v_dependency;
    END IF;
END;
$fn$;

-- Names are merely diagnostics; OID, owner, namespace and kind are the cleanup
-- authority.  All listed objects are created and removed in this transaction.
CREATE OR REPLACE FUNCTION synchdb_bootstrap_resources(p_schemas name[], p_server oid)
RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog AS $sql$
    SELECT coalesce(jsonb_agg(value ORDER BY ordering, object_oid), '[]'::jsonb)
      FROM (
        SELECT 1 AS ordering, n.oid AS object_oid, jsonb_build_object(
            'kind', 'schema', 'oid', n.oid, 'owner', n.nspowner, 'name', n.nspname) AS value
          FROM pg_namespace n WHERE n.nspname = ANY(p_schemas)
        UNION ALL
        SELECT 2, c.oid, jsonb_build_object('kind', 'relation', 'oid', c.oid,
            'owner', c.relowner, 'namespace_oid', c.relnamespace,
            'schema', n.nspname, 'name', c.relname, 'relkind', c.relkind)
          FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = ANY(p_schemas)
        UNION ALL
        SELECT 3, s.oid, jsonb_build_object('kind', 'server', 'oid', s.oid,
            'owner', s.srvowner, 'name', s.srvname)
          FROM pg_foreign_server s WHERE s.oid = p_server
        UNION ALL
        -- The underlying catalog protects credentials from ordinary roles.
        -- Its public view exposes the same identity OIDs without requiring
        -- access to mapping options or granting catalog-table privileges.
        SELECT 4, u.umid, jsonb_build_object('kind', 'user_mapping', 'oid', u.umid,
            'server_oid', u.srvid, 'user_oid', u.umuser)
          FROM pg_user_mappings u WHERE u.srvid = p_server
      ) objects
$sql$;

CREATE OR REPLACE FUNCTION synchdb_bootstrap_cleanup(p_resources jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $fn$
DECLARE
    r jsonb;
    c record;
    v_server name;
BEGIN
    IF jsonb_typeof(p_resources) <> 'array' THEN
        RAISE EXCEPTION 'Invalid bootstrap resource manifest' USING ERRCODE = '22023';
    END IF;
    -- Check the whole manifest before deleting any object.  Indexes/row types
    -- owned by a table disappear with that exact table, never by CASCADE.
    FOR r IN SELECT value FROM jsonb_array_elements(p_resources) LOOP
        IF r->>'kind' = 'schema' THEN
            IF NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.oid = (r->>'oid')::oid
                            AND n.nspname = r->>'name' AND n.nspowner = (r->>'owner')::oid) THEN
                RAISE EXCEPTION 'Bootstrap schema resource changed' USING ERRCODE = '55000';
            END IF;
            IF EXISTS (SELECT 1 FROM pg_class x WHERE x.relnamespace = (r->>'oid')::oid
                        AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_resources) saved
                                         WHERE saved->>'kind' = 'relation' AND (saved->>'oid')::oid = x.oid)) THEN
                RAISE EXCEPTION 'Bootstrap workspace contains an unowned relation' USING ERRCODE = '55000';
            END IF;
        ELSIF r->>'kind' = 'relation' THEN
            IF NOT EXISTS (SELECT 1 FROM pg_class x WHERE x.oid = (r->>'oid')::oid
                            AND x.relnamespace = (r->>'namespace_oid')::oid
                            AND x.relname = r->>'name' AND x.relkind::text = r->>'relkind'
                            AND x.relowner = (r->>'owner')::oid) THEN
                RAISE EXCEPTION 'Bootstrap relation resource changed' USING ERRCODE = '55000';
            END IF;
        ELSIF r->>'kind' = 'server' THEN
            IF NOT EXISTS (SELECT 1 FROM pg_foreign_server s WHERE s.oid = (r->>'oid')::oid
                            AND s.srvname = r->>'name' AND s.srvowner = (r->>'owner')::oid) THEN
                RAISE EXCEPTION 'Bootstrap server resource changed' USING ERRCODE = '55000';
            END IF;
        ELSIF r->>'kind' = 'user_mapping' THEN
            IF NOT EXISTS (SELECT 1 FROM pg_user_mappings u WHERE u.umid = (r->>'oid')::oid
                            AND u.srvid = (r->>'server_oid')::oid AND u.umuser = (r->>'user_oid')::oid) THEN
                RAISE EXCEPTION 'Bootstrap user-mapping resource changed' USING ERRCODE = '55000';
            END IF;
        ELSE
            RAISE EXCEPTION 'Unknown bootstrap resource kind' USING ERRCODE = '22023';
        END IF;
    END LOOP;
    FOR r IN SELECT value FROM jsonb_array_elements(p_resources)
              WHERE value->>'kind' = 'relation'
              ORDER BY CASE value->>'relkind' WHEN 'v' THEN 0 WHEN 'm' THEN 1 ELSE 2 END,
                       (value->>'oid')::oid DESC LOOP
        IF r->>'relkind' IN ('i', 'I', 'S', 't') THEN CONTINUE; END IF;
        IF r->>'relkind' NOT IN ('v', 'm', 'r', 'f') THEN
            RAISE EXCEPTION 'Unsupported bootstrap resource relation kind' USING ERRCODE = '0A000';
        END IF;
        EXECUTE format('DROP %s %I.%I', CASE r->>'relkind'
            WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW'
            WHEN 'f' THEN 'FOREIGN TABLE' ELSE 'TABLE' END, r->>'schema', r->>'name');
    END LOOP;
    FOR r IN SELECT value FROM jsonb_array_elements(p_resources) WHERE value->>'kind' = 'schema' LOOP
        EXECUTE format('DROP SCHEMA %I', r->>'name');
    END LOOP;
    FOR r IN SELECT value FROM jsonb_array_elements(p_resources) WHERE value->>'kind' = 'user_mapping' LOOP
        SELECT srvname INTO STRICT v_server FROM pg_foreign_server WHERE oid = (r->>'server_oid')::oid;
        EXECUTE format('DROP USER MAPPING FOR %s SERVER %I',
            CASE WHEN (r->>'user_oid')::oid = 0 THEN 'PUBLIC'
                 ELSE quote_ident(pg_get_userbyid((r->>'user_oid')::oid)) END, v_server);
    END LOOP;
    FOR r IN SELECT value FROM jsonb_array_elements(p_resources) WHERE value->>'kind' = 'server' LOOP
        EXECUTE format('DROP SERVER %I', r->>'name');
    END LOOP;
END;
$fn$;

CREATE OR REPLACE FUNCTION synchdb_bootstrap(p_request jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE
    v_extension oid;
    v_ext name;
    v_name text;
    v_connector_id uuid;
    v_revision bigint;
    v_config jsonb;
    v_connector text;
    v_mode text;
    v_failure text;
    v_policy text;
    v_case text;
    v_copy boolean;
    v_handoff boolean;
    v_after text;
    v_history jsonb := '[]'::jsonb;
    v_offset jsonb;
    v_resume jsonb;
    v_binlog_file text;
    v_binlog_pos bigint;
    v_server_id text;
    v_lsn bigint;
    v_commit_lsn text;
    v_generation uuid;
    v_prefix text;
    v_source name;
    v_stage name;
    v_meta name;
    v_load name;
    v_dest text;
    v_db text;
    v_owner text;
    v_server name;
    v_server_oid oid;
    v_selection jsonb;
    v_requested jsonb := '[]'::jsonb;
    v_selected jsonb := '[]'::jsonb;
    v_inventory jsonb;
    v_mappings jsonb;
    v_plan jsonb;
    v_table jsonb;
    v_tables jsonb := '[]'::jsonb;
    v_result jsonb;
    v_results jsonb := '[]'::jsonb;
    v_resources jsonb;
    v_artifact_intents jsonb;
    v_candidate jsonb := NULL;
    v_boundary text := 'UNSYNCHRONIZED_FDW_READ';
    v_read_contract jsonb;
    v_scn numeric;
    v_source_id jsonb;
    v_source_key text;
    v_column jsonb;
    v_columns jsonb;
    v_pk jsonb;
    v_rule jsonb;
    v_parts text[];
    v_dst_parts text[];
    v_src_parts text[];
    v_old_oid oid;
    v_new_oid oid;
    v_old_descriptor jsonb;
    v_descriptor jsonb;
    v_old_ledger jsonb;
    v_target_schema text;
    v_target_name text;
    v_local_name text;
    v_source_name text;
    v_target_column text;
    v_type_sql text;
    v_type_oid oid;
    v_transform text;
    v_ddl text;
    v_dst_list text;
    v_src_list text;
    v_laterals text;
    v_pk_list text;
    v_rows bigint;
    v_total_rows bigint := 0;
    v_success integer := 0;
    v_failed integer := 0;
    v_stage_name text;
    v_state text;
    v_message text;
    v_detail text;
    v_metadata_attempt integer;
    v_count integer;
    r record;
BEGIN
    IF p_request IS NULL OR jsonb_typeof(p_request) <> 'object'
       OR p_request->'version' IS DISTINCT FROM '1'::jsonb THEN
        RAISE EXCEPTION 'Expected bootstrap request version 1' USING ERRCODE = '22023';
    END IF;
    IF p_request ?| ARRAY['cdc_eligible','cdc_resume_material','source_read_boundary','consistency_proof'] THEN
        RAISE EXCEPTION 'Bootstrap callers cannot supply source consistency or CDC handoff proof'
            USING ERRCODE = '22023';
    END IF;
    v_mode := lower(p_request->>'mode');
    IF v_mode IS NULL OR v_mode NOT IN ('initial_only', 'schemasync', 'initial', 'always', 'no_data', 'never') THEN
        RAISE EXCEPTION 'Unsupported FDW bootstrap mode: %', v_mode USING ERRCODE = '22023';
    END IF;
    v_copy := v_mode IN ('initial_only', 'initial', 'always');
    v_handoff := v_mode <> 'initial_only';
    v_after := CASE WHEN v_mode = 'initial_only' THEN 'STOP'
                    WHEN v_copy THEN 'CDC' ELSE 'PAUSE' END;
    v_failure := coalesce(p_request->>'failure_policy', 'fail_fast');
    v_policy := coalesce(p_request->>'target_policy', 'error');
    v_case := coalesce(p_request->>'case_strategy', 'asis');
    IF v_failure NOT IN ('fail_fast', 'continue_tables') OR v_case NOT IN ('lower', 'upper', 'asis') THEN
        RAISE EXCEPTION 'Invalid bootstrap failure policy or case strategy' USING ERRCODE = '22023';
    END IF;
    IF v_policy IN ('validate_existing', 'skip', 'drop') THEN
        RAISE EXCEPTION 'Bootstrap target policy % is unsupported', v_policy USING ERRCODE = '0A000';
    ELSIF v_policy NOT IN ('error', 'replace') THEN
        RAISE EXCEPTION 'Invalid bootstrap target policy %', v_policy USING ERRCODE = '22023';
    END IF;
    IF (p_request ? 'offset' AND p_request->'offset' <> 'null'::jsonb)
       OR coalesce((p_request->>'write_schema_history')::boolean, false) THEN
        RAISE EXCEPTION 'Bootstrap offset overrides and schema-history publication require a verified artifact handoff'
            USING ERRCODE = '0A000';
    END IF;
    v_name := p_request->>'connector_name';
    IF v_name IS NULL OR btrim(v_name) = '' OR octet_length(v_name)>current_setting('max_identifier_length')::int THEN
        RAISE EXCEPTION 'Bootstrap connector name must be nonempty' USING ERRCODE = '22023';
    END IF;
    SELECT e.oid, n.nspname INTO STRICT v_extension, v_ext
      FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'synchdb';
    PERFORM set_config('search_path', format('pg_catalog,%I,pg_temp', v_ext), true);
    -- The connector row is frozen for this local transaction; expressions and
    -- mapping rows are copied below and never rediscovered during execution.
    SELECT connector_id, config_revision, data INTO STRICT v_connector_id, v_revision, v_config
      FROM synchdb_conninfo WHERE name COLLATE "C" = v_name COLLATE "C" FOR SHARE;
    IF p_request ? 'connector_id' AND (p_request->>'connector_id')::uuid IS DISTINCT FROM v_connector_id THEN
        RAISE EXCEPTION 'Bootstrap connector incarnation changed' USING ERRCODE = '55000';
    END IF;
    IF p_request ? 'config_revision' AND (p_request->>'config_revision')::bigint IS DISTINCT FROM v_revision THEN
        RAISE EXCEPTION 'Bootstrap connector configuration changed' USING ERRCODE = '55000';
    END IF;
    v_connector := lower(v_config->>'connector');
    IF v_connector IS NULL OR v_connector NOT IN ('oracle', 'olr', 'postgres', 'mysql', 'sqlserver') THEN
        RAISE EXCEPTION 'Unsupported FDW connector %', v_connector USING ERRCODE = '0A000';
    END IF;
    v_read_contract := synchdb_source_consistency_contract(v_connector,'fdw',v_mode);
    v_db := v_config->>'srcdb';
    IF v_connector IN ('oracle', 'olr') AND strpos(v_db, '/') > 0 THEN v_db := split_part(v_db, '/', 2); END IF;
    v_owner := nullif(nullif(v_config->>'srcschema', ''), 'null');
    IF v_owner IS NULL AND v_connector = 'mysql' THEN v_owner := v_db; END IF;
    IF v_owner IS NULL OR v_db IS NULL OR v_db = '' THEN
        RAISE EXCEPTION 'FDW bootstrap requires one explicit source database and schema'
            USING ERRCODE = '22023';
    END IF;
    v_dest := coalesce(p_request->>'dest_schema', v_db);
    IF v_dest = '' OR octet_length(v_dest) > current_setting('max_identifier_length')::int THEN
        RAISE EXCEPTION 'Invalid bootstrap destination schema' USING ERRCODE = '22023';
    END IF;
    v_generation := coalesce((p_request->>'generation')::uuid, gen_random_uuid());
    IF EXISTS (SELECT 1 FROM synchdb_bootstrap_runs WHERE generation=v_generation) THEN
        RAISE EXCEPTION 'Bootstrap generation % already has a committed result',v_generation USING ERRCODE='23505';
    END IF;
    v_prefix := 'sdb_b_' || replace(v_generation::text, '-', '');
    v_source := (v_prefix || '_src')::name;
    v_stage := (v_prefix || '_ft')::name;
    v_meta := (v_prefix || '_meta')::name;
    v_load := (v_prefix || '_load')::name;
    v_server := (v_prefix || '_srv')::name;
    IF v_dest = ANY(ARRAY[v_source::text, v_stage::text, v_meta::text, v_load::text]) THEN
        RAISE EXCEPTION 'Bootstrap destination aliases a private workspace' USING ERRCODE = '22023';
    END IF;
    -- Read the mapping catalog once.  No helper executes live mapping rows.
    SELECT coalesce(jsonb_agg(jsonb_build_object('kind', objtype, 'source', srcobj, 'destination', dstobj)
                              ORDER BY objtype, srcobj), '[]'::jsonb)
      INTO v_mappings FROM synchdb_objmap WHERE name::text COLLATE "C" = v_name COLLATE "C" AND enabled;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_mappings) m
               WHERE m->>'kind' NOT IN ('table','column','datatype','transform')
                  OR m->>'source' IS NULL OR btrim(m->>'source')=''
                  OR m->>'destination' IS NULL OR btrim(m->>'destination')='') THEN
        RAISE EXCEPTION 'Invalid enabled bootstrap mapping rule' USING ERRCODE='22023';
    END IF;
    EXECUTE format('CREATE SCHEMA %I', v_source);
    EXECUTE format('CREATE SCHEMA %I', v_stage);
    EXECUTE format('CREATE SCHEMA %I', v_meta);
    EXECUTE format('CREATE SCHEMA %I', v_load);
    v_server := synchdb_prepare_initial_snapshot(v_name, v_server);
    SELECT oid INTO STRICT v_server_oid FROM pg_foreign_server WHERE srvname = v_server;
    CASE v_connector
      WHEN 'oracle', 'olr' THEN PERFORM synchdb_create_oraviews(v_server, v_source, '{"case":"original"}'::jsonb);
      WHEN 'postgres' THEN PERFORM synchdb_create_pg_objs(v_server, v_source, '{"case":"original"}'::jsonb);
      WHEN 'mysql' THEN PERFORM synchdb_create_mysql_objs(v_server, v_source, '{"case":"original"}'::jsonb);
      WHEN 'sqlserver' THEN PERFORM synchdb_create_sqlserver_objs(v_server, v_source, '{"case":"original"}'::jsonb);
    END CASE;
    IF v_connector = 'sqlserver' THEN
        FOR v_metadata_attempt IN 1..3 LOOP
            BEGIN
                -- Roll back the entire metadata set on failure, including any
                -- tables already copied before a later catalog read deadlocks.
                PERFORM synchdb_materialize_metadata(v_source, v_meta, 'error');
                EXIT;
            EXCEPTION WHEN SQLSTATE 'HV00L' THEN
                -- FreeTDS reports server errors as DB-Library 20018. Only
                -- severity 13 denotes a remote transaction deadlock; do not
                -- retry other FDW, permission, connection or query errors.
                IF SQLERRM NOT LIKE 'DB-Library error: DB #: 20018,%, Level: 13'
                   OR v_metadata_attempt = 3 THEN
                    RAISE;
                END IF;
                RAISE NOTICE 'SQL Server metadata read deadlocked (attempt %/3); retrying',
                    v_metadata_attempt USING DETAIL = SQLERRM;
            END;
            PERFORM pg_sleep(0.2 * v_metadata_attempt);
        END LOOP;
    ELSE
        PERFORM synchdb_materialize_metadata(v_source, v_meta, 'error');
    END IF;
    EXECUTE format('SELECT coalesce(jsonb_agg(jsonb_build_object(''db'',$1,''schema'',"schema",''table'',table_name)
                       ORDER BY "schema",table_name),''[]''::jsonb)
                    FROM (SELECT DISTINCT "schema",table_name FROM %I.columns WHERE "schema"=$2) inventory', v_meta)
      INTO v_inventory USING v_db, v_owner;
    v_selection := p_request->'selection';
    IF v_selection IS NULL OR v_selection = 'null'::jsonb THEN
        v_selection := coalesce(nullif(nullif(v_config->'snapshottable', 'null'::jsonb), '"null"'::jsonb),
                                nullif(nullif(v_config->'table', 'null'::jsonb), '"null"'::jsonb));
    END IF;
    IF v_selection IS NULL OR v_selection = 'null'::jsonb
       OR (jsonb_typeof(v_selection) = 'object' AND v_selection->>'kind' = 'all') THEN
        v_selected := v_inventory;
    ELSE
        IF jsonb_typeof(v_selection) = 'object' AND v_selection->>'kind' = 'tables' THEN
            v_requested := v_selection->'tables';
        ELSE
            IF jsonb_typeof(v_selection) = 'string' THEN
                v_message := v_selection #>> '{}';
                IF left(v_message, 5) = 'file:' THEN
                    v_selection := pg_read_file(substr(v_message, 6))::jsonb->'snapshot_table_list';
                ELSE
                    v_selection := to_jsonb(regexp_split_to_array(v_message, '\s*,\s*'));
                END IF;
            END IF;
            IF jsonb_typeof(v_selection) <> 'array' THEN
                RAISE EXCEPTION 'Unsupported FDW table selection' USING ERRCODE = '22023';
            END IF;
            FOR v_rule IN SELECT value FROM jsonb_array_elements(v_selection) LOOP
                IF jsonb_typeof(v_rule) <> 'string' THEN
                    RAISE EXCEPTION 'FDW table selection entries must be strings or structured identities' USING ERRCODE = '22023';
                END IF;
                v_message := v_rule #>> '{}';
                IF v_message ~ '[*+?()\[\]|^$\\]' THEN
                    RAISE EXCEPTION 'FDW table selection does not accept connector regular expressions' USING ERRCODE = '0A000';
                END IF;
                v_parts := parse_ident(v_message, true);
                IF cardinality(v_parts) = 3 THEN
                    v_source_id := jsonb_build_object('db',v_parts[1],'schema',v_parts[2],'table',v_parts[3]);
                ELSIF cardinality(v_parts) = 2 AND v_connector = 'mysql' THEN
                    v_source_id := jsonb_build_object('db',v_parts[1],'schema',v_owner,'table',v_parts[2]);
                ELSIF cardinality(v_parts) = 2 THEN
                    v_source_id := jsonb_build_object('db',v_db,'schema',v_parts[1],'table',v_parts[2]);
                ELSE
                    RAISE EXCEPTION 'Invalid FDW table identity: %', v_message USING ERRCODE = '22023';
                END IF;
                SELECT count(*), (jsonb_agg(value)->0) INTO v_count, v_source_id
                  FROM jsonb_array_elements(v_inventory)
                 WHERE lower(value->>'db') = lower(v_source_id->>'db')
                   AND lower(value->>'schema') = lower(v_source_id->>'schema')
                   AND lower(value->>'table') = lower(v_source_id->>'table');
                IF v_count <> 1 THEN
                    RAISE EXCEPTION 'FDW table selection has no unique match: %', v_message USING ERRCODE = '22023';
                END IF;
                v_requested := v_requested || jsonb_build_array(v_source_id);
            END LOOP;
        END IF;
        IF v_requested IS NULL OR jsonb_typeof(v_requested) <> 'array' OR jsonb_array_length(v_requested) = 0 THEN
            RAISE EXCEPTION 'FDW table selection is explicitly empty' USING ERRCODE = '22023';
        END IF;
        FOR v_source_id IN SELECT value FROM jsonb_array_elements(v_requested) LOOP
            IF jsonb_typeof(v_source_id) <> 'object' OR v_source_id->>'db' IS DISTINCT FROM v_db
               OR v_source_id->>'schema' IS DISTINCT FROM v_owner
               OR NOT v_inventory @> jsonb_build_array(v_source_id) THEN
                RAISE EXCEPTION 'FDW table selection is outside the exact configured source scope: %', v_source_id USING ERRCODE = '22023';
            END IF;
            IF NOT v_selected @> jsonb_build_array(v_source_id) THEN
                v_selected := v_selected || jsonb_build_array(v_source_id);
            END IF;
        END LOOP;
    END IF;
    -- TODO: coordinate this candidate with the FDW remote read view and
    -- in-flight transactions. This restores the legacy runnable handoff only;
    -- source_read_boundary.cdc_verified deliberately remains false.
    IF v_connector IN ('oracle', 'olr') AND (v_copy OR v_handoff) THEN
        PERFORM synchdb_create_current_scn_ft(v_meta, v_server);
        EXECUTE format('SELECT current_scn FROM %I.current_scn', v_meta) INTO STRICT v_scn;
        IF v_scn IS NULL OR v_scn <= 0 OR v_scn <> trunc(v_scn) THEN
            RAISE EXCEPTION 'Invalid Oracle data-read SCN' USING ERRCODE = '22000';
        END IF;
        v_candidate := jsonb_build_object('scn', v_scn);
        v_offset := jsonb_build_object('commit_scn', (v_scn + 1)::text,
            'snapshot_scn', v_scn::text, 'scn', v_scn::text);
        v_boundary := 'ORACLE_AS_OF_SCN_DATA_ONLY';
    ELSIF v_connector = 'mysql' AND v_handoff THEN
        PERFORM synchdb_create_current_binlog_pos_ft(v_meta, v_server);
        EXECUTE format('SELECT local::jsonb->>''binary_log_file'',
            (local::jsonb->>''binary_log_position'')::bigint FROM %I.log_status LIMIT 1', v_meta)
            INTO STRICT v_binlog_file, v_binlog_pos;
        EXECUTE format('SELECT variable_value FROM %I.global_variables
            WHERE lower(variable_name) = ''server_id'' LIMIT 1', v_meta) INTO STRICT v_server_id;
        IF v_binlog_file IS NULL OR v_binlog_pos IS NULL OR v_binlog_pos < 0 OR v_server_id IS NULL THEN
            RAISE EXCEPTION 'Invalid MySQL CDC candidate' USING ERRCODE = '22000';
        END IF;
        v_candidate := jsonb_build_object('file', v_binlog_file, 'pos', v_binlog_pos,
            'server_id', v_server_id, 'ts_sec', floor(extract(epoch FROM clock_timestamp())));
        v_offset := v_candidate || jsonb_build_object('snapshot_completed', true);
    ELSIF v_connector = 'postgres' AND v_handoff THEN
        PERFORM synchdb_create_current_lsn_ft(v_meta, v_server);
        EXECUTE format('SELECT (wal_lsn - ''0/0''::pg_lsn)::bigint FROM %I.wal_lsn', v_meta) INTO STRICT v_lsn;
        IF v_lsn IS NULL OR v_lsn <= 0 THEN
            RAISE EXCEPTION 'Invalid PostgreSQL CDC candidate' USING ERRCODE = '22000';
        END IF;
        v_candidate := jsonb_build_object('lsn', v_lsn);
        v_offset := v_candidate;
    ELSIF v_connector = 'sqlserver' AND v_handoff THEN
        PERFORM synchdb_create_current_sqlserver_lsn_ft(v_meta, v_server);
        EXECUTE format('SELECT max_lsn FROM %I.current_lsn', v_meta) INTO STRICT v_commit_lsn;
        IF v_commit_lsn IS NULL OR v_commit_lsn !~ '^[0-9a-f]{8}:[0-9a-f]{8}:[0-9a-f]{4}$'
           OR v_commit_lsn = '00000000:00000000:0000' THEN
            RAISE EXCEPTION 'Invalid SQL Server CDC candidate' USING ERRCODE = '22000';
        END IF;
        v_candidate := jsonb_build_object('commit_lsn', v_commit_lsn);
        -- Recovery reconstructs history without re-copying FDW target rows.
        v_offset := v_candidate || jsonb_build_object('change_lsn', 'NULL',
            'snapshot', true, 'snapshot_completed', false);
    ELSIF NOT v_copy THEN
        v_boundary := 'METADATA_ONLY';
    END IF;
    PERFORM synchdb_create_stage_fts_plan(v_name, v_db, v_owner,
        v_stage, v_server, v_case = 'lower', 'error', v_candidate::text,
        v_meta, NULL::text, v_handoff AND v_connector IN ('mysql','oracle'), v_case, v_selected, false);
    IF v_handoff AND v_connector IN ('mysql','oracle') THEN
        EXECUTE format('SELECT coalesce(jsonb_agg(line::jsonb), ''[]''::jsonb) FROM %I.schema_history', v_stage)
            INTO v_history;
        IF jsonb_array_length(v_history) <> jsonb_array_length(v_selected) THEN
            RAISE EXCEPTION 'FDW handoff history does not cover every selected table' USING ERRCODE = '55000';
        END IF;
    END IF;
    -- Freeze all final target identities, columns and PKs before local target
    -- DDL or copying.  A malformed/colliding plan is a global preflight error.
    FOR v_source_id IN SELECT value FROM jsonb_array_elements(v_selected) LOOP
        v_source_key:=synchdb_bootstrap_source_id(v_source_id->>'db',v_source_id->>'schema',v_source_id->>'table',v_connector);
        v_local_name := synchdb_bootstrap_normalize_name(v_source_id->>'table',v_case);
        IF octet_length(v_local_name) > current_setting('max_identifier_length')::int THEN
            RAISE EXCEPTION 'Normalized FDW table name is too long' USING ERRCODE = '22023';
        END IF;
        v_target_schema := v_dest;
        v_target_name := v_local_name;
        v_columns := '[]'::jsonb;
        v_pk := '[]'::jsonb;
        v_src_parts := CASE WHEN v_connector = 'mysql'
            THEN ARRAY[v_db,v_source_id->>'table'] ELSE ARRAY[v_db,v_owner,v_source_id->>'table'] END;
        FOR v_rule IN SELECT value FROM jsonb_array_elements(v_mappings) WHERE value->>'kind' = 'table' LOOP
            IF v_rule->>'source' = v_source_key THEN
                v_dst_parts := parse_ident(v_rule->>'destination', true);
                IF cardinality(v_dst_parts) = 1 THEN v_target_schema := 'public'; v_target_name := v_dst_parts[1];
                ELSIF cardinality(v_dst_parts) = 2 THEN v_target_schema := v_dst_parts[1]; v_target_name := v_dst_parts[2];
                ELSE RAISE EXCEPTION 'Invalid FDW table mapping destination' USING ERRCODE = '22023'; END IF;
            END IF;
        END LOOP;
        IF v_target_schema = ANY(ARRAY[v_source::text,v_stage::text,v_meta::text,v_load::text])
           OR v_target_schema = 'pg_catalog' OR left(v_target_schema, 3) = 'pg_'
           OR octet_length(v_target_name) > current_setting('max_identifier_length')::int
           OR octet_length(v_target_schema) > current_setting('max_identifier_length')::int THEN
            RAISE EXCEPTION 'Invalid or reserved FDW final target identity' USING ERRCODE = '22023';
        END IF;
        IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_tables) t
                    WHERE t->>'target_schema' = v_target_schema AND t->>'target_name' = v_target_name) THEN
            RAISE EXCEPTION 'FDW table mappings collide at %.%', v_target_schema, v_target_name USING ERRCODE = '22023';
        END IF;
        SELECT c.oid INTO STRICT v_new_oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = v_stage AND c.relname = v_local_name AND c.relkind = 'f';
        FOR r IN EXECUTE format('SELECT c.*, a.attname::text AS local_name, a.atttypid, a.atttypmod, a.attnotnull
                 FROM (SELECT c.*, row_number() OVER (ORDER BY c.position) AS local_position
                         FROM %I.columns c WHERE c."schema"=$2 AND c.table_name=$3) c
                 JOIN pg_attribute a ON a.attrelid = $1 AND a.attnum = c.local_position
                WHERE c."schema" = $2 AND c.table_name = $3 AND NOT a.attisdropped ORDER BY c.position', v_meta)
                 USING v_new_oid, v_owner, v_source_id->>'table' LOOP
            v_source_name := r.local_name;
            v_target_column := v_source_name;
            v_type_sql := synchdb_bootstrap_type(v_connector,v_source_key,
                jsonb_build_object('name',r.column_name,'type_name',r.type_name,
                    'physical_type',format_type(r.atttypid,r.atttypmod),
                    'length',CASE WHEN coalesce(nullif(r.length,0),r.precision,-1)>=2147483647 THEN -1
                                  ELSE coalesce(nullif(r.length,0),r.precision,-1) END,
                    'scale',r.scale,'auto_incremented',false),v_mappings);
            v_type_oid := to_regtype(v_type_sql);
            v_transform := NULL;
            FOR v_rule IN SELECT value FROM jsonb_array_elements(v_mappings)
                           WHERE value->>'kind' IN ('column','transform') LOOP
                IF v_rule->>'source' <> synchdb_bootstrap_column_id(v_source_key,r.column_name::text) THEN CONTINUE; END IF;
                CASE v_rule->>'kind'
                  WHEN 'column' THEN v_target_column := v_rule->>'destination';
                  WHEN 'transform' THEN v_transform := v_rule->>'destination';
                END CASE;
            END LOOP;
            IF v_target_column IS NULL OR v_target_column = ''
               OR octet_length(v_target_column) > current_setting('max_identifier_length')::int
               OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_columns) x WHERE x->>'target' = v_target_column) THEN
                RAISE EXCEPTION 'Invalid or colliding FDW target column' USING ERRCODE = '22023';
            END IF;
            v_column := jsonb_build_object('position', r.position, 'source', r.column_name,
                'source_key',synchdb_bootstrap_column_id(v_source_key,r.column_name::text),
                'local_source', v_source_name, 'source_type', r.type_name, 'target', v_target_column,
                'type_sql', v_type_sql, 'type_oid', v_type_oid, 'not_null', r.attnotnull,
                'transform', v_transform);
            PERFORM synchdb_bootstrap_validate_transform(v_transform);
            v_columns := v_columns || jsonb_build_array(v_column);
        END LOOP;
        FOR v_rule IN SELECT value FROM jsonb_array_elements(v_mappings)
                       WHERE value->>'kind' IN ('column','datatype','transform') LOOP
            IF left((CASE WHEN v_rule->>'kind'='datatype' THEN split_part(v_rule->>'source','|',1) ELSE v_rule->>'source' END),length(v_source_key)+1)
                  = v_source_key||'.'
               AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(v_columns) c
                   WHERE (CASE WHEN v_rule->>'kind'='datatype' THEN split_part(v_rule->>'source','|',1) ELSE v_rule->>'source' END)=synchdb_bootstrap_column_id(v_source_key,c->>'source')) THEN
                RAISE EXCEPTION 'FDW mapping refers to an unknown selected source column: %',v_rule->>'source' USING ERRCODE='42703';
            END IF;
        END LOOP;
        IF jsonb_array_length(v_columns) = 0 THEN
            RAISE EXCEPTION 'FDW selected table has no validated columns' USING ERRCODE = '22000';
        END IF;
        FOR r IN EXECUTE format('SELECT column_name FROM %I.keys WHERE "schema"=$1 AND table_name=$2 AND is_primary ORDER BY position', v_meta)
                 USING v_owner, v_source_id->>'table' LOOP
            SELECT x->>'target' INTO STRICT v_target_column FROM jsonb_array_elements(v_columns) x WHERE x->>'source' = r.column_name;
            v_pk := v_pk || jsonb_build_array(v_target_column);
        END LOOP;
        v_old_oid := to_regclass(format('%I.%I', v_target_schema, v_target_name));
        v_old_descriptor := NULL;
        IF v_old_oid IS NOT NULL THEN
            IF v_policy = 'error' THEN
                RAISE EXCEPTION 'Bootstrap final target %.% already exists', v_target_schema, v_target_name USING ERRCODE = '42P07';
            END IF;
            SELECT to_jsonb(t) INTO v_old_ledger FROM synchdb_bootstrap_targets t
             WHERE target_oid = v_old_oid AND connector_id = v_connector_id AND source_identity = v_source_id;
            v_old_descriptor := synchdb_bootstrap_relation_descriptor(v_old_oid);
            IF v_old_ledger IS NULL OR (v_old_ledger->>'target_owner')::oid <> (current_user::regrole)::oid
               OR v_old_ledger->>'target_schema' IS DISTINCT FROM v_target_schema
               OR v_old_ledger->>'target_name' IS DISTINCT FROM v_target_name
               OR v_old_ledger->'descriptor' IS DISTINCT FROM v_old_descriptor THEN
                RAISE EXCEPTION 'Bootstrap final target %.% is unowned or has changed', v_target_schema, v_target_name USING ERRCODE = '55000';
            END IF;
            PERFORM synchdb_bootstrap_check_replaceable(v_old_oid);
        END IF;
        v_tables := v_tables || jsonb_build_array(jsonb_build_object('source',v_source_id,'source_key',v_source_key,
            'stage_oid', v_new_oid, 'local_name',v_local_name,'target_schema',v_target_schema,
            'target_name',v_target_name,'columns',v_columns,'primary_key',v_pk,
            'expected_old_oid',v_old_oid,'expected_old_descriptor',v_old_descriptor));
    END LOOP;
    v_artifact_intents := jsonb_build_array(jsonb_build_object('kind','HISTORY','provider',v_connector,
        'required_for_future_cdc',v_connector IN ('oracle','mysql','sqlserver'),
        'publish',false,'relation_oid',NULL,'disposition','UNPUBLISHED'));
    v_plan := jsonb_build_object('version',1,'generation',v_generation,'connector_id',v_connector_id,
        'connector_name',v_name,'config_revision',v_revision,'provider',v_connector,
        'config_fingerprint',md5(v_config::text),'selection_fingerprint',md5(v_selected::text),
        'mapping_fingerprint',md5(v_mappings::text),'schema_fingerprint',md5(v_tables::text),
        'copy_data',v_copy,'after_bootstrap',v_after,
        'failure_policy',v_failure,'target_policy',v_policy,'case_strategy',v_case,
        'selection',v_selected,'mapping_snapshot',v_mappings,'tables',v_tables,'artifact_intents',v_artifact_intents,
        'source_read_policy',v_boundary,'candidate_boundary',v_candidate,
        'source_consistency',v_read_contract,
        'metadata_consistent_with_data',false,'cdc_protocol_verified',false);
    -- Capture preparation resources before executing user expressions.  A
    -- callback cannot smuggle a preexisting relation into cleanup by moving it
    -- into a workspace; the manifest rejects any later unplanned object.
    v_resources := synchdb_bootstrap_resources(ARRAY[v_source,v_stage,v_meta,v_load],v_server_oid);
    -- A table unit owns every local action, including binding and publication.
    FOR v_table IN SELECT value FROM jsonb_array_elements(v_tables) LOOP
        v_source_id := v_table->'source';
        v_source_key := v_table->>'source_key';
        v_target_schema := v_table->>'target_schema';
        v_target_name := v_table->>'target_name';
        v_local_name := v_table->>'local_name';
        v_rows := 0;
        BEGIN
            v_stage_name := 'CREATE';
            SELECT string_agg(format('%I %s%s', x->>'target', x->>'type_sql',
                CASE WHEN (x->>'not_null')::boolean THEN ' NOT NULL' ELSE '' END),', ' ORDER BY ord)
              INTO v_ddl FROM jsonb_array_elements(v_table->'columns') WITH ORDINALITY f(x,ord);
            EXECUTE format('CREATE TABLE %I.%I (%s) USING heap',v_load,v_local_name,v_ddl);
            v_new_oid := to_regclass(format('%I.%I',v_load,v_local_name));
            v_stage_name := 'PRIMARY_KEY';
            SELECT string_agg(quote_ident(value #>> '{}'),', ' ORDER BY ord) INTO v_pk_list
              FROM jsonb_array_elements(v_table->'primary_key') WITH ORDINALITY f(value,ord);
            IF v_pk_list IS NOT NULL THEN
                EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I PRIMARY KEY (%s)',
                    v_load,v_local_name,v_prefix || '_pk_' || (v_success+v_failed)::text,v_pk_list);
            END IF;
            v_stage_name := 'COPY';
            IF v_copy THEN
                SELECT string_agg(quote_ident(x->>'target'),', ' ORDER BY ord),
                       string_agg(format('_sdb_value_%s.v',ord),', ' ORDER BY ord),
                       string_agg(format(' CROSS JOIN LATERAL %I.synchdb_bootstrap_value(s.%I::text,%L) AS _sdb_value_%s(v %s)',
                           v_ext,x->>'local_source',x->>'transform',ord,x->>'type_sql'),' ' ORDER BY ord)
                  INTO v_dst_list,v_src_list,v_laterals
                  FROM jsonb_array_elements(v_table->'columns') WITH ORDINALITY f(x,ord);
                EXECUTE format('INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I s %s',
                    v_load,v_local_name,v_dst_list,v_src_list,v_stage,v_local_name,v_laterals);
                GET DIAGNOSTICS v_rows = ROW_COUNT;
            END IF;
            v_stage_name := 'PUBLISH';
            v_old_oid := (v_table->>'expected_old_oid')::oid;
            EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I',v_target_schema);
            IF v_old_oid IS NOT NULL THEN
                EXECUTE format('LOCK TABLE ONLY %I.%I IN ACCESS EXCLUSIVE MODE',v_target_schema,v_target_name);
                IF to_regclass(format('%I.%I',v_target_schema,v_target_name)) IS DISTINCT FROM v_old_oid
                   OR synchdb_bootstrap_relation_descriptor(v_old_oid) IS DISTINCT FROM v_table->'expected_old_descriptor'
                   OR NOT EXISTS (SELECT 1 FROM synchdb_bootstrap_targets t WHERE t.target_oid = v_old_oid
                       AND t.connector_id = v_connector_id AND t.source_identity = v_source_id
                       AND t.target_owner = (current_user::regrole)::oid
                       AND t.target_schema = v_target_schema AND t.target_name = v_target_name
                       AND t.descriptor = v_table->'expected_old_descriptor') THEN
                    RAISE EXCEPTION 'Bootstrap replacement target changed before publication' USING ERRCODE = '55000';
                END IF;
                PERFORM synchdb_bootstrap_check_replaceable(v_old_oid);
                -- The old object remains locked. DROP RESTRICT can never remove
                -- a dependent object; any failure rolls this complete unit back.
                EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',v_target_schema,v_target_name,
                    v_prefix || '_old_' || v_old_oid::text);
            ELSIF to_regclass(format('%I.%I',v_target_schema,v_target_name)) IS NOT NULL THEN
                RAISE EXCEPTION 'Bootstrap target appeared before publication' USING ERRCODE = '42P07';
            END IF;
            IF v_local_name <> v_target_name THEN
                EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',v_load,v_local_name,v_target_name);
            END IF;
            EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I',v_load,v_target_name,v_target_schema);
            v_stage_name := 'BIND';
            DELETE FROM synchdb_attribute WHERE name::text COLLATE "C" = v_name COLLATE "C" AND type::text = v_connector
                AND ext_tbname = v_source_key COLLATE "C";
            INSERT INTO synchdb_attribute(name,type,attrelid,attnum,ext_tbname,ext_attname,ext_atttypename)
                SELECT v_name::name,v_connector::name,v_new_oid,ord::smallint,
                    v_source_key,
                    x->>'source',lower(x->>'source_type')
                  FROM jsonb_array_elements(v_table->'columns') WITH ORDINALITY f(x,ord);
            GET DIAGNOSTICS v_count = ROW_COUNT;
            IF v_count <> jsonb_array_length(v_table->'columns') THEN
                RAISE EXCEPTION 'Bootstrap attribute publication did not bind every selected column' USING ERRCODE='55000';
            END IF;
            IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_table->'columns') WITH ORDINALITY f(x,ord)
                WHERE NOT EXISTS (SELECT 1 FROM synchdb_attribute a
                    WHERE a.name::text COLLATE "C"=v_name COLLATE "C" AND a.type::text=v_connector
                      AND a.attrelid=v_new_oid AND a.attnum=ord
                      AND a.ext_tbname=v_source_key COLLATE "C"
                      AND a.ext_attname=x->>'source' AND a.ext_atttypename=lower(x->>'source_type'))) THEN
                RAISE EXCEPTION 'Bootstrap attribute publication changed the required binding' USING ERRCODE='55000';
            END IF;
            v_stage_name := 'OWNERSHIP';
            IF v_old_oid IS NOT NULL THEN
                EXECUTE format('DROP TABLE %I.%I',v_target_schema,v_prefix || '_old_' || v_old_oid::text);
                DELETE FROM synchdb_bootstrap_targets WHERE target_oid = v_old_oid;
                GET DIAGNOSTICS v_count = ROW_COUNT;
                IF v_count <> 1 THEN
                    RAISE EXCEPTION 'Bootstrap ownership changed during replacement' USING ERRCODE='55000';
                END IF;
            END IF;
            v_descriptor := synchdb_bootstrap_relation_descriptor(v_new_oid);
            INSERT INTO synchdb_bootstrap_targets(target_oid,connector_id,generation,source_identity,
                target_schema,target_name,target_owner,descriptor,descriptor_hash,plan)
                VALUES(v_new_oid,v_connector_id,v_generation,v_source_id,v_target_schema,v_target_name,
                       (current_user::regrole)::oid,v_descriptor,md5(v_descriptor::text),v_plan);
            GET DIAGNOSTICS v_count = ROW_COUNT;
            IF v_count <> 1 THEN
                RAISE EXCEPTION 'Bootstrap target ownership was not published' USING ERRCODE='55000';
            END IF;
            IF NOT EXISTS (SELECT 1 FROM synchdb_bootstrap_targets t
                WHERE t.target_oid=v_new_oid AND t.connector_id=v_connector_id
                  AND t.generation=v_generation AND t.source_identity=v_source_id
                  AND t.target_schema=v_target_schema AND t.target_name=v_target_name
                  AND t.target_owner=(current_user::regrole)::oid AND t.descriptor=v_descriptor
                  AND t.descriptor_hash=md5(v_descriptor::text) AND t.plan=v_plan) THEN
                RAISE EXCEPTION 'Bootstrap ownership publication changed the required ledger entry' USING ERRCODE='55000';
            END IF;
            v_results := v_results || jsonb_build_array(jsonb_build_object('source',v_source_id,
                'target_schema',v_target_schema,'target_name',v_target_name,'target_oid',v_new_oid,
                'status','COMPLETE','stage','PUBLISHED','rows',v_rows,'sqlstate',NULL));
            v_success := v_success + 1;
            v_total_rows := v_total_rows + v_rows;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE,v_message = MESSAGE_TEXT,v_detail = PG_EXCEPTION_DETAIL;
            IF v_failure = 'fail_fast' THEN RAISE; END IF;
            v_failed := v_failed + 1;
            v_results := v_results || jsonb_build_array(jsonb_build_object('source',v_source_id,
                'target_schema',v_target_schema,'target_name',v_target_name,'target_oid',NULL,
                'status','FAILED','stage',v_stage_name,'rows',0,'sqlstate',v_state,'message',v_message,'detail',v_detail));
        END;
    END LOOP;
    PERFORM synchdb_bootstrap_cleanup(v_resources);
    IF v_handoff AND v_failed = 0 THEN
        v_resume := jsonb_build_object('offset',v_offset,'history',v_history);
    END IF;
    v_result := jsonb_build_object('version',1,'status',CASE WHEN v_failed = 0 THEN 'COMPLETE'
             WHEN v_success > 0 THEN 'PARTIAL' ELSE 'FAILED' END,
        'generation',v_generation,'copy_data',v_copy,
        'after_bootstrap',v_after,
        'cdc_eligible',v_handoff AND v_failed = 0,'cdc_resume_material',v_resume,
        'handoff_verified',false,
        'retry_disposition',CASE WHEN v_failed = 0 THEN 'NONE' ELSE 'RESTART_BOOTSTRAP' END,
        'source_read_boundary',jsonb_build_object('kind',v_boundary,'candidate',v_candidate,'cdc_verified',false),
        'source_consistency',v_read_contract,
        'plan',v_plan,'tables',v_results,'resources',v_resources,
        'resources_disposition','CLEANED','rows',v_total_rows,
        'artifact_intents',v_artifact_intents);
    INSERT INTO synchdb_bootstrap_runs(generation,connector_id,config_revision,result)
      VALUES(v_generation,v_connector_id,v_revision,v_result);
    GET DIAGNOSTICS v_count=ROW_COUNT;
    IF v_count<>1 OR NOT EXISTS (SELECT 1 FROM synchdb_bootstrap_runs saved_run
        WHERE saved_run.generation=v_generation AND saved_run.connector_id=v_connector_id
          AND saved_run.config_revision=v_revision AND saved_run.result=v_result) THEN
        RAISE EXCEPTION 'Bootstrap structured result was not persisted' USING ERRCODE='55000';
    END IF;
    RETURN v_result;
END;
$fn$;


-- Source: src/sql/fdw/snapshot.sql; sha256=9b56d048bd48df2291933ba2d34b0f27daf29b3c1cfb4a084ff25414edd010f7

CREATE OR REPLACE FUNCTION synchdb_do_initial_snapshot(
    p_connector_name  name,               -- e.g. 'oracleconn'
    p_source_schema   name,               -- e.g. 'ora_obj'   (FDW objects + current_scn FT)
    p_stage_schema    name,               -- e.g. 'ora_stage' (staging foreign tables)
    p_dest_schema     name,               -- e.g. 'dst_stage' (materialized tables)
    p_lookup_db       text,               -- e.g. 'free'
    p_lookup_schema   name,               -- e.g. 'dbzuser'
    p_lower_names     boolean DEFAULT true,
    p_on_exists       text    DEFAULT 'error',    -- only 'error' until target plans support adoption/replacement
    p_offset          text    DEFAULT null,          -- >0 to force a specific SCN; else auto-read
    p_snapshot_tables text    DEFAULT null,
    p_use_subtx         boolean DEFAULT true,
    p_write_schema_hist boolean DEFAULT false,
    p_case_strategy  text DEFAULT 'asis'
)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    v_schema name;
    v_request jsonb;
    v_selection jsonb := NULL;
    v_tables jsonb := '[]'::jsonb;
    v_parts text[];
    v_item text;
    v_result jsonb;
BEGIN
    IF p_on_exists IS NULL OR p_use_subtx IS NULL THEN
        RAISE EXCEPTION 'Bootstrap target and failure policies must be explicit' USING ERRCODE='22023';
    END IF;
    IF p_snapshot_tables IS NOT NULL THEN
        IF btrim(p_snapshot_tables)='' THEN
            RAISE EXCEPTION 'Snapshot selection is explicitly empty' USING ERRCODE='22023';
        END IF;
        FOREACH v_item IN ARRAY regexp_split_to_array(p_snapshot_tables,'\s*,\s*') LOOP
            v_parts:=parse_ident(v_item,true);
            IF cardinality(v_parts)=3 THEN
                v_tables:=v_tables||jsonb_build_array(jsonb_build_object('db',v_parts[1],'schema',v_parts[2],'table',v_parts[3]));
            ELSIF cardinality(v_parts)=2 THEN
                v_tables:=v_tables||jsonb_build_array(jsonb_build_object('db',v_parts[1],'schema',p_lookup_schema::text,'table',v_parts[2]));
            ELSE
                RAISE EXCEPTION 'Invalid explicit snapshot table identity: %',v_item USING ERRCODE='22023';
            END IF;
        END LOOP;
        v_selection:=jsonb_build_object('kind','tables','tables',v_tables);
    END IF;
    v_request:=jsonb_build_object('version',1,'connector_name',p_connector_name::text,
        'mode','initial_only','dest_schema',p_dest_schema::text,
        'case_strategy',coalesce(nullif(lower(p_case_strategy),''),CASE WHEN p_lower_names THEN 'lower' ELSE 'asis' END),
        'target_policy',p_on_exists,'failure_policy',CASE WHEN p_use_subtx THEN 'continue_tables' ELSE 'fail_fast' END,
        'offset',p_offset,'write_schema_history',p_write_schema_hist,'selection',v_selection);
    SELECT n.nspname INTO STRICT v_schema FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='synchdb';
    EXECUTE pg_catalog.format('SELECT %I.synchdb_bootstrap($1)',v_schema)
      INTO STRICT v_result USING v_request;
    RETURN v_result::text;
END;
$$;


-- Source: src/sql/fdw/schema_sync.sql; sha256=cce5ae022b726fd35a8d53ca5ce8251da25738c4c3031ea3cb5bdd2092559123

CREATE OR REPLACE FUNCTION synchdb_do_schema_sync(
    p_connector_name  name,               -- e.g. 'oracleconn'
    p_source_schema   name,               -- e.g. 'ora_obj'   (FDW objects + current_scn FT)
    p_stage_schema    name,               -- e.g. 'ora_stage' (staging foreign tables)
    p_dest_schema     name,               -- e.g. 'dst_stage' (materialized tables)
    p_lookup_db       text,               -- e.g. 'free'
    p_lookup_schema   name,               -- e.g. 'dbzuser'
    p_lower_names     boolean DEFAULT true,
    p_on_exists       text    DEFAULT 'error',    -- only 'error' until target plans support adoption/replacement
    p_offset          text    DEFAULT null,          -- >0 to force a specific SCN; else auto-read
    p_snapshot_tables text    DEFAULT null,
    p_use_subtx         boolean DEFAULT true,
    p_write_schema_hist boolean DEFAULT false,
    p_case_strategy  text DEFAULT 'asis'
)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    v_schema name;
    v_request jsonb;
    v_selection jsonb := NULL;
    v_tables jsonb := '[]'::jsonb;
    v_parts text[];
    v_item text;
    v_result jsonb;
BEGIN
    IF p_on_exists IS NULL OR p_use_subtx IS NULL THEN
        RAISE EXCEPTION 'Bootstrap target and failure policies must be explicit' USING ERRCODE='22023';
    END IF;
    IF p_snapshot_tables IS NOT NULL THEN
        IF btrim(p_snapshot_tables)='' THEN
            RAISE EXCEPTION 'Snapshot selection is explicitly empty' USING ERRCODE='22023';
        END IF;
        FOREACH v_item IN ARRAY regexp_split_to_array(p_snapshot_tables,'\s*,\s*') LOOP
            v_parts:=parse_ident(v_item,true);
            IF cardinality(v_parts)=3 THEN
                v_tables:=v_tables||jsonb_build_array(jsonb_build_object('db',v_parts[1],'schema',v_parts[2],'table',v_parts[3]));
            ELSIF cardinality(v_parts)=2 THEN
                v_tables:=v_tables||jsonb_build_array(jsonb_build_object('db',v_parts[1],'schema',p_lookup_schema::text,'table',v_parts[2]));
            ELSE
                RAISE EXCEPTION 'Invalid explicit snapshot table identity: %',v_item USING ERRCODE='22023';
            END IF;
        END LOOP;
        v_selection:=jsonb_build_object('kind','tables','tables',v_tables);
    END IF;
    v_request:=jsonb_build_object('version',1,'connector_name',p_connector_name::text,
        'mode','schemasync','dest_schema',p_dest_schema::text,
        'case_strategy',coalesce(nullif(lower(p_case_strategy),''),CASE WHEN p_lower_names THEN 'lower' ELSE 'asis' END),
        'target_policy',p_on_exists,'failure_policy',CASE WHEN p_use_subtx THEN 'continue_tables' ELSE 'fail_fast' END,
        'offset',p_offset,'write_schema_history',p_write_schema_hist,'selection',v_selection);
    SELECT n.nspname INTO STRICT v_schema FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='synchdb';
    EXECUTE pg_catalog.format('SELECT %I.synchdb_bootstrap($1)',v_schema)
      INTO STRICT v_result USING v_request;
    RETURN v_result::text;
END;
$$;


-- Source: src/sql/fdw/types.sql; sha256=a053051e29fdddad3ee90f085b3750c008593d86a8698a87e6db49ba5dbf2f60

CREATE OR REPLACE FUNCTION oracle_type_to_jdbc(ora_type_text text)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  t    text;      -- normalized input
  base text;      -- base type token(s)
  m    text[];    -- regex match for (...) args
BEGIN
  -- Normalize: trim, compress spaces, uppercase, strip quotes
  t := regexp_replace(upper(btrim(ora_type_text)), '\s+', ' ', 'g');
  t := replace(replace(t, '"', ''), '''', '');

  -- Multi-word TZ types FIRST (match full text)
  IF t LIKE 'TIMESTAMP% WITH TIME ZONE%' THEN
    RETURN -101;     -- OracleTypes.TIMESTAMPTZ
  ELSIF t LIKE 'TIMESTAMP% WITH LOCAL TIME ZONE%' THEN
    RETURN -102;     -- OracleTypes.TIMESTAMPLTZ
  ELSIF t LIKE 'INTERVAL YEAR% TO MONTH%' THEN
    RETURN -103;     -- OracleTypes.INTERVALYM
  ELSIF t LIKE 'INTERVAL DAY% TO SECOND%' THEN
    RETURN -104;     -- OracleTypes.INTERVALDS
  END IF;

  -- Extract base token before any ( ... ) or trailing qualifiers
  m := regexp_match(t, '^([A-Z ]+)\s*\(');
  IF m IS NOT NULL THEN
    base := btrim(m[1]);
  ELSE
    base := t;
  END IF;

  -- Switch on base (single-word & simple multi-word that remain)
  CASE base
    -- Numeric family (Debezium sets NUMBER/DECIMAL/NUMERIC → Types.NUMERIC)
    WHEN 'NUMBER', 'DECIMAL', 'NUMERIC', 'INT', 'INTEGER', 'SMALLINT' THEN
      RETURN 2;          -- java.sql.Types.NUMERIC

    -- Floating family
    WHEN 'BINARY_FLOAT' THEN
      RETURN 100;        -- OracleTypes.BINARY_FLOAT
    WHEN 'BINARY_DOUBLE' THEN
      RETURN 101;        -- OracleTypes.BINARY_DOUBLE
    WHEN 'FLOAT' THEN
      RETURN 6;          -- java.sql.Types.FLOAT
    WHEN 'DOUBLE' THEN
      -- In Debezium grammar DOUBLE PRECISION maps to FLOAT (length set separately)
      RETURN 6;          -- java.sql.Types.FLOAT
    WHEN 'REAL' THEN
      RETURN 6;          -- Debezium maps REAL → FLOAT too

    -- Date/Time (Oracle JDBC reports DATE as TIMESTAMP)
    WHEN 'DATE' THEN
      RETURN 93;         -- java.sql.Types.TIMESTAMP
    WHEN 'TIMESTAMP' THEN
      RETURN 93;         -- java.sql.Types.TIMESTAMP

    -- Character family
    WHEN 'CHAR', 'CHARACTER' THEN
      RETURN 1;          -- CHAR
    WHEN 'NCHAR' THEN
      RETURN -15;        -- NCHAR
    WHEN 'VARCHAR2', 'VARCHAR' THEN
      RETURN 12;         -- VARCHAR
    WHEN 'NVARCHAR2', 'NVARCHAR' THEN
      RETURN -9;         -- NVARCHAR
	WHEN 'LONG' THEN
	  RETURN -1;         -- LONG

    -- LOBs
    WHEN 'BLOB' THEN
      RETURN 2004;       -- BLOB
    WHEN 'CLOB' THEN
      RETURN 2005;       -- CLOB
    WHEN 'NCLOB' THEN
      RETURN 2011;       -- NCLOB
    WHEN 'BFILE' THEN
      RETURN -13;        -- OracleTypes.BFILE

    -- Binary
    WHEN 'RAW' THEN
      RETURN -3;         -- OracleTypes.RAW (maps to BINARY)
    WHEN 'LONG RAW' THEN
      RETURN -4;         -- LONGVARBINARY

    -- Rowid (Debezium code sets ROWID → VARCHAR)
    WHEN 'ROWID', 'UROWID' THEN
      RETURN -8;         -- VARCHAR

    -- XML / Spatial / Other
    WHEN 'XMLTYPE' THEN
      RETURN 2009;       -- SQLXML (fall back to 1111 if your driver lacks it)
    WHEN 'SDO_GEOMETRY' THEN
      RETURN 1111;       -- OTHER

    -- 23ai BOOLEAN (if encountered)
    WHEN 'BOOLEAN' THEN
      RETURN 16;         -- java.sql.Types.BOOLEAN

    ELSE
      RETURN 1111;       -- OTHER (catch-all for unrecognized)
  END CASE;
END;
$$;

CREATE OR REPLACE FUNCTION mysql_type_to_jdbc(mysql_type_text text)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  t           text;     -- normalized input
  base        text;     -- base type token(s) without length/precision
  m           text[];   -- regex match for (...) args
  is_unsigned boolean := false;
BEGIN
  -- Normalize: trim, compress spaces, uppercase, strip quotes/backticks
  t := regexp_replace(upper(btrim(mysql_type_text)), '\s+', ' ', 'g');
  t := replace(replace(t, '"', ''), '''', '');
  t := replace(t, '`', '');

  -- Detect UNSIGNED flag (doesn't usually affect jdbcType, but matters for BIGINT)
  IF t LIKE '% UNSIGNED' THEN
    is_unsigned := true;
    t := regexp_replace(t, '\s+UNSIGNED$', '');
  END IF;

  -- Extract base token before any ( ... ) or trailing qualifiers
  m := regexp_match(t, '^([A-Z0-9_ ]+)\s*\(');
  IF m IS NOT NULL THEN
    base := btrim(m[1]);
  ELSE
    base := t;
  END IF;

  /*
   * Map to java.sql.Types (int) in a Debezium-like way.
   *
   * Reference (commonly used values):
   *   CHAR            -> 1
   *   VARCHAR/TEXT    -> 12
   *   BIT             -> -7
   *   TINYINT/SMALLINT-> 5
   *   INTEGER         -> 4
   *   BIGINT          -> -5 (or 3 when UNSIGNED to avoid overflow)
   *   NUMERIC/DECIMAL -> 3
   *   REAL/FLOAT      -> 6
   *   DOUBLE          -> 8
   *   DATE            -> 91
   *   TIME            -> 92
   *   TIMESTAMP       -> 2014 (timestamp with time zone, as in your example)
   *   BINARY          -> -2
   *   VARBINARY       -> -3
   *   BLOB*           -> 2004
   *   JSON/GEOMETRY   -> 1111 (OTHER)
   */

  CASE base

    ------------------------------------------------------------------
    -- Exact numeric / decimal family
    ------------------------------------------------------------------
    WHEN 'DECIMAL', 'NUMERIC', 'FIXED' THEN
      RETURN 3;       -- DECIMAL (signed or unsigned; length/scale handled elsewhere)

    ------------------------------------------------------------------
    -- Float / double family
    ------------------------------------------------------------------
    WHEN 'DOUBLE', 'DOUBLE PRECISION' THEN
      RETURN 8;       -- DOUBLE
    WHEN 'REAL', 'FLOAT' THEN
      RETURN 6;       -- FLOAT

    ------------------------------------------------------------------
    -- Integer family
    ------------------------------------------------------------------
    WHEN 'BIGINT' THEN
      -- Debezium uses DECIMAL for BIGINT UNSIGNED to avoid overflow.
      IF is_unsigned THEN
        RETURN 3;     -- DECIMAL
      ELSE
        RETURN -5;    -- BIGINT
      END IF;

    WHEN 'INT', 'INTEGER', 'MEDIUMINT' THEN
      RETURN 4;       -- INTEGER

    WHEN 'SMALLINT', 'TINYINT', 'YEAR' THEN
      -- Your example shows TINYINT and YEAR as int/short-ish types
      RETURN 5;       -- SMALLINT

    ------------------------------------------------------------------
    -- Bit / boolean
    ------------------------------------------------------------------
    WHEN 'BIT' THEN
      RETURN -7;      -- BIT

    WHEN 'BOOL', 'BOOLEAN' THEN
      RETURN 16;      -- BOOLEAN (not from your example, but sane to support)

    ------------------------------------------------------------------
    -- Character / text
    ------------------------------------------------------------------
    WHEN 'CHAR', 'NCHAR' THEN
      RETURN 1;       -- CHAR

    WHEN 'VARCHAR', 'NVARCHAR',
         'TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT',
         'ENUM', 'SET' THEN
      -- Debezium maps all these to VARCHAR-ish
      RETURN 12;      -- VARCHAR

    ------------------------------------------------------------------
    -- Date / time
    ------------------------------------------------------------------
    WHEN 'DATE' THEN
      RETURN 91;      -- DATE

    WHEN 'TIME' THEN
      RETURN 92;      -- TIME (with or without fractional seconds; length handled separately)

    WHEN 'DATETIME' THEN
      RETURN 93;      -- TIMESTAMP

    WHEN 'TIMESTAMP' THEN
      -- In your example, TIMESTAMP maps to 2014 (TIMESTAMP_WITH_TIMEZONE)
      RETURN 2014;

    ------------------------------------------------------------------
    -- Binary / blobs
    ------------------------------------------------------------------
    WHEN 'BINARY' THEN
      RETURN -2;      -- BINARY

    WHEN 'VARBINARY' THEN
      RETURN -3;      -- VARBINARY

    WHEN 'TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB' THEN
      RETURN 2004;    -- BLOB

    ------------------------------------------------------------------
    -- JSON and spatial types
    ------------------------------------------------------------------
    WHEN 'JSON' THEN
      RETURN 1111;    -- OTHER

    WHEN 'GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON',
         'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON',
         'GEOMETRYCOLLECTION' THEN
      RETURN 1111;    -- OTHER

    ------------------------------------------------------------------
    -- Fallback
    ------------------------------------------------------------------
    ELSE
      RETURN 1111;    -- OTHER (catch-all)
  END CASE;
END;
$$;

CREATE OR REPLACE FUNCTION synchdb_type_to_jdbc(
    p_connector text,
    p_type_text text
)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
    v_conn text := lower(p_connector);
BEGIN
    CASE v_conn
        WHEN 'oracle', 'olr' THEN
            RETURN oracle_type_to_jdbc(p_type_text);

        WHEN 'mysql' THEN
            RETURN mysql_type_to_jdbc(p_type_text);
        ELSE
            RAISE EXCEPTION
                'synchdb_type_to_jdbc: unsupported connector type % for type %',
                p_connector, p_type_text;
    END CASE;
END;
$$;


-- Source: src/sql/fdw/compat.sql; sha256=fda908c6ebe6cd5dc4754a9edbe46c250efc9ec94394735a74ae8a356b78707c

-- Compatibility entry points for the original Oracle-specific names.
-- Both helpers serve all FDW connectors.  Preserve argument names and defaults
-- so existing positional, named, and default-argument calls keep working.

CREATE OR REPLACE FUNCTION synchdb_materialize_ora_metadata(
    p_source_schema name,
    p_dest_schema   name,
    p_on_exists     text DEFAULT 'replace'
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM synchdb_materialize_metadata(
        p_source_schema, p_dest_schema, p_on_exists
    );
END;
$$;

COMMENT ON FUNCTION synchdb_materialize_ora_metadata(name, name, text) IS
   'compatibility alias for synchdb_materialize_metadata';

CREATE OR REPLACE FUNCTION synchdb_create_ora_stage_fts(
    p_connector_name        name,
    p_desired_db            name,
    p_desired_schema        name,
    p_stage_schema          name DEFAULT 'ora_stage'::name,
    p_server_name           name DEFAULT 'oracle'::name,
    p_lower_names           boolean DEFAULT true,
    p_on_exists             text DEFAULT 'replace',
    p_offset                text DEFAULT NULL,
    p_source_schema         name DEFAULT 'ora_obj'::name,
    p_snapshot_tables       text DEFAULT NULL,
    p_write_dbz_schema_info boolean DEFAULT false,
    p_case_strategy         text DEFAULT 'asis'
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM synchdb_create_stage_fts(
        p_connector_name, p_desired_db, p_desired_schema, p_stage_schema,
        p_server_name, p_lower_names, p_on_exists, p_offset, p_source_schema,
        p_snapshot_tables, p_write_dbz_schema_info, p_case_strategy
    );
END;
$$;

COMMENT ON FUNCTION synchdb_create_ora_stage_fts(name, name, name, name, name, boolean, text, text, name, text, boolean, text) IS
   'compatibility alias for synchdb_create_stage_fts';


-- Source: src/sql/catalog_config_backup.sql; sha256=0163e32a5a196fbf8f39584675234d5f538184f4fa61a754e05d4dfd405f0de3

-- Portable configuration is separate from database backup and CDC recovery.
-- This format carries caller-visible configuration and encrypted credentials,
-- never grants, RLS policies, accepted execution roles, physical OIDs or offsets.
CREATE OR REPLACE FUNCTION synchdb_export_configuration()
RETURNS pg_catalog.jsonb
LANGUAGE plpgsql VOLATILE SECURITY INVOKER
SET search_path = pg_catalog
AS $synchdb_export_configuration$
DECLARE
    v_extension pg_catalog.oid;
    v_namespace pg_catalog.oid;
    v_schema pg_catalog.name;
    v_tables pg_catalog.text[] := ARRAY['synchdb_conninfo','synchdb_objmap'];
    v_relations pg_catalog.oid[] := ARRAY[]::pg_catalog.oid[];
    v_relation pg_catalog.oid;
    v_table pg_catalog.text;
    v_lock pg_catalog.text := 'LOCK TABLE ';
    v_limit pg_catalog.int4 := pg_catalog.current_setting('max_identifier_length')::pg_catalog.int4;
    v_valid pg_catalog.bool;
    v_result pg_catalog.jsonb;
    i pg_catalog.int4;
BEGIN
    SELECT e.oid, e.extnamespace, n.nspname
      INTO STRICT v_extension, v_namespace, v_schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'synchdb';
    FOREACH v_table IN ARRAY v_tables LOOP
        SELECT c.oid INTO v_relation
          FROM pg_catalog.pg_class c
         WHERE c.relnamespace = v_namespace AND c.relname::pg_catalog.text = v_table
           AND c.relkind = 'r' AND NOT c.relispartition
           AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h
                            WHERE h.inhrelid = c.oid OR h.inhparent = c.oid)
           AND EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                        WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                          AND d.objid = c.oid AND d.objsubid = 0
                          AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                          AND d.refobjid = v_extension AND d.deptype = 'e');
        IF v_relation IS NULL THEN
            RAISE EXCEPTION 'Configuration catalog is not an ordinary synchdb extension table'
                USING ERRCODE = '55000';
        END IF;
        v_relations := pg_catalog.array_append(v_relations, v_relation);
        IF pg_catalog.array_length(v_relations, 1) > 1 THEN v_lock := v_lock || ', '; END IF;
        v_lock := v_lock || pg_catalog.format('ONLY %I.%I', v_schema, v_table);
    END LOOP;
    -- The caller must have its own table privileges; neither this lock nor the
    -- data query changes the effective role or disables row security.
    EXECUTE v_lock || ' IN ACCESS SHARE MODE';
    FOR i IN 1..pg_catalog.array_length(v_tables, 1) LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_extension e
            JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
            JOIN pg_catalog.pg_class c ON c.relnamespace = e.extnamespace
            JOIN pg_catalog.pg_depend d
              ON d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
             AND d.objid = c.oid AND d.objsubid = 0
             AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
             AND d.refobjid = e.oid AND d.deptype = 'e'
           WHERE e.oid = v_extension AND e.extname = 'synchdb'
             AND e.extnamespace = v_namespace AND n.nspname = v_schema
             AND c.oid = v_relations[i] AND c.relname::pg_catalog.text = v_tables[i]
             AND c.relkind = 'r' AND NOT c.relispartition
             AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h
                              WHERE h.inhrelid = c.oid OR h.inhparent = c.oid)
        ) THEN
            RAISE EXCEPTION 'Configuration catalog changed while acquiring its lock'
                USING ERRCODE = '55000';
        END IF;
    END LOOP;
    -- Both materialized inputs and all their validation use one statement
    -- snapshot. RLS filters each input before it is aggregated or validated.
    EXECUTE pg_catalog.format($query$
        WITH connectors AS MATERIALIZED (
            SELECT name, connector_id, config_revision, data FROM %1$I.synchdb_conninfo
        ), mappings AS MATERIALIZED (
            SELECT name, objtype, enabled, srcobj, dstobj FROM %1$I.synchdb_objmap
        )
        SELECT pg_catalog.jsonb_build_object(
            'format', 'synchdb-configuration', 'version', 1,
            'connectors', (SELECT COALESCE(pg_catalog.jsonb_agg(
                pg_catalog.jsonb_build_object('name', c.name, 'connector_id', c.connector_id,
                    'config_revision', c.config_revision, 'data', c.data)
                ORDER BY c.name COLLATE pg_catalog."C"), '[]'::pg_catalog.jsonb) FROM connectors c),
            'mappings', (SELECT COALESCE(pg_catalog.jsonb_agg(
                pg_catalog.jsonb_build_object('name', m.name, 'objtype', m.objtype,
                    'enabled', m.enabled, 'srcobj', m.srcobj, 'dstobj', m.dstobj)
                ORDER BY m.name::pg_catalog.text COLLATE pg_catalog."C",
                    m.objtype::pg_catalog.text COLLATE pg_catalog."C", m.srcobj COLLATE pg_catalog."C"),
                '[]'::pg_catalog.jsonb) FROM mappings m)),
            (SELECT COALESCE(pg_catalog.bool_and((
                c.name <> '' AND pg_catalog.octet_length(c.name) <= $1
                AND c.connector_id <> '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid
                AND c.config_revision > 0 AND pg_catalog.jsonb_typeof(c.data) = 'object'
            ) IS TRUE), true) FROM connectors c)
            AND (SELECT COALESCE(pg_catalog.bool_and((
                m.name::pg_catalog.text <> '' AND pg_catalog.octet_length(m.name::pg_catalog.text) <= $1
                AND m.objtype::pg_catalog.text IN ('table','column','datatype','transform')
                AND m.srcobj <> '' AND pg_catalog.octet_length(m.srcobj) <= 2400
            ) IS TRUE), true) FROM mappings m)
    $query$, v_schema) INTO v_result, v_valid USING v_limit;
    IF v_valid IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'Caller-visible configuration contains a row outside the portable format'
            USING ERRCODE = '22023';
    END IF;
    RETURN v_result;
END;
$synchdb_export_configuration$;

CREATE OR REPLACE FUNCTION synchdb_import_configuration(p_configuration pg_catalog.jsonb)
RETURNS pg_catalog.int8
LANGUAGE plpgsql VOLATILE SECURITY INVOKER
SET search_path = pg_catalog
AS $synchdb_import_configuration$
DECLARE
    v_extension pg_catalog.oid;
    v_namespace pg_catalog.oid;
    v_schema pg_catalog.name;
    v_tables pg_catalog.text[] := ARRAY[
        'synchdb_conninfo','synchdb_objmap','synchdb_attribute',
        'synchdb_control_state','synchdb_control_commands','synchdb_artifacts','synchdb_olr_positions',
        'synchdb_bootstrap_targets','synchdb_bootstrap_runs'];
    v_relations pg_catalog.oid[] := ARRAY[]::pg_catalog.oid[];
    v_relation pg_catalog.oid;
    v_table pg_catalog.text;
    v_lock pg_catalog.text := 'LOCK TABLE ';
    v_empty_query pg_catalog.text := 'SELECT ';
    v_nonempty pg_catalog.bool;
    v_row pg_catalog.jsonb;
    v_keys pg_catalog.int8;
    v_revision pg_catalog.text;
    v_limit pg_catalog.int4 := pg_catalog.current_setting('max_identifier_length')::pg_catalog.int4;
    v_expected pg_catalog.int8;
    v_inserted pg_catalog.int8;
    i pg_catalog.int4;
BEGIN
    -- Check the effective execution role, including SET ROLE, rather than the
    -- session's original authenticated role. Import grants no extra authority.
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                    WHERE rolname = CURRENT_USER AND rolsuper) THEN
        RAISE EXCEPTION 'Configuration import requires a superuser'
            USING ERRCODE = '42501';
    END IF;
    -- Under a transaction-wide old snapshot an empty check could miss rows
    -- committed before these locks. VOLATILE queries in READ COMMITTED obtain
    -- a fresh snapshot after all competing catalog writers have been excluded.
    IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'Configuration import requires READ COMMITTED isolation'
            USING ERRCODE = '0A000';
    END IF;
    IF p_configuration IS NULL OR pg_catalog.jsonb_typeof(p_configuration) <> 'object' THEN
        RAISE EXCEPTION 'Expected a portable configuration object' USING ERRCODE = '22023';
    END IF;
    SELECT pg_catalog.count(*) INTO v_keys FROM pg_catalog.jsonb_object_keys(p_configuration);
    IF v_keys <> 4 OR NOT (p_configuration ?& ARRAY['format','version','connectors','mappings'])
       OR pg_catalog.jsonb_typeof(p_configuration->'format') IS DISTINCT FROM 'string'
       OR p_configuration->>'format' IS DISTINCT FROM 'synchdb-configuration'
       OR pg_catalog.jsonb_typeof(p_configuration->'version') IS DISTINCT FROM 'number'
       OR p_configuration->>'version' IS DISTINCT FROM '1'
       OR pg_catalog.jsonb_typeof(p_configuration->'connectors') IS DISTINCT FROM 'array'
       OR pg_catalog.jsonb_typeof(p_configuration->'mappings') IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'Invalid portable configuration envelope or version'
            USING ERRCODE = '22023';
    END IF;
    SELECT e.oid, e.extnamespace, n.nspname
      INTO STRICT v_extension, v_namespace, v_schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'synchdb';
    FOREACH v_table IN ARRAY v_tables LOOP
        SELECT c.oid INTO v_relation
          FROM pg_catalog.pg_class c
         WHERE c.relnamespace = v_namespace AND c.relname::pg_catalog.text = v_table
           AND c.relkind = 'r' AND NOT c.relispartition
           AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h
                            WHERE h.inhrelid = c.oid OR h.inhparent = c.oid)
           AND EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                        WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                          AND d.objid = c.oid AND d.objsubid = 0
                          AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                          AND d.refobjid = v_extension AND d.deptype = 'e');
        IF v_relation IS NULL THEN
            RAISE EXCEPTION 'Configuration import requires ordinary synchdb extension catalogs'
                USING ERRCODE = '55000';
        END IF;
        v_relations := pg_catalog.array_append(v_relations, v_relation);
        IF pg_catalog.array_length(v_relations, 1) > 1 THEN
            v_lock := v_lock || ', ';
            v_empty_query := v_empty_query || ' OR ';
        END IF;
        v_lock := v_lock || pg_catalog.format('ONLY %I.%I', v_schema, v_table);
        v_empty_query := v_empty_query || pg_catalog.format('EXISTS (SELECT 1 FROM ONLY %I.%I)', v_schema, v_table);
    END LOOP;
    -- Do not wait while holding part of this set: controller reads and control
    -- submissions use different catalog lock orders. Retry a lock conflict in
    -- a new transaction; no configuration has been written at that point.
    EXECUTE v_lock || ' IN ACCESS EXCLUSIVE MODE NOWAIT';
    FOR i IN 1..pg_catalog.array_length(v_tables, 1) LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_extension e
            JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
            JOIN pg_catalog.pg_class c ON c.relnamespace = e.extnamespace
            JOIN pg_catalog.pg_depend d
              ON d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
             AND d.objid = c.oid AND d.objsubid = 0
             AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
             AND d.refobjid = e.oid AND d.deptype = 'e'
           WHERE e.oid = v_extension AND e.extname = 'synchdb'
             AND e.extnamespace = v_namespace AND n.nspname = v_schema
             AND c.oid = v_relations[i] AND c.relname::pg_catalog.text = v_tables[i]
             AND c.relkind = 'r' AND NOT c.relispartition
             AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h
                              WHERE h.inhrelid = c.oid OR h.inhparent = c.oid)
        ) THEN
            RAISE EXCEPTION 'Configuration catalog changed while acquiring its lock'
                USING ERRCODE = '55000';
        END IF;
    END LOOP;
    EXECUTE v_empty_query INTO v_nonempty;
    IF v_nonempty THEN
        RAISE EXCEPTION 'Configuration import requires all eight synchdb catalogs to be empty'
            USING ERRCODE = '55000';
    END IF;
    -- Validate before typed conversion: jsonb_to_recordset otherwise accepts
    -- string-to-number coercion and NAME silently truncates overlong input.
    -- jsonb has already normalized object keys; this is not a duplicate-key
    -- check of the original JSON text supplied by a client.
    FOR v_row IN SELECT value FROM pg_catalog.jsonb_array_elements(p_configuration->'connectors') LOOP
        IF pg_catalog.jsonb_typeof(v_row) IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'Invalid portable connector row' USING ERRCODE = '22023';
        END IF;
        SELECT pg_catalog.count(*) INTO v_keys FROM pg_catalog.jsonb_object_keys(v_row);
        IF v_keys <> 4 OR NOT (v_row ?& ARRAY['name','connector_id','config_revision','data'])
           OR pg_catalog.jsonb_typeof(v_row->'name') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_row->'connector_id') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_row->'config_revision') IS DISTINCT FROM 'number'
           OR pg_catalog.jsonb_typeof(v_row->'data') IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'Invalid portable connector fields' USING ERRCODE = '22023';
        END IF;
        v_revision := v_row->>'config_revision';
        IF v_row->>'name' = '' OR pg_catalog.octet_length(v_row->>'name') > v_limit
           OR (v_row->>'connector_id') COLLATE pg_catalog."C" !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           OR v_row->>'connector_id' = '00000000-0000-0000-0000-000000000000'
           OR v_revision COLLATE pg_catalog."C" !~ '^[1-9][0-9]*$'
           OR pg_catalog.length(v_revision) > 19
           OR (pg_catalog.length(v_revision) = 19 AND v_revision COLLATE pg_catalog."C" > '9223372036854775807' COLLATE pg_catalog."C") THEN
            RAISE EXCEPTION 'Portable connector identity or revision is outside its supported range'
                USING ERRCODE = '22023';
        END IF;
    END LOOP;
    FOR v_row IN SELECT value FROM pg_catalog.jsonb_array_elements(p_configuration->'mappings') LOOP
        IF pg_catalog.jsonb_typeof(v_row) IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'Invalid portable mapping row' USING ERRCODE = '22023';
        END IF;
        SELECT pg_catalog.count(*) INTO v_keys FROM pg_catalog.jsonb_object_keys(v_row);
        IF v_keys <> 5 OR NOT (v_row ?& ARRAY['name','objtype','enabled','srcobj','dstobj'])
           OR pg_catalog.jsonb_typeof(v_row->'name') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_row->'objtype') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_row->'enabled') NOT IN ('boolean','null')
           OR pg_catalog.jsonb_typeof(v_row->'srcobj') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_row->'dstobj') NOT IN ('string','null') THEN
            RAISE EXCEPTION 'Invalid portable mapping fields' USING ERRCODE = '22023';
        END IF;
        IF v_row->>'name' = '' OR pg_catalog.octet_length(v_row->>'name') > v_limit
           OR pg_catalog.octet_length(v_row->>'objtype') > v_limit
           OR v_row->>'objtype' NOT IN ('table','column','datatype','transform')
           OR v_row->>'srcobj' = '' OR pg_catalog.octet_length(v_row->>'srcobj') > 2400 THEN
            RAISE EXCEPTION 'Portable mapping identifier is outside its supported range'
                USING ERRCODE = '22023';
        END IF;
        -- No connector FK is invented here. An RLS-filtered export may contain
        -- mappings whose connector was invisible to that caller. enabled and
        -- dstobj retain the catalog's nullable-field semantics.
    END LOOP;
    v_expected := pg_catalog.jsonb_array_length(p_configuration->'connectors');
    EXECUTE pg_catalog.format($insert$
        INSERT INTO %I.synchdb_conninfo(name,isactive,data,connector_id,config_revision)
        SELECT c.name, false, c.data, c.connector_id, c.config_revision
          FROM pg_catalog.jsonb_to_recordset($1) AS c(
              name pg_catalog.text, connector_id pg_catalog.uuid,
              config_revision pg_catalog.int8, data pg_catalog.jsonb)
    $insert$, v_schema) USING p_configuration->'connectors';
    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    IF v_inserted <> v_expected THEN
        RAISE EXCEPTION 'Configuration import did not insert every connector row' USING ERRCODE = '55000';
    END IF;
    EXECUTE pg_catalog.format($insert$
        INSERT INTO %I.synchdb_objmap(name,objtype,enabled,srcobj,dstobj)
        SELECT m.name, m.objtype, m.enabled, m.srcobj, m.dstobj
          FROM pg_catalog.jsonb_to_recordset($1) AS m(
              name pg_catalog.name, objtype pg_catalog.name, enabled pg_catalog.bool,
              srcobj pg_catalog.text, dstobj pg_catalog.text)
    $insert$, v_schema) USING p_configuration->'mappings';
    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    IF v_inserted <> pg_catalog.jsonb_array_length(p_configuration->'mappings') THEN
        RAISE EXCEPTION 'Configuration import did not insert every mapping row' USING ERRCODE = '55000';
    END IF;
    RETURN v_expected;
END;
$synchdb_import_configuration$;

REVOKE ALL ON FUNCTION synchdb_export_configuration(), synchdb_import_configuration(pg_catalog.jsonb)
    FROM PUBLIC;


-- Source: src/sql/permissions.sql; sha256=f2042c4573747d685c3c0ee27df4828c998e6973ccaaa793809526ed7b6c7a28

-- New installations expose APIs only through explicit grants. Resolve members
-- through extension dependency, including relocated extensions.
DO $synchdb_permissions$
DECLARE
    f record;
BEGIN
    FOR f IN
        SELECT p.oid::pg_catalog.regprocedure AS signature,
               l.lanname AS language_name
          FROM pg_catalog.pg_proc p
          JOIN pg_catalog.pg_language l ON l.oid = p.prolang
          JOIN pg_catalog.pg_depend d
            ON d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
           AND d.objid = p.oid AND d.objsubid = 0
           AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
           AND d.deptype = 'e'
          JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid
         WHERE e.extname = 'synchdb'
    LOOP
        IF f.language_name = 'c' THEN
            EXECUTE pg_catalog.format('ALTER FUNCTION %s VOLATILE', f.signature);
        END IF;
        EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', f.signature);
    END LOOP;
END
$synchdb_permissions$;
