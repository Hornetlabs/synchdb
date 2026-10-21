/*
 * run_spec.c
 *
 * Validate and normalize the value passed from a launch request to its worker.
 * This module deliberately has no dependency on configuration globals or on
 * the connector engine lifecycle.
 */
#include "postgres.h"

#include "runtime/run_spec.h"
#include "source/consistency.h"

static void
run_check_string(const char *value, Size capacity, const char *field)
{
	if (memchr(value, '\0', capacity) == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("runtime configuration field \"%s\" is not terminated", field)));
}

static void
run_check_range(int value, int minimum, int maximum, const char *field)
{
	if (value < minimum || value > maximum)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("runtime configuration field \"%s\" must be between %d and %d",
						field, minimum, maximum)));
}

static void
run_check_options(const SynchdbRunOptions *options)
{
	if (options->version != SYNCHDB_RUN_OPTIONS_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported runtime options version %u", options->version)));

#define CHECK_RANGE(field, minimum, maximum) \
	run_check_range(options->field, minimum, maximum, #field)
#define CHECK_STRING(field) \
	run_check_string(options->field, sizeof(options->field), #field)
	CHECK_RANGE(dbz_batch_size, 1, 65535);
	CHECK_RANGE(dbz_queue_size, 64, 65535);
	if (options->dbz_queue_size <= options->dbz_batch_size)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Debezium queue size must exceed batch size")));
	CHECK_RANGE(dbz_connect_timeout_ms, 1000, 3600000);
	CHECK_RANGE(dbz_query_timeout_ms, 1000, 3600000);
	CHECK_STRING(dbz_skipped_operations);
	CHECK_RANGE(dbz_snapshot_thread_num, 1, 16);
	CHECK_RANGE(dbz_snapshot_fetch_size, 0, 65535);
	CHECK_RANGE(dbz_snapshot_min_row_to_stream_results, 0, 65535);
	CHECK_RANGE(dbz_incremental_snapshot_chunk_size, 1, 65535);
	CHECK_STRING(dbz_incremental_snapshot_watermarking_strategy);
	CHECK_RANGE(dbz_offset_flush_interval_ms, 1000, 3600000);
	CHECK_RANGE(dbz_log_level, LOG_LEVEL_ALL, LOG_LEVEL_TRACE);
	CHECK_RANGE(dbz_logminer_stream_mode, LOGMINER_MODE_UNDEF,
				LOGMINER_MODE_COMMITTED);
	CHECK_RANGE(cdc_start_delay_ms, 0, 65536);
	CHECK_RANGE(jvm_max_heap_size, 1, 65536);
	CHECK_RANGE(jvm_max_direct_buffer_size, 0, 65536);
	CHECK_RANGE(synchdb_letter_casing_strategy, LCS_AS_IS,
				LCS_NORMALIZE_UPPERCASE);
#undef CHECK_STRING
#undef CHECK_RANGE
}

static void
run_check_connection(const ConnectionInfo *connection)
{
	if (!synchdb_connector_key_is_valid(&connection->key) || connection->config_revision <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("runtime configuration requires a stable connector identity and positive revision")));
#define CHECK_STRING(field) \
	run_check_string(connection->field, sizeof(connection->field), #field)
	CHECK_STRING(name);
	CHECK_STRING(hostname);
	CHECK_STRING(user);
	CHECK_STRING(pwd);
	CHECK_STRING(srcdb);
	CHECK_STRING(srcschema);
	CHECK_STRING(dstdb);
	CHECK_STRING(table);
	CHECK_STRING(snapshottable);
	CHECK_STRING(extra.ssl_mode);
	CHECK_STRING(extra.ssl_keystore);
	CHECK_STRING(extra.ssl_keystore_pass);
	CHECK_STRING(extra.ssl_truststore);
	CHECK_STRING(extra.ssl_truststore_pass);
	CHECK_STRING(jmx.jmx_listenaddr);
	CHECK_STRING(jmx.jmx_rmiserveraddr);
	CHECK_STRING(jmx.jmx_auth_passwdfile);
	CHECK_STRING(jmx.jmx_auth_accessfile);
	CHECK_STRING(jmx.jmx_ssl_keystore);
	CHECK_STRING(jmx.jmx_ssl_keystore_pass);
	CHECK_STRING(jmx.jmx_ssl_truststore);
	CHECK_STRING(jmx.jmx_ssl_truststore_pass);
	CHECK_STRING(jmx.jmx_exporter);
	CHECK_STRING(jmx.jmx_exporter_conf);
	CHECK_STRING(olr.olr_host);
	CHECK_STRING(olr.olr_source);
	CHECK_STRING(fdw.ssl_cert);
	CHECK_STRING(fdw.ssl_key);
	CHECK_STRING(fdw.ssl_rootcert);
	CHECK_STRING(fdw.ssl_cipher);
	CHECK_STRING(ispn.ispn_cache_type);
	CHECK_STRING(ispn.ispn_memory_type);
#undef CHECK_STRING
	run_check_options(&connection->run_options);
}

void
synchdb_run_spec_init(SynchdbRunSpec *out, ConnectorType type,
					  const ConnectionInfo *connection,
					  const char *snapshot_mode)
{
	SynchdbRunSpec spec = {0};
	Size mode_length;

	if (out == NULL || connection == NULL || snapshot_mode == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("runtime configuration inputs must not be null")));

	mode_length = strnlen(snapshot_mode, sizeof(spec.snapshot_mode));
	if (mode_length >= sizeof(spec.snapshot_mode))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("snapshot mode must be shorter than %d bytes",
						SYNCHDB_SNAPSHOT_MODE_SIZE)));
	run_check_connection(connection);
	spec.legacy_type = type;
	spec.capture_backend = SYNCHDB_CAPTURE_DEBEZIUM;
	switch (type)
	{
		case TYPE_MYSQL:
			spec.source_dialect = SYNCHDB_SOURCE_MYSQL;
			break;
		case TYPE_ORACLE:
			spec.source_dialect = SYNCHDB_SOURCE_ORACLE;
			break;
		case TYPE_SQLSERVER:
			spec.source_dialect = SYNCHDB_SOURCE_SQLSERVER;
			break;
		case TYPE_POSTGRES:
			spec.source_dialect = SYNCHDB_SOURCE_POSTGRES;
			break;
		case TYPE_OLR:
			spec.source_dialect = SYNCHDB_SOURCE_ORACLE;
			spec.capture_backend = SYNCHDB_CAPTURE_OLR;
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsupported connector type %d", type)));
	}

	switch (connection->snapengine)
	{
		case ENGINE_DEBEZIUM:
			spec.snapshot_provider = SYNCHDB_SNAPSHOT_DEBEZIUM;
			break;
		case ENGINE_FDW:
			spec.snapshot_provider = SYNCHDB_SNAPSHOT_FDW;
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsupported snapshot engine %d", connection->snapengine)));
	}

	spec.connection = *connection;
	spec.connection.flag &= ~(CONNFLAG_INITIAL_SNAPSHOT_MODE);
	if (pg_strcasecmp(snapshot_mode, "schemasync") == 0)
	{
		strlcpy(spec.snapshot_mode, "no_data", sizeof(spec.snapshot_mode));
		spec.connection.flag |= CONNFLAG_SCHEMA_SYNC_MODE;
	}
	else
		memcpy(spec.snapshot_mode, snapshot_mode, mode_length + 1);
	/* NO_CDC describes this request, while INITIAL_SNAPSHOT is execution state. */
	spec.connection.flag &= ~(CONNFLAG_NO_CDC_MODE);
	if (pg_strcasecmp(spec.snapshot_mode, "initial_only") == 0)
		spec.connection.flag |= CONNFLAG_NO_CDC_MODE;

	if (spec.source_dialect == SYNCHDB_SOURCE_POSTGRES &&
		spec.snapshot_provider == SYNCHDB_SNAPSHOT_DEBEZIUM &&
		(spec.connection.flag & CONNFLAG_SCHEMA_SYNC_MODE))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("PostgreSQL Debezium connectors do not support automatic schema sync"),
				 errhint("Use the FDW snapshot engine for schema sync.")));
	if (spec.snapshot_provider == SYNCHDB_SNAPSHOT_FDW &&
		!(spec.connection.flag & CONNFLAG_SCHEMA_SYNC_MODE) &&
		pg_strcasecmp(spec.snapshot_mode, "initial_only") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("FDW bootstrap cannot establish a verified CDC handoff for mode %s",
						spec.snapshot_mode),
				 errhint("Use initial_only or schemasync; select the capture provider for engine-owned CDC.")));

	synchdb_source_consistency_validate_run(&spec);
	*out = spec;
}

static bool
run_jmx_equal(const JMXConnectionInfo *left, const JMXConnectionInfo *right)
{
#define SAME_STRING(field) (strcmp(left->field, right->field) == 0)
	return SAME_STRING(jmx_listenaddr) && left->jmx_port == right->jmx_port &&
		SAME_STRING(jmx_rmiserveraddr) && left->jmx_rmiport == right->jmx_rmiport &&
		left->jmx_auth == right->jmx_auth &&
		SAME_STRING(jmx_auth_passwdfile) && SAME_STRING(jmx_auth_accessfile) &&
		left->jmx_ssl == right->jmx_ssl &&
		SAME_STRING(jmx_ssl_keystore) && SAME_STRING(jmx_ssl_keystore_pass) &&
		SAME_STRING(jmx_ssl_truststore) && SAME_STRING(jmx_ssl_truststore_pass) &&
		SAME_STRING(jmx_exporter) && left->jmx_exporter_port == right->jmx_exporter_port &&
		SAME_STRING(jmx_exporter_conf);
#undef SAME_STRING
}

static void
run_require_new_worker(const char *field)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot change %s in a running connector", field),
			 errhint("Stop the connector and start a new worker to use this configuration.")));
}

void
synchdb_run_spec_validate_restart(const SynchdbRunSpec *current,
								  const SynchdbRunSpec *next)
{
	if (current == NULL || next == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("restart configurations must not be null")));

	/* Both inputs are accepted specs, but do not trust unterminated arrays. */
	run_check_connection(&current->connection);
	run_check_connection(&next->connection);
	if (!synchdb_connector_key_equal(&current->connection.key, &next->connection.key))
		run_require_new_worker("the connector identity");
	if (next->connection.config_revision < current->connection.config_revision)
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("cannot apply an older connector configuration revision"),
				 errhint("Read the current connector configuration and retry.")));
	if (current->legacy_type != next->legacy_type ||
		current->source_dialect != next->source_dialect ||
		current->capture_backend != next->capture_backend)
		run_require_new_worker("the source dialect or capture backend");
	if (current->snapshot_provider != next->snapshot_provider ||
		current->connection.snapengine != next->connection.snapengine)
		run_require_new_worker("the snapshot provider");
	if (strcmp(current->connection.name, next->connection.name) != 0 ||
		strcmp(current->connection.dstdb, next->connection.dstdb) != 0)
		run_require_new_worker("the connector name or destination database");
	if (current->connection.isOraCompat != next->connection.isOraCompat)
		run_require_new_worker("the database compatibility mode");
	if (!run_jmx_equal(&current->connection.jmx, &next->connection.jmx))
		run_require_new_worker("JMX configuration");
	if (current->connection.run_options.jvm_max_heap_size !=
		next->connection.run_options.jvm_max_heap_size ||
		current->connection.run_options.jvm_max_direct_buffer_size !=
		next->connection.run_options.jvm_max_direct_buffer_size)
		run_require_new_worker("JVM memory settings");
	if (current->connection.run_options.synchdb_letter_casing_strategy !=
		next->connection.run_options.synchdb_letter_casing_strategy)
		run_require_new_worker("the identifier casing strategy");
}
