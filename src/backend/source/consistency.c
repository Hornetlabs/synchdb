#include "postgres.h"
#include "fmgr.h"

#include "lib/stringinfo.h"
#include "runtime/run_spec.h"
#include "source/consistency.h"
#include "utils/builtins.h"
#include "utils/json.h"

PG_FUNCTION_INFO_V1(synchdb_source_consistency_contract);

typedef struct SourceProtocol
{
	const char *provider;
	const char *native_protocol;
	const char *fdw_read_view;
	const char *prerequisites;
	const char *native_settings;
} SourceProtocol;

/* Prerequisites remain statements to be checked by the native connector and
 * deployment. They are not self-asserted evidence supplied by a SQL caller. */
static const SourceProtocol protocols[] =
{
	{"mysql", "DEBEZIUM_MYSQL_LOCKED_BINLOG_SNAPSHOT_V1", "UNCOORDINATED_FDW_READ",
	 "[\"unique replication server id\",\"ROW binlog with FULL row images\","
	 "\"snapshot SELECT and global/table-lock privileges\",\"transactional InnoDB captured tables\","
	 "\"no concurrent captured-table DDL during snapshot\",\"binlogs retained through snapshot and outage\"]",
	 "{\"snapshot.locking.mode\":\"minimal\"}"},
	{"oracle", "DEBEZIUM_ORACLE_LOGMINER_SNAPSHOT_V1", "ORACLE_AS_OF_SCN_DATA_ONLY",
	 "[\"ARCHIVELOG and required supplemental logging\",\"LogMiner and captured-table SELECT/FLASHBACK privileges\","
	 "\"undo covers the complete snapshot\",\"redo/archive logs cover oldest active transaction through recovery\","
	 "\"no concurrent captured-table DDL during snapshot\",\"complete engine-owned schema history and transaction-aware offset\"]",
	 "{\"snapshot.locking.mode\":\"shared\",\"internal.log.mining.transaction.snapshot.boundary.mode\":\"all\","
	 "\"log.mining.transaction.retention.ms\":\"0\",\"log.mining.buffer.transaction.events.threshold\":\"0\","
	 "\"log.mining.window.max.ms\":\"0\",\"snapshot.database.errors.max.retries\":\"0\"}"},
	{"sqlserver", "DEBEZIUM_SQLSERVER_SNAPSHOT_ONLY_CDC_HANDOFF_UNIMPLEMENTED_V1", "UNCOORDINATED_FDW_READ",
	 "[\"CDC enabled on database and captured tables\",\"SQL Server Agent/capture and cleanup jobs configured\","
	 "\"SELECT/CDC privileges and captured-table exclusive locks\",\"capture lag and CDC retention monitored\","
	 "\"captured-table writes quiesced and CDC capture fully drained before starting the snapshot\","
	 "\"CDC changes retained through snapshot and outage\",\"complete engine-owned schema history\"]",
	 "{\"snapshot.isolation.mode\":\"exclusive\",\"snapshot.locking.mode\":\"exclusive\"}"},
	{"postgres", "DEBEZIUM_POSTGRES_SLOT_SNAPSHOT_V1", "POSTGRES_FDW_REMOTE_TRANSACTION",
	 "[\"wal_level=logical and available replication slots/walsenders\",\"replication and captured-table SELECT privileges\","
	 "\"matching pgoutput publication and valid row identity\",\"exclusive ownership of replication slot and offset artifacts\","
	 "\"slot WAL retained through snapshot and outage\",\"slot/offset mismatch fails instead of advancing stored offset\"]",
	 "{\"snapshot.isolation.mode\":\"serializable\",\"offset.mismatch.strategy\":\"trust_offset\","
	 "\"lsn.flush.mode\":\"connector\",\"slot.drop.on.stop\":\"false\"}"}
};

static const SourceProtocol *
source_protocol(SynchdbSourceDialect dialect)
{
	switch (dialect)
	{
		case SYNCHDB_SOURCE_MYSQL: return &protocols[0];
		case SYNCHDB_SOURCE_ORACLE: return &protocols[1];
		case SYNCHDB_SOURCE_SQLSERVER: return &protocols[2];
		case SYNCHDB_SOURCE_POSTGRES: return &protocols[3];
		default:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("unsupported source consistency provider")));
	}
	pg_unreachable();
}

static bool
source_data_snapshot(const char *mode)
{
	return strcmp(mode, "initial") == 0 || strcmp(mode, "initial_only") == 0 ||
		strcmp(mode, "always") == 0;
}

void
synchdb_source_consistency_validate_run(const SynchdbRunSpec *spec)
{
	const char *mode;

	if (spec == NULL)
		elog(ERROR, "source consistency requires a run specification");
	(void) source_protocol(spec->source_dialect);
	mode = spec->snapshot_mode;
	if (spec->snapshot_provider == SYNCHDB_SNAPSHOT_FDW)
	{
		if (!(spec->connection.flag & CONNFLAG_SCHEMA_SYNC_MODE) &&
			strcmp(mode, "initial_only") != 0)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("FDW source reads do not establish a verified CDC handoff")));
		return;
	}
	if (spec->snapshot_provider != SYNCHDB_SNAPSHOT_DEBEZIUM)
		elog(ERROR, "source consistency requires a recognized snapshot provider");
	if (!source_data_snapshot(mode) && strcmp(mode, "no_data") != 0 &&
		!(strcmp(mode, "never") == 0 &&
		  (spec->source_dialect == SYNCHDB_SOURCE_POSTGRES ||
		   spec->source_dialect == SYNCHDB_SOURCE_MYSQL)))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unsupported native source consistency snapshot mode: %s", mode),
						errhint("Use initial, initial_only, always, or no_data; recovery and custom snapshot policies require a separate target recovery protocol.")));
	if (spec->connection.run_options.dbz_snapshot_thread_num != 1)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("native source consistency requires one snapshot thread"),
						errhint("Set synchdb.dbz_snapshot_thread_num to 1; a shared read view across additional JDBC connections has not been established.")));
	if (spec->source_dialect == SYNCHDB_SOURCE_SQLSERVER &&
		(strcmp(mode, "initial") == 0 || strcmp(mode, "always") == 0))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("SQL Server native snapshot-to-CDC has no implemented capture-drain fence"),
						errhint("Use initial_only for a snapshot without CDC, or no_data for CDC without establishing a baseline. Exclusive snapshot locks alone do not synchronize the asynchronous CDC capture job.")));
	if (spec->capture_backend == SYNCHDB_CAPTURE_OLR &&
		strcmp(mode, "initial_only") != 0 &&
		((strcmp(spec->connection.table, "null") != 0 && spec->connection.table[0] != '\0') ||
		 (strcmp(spec->connection.snapshottable, "null") != 0 && spec->connection.snapshottable[0] != '\0')))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("verified native OLR handoff currently requires the complete configured schema")));
}

Jsonb *
synchdb_source_consistency_describe(const SynchdbRunSpec *spec, MemoryContext context)
{
	const SourceProtocol *protocol;
	MemoryContext caller;
	StringInfoData json;
	Jsonb *result;
	bool native;
	bool schema_only;

	synchdb_source_consistency_validate_run(spec);
	if (context == NULL)
		elog(ERROR, "source consistency result requires an owning memory context");
	protocol = source_protocol(spec->source_dialect);
	native = spec->snapshot_provider == SYNCHDB_SNAPSHOT_DEBEZIUM;
	schema_only = (spec->connection.flag & CONNFLAG_SCHEMA_SYNC_MODE) != 0 ||
		strcmp(spec->snapshot_mode, "no_data") == 0;
	caller = MemoryContextSwitchTo(context);
	initStringInfo(&json);
	appendStringInfoString(&json, "{\"version\":1,\"provider\":");
	escape_json(&json, spec->capture_backend == SYNCHDB_CAPTURE_OLR ? "olr" : protocol->provider);
	appendStringInfoString(&json, ",\"engine_version\":\"3.5.2.Final\",\"snapshot_provider\":");
	escape_json(&json, native ? "debezium" : "fdw");
	appendStringInfoString(&json, ",\"mode\":");
	escape_json(&json, schema_only ? "no_data" : spec->snapshot_mode);
	appendStringInfoString(&json, ",\"protocol\":");
	escape_json(&json, !native ? "FDW_BOOTSTRAP_ONLY_V1" :
		(spec->capture_backend == SYNCHDB_CAPTURE_OLR ?
		 "ORACLE_OLR_SOURCE_FENCE_SNAPSHOT_V1" : protocol->native_protocol));
	appendStringInfoString(&json, ",\"read_view\":");
	escape_json(&json, schema_only ? "METADATA_ONLY" :
		(native ? "ENGINE_OWNED_READ_SESSION" : protocol->fdw_read_view));
	appendStringInfo(&json, ",\"required_native_settings\":%s",
		spec->capture_backend == SYNCHDB_CAPTURE_OLR ?
		"{\"snapshot.locking.mode\":\"shared\",\"internal.log.mining.transaction.snapshot.boundary.mode\":\"all\"}" : protocol->native_settings);
	if (spec->capture_backend == SYNCHDB_CAPTURE_OLR)
		appendStringInfoString(&json, ",\"required_olr_settings\":{\"format.message\":2,\"format.schema\":7,\"format.scn-type\":3,\"flags.include\":4096},\"position_commit\":\"TARGET_TRANSACTION\"");
	if (spec->source_dialect == SYNCHDB_SOURCE_SQLSERVER)
		appendStringInfoString(&json, ",\"automatic_capture_drain_fence\":false,\"unsupported_native_modes\":[\"initial\",\"always\"]");
	appendStringInfo(&json,
		",\"native_snapshot_to_cdc\":%s,\"external_handoff_supported\":false,"
		"\"cdc_eligible\":false,\"handoff_verified\":false,\"prerequisites_verified\":false,"
		"\"evidence_owner\":\"%s\",\"snapshot_threads\":%d,"
		"\"external_retry_after_read_session_end\":\"RESTART_BOOTSTRAP\",\"prerequisites\":%s}",
		native &&
		spec->source_dialect != SYNCHDB_SOURCE_SQLSERVER &&
		(strcmp(spec->snapshot_mode, "initial") == 0 || strcmp(spec->snapshot_mode, "always") == 0) ? "true" : "false",
		native ? "NATIVE_ENGINE" : "NONE", native ? 1 : 0, protocol->prerequisites);
	result = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(json.data)));
	pfree(json.data);
	MemoryContextSwitchTo(caller);
	return result;
}

Datum
synchdb_source_consistency_contract(PG_FUNCTION_ARGS)
{
	SynchdbRunSpec spec = {0};
	char *provider;
	char *engine;
	char *mode;
	Size length;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("source consistency provider, snapshot engine and mode are required")));
	provider = text_to_cstring(PG_GETARG_TEXT_PP(0));
	engine = text_to_cstring(PG_GETARG_TEXT_PP(1));
	mode = text_to_cstring(PG_GETARG_TEXT_PP(2));
	if (strcmp(provider, "mysql") == 0)
		spec.source_dialect = SYNCHDB_SOURCE_MYSQL;
	else if (strcmp(provider, "oracle") == 0 || strcmp(provider, "olr") == 0)
		spec.source_dialect = SYNCHDB_SOURCE_ORACLE;
	else if (strcmp(provider, "sqlserver") == 0)
		spec.source_dialect = SYNCHDB_SOURCE_SQLSERVER;
	else if (strcmp(provider, "postgres") == 0)
		spec.source_dialect = SYNCHDB_SOURCE_POSTGRES;
	else
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unsupported source consistency provider")));
	spec.capture_backend = strcmp(provider, "olr") == 0 ? SYNCHDB_CAPTURE_OLR : SYNCHDB_CAPTURE_DEBEZIUM;
	if (strcmp(engine, "fdw") == 0)
		spec.snapshot_provider = SYNCHDB_SNAPSHOT_FDW;
	else if (strcmp(engine, "debezium") == 0)
		spec.snapshot_provider = SYNCHDB_SNAPSHOT_DEBEZIUM;
	else
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unsupported source consistency snapshot engine")));
	length = strnlen(mode, sizeof(spec.snapshot_mode));
	if (length >= sizeof(spec.snapshot_mode))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("source consistency snapshot mode exceeds capacity")));
	strlcpy(spec.snapshot_mode, mode, sizeof(spec.snapshot_mode));
	if (strcmp(mode, "schemasync") == 0)
	{
		spec.connection.flag |= CONNFLAG_SCHEMA_SYNC_MODE;
		strlcpy(spec.snapshot_mode, "no_data", sizeof(spec.snapshot_mode));
	}
	spec.connection.run_options.dbz_snapshot_thread_num = 1;
	PG_RETURN_JSONB_P(synchdb_source_consistency_describe(&spec, CurrentMemoryContext));
}
