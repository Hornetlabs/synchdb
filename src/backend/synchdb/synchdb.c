/*
 * synchdb.c
 *
 * Implementation of SynchDB functionality for PostgreSQL
 *
 * This file contains the core functionality for the SynchDB extension,
 * including background worker management, JNI interactions with the
 * Debezium engine, and shared memory state management.
 *
 * Key components:
 * - JNI setup and interaction with Debezium
 * - Background worker management
 * - Shared memory state management
 * - User-facing functions for controlling SynchDB
 *
 * Copyright (c) 2024 Hornetlabs Technology, Inc.
 *
 */

#include "postgres.h"
#include "fmgr.h"
#include <ctype.h>
#include <jni.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <dlfcn.h>
#include "tcop/utility.h"
#include "tcop/tcopprot.h"
#include "nodes/parsenodes.h"
#include "utils/elog.h"

/* synchdb includes */
#include "converter/format_converter.h"
#include "converter/debezium_event_handler.h"
#include "synchdb/synchdb.h"
#include "executor/replication_agent.h"
#include "executor/pg_compat.h"
#include "catalog/connector_store.h"
#include "catalog/credentials.h"
#include "catalog/mapping_store.h"
#include "runtime/run_spec.h"
#include "runtime/run_registry.h"
#include "runtime/worker_runtime.h"
#include "runtime/java_operations.h"
#include "snapshot/bootstrap.h"
#include "snapshot/artifact_store.h"
#include "runtime/controller.h"
#include "runtime/config.h"

#ifndef SYNCHDB_LIBRARY_NAME
#define SYNCHDB_LIBRARY_NAME "synchdb"
#endif
#ifdef WITH_OLR
#include "olr/olr_client.h"
#include "olr/olr_handoff.h"
#endif

/* postgresql includes */
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/procsignal.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "storage/fd.h"
#include "miscadmin.h"
#include "utils/wait_event.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "varatt.h"
#include "funcapi.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "commands/dbcommands.h"

PG_MODULE_MAGIC;

/* Function declarations for user-facing functions */
PG_FUNCTION_INFO_V1(synchdb_stop_engine_bgw);
PG_FUNCTION_INFO_V1(synchdb_start_engine_bgw);
PG_FUNCTION_INFO_V1(synchdb_start_engine_bgw_snapshot_mode);
PG_FUNCTION_INFO_V1(synchdb_get_state);
PG_FUNCTION_INFO_V1(synchdb_pause_engine);
PG_FUNCTION_INFO_V1(synchdb_resume_engine);
PG_FUNCTION_INFO_V1(synchdb_set_offset);
PG_FUNCTION_INFO_V1(synchdb_add_conninfo);
PG_FUNCTION_INFO_V1(synchdb_restart_connector);
PG_FUNCTION_INFO_V1(synchdb_log_jvm_meminfo);
PG_FUNCTION_INFO_V1(synchdb_get_stats);
PG_FUNCTION_INFO_V1(synchdb_reset_stats);
PG_FUNCTION_INFO_V1(synchdb_add_objmap);
PG_FUNCTION_INFO_V1(synchdb_add_objmap_text);
PG_FUNCTION_INFO_V1(synchdb_reload_objmap);
PG_FUNCTION_INFO_V1(synchdb_add_extra_conninfo);
PG_FUNCTION_INFO_V1(synchdb_del_extra_conninfo);
PG_FUNCTION_INFO_V1(synchdb_add_fdw_conninfo);
PG_FUNCTION_INFO_V1(synchdb_del_fdw_conninfo);
PG_FUNCTION_INFO_V1(synchdb_del_conninfo);
PG_FUNCTION_INFO_V1(synchdb_del_objmap);
PG_FUNCTION_INFO_V1(synchdb_del_objmap_text);
PG_FUNCTION_INFO_V1(synchdb_add_jmx_conninfo);
PG_FUNCTION_INFO_V1(synchdb_del_jmx_conninfo);
PG_FUNCTION_INFO_V1(synchdb_add_jmx_exporter_conninfo);
PG_FUNCTION_INFO_V1(synchdb_del_jmx_exporter_conninfo);
PG_FUNCTION_INFO_V1(synchdb_add_olr_conninfo);
PG_FUNCTION_INFO_V1(synchdb_del_olr_conninfo);
PG_FUNCTION_INFO_V1(synchdb_add_infinispan);
PG_FUNCTION_INFO_V1(synchdb_del_infinispan);
PG_FUNCTION_INFO_V1(synchdb_translate_datatype);
PG_FUNCTION_INFO_V1(synchdb_set_snapstats);
PG_FUNCTION_INFO_V1(synchdb_set_dbz_loglevel);

/* Global variables */
static SynchdbSharedState *sdb_state = NULL; /* Pointer to shared-memory state. */
int myConnectorId = -1;	/* Global index number to SynchdbSharedState in shared memory - global per worker */
const char * g_eventStr = NULL;	/* global pointer to the JSON event currently working on */

/* GUC variables */
static int synchdb_worker_naptime = 10;
bool synchdb_dml_use_spi = false;
static bool synchdb_auto_launcher = true;
static int dbz_batch_size = 2048;
static int dbz_queue_size = 8192;
char * dbz_skipped_operations = "t";
static int dbz_connect_timeout_ms = 30000;
static int dbz_query_timeout_ms = 600000;
static int jvm_max_heap_size = 1024;
static int jvm_max_direct_buffer_size = 1024;
static int dbz_snapshot_thread_num = 1;
static int dbz_snapshot_fetch_size = 0; /* 0: auto */
static int dbz_snapshot_min_row_to_stream_results = 0; /* 0: always stream */
static int dbz_incremental_snapshot_chunk_size = 2048;
static char * dbz_incremental_snapshot_watermarking_strategy = "insert_insert";
int dbz_offset_flush_interval_ms = 60000;
static bool dbz_capture_only_selected_table_ddl = true;
int synchdb_max_connector_workers = 30;
int synchdb_error_strategy = STRAT_EXIT_ON_ERROR;
static int dbz_log_level = LOG_LEVEL_WARN;
bool synchdb_log_event_on_error = true;
int olr_read_buffer_size = 128;	/* in MB */
static int dbz_logminer_stream_mode = LOGMINER_MODE_UNCOMMITTED;
int olr_connect_timeout_ms = 5000;
int olr_read_timeout_ms = 5000;
static int synchdb_snapshot_engine = ENGINE_DEBEZIUM;
static int cdc_start_delay_ms = 0;
static bool synchdb_fdw_use_subtx = true;
int synchdb_letter_casing_strategy = LCS_NORMALIZE_LOWERCASE;

static const struct config_enum_entry letter_casing_strategies[] =
{
	{"asis", LCS_AS_IS, false},
	{"lowercase", LCS_NORMALIZE_LOWERCASE, false},
	{"uppercase", LCS_NORMALIZE_UPPERCASE, false},
	{NULL, 0, false}
};

static const struct config_enum_entry error_strategies[] =
{
	{"exit", STRAT_EXIT_ON_ERROR, false},
	{"skip", STRAT_SKIP_ON_ERROR, false},
	{"retry", STRAT_RETRY_ON_ERROR, false},
	{NULL, 0, false}
};

static const struct config_enum_entry dbz_log_levels[] =
{
	{"debug", LOG_LEVEL_DEBUG, false},
	{"info", LOG_LEVEL_INFO, false},
	{"warn", LOG_LEVEL_WARN, false},
	{"error", LOG_LEVEL_ERROR, false},
	{"all", LOG_LEVEL_ALL, false},
	{"fatal", LOG_LEVEL_FATAL, false},
	{"off", LOG_LEVEL_OFF, false},
	{"trace", LOG_LEVEL_TRACE, false},
	{NULL, 0, false}
};

static const struct config_enum_entry ora_logminer_stream_mode[] =
{
	{"uncommitted", LOGMINER_MODE_UNCOMMITTED, false},
	{"committed", LOGMINER_MODE_COMMITTED, false},
	{NULL, 0, false}
};

static const struct config_enum_entry snapshot_engines[] =
{
	{"debezium", ENGINE_DEBEZIUM, false},
	{"fdw", ENGINE_FDW, false},
	{NULL, 0, false}
};

/* Exit callbacks retain this process-owned context after ERROR unwinding. */
static SynchdbWorkerContext worker;

/* Function declarations */
PGDLLEXPORT void synchdb_engine_main(Datum main_arg);

StaticAssertDecl(sizeof(SynchdbWorkerStartup) <= BGW_EXTRALEN,
				 "connector startup identity must fit background worker extra data");

/* Static function prototypes */
static int dbz_engine_stop(void);
static int dbz_engine_get_change(int connectorId, bool *dbzExitSignal,
		BatchInfo * batchinfo, SynchdbStatistics * myBatchStats, int flag);
static int dbz_engine_start(const ConnectionInfo *connInfo, ConnectorType connectorType, const char * snapshotMode);
static char *dbz_engine_get_offset(int connectorId);
static int dbz_mark_batch_complete(int batchid);
static void process_debezium_batch(int connectorId, bool *exit_signal,
		SynchdbStatistics *statistics, int flag, bool refresh_offset);
static TupleDesc synchdb_state_tupdesc(void);
static TupleDesc synchdb_stats_tupdesc(void);
static bool shm_connector_id_is_valid(int connectorId);
static bool read_connector_view(int connectorId, SynchdbRunView *view);
static void synchdb_detach_shmem(int code, Datum arg);
static void capture_run_options(SynchdbRunOptions *options);
static void build_run_spec(SynchdbRunSpec *spec, ConnectionInfo *connection,
		ConnectorType type, const char *snapshotMode);
static void publish_worker_run(int connectorId, SynchdbWorkerRun *run,
		const SynchdbRunSpec *spec, const SynchdbRequest *consumed_request);
static void prepare_bgw(BackgroundWorker *worker, const SynchdbRunSpec *spec, const SynchdbRunHandle *handle);
static bool worker_should_stop(void);
static int32 start_registered_worker(const SynchdbRunSpec *spec);
static const char *connectorStateAsString(ConnectorState state);
static void reset_shm_request_state(int connectorId);
static int dbz_engine_set_offset(ConnectorType connectorType, char *db, char *offset, char *file);
static bool processRequestInterrupt(SynchdbWorkerRun *run, int connectorId);
static void worker_sigterm(SIGNAL_ARGS);
static void setup_environment(SynchdbWorkerRun *run);
static void initialize_jvm(const JMXConnectionInfo *jmx, const SynchdbRunOptions *options);
static void start_debezium_engine(ConnectorType connectorType, const ConnectionInfo *connInfo, const char * snapshotMode);
static void validate_schema_sync_request(ConnectorType type, int snapshotEngine, int flag);
static bool start_snapshot_run(ConnectorType type, ConnectionInfo *connInfo, const char *snapshotMode);
static bool main_loop(SynchdbWorkerRun *run);
static bool cleanup(ConnectorType connectorType);
static bool prepare_worker_close(SynchdbWorkerContext *context);
static bool close_worker_local(SynchdbWorkerContext *context);

static const SynchdbWorkerResourceOps worker_resources =
{
	.prepare_close = prepare_worker_close,
	.close_local = close_worker_local
};

static void set_shm_connector_statistics(int connectorId, SynchdbStatistics * stats);
#ifdef WITH_OLR
static void try_reconnect_olr(ConnectionInfo * connInfo);
#endif
static bool dbz_read_snapshot_state(ConnectorType type, const char * offset);
static bool offset_json_bool_value(const char *offset, const char *key, bool *found);
static bool has_running_connectors_for_db(const char *dbname, char *active_name, int active_len);
static int launch_fdw_based_snapshot(ConnectorType connectorType, ConnectionInfo *connInfo,
		char * snapshotMode, bool schemahistory);

/*
 * has_running_connectors_for_db
 *
 * Check if the current database still has running connectors before allowing
 * DROP EXTENSION to proceed, to prevent orphaned workers.
 */
static bool
has_running_connectors_for_db(const char *dbname, char *active_name, int active_len)
{
	if (active_name && active_len > 0)
		active_name[0] = '\0';
	return dbname != NULL && synchdb_registry_any_active(sdb_state, MyDatabaseId,
														active_name, Max(active_len, 0));
}

/*
 * dbz_engine_stop - Stop the Debezium engine
 *
 * This function stops the Debezium engine by calling the stopEngine method
 * on the DebeziumRunner object.
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_stop(void)
{
	return synchdb_java_stop(&worker.java);
}

/* The adapter supplies event semantics; Batch Apply owns the transaction. */
typedef struct SynchdbDbzApplyContext
{
	SynchdbStatistics *statistics;
	int connector_id;
	int flag;
	int32 batch_id;
} SynchdbDbzApplyContext;

static void
apply_debezium_record(const char *json, int32 index, bool first, bool last, void *argument)
{
	SynchdbDbzApplyContext *context = argument;
	SynchdbEventOutcome outcome;

	g_eventStr = synchdb_log_event_on_error ? json : NULL;
	outcome = fc_processDBZChangeEvent(json, context->statistics, context->flag,
										worker.run.spec.connection.name, first, last);
	switch (outcome)
	{
		case SYNCHDB_EVENT_APPLIED:
		case SYNCHDB_EVENT_CONTROL:
		case SYNCHDB_EVENT_NOOP:
			break;
		case SYNCHDB_EVENT_UNSUPPORTED:
		case SYNCHDB_EVENT_FAILED:
			set_shm_connector_errmsg(context->connector_id, "failed to apply Debezium change event");
			if (g_eventStr != NULL)
				elog(LOG, "%s", g_eventStr);
			ereport(ERROR,
					(errcode(outcome == SYNCHDB_EVENT_UNSUPPORTED ?
							 ERRCODE_FEATURE_NOT_SUPPORTED : ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to apply Debezium record %d in batch %d", index, context->batch_id)));
			break;
		default:
			/* SKIPPED is not enabled until rollback and policy are explicit. */
			elog(ERROR, "unexpected Debezium event outcome: %d", (int) outcome);
	}
	g_eventStr = NULL;
}

static void
apply_debezium_frame(const SynchdbDbzFrame *frame, SynchdbDbzApplyContext *context)
{
	bool previous = synchdb_worker_sql_interruptible();

	PG_TRY();
	{
		synchdb_worker_sql_enter();
		synchdb_batch_apply(frame, &worker.apply, apply_debezium_record, context);
	}
	PG_FINALLY();
	{
		synchdb_worker_sql_restore(previous);
	}
	PG_END_TRY();
}

static void
load_worker_rules(const char *name, ConnectorType type)
{
	bool previous = synchdb_worker_sql_interruptible();

	PG_TRY();
	{
		synchdb_worker_sql_enter();
		fc_load_objmap(name, type);
	}
	PG_FINALLY();
	{
		synchdb_worker_sql_restore(previous);
	}
	PG_END_TRY();
}

/* A view never outlives its JNI lease, including PostgreSQL ERROR paths. */
static int
dbz_engine_get_change(int connectorId, bool *dbzExitSignal,
		BatchInfo *batchinfo, SynchdbStatistics *myBatchStats, int flag)
{
	SynchdbDbzFrame frame;
	SynchdbDbzApplyContext context = {myBatchStats, connectorId, flag, -1};
	volatile bool finished = false;

	if (!dbzExitSignal || !batchinfo || !myBatchStats)
		elog(ERROR, "invalid Debezium batch output arguments");
	batchinfo->batchId = SYNCHDB_INVALID_BATCH_ID;
	batchinfo->batchSize = 0;

	PG_TRY();
	{
		if (synchdb_java_poll_frame(&worker.java, &frame))
		{
			if (frame.kind == SYNCHDB_DBZ_BATCH)
			{
				MemSet(&worker.apply, 0, sizeof(worker.apply));
				context.batch_id = frame.batch_id;
				apply_debezium_frame(&frame, &context);
				synchdb_java_batch_committed(&worker.java, frame.batch_id);
				increment_connector_statistics(myBatchStats, STATS_TOTAL_CHANGE_EVENT, frame.record_count);
				batchinfo->batchId = frame.batch_id;
				batchinfo->batchSize = frame.record_count;
			}
			else
			{
				char *message = pnstrdup(frame.message, frame.message_length);

				if (!frame.success)
				{
					set_shm_connector_errmsg(connectorId, message);
					ereport(ERROR, (errmsg("Debezium engine failed: %s", message)));
				}
				elog(LOG, "Debezium engine completed: %s", message);
				*dbzExitSignal = true;
				pfree(message);
			}
		}
		finished = true;
	}
	PG_FINALLY();
	{
		g_eventStr = NULL;
		if (!finished)
		{
			batchinfo->batchId = SYNCHDB_INVALID_BATCH_ID;
			batchinfo->batchSize = 0;
			synchdb_java_batch_failed(&worker.java,
					worker.apply.phase == SYNCHDB_APPLY_COMMIT_UNKNOWN);
		}
		synchdb_java_release_frame(&worker.java);
	}
	PG_END_TRY();
	return 0;
}

/*
 * dbz_engine_start - Start the Debezium engine
 *
 * This function starts the Debezium engine with the provided connection information.
 *
 * @param connInfo: Pointer to the ConnectionInfo structure containing connection details
 * @param connectorType: The type of connector to start
 * @param snapshotMode: Snapshot mode to start the connector with
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_start(const ConnectionInfo *connInfo, ConnectorType connectorType, const char *snapshotMode)
{
	int result;

	elog(LOG, "Starting Debezium engine %s:%d", connInfo->hostname, connInfo->port);
	result = synchdb_java_start(&worker.java, connInfo, connectorType, snapshotMode);
	if (result == 0)
		elog(LOG, "Debezium engine started successfully for %s connector", connectorTypeToString(connectorType));
	return result;
}

/*
 * dbz_engine_get_offset - Get the current offset from the Debezium engine
 *
 * This function retrieves the current offset for a specific connector type
 * from the Debezium engine.
 *
 * @param connectorId: The connector ID of interest
 *
 * @return: The offset as a string (caller must free), or NULL on failure
 */
static char *
dbz_engine_get_offset(int connectorId)
{
	const SynchdbRunSpec *spec = &worker.run.spec;

	if (!shm_connector_id_is_valid(connectorId))
		return NULL;
	return synchdb_java_get_offset(&worker.java, spec->legacy_type,
								   spec->connection.srcdb, spec->connection.name,
								   spec->connection.dstdb);
}

/*
 * dbz_engine_memory_dump - Logs memory summary of JVM
 *
 * This function prints current heap and non-heap memory allocated
 * and used by the connector JVM.
 */
static int
dbz_engine_memory_dump(void)
{
	int result = synchdb_java_memory_dump(&worker.java);

	if (result != 0)
		elog(WARNING, "failed to dump Debezium Java memory status");
	return result;
}

/**
 * dbz_engine_set_loglevel - Set Debezium log level
 *
 * This function set Debezium runner's log level during run time
 */
static int
dbz_engine_set_loglevel(int level)
{
	return synchdb_java_set_loglevel(&worker.java, level);
}

/*
 * synchdb_state_tupdesc - Create a TupleDesc for SynchDB state information
 *
 * This function constructs a TupleDesc that describes the structure of
 * the tuple returned by SynchDB state queries. It defines the columns
 * that will be present in the result set.
 *
 * @return: A blessed TupleDesc, or NULL on failure
 */
static TupleDesc
synchdb_state_tupdesc(void)
{
	TupleDesc tupdesc;
	AttrNumber attrnum = 7;
	AttrNumber a = 0;

	tupdesc = CreateTemplateTupleDesc(attrnum);

	/* add more columns here per connector if needed */
	TupleDescInitEntry(tupdesc, ++a, "name", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "connector type", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "pid", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "stage", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "state", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "err", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "last_dbz_offset", TEXTOID, -1, 0);

	return BlessTupleDesc(tupdesc);
}

/*
 * synchdb_stats_tupdesc - Create a TupleDesc for SynchDB statistic information
 *
 * This function constructs a TupleDesc that describes the structure of
 * the tuple returned by SynchDB statistic queries. It defines the columns
 * that will be present in the result set.
 *
 * @return: A blessed TupleDesc, or NULL on failure
 */
static TupleDesc
synchdb_stats_tupdesc(void)
{
	TupleDesc tupdesc;
	AttrNumber attrnum = 20;
	AttrNumber a = 0;

	tupdesc = CreateTemplateTupleDesc(attrnum);

	/* add more columns here per connector if needed */
	TupleDescInitEntry(tupdesc, ++a, "name", TEXTOID, -1, 0);

	/* cdc stats */
	TupleDescInitEntry(tupdesc, ++a, "ddls", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "dmls", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "creates", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "updates", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "deletes", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "txs", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "truncates", INT8OID, -1, 0);

	/* general stats */
	TupleDescInitEntry(tupdesc, ++a, "bad_events", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "total_events", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "batches_done", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "average_batch_size", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "first_src_ts", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "first_pg_ts", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "last_src_ts", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "last_pg_ts", INT8OID, -1, 0);

	/* snapshot stats */
	TupleDescInitEntry(tupdesc, ++a, "tables", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "rows", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "snapshot_begin_ts", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "snapshot_end_ts", INT8OID, -1, 0);

	return BlessTupleDesc(tupdesc);
}

/*
 * synchdb_init_shmem - Initialize or attach to synchdb shared memory
 *
 * Allocate and initialize synchdb related shared memory, if not already
 * done, and set up backend-local pointer to that state.
 */
static void
synchdb_init_shmem(void)
{
	sdb_state = synchdb_registry_try_attach(synchdb_max_connector_workers);
}

/* This must also be safe when called while reporting a SQL backend error. */
static bool
shm_connector_id_is_valid(int connectorId)
{
	return synchdb_registry_valid_slot(sdb_state, connectorId);
}

/* Legacy report adapters pass a complete owner, never just the physical slot. */
static bool
read_connector_view(int connectorId, SynchdbRunView *view)
{
	const SynchdbWorkerOwner *owner = worker.owner.run.generation != 0 ? &worker.owner : NULL;

	return synchdb_registry_read_slot(sdb_state, connectorId, owner, view);
}

static bool
worker_should_stop(void)
{
	return ShutdownRequestPending ||
		synchdb_worker_gate(&worker) == SYNCHDB_GATE_STOP;
}

/* Cleanup owns private process resources; registry release checks the captured owner. */
static void
synchdb_detach_shmem(int code, Datum arg)
{
	SynchdbWorkerContext *context = (SynchdbWorkerContext *) DatumGetPointer(arg);

	synchdb_worker_finalize(context, code);
}

/* Capture caller settings once; worker retries keep this accepted snapshot. */
static void
capture_run_options(SynchdbRunOptions *options)
{
	MemSet(options, 0, sizeof(*options));
	options->version = SYNCHDB_RUN_OPTIONS_VERSION;
	options->dbz_batch_size = dbz_batch_size;
	options->dbz_queue_size = dbz_queue_size;
	options->dbz_connect_timeout_ms = dbz_connect_timeout_ms;
	options->dbz_query_timeout_ms = dbz_query_timeout_ms;
	if (!dbz_skipped_operations || strlen(dbz_skipped_operations) >= sizeof(options->dbz_skipped_operations))
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("run option \"dbz_skipped_operations\" exceeds its supported length")));
	strlcpy(options->dbz_skipped_operations, dbz_skipped_operations, sizeof(options->dbz_skipped_operations));
	options->dbz_snapshot_thread_num = dbz_snapshot_thread_num;
	options->dbz_snapshot_fetch_size = dbz_snapshot_fetch_size;
	options->dbz_snapshot_min_row_to_stream_results = dbz_snapshot_min_row_to_stream_results;
	options->dbz_incremental_snapshot_chunk_size = dbz_incremental_snapshot_chunk_size;
	if (!dbz_incremental_snapshot_watermarking_strategy || strlen(dbz_incremental_snapshot_watermarking_strategy) >= sizeof(options->dbz_incremental_snapshot_watermarking_strategy))
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("run option \"dbz_incremental_snapshot_watermarking_strategy\" exceeds its supported length")));
	strlcpy(options->dbz_incremental_snapshot_watermarking_strategy, dbz_incremental_snapshot_watermarking_strategy, sizeof(options->dbz_incremental_snapshot_watermarking_strategy));
	options->dbz_offset_flush_interval_ms = dbz_offset_flush_interval_ms;
	options->dbz_capture_only_selected_table_ddl = dbz_capture_only_selected_table_ddl;
	options->dbz_log_level = dbz_log_level;
	options->dbz_logminer_stream_mode = dbz_logminer_stream_mode;
	options->cdc_start_delay_ms = cdc_start_delay_ms;
	options->jvm_max_heap_size = jvm_max_heap_size;
	options->jvm_max_direct_buffer_size = jvm_max_direct_buffer_size;
	options->synchdb_fdw_use_subtx = synchdb_fdw_use_subtx;
	options->synchdb_letter_casing_strategy = synchdb_letter_casing_strategy;
}

static void
build_run_spec(SynchdbRunSpec *spec, ConnectionInfo *connection,
			   ConnectorType type, const char *snapshotMode)
{
	const char *compatible = GetConfigOption("ivorysql.compatible_mode", true, false);

	connection->snapengine = synchdb_snapshot_engine;
	connection->isOraCompat = compatible && pg_strcasecmp(compatible, "oracle") == 0;
	capture_run_options(&connection->run_options);
	synchdb_run_spec_init(spec, type, connection, snapshotMode);
}

/* Publish the accepted attempt before starting an engine that can fail. */
static void
publish_worker_run(int connectorId, SynchdbWorkerRun *run,
				   const SynchdbRunSpec *spec, const SynchdbRequest *consumed_request)
{
	Assert(run == &worker.run);
	if (connectorId != worker.owner.run.slot ||
		!synchdb_worker_publish(&worker, spec, consumed_request))
		proc_exit(0);
	synchdb_letter_casing_strategy = spec->connection.run_options.synchdb_letter_casing_strategy;
}

static void
prepare_bgw(BackgroundWorker *worker, const SynchdbRunSpec *spec, const SynchdbRunHandle *handle)
{
	const ConnectionInfo *connection = &spec->connection;
	const char *connector = connectorTypeToString(spec->legacy_type);
	SynchdbWorkerStartup startup = {0};

	startup.version = SYNCHDB_WORKER_STARTUP_VERSION;
	startup.run = *handle;
	startup.key = connection->key;
	startup.config_revision = connection->config_revision;
	memset(worker->bgw_extra, 0, sizeof(worker->bgw_extra));
	memcpy(worker->bgw_extra, &startup, sizeof(startup));
	worker->bgw_main_arg = UInt32GetDatum(handle->slot);
	snprintf(worker->bgw_name, BGW_MAXLEN, "synchdb engine: %s@%s:%u",
			 connector, connection->hostname, connection->port);
	snprintf(worker->bgw_type, BGW_MAXLEN, "synchdb engine: %s", connector);
	strlcat(worker->bgw_name, " -> ", BGW_MAXLEN);
	strlcat(worker->bgw_name, connection->dstdb, BGW_MAXLEN);
}

/*
 * connectorStateAsString - Convert ConnectorState to string representation
 *
 * This function sets up a BackgroundWorker structure with the appropriate
 * information based on the connector type and connection details.
 *
 * @param state: Connector state enum
 *
 * @return connector state in string
 */
static const char *
connectorStateAsString(ConnectorState state)
{
	switch (state)
	{
	case STATE_UNDEF:
	case STATE_STOPPED:
		return "stopped";
	case STATE_INITIALIZING:
		return "initializing";
	case STATE_PAUSED:
		return "paused";
	case STATE_SYNCING:
		return "polling";
	case STATE_PARSING:
		return "parsing";
	case STATE_CONVERTING:
		return "converting";
	case STATE_EXECUTING:
		return "executing";
	case STATE_OFFSET_UPDATE:
		return "updating offset";
	case STATE_RESTARTING:
		return "restarting";
	case STATE_MEMDUMP:
		return "dumping memory";
	case STATE_SCHEMA_SYNC_DONE:
		return "schema sync";
	case STATE_RELOAD_OBJMAP:
		return "reloading objmap";
	case STATE_DBZ_LOGLEVEL_UPDATE:
		return "updating dbz log level";
	}
	return "UNKNOWN";
}

/*
 * reset_shm_request_state - Reset the shared memory request state for a connector
 *
 * This function resets the request state and clears the request data for a
 * specific connector type in shared memory.
 *
 * @param connectorId: The connector ID of interest
 */
static void
reset_shm_request_state(int connectorId)
{
	if (connectorId == worker.owner.run.slot)
		synchdb_worker_ack(&worker);
}

/*
 * dbz_engine_set_offset - Set the offset for the Debezium engine
 *
 * This function interacts with the Java VM to set the offset for a specific
 * Debezium connector.
 *
 * @param connectorType: The type of connector
 * @param db: The database name
 * @param offset: The offset to set
 * @param file: The file to store the offset
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_set_offset(ConnectorType connectorType, char *db, char *offset, char *file)
{
	int result = synchdb_java_set_offset(&worker.java, connectorType, db, offset, file);

	if (result == 0)
		elog(LOG, "Successfully set offset for %s connector", connectorTypeToString(connectorType));
	return result;
}

/*
 * processRequestInterrupt - Handles state transition requests for SynchDB connectors
 *
 * This function processes requests to change the state of a SynchDB connector,
 * such as pausing, resuming, or updating offsets.
 *
 * @param connInfo: Pointer to the connection information
 * @param type: The type of connector being processed
 * @param connectorId: Connector ID of interest
 * @param snapshotMode: The new snapshot mode requested
 */
static bool
processRequestInterrupt(SynchdbWorkerRun *run, int connectorId)
{
	ConnectionInfo *connInfo = &run->execution;
	ConnectorType type = run->spec.legacy_type;
	char *snapshotMode = run->snapshot_mode;
	SynchdbRequest *reqcopy;
	ConnectorState *currstatecopy;
	char offsetfile[MAX_PATH_LENGTH] = {0};
	char *srcdb;
	int ret;
	volatile bool completed = false;
	bool request_consumed = false;

	if (!shm_connector_id_is_valid(connectorId))
		return false;

	srcdb = connInfo->srcdb;


	/*
	 * make a copy of requested state, its data and curr state to avoid holding locks
	 * for too long. Currently we support 1 request at any one time.
	 */
	reqcopy = palloc0(sizeof(SynchdbRequest));
	currstatecopy = palloc0(sizeof(ConnectorState));

	if (connectorId != worker.owner.run.slot ||
		!synchdb_worker_poll(&worker, reqcopy, currstatecopy))
	{
		pfree(reqcopy);
		pfree(currstatecopy);
		return false;
	}

	/* Process the request based on current and requested states */
	if (reqcopy->reqstate == STATE_PAUSED && *currstatecopy == STATE_SYNCING)
	{
		/* we can only transition to STATE_PAUSED from STATE_SYNCING */
		elog(LOG, "Pausing %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));
#ifdef WITH_OLR
		if (type == TYPE_OLR)
		{
			olr_client_shutdown();
			set_shm_connector_state(connectorId, STATE_PAUSED);
		}
		else
#endif
		{
			elog(DEBUG1, "shut down dbz engine...");
			ret = dbz_engine_stop();
			if (ret)
			{
				synchdb_registry_complete_request(sdb_state, &worker.owner, reqcopy->command_id, false,
					ERRCODE_EXTERNAL_ROUTINE_EXCEPTION, "failed to confirm Debezium runner shutdown while pausing");
				worker.command_id = 0;
				pfree(reqcopy);
				pfree(currstatecopy);
				ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("failed to confirm Debezium runner shutdown while pausing")));
			}
			set_shm_connector_state(connectorId, STATE_PAUSED);

			/*
			 * todo: if a connector is marked paused, it should be noted somewhere in
			 * synchdb_conninfo table so when it restarts next time, it could start with
			 * initial state = paused rather than syncing.
			 */
		}
	}
	else if (reqcopy->reqstate == STATE_SYNCING && *currstatecopy == STATE_PAUSED)
	{
		synchdb_registry_report_ready(sdb_state, &worker.owner, false);
		if (worker.bootstrap != NULL)
		{
			set_shm_connector_errmsg(connectorId,
				"FDW bootstrap has no verified CDC handoff. Inspect its result and explicitly restart bootstrap or use a supported capture protocol.");
            synchdb_registry_complete_request(sdb_state,&worker.owner,reqcopy->command_id,false,
                ERRCODE_FEATURE_NOT_SUPPORTED,"FDW bootstrap has no verified CDC handoff");
            worker.command_id=0;
            pfree(reqcopy);
            pfree(currstatecopy);
            return false;
        }
        /* Handle resume request, we can only transition to STATE_SYNCING from STATE_PAUSED */
		elog(LOG, "Resuming %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));
#ifdef WITH_OLR
		if (type == TYPE_OLR)
		{
			/*
			 * if we are still in snapshot or schema sync stage, we shall not
			 * reconnect olr yet
			 */
			if ((connInfo->flag & (CONNFLAG_SCHEMA_SYNC_MODE |
					CONNFLAG_INITIAL_SNAPSHOT_MODE)) == 0)
			{
				try_reconnect_olr(connInfo);
			}
			set_shm_connector_state(connectorId, STATE_SYNCING);
		}
		else
#endif
		{
			/* restart dbz engine */
			elog(DEBUG1, "restart dbz engine...");

			/*
			 * we are using debezium based snapshot engine, we should always start
			 * debezium to allow it to resume or retry whatever it needs to do
			 */
			if (connInfo->snapengine == ENGINE_DEBEZIUM)
			{
				ret = dbz_engine_start(connInfo, type, snapshotMode);
				if (ret < 0)
				{
					elog(WARNING, "Failed to restart dbz engine");
					synchdb_registry_complete_request(sdb_state, &worker.owner, reqcopy->command_id, false,
						ERRCODE_EXTERNAL_ROUTINE_EXCEPTION, "could not resume Debezium runner");
					worker.command_id = 0;
					pfree(reqcopy);
					pfree(currstatecopy);
					if (worker.java.engine_start_attempted && !worker.java.stop_confirmed)
						ereport(ERROR,
							(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
							 errmsg("Debezium start failed after engine creation may have begun; closing worker resources")));
					return false;
				}
			}
			else if (connInfo->snapengine == ENGINE_FDW)
			{
				/*
				 * for fdw based snapshot, we can only start debezium to resume CDC
				 * once we have successfully done with the fdw based initial snapshot
				 */
				if ((connInfo->flag & (CONNFLAG_SCHEMA_SYNC_MODE |
						CONNFLAG_INITIAL_SNAPSHOT_MODE)) == 0 )
				{
					const char *resumeMode = !strcasecmp(snapshotMode, "recovery") ?
						"initial" : snapshotMode;

					if (connInfo->flag & CONNFLAG_NO_CDC_MODE)
						elog(ERROR, "cannot resume CDC for an initial_only connector");

					/*
					 * A SQL Server FDW schema-sync handoff has an incomplete
					 * snapshot marker until Debezium has rebuilt schema history.
					 */
					if (type == TYPE_SQLSERVER &&
							!dbz_read_snapshot_state(type, get_shm_dbz_offset(connectorId)))
						resumeMode = "recovery";

					ret = dbz_engine_start(connInfo, type, resumeMode);
					if (ret < 0)
					{
						elog(WARNING, "Failed to restart dbz engine");
						synchdb_registry_complete_request(sdb_state, &worker.owner, reqcopy->command_id, false,
							ERRCODE_EXTERNAL_ROUTINE_EXCEPTION, "could not resume Debezium runner");
						worker.command_id = 0;
						pfree(reqcopy);
						pfree(currstatecopy);
						if (worker.java.engine_start_attempted && !worker.java.stop_confirmed)
							ereport(ERROR,
								(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
								 errmsg("Debezium start failed after engine creation may have begun; closing worker resources")));
						return false;
					}
				}
			}

			set_shm_connector_state(connectorId, STATE_SYNCING);
		}
	}
	else if (reqcopy->reqstate == STATE_OFFSET_UPDATE && *currstatecopy == STATE_PAUSED)
	{
		/* Handle offset update request */
		elog(LOG, "Updating offset for %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));
#ifdef WITH_OLR
        if (type == TYPE_OLR)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("verified OLR positions cannot be manually overwritten")));
		else
#endif
		{
				SynchdbArtifactPaths paths;
				bool previous = synchdb_worker_sql_interruptible();

				PG_TRY();
				{
					synchdb_worker_sql_enter();
					StartTransactionCommand();
					PushActiveSnapshot(GetTransactionSnapshot());
					if (!synchdb_artifact_store_lookup(&connInfo->key, &paths))
						elog(ERROR, "connector has no active artifact generation");
					strlcpy(offsetfile, paths.offset, sizeof(offsetfile));
					PopActiveSnapshot();
					CommitTransactionCommand();
				}
				PG_FINALLY();
				{
					synchdb_worker_sql_restore(previous);
				}
				PG_END_TRY();

			set_shm_connector_state(connectorId, STATE_OFFSET_UPDATE);
			ret = dbz_engine_set_offset(type, srcdb, reqcopy->reqdata, offsetfile);
			if (ret < 0)
			{
				elog(WARNING, "Failed to set offset for %s connector", connectorTypeToString(type));
				synchdb_registry_complete_request(sdb_state,&worker.owner,reqcopy->command_id,false,
					ERRCODE_EXTERNAL_ROUTINE_EXCEPTION,"could not update connector offset");
				worker.command_id=0;
				set_shm_connector_state(connectorId, STATE_PAUSED);
				pfree(reqcopy);
				pfree(currstatecopy);
				return false;
			}

			/* after new offset is set, change state back to STATE_PAUSED */
			set_shm_connector_state(connectorId, STATE_PAUSED);

			/* and also update this worker's shm offset */
			set_shm_dbz_offset(connectorId);
		}
	}
	else if (reqcopy->reqstate == STATE_RESTARTING &&
			(*currstatecopy == STATE_SYNCING || *currstatecopy == STATE_PAUSED))
	{
		SynchdbRunSpec next;

		/* Recheck the complete request before stopping the current engine. */
		synchdb_run_spec_init(&next, reqcopy->reqtype, &reqcopy->reqconninfo, reqcopy->reqdata);
		synchdb_run_spec_validate_restart(&run->spec, &next);
#ifdef WITH_OLR
		if (type == TYPE_OLR &&
			(pg_strcasecmp(next.snapshot_mode, "initial") != 0 ||
			 (connInfo->flag & (CONNFLAG_SCHEMA_SYNC_MODE |
				CONNFLAG_INITIAL_SNAPSHOT_MODE | CONNFLAG_NO_CDC_MODE))))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("OLR snapshot requests require a new worker start")));
#endif
		set_shm_connector_state(connectorId, STATE_RESTARTING);
#ifdef WITH_OLR
		if (type == TYPE_OLR)
		{
			olr_client_shutdown();
			publish_worker_run(connectorId, run, &next, reqcopy);
			request_consumed = true;
			sleep(5);
			try_reconnect_olr(connInfo);
			set_shm_connector_state(connectorId, STATE_SYNCING);
		}
		else
#endif
		{
			ret = dbz_engine_stop();
			if (ret)
			{
				synchdb_registry_complete_request(sdb_state, &worker.owner, reqcopy->command_id, false,
					ERRCODE_EXTERNAL_ROUTINE_EXCEPTION, "failed to confirm Debezium runner shutdown while restarting");
				worker.command_id = 0;
				pfree(reqcopy);
				pfree(currstatecopy);
				ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("failed to confirm Debezium runner shutdown while restarting")));
			}
			publish_worker_run(connectorId, run, &next, reqcopy);
			request_consumed = true;
			sleep(1);
			completed = !start_snapshot_run(type, connInfo, snapshotMode);
		}
	}
		else if (reqcopy->reqstate == STATE_MEMDUMP)
		{
			ConnectorState oldstate = get_shm_connector_state_enum(connectorId);

		elog(LOG, "Requesting memdump for %s connector", connInfo->name);
		set_shm_connector_state(connectorId, STATE_MEMDUMP);
		ret = dbz_engine_memory_dump();
		set_shm_connector_state(connectorId, oldstate);
		if (ret != 0)
		{
			synchdb_registry_complete_request(sdb_state, &worker.owner, reqcopy->command_id, false,
				ERRCODE_EXTERNAL_ROUTINE_EXCEPTION, "could not dump Debezium Java memory status");
			worker.command_id = 0;
		}
	}
		else if (reqcopy->reqstate == STATE_RELOAD_OBJMAP)
		{
			ConnectorState oldstate = get_shm_connector_state_enum(connectorId);

		elog(LOG, "Reloading objmap for %s connector", connInfo->name);
		set_shm_connector_state(connectorId, STATE_RELOAD_OBJMAP);
		/* A rejected candidate does not stop the engine using the old snapshot. */
		PG_TRY();
		{
				load_worker_rules(connInfo->name, type);
		}
		PG_CATCH();
		{
			ErrorData *error;

			MemoryContextSwitchTo(TopMemoryContext);
			error = CopyErrorData();
			FlushErrorState();
			set_shm_connector_errmsg(connectorId, error->message);
			ereport(WARNING, (errmsg("mapping reload rejected; keeping the previous rules: %s",
								   error->message)));
            synchdb_registry_complete_request(sdb_state,&worker.owner,reqcopy->command_id,false,
                error->sqlerrcode,error->message);
            worker.command_id=0;
            FreeErrorData(error);
        }
        PG_END_TRY();
        set_shm_connector_state(connectorId, oldstate);
	}
	else if (reqcopy->reqstate == STATE_DBZ_LOGLEVEL_UPDATE)
	{
		ConnectorState oldstate = get_shm_connector_state_enum(connectorId);
		int level = pg_strtoint32(reqcopy->reqdata);

		elog(LOG, "Updating log level for %s connector to %d", connInfo->name, level);
		set_shm_connector_state(connectorId, STATE_DBZ_LOGLEVEL_UPDATE);
		if (level < LOG_LEVEL_ALL || level > LOG_LEVEL_TRACE || dbz_engine_set_loglevel(level) != 0)
		{
			synchdb_registry_complete_request(sdb_state, &worker.owner, reqcopy->command_id, false,
				level < LOG_LEVEL_ALL || level > LOG_LEVEL_TRACE ?
				ERRCODE_INVALID_PARAMETER_VALUE : ERRCODE_EXTERNAL_ROUTINE_EXCEPTION,
				"could not update Debezium log level");
			worker.command_id = 0;
			set_shm_connector_state(connectorId, oldstate);
			pfree(reqcopy);
			pfree(currstatecopy);
			return false;
		}
		run->spec.connection.run_options.dbz_log_level = level;
		run->execution.run_options.dbz_log_level = level;
		synchdb_registry_publish_loglevel(sdb_state, &worker.owner, level);
		set_shm_connector_state(connectorId, oldstate);
	}
	else
	{
		/* unsupported request state combinations */
		elog(WARNING, "Invalid state transition requested for %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));
		synchdb_registry_complete_request(sdb_state, &worker.owner, reqcopy->command_id, false,
			ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, "connector state does not permit the requested operation");
		worker.command_id = 0;
	}

	/* reset request state so we can receive more requests to process */
	if (!request_consumed)
		reset_shm_request_state(connectorId);
	pfree(reqcopy);
	pfree(currstatecopy);
	return completed;
}

/*
 * setup_environment - Prepares the environment for the SynchDB background worker
 *
 * This function sets up signal handlers, initializes the database connection,
 * sets up shared memory, and checks for existing worker processes.
 *
 * @param connectorType: The type of connector being set up
 * @param dst_db: The name of the destination database to connect to
 * @param snapshotMode: The snapshot mode requested
 */
static void
worker_sigterm(SIGNAL_ARGS)
{
	SignalHandlerForShutdownRequest(postgres_signal_arg);
	if (synchdb_worker_sql_interruptible())
		die(postgres_signal_arg);
}

static void
setup_environment(SynchdbWorkerRun *run)
{
	SynchdbWorkerStartup startup;
	SynchdbRunGate gate;

	Assert(run == &worker.run);

	/* Stale registrations terminate normally, including those configured to retry. */
	if (!MyBgworkerEntry)
		proc_exit(0);
	memcpy(&startup, MyBgworkerEntry->bgw_extra, sizeof(startup));
	if (startup.version != SYNCHDB_WORKER_STARTUP_VERSION ||
		!synchdb_connector_key_is_valid(&startup.key) || startup.config_revision <= 0 ||
		startup.run.slot != myConnectorId || !OidIsValid(startup.execution_role) ||
		!synchdb_registry_controller_valid(synchdb_registry_try_attach(synchdb_max_connector_workers),
			startup.key.database_oid,startup.controller_generation))
		proc_exit(0);

	pqsignal(SIGTERM, worker_sigterm);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();
	synchdb_init_shmem();
	synchdb_worker_init(&worker, sdb_state, &worker_resources);
	/* Manual runtimes use DSM, which core unmaps before on_shmem_exit. */
	before_shmem_exit(synchdb_detach_shmem, PointerGetDatum(&worker));
	if (!synchdb_worker_claim(&worker, &startup, myConnectorId))
		proc_exit(0);
	/* A registration can fork before its SQL transaction commits, but cannot act. */
	for (;;)
	{
		ResetLatch(MyLatch);
		gate = synchdb_worker_gate(&worker);
		if (ShutdownRequestPending || gate == SYNCHDB_GATE_STOP)
			proc_exit(0);
		if (gate == SYNCHDB_GATE_READY)
			break;
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 100L, PG_WAIT_EXTENSION);
		CHECK_FOR_INTERRUPTS();
	}
	/* Startup SQL precedes native engine creation. A stop can safely abort a
	 * connection/rule-loading lock wait without entering JNI or OLR cleanup. */
	synchdb_worker_sql_enter();
	synchdb_pg_worker_initialize_connection(run->execution.key.database_oid, startup.execution_role);
	synchdb_letter_casing_strategy = run->spec.connection.run_options.synchdb_letter_casing_strategy;
	if (run->execution.isOraCompat)
	{
		SetConfigOption("ivorysql.compatible_mode", "oracle", PGC_USERSET, PGC_S_OVERRIDE);
		SetConfigOption("ivorysql.identifier_case_switch", "normal", PGC_USERSET, PGC_S_OVERRIDE);
	}
	elog(LOG, "loaded accepted run configuration for connector %s (type %s, mode %s)",
		 run->spec.connection.name, connectorTypeToString(run->spec.legacy_type), run->spec.snapshot_mode);
}

/*
 * initialize_jvm - Initialize the Java Virtual Machine and Debezium engine
 *
 * This function sets up the Java environment, locates the Debezium engine JAR file,
 * creates a Java VM, and initializes the Debezium engine.
 */
static void
initialize_jvm(const JMXConnectionInfo *jmx, const SynchdbRunOptions *run_options)
{
	JavaVMInitArgs vm_args;
	JavaVMOption options[30];	/* ensure we do not exceed this max number of java options */
	char jar_path[MAX_PATH_LENGTH] = {0};
	const char *dbzpath = getenv("DBZ_ENGINE_DIR");
	MemoryContext jvmContext, oldContext;
	int ret, optcount = 0, i = 0;

	if (worker.java.creation_attempted)
		elog(ERROR, "a JVM has already been created or attempted in this worker");

	/* Determine the path to the Debezium engine JAR file */
	if (dbzpath)
		snprintf(jar_path, sizeof(jar_path), "%s/%s", dbzpath, DBZ_ENGINE_JAR_FILE);
	else
		snprintf(jar_path, sizeof(jar_path), "%s/dbz_engine/%s", pkglib_path, DBZ_ENGINE_JAR_FILE);

	/* Check if the JAR file exists */
	if (access(jar_path, F_OK) == -1)
	{
		set_shm_connector_errmsg(myConnectorId, "Cannot find DBZ engine jar file");
		elog(ERROR, "Cannot find DBZ engine jar file at %s", jar_path);
	}

	jvmContext = AllocSetContextCreate(TopMemoryContext, "JVMINIT", ALLOCSET_DEFAULT_SIZES);
	oldContext = MemoryContextSwitchTo(jvmContext);

	PG_TRY();
	{
		/* Configure JVM options */
		options[optcount++].optionString = psprintf("-Djava.class.path=%s", jar_path);
		options[optcount++].optionString = "-Xrs"; // Reduce use of OS signals by JVM
		options[optcount++].optionString = psprintf("-Xmx%dm", run_options->jvm_max_heap_size);
		options[optcount++].optionString = psprintf("-XX:MaxDirectMemorySize=%dm", run_options->jvm_max_direct_buffer_size);

		/* jmx parameters if enabled */
		if (jmx && strcasecmp(jmx->jmx_listenaddr, "null") &&
				strcasecmp(jmx->jmx_rmiserveraddr, "null"))
		{
			options[optcount++].optionString = "-Dcom.sun.management.jmxremote";
			options[optcount++].optionString = psprintf("-Dcom.sun.management.jmxremote.host=%s", jmx->jmx_listenaddr);
			options[optcount++].optionString = psprintf("-Dcom.sun.management.jmxremote.port=%u", jmx->jmx_port);
			options[optcount++].optionString = psprintf("-Djava.rmi.server.hostname=%s", jmx->jmx_rmiserveraddr);
			options[optcount++].optionString = psprintf("-Dcom.sun.management.jmxremote.rmi.port=%u", jmx->jmx_rmiport);

			/* optional jmx auth options */
			if (jmx->jmx_auth)
			{
				options[optcount++].optionString = "-Dcom.sun.management.jmxremote.authenticate=true";
				options[optcount++].optionString = psprintf("-Dcom.sun.management.jmxremote.password.file=%s",
						jmx->jmx_auth_passwdfile);
				options[optcount++].optionString = psprintf("-Dcom.sun.management.jmxremote.access.file=%s",
						jmx->jmx_auth_accessfile);
			}
			else
				options[optcount++].optionString = "-Dcom.sun.management.jmxremote.authenticate=false";

			/* optional jmx ssl options */
			if (jmx->jmx_ssl)
			{
				options[optcount++].optionString = "-Dcom.sun.management.jmxremote.ssl=true";
				options[optcount++].optionString = psprintf("-Djavax.net.ssl.keyStore=%s", jmx->jmx_ssl_keystore);
				options[optcount++].optionString = psprintf("-Djavax.net.ssl.keyStorePassword=%s", jmx->jmx_ssl_keystore_pass);

				/* optional truststore options for authenticating clients */
				if (strcasecmp(jmx->jmx_ssl_truststore, "null"))
				{
					options[optcount++].optionString = psprintf("-Djavax.net.ssl.trustStore=%s", jmx->jmx_ssl_truststore);
					options[optcount++].optionString = psprintf("-Djavax.net.ssl.trustStorePassword=%s", jmx->jmx_ssl_truststore_pass);
				}
			}
			else
				options[optcount++].optionString = "-Dcom.sun.management.jmxremote.ssl=false";
		}

		/* jmx exporter settings - for prometheus and graphana */
		if (jmx && strcasecmp(jmx->jmx_exporter_conf, "null") && strcasecmp(jmx->jmx_exporter, "null"))
		{
			options[optcount++].optionString = psprintf("-javaagent:%s=%u:%s",jmx->jmx_exporter,
					jmx->jmx_exporter_port, jmx->jmx_exporter_conf);
		}

		elog(LOG, "Initialize JVM with %d options:", optcount);
		for (i = 0; i < optcount; i++)
		{
			if (strstr(options[i].optionString, "Password=") != NULL)
				elog(LOG, "options[%d]=<redacted password option>", i);
			else
				elog(LOG, "options[%d]=%s", i, options[i].optionString);
		}

		vm_args.version = JNI_VERSION_10;
		vm_args.nOptions = optcount;
		vm_args.options = options;
		vm_args.ignoreUnrecognized = JNI_FALSE;

		/* Create the Java VM */
		if (worker_should_stop())
			proc_exit(0);
		worker.java.creation_attempted = true;
		ret = JNI_CreateJavaVM(&worker.java.vm, (void **)&worker.java.env, &vm_args);
		worker.java.creation_succeeded = ret == JNI_OK && worker.java.vm != NULL;
		if (!worker.java.creation_succeeded || !worker.java.env)
		{
			set_shm_connector_errmsg(myConnectorId, "Unable to Launch JVM");
			elog(ERROR, "Failed to create Java VM (return code: %d)", ret);
		}

		elog(INFO, "Java VM created successfully");

		/* Initialize the Debezium engine */
		ret = synchdb_java_bind(&worker.java);
		if (ret < 0)
		{
			set_shm_connector_errmsg(myConnectorId, "Failed to initialize Debezium engine");
			elog(ERROR, "Failed to initialize Debezium engine");
		}

		elog(INFO, "Debezium engine initialized successfully");
	}
	PG_FINALLY();
	{
		MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(jvmContext);
	}
	PG_END_TRY();
}

/*
 * start_debezium_engine - Starts the Debezium engine for a given connector
 *
 * This function initiates the Debezium engine using the provided connection
 * information and sets the connector state to SYNCING upon successful start.
 *
 * @param connectorType: The type of connector being started
 * @param connInfo: Pointer to the ConnectionInfo structure containing connection details
 * @param snapshotMode: Snapshot mode requested
 */
static void
start_debezium_engine(ConnectorType connectorType, const ConnectionInfo *connInfo, const char * snapshotMode)
{
	int ret;

	if (worker_should_stop())
		proc_exit(0);
	ret = dbz_engine_start(connInfo, connectorType, snapshotMode);
	if (ret < 0)
	{
		set_shm_connector_errmsg(myConnectorId, "Failed to start dbz engine");
		elog(ERROR, "Failed to start Debezium engine for connector type %d", connectorType);
	}

	set_shm_connector_state(myConnectorId, STATE_SYNCING);

	elog(LOG, "Debezium engine started successfully for %s:%d (connector type %d)",
		 connInfo->hostname, connInfo->port, connectorType);
}

static void
validate_schema_sync_request(ConnectorType type, int snapshotEngine, int flag)
{
	if (type == TYPE_POSTGRES && snapshotEngine == ENGINE_DEBEZIUM &&
		(flag & CONNFLAG_SCHEMA_SYNC_MODE))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("PostgreSQL Debezium connectors do not support automatic schema sync"),
				 errhint("Use the FDW snapshot engine for schema sync.")));
}

/* Select this run's work without turning a snapshot-only request into CDC. */
static bool
start_snapshot_run(ConnectorType type, ConnectionInfo *connInfo, const char *snapshotMode)
{
	if (worker_should_stop())
		proc_exit(0);
	connInfo->flag &= ~(CONNFLAG_NO_CDC_MODE | CONNFLAG_INITIAL_SNAPSHOT_MODE);
	if (!strcasecmp(snapshotMode, "initial_only"))
		connInfo->flag |= CONNFLAG_NO_CDC_MODE;

	validate_schema_sync_request(type, connInfo->snapengine, connInfo->flag);

	if (connInfo->snapengine == ENGINE_DEBEZIUM)
	{
		start_debezium_engine(type, connInfo, snapshotMode);
		return true;
	}
	if (connInfo->snapengine != ENGINE_FDW)
		elog(ERROR, "unsupported snapshot engine %d", connInfo->snapengine);

	synchdb_bootstrap_require_mode(snapshotMode,
			(connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE) != 0);
	if (worker.bootstrap)
	{
		pfree(worker.bootstrap->detail);
		pfree(worker.bootstrap);
		worker.bootstrap = NULL;
	}
	connInfo->flag |= CONNFLAG_INITIAL_SNAPSHOT_MODE | CONNFLAG_NO_CDC_MODE;
	set_shm_connector_state(myConnectorId, STATE_SYNCING);
	return true;
}

/*
 * Read a top-level boolean from the compact JSON stored in an offset file.
 * This deliberately avoids treating mere key presence as a true value.
 */
static bool
offset_json_bool_value(const char *offset, const char *key, bool *found)
{
	char		pattern[64];
	const char *value;

	*found = false;
	if (offset == NULL || key == NULL)
		return false;

	snprintf(pattern, sizeof(pattern), "\"%s\"", key);
	value = strstr(offset, pattern);
	if (value == NULL)
		return false;

	value = strchr(value + strlen(pattern), ':');
	if (value == NULL)
		return false;

	value++;
	while (*value && isspace((unsigned char) *value))
		value++;

	if (pg_strncasecmp(value, "true", 4) == 0)
	{
		*found = true;
		return true;
	}
	if (pg_strncasecmp(value, "false", 5) == 0)
	{
		*found = true;
		return false;
	}

	return false;
}

/*
 * dbz_read_snapshot_state
 *
 * This function determines if initial snapshot has been completed based
 * on debezium style offset information.
 *
 * @param connectorType: The type of connector
 * @param offset: current offset of the connector
 */
static bool
dbz_read_snapshot_state(ConnectorType type, const char * offset)
{
	if (!offset)
		return false;

	/* no offset available - assuming snapshot is not done */
	if (offset && (!strcasecmp(offset, "no offset") ||
			!strcasecmp(offset, "offset file not flushed yet")))
	{
		elog(WARNING, "offset file absent. Assuming snapshot is not done...");
		return false;
	}

	switch(type)
	{
		case TYPE_ORACLE:
		{
			/*
			 * if all 3 of these are present, that means debezium has already
			 * been in CDC stage, so we can assume that initial snapshot has
			 * been completed.
			 */
			if (strstr(offset, "commit_scn") &&
				strstr(offset, "snapshot_scn") &&
				strstr(offset, "scn"))
			{
				return true;
			}

			/*
			 * snapshot is also considered done if debezium explicitly said
			 * it is completed
			 */
			if (strstr(offset, "\"snapshot_completed\":true"))
			{
				return true;
			}

			break;
		}
		case TYPE_MYSQL:
		{
			/*
			 * if all 4 of these are present, that means debezium has already
			 * been in CDC stage, so we can assume that initial snapshot has
			 * been completed. todo: gtid enable mode handling
			 */
			if (strstr(offset, "ts_sec") &&
				strstr(offset, "file") &&
				strstr(offset, "pos") &&
				strstr(offset, "server_id"))
			{
				return true;
			}

			/*
			 * snapshot is also considered done if debezium explicitly said
			 * it is completed
			 */
			if (strstr(offset, "\"snapshot_completed\":true"))
			{
				return true;
			}
			break;
		}
		case TYPE_POSTGRES:
		{
			/*
			 * if there is no snapshot tag any more. Snapshot is considered done
			 */
			if (!strstr(offset, "\"snapshot\":"))
			{
				return true;
			}

			/* snapshot is considered done if the offset indicates a last snapshot record */
			if (strstr(offset, "\"last_snapshot_record\":true"))
			{
				return true;
			}

			/*
			 * snapshot is also considered done if debezium explicitly said
			 * it is completed
			 */
			if (strstr(offset, "\"snapshot_completed\":true"))
			{
				return true;
			}
			break;
		}
		case TYPE_SQLSERVER:
		{
			bool		found;
			bool		completed;
			bool		snapshot;

			/*
			 * The FDW handoff offset intentionally contains a commit_lsn while
			 * snapshot_completed is false.  Check the explicit state first so an
			 * interrupted handoff is never mistaken for a completed snapshot.
			 */
			completed = offset_json_bool_value(offset, "snapshot_completed", &found);
			if (found)
				return completed;

			snapshot = offset_json_bool_value(offset, "snapshot", &found);
			if (found && snapshot)
				return false;

			/* Streaming offsets retain commit_lsn but no active snapshot marker. */
			if (strstr(offset, "\"commit_lsn\":"))
				return true;
			break;
		}
		case TYPE_OLR:
		default:
		{
			set_shm_connector_errmsg(myConnectorId, "unsupported connector type"
					" to read snapshot status");
			elog(ERROR, "unsupported connector type %d to read snapshot status",
					type);
			break;
		}
	}
	return false;
}

/*
 * main_loop - Main processor of SynchDB connector worker
 *
 * This function continuously fetches change requests from Debezium runner and
 * process them until exit signal is received.
 *
 * @param connectorType: The type of connector being started
 * @param connInfo: Pointer to the ConnectionInfo structure containing connection details
 * @param snapshotMode: Snapshot mode requested
 */
static bool
main_loop(SynchdbWorkerRun *run)
{
	ConnectorType connectorType = run->spec.legacy_type;
	ConnectionInfo *connInfo = &run->execution;
	char *snapshotMode = run->snapshot_mode;
	ConnectorState currstate;
	bool dbzExitSignal = false;
	SynchdbStatistics myBatchStats = {0};


	elog(LOG, "Main LOOP ENTER ");

	while (!worker_should_stop())
	{
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			/* Converter normalization remains fixed for this worker's targets. */
			synchdb_letter_casing_strategy = run->spec.connection.run_options.synchdb_letter_casing_strategy;
		}

        if (dbzExitSignal)
        {
#ifdef WITH_OLR
            if (connectorType == TYPE_OLR && olr_handoff_active() &&
                !(connInfo->flag & CONNFLAG_NO_CDC_MODE))
            {
                bool schema_only = (connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE) != 0;
                if (dbz_engine_stop())
                    elog(ERROR, "could not close the completed OLR snapshot producer");
                olr_handoff_complete(connInfo, &worker.java);
                fc_resetDataCache();
                connInfo->flag &= ~(CONNFLAG_INITIAL_SNAPSHOT_MODE | CONNFLAG_SCHEMA_SYNC_MODE);
                set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
                set_shm_connector_state(myConnectorId, schema_only ? STATE_PAUSED : STATE_SYNCING);
                dbzExitSignal = false;
                continue;
            }
#endif
            break;
        }

		CHECK_FOR_INTERRUPTS();

		if (processRequestInterrupt(run, myConnectorId))
			return true;

		if (worker.bootstrap && worker.bootstrap->stop_after &&
			worker.bootstrap->status == SYNCHDB_BOOTSTRAP_COMPLETE)
		{
			dbzExitSignal = true;
			break;
		}
		currstate = get_shm_connector_state_enum(myConnectorId);
		switch (currstate)
		{
			case STATE_SYNCING:
			{
				switch(connectorType)
				{
					case TYPE_MYSQL:
					case TYPE_ORACLE:
					case TYPE_POSTGRES:
					case TYPE_SQLSERVER:
					{
						if (((connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE) ||
							(connInfo->flag & CONNFLAG_INITIAL_SNAPSHOT_MODE)) &&
							connInfo->snapengine == ENGINE_FDW)
						{
							/*
							 * Prepare to launch FDW based snapshot here and request a schema
							 * history file to be populated for mysql/oracle. Postgres and
							 * sqlserver don't have a dbz-schema-history JSON builder implemented
							 * in synchdb_create_stage_fts() yet, so schema history writing
							 * must stay disabled for them.
							 */
							launch_fdw_based_snapshot(connectorType, connInfo, snapshotMode,
									(connectorType == TYPE_POSTGRES || connectorType == TYPE_SQLSERVER) ? false: true);
						}
						else
						{
							/* Debezium based snapshot, schema and CDC processing logics here */
							process_debezium_batch(myConnectorId, &dbzExitSignal,
												  &myBatchStats, connInfo->flag, true);
						}
						break;
					}
					case TYPE_OLR:
					{
#ifdef WITH_OLR
						if ((connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE) ||
							(connInfo->flag & CONNFLAG_INITIAL_SNAPSHOT_MODE))
						{
							/* initial snapshot processing logics here */
							if (connInfo->snapengine == ENGINE_DEBEZIUM)
							{
								/* continuously poll changes from debezium engine */
								process_debezium_batch(myConnectorId, &dbzExitSignal,
													  &myBatchStats, connInfo->flag, false);
							}
							else if (connInfo->snapengine == ENGINE_FDW)
							{
								/* Prepare to launch FDW based snapshot here with schemahistory not requested */
								launch_fdw_based_snapshot(connectorType, connInfo, snapshotMode, false);
							}
						}
						else
						{
							/* Openlog Replicator based CDC processing logics here */

							int ret = -1;
							bool sendconfirm = false;

							/* is cdc even needed? */
							if (connInfo->flag & CONNFLAG_NO_CDC_MODE)
							{
								elog(WARNING, "CDC is not requested under snapshot mode %s",
										snapshotMode);
								set_shm_connector_errmsg(myConnectorId, "CDC is not requested");
								dbzExitSignal = true;
								break;
							}

							/* check if connection is still alive */
							if (olr_client_get_connect_status())
							{
								memset(&myBatchStats, 0, sizeof(myBatchStats));
								ret = olr_client_get_change(myConnectorId, &dbzExitSignal, &myBatchStats,
										&sendconfirm);

								/* send confirm message to OLR if it is necessary */
								if (sendconfirm)
								{
									elog(DEBUG1, "successfully applied - send confirm message for "
											"scn %llu and c_scn %llu", olr_client_get_scn(),
											olr_client_get_c_scn());

									olr_client_confirm_scn(connInfo->olr.olr_source);

                                    set_shm_dbz_offset(myConnectorId);
								}

								/* update statistics if at least one batch is attempted (ret != -2) */
								if (ret != -2)
								{
									/* increment batch connector statistics */
									increment_connector_statistics(&myBatchStats, STATS_BATCH_COMPLETION, 1);

									/* update the batch statistics to shared memory */
									set_shm_connector_statistics(myConnectorId, &myBatchStats);
								}
							}
							else
							{
								/*
								 * peer has disconnected, let's retry connection again with
								 * a little bit of delay in between...
								 */
								sleep(3);
								try_reconnect_olr(connInfo);
							}
						}

#else
						set_shm_connector_errmsg(myConnectorId, "OLR connector is not enabled in this synchdb build");
						elog(ERROR, "OLR connector is not enabled in this synchdb build");
#endif
						break;
					}
					default:
					{
						set_shm_connector_errmsg(myConnectorId, "unsupported connector type");
						elog(ERROR, "unsupported connector type");
						break;
					}
				}
				break;
			}
			case STATE_PAUSED:
			{
				/* Do nothing when paused */
				break;
			}
			case STATE_SCHEMA_SYNC_DONE:
			{
				if (connInfo->snapengine == ENGINE_FDW)
					elog(ERROR, "FDW bootstrap cannot enter a CDC handoff state");
				switch(connectorType)
				{
					case TYPE_MYSQL:
					case TYPE_SQLSERVER:
					case TYPE_ORACLE:
					case TYPE_POSTGRES:
					{
						/* exit schema sync mode if set and enter pause state - takes precedence */
						if ((connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE))
						{
							/* Finish the handoff before advertising that resume is safe. */
							if (dbz_engine_stop())
								elog(ERROR, "failed to stop Debezium engine after schema sync");


							connInfo->flag &= ~(CONNFLAG_SCHEMA_SYNC_MODE |
								CONNFLAG_INITIAL_SNAPSHOT_MODE | CONNFLAG_NO_CDC_MODE);
							set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
							set_shm_connector_state(myConnectorId, STATE_PAUSED);
						}
						else
						{
							/* exit initial snapshot mode if set and resume syncing state */
							if ((connInfo->flag & CONNFLAG_INITIAL_SNAPSHOT_MODE))
							{
								connInfo->flag &= ~CONNFLAG_INITIAL_SNAPSHOT_MODE;
								set_shm_connector_state(myConnectorId, STATE_SYNCING);


								if (connInfo->flag & CONNFLAG_NO_CDC_MODE)
								{
									/* The FDW workflow and its error check have both succeeded. */
									dbzExitSignal = true;
									break;
								}

								/*
								 * SQL Server requires schema history before streaming.  Recovery
								 * captures schema only and, unlike an initial snapshot, preserves
								 * the cutoff offset produced by the FDW snapshot.
								 */
								strlcpy(snapshotMode, connectorType == TYPE_SQLSERVER &&
										connInfo->snapengine == ENGINE_FDW ? "recovery" : "initial",
										sizeof(run->snapshot_mode));

								/* clear error messages */
								set_shm_connector_errmsg(myConnectorId, NULL);

								/*
								 * if connInfo->run_options.cdc_start_delay_ms is set > 0, we need to delay here before the
								 * next iteration, which will begin the CDC process
								 */
								if (connInfo->run_options.cdc_start_delay_ms > 0)
									usleep(connInfo->run_options.cdc_start_delay_ms * 1000);

								/* start debezium now */
								start_debezium_engine(connectorType, connInfo, snapshotMode);
							}

							/* set the stage to change data capture */
							set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
						}
						break;
					}
					case TYPE_OLR:
					{
#ifdef WITH_OLR
                        /* snapshot:last only completes a data row. Continue
                         * draining through the finite producer's acknowledged EOF. */
                        set_shm_connector_state(myConnectorId, STATE_SYNCING);

#endif
						break;
					}
					default:
					{
						break;
					}
				}
				break;
			}
			default:
				/* Handle other states if necessary */
				break;
		}

		(void)WaitLatch(MyLatch,
						WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						synchdb_worker_naptime,
						PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);
	}
	elog(LOG, "Main LOOP QUIT");

	return dbzExitSignal && !ShutdownRequestPending &&
		(connInfo->flag & CONNFLAG_NO_CDC_MODE) != 0;
}

/*
 * cleanup - Cleanup routine
 *
 * This function shuts down Debezium runner, JVM and cleans up format converter
 * resources
 *
 * @param connectorType: The type of connector being started
 */
static bool
cleanup(ConnectorType connectorType)
{
	Assert(connectorType == worker.run.spec.legacy_type);
	return synchdb_worker_cleanup(&worker);
}

static bool
prepare_worker_close(SynchdbWorkerContext *context)
{
	/* Exit can also bypass the normal per-frame ERROR boundary. */
	g_eventStr = NULL;

	return true;
}

static bool
close_worker_local(SynchdbWorkerContext *context)
{
#ifdef WITH_OLR
	if (context->run.spec.legacy_type == TYPE_OLR)
		olr_client_shutdown();
#endif
	if (context->converter_live)
	{
		fc_deinitFormatConverter(context->run.spec.legacy_type);
		context->converter_live = false;
	}
	if (context->cache_live)
	{
		fc_deinitDataCache();
		context->cache_live = false;
	}
	return true;
}

/*
 * dbz_mark_batch_complete - notify Debezium a completed batch
 *
 * This function notifies Debezium runner that a batch has been successfully completed
 *
 * @param batchid: The unique batch ID that has been completed successfully
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_mark_batch_complete(int batchid)
{
	return synchdb_java_ack_batch(&worker.java, batchid);
}

/* Target commit and source acknowledgement publish distinct statistics deltas. */
static void
process_debezium_batch(int connectorId, bool *exit_signal,
					   SynchdbStatistics *statistics, int flag, bool refresh_offset)
{
	BatchInfo batch = {SYNCHDB_INVALID_BATCH_ID, 0};

	MemSet(statistics, 0, sizeof(*statistics));
	dbz_engine_get_change(connectorId, exit_signal, &batch, statistics, flag);
	if (batch.batchId == SYNCHDB_INVALID_BATCH_ID)
		return;
	/* Committed target work remains visible if subsequent ACK fails. */
	set_shm_connector_statistics(connectorId, statistics);
	if (dbz_mark_batch_complete(batch.batchId) < 0)
		elog(ERROR, "Debezium batch confirmation failed after target commit; source session is frozen");
	synchdb_registry_report_batch_completion(sdb_state, &worker.owner);
	synchdb_registry_report_ready(sdb_state, &worker.owner, true);
	if (refresh_offset)
		set_shm_dbz_offset(connectorId);
}

/*
 * connectorTypeToString
 *
 * This function converts connector type from enum to string
 *
 * @param type: Connector type in enum
 *
 * @return connector type in string
 */
const char *
connectorTypeToString(ConnectorType type)
{
	switch (type)
	{
	case TYPE_UNDEF:
		return "UNDEFINED";
	case TYPE_MYSQL:
		return "MYSQL";
	case TYPE_ORACLE:
		return "ORACLE";
	case TYPE_SQLSERVER:
		return "SQLSERVER";
	case TYPE_OLR:
		return "OLR";
	case TYPE_POSTGRES:
		return "POSTGRES";
	default:
		return "UNKNOWN";
	}
}

/*
 * stringToConnectorType
 *
 * This function converts string to connector type
 *
 * @param type: connector type in string
 *
 * @return connector type enum
 */
ConnectorType
stringToConnectorType(const char * type)
{
	if (!strcasecmp(type, "mysql"))
		return TYPE_MYSQL;
	else if(!strcasecmp(type, "sqlserver"))
		return TYPE_SQLSERVER;
	else if(!strcasecmp(type, "oracle"))
		return TYPE_ORACLE;
	else if(!strcasecmp(type, "olr"))
		return TYPE_OLR;
	else if(!strcasecmp(type, "postgres"))
		return TYPE_POSTGRES;
	else
		return TYPE_UNDEF;
}

/*
 * get_shm_connector_name
 *
 * This function converts connector type from enum to string in lowercase
 *
 * @param type: Connector type in enum
 *
 * @return connector type in string
 * todo: potential duplicate
 */
const char *
get_shm_connector_name(ConnectorType type)
{
	switch (type)
	{
	case TYPE_MYSQL:
		return "mysql";
	case TYPE_ORACLE:
		return "oracle";
	case TYPE_SQLSERVER:
		return "sqlserver";
	case TYPE_OLR:
		return "olr";
	case TYPE_POSTGRES:
		return "postgres";
	/* todo: support more dbz connector types here */
	default:
		return "null";
	}
}

/*
 * get_shm_connector_pid
 *
 * This function gets pid of the given connectorID
 *
 * @param connectorId: Connector ID of interest
 *
 * @return pid of the connector, -1 if connector is not running
 */
pid_t
get_shm_connector_pid(int connectorId)
{
	SynchdbRunView view;

	return read_connector_view(connectorId, &view) ? view.pid : InvalidPid;
}

/*
 * get_shm_connector_errmsg
 *
 * This function gets the last error message of the given connectorID
 *
 * @param connectorId: Connector ID of interest
 *
 * @return the error message in string
 */
const char *
get_shm_connector_errmsg(int connectorId)
{
	static char value[SYNCHDB_ERRMSG_SIZE];
	SynchdbRunView view;

	strlcpy(value, "no error", sizeof(value));
	if (read_connector_view(connectorId, &view) && view.errmsg[0] != '\0')
		strlcpy(value, view.errmsg, sizeof(value));
	return value;
}

/*
 * set_shm_connector_errmsg - Set the error message for a specific connector in shared memory
 *
 * This function sets the error message for a given connector type in the shared
 * memory state. It ensures thread-safety by using a lock when accessing shared memory.
 *
 * @param connectorId: Connector ID of interest
 * @param err: The error message to set. If NULL, an empty string will be set.
 */
void
set_shm_connector_errmsg(int connectorId, const char *err)
{
	if (connectorId == worker.owner.run.slot)
		synchdb_registry_report_error(sdb_state, &worker.owner, err);
}

/*
 * set_shm_connector_statistics - adds the give stats
 *
 * This function adds the given stats info to the one in shared memory so user
 * can see updated stats
 *
 * @param connectorId: Connector ID of interest
 * @param stats: connector statistics struct
 */
static void
set_shm_connector_statistics(int connectorId, SynchdbStatistics * stats)
{
	if (connectorId == worker.owner.run.slot)
		synchdb_registry_report_stats(sdb_state, &worker.owner, stats);
}


#ifdef WITH_OLR
static void
try_reconnect_olr(ConnectionInfo * connInfo)
{
    if (worker_should_stop())
        proc_exit(0);
    if (!olr_client_get_connect_status())
        olr_handoff_reconnect(connInfo, &worker.java);
}

#endif

static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void
synchdb_ProcessUtility(PlannedStmt *pstmt,
                       const char *queryString,
                       bool readOnlyTree,
                       ProcessUtilityContext context,
                       ParamListInfo params,
                       QueryEnvironment *queryEnv,
                       DestReceiver *dest,
                       QueryCompletion *qc)
{
    Node *parsetree = pstmt->utilityStmt;
    if (nodeTag(parsetree) == T_DropStmt)
    {
        DropStmt *drop = (DropStmt *) parsetree;
        if (drop->removeType == OBJECT_EXTENSION)
        {
            ListCell *lc;
            foreach(lc, drop->objects)
            {
                Node *object = (Node *) lfirst(lc);
                /* Handle both string names and list of names */
                char *name = NULL;
                if (IsA(object, String))
                    name = strVal(object);
                else if (IsA(object, List))
                {
                    /* Extract name from list */
                    List *names = (List *) object;
                    if (list_length(names) == 1)
                        name = strVal(linitial(names));
                }
                if (name && strcmp(name, "synchdb") == 0)
                {
                    char running_name[SYNCHDB_CONNINFO_NAME_SIZE] = {0};
                    const char *dbname = get_database_name(MyDatabaseId);

                    if (!sdb_state)
                        sdb_state = synchdb_registry_try_attach(synchdb_max_connector_workers);

                    if (has_running_connectors_for_db(dbname, running_name, sizeof(running_name)))
                    {
                        if (running_name[0] != '\0')
                            ereport(ERROR,
                                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                     errmsg("cannot drop extension \"synchdb\" because connectors are still running in database \"%s\"", dbname),
                                     errdetail("Example: connector \"%s\" is still active.", running_name),
                                     errhint("Stop every connector with synchdb_stop_engine_bgw() before running DROP EXTENSION.")));
                        else
                            ereport(ERROR,
                                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                     errmsg("cannot drop extension \"synchdb\" while connectors are running"),
                                     errhint("Use synchdb_stop_engine_bgw() to stop them first.")));
                    }
                }
            }
        }
    }
    if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
}

static int
launch_fdw_based_snapshot(ConnectorType connectorType, ConnectionInfo *connInfo,
						  char *snapshotMode, bool schemahistory)
{
	SynchdbRunSpec spec = worker.run.spec;
	SynchdbBootstrapResult *volatile result = NULL;
	bool own_transaction = !IsTransactionOrTransactionBlock();
	bool previous = synchdb_worker_sql_interruptible();
	char *compatible_mode = NULL;

	(void) connectorType;
	(void) schemahistory; /* History publication belongs to verified handoff. */
	spec.connection = *connInfo;
	strlcpy(spec.snapshot_mode, snapshotMode, sizeof(spec.snapshot_mode));
	synchdb_bootstrap_require_mode(snapshotMode,
			(connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE) != 0);
	set_shm_connector_stage(myConnectorId,
		(connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE) ? STAGE_SCHEMA_SYNC : STAGE_INITIAL_SNAPSHOT);
	if (connInfo->isOraCompat)
		compatible_mode = pstrdup(GetConfigOption("ivorysql.compatible_mode", false, false));
	PG_TRY();
	{
		synchdb_worker_sql_enter();
		if (own_transaction)
		{
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
		}
		if (compatible_mode)
			SetConfigOption("ivorysql.compatible_mode", "pg", PGC_USERSET, PGC_S_OVERRIDE);
		result = synchdb_bootstrap_execute(&spec, TopMemoryContext);
		if (own_transaction)
		{
			PopActiveSnapshot();
			CommitTransactionCommand();
		}
	}
	PG_FINALLY();
	{
		synchdb_worker_sql_restore(previous);
		if (compatible_mode)
		{
			SetConfigOption("ivorysql.compatible_mode", compatible_mode, PGC_USERSET, PGC_S_OVERRIDE);
			pfree(compatible_mode);
		}
	}
	PG_END_TRY();
	worker.bootstrap = result;
	synchdb_registry_report_bootstrap(sdb_state, &worker.owner, (int) result->status + 1);
	/* Retain bootstrap flags: a resume command cannot jump into CDC. */
	set_shm_connector_state(myConnectorId, STATE_PAUSED);
	if (result->status != SYNCHDB_BOOTSTRAP_COMPLETE)
	{
		char *message = psprintf("FDW bootstrap %s: %llu complete, %llu failed tables. No CDC handoff; inspect the structured result before a new bootstrap.",
			result->status == SYNCHDB_BOOTSTRAP_PARTIAL ? "PARTIAL" : "FAILED",
			(unsigned long long) result->completed_tables, (unsigned long long) result->failed_tables);
		set_shm_connector_errmsg(myConnectorId, message);
		pfree(message);
	}
	else
		set_shm_connector_errmsg(myConnectorId, result->stop_after ? NULL :
			"Schema bootstrap completed. CDC handoff is unavailable; resume is blocked.");
	return 0;
}

/*
 * increment_connector_statistics - increment statistics
 *
 * This function increments statistic counter for specified type
 *
 * @param which: type of statistic to increment
 */
void
increment_connector_statistics(SynchdbStatistics * myStats, ConnectorStatistics which, int incby)
{
	if (!myStats)
		return;

	switch(which)
	{
		/* CDC stats */
		case STATS_DDL:
			myStats->cdcstats.stats_ddl += incby;
			break;
		case STATS_DML:
			myStats->cdcstats.stats_dml += incby;
			break;
		case STATS_CREATE:
			myStats->cdcstats.stats_create += incby;
			break;
		case STATS_UPDATE:
			myStats->cdcstats.stats_update += incby;
			break;
		case STATS_DELETE:
			myStats->cdcstats.stats_delete += incby;
			break;
		case STATS_TRUNCATE:
			myStats->cdcstats.stats_truncate += incby;
			break;
		case STATS_TX:
			myStats->cdcstats.stats_tx += incby;
			break;

		/* snapshot stats */
		case STATS_TABLES:
			myStats->snapstats.snapstats_tables += incby;
			break;
		case STATS_ROWS:
			myStats->snapstats.snapstats_rows += incby;
			break;

		/* general stats */
		case STATS_BAD_CHANGE_EVENT:
			myStats->genstats.stats_bad_change_event += incby;
			break;
		case STATS_TOTAL_CHANGE_EVENT:
			myStats->genstats.stats_total_change_event += incby;
			break;
		case STATS_BATCH_COMPLETION:
			myStats->genstats.stats_batch_completion += incby;
			break;
		default:
			break;
	}
}

/*
 * get_shm_connector_type - Get the connector type in enum
 *
 * This function gets specified connector type.
 *
 * @param connectorId: Connector ID of interest
 *
 * @return ConnectorType enum
 */
ConnectorType
get_shm_connector_type_enum(int connectorId)
{
	SynchdbRunView view;

	return read_connector_view(connectorId, &view) ? view.type : TYPE_UNDEF;
}

/*
 * get_shm_connector_stage_enum - Get the current connector stage in enum
 *
 * This function gets current connector stage based on the change request received.
 *
 * @param connectorId: Connector ID of interest
 *
 * @return connector stage in enum
 */
ConnectorStage
get_shm_connector_stage_enum(int connectorId)
{
	SynchdbRunView view;

	return read_connector_view(connectorId, &view) ? view.stage : STAGE_UNDEF;
}

/*
 * set_shm_connector_stage - Set the current connector stage in enum
 *
 * This function sets current connector stage
 *
 * @param connectorId: Connector ID of interest
 * @param stage: new connector stage
 */
void
set_shm_connector_stage(int connectorId, ConnectorStage stage)
{
	if (connectorId == worker.owner.run.slot)
		synchdb_registry_report_stage(sdb_state, &worker.owner, stage);
}

/*
 * get_shm_connector_state - Get the current state of a specific connector from shared memory
 *
 * This function retrieves the current state of a given connector type from the shared
 * memory state. It returns the state as a string representation.
 *
 * @param connectorId: Connector ID of interest
 *
 * @return: A string representation of the connector's state. If the shared memory
 *          is not initialized or the connector type is unknown, it returns "stopped"
 *          or "undefined" respectively.
 */
const char *
get_shm_connector_state(int connectorId)
{
	if (!shm_connector_id_is_valid(connectorId))
		return "stopped";
	return connectorStateAsString(get_shm_connector_state_enum(connectorId));
}

/*
 * get_shm_connector_state_enum - Get the current state enum of a specific connector from shared memory
 *
 * This function retrieves the current state of a given connector type from the shared
 * memory state as a ConnectorState enum value.
 *
 * @param connectorId: Connector ID of interest
 *
 * @return: A ConnectorState enum representing the connector's state. If the shared memory
 *          is not initialized or the connector type is unknown, it returns STATE_UNDEF.
 */
ConnectorState
get_shm_connector_state_enum(int connectorId)
{
	SynchdbRunView view;

	return read_connector_view(connectorId, &view) ? view.state : STATE_UNDEF;
}

/*
 * set_shm_connector_state - Set the state of a specific connector in shared memory
 *
 * This function sets the state of a given connector type in the shared memory.
 * It ensures thread-safety by using an exclusive lock when accessing shared memory.
 *
 * @param connectorId: Connector ID of interest
 * @param state: The new state to set for the connector
 */
void
set_shm_connector_state(int connectorId, ConnectorState state)
{
	if (connectorId == worker.owner.run.slot)
		synchdb_registry_report_state(sdb_state, &worker.owner, state);
}

/*
 * set_shm_dbz_offset - Set the offset of a paused connector
 *
 * This method reads from dbz's offset file per connector type, which does not
 * reflect the real-time offset of dbz engine. If we were to resume from this point
 * due to an error, there may be duplicate values after the resume in which we must
 * handle. In the future, we will need to explore a more accurate way to find out
 * the offset managed within dbz so we could freely resume from any reference not
 * just at the flushed locations. todo
 *
 * @param connectorId: Connector ID of interest
 */
void
set_shm_dbz_offset(int connectorId)
{
	char *offset;

	if (!shm_connector_id_is_valid(connectorId))
		return;

#ifdef WITH_OLR
	if (worker.run.spec.legacy_type == TYPE_OLR)
	{
		offset = psprintf("{\"scn\":%llu, \"c_scn\":%llu, \"c_idx\":%llu}",
				olr_client_get_scn(), olr_client_get_c_scn(),
				olr_client_get_c_idx());
	}
	else
#endif
	{
		offset = dbz_engine_get_offset(connectorId);
		if (!offset)
			return;
	}

	if (connectorId == worker.owner.run.slot)
		synchdb_registry_report_offset(sdb_state, &worker.owner, offset);
	pfree(offset);
}

/*
 * get_shm_dbz_offset - Get the offset from a connector
 *
 * This method gets the offset value of the given connector from shared memory
 *
 * @param connectorId: Connector ID of interest
 */
const char *
get_shm_dbz_offset(int connectorId)
{
	static char value[SYNCHDB_ERRMSG_SIZE];
	SynchdbRunView view;

	strlcpy(value, "no offset", sizeof(value));
	if (read_connector_view(connectorId, &view) && view.dbzoffset[0] != '\0')
		strlcpy(value, view.dbzoffset, sizeof(value));
	return value;
}

/*
 * get_shm_connector_name - Get the unique connector name based on connectorId
 *
 * This method gets the name value of the given connector from shared memory
 *
 * @param connectorId: Connector ID of interest
 */
const char *
get_shm_connector_name_by_id(int connectorId)
{
	static char value[SYNCHDB_ERRMSG_SIZE];
	SynchdbRunView view;

	strlcpy(value, "no name", sizeof(value));
	if (read_connector_view(connectorId, &view) && view.name[0] != '\0')
		strlcpy(value, view.name, sizeof(value));
	return value;
}

/*
 * get_shm_conn_user_by_id - Get the username associated with connector id
 *
 * This method gets the username value of the given connector from shared memory
 *
 * @param connectorId: Connector ID of interest
 */
const char *
get_shm_connector_user_by_id(int connectorId)
{
	static char value[SYNCHDB_ERRMSG_SIZE];
	SynchdbRunView view;

	strlcpy(value, "no user", sizeof(value));
	if (read_connector_view(connectorId, &view) && view.user[0] != '\0')
		strlcpy(value, view.user, sizeof(value));
	return value;
}

/*
 * get_shm_ora_compat - check if we are running against oracle compatible ivorysql
 *
 * This method returns true if we are in oracle compatible mode, false otherwise
 *
 * @param connectorId: Connector ID of interest
 */
bool
get_shm_ora_compat(int connectorId)
{
	SynchdbRunView view;

	return read_connector_view(connectorId, &view) && view.isOraCompat;
}

/*
 * _PG_init - Initialize the SynchDB extension
 */
static bool
check_connector_capacity(int *value, void **extra, GucSource source)
{
	if (!synchdb_registry_capacity_compatible(*value))
	{
		GUC_check_errdetail("Connector capacity is fixed for the current server runtime; restart PostgreSQL to change it.");
		return false;
	}
	return true;
}

void
_PG_init(void)
{
	synchdb_credentials_register_guc();
	DefineCustomIntVariable("synchdb.naptime",
							"Duration between each data polling (in milliseconds).",
							NULL,
							&synchdb_worker_naptime,
							10,
							1,
							30000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("synchdb.dml_use_spi",
							 "option to use SPI to handle DML operations. Default false",
							 NULL,
							 &synchdb_dml_use_spi,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.dbz_batch_size",
							"the maximum number of change events in a batch",
							NULL,
							&dbz_batch_size,
							2048,
							1,
							65535,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.dbz_queue_size",
							"the maximum size of Debezium's change event queue",
							NULL,
							&dbz_queue_size,
							8192,
							64,
							65535,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.dbz_connect_timeout_ms",
							"Debezium's connection timeout value in milliseconds",
							NULL,
							&dbz_connect_timeout_ms,
							30000,
							1000,
							3600000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.dbz_query_timeout_ms",
							"Debezium's query timeout value in milliseconds",
							NULL,
							&dbz_query_timeout_ms,
							600000,
							1000,
							3600000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomStringVariable("synchdb.dbz_skipped_oeprations",
							   "a comma-separated list of operations Debezium shall skip: "
							   "c for inserts, u for updates, d for deletes, t for truncates",
							   NULL,
							   &dbz_skipped_operations,
							   "t",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.jvm_max_heap_size",
							"max heap size allocated to JVM",
							NULL,
							&jvm_max_heap_size,
							1024,
							0,
							65536,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.dbz_snapshot_thread_num",
							"number of threads to perform Debezium initial snapshot",
							NULL,
							&dbz_snapshot_thread_num,
							1,
							1,
							16,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.dbz_snapshot_fetch_size",
							"number of rows Debezium fetches at a time during a snapshot",
							NULL,
							&dbz_snapshot_fetch_size,
							0,
							0,
							65535,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.dbz_snapshot_min_row_to_stream_results",
							"minimum row in a remote table before switching to streaming mode",
							NULL,
							&dbz_snapshot_min_row_to_stream_results,
							0,
							0,
							65535,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.dbz_incremental_snapshot_chunk_size",
							"batch size of incremental snapshot process",
							NULL,
							&dbz_incremental_snapshot_chunk_size,
							2048,
							1,
							65535,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomStringVariable("synchdb.dbz_incremental_snapshot_watermarking_strategy",
							   "watermarking strategy of incremental snapshot",
							   NULL,
							   &dbz_incremental_snapshot_watermarking_strategy,
							   "insert_insert",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.dbz_offset_flush_interval_ms",
							"time in milliseconds to flush offset file to disk",
							NULL,
							&dbz_offset_flush_interval_ms,
							60000,
							1000,
							3600000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("synchdb.dbz_capture_only_selected_table_ddl",
							 "whether or not debezium should capture the schema or all tables(false) or selected tables(true).",
							 NULL,
							 &dbz_capture_only_selected_table_ddl,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/* Shared-array capacity cannot change after server startup. */
	{
		DefineCustomIntVariable("synchdb.max_connector_workers",
								"max number of connector workers that can be run at any time. Higher number would occupy more"
								"shared memory space",
								NULL,
								&synchdb_max_connector_workers,
								30,
								1,
								65535,
								/* Late definitions must replay SIGHUP-sourced placeholders. */
								process_shared_preload_libraries_in_progress ? PGC_POSTMASTER : PGC_SIGHUP,
								0,
								check_connector_capacity, NULL, NULL);
	}

	DefineCustomEnumVariable("synchdb.error_handling_strategy",
							 "strategy to handle error. Possible values are skip, exit, or retry",
							 NULL,
							 &synchdb_error_strategy,
							 STRAT_EXIT_ON_ERROR,
							 error_strategies,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("synchdb.dbz_log_level",
							 "log level of Debezium runner",
							 NULL,
							 &dbz_log_level,
							 LOG_LEVEL_WARN,
							 dbz_log_levels,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("synchdb.log_change_on_error",
							 "option to log JSON change event from DBZ in case of error",
							 NULL,
							 &synchdb_log_event_on_error,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.olr_read_buffer_size",
							"size of buffer in megabytes for network IO reads",
							NULL,
							&olr_read_buffer_size,
							128,
							8,
							512,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.jvm_max_direct_buffer_size",
							"max direct buffer size to allocate safely to hold JSON change events",
							NULL,
							&jvm_max_direct_buffer_size,
							1024,
							0,
							65536,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomEnumVariable("synchdb.dbz_logminer_stream_mode",
							 "debezium oracle connector logminer stream mode ",
							 NULL,
							 &dbz_logminer_stream_mode,
							 LOGMINER_MODE_UNCOMMITTED,
							 ora_logminer_stream_mode,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("synchdb.olr_connect_timeout_ms",
							"connect timeout in ms to establish connection to openlog replicator",
							NULL,
							&olr_connect_timeout_ms,
							5000,
							3000,
							3600000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("synchdb.olr_read_timeout_ms",
							"netio read timeout from openlog replicator",
							NULL,
							&olr_read_timeout_ms,
							5000,
							3000,
							3600000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomEnumVariable("synchdb.snapshot_engine",
							"engine used to complete initial snapshot for OLR connector",
							 NULL,
							 &synchdb_snapshot_engine,
							 ENGINE_DEBEZIUM,
							 snapshot_engines,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("synchdb.cdc_start_delay_ms",
							"a delay after initial snapshot completes and before cdc begins",
							NULL,
							&cdc_start_delay_ms,
							0,
							0,
							65536,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("synchdb.fdw_migrate_with_subtx",
							 "whether or not to use subtransaction for per table migration",
							 NULL,
							 &synchdb_fdw_use_subtx,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("synchdb.letter_casing_strategy",
							"letter casing strategy to use",
							 NULL,
							 &synchdb_letter_casing_strategy,
							 LCS_NORMALIZE_LOWERCASE,
							 letter_casing_strategies,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	if (process_shared_preload_libraries_in_progress)
		synchdb_registry_register_shmem(synchdb_max_connector_workers);

	/* initialize data type mapping engine for all connectors */
	fc_initFormatConverter(TYPE_MYSQL);
	fc_initFormatConverter(TYPE_SQLSERVER);
	fc_initFormatConverter(TYPE_ORACLE);
	fc_initFormatConverter(TYPE_POSTGRES);

	{
		DefineCustomBoolVariable("synchdb.synchdb_auto_launcher",
								 "Legacy compatibility option; durable controller recovery is always enabled "
								 "when synchdb is preloaded. Legacy isactive rows require an explicit START.",
								 NULL,
								 &synchdb_auto_launcher,
								 true,
								 process_shared_preload_libraries_in_progress ? PGC_POSTMASTER : PGC_SIGHUP,
								 0,
								 NULL,
								 NULL,
								 NULL);
	}

	MarkGUCPrefixReserved("synchdb");

	/* create a pg_synchdb directory under $PGDATA to store connector meta data */
	if (MakePGDirectory(SYNCHDB_METADATA_DIR) < 0)
	{
		if (errno != EEXIST)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create directory \"%s\": %m",
							SYNCHDB_METADATA_DIR)));
		}
	}
	else
	{
		fsync_fname(SYNCHDB_METADATA_DIR, true);
	}

	/* Register synchdb auto launch worker, if enabled. */
	if (process_shared_preload_libraries_in_progress)
		synchdb_control_register_launcher();

 	prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = synchdb_ProcessUtility;
}

/*
 * synchdb_engine_main - Main entry point for the SynchDB background worker
 */
static void
synchdb_engine_run(Datum main_arg)
{
	SynchdbWorkerRun *run = &worker.run;
	ConnectorType connectorType;
	ConnectionInfo *connInfo = &run->execution;
	char *snapshotMode = run->snapshot_mode;
	bool completed = false;

	/* extract connectorId from main_arg */
	myConnectorId = DatumGetUInt32(main_arg);

	/* Set up signal handlers, initialize shared memory and obtain connInfo*/
	setup_environment(run);
	connectorType = run->spec.legacy_type;

	/* Initialize the connector state */
	set_shm_connector_state(myConnectorId, STATE_INITIALIZING);

	/*
	 * Initialize the stage to be CDC and it may get changed when the connector
	 * detects that it is in initial snapshot or other stages
	 */
	set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
	set_shm_connector_errmsg(myConnectorId, NULL);

	/* initialize format converter and its cache */
	worker.converter_live = true;
	fc_initFormatConverter(connectorType);
	worker.cache_live = true;
	fc_initDataCache();

	/* load custom object mappings */
	load_worker_rules(connInfo->name, connectorType);
	synchdb_worker_sql_restore(false);

	switch (connectorType)
	{
		case TYPE_MYSQL:
		case TYPE_SQLSERVER:
		case TYPE_ORACLE:
		case TYPE_POSTGRES:
		{
			/* A local bootstrap needs neither a JVM nor offset/history files. */
			if (connInfo->snapengine == ENGINE_FDW)
				synchdb_bootstrap_require_mode(snapshotMode,
					(connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE) != 0);
			else
			{
				initialize_jvm(&connInfo->jmx, &connInfo->run_options);
				set_shm_dbz_offset(myConnectorId);
			}

			if (!start_snapshot_run(connectorType, connInfo, snapshotMode))
			{
				completed = true;
				break;
			}

			elog(LOG, "Going to main loop .... ");
			completed = main_loop(run);
			break;
		}
		case TYPE_OLR:
		{
#ifdef WITH_OLR
            bool resume = false;

            if (connInfo->snapengine == ENGINE_FDW)
            {
                synchdb_bootstrap_require_mode(snapshotMode,
                    (connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE) != 0);
                connInfo->flag |= CONNFLAG_INITIAL_SNAPSHOT_MODE | CONNFLAG_NO_CDC_MODE;
                set_shm_connector_state(myConnectorId, STATE_SYNCING);
                completed = main_loop(run);
                break;
            }
            initialize_jvm(&connInfo->jmx, &connInfo->run_options);
            if (strcmp(snapshotMode, "initial_only") == 0)
                connInfo->flag |= CONNFLAG_NO_CDC_MODE;
            else
                resume = olr_handoff_prepare(connInfo, &worker.java);
            if (resume)
            {
                if (strcmp(snapshotMode, "always") == 0)
                    elog(ERROR, "OLR always mode requires a fresh target and artifact generation");
                connInfo->flag &= ~(CONNFLAG_INITIAL_SNAPSHOT_MODE | CONNFLAG_SCHEMA_SYNC_MODE | CONNFLAG_NO_CDC_MODE);
                set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
                set_shm_connector_state(myConnectorId, STATE_SYNCING);
                synchdb_registry_report_ready(sdb_state, &worker.owner, true);
            }
            else
            {
                connInfo->flag |= CONNFLAG_INITIAL_SNAPSHOT_MODE;
                start_debezium_engine(connectorType, connInfo, snapshotMode);
            }
            completed = main_loop(run);

#else
			set_shm_connector_errmsg(myConnectorId, "OLR connector is not enabled in this synchdb build.");
			elog(ERROR, "OLR connector is not enabled in this synchdb build.");
#endif
			break;
		}
		default:
		{
			set_shm_connector_errmsg(myConnectorId, "Unsupported connector type");
			elog(ERROR, "Unsupported connector type %d", connectorType);
			break;
		}
	}

	if (completed)
	{
		if (!cleanup(connectorType))
			elog(ERROR, "could not finish cleanup of snapshot-only connector %s", connInfo->name);
		/* The controller retires the compatibility active flag after this
		 * process exits; terminal worker cleanup must not wait on catalog rows. */
		elog(LOG, "snapshot-only connector %s completed", connInfo->name);
	}

	elog(LOG, "synchdb worker shutting down .... ");
	proc_exit(0);
}

void
synchdb_engine_main(Datum main_arg)
{
    PG_TRY();
    {
        synchdb_engine_run(main_arg);
    }
    PG_CATCH();
    {
        ErrorData *error;

        MemoryContextSwitchTo(TopMemoryContext);
        error=CopyErrorData();
        synchdb_registry_report_failure(sdb_state,&worker.owner,error->sqlerrcode,error->message);
        FreeErrorData(error);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/* The controller owns native handles and decides whether to retry. */
BackgroundWorkerHandle *
synchdb_control_start_worker(const SynchdbRunSpec *spec, Oid execution_role,
                            uint64 controller_generation, SynchdbRunHandle *run)
{
    BackgroundWorker bgw={0};
    BackgroundWorkerHandle *volatile handle=NULL;
    SynchdbWorkerStartup startup;
    MemoryContext caller=CurrentMemoryContext;

    synchdb_init_shmem();
    *run=synchdb_registry_reserve(sdb_state,spec,false);
    PG_TRY();
    {
        if (!synchdb_registry_assign_controller(sdb_state,run,controller_generation))
            elog(ERROR,"could not assign connector controller");
        bgw.bgw_flags=BGWORKER_SHMEM_ACCESS|BGWORKER_BACKEND_DATABASE_CONNECTION;
        bgw.bgw_start_time=BgWorkerStart_ConsistentState;
        bgw.bgw_restart_time=BGW_NEVER_RESTART;
        bgw.bgw_notify_pid=MyProcPid;
        strlcpy(bgw.bgw_library_name,SYNCHDB_LIBRARY_NAME,BGW_MAXLEN);
        strlcpy(bgw.bgw_function_name,"synchdb_engine_main",BGW_MAXLEN);
        prepare_bgw(&bgw,spec,run);
        memcpy(&startup,bgw.bgw_extra,sizeof(startup));
        startup.execution_role=execution_role;
        startup.controller_generation=controller_generation;
        memcpy(bgw.bgw_extra,&startup,sizeof(startup));
        MemoryContextSwitchTo(TopMemoryContext);
        if (!RegisterDynamicBackgroundWorker(&bgw,(BackgroundWorkerHandle **)&handle))
            ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                            errmsg("no background worker slot for connector")));
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if (handle)
            TerminateBackgroundWorker(handle);
        (void) synchdb_registry_cancel(sdb_state,run);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return handle;
}

void
synchdb_control_capture_spec(const char *name, const char *mode, SynchdbRunSpec *spec)
{
    ConnectionInfo connection={0};
    char *connector=NULL;

    if (ra_getConninfoByName(name,&connection,&connector)!=0)
        ereport(ERROR,(errcode(ERRCODE_UNDEFINED_OBJECT),errmsg("connector does not exist")));
    build_run_spec(spec,&connection,fc_get_connector_type(connector),mode);
}

void
synchdb_control_build_spec(const char *name, const char *mode, Jsonb *options,
                          int snapshot_engine, bool schema_sync, bool oracle_compatible,
                          SynchdbRunSpec *spec)
{
    ConnectionInfo connection={0};
    char *connector=NULL;

    if (ra_getConninfoByName(name,&connection,&connector)!=0)
        ereport(ERROR,(errcode(ERRCODE_UNDEFINED_OBJECT),errmsg("connector does not exist")));
    connection.snapengine=snapshot_engine;
    connection.isOraCompat=oracle_compatible;
    if (schema_sync)
        connection.flag |= CONNFLAG_SCHEMA_SYNC_MODE;
    synchdb_run_options_from_json(options,&connection.run_options);
    synchdb_run_spec_init(spec,fc_get_connector_type(connector),&connection,mode);
}

static int32
start_registered_worker(const SynchdbRunSpec *spec)
{
    return synchdb_control_receipt(synchdb_control_enqueue(spec->connection.name,"START",spec,NULL));
}

/*
 * synchdb_start_engine_bgw_snapshot_mode
 *
 * This function starts a connector with a custom snapshot mode
 */
Datum
synchdb_start_engine_bgw_snapshot_mode(PG_FUNCTION_ARGS)
{
	ConnectionInfo connInfo = {0};
	SynchdbRunSpec run;
	char *connector = NULL;
	int ret = -1;
	char * _snapshotMode = "initial";

	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);
	Name snapshotmode = PG_GETARG_NAME(1);

	ret = ra_getConninfoByName(NameStr(*name), &connInfo, &connector);
	if (ret)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("[synchdb_start_engine_bgw_snapshot_mode] connection name does not exist: %s", NameStr(*name)),
				 errhint("use synchdb_add_conninfo to add one first")));

	_snapshotMode = NameStr(*snapshotmode);
	if (!strcasecmp(_snapshotMode, "schemasync"))
	{
		_snapshotMode = "no_data";
		connInfo.flag |= CONNFLAG_SCHEMA_SYNC_MODE;
	}


#ifdef WITH_OLR
	/* check if OLR conninfo exists and warn user to add it as needed */
	if (fc_get_connector_type(connector) == TYPE_OLR &&
		(strlen(connInfo.olr.olr_host) == 0 ||
		strlen(connInfo.olr.olr_source) == 0 ||
		connInfo.olr.olr_port <= 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("OLR conninfo is missing"),
				 errhint("use synchdb_add_olr_conninfo to add it")));
	}
#endif
	build_run_spec(&run, &connInfo, fc_get_connector_type(connector), _snapshotMode);

	PG_RETURN_INT32(start_registered_worker(&run));
}

/*
 * synchdb_start_engine_bgw
 *
 * This function starts a connector with the default initial snapshot mode
 */
Datum
synchdb_start_engine_bgw(PG_FUNCTION_ARGS)
{
	ConnectionInfo connInfo = {0};
	SynchdbRunSpec run;
	char *connector = NULL;
	int ret = -1;
	/* By default, we use snapshot mode = initial */
	char * snapshotMode = "initial";

	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);

	ret = ra_getConninfoByName(NameStr(*name), &connInfo, &connector);
	if (ret)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("[synchdb_start_engine_bgw] connection name does not exist: %s", NameStr(*name)),
				 errhint("use synchdb_add_conninfo to add one first")));

#ifdef WITH_OLR
	/* check if OLR conninfo exists and warn user to add it as needed */
	if (fc_get_connector_type(connector) == TYPE_OLR &&
		(strlen(connInfo.olr.olr_host) == 0 ||
		strlen(connInfo.olr.olr_source) == 0 ||
		connInfo.olr.olr_port <= 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("OLR conninfo is missing"),
				 errhint("use synchdb_add_olr_conninfo to add it")));
	}
#endif
	build_run_spec(&run, &connInfo, fc_get_connector_type(connector), snapshotMode);

	PG_RETURN_INT32(start_registered_worker(&run));
}

/*
 * synchdb_stop_engine_bgw
 *
 * This function stops a running connector
 */
Datum
synchdb_stop_engine_bgw(PG_FUNCTION_ARGS)
{
    Name name=PG_GETARG_NAME(0);

    PG_RETURN_INT32(synchdb_control_receipt(synchdb_control_enqueue(NameStr(*name),"STOP",NULL,NULL)));
}

/*
 * synchdb_get_state
 *
 * This function dumps the states of all connectors
 */
/* Catalog identity resolution stays outside the runtime registry. */
static SynchdbControllerTarget
controller_target(const char *name, const SynchdbConnectorKey *known_key, bool running)
{
	SynchdbControllerTarget target = {0};
	SynchdbConnectorKey key;
	int64 revision;

	synchdb_init_shmem();
	if (!sdb_state)
		elog(ERROR, "could not attach to connector registry");
	if (known_key)
		key = *known_key;
	else if (!synchdb_connector_store_get_identity(name, &key, &revision))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("connector \"%s\" does not exist", name)));
	if (!synchdb_registry_capture(sdb_state, &key, running, &target))
	{
		if (target.run.slot < 0)
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("connector \"%s\" has no assigned worker slot", name)));
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("connector \"%s\" has no running worker", name)));
	}
	return target;
}

static const char *
controller_stage_name(ConnectorStage stage)
{
	switch (stage)
	{
		case STAGE_INITIAL_SNAPSHOT: return "initial snapshot";
		case STAGE_CHANGE_DATA_CAPTURE: return "change data capture";
		case STAGE_SCHEMA_SYNC: return "schema sync";
		default: return "unknown";
	}
}

static const char *
controller_state_name(const SynchdbRunView *entry)
{
	switch (entry->lifecycle)
	{
		case SYNCHDB_RUN_RESERVED: return "reserved";
		case SYNCHDB_RUN_RETRY_WAIT: return "retry_wait";
		case SYNCHDB_RUN_STOPPING: return "stopping";
		default: return connectorStateAsString(entry->state);
	}
}

Datum
synchdb_get_state(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int *idx = NULL;

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		funcctx->tuple_desc = synchdb_state_tupdesc();
		funcctx->user_fctx = palloc0(sizeof(int));
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	idx = (int *)funcctx->user_fctx;

	while (true)
	{
		Datum values[7];
		bool nulls[7] = {0};
		HeapTuple tuple;
		SynchdbRunView row;

		if (!synchdb_registry_next_view(sdb_state, MyDatabaseId, idx, &row))
			break;
		values[0] = CStringGetTextDatum(row.name);
		values[1] = CStringGetTextDatum(get_shm_connector_name(row.type));
		values[2] = Int32GetDatum((int)row.pid);
		values[3] = CStringGetTextDatum(controller_stage_name(row.stage));
		values[4] = CStringGetTextDatum(controller_state_name(&row));
		values[5] = CStringGetTextDatum(row.errmsg[0] ? row.errmsg : "no error");
		values[6] = CStringGetTextDatum(row.dbzoffset[0] ? row.dbzoffset : "no offset");

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	SRF_RETURN_DONE(funcctx);
}

/*
 * synchdb_get_stats
 *
 * This function dumps the statistics of all connectors
 */
Datum
synchdb_get_stats(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int *idx = NULL;

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		funcctx->tuple_desc = synchdb_stats_tupdesc();
		funcctx->user_fctx = palloc0(sizeof(int));
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	idx = (int *)funcctx->user_fctx;

	while (true)
	{
		Datum values[20];
		bool nulls[20] = {0};
		HeapTuple tuple;
		SynchdbRunView row;

		if (!synchdb_registry_next_view(sdb_state, MyDatabaseId, idx, &row))
			break;
		values[0] = CStringGetTextDatum(row.name);
		/* cdc stats */
		values[1] = Int64GetDatum(row.stats.cdcstats.stats_ddl);
		values[2] = Int64GetDatum(row.stats.cdcstats.stats_dml);
		values[3] = Int64GetDatum(row.stats.cdcstats.stats_create);
		values[4] = Int64GetDatum(row.stats.cdcstats.stats_update);
		values[5] = Int64GetDatum(row.stats.cdcstats.stats_delete);
		values[6] = Int64GetDatum(row.stats.cdcstats.stats_tx);
		values[7] = Int64GetDatum(row.stats.cdcstats.stats_truncate);

		/* general stats */
		values[8] = Int64GetDatum(row.stats.genstats.stats_bad_change_event);
		values[9] = Int64GetDatum(row.stats.genstats.stats_total_change_event);
		values[10] = Int64GetDatum(row.stats.genstats.stats_batch_completion);
		values[11] = row.stats.genstats.stats_batch_completion > 0?
					Int64GetDatum(row.stats.genstats.stats_total_change_event /
							row.stats.genstats.stats_batch_completion) :
					Int64GetDatum(0);
		values[12] = Int64GetDatum(row.stats.genstats.stats_first_src_ts);
		values[13] = Int64GetDatum(row.stats.genstats.stats_first_pg_ts);
		values[14] = Int64GetDatum(row.stats.genstats.stats_last_src_ts);
		values[15] = Int64GetDatum(row.stats.genstats.stats_last_pg_ts);

		/* snapshot stats */
		values[16] = Int64GetDatum(row.stats.snapstats.snapstats_tables);
		values[17] = Int64GetDatum(row.stats.snapstats.snapstats_rows);
		values[18] = Int64GetDatum(row.stats.snapstats.snapstats_begintime_ts);
		values[19] = Int64GetDatum(row.stats.snapstats.snapstats_endtime_ts);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	SRF_RETURN_DONE(funcctx);
}

/*
 * synchdb_reset_stats
 *
 * This function resets the statistics information of specified connector
 */
Datum
synchdb_reset_stats(PG_FUNCTION_ARGS)
{
    Name name=PG_GETARG_NAME(0);

    PG_RETURN_INT32(synchdb_control_receipt(synchdb_control_enqueue(NameStr(*name),"RESET_STATS",NULL,NULL)));
}

/*
 * synchdb_pause_engine
 *
 * This function pauses a running connector
 */
Datum
synchdb_pause_engine(PG_FUNCTION_ARGS)
{
    Name name=PG_GETARG_NAME(0);

    PG_RETURN_INT32(synchdb_control_receipt(synchdb_control_enqueue(NameStr(*name),"PAUSE",NULL,NULL)));
}

/*
 * synchdb_resume_engine
 *
 * This function resumes a paused connector
 */
Datum
synchdb_resume_engine(PG_FUNCTION_ARGS)
{
    Name name=PG_GETARG_NAME(0);

    PG_RETURN_INT32(synchdb_control_receipt(synchdb_control_enqueue(NameStr(*name),"RESUME",NULL,NULL)));
}

/*
 * synchdb_set_offset
 *
 * This function sets the given offset to debezium's offset file
 */
Datum
synchdb_set_offset(PG_FUNCTION_ARGS)
{
    Name name=PG_GETARG_NAME(0);
    char *offset=text_to_cstring(PG_GETARG_TEXT_PP(1));

    PG_RETURN_INT32(synchdb_control_receipt(synchdb_control_enqueue(NameStr(*name),"OFFSET",NULL,offset)));
}

/*
 * synchdb_add_conninfo
 *
 * This function adds a connector info to system
 */
Datum
synchdb_add_conninfo(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	text *hostname_text = PG_GETARG_TEXT_PP(1);
	unsigned int port = PG_GETARG_UINT32(2);
	text *user_text = PG_GETARG_TEXT_PP(3);
	text *pwd_text = PG_GETARG_TEXT_PP(4);
	text *src_db_text = PG_GETARG_TEXT_PP(5);
	text *src_schema_text = PG_GETARG_TEXT_PP(6);
	text *table_text = PG_GETARG_TEXT_PP(7);
	text *snapshottable_text = PG_GETARG_TEXT_PP(8);
	text *connector_text = PG_GETARG_TEXT_PP(9);
	char *connector = NULL;

	ConnectionInfo connInfo = {0};

	strlcpy(connInfo.name, NameStr(*name), SYNCHDB_CONNINFO_NAME_SIZE);

	/* Sanity check on input arguments */
	if (VARSIZE_ANY_EXHDR(hostname_text) == 0 ||
			VARSIZE_ANY_EXHDR(hostname_text) >= SYNCHDB_CONNINFO_HOSTNAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("hostname cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_HOSTNAME_SIZE - 1)));
	}
	strlcpy(connInfo.hostname, text_to_cstring(hostname_text), SYNCHDB_CONNINFO_HOSTNAME_SIZE);

	connInfo.port = port;
	if (connInfo.port == 0 || connInfo.port > 65535)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid port number")));
	}

	if (VARSIZE_ANY_EXHDR(user_text) == 0 ||
			VARSIZE_ANY_EXHDR(user_text) >= SYNCHDB_CONNINFO_USERNAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("username cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_USERNAME_SIZE - 1)));
	}
	strlcpy(connInfo.user, text_to_cstring(user_text), SYNCHDB_CONNINFO_USERNAME_SIZE);

	if (VARSIZE_ANY_EXHDR(pwd_text) == 0 ||
			VARSIZE_ANY_EXHDR(pwd_text) >= SYNCHDB_CONNINFO_PASSWORD_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("password cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_PASSWORD_SIZE - 1)));
	}
	strlcpy(connInfo.pwd, text_to_cstring(pwd_text), SYNCHDB_CONNINFO_PASSWORD_SIZE);

	if (VARSIZE_ANY_EXHDR(src_db_text) == 0 ||
			VARSIZE_ANY_EXHDR(src_db_text) >= SYNCHDB_CONNINFO_DB_NAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("source database cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_DB_NAME_SIZE - 1)));
	}
	strlcpy(connInfo.srcdb, text_to_cstring(src_db_text), SYNCHDB_CONNINFO_DB_NAME_SIZE);

	/* source schema can be empty or NULL but only with MySQL connector */
	if (VARSIZE_ANY_EXHDR(src_schema_text) == 0)
		strlcpy(connInfo.srcschema, "null", SYNCHDB_CONNINFO_DB_NAME_SIZE);
	else if (VARSIZE_ANY_EXHDR(src_schema_text) >= SYNCHDB_CONNINFO_DB_NAME_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("source schema cannot be longer than %d",
						 SYNCHDB_CONNINFO_DB_NAME_SIZE - 1)));
	else
		strlcpy(connInfo.srcschema, text_to_cstring(src_schema_text), SYNCHDB_CONNINFO_DB_NAME_SIZE);

	/* destination db is always set to the current database where synchdb is installed */
	strlcpy(connInfo.dstdb, get_database_name(MyDatabaseId), SYNCHDB_CONNINFO_DB_NAME_SIZE);

	/* table can be empty or NULL */
	if (VARSIZE_ANY_EXHDR(table_text) == 0)
		strlcpy(connInfo.table, "null", SYNCHDB_CONNINFO_TABLELIST_SIZE);
	else if (VARSIZE_ANY_EXHDR(table_text) >= SYNCHDB_CONNINFO_TABLELIST_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("table list cannot be longer than %d",
						 SYNCHDB_CONNINFO_TABLELIST_SIZE - 1)));
	else
		strlcpy(connInfo.table, text_to_cstring(table_text), SYNCHDB_CONNINFO_TABLELIST_SIZE);

	/* snapshot table can be empty or NULL */
	if (VARSIZE_ANY_EXHDR(snapshottable_text) == 0)
		strlcpy(connInfo.snapshottable, "null", SYNCHDB_CONNINFO_TABLELIST_SIZE);
	else if (VARSIZE_ANY_EXHDR(snapshottable_text) >= SYNCHDB_CONNINFO_TABLELIST_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("snapshot table cannot be longer than %d",
						 SYNCHDB_CONNINFO_TABLELIST_SIZE - 1)));
	else
		strlcpy(connInfo.snapshottable, text_to_cstring(snapshottable_text),
				SYNCHDB_CONNINFO_TABLELIST_SIZE);

	if (VARSIZE_ANY_EXHDR(connector_text) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connector type cannot be empty")));
	}
	connector = text_to_cstring(connector_text);

#ifdef WITH_OLR
	if (strcasecmp(connector, "mysql") && strcasecmp(connector, "sqlserver")
			&& strcasecmp(connector, "oracle") && strcasecmp(connector, "olr")
			&& strcasecmp(connector, "postgres"))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported connector")));
#else
	if (strcasecmp(connector, "mysql") && strcasecmp(connector, "sqlserver")
			&& strcasecmp(connector, "oracle" ) && strcasecmp(connector, "postgres"))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported connector")));
#endif

	if (strcasecmp(connector, "mysql") && !strcasecmp(connInfo.srcschema, "null"))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("schema cannot be null for creating a %s connector",
						 connector)));

	synchdb_connector_store_create(&connInfo, connector);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_restart_connector
 *
 * This function restarts a connector with given snapshot mode
 */
Datum
synchdb_restart_connector(PG_FUNCTION_ARGS)
{
    Name name=PG_GETARG_NAME(0);
    Name mode=PG_GETARG_NAME(1);
    SynchdbRunSpec spec;

    synchdb_control_capture_spec(NameStr(*name),NameStr(*mode)[0] ? NameStr(*mode) : "initial",&spec);
    PG_RETURN_INT32(synchdb_control_receipt(synchdb_control_enqueue(NameStr(*name),"RESTART",&spec,NULL)));
}

/*
 * synchdb_log_jvm_meminfo
 *
 * This function dumps JVM memory usage info in PostgreSQL log
 */
Datum
synchdb_log_jvm_meminfo(PG_FUNCTION_ARGS)
{
    Name name=PG_GETARG_NAME(0);

    PG_RETURN_INT32(synchdb_control_receipt(synchdb_control_enqueue(NameStr(*name),"MEMDUMP",NULL,NULL)));
}

Datum
synchdb_add_objmap_text(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	Name objtype = PG_GETARG_NAME(1);
	char *source = text_to_cstring(PG_GETARG_TEXT_PP(2));
	char *destination = text_to_cstring(PG_GETARG_TEXT_PP(3));

	(void) synchdb_mapping_store_upsert(NameStr(*name),
										 synchdb_mapping_kind(NameStr(*objtype)),
										 source, destination);
	PG_RETURN_INT32(0);
}

Datum
synchdb_add_objmap(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	Name objtype = PG_GETARG_NAME(1);
	Name srcobj = PG_GETARG_NAME(2);
	char *destination = text_to_cstring(PG_GETARG_TEXT_PP(3));

	(void) synchdb_mapping_store_upsert(NameStr(*name),
										 synchdb_mapping_kind(NameStr(*objtype)),
										 NameStr(*srcobj), destination);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_reload_objmap
 *
 * This function forces a connector to reload objmap table entries
 */
Datum
synchdb_reload_objmap(PG_FUNCTION_ARGS)
{
    Name name=PG_GETARG_NAME(0);

    PG_RETURN_INT32(synchdb_control_receipt(synchdb_control_enqueue(NameStr(*name),"RELOAD",NULL,NULL)));
}

/*
 * synchdb_add_extra_conninfo
 *
 * This function configures extra connector parameters and stores them to synchdb_conninfo table
 */
Datum
synchdb_add_extra_conninfo(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	Name sslmode = PG_GETARG_NAME(1);
	text * ssl_keystore_text = PG_GETARG_TEXT_PP(2);
	text * ssl_keystore_pass_text = PG_GETARG_TEXT_PP(3);
	text * ssl_truststore_text = PG_GETARG_TEXT_PP(4);
	text * ssl_truststore_pass_text = PG_GETARG_TEXT_PP(5);

	ExtraConnectionInfo extraconninfo = {0};

	strlcpy(extraconninfo.ssl_mode, NameStr(*sslmode), SYNCHDB_CONNINFO_NAME_SIZE);

	if (VARSIZE_ANY_EXHDR(ssl_keystore_text) == 0)
		strlcpy(extraconninfo.ssl_keystore, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
	else if (VARSIZE_ANY_EXHDR(ssl_keystore_text) >= SYNCHDB_CONNINFO_KEYSTORE_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_keystore cannot be longer than %d",
						 SYNCHDB_CONNINFO_KEYSTORE_SIZE - 1)));
	else
		strlcpy(extraconninfo.ssl_keystore, text_to_cstring(ssl_keystore_text), SYNCHDB_CONNINFO_KEYSTORE_SIZE);

	if (VARSIZE_ANY_EXHDR(ssl_keystore_pass_text) == 0)
		strlcpy(extraconninfo.ssl_keystore_pass, "null", SYNCHDB_CONNINFO_PASSWORD_SIZE);
	else if (VARSIZE_ANY_EXHDR(ssl_keystore_pass_text) >= SYNCHDB_CONNINFO_PASSWORD_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_keystore_pass cannot be longer than %d",
						 SYNCHDB_CONNINFO_PASSWORD_SIZE - 1)));
	else
		strlcpy(extraconninfo.ssl_keystore_pass, text_to_cstring(ssl_keystore_pass_text), SYNCHDB_CONNINFO_PASSWORD_SIZE);

	if (VARSIZE_ANY_EXHDR(ssl_truststore_text) == 0)
		strlcpy(extraconninfo.ssl_truststore, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
	else if (VARSIZE_ANY_EXHDR(ssl_truststore_text) >= SYNCHDB_CONNINFO_KEYSTORE_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_truststore cannot be longer than %d",
						 SYNCHDB_CONNINFO_KEYSTORE_SIZE - 1)));
	else
		strlcpy(extraconninfo.ssl_truststore, text_to_cstring(ssl_truststore_text), SYNCHDB_CONNINFO_KEYSTORE_SIZE);

	if (VARSIZE_ANY_EXHDR(ssl_truststore_pass_text) == 0)
		strlcpy(extraconninfo.ssl_truststore_pass, "null", SYNCHDB_CONNINFO_PASSWORD_SIZE);
	else if (VARSIZE_ANY_EXHDR(ssl_truststore_pass_text) >= SYNCHDB_CONNINFO_PASSWORD_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_truststore_pass cannot be longer than %d",
						 SYNCHDB_CONNINFO_PASSWORD_SIZE - 1)));
	else
		strlcpy(extraconninfo.ssl_truststore_pass, text_to_cstring(ssl_truststore_pass_text), SYNCHDB_CONNINFO_PASSWORD_SIZE);

	(void) synchdb_connector_store_set_extra(NameStr(*name), &extraconninfo);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_del_extra_conninfo
 *
 * This function deletes all extra connector parameters created by synchdb_add_extra_conninfo()
 */
Datum
synchdb_del_extra_conninfo(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);

	(void) synchdb_connector_store_clear_options(NameStr(*name),
			SYNCHDB_CONNECTOR_OPTIONS_EXTRA);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_add_fdw_conninfo
 *
 * Stores FDW TLS certificate file paths (client cert, private key, CA/root cert)
 * for connectors using FDW snapshot mode. These are PEM file paths consumed
 * directly by postgres_fdw / mysql_fdw. For oracle_fdw, ssl_rootcert is the
 * Oracle Wallet directory path. Distinct from synchdb_add_extra_conninfo which
 * configures Java Keystore/Truststore for the Debezium connector.
 */
Datum
synchdb_add_fdw_conninfo(PG_FUNCTION_ARGS)
{
	Name name                   = PG_GETARG_NAME(0);
	text *ssl_cert_text         = PG_GETARG_TEXT_PP(1);
	text *ssl_key_text          = PG_GETARG_TEXT_PP(2);
	text *ssl_rootcert_text     = PG_GETARG_TEXT_PP(3);
	text *ssl_cipher_text       = PG_GETARG_TEXT_PP(4);

	FdwConnectionInfo fdwssl = {0};

	if (VARSIZE_ANY_EXHDR(ssl_cert_text) == 0)
		strlcpy(fdwssl.ssl_cert, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
	else if (VARSIZE_ANY_EXHDR(ssl_cert_text) >= SYNCHDB_CONNINFO_KEYSTORE_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_cert path cannot be longer than %d",
						SYNCHDB_CONNINFO_KEYSTORE_SIZE - 1)));
	else
		strlcpy(fdwssl.ssl_cert, text_to_cstring(ssl_cert_text), SYNCHDB_CONNINFO_KEYSTORE_SIZE);

	if (VARSIZE_ANY_EXHDR(ssl_key_text) == 0)
		strlcpy(fdwssl.ssl_key, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
	else if (VARSIZE_ANY_EXHDR(ssl_key_text) >= SYNCHDB_CONNINFO_KEYSTORE_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_key path cannot be longer than %d",
						SYNCHDB_CONNINFO_KEYSTORE_SIZE - 1)));
	else
		strlcpy(fdwssl.ssl_key, text_to_cstring(ssl_key_text), SYNCHDB_CONNINFO_KEYSTORE_SIZE);

	if (VARSIZE_ANY_EXHDR(ssl_rootcert_text) == 0)
		strlcpy(fdwssl.ssl_rootcert, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
	else if (VARSIZE_ANY_EXHDR(ssl_rootcert_text) >= SYNCHDB_CONNINFO_KEYSTORE_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_rootcert path cannot be longer than %d",
						SYNCHDB_CONNINFO_KEYSTORE_SIZE - 1)));
	else
		strlcpy(fdwssl.ssl_rootcert, text_to_cstring(ssl_rootcert_text), SYNCHDB_CONNINFO_KEYSTORE_SIZE);

	if (VARSIZE_ANY_EXHDR(ssl_cipher_text) == 0)
		strlcpy(fdwssl.ssl_cipher, "null", SYNCHDB_CONNINFO_NAME_SIZE);
	else if (VARSIZE_ANY_EXHDR(ssl_cipher_text) >= SYNCHDB_CONNINFO_NAME_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_cipher cannot be longer than %d",
						SYNCHDB_CONNINFO_NAME_SIZE - 1)));
	else
		strlcpy(fdwssl.ssl_cipher, text_to_cstring(ssl_cipher_text), SYNCHDB_CONNINFO_NAME_SIZE);

	(void) synchdb_connector_store_set_fdw(NameStr(*name), &fdwssl);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_del_fdw_conninfo
 *
 * Deletes all FDW TLS certificate paths set by synchdb_add_fdw_conninfo().
 */
Datum
synchdb_del_fdw_conninfo(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);

	(void) synchdb_connector_store_clear_options(NameStr(*name),
			SYNCHDB_CONNECTOR_OPTIONS_FDW);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_add_jmx_conninfo
 *
 * This function configures extra JMX related parameters to a connector
 */
Datum
synchdb_add_jmx_conninfo(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);

	/* jmx and rmi server addresses and ports (for jconsole and similar) */
	text * jmx_listenaddr_text = PG_GETARG_TEXT_PP(1);
	unsigned int jmx_port = PG_GETARG_UINT32(2);
	text * jmx_rmiserver_text = PG_GETARG_TEXT_PP(3);
	unsigned int jmx_rmiport = PG_GETARG_UINT32(4);

	/* jmx authentication options */
	bool jmx_authenticate = PG_GETARG_BOOL(5);
	text * jmx_passwdfile_text = PG_GETARG_TEXT_PP(6);
	text * jmx_accessfile_text = PG_GETARG_TEXT_PP(7);

	/* jmx ssl options */
	bool jmx_ssl = PG_GETARG_BOOL(8);
	text * jmx_keystore_text = PG_GETARG_TEXT_PP(9);
	text * jmx_keystore_pass_text = PG_GETARG_TEXT_PP(10);
	text * jmx_truststore_text = PG_GETARG_TEXT_PP(11);
	text * jmx_truststore_pass_text = PG_GETARG_TEXT_PP(12);

	JMXConnectionInfo jmxconninfo = {0};

	if (VARSIZE_ANY_EXHDR(jmx_listenaddr_text) == 0 ||
			VARSIZE_ANY_EXHDR(jmx_listenaddr_text) >= SYNCHDB_CONNINFO_HOSTNAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jmx listen address cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_HOSTNAME_SIZE - 1)));
	}
	strlcpy(jmxconninfo.jmx_listenaddr, text_to_cstring(jmx_listenaddr_text), SYNCHDB_CONNINFO_HOSTNAME_SIZE);

	jmxconninfo.jmx_port = jmx_port;
	if (jmxconninfo.jmx_port == 0 || jmxconninfo.jmx_port > 65535)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid port number")));
	}

	if (VARSIZE_ANY_EXHDR(jmx_rmiserver_text) == 0 ||
			VARSIZE_ANY_EXHDR(jmx_rmiserver_text) >= SYNCHDB_CONNINFO_HOSTNAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jmx rmi server address cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_HOSTNAME_SIZE - 1)));
	}
	strlcpy(jmxconninfo.jmx_rmiserveraddr, text_to_cstring(jmx_rmiserver_text), SYNCHDB_CONNINFO_HOSTNAME_SIZE);

	jmxconninfo.jmx_rmiport = jmx_rmiport;
	if (jmxconninfo.jmx_rmiport == 0 || jmxconninfo.jmx_rmiport > 65535)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid port number")));
	}

	jmxconninfo.jmx_auth = jmx_authenticate;
	if (jmxconninfo.jmx_auth)
	{
		if (VARSIZE_ANY_EXHDR(jmx_passwdfile_text) == 0 ||
				VARSIZE_ANY_EXHDR(jmx_passwdfile_text) >= SYNCHDB_METADATA_PATH_SIZE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("jmx auth password file cannot be empty or longer than %d",
							 SYNCHDB_METADATA_PATH_SIZE - 1)));
		}
		strlcpy(jmxconninfo.jmx_auth_passwdfile, text_to_cstring(jmx_passwdfile_text), SYNCHDB_METADATA_PATH_SIZE);

		if (VARSIZE_ANY_EXHDR(jmx_accessfile_text) == 0 ||
				VARSIZE_ANY_EXHDR(jmx_accessfile_text) >= SYNCHDB_METADATA_PATH_SIZE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("jmx auth password file cannot be empty or longer than %d",
							 SYNCHDB_METADATA_PATH_SIZE - 1)));
		}
		strlcpy(jmxconninfo.jmx_auth_accessfile, text_to_cstring(jmx_accessfile_text), SYNCHDB_METADATA_PATH_SIZE);
	}
	else
	{
		strlcpy(jmxconninfo.jmx_auth_passwdfile, "null", SYNCHDB_METADATA_PATH_SIZE);
		strlcpy(jmxconninfo.jmx_auth_accessfile, "null", SYNCHDB_METADATA_PATH_SIZE);
	}

	jmxconninfo.jmx_ssl = jmx_ssl;
	if (jmxconninfo.jmx_ssl)
	{
		if (VARSIZE_ANY_EXHDR(jmx_keystore_text) == 0)
			strlcpy(jmxconninfo.jmx_ssl_keystore, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
		else if (VARSIZE_ANY_EXHDR(jmx_keystore_text) >= SYNCHDB_CONNINFO_KEYSTORE_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ssl_keystore cannot be longer than %d",
							 SYNCHDB_CONNINFO_KEYSTORE_SIZE - 1)));
		else
			strlcpy(jmxconninfo.jmx_ssl_keystore, text_to_cstring(jmx_keystore_text), SYNCHDB_CONNINFO_KEYSTORE_SIZE);

		if (VARSIZE_ANY_EXHDR(jmx_keystore_pass_text) == 0)
			strlcpy(jmxconninfo.jmx_ssl_keystore_pass, "null", SYNCHDB_CONNINFO_PASSWORD_SIZE);
		else if (VARSIZE_ANY_EXHDR(jmx_keystore_pass_text) >= SYNCHDB_CONNINFO_PASSWORD_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ssl_keystore_pass cannot be longer than %d",
							 SYNCHDB_CONNINFO_PASSWORD_SIZE - 1)));
		else
			strlcpy(jmxconninfo.jmx_ssl_keystore_pass, text_to_cstring(jmx_keystore_pass_text), SYNCHDB_CONNINFO_PASSWORD_SIZE);

		if (VARSIZE_ANY_EXHDR(jmx_truststore_text) == 0)
			strlcpy(jmxconninfo.jmx_ssl_truststore, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
		else if (VARSIZE_ANY_EXHDR(jmx_truststore_text) >= SYNCHDB_CONNINFO_KEYSTORE_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ssl_truststore cannot be longer than %d",
							 SYNCHDB_CONNINFO_KEYSTORE_SIZE - 1)));
		else
			strlcpy(jmxconninfo.jmx_ssl_truststore, text_to_cstring(jmx_truststore_text), SYNCHDB_CONNINFO_KEYSTORE_SIZE);

		if (VARSIZE_ANY_EXHDR(jmx_truststore_pass_text) == 0)
			strlcpy(jmxconninfo.jmx_ssl_truststore_pass, "null", SYNCHDB_CONNINFO_PASSWORD_SIZE);
		else if (VARSIZE_ANY_EXHDR(jmx_truststore_pass_text) >= SYNCHDB_CONNINFO_PASSWORD_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ssl_truststore_pass cannot be longer than %d",
							 SYNCHDB_CONNINFO_PASSWORD_SIZE - 1)));
		else
			strlcpy(jmxconninfo.jmx_ssl_truststore_pass, text_to_cstring(jmx_truststore_pass_text), SYNCHDB_CONNINFO_PASSWORD_SIZE);
	}
	else
	{
		strlcpy(jmxconninfo.jmx_ssl_keystore, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
		strlcpy(jmxconninfo.jmx_ssl_keystore_pass, "null", SYNCHDB_CONNINFO_PASSWORD_SIZE);
		strlcpy(jmxconninfo.jmx_ssl_truststore, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
		strlcpy(jmxconninfo.jmx_ssl_truststore_pass, "null", SYNCHDB_CONNINFO_PASSWORD_SIZE);
	}

	(void) synchdb_connector_store_set_jmx(NameStr(*name), &jmxconninfo);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_del_extra_conninfo
 *
 * This function deletes all extra connector parameters created by synchdb_add_extra_conninfo()
 */
Datum
synchdb_del_jmx_conninfo(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);

	(void) synchdb_connector_store_clear_options(NameStr(*name),
			SYNCHDB_CONNECTOR_OPTIONS_JMX);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_add_jmx_exporter_conninfo
 *
 * This function configures extra JMX exporter related parameters to a connector
 */
Datum
synchdb_add_jmx_exporter_conninfo(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);

	/* jmx exporter options (for prometheus and graphana) */
	text * jmx_exporterpath_text = PG_GETARG_TEXT_PP(1);
	unsigned int jmx_exporterport =  PG_GETARG_UINT32(2);
	text * jmx_exporterconf_text = PG_GETARG_TEXT_PP(3);

	JMXConnectionInfo jmxconninfo = {0};

	/* Sanity check on input arguments */
	if (VARSIZE_ANY_EXHDR(jmx_exporterpath_text) == 0 ||
			VARSIZE_ANY_EXHDR(jmx_exporterpath_text) >= SYNCHDB_METADATA_PATH_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jmx exporter jar library path cannot be empty or longer than %d",
						 SYNCHDB_METADATA_PATH_SIZE - 1)));
	}
	strlcpy(jmxconninfo.jmx_exporter, text_to_cstring(jmx_exporterpath_text),
			SYNCHDB_METADATA_PATH_SIZE);

	jmxconninfo.jmx_exporter_port = jmx_exporterport;
	if (jmxconninfo.jmx_exporter_port == 0 || jmxconninfo.jmx_exporter_port > 65535)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid jmx exporter port number")));
	}

	if (VARSIZE_ANY_EXHDR(jmx_exporterconf_text) == 0 ||
			VARSIZE_ANY_EXHDR(jmx_exporterconf_text) >= SYNCHDB_METADATA_PATH_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jmx exporter conf file path cannot be empty or longer than %d",
						 SYNCHDB_METADATA_PATH_SIZE - 1)));
	}
	strlcpy(jmxconninfo.jmx_exporter_conf, text_to_cstring(jmx_exporterconf_text),
			SYNCHDB_METADATA_PATH_SIZE);


	(void) synchdb_connector_store_set_jmx_exporter(NameStr(*name), &jmxconninfo);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_del_extra_conninfo
 *
 * This function deletes all extra connector parameters created by synchdb_add_extra_conninfo()
 */
Datum
synchdb_del_jmx_exporter_conninfo(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);

	(void) synchdb_connector_store_clear_options(NameStr(*name),
			SYNCHDB_CONNECTOR_OPTIONS_JMX_EXPORTER);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_add_olr_conninfo
 *
 * This function configures optional Openlog Replicator parameters to a Oracle
 * connector. Having this enables Openlog Replicator adapter instead of the default
 * Logminer.
 */
Datum
synchdb_add_olr_conninfo(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	text * olr_host_text = PG_GETARG_TEXT_PP(1);
	unsigned int olr_port = PG_GETARG_UINT32(2);
	text * olr_source_text = PG_GETARG_TEXT_PP(3);

	OLRConnectionInfo olrconninfo = {0};

	if (VARSIZE_ANY_EXHDR(olr_host_text) == 0 ||
			VARSIZE_ANY_EXHDR(olr_host_text) >= SYNCHDB_CONNINFO_HOSTNAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Openlog Replicator host address cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_HOSTNAME_SIZE - 1)));
	}
	strlcpy(olrconninfo.olr_host, text_to_cstring(olr_host_text), SYNCHDB_CONNINFO_HOSTNAME_SIZE);

	olrconninfo.olr_port = olr_port;
	if (olrconninfo.olr_port == 0 || olrconninfo.olr_port > 65535)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid port number")));
	}

	if (VARSIZE_ANY_EXHDR(olr_source_text) == 0 ||
			VARSIZE_ANY_EXHDR(olr_source_text) >= SYNCHDB_CONNINFO_NAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Openlog Replicator data source cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_NAME_SIZE - 1)));
	}
	strlcpy(olrconninfo.olr_source, text_to_cstring(olr_source_text), SYNCHDB_CONNINFO_NAME_SIZE);

	(void) synchdb_connector_store_set_olr(NameStr(*name), &olrconninfo);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_del_olr_conninfo
 *
 * This function deletes all Openlog Replicator parameters created by synchdb_add_olr_conninfo()
 */
Datum
synchdb_del_olr_conninfo(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);

	(void) synchdb_connector_store_clear_options(NameStr(*name),
			SYNCHDB_CONNECTOR_OPTIONS_OLR);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_del_conninfo
 *
 * This function deletes a connector info record created by synchdb_add_conninfo()
 */
Datum
synchdb_del_conninfo(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	SynchdbConnectorKey key;
	SynchdbCatalogFence fence;
	int64 revision;

	/* Without preload this instance cannot own a connector run. */
	sdb_state = synchdb_registry_try_attach(synchdb_max_connector_workers);
	if (!synchdb_connector_store_get_identity(NameStr(*name), &key, &revision))
		PG_RETURN_INT32(0);
	if (sdb_state != NULL)
		synchdb_registry_catalog_prepare(sdb_state, &key, NameStr(*name), &fence);
	synchdb_control_check_delete(&key);
	if (!synchdb_connector_store_delete_identity(NameStr(*name), &key))
		PG_RETURN_INT32(0);
	if (sdb_state != NULL && !synchdb_registry_catalog_finish(sdb_state, &fence, NULL, true))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
						errmsg("connector \"%s\" changed during deletion", NameStr(*name))));
	/* Recovery artifacts are retained; explicit offline purge owns deletion. */
	PG_RETURN_INT32(0);
}

/*
 * synchdb_del_objmap
 *
 * This function marks a objmap rule as disabled
 */
Datum
synchdb_del_objmap_text(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	Name objtype = PG_GETARG_NAME(1);
	char *source = text_to_cstring(PG_GETARG_TEXT_PP(2));

	(void) synchdb_mapping_store_disable(NameStr(*name),
										  synchdb_mapping_kind(NameStr(*objtype)),
										  source);
	PG_RETURN_INT32(0);
}

Datum
synchdb_del_objmap(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	Name objtype = PG_GETARG_NAME(1);
	Name srcobj = PG_GETARG_NAME(2);

	(void) synchdb_mapping_store_disable(NameStr(*name),
										  synchdb_mapping_kind(NameStr(*objtype)),
										  NameStr(*srcobj));
	PG_RETURN_INT32(0);
}

/*
 * synchdb_add_infinispan
 *
 * This function configures an existing oracle connector to use infinispan as
 * cache mechanism to support potentiall large transactions
 */
Datum
synchdb_add_infinispan(PG_FUNCTION_ARGS)
{
	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);
	Name memoryType = PG_GETARG_NAME(1);	/* heap or off heap*/
	int32 memorySize = PG_GETARG_INT32(2);

	IspnInfo ispninfo = {0};

	/* only embedded infinispan is supported as of now */
	strlcpy(ispninfo.ispn_cache_type, "embedded", INFINISPAN_TYPE_SIZE);

	if (strcasecmp(NameStr(*memoryType), "heap") && strcasecmp(NameStr(*memoryType), "off heap"))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid infinispan memory type: expect 'heap' or 'off heap'")));
	}
	strlcpy(ispninfo.ispn_memory_type, NameStr(*memoryType), INFINISPAN_TYPE_SIZE);

	if (memorySize <= 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid infinispan memory size")));
	}
	ispninfo.ispn_memory_size = memorySize;

	(void) synchdb_connector_store_set_ispn(NameStr(*name), &ispninfo);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_del_infinispan
 *
 * This function deletes infinispan configuration from an existing oracle connector.
 * This removes the configuration as well as the on-disk meta data and cached files.
 */
Datum
synchdb_del_infinispan(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	SynchdbConnectorKey key;
	SynchdbConnectorKey after_key;
	SynchdbCatalogFence fence;
	int64 revision;

	/* Without preload this instance cannot own a connector run. */
	sdb_state = synchdb_registry_try_attach(synchdb_max_connector_workers);
	if (!synchdb_connector_store_get_identity(NameStr(*name), &key, &revision))
		PG_RETURN_INT32(0);
	if (sdb_state != NULL)
		synchdb_registry_catalog_prepare(sdb_state, &key, NameStr(*name), &fence);
	if (!synchdb_connector_store_clear_options(NameStr(*name), SYNCHDB_CONNECTOR_OPTIONS_ISPN))
		PG_RETURN_INT32(0);
	if (!synchdb_connector_store_get_identity(NameStr(*name), &after_key, &revision) ||
		!synchdb_connector_key_equal(&key, &after_key))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
						errmsg("connector identity changed while removing Infinispan")));
	if (sdb_state != NULL && !synchdb_registry_catalog_finish(sdb_state, &fence,
										 worker.owner.attempt ? &worker.owner : NULL, false))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
						errmsg("connector run changed while removing Infinispan")));

	/* Removing configuration preserves recovery artifacts. */
	PG_RETURN_INT32(0);
}

/* FIXME: to bigints */
Datum
synchdb_translate_datatype(PG_FUNCTION_ARGS)
{
	Name type = PG_GETARG_NAME(0);
	Name ext_datatype = PG_GETARG_NAME(1);
	unsigned long long ext_datatype_len = PG_GETARG_INT64(2);
	unsigned long long ext_datatype_scale = PG_GETARG_INT64(3);
	unsigned long long ext_datatype_precision = PG_GETARG_INT64(4);
	char * pg_datatype = NULL;
	int pg_datatype_len = 0;
	StringInfoData strinfo;
	ConnectorType connectorType = TYPE_UNDEF;
	int len = 0, scale = 0, precision = 0;

	/*
	 * connectors like mysql via FDW sometimes will set length = maximum
	 * value of uint32 to indicate unbounded or variable length data type such as
	 * longtext, longblob...etc, but internally we use int type for datatype length,
	 * scale and precision so it would overflow. If this is the case, we will just
	 * -1 it because these types most likely will be translated to variable length
	 * data type in postgresql in which length is not required to be specified.
	 */
	if (ext_datatype_len >= SYNCHDB_MAX_INT32_VAL)
		len = -1;
	else
		len = (int)ext_datatype_len;

	if (ext_datatype_scale >= SYNCHDB_MAX_INT32_VAL)
		scale = -1;
	else
		scale = (int)ext_datatype_scale;

	if (ext_datatype_precision >= SYNCHDB_MAX_INT32_VAL)
		precision = -1;
	else
		precision = (int)ext_datatype_precision;

	initStringInfo(&strinfo);

	connectorType = stringToConnectorType(NameStr(*type));
	if (connectorType == TYPE_UNDEF)
	{
		elog(WARNING, "unsupported connector type %s", NameStr(*type));
		PG_RETURN_TEXT_P(cstring_to_text("text"));
	}

	/*
	 * precision shall replace len if valid while len = -1 similar
	 * to debezium's change event representations
	 */
	if (len <= 0 && precision > 0)
		len = precision;

	if (fc_translate_datatype(connectorType, NameStr(*ext_datatype),
			&len, &scale,
			&pg_datatype, &pg_datatype_len))
	{
		appendStringInfo(&strinfo, "%s", pg_datatype);

		/* pg_datatype_len == -1 means to use original length */
		if (pg_datatype_len == -1)
			pg_datatype_len = len;

		if (pg_datatype_len > 0 && scale > 0)
		{
			appendStringInfo(&strinfo, "(%d, %d)",
					pg_datatype_len, scale);
			elog(DEBUG1, "%s translates to %s", NameStr(*ext_datatype), strinfo.data);
			PG_RETURN_TEXT_P(cstring_to_text(strinfo.data));
		}

		if (pg_datatype_len > 0 && (scale == 0 || scale == -1))
		{
			appendStringInfo(&strinfo, "(%d)",
					pg_datatype_len);
			elog(DEBUG1, "%s translates to %s", NameStr(*ext_datatype), strinfo.data);
			PG_RETURN_TEXT_P(cstring_to_text(strinfo.data));
		}
		elog(DEBUG1, "%s translates to %s", NameStr(*ext_datatype), strinfo.data);
		PG_RETURN_TEXT_P(cstring_to_text(strinfo.data));
	}

	/*
	 * control comes here if no default mapping is found in hash lookup, we will leave
	 * the data type as is plus length and scale parameters if applicable
	 */
	elog(WARNING, "no data type mapping found for %s, returning original data type",
			NameStr(*ext_datatype));
	appendStringInfo(&strinfo, "%s", NameStr(*ext_datatype));

	if (len > 0 && scale > 0)
	{
		appendStringInfo(&strinfo, "(%d, %d)", len, scale);
		elog(DEBUG1, "%s translates to %s", NameStr(*ext_datatype), strinfo.data);
		PG_RETURN_TEXT_P(cstring_to_text(strinfo.data));
	}

	if (len > 0 && (scale == 0 || scale == -1))
	{
		appendStringInfo(&strinfo, "(%d)", len);
		elog(DEBUG1, "%s translates to %s", NameStr(*ext_datatype), strinfo.data);
		PG_RETURN_TEXT_P(cstring_to_text(strinfo.data));
	}
	elog(DEBUG1, "%s translates to %s", NameStr(*ext_datatype), strinfo.data);
	PG_RETURN_TEXT_P(cstring_to_text(strinfo.data));
}

Datum
synchdb_set_snapstats(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	int64 tables = PG_GETARG_INT64(1);
	int64 rows = PG_GETARG_INT64(2);
	int64 begin = PG_GETARG_INT64(3);
	int64 end = PG_GETARG_INT64(4);
	SynchdbControllerTarget target;

	/* Preserve parameter errors before catalog/worker lookup errors. */
	if (tables < 0 || rows < 0 || begin < 0 || end < 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("snapshot statistics must not be negative")));
	target = controller_target(NameStr(*name), NULL, false);
	synchdb_registry_add_snapstats(sdb_state, &target,
								   worker.owner.attempt ? &worker.owner : NULL,
								   tables, rows, begin, end);
	PG_RETURN_VOID();
}

/*
* synchdb_set_dbz_loglevel
*
* This function dynamically changes the Debezium log4j log level at runtime
* for the specified connector without requiring a restart.
*/
Datum
synchdb_set_dbz_loglevel(PG_FUNCTION_ARGS)
{
	SynchdbRequest request = {0};
	int level;
	text *level_text;
	char *level_str;

	Name name = PG_GETARG_NAME(0);
	level_text = PG_GETARG_TEXT_PP(1);
	level_str = text_to_cstring(level_text);

	/* Convert level string to integer */
	if (pg_strcasecmp(level_str, "all") == 0)
		level = LOG_LEVEL_ALL;
	else if (pg_strcasecmp(level_str, "debug") == 0)
		level = LOG_LEVEL_DEBUG;
	else if (pg_strcasecmp(level_str, "info") == 0)
		level = LOG_LEVEL_INFO;
	else if (pg_strcasecmp(level_str, "warn") == 0)
		level = LOG_LEVEL_WARN;
	else if (pg_strcasecmp(level_str, "error") == 0)
		level = LOG_LEVEL_ERROR;
	else if (pg_strcasecmp(level_str, "fatal") == 0)
		level = LOG_LEVEL_FATAL;
	else if (pg_strcasecmp(level_str, "off") == 0)
		level = LOG_LEVEL_OFF;
	else if (pg_strcasecmp(level_str, "trace") == 0)
		level = LOG_LEVEL_TRACE;
	else
		ereport(ERROR,
				(errmsg("invalid log level \"%s\"", level_str),
				errhint("Valid levels: all, trace, debug, info, warn, error, fatal, off")));

	request.reqstate = STATE_DBZ_LOGLEVEL_UPDATE;
	snprintf(request.reqdata, sizeof(request.reqdata), "%d", level);
	PG_RETURN_INT32(synchdb_control_receipt(synchdb_control_enqueue(NameStr(*name),"LOGLEVEL",NULL,request.reqdata)));
}
