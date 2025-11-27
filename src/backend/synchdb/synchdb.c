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
#include <jni.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <dlfcn.h>
#include "tcop/utility.h"
#include "nodes/parsenodes.h"
#include "utils/elog.h"

/* synchdb includes */
#include "converter/format_converter.h"
#include "converter/debezium_event_handler.h"
#include "synchdb/synchdb.h"
#include "executor/replication_agent.h"
#ifdef WITH_OLR
#include "olr/olr_client.h"
#endif

/* postgresql includes */
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/procsignal.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "storage/fd.h"
#include "miscadmin.h"
#include "utils/wait_event.h"
#include "utils/guc.h"
#include "varatt.h"
#include "funcapi.h"
#include "access/xact.h"
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
PG_FUNCTION_INFO_V1(synchdb_reload_objmap);
PG_FUNCTION_INFO_V1(synchdb_add_extra_conninfo);
PG_FUNCTION_INFO_V1(synchdb_del_extra_conninfo);
PG_FUNCTION_INFO_V1(synchdb_del_conninfo);
PG_FUNCTION_INFO_V1(synchdb_del_objmap);
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

/* Global variables */
SynchdbSharedState *sdb_state = NULL; /* Pointer to shared-memory state. */
int myConnectorId = -1;	/* Global index number to SynchdbSharedState in shared memory - global per worker */
const char * g_eventStr = NULL;	/* global pointer to the JSON event currently working on */

/* GUC variables */
int synchdb_worker_naptime = 10;
bool synchdb_dml_use_spi = false;
bool synchdb_auto_launcher = true;
int dbz_batch_size = 2048;
int dbz_queue_size = 8192;
char * dbz_skipped_operations = "t";
int dbz_connect_timeout_ms = 30000;
int dbz_query_timeout_ms = 600000;
int jvm_max_heap_size = 1024;
int jvm_max_direct_buffer_size = 1024;
int dbz_snapshot_thread_num = 2;
int dbz_snapshot_fetch_size = 0; /* 0: auto */
int dbz_snapshot_min_row_to_stream_results = 0; /* 0: always stream */
int dbz_incremental_snapshot_chunk_size = 2048;
char * dbz_incremental_snapshot_watermarking_strategy = "insert_insert";
int dbz_offset_flush_interval_ms = 60000;
bool dbz_capture_only_selected_table_ddl = true;
int synchdb_max_connector_workers = 30;
int synchdb_error_strategy = STRAT_EXIT_ON_ERROR;
int dbz_log_level = LOG_LEVEL_WARN;
bool synchdb_log_event_on_error = true;
int olr_read_buffer_size = 128;	/* in MB */
int dbz_logminer_stream_mode = LOGMINER_MODE_UNCOMMITTED;
int olr_connect_timeout_ms = 5000;
int olr_read_timeout_ms = 5000;
int synchdb_snapshot_engine = ENGINE_DEBEZIUM;
int cdc_start_delay_ms = 0;
bool synchdb_fdw_use_subtx = true;

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

/* JNI-related objects */
static JavaVM *jvm = NULL; /* represents java vm instance */
static JNIEnv *env = NULL; /* represents JNI run-time environment */
static jclass cls;		   /* represents debezium runner java class */
static jobject obj;		   /* represents debezium runner java class object */
static jmethodID getChangeEvents;
static jmethodID markBatchComplete;
static jmethodID getoffsets;

/* Function declarations */
PGDLLEXPORT void synchdb_engine_main(Datum main_arg);
PGDLLEXPORT void synchdb_auto_launcher_main(Datum main_arg);

/* Static function prototypes */
static int dbz_engine_stop(void);
static int dbz_engine_init(JNIEnv *env, jclass *cls, jobject *obj);
static int dbz_engine_get_change(JavaVM *jvm, JNIEnv *env, jclass *cls, jobject *obj, int myConnectorId, bool * dbzExitSignal,
		BatchInfo * batchinfo, SynchdbStatistics * myBatchStats, int flag);
static int dbz_engine_start(const ConnectionInfo *connInfo, ConnectorType connectorType, const char * snapshotMode);
static char *dbz_engine_get_offset(int connectorId);
static int dbz_mark_batch_complete(int batchid);
static TupleDesc synchdb_state_tupdesc(void);
static TupleDesc synchdb_stats_tupdesc(void);
static void synchdb_detach_shmem(int code, Datum arg);
static void prepare_bgw(BackgroundWorker *worker, const ConnectionInfo *connInfo, const char *connector, int connectorid, const char * snapshotMode);
static const char *connectorStateAsString(ConnectorState state);
static void reset_shm_request_state(int connectorId);
static int dbz_engine_set_offset(ConnectorType connectorType, char *db, char *offset, char *file);
static void processRequestInterrupt(ConnectionInfo *connInfo, ConnectorType type, int connectorId);
static void setup_environment(ConnectorType * connectorType, ConnectionInfo *conninfo, char ** snapshotMode);
static void initialize_jvm(JMXConnectionInfo * jmx);
static void start_debezium_engine(ConnectorType connectorType, const ConnectionInfo *connInfo, const char * snapshotMode);
static void main_loop(ConnectorType connectorType, ConnectionInfo *connInfo, char * snapshotMode);
static void cleanup(ConnectorType connectorType);
static void set_extra_dbz_parameters(jobject myParametersObj, jclass myParametersClass,
		const ExtraConnectionInfo * extraConnInfo, const OLRConnectionInfo * olrConnInfo,
		const IspnInfo * ispnInfo);
static void set_shm_connector_statistics(int connectorId, SynchdbStatistics * stats);
static void is_snapshot_cdc_needed(const char* snapshotMode, bool isSnapshotDone, bool * snapshot, bool * cdc);
#ifdef WITH_OLR
static void try_reconnect_olr(ConnectionInfo * connInfo);
static int olr_set_offset_from_raw(char * offsetdata);
#endif
static bool dbz_read_snapshot_state(ConnectorType type, const char * offset);
static int populate_debezium_metadata(const char * name, ConnectorType connectorType,
		orascn scn, const char * dstdb, const char * srcdb);
static bool has_running_connectors_for_db(const char *dbname, char *active_name, int active_len);
/*
 * count_active_connectors
 *
 * helper function to count number of active connectors
 *
 * @return: number of active connectors
 */
static int
count_active_connectors(void)
{
	int i = 0;

	for (i = 0; i < synchdb_max_connector_workers; i++)
	{
		/* if an empty name is found, there is no need to continue counting */
		if (strlen(sdb_state->connectors[i].conninfo.name) == 0)
			break;
	}
	return i;
}

/*
 * has_running_connectors_for_db
 *
 * Check if the current database still has running connectors before allowing
 * DROP EXTENSION to proceed, to prevent orphaned workers.
 */
static bool
has_running_connectors_for_db(const char *dbname, char *active_name, int active_len)
{
	int i;
	bool found = false;

	if (active_name && active_len > 0)
		active_name[0] = '\0';

	if (!sdb_state || !dbname)
		return false;

	LWLockAcquire(&sdb_state->lock, LW_SHARED);
	for (i = 0; i < synchdb_max_connector_workers; i++)
	{
		ActiveConnectors *entry = &sdb_state->connectors[i];

		if (entry->pid == InvalidPid)
			continue;
		if (entry->state == STATE_UNDEF || entry->state == STATE_STOPPED)
			continue;
		if (entry->conninfo.dstdb[0] == '\0')
			continue;
		if (pg_strcasecmp(entry->conninfo.dstdb, dbname) != 0)
			continue;

		found = true;
		if (active_name && active_len > 0)
			strlcpy(active_name, entry->conninfo.name, active_len);
		break;
	}
	LWLockRelease(&sdb_state->lock);

	return found;
}

/*
 * set_extra_dbz_parameters - configures extra paramters for Debezium runner
 *
 * This function builds myParametersObj with extra parameters to be passed
 * to the Debezium Java side.
 *
 * @return: void
 */
static void set_extra_dbz_parameters(jobject myParametersObj, jclass myParametersClass, const ExtraConnectionInfo * extraConnInfo,
		const OLRConnectionInfo * olrConnInfo, const IspnInfo * ispnInfo)
{
	jmethodID setBatchSize, setQueueSize, setSkippedOperations, setConnectTimeout, setQueryTimeout;
	jmethodID setSnapshotThreadNum, setSnapshotFetchSize, setSnapshotMinRowToStreamResults;
	jmethodID setIncrementalSnapshotChunkSize, setIncrementalSnapshotWatermarkingStrategy;
	jmethodID setOffsetFlushIntervalMs, setCaptureOnlySelectedTableDDL;
	jmethodID setSslmode, setSslKeystore, setSslKeystorePass, setSslTruststore, setSslTruststorePass;
	jmethodID setLogLevel, setOlr, setIspn, setLogminerStreamMode, setCdcDelay;
	jstring jdbz_skipped_operations, jdbz_watermarking_strategy;
	jstring jdbz_sslmode, jdbz_sslkeystore, jdbz_sslkeystorepass, jdbz_ssltruststore, jdbz_ssltruststorepass;
	jstring jolrHost, jolrSource;
	jstring jispnMemoryType, jispnCacheType;
	jstring jlogminerStreamMode;

	setBatchSize = (*env)->GetMethodID(env, myParametersClass, "setBatchSize",
			"(I)Lcom/example/DebeziumRunner$MyParameters;");
	if (setBatchSize)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setBatchSize, dbz_batch_size);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setBatchSize method");
		}
	}
	else
		elog(WARNING, "failed to find setBatchSize method");

	setQueueSize = (*env)->GetMethodID(env, myParametersClass, "setQueueSize",
			"(I)Lcom/example/DebeziumRunner$MyParameters;");
	if (setQueueSize)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setQueueSize, dbz_queue_size);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setQueueSize method");
		}
	}
	else
		elog(WARNING, "failed to find setQueueSize method");

	setConnectTimeout = (*env)->GetMethodID(env, myParametersClass, "setConnectTimeout",
			"(I)Lcom/example/DebeziumRunner$MyParameters;");
	if (setConnectTimeout)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setConnectTimeout, dbz_connect_timeout_ms);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setConnectTimeout method");
		}
	}
	else
		elog(WARNING, "failed to find setConnectTimeout method");

	setQueryTimeout = (*env)->GetMethodID(env, myParametersClass, "setQueryTimeout",
			"(I)Lcom/example/DebeziumRunner$MyParameters;");
	if (setQueryTimeout)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setQueryTimeout, dbz_query_timeout_ms);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setQueryTimeout method");
		}
	}
	else
		elog(WARNING, "failed to find setQueryTimeout method");

	jdbz_skipped_operations = (*env)->NewStringUTF(env, dbz_skipped_operations);

	setSkippedOperations = (*env)->GetMethodID(env, myParametersClass, "setSkippedOperations",
			"(Ljava/lang/String;)Lcom/example/DebeziumRunner$MyParameters;");
	if (setSkippedOperations)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setSkippedOperations, jdbz_skipped_operations);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setSkippedOperations method");
		}
	}
	else
		elog(WARNING, "failed to find setSkippedOperations method");

	if (jdbz_skipped_operations)
			(*env)->DeleteLocalRef(env, jdbz_skipped_operations);

	setSnapshotThreadNum = (*env)->GetMethodID(env, myParametersClass, "setSnapshotThreadNum",
			"(I)Lcom/example/DebeziumRunner$MyParameters;");
	if (setSnapshotThreadNum)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setSnapshotThreadNum, dbz_snapshot_thread_num);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setSnapshotThreadNum method");
		}
	}
	else
		elog(WARNING, "failed to find setSnapshotThreadNum method");

	setSnapshotFetchSize = (*env)->GetMethodID(env, myParametersClass, "setSnapshotFetchSize",
			"(I)Lcom/example/DebeziumRunner$MyParameters;");
	if (setSnapshotFetchSize)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setSnapshotFetchSize, dbz_snapshot_fetch_size);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setSnapshotFetchSize method");
		}
	}
	else
		elog(WARNING, "failed to find setSnapshotFetchSize method");

	setSnapshotMinRowToStreamResults = (*env)->GetMethodID(env, myParametersClass, "setSnapshotMinRowToStreamResults",
			"(I)Lcom/example/DebeziumRunner$MyParameters;");
	if (setSnapshotMinRowToStreamResults)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setSnapshotMinRowToStreamResults, dbz_snapshot_min_row_to_stream_results);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setSnapshotMinRowToStreamResults method");
		}
	}
	else
		elog(WARNING, "failed to find setSnapshotMinRowToStreamResults method");

	setIncrementalSnapshotChunkSize = (*env)->GetMethodID(env, myParametersClass, "setIncrementalSnapshotChunkSize",
			"(I)Lcom/example/DebeziumRunner$MyParameters;");
	if (setIncrementalSnapshotChunkSize)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setIncrementalSnapshotChunkSize, dbz_incremental_snapshot_chunk_size);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setIncrementalSnapshotChunkSize method");
		}
	}
	else
		elog(WARNING, "failed to find setIncrementalSnapshotChunkSize method");

	jdbz_watermarking_strategy = (*env)->NewStringUTF(env, dbz_incremental_snapshot_watermarking_strategy);

	setIncrementalSnapshotWatermarkingStrategy = (*env)->GetMethodID(env, myParametersClass, "setIncrementalSnapshotWatermarkingStrategy",
			"(Ljava/lang/String;)Lcom/example/DebeziumRunner$MyParameters;");
	if (setIncrementalSnapshotWatermarkingStrategy)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setIncrementalSnapshotWatermarkingStrategy, jdbz_watermarking_strategy);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setIncrementalSnapshotWatermarkingStrategy method");
		}
	}
	else
		elog(WARNING, "failed to find setIncrementalSnapshotWatermarkingStrategy method");

	if (jdbz_watermarking_strategy)
			(*env)->DeleteLocalRef(env, jdbz_watermarking_strategy);

	setOffsetFlushIntervalMs = (*env)->GetMethodID(env, myParametersClass, "setOffsetFlushIntervalMs",
			"(I)Lcom/example/DebeziumRunner$MyParameters;");
	if (setOffsetFlushIntervalMs)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setOffsetFlushIntervalMs, dbz_offset_flush_interval_ms);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setOffsetFlushIntervalMs method");
		}
	}
	else
		elog(WARNING, "failed to find setOffsetFlushIntervalMs method");

	setCaptureOnlySelectedTableDDL = (*env)->GetMethodID(env, myParametersClass, "setCaptureOnlySelectedTableDDL",
			"(Z)Lcom/example/DebeziumRunner$MyParameters;");
	if (setCaptureOnlySelectedTableDDL)
	{
		jboolean bval = dbz_capture_only_selected_table_ddl ? JNI_TRUE : JNI_FALSE;
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setCaptureOnlySelectedTableDDL, bval);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setCaptureOnlySelectedTableDDL method");
		}
	}
	else
		elog(WARNING, "failed to find setCaptureOnlySelectedTableDDL method");

	jdbz_watermarking_strategy = (*env)->NewStringUTF(env, dbz_incremental_snapshot_watermarking_strategy);

	setIncrementalSnapshotWatermarkingStrategy = (*env)->GetMethodID(env, myParametersClass, "setIncrementalSnapshotWatermarkingStrategy",
			"(Ljava/lang/String;)Lcom/example/DebeziumRunner$MyParameters;");
	if (setIncrementalSnapshotWatermarkingStrategy)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setIncrementalSnapshotWatermarkingStrategy, jdbz_watermarking_strategy);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setIncrementalSnapshotWatermarkingStrategy method");
		}
	}
	else
		elog(WARNING, "failed to find setIncrementalSnapshotWatermarkingStrategy method");

	if (jdbz_watermarking_strategy)
			(*env)->DeleteLocalRef(env, jdbz_watermarking_strategy);

	if (strcasecmp(extraConnInfo->ssl_mode, "null"))
	{
		jdbz_sslmode = (*env)->NewStringUTF(env, extraConnInfo->ssl_mode);

		setSslmode = (*env)->GetMethodID(env, myParametersClass, "setSslmode",
				"(Ljava/lang/String;)Lcom/example/DebeziumRunner$MyParameters;");
		if (setSslmode)
		{
			myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setSslmode, jdbz_sslmode);
			if (!myParametersObj)
			{
				elog(WARNING, "failed to call setSslmode method");
			}
		}
		else
			elog(WARNING, "failed to find setSslmode method");

		if (jdbz_sslmode)
				(*env)->DeleteLocalRef(env, jdbz_sslmode);
	}

	if (strcasecmp(extraConnInfo->ssl_keystore, "null"))
	{
		jdbz_sslkeystore = (*env)->NewStringUTF(env, extraConnInfo->ssl_keystore);

		setSslKeystore = (*env)->GetMethodID(env, myParametersClass, "setSslKeystore",
				"(Ljava/lang/String;)Lcom/example/DebeziumRunner$MyParameters;");
		if (setSslKeystore)
		{
			myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setSslKeystore, jdbz_sslkeystore);
			if (!myParametersObj)
			{
				elog(WARNING, "failed to call setSslKeystore method");
			}
		}
		else
			elog(WARNING, "failed to find setSslKeystore method");

		if (jdbz_sslkeystore)
				(*env)->DeleteLocalRef(env, jdbz_sslkeystore);
	}

	if (strcasecmp(extraConnInfo->ssl_keystore_pass, "null"))
	{
		jdbz_sslkeystorepass = (*env)->NewStringUTF(env, extraConnInfo->ssl_keystore_pass);

		setSslKeystorePass = (*env)->GetMethodID(env, myParametersClass, "setSslKeystorePass",
				"(Ljava/lang/String;)Lcom/example/DebeziumRunner$MyParameters;");
		if (setSslKeystorePass)
		{
			myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setSslKeystorePass, jdbz_sslkeystorepass);
			if (!myParametersObj)
			{
				elog(WARNING, "failed to call setSslKeystorePass method");
			}
		}
		else
			elog(WARNING, "failed to find setSslKeystorePass method");

		if (jdbz_sslkeystorepass)
				(*env)->DeleteLocalRef(env, jdbz_sslkeystorepass);
	}

	if (strcasecmp(extraConnInfo->ssl_truststore, "null"))
	{
		jdbz_ssltruststore = (*env)->NewStringUTF(env, extraConnInfo->ssl_truststore);

		setSslTruststore = (*env)->GetMethodID(env, myParametersClass, "setSslTruststore",
				"(Ljava/lang/String;)Lcom/example/DebeziumRunner$MyParameters;");
		if (setSslTruststore)
		{
			myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setSslTruststore, jdbz_ssltruststore);
			if (!myParametersObj)
			{
				elog(WARNING, "failed to call setSslTruststore method");
			}
		}
		else
			elog(WARNING, "failed to find setSslTruststore method");

		if (jdbz_ssltruststore)
				(*env)->DeleteLocalRef(env, jdbz_ssltruststore);
	}

	if (strcasecmp(extraConnInfo->ssl_truststore_pass, "null"))
	{
		jdbz_ssltruststorepass = (*env)->NewStringUTF(env, extraConnInfo->ssl_truststore_pass);

		setSslTruststorePass = (*env)->GetMethodID(env, myParametersClass, "setSslTruststorePass",
				"(Ljava/lang/String;)Lcom/example/DebeziumRunner$MyParameters;");
		if (setSslTruststorePass)
		{
			myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setSslTruststorePass, jdbz_ssltruststorepass);
			if (!myParametersObj)
			{
				elog(WARNING, "failed to call setSslTruststorePass method");
			}
		}
		else
			elog(WARNING, "failed to find setSslTruststorePass method");

		if (jdbz_ssltruststorepass)
				(*env)->DeleteLocalRef(env, jdbz_ssltruststorepass);
	}

	setLogLevel = (*env)->GetMethodID(env, myParametersClass, "setLogLevel",
			"(I)Lcom/example/DebeziumRunner$MyParameters;");
	if (setLogLevel)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setLogLevel, dbz_log_level);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setLogLevel method");
		}
	}
	else
		elog(WARNING, "failed to find setLogLevel method");

	/* Openlog Replicator Options for Oracle */
	if (strcasecmp(olrConnInfo->olr_host, "null") && olrConnInfo->olr_port != 0)
	{
		jolrHost = (*env)->NewStringUTF(env, olrConnInfo->olr_host);
		jolrSource = (*env)->NewStringUTF(env, olrConnInfo->olr_source);

		setOlr = (*env)->GetMethodID(env, myParametersClass, "setOlr",
				"(Ljava/lang/String;ILjava/lang/String;)Lcom/example/DebeziumRunner$MyParameters;");
		if (setOlr)
		{
			myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setOlr, jolrHost,
					olrConnInfo->olr_port, jolrSource);
			if (!myParametersObj)
			{
				elog(WARNING, "failed to call setOlr method");
			}
		}
		else
			elog(WARNING, "failed to find setOlr method");

		if (jolrHost)
				(*env)->DeleteLocalRef(env, jolrHost);
		if (jolrSource)
				(*env)->DeleteLocalRef(env, jolrSource);
	}

	/* Infinispan Options for Oracle */
	if (strcasecmp(ispnInfo->ispn_cache_type, "null"))
	{
		jispnCacheType = (*env)->NewStringUTF(env, ispnInfo->ispn_cache_type);
		jispnMemoryType = (*env)->NewStringUTF(env, ispnInfo->ispn_memory_type);

		setIspn = (*env)->GetMethodID(env, myParametersClass, "setIspn",
				"(Ljava/lang/String;Ljava/lang/String;I)Lcom/example/DebeziumRunner$MyParameters;");
		if (setIspn)
		{
			myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setIspn, jispnCacheType,
					jispnMemoryType, ispnInfo->ispn_memory_size);
			if (!myParametersObj)
			{
				elog(WARNING, "failed to call setIspn method");
			}
		}
		else
			elog(WARNING, "failed to find setIspn method");

		if (jispnCacheType)
				(*env)->DeleteLocalRef(env, jispnCacheType);
		if (jispnMemoryType)
				(*env)->DeleteLocalRef(env, jispnMemoryType);
	}

	/* Oracle logminer stream mode if defined */
	if (dbz_logminer_stream_mode != LOGMINER_MODE_UNDEF)
	{
		if (dbz_logminer_stream_mode == LOGMINER_MODE_UNCOMMITTED)
			jlogminerStreamMode = (*env)->NewStringUTF(env, "logminer");
		else if (dbz_logminer_stream_mode == LOGMINER_MODE_COMMITTED)
			/*
			 * XXX: current version of debezium 2.7.x does not yet support
			 * logminer in committed mode: "logminer_unbuffered", so we will
			 * default to "logminer" as of now. When we upgrade debezium to
			 * 3.2.x, we will fix this part accordingly. This can be considered
			 * as a placeholder for now.
			 */
			jlogminerStreamMode = (*env)->NewStringUTF(env, "logminer");
		else
			jlogminerStreamMode = (*env)->NewStringUTF(env, "logminer");

		setLogminerStreamMode = (*env)->GetMethodID(env, myParametersClass, "setLogminerStreamMode",
				"(Ljava/lang/String;)Lcom/example/DebeziumRunner$MyParameters;");
		if (setLogminerStreamMode)
		{
			myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setLogminerStreamMode, jlogminerStreamMode);
			if (!myParametersObj)
			{
				elog(WARNING, "failed to call setLogminerStreamMode method");
			}
		}
		else
			elog(WARNING, "failed to find setLogminerStreamMode method");

		if (jlogminerStreamMode)
			(*env)->DeleteLocalRef(env, jlogminerStreamMode);
	}

	setCdcDelay = (*env)->GetMethodID(env, myParametersClass, "setCdcDelay",
			"(I)Lcom/example/DebeziumRunner$MyParameters;");
	if (setCdcDelay)
	{
		myParametersObj = (*env)->CallObjectMethod(env, myParametersObj, setCdcDelay, cdc_start_delay_ms);
		if (!myParametersObj)
		{
			elog(WARNING, "failed to call setCdcDelay method");
		}
	}
	else
		elog(WARNING, "failed to find setCdcDelay method");
	/*
	 * additional parameters that we want to pass to Debezium on the java side
	 * will be added here, Make sure to add the matching methods in the MyParameters
	 * inner class inside DebeziumRunner class.
	 */
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
	jmethodID stopEngine;
	jthrowable exception;

	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
		return -1;
	}
	if (!env)
	{
		elog(WARNING, "jvm env not initialized");
		return -1;
	}

	/* Find the stopEngine method */
	stopEngine = (*env)->GetMethodID(env, cls, "stopEngine", "()V");
	if (stopEngine == NULL)
	{
		elog(WARNING, "Failed to find stopEngine method");
		return -1;
	}

	(*env)->CallVoidMethod(env, obj, stopEngine);

	/* Check for exceptions */
	exception = (*env)->ExceptionOccurred(env);
	if (exception)
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while stopping Debezium engine");
		return -1;
	}

	return 0;
}

/*
 * dbz_engine_init - Initialize the Debezium engine
 *
 * This function initializes the Debezium engine by finding the DebeziumRunner
 * class and allocating an instance of it. It handles JNI interactions and
 * exception checking.
 *
 * @param env: JNI environment pointer
 * @param cls: Pointer to store the found Java class
 * @param obj: Pointer to store the allocated Java object
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_init(JNIEnv *env, jclass *cls, jobject *obj)
{
	elog(DEBUG1, "dbz_engine_init - Starting initialization");

	/* Find the DebeziumRunner class */
	*cls = (*env)->FindClass(env, "com/example/DebeziumRunner");
	if (*cls == NULL)
	{
		if ((*env)->ExceptionCheck(env))
		{
			(*env)->ExceptionDescribe(env);
			(*env)->ExceptionClear(env);
		}
		elog(WARNING, "Failed to find com.example.DebeziumRunner class");
		return -1;
	}

	elog(DEBUG1, "dbz_engine_init - Class found, allocating object");

	/* Allocate an instance of the DebeziumRunner class */
	*obj = (*env)->AllocObject(env, *cls);
	if (*obj == NULL)
	{
		if ((*env)->ExceptionCheck(env))
		{
			(*env)->ExceptionDescribe(env);
			(*env)->ExceptionClear(env);
		}
		elog(WARNING, "Failed to allocate DBZ Runner object");
		return -1;
	}

	elog(DEBUG1, "dbz_engine_init - Object allocated successfully");

	return 0;
}

/*
 * dbz_engine_get_change - Retrieve and process change events from the Debezium engine
 *
 * This function retrieves change events from the Debezium engine and processes them.
 *
 * @param jvm: Pointer to the Java VM
 * @param env: Pointer to the JNI environment
 * @param cls: Pointer to the DebeziumRunner class
 * @param obj: Pointer to the DebeziumRunner object
 * @param myConnectorId: The connector ID of interest
 * @param dbzExitSignal: Set by this function to indicate the connector has exited
 * @param batchinfo: Set by this function to indicate a valid batch is in progress
 * @param myBatchStats: update connector statistics to this struct
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_get_change(JavaVM *jvm, JNIEnv *env, jclass *cls, jobject *obj, int myConnectorId,
		bool * dbzExitSignal, BatchInfo * batchinfo, SynchdbStatistics * myBatchStats,
		int flag)
{
	jobject jbytebuffer;
	int offset = 0;
	unsigned char * data;
	jsize datalen = 0;

	/* Validate input parameters */
	if (!jvm || !env || !cls || !obj)
	{
		elog(WARNING, "dbz_engine_get_change: Invalid input parameters");
		return -1;
	}

	/* Get the getChangeEvents method if needed */
	if (!getChangeEvents)
	{
		getChangeEvents = (*env)->GetMethodID(env, *cls, "getChangeEvents", "()Ljava/nio/ByteBuffer;");
		if (getChangeEvents == NULL)
		{
			elog(WARNING, "Failed to find getChangeEvents method");
			return -1;
		}
	}

	/* Call getChangeEvents method */
	jbytebuffer = (*env)->CallObjectMethod(env, *obj, getChangeEvents);
	if ((*env)->ExceptionCheck(env))
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while calling getChangeEvents");
		return -1;
	}
	if (jbytebuffer == NULL)
	{
		return -1;
	}

	data = (*env)->GetDirectBufferAddress(env, jbytebuffer);
	datalen = (*env)->GetDirectBufferCapacity(env, jbytebuffer);

	elog(DEBUG1, "datalen %d", datalen);

	if (data[0] == 'B')
	{
		int batchsize = 0;
		int curr = 0;

		offset += 1;
		memcpy(&(batchinfo->batchId), data + offset, 4);
		batchinfo->batchId =  ntohl(batchinfo->batchId);
		offset += 4;

		memcpy(&batchsize, data + offset, 4);
		batchsize =  ntohl(batchsize);
		offset += 4;

		elog(DEBUG1, "batch id %d contains %d change events", batchinfo->batchId, batchsize);

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		while (offset + 4 <= datalen && curr < batchsize)
		{
			int json_len = 0;
			memcpy(&json_len, data + offset, 4);
			offset += 4;
			json_len = ntohl(json_len);

			/* json_len includes the null terminator */
			elog(DEBUG1,"json len %d", json_len);
			if (offset + json_len > datalen)
			{
				elog(WARNING, "Invalid JSON length %d at offset %d (buffer length = %d)",
						json_len, offset, datalen);
				break;
			}
			if (json_len == 0)
			{
				/* should not happen as dbz also skips empty change events */
				elog(WARNING, "empty json skipped");
				offset += 4;
				curr++;
				continue;
			}

			/* data + offset is already null-terminated */
			if (synchdb_log_event_on_error)
				g_eventStr = (char *)(data + offset);

			fc_processDBZChangeEvent((char *)(data + offset), myBatchStats, flag,
					get_shm_connector_name_by_id(myConnectorId),
					(curr == 0), (curr == batchsize - 1));

			offset += json_len;
			curr++;
		}

		PopActiveSnapshot();
		CommitTransactionCommand();
		increment_connector_statistics(myBatchStats, STATS_TOTAL_CHANGE_EVENT, batchsize);
	}
	else if (data[0] == 'K')
	{
		int msg_len = 0;
		char * successflag = NULL, * dbzmsg = NULL;

		offset += 1;

		/* success flag length */
		memcpy(&msg_len, data + offset, 4);
		offset += 4;
		msg_len = ntohl(msg_len);

		/* success flag */
		successflag = palloc0(msg_len + 1);
		memcpy(successflag, data + offset, msg_len);
		offset += msg_len;

		/* dbz message length */
		memcpy(&msg_len, data + offset, 4);
		offset += 4;
		msg_len = ntohl(msg_len);

		/* dbz message */
		dbzmsg = palloc0(msg_len + 1);
		memcpy(dbzmsg, data + offset, msg_len);
		offset += msg_len;

		/* process the payload */
		if (successflag && strlen(successflag) > 0)
			*dbzExitSignal = true;

		if (dbzmsg && strlen(dbzmsg) > 0)
			set_shm_connector_errmsg(myConnectorId, dbzmsg);

		if (successflag)
			pfree(successflag);

		if (dbzmsg)
			pfree(dbzmsg);
	}
	else
	{
		elog(WARNING, "unknown change request");
	}

	(*env)->DeleteLocalRef(env, jbytebuffer);
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
dbz_engine_start(const ConnectionInfo *connInfo, ConnectorType connectorType, const char * snapshotMode)
{
	jmethodID mid, paramConstruct;
	jstring jHostname, jUser, jPassword, jDatabase, jTable, jSnapshotTable, jName, jSnapshot, jdstdb;
	jthrowable exception;
	jclass myParametersClass;
	jobject myParametersObj;

	elog(LOG, "dbz_engine_start: Starting dbz engine %s:%d ", connInfo->hostname, connInfo->port);
	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
		return -1;
	}

	if (!env)
	{
		elog(WARNING, "jvm env not initialized");
		return -1;
	}

	myParametersClass = (*env)->FindClass(env, "com/example/DebeziumRunner$MyParameters");
	if (!myParametersClass)
	{
		elog(WARNING, "failed to find MyParameters class");
		return -1;
	}

	paramConstruct = (*env)->GetMethodID(env, myParametersClass, "<init>",
			"(Lcom/example/DebeziumRunner;Ljava/lang/String;ILjava/lang/String;ILjava/lang/String;"
			"Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;"
			"Ljava/lang/String;Ljava/lang/String;)V");
	if (paramConstruct == NULL)
	{
		elog(WARNING, "failed to find myParameters Constructor");
		return -1;
	}

	/* prepare required parameters */
	jHostname = (*env)->NewStringUTF(env, connInfo->hostname);
	jUser = (*env)->NewStringUTF(env, connInfo->user);
	jPassword = (*env)->NewStringUTF(env, connInfo->pwd);
	jDatabase = (*env)->NewStringUTF(env, connInfo->srcdb);
	jTable = (*env)->NewStringUTF(env, connInfo->table);
	jSnapshotTable = (*env)->NewStringUTF(env, connInfo->snapshottable);
	jName = (*env)->NewStringUTF(env, connInfo->name);
	jSnapshot = (*env)->NewStringUTF(env, snapshotMode);
	jdstdb = (*env)->NewStringUTF(env, connInfo->dstdb);

	myParametersObj = (*env)->NewObject(env, myParametersClass, paramConstruct, obj,
			jName, connectorType, jHostname, connInfo->port, jUser, jPassword,
			jDatabase, jTable, jSnapshotTable, jSnapshot, jdstdb);
	if (!myParametersObj)
	{
		elog(WARNING, "failed to create MyParameters object");
		return -1;
	}

	/* set extra parameters */
	set_extra_dbz_parameters(myParametersObj, myParametersClass, &(connInfo->extra), &(connInfo->olr),
			&(connInfo->ispn));

	/* Find the startEngine method */
	mid = (*env)->GetMethodID(env, cls, "startEngine", "(Lcom/example/DebeziumRunner$MyParameters;)V");
	if (mid == NULL)
	{
		elog(WARNING, "Failed to find startEngine method");
		return -1;
	}

	/* Call the Java method */
	(*env)->CallVoidMethod(env, obj, mid, myParametersObj);

	/* Check for exceptions */
	exception = (*env)->ExceptionOccurred(env);
	if (exception)
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while starting Debezium engine");
		goto cleanup;
	}

	elog(LOG, "Debezium engine started successfully for %s connector", connectorTypeToString(connectorType));

cleanup:
	/* Clean up local references */
	if (jHostname)
		(*env)->DeleteLocalRef(env, jHostname);
	if (jUser)
		(*env)->DeleteLocalRef(env, jUser);
	if (jPassword)
		(*env)->DeleteLocalRef(env, jPassword);
	if (jDatabase)
		(*env)->DeleteLocalRef(env, jDatabase);
	if (jTable)
		(*env)->DeleteLocalRef(env, jTable);
	if (jName)
		(*env)->DeleteLocalRef(env, jName);
	if (jSnapshot)
		(*env)->DeleteLocalRef(env, jSnapshot);
	if (jdstdb)
		(*env)->DeleteLocalRef(env, jdstdb);

	return exception ? -1 : 0;
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
	jstring jdb, result, jName, jdstdb;
	char *resultStr = NULL;
	char *db = NULL, *name = NULL;
	char *dstdb = NULL;
	const char *tmp;
	jthrowable exception;

	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
		return NULL;
	}

	if (!env)
	{
		elog(WARNING, "jvm env not initialized");
		return NULL;
	}

	/* Get the source database name based on connector type */
	db = sdb_state->connectors[connectorId].conninfo.srcdb;
	if (!db)
	{
		elog(WARNING, "Source database name not set for connector type: %d",
				sdb_state->connectors[connectorId].type);
		return NULL;
	}

	/* Get the destination database name based on connector type */
	dstdb = sdb_state->connectors[connectorId].conninfo.dstdb;
	if (!dstdb)
	{
		elog(WARNING, "Destination database name not set for connector type: %d",
				sdb_state->connectors[connectorId].type);
		return NULL;
	}

	/* Get the unique name */
	name = sdb_state->connectors[connectorId].conninfo.name;
	if (!name)
	{
		elog(WARNING, "Unique name not set for connector type: %d",
				sdb_state->connectors[connectorId].type);
		return NULL;
	}

	if (!getoffsets)
	{
		getoffsets = (*env)->GetMethodID(env, cls, "getConnectorOffset",
										 "(ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;)"
										 "Ljava/lang/String;");
		if (getoffsets == NULL)
		{
			elog(WARNING, "Failed to find getConnectorOffset method");
			return NULL;
		}
	}

	jdb = (*env)->NewStringUTF(env, db);
	jName = (*env)->NewStringUTF(env, name);
	jdstdb = (*env)->NewStringUTF(env, dstdb);

	result = (jstring)(*env)->CallObjectMethod(env, obj, getoffsets,
			(int)sdb_state->connectors[connectorId].type, jdb, jName, jdstdb);
	/* Check for exceptions */
	exception = (*env)->ExceptionOccurred(env);
	if (exception)
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while getting connector offset");
		(*env)->DeleteLocalRef(env, jdb);
		(*env)->DeleteLocalRef(env, jName);
		return NULL;
	}

	/* Convert Java string to C string */
	tmp = (*env)->GetStringUTFChars(env, result, NULL);
	if (tmp && strlen(tmp) > 0)
		resultStr = pstrdup(tmp);
	else
		resultStr = pstrdup("no offset");

	/* Clean up */
	(*env)->ReleaseStringUTFChars(env, result, tmp);
	(*env)->DeleteLocalRef(env, jdb);
	(*env)->DeleteLocalRef(env, result);
	(*env)->DeleteLocalRef(env, jName);

	elog(DEBUG1, "Retrieved offset for %s connector: %s",
			connectorTypeToString(sdb_state->connectors[connectorId].type), resultStr);

	return resultStr;
}

/*
 * dbz_engine_memory_dump - Logs memory summary of JVM
 *
 * This function prints current heap and non-heap memory allocated
 * and used by the connector JVM.
 */
static void
dbz_engine_memory_dump(void)
{
	jmethodID jvmMemDump;

	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
		return;
	}

	if (!env)
	{
		elog(WARNING, "jvm env not initialized");
		return;
	}

	jvmMemDump = (*env)->GetMethodID(env, cls, "jvmMemDump", "()V");
	if (jvmMemDump == NULL)
	{
		elog(WARNING, "Failed to find jvmMemDump method");
		return;
	}

	(*env)->CallVoidMethod(env, obj, jvmMemDump);
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
	bool found;
	int i = 0;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	sdb_state = ShmemInitStruct("synchdb",
								sizeof(SynchdbSharedState),
								&found);
	if (!found)
	{
		/* First time through ... */
		LWLockInitialize(&sdb_state->lock, LWLockNewTrancheId());
	}
	sdb_state->connectors =
			ShmemInitStruct("synchdb_connectors",
							sizeof(ActiveConnectors) * synchdb_max_connector_workers,
							&found);
	if (!found)
	{
		for (i = 0; i < synchdb_max_connector_workers; i++)
		{
			sdb_state->connectors[i].pid = InvalidPid;
			sdb_state->connectors[i].state = STATE_UNDEF;
			sdb_state->connectors[i].type = TYPE_UNDEF;
		}
	}
	LWLockRelease(AddinShmemInitLock);
	LWLockRegisterTranche(sdb_state->lock.tranche, "synchdb");
}

/*
 * synchdb_detach_shmem - Detach from synchdb shared memory
 *
 * This function is responsible for detaching a process from the synchdb shared memory.
 * It updates the shared memory state if the current process is the one registered for
 * the specified connector type.
 *
 * @param code: An integer representing the exit code or reason for detachment
 * @param arg: A Datum containing the connector type as an unsigned integer
 */
static void
synchdb_detach_shmem(int code, Datum arg)
{
	pid_t enginepid;

	elog(LOG, "synchdb detach shm ... myConnectorId %d, code %d",
			DatumGetUInt32(arg), code);

	enginepid = get_shm_connector_pid(DatumGetUInt32(arg));
	if (enginepid == MyProcPid)
	{
		set_shm_connector_pid(DatumGetUInt32(arg), InvalidPid);
		set_shm_connector_state(DatumGetUInt32(arg), STATE_UNDEF);
	}

	cleanup(sdb_state->connectors[DatumGetUInt32(arg)].type);
}

/*
 * prepare_bgw - Prepare a background worker for synchdb
 *
 * This function sets up a BackgroundWorker structure with the appropriate
 * information based on the connector type and connection details.
 *
 * @param worker: Pointer to the BackgroundWorker structure to be prepared
 * @param connInfo: Pointer to the ConnectionInfo structure containing connection details
 * @param connector: String representing the connector type
 * @param connectorid: Connector ID of interest
 * @param snapshotMode: Snapshot mode to use to start Debezium engine
 */
static void
prepare_bgw(BackgroundWorker *worker, const ConnectionInfo *connInfo, const char *connector, int connectorid, const char * snapshotMode)

{
	const char * val = NULL;
	ConnectorType type = fc_get_connector_type(connector);

	worker->bgw_main_arg = UInt32GetDatum(connectorid);
	snprintf(worker->bgw_name, BGW_MAXLEN, "synchdb engine: %s@%s:%u", connector, connInfo->hostname, connInfo->port);
	snprintf(worker->bgw_type, BGW_MAXLEN, "synchdb engine: %s", connector);

	/* append destination database to worker->bgw_name for clarity */
	strcat(worker->bgw_name, " -> ");
	strcat(worker->bgw_name, connInfo->dstdb);

	/* [ivorysql] check if we are running under ivorysql's oracle compatible mode */
	val = GetConfigOption("ivorysql.compatible_mode", true, false);

	/*
	 * save connInfo to synchdb shared memory at index[connectorid]. When the connector
	 * worker starts, it will obtain the same connInfo from shared memory from the same
	 * index location
	 */
	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	sdb_state->connectors[connectorid].type = type;
	memset(sdb_state->connectors[connectorid].snapshotMode, 0, SYNCHDB_SNAPSHOT_MODE_SIZE);
	strcpy(sdb_state->connectors[connectorid].snapshotMode, snapshotMode);

	memset(&(sdb_state->connectors[connectorid].conninfo), 0, sizeof(ConnectionInfo));
	memcpy(&(sdb_state->connectors[connectorid].conninfo), connInfo, sizeof(ConnectionInfo));
	if (val && !strcasecmp(val, "oracle"))
		sdb_state->connectors[connectorid].conninfo.isOraCompat = true;
	LWLockRelease(&sdb_state->lock);
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
	if (!sdb_state)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	sdb_state->connectors[connectorId].req.reqstate = STATE_UNDEF;
	memset(sdb_state->connectors[connectorId].req.reqdata, 0, SYNCHDB_ERRMSG_SIZE);
	LWLockRelease(&sdb_state->lock);
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
	jmethodID setoffsets;
	jstring joffsetstr, jdb, jfile;
	jthrowable exception;

	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
		return -1;
	}

	if (!env)
	{
		elog(WARNING, "jvm env not initialized");
		return -1;
	}

	/* Find the setConnectorOffset method */
	setoffsets = (*env)->GetMethodID(env, cls, "setConnectorOffset",
									 "(Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;)V");
	if (setoffsets == NULL)
	{
		elog(WARNING, "Failed to find setConnectorOffset method");
		return -1;
	}

	/* Create Java strings from C strings */
	joffsetstr = (*env)->NewStringUTF(env, offset);
	jdb = (*env)->NewStringUTF(env, db);
	jfile = (*env)->NewStringUTF(env, file);

	/* Call the Java method */
	(*env)->CallVoidMethod(env, obj, setoffsets, jfile, (int)connectorType, jdb, joffsetstr);

	/* Check for exceptions */
	exception = (*env)->ExceptionOccurred(env);
	if (exception)
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while setting connector offset");
		return -1;
	}

	/* Clean up local references */
	(*env)->DeleteLocalRef(env, joffsetstr);
	(*env)->DeleteLocalRef(env, jdb);
	(*env)->DeleteLocalRef(env, jfile);

	elog(LOG, "Successfully set offset for %s connector", connectorTypeToString(connectorType));
	return 0;
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
static void
processRequestInterrupt(ConnectionInfo *connInfo, ConnectorType type, int connectorId)
{
	SynchdbRequest *req, *reqcopy;
	ConnectorState *currstate, *currstatecopy;
	char offsetfile[MAX_PATH_LENGTH] = {0};
	char *srcdb;
	int ret;

	if (!sdb_state)
		return;

	req = &(sdb_state->connectors[connectorId].req);
	currstate = &(sdb_state->connectors[connectorId].state);
	srcdb = sdb_state->connectors[connectorId].conninfo.srcdb;

	/* no requests, do nothing */
	if (req->reqstate == STATE_UNDEF)
		return;

	/*
	 * make a copy of requested state, its data and curr state to avoid holding locks
	 * for too long. Currently we support 1 request at any one time.
	 */
	reqcopy = palloc0(sizeof(SynchdbRequest));
	currstatecopy = palloc0(sizeof(ConnectorState));

	LWLockAcquire(&sdb_state->lock, LW_SHARED);
	memcpy(reqcopy, req, sizeof(SynchdbRequest));
	memcpy(currstatecopy, currstate, sizeof(ConnectorState));
	LWLockRelease(&sdb_state->lock);

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
				elog(WARNING, "failed to stop dbz engine...");
				reset_shm_request_state(connectorId);
				pfree(reqcopy);
				pfree(currstatecopy);
				return;
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
				ret = dbz_engine_start(connInfo, type, "initial");
				if (ret < 0)
				{
					elog(WARNING, "Failed to restart dbz engine");
					reset_shm_request_state(connectorId);
					pfree(reqcopy);
					pfree(currstatecopy);
					return;
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
					ret = dbz_engine_start(connInfo, type, "initial");
					if (ret < 0)
					{
						elog(WARNING, "Failed to restart dbz engine");
						reset_shm_request_state(connectorId);
						pfree(reqcopy);
						pfree(currstatecopy);
						return;
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
		{
			set_shm_connector_state(connectorId, STATE_OFFSET_UPDATE);
			ret = olr_set_offset_from_raw(reqcopy->reqdata);
			if (ret < 0)
			{
				elog(WARNING, "Failed to set offset for %s connector", connectorTypeToString(type));
				reset_shm_request_state(connectorId);
				set_shm_connector_state(connectorId, STATE_PAUSED);
				pfree(reqcopy);
				pfree(currstatecopy);
				return;
			}

			/* after new offset is set, change state back to STATE_PAUSED */
			set_shm_connector_state(connectorId, STATE_PAUSED);

			/* and also update this worker's shm offset */
			set_shm_dbz_offset(connectorId);
		}
		else
#endif
		{
			/* derive offset file*/
			snprintf(offsetfile, MAX_PATH_LENGTH, SYNCHDB_OFFSET_FILE_PATTERN,
					get_shm_connector_name(type), connInfo->name, connInfo->dstdb);

			set_shm_connector_state(connectorId, STATE_OFFSET_UPDATE);
			ret = dbz_engine_set_offset(type, srcdb, reqcopy->reqdata, offsetfile);
			if (ret < 0)
			{
				elog(WARNING, "Failed to set offset for %s connector", connectorTypeToString(type));
				reset_shm_request_state(connectorId);
				set_shm_connector_state(connectorId, STATE_PAUSED);
				pfree(reqcopy);
				pfree(currstatecopy);
				return;
			}

			/* after new offset is set, change state back to STATE_PAUSED */
			set_shm_connector_state(connectorId, STATE_PAUSED);

			/* and also update this worker's shm offset */
			set_shm_dbz_offset(connectorId);
		}
	}
	else if (reqcopy->reqstate == STATE_RESTARTING && *currstatecopy == STATE_SYNCING)
	{
		ConnectionInfo newConnInfo = {0};

		elog(WARNING, "got a restart request: %s", reqcopy->reqdata);
		set_shm_connector_state(connectorId, STATE_RESTARTING);

		/* get a copy of more recent conninfo from reqdata */
		memcpy(&newConnInfo, &(reqcopy->reqconninfo), sizeof(ConnectionInfo));

#ifdef WITH_OLR
		if (type == TYPE_OLR)
		{
			set_shm_connector_state(connectorId, STATE_RESTARTING);
			olr_client_shutdown();
			sleep(5);
			try_reconnect_olr(&newConnInfo);
			set_shm_connector_state(connectorId, STATE_SYNCING);
		}
		else
#endif
		{
			elog(WARNING, "stopping dbz engine...");
			ret = dbz_engine_stop();
			if (ret)
			{
				elog(WARNING, "failed to stop dbz engine...");
				reset_shm_request_state(connectorId);
				pfree(reqcopy);
				pfree(currstatecopy);
				set_shm_connector_state(connectorId, STATE_SYNCING);
				return;
			}
			sleep(1);

			elog(LOG, "resuimg dbz engine with host %s, port %u, user %s, src_db %s, "
					"dst_db %s, table %s, snapshotMode %s",
					newConnInfo.hostname, newConnInfo.port, newConnInfo.user,
					strlen(newConnInfo.srcdb) > 0 ? newConnInfo.srcdb : "N/A",
					newConnInfo.dstdb,
					strlen(newConnInfo.table) ? newConnInfo.table : "N/A",
					reqcopy->reqdata);

			elog(WARNING, "resuimg dbz engine with snapshot_mode %s...", reqcopy->reqdata);
			ret = dbz_engine_start(&newConnInfo, type, reqcopy->reqdata);
			if (ret < 0)
			{
				elog(WARNING, "Failed to restart dbz engine");
				reset_shm_request_state(connectorId);
				pfree(reqcopy);
				pfree(currstatecopy);
				set_shm_connector_state(connectorId, STATE_STOPPED);
				return;
			}
			set_shm_connector_state(connectorId, STATE_SYNCING);
		}
	}
	else if (reqcopy->reqstate == STATE_MEMDUMP)
	{
		ConnectorState oldstate = get_shm_connector_state_enum(connectorId);

		elog(LOG, "Requesting memdump for %s connector", connInfo->name);
		set_shm_connector_state(connectorId, STATE_MEMDUMP);
		dbz_engine_memory_dump();
		set_shm_connector_state(connectorId, oldstate);
	}
	else if (reqcopy->reqstate == STATE_RELOAD_OBJMAP)
	{
		ConnectorState oldstate = get_shm_connector_state_enum(connectorId);

		elog(LOG, "Reloading objmap for %s connector", connInfo->name);
		set_shm_connector_state(connectorId, STATE_RELOAD_OBJMAP);
		fc_load_objmap(connInfo->name, type);
		set_shm_connector_state(connectorId, oldstate);
	}
	else
	{
		/* unsupported request state combinations */
		elog(WARNING, "Invalid state transition requested for %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));
	}

	/* reset request state so we can receive more requests to process */
	reset_shm_request_state(connectorId);
	pfree(reqcopy);
	pfree(currstatecopy);
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
setup_environment(ConnectorType * connectorType, ConnectionInfo *conninfo, char ** snapshotMode)
{
	pid_t enginepid;

	/* Establish signal handlers */
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);

	/* Unblock signals to allow handling */
	BackgroundWorkerUnblockSignals();

	/* Initialize or attach to SynchDB shared memory and set cleanup handler */
	synchdb_init_shmem();
	on_shmem_exit(synchdb_detach_shmem, UInt32GetDatum(myConnectorId));

	/* Check if the worker is already running */
	enginepid = get_shm_connector_pid(myConnectorId);
	if (enginepid != InvalidPid)
		ereport(ERROR,
				(errmsg("synchdb worker is already running under PID %d",
						(int)enginepid)));

	/* read the connector type, conninfo and snapshot mode from synchdb shared memory */
	*connectorType = sdb_state->connectors[myConnectorId].type;
	*snapshotMode = pstrdup(sdb_state->connectors[myConnectorId].snapshotMode);
	memcpy(conninfo, &(sdb_state->connectors[myConnectorId].conninfo), sizeof(ConnectionInfo));

	/* Register this process as the worker for this connector type */
	set_shm_connector_pid(myConnectorId, MyProcPid);

	/* Connect to current database: NULL user - bootstrap superuser is used */
	BackgroundWorkerInitializeConnection(conninfo->dstdb, NULL, 0);

	/* [ivorysql] enable oracle compatible mode if specified */
	if (conninfo->isOraCompat)
	{
		SetConfigOption("ivorysql.compatible_mode", "oracle", PGC_USERSET, PGC_S_OVERRIDE);
		SetConfigOption("ivorysql.identifier_case_switch", "normal", PGC_USERSET, PGC_S_OVERRIDE);
		elog(LOG,"IvorySQL Oracle compatible mode enabled");
	}

	elog(LOG, "obtained conninfo from shm: myConnectorId %d, name %s, host %s, port %u, "
			"user %s, src_db %s, dst_db %s, table %s, snapshottable %s, connectorType %u (%s), conninfo_name %s"
			" snapshotMode %s",
			myConnectorId, conninfo->name,
			conninfo->hostname, conninfo->port, conninfo->user,
			strlen(conninfo->srcdb) > 0 ? conninfo->srcdb : "N/A",
			conninfo->dstdb,
			strlen(conninfo->table) > 0 ? conninfo->table : "N/A",
			strlen(conninfo->snapshottable) > 0 ? conninfo->snapshottable : "N/A",
			*connectorType, connectorTypeToString(*connectorType),
			conninfo->name, *snapshotMode);

	elog(LOG, "Environment setup completed for SynchDB %s worker (type %u)",
		 connectorTypeToString(*connectorType), *connectorType);
}

/*
 * initialize_jvm - Initialize the Java Virtual Machine and Debezium engine
 *
 * This function sets up the Java environment, locates the Debezium engine JAR file,
 * creates a Java VM, and initializes the Debezium engine.
 */
static void
initialize_jvm(JMXConnectionInfo * jmx)
{
	JavaVMInitArgs vm_args;
	JavaVMOption options[30];	/* ensure we do not exceed this max number of java options */
	char jar_path[MAX_PATH_LENGTH] = {0};
	const char *dbzpath = getenv("DBZ_ENGINE_DIR");
	MemoryContext jvmContext, oldContext;
	int ret, optcount = 0, i = 0;

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

	/* Configure JVM options */
	options[optcount++].optionString = psprintf("-Djava.class.path=%s", jar_path);
	options[optcount++].optionString = "-Xrs"; // Reduce use of OS signals by JVM
	options[optcount++].optionString = psprintf("-Xmx%dm", jvm_max_heap_size);
	options[optcount++].optionString = psprintf("-XX:MaxDirectMemorySize=%dm", jvm_max_direct_buffer_size);

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
		elog(LOG, "options[%d]=%s", i, options[i].optionString);

	vm_args.version = JNI_VERSION_10;
	vm_args.nOptions = optcount;
	vm_args.options = options;
	vm_args.ignoreUnrecognized = JNI_FALSE;

	/* Create the Java VM */
	ret = JNI_CreateJavaVM(&jvm, (void **)&env, &vm_args);
	if (ret < 0 || !env)
	{
		set_shm_connector_errmsg(myConnectorId, "Unable to Launch JVM");
		elog(ERROR, "Failed to create Java VM (return code: %d)", ret);
	}

	elog(INFO, "Java VM created successfully");

	/* Initialize the Debezium engine */
	ret = dbz_engine_init(env, &cls, &obj);
	if (ret < 0)
	{
		set_shm_connector_errmsg(myConnectorId, "Failed to initialize Debezium engine");
		elog(ERROR, "Failed to initialize Debezium engine");
	}

	elog(INFO, "Debezium engine initialized successfully");

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(jvmContext);
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
	int ret = dbz_engine_start(connInfo, connectorType, snapshotMode);
	if (ret < 0)
	{
		set_shm_connector_errmsg(myConnectorId, "Failed to start dbz engine");
		elog(ERROR, "Failed to start Debezium engine for connector type %d", connectorType);
	}

	set_shm_connector_state(myConnectorId, STATE_SYNCING);

	elog(LOG, "Debezium engine started successfully for %s:%d (connector type %d)",
		 connInfo->hostname, connInfo->port, connectorType);
}

/*
 * populate_debezium_metadata - Create debezium metadata file based on current conditions
 *
 * This function populates a schema history file by reading the schema_history_{} table
 * and an offset file derived from the input argument. (Oracle connector only for now).
 *
 * @param name: The name of the connector
 * @param connectorType: The type of connector
 * @param scn: the scn value to build the offset
 * @param dstdb: destination database name
 */
static int
populate_debezium_metadata(const char * name, ConnectorType connectorType,
		orascn scn, const char * dstdb, const char * srcdb)
{
	int ret = -1;
	jmethodID createoffsets;
	char * offsetstr = NULL;
	jstring joffsetstr, jdb, jfile;
	jthrowable exception;
	char * sql = NULL;
	char * offsetfile = psprintf(SYNCHDB_OFFSET_FILE_PATTERN,
			get_shm_connector_name(connectorType), name, dstdb);
	char * schemahistoryfile = psprintf(SYNCHDB_SCHEMA_FILE_PATTERN,
			get_shm_connector_name(connectorType), name, dstdb);

	if (connectorType != TYPE_ORACLE)
	{
		elog(WARNING, "only debezium oracle connector type can populate metadata (for now)");
		goto end;
	}

	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
		goto end;
	}

	if (!env)
	{
		elog(WARNING, "jvm env not initialized");
		goto end;
	}

	/* Find the createOffsetFile method */
	createoffsets = (*env)->GetMethodID(env, cls, "createOffsetFile",
									 "(Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;)V");
	if (createoffsets == NULL)
	{
		elog(WARNING, "Failed to find createoffsets method");
		goto end;
	}

	/* Create Java strings from C strings */
	offsetstr = psprintf("{\"commit_scn\":\"%llu\",\"snapshot_scn\":\"%llu\",\"scn\":\"%llu\"}", scn+1, scn, scn);
	joffsetstr = (*env)->NewStringUTF(env, offsetstr);
	jdb = (*env)->NewStringUTF(env, srcdb);
	jfile = (*env)->NewStringUTF(env, offsetfile);

	/* Call the Java method */
	(*env)->CallVoidMethod(env, obj, createoffsets, jfile, (int)connectorType, jdb, joffsetstr);

	/* Check for exceptions */
	exception = (*env)->ExceptionOccurred(env);
	if (exception)
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while creating connector offset");
		goto end;
	}

	/* populate schema history file */
	ret = dump_schema_history_to_file(name, schemahistoryfile);
	if (ret)
	{
		elog(WARNING, "Failed to populate schema history file %s",
				schemahistoryfile);
		return -1;
	}

	/* after schema history file is populated we can drop the source schema history table */
	sql = psprintf("DROP TABLE IF EXISTS schema_history_%s", name);
	ra_executeCommand(sql);
	pfree(sql);

	ret = 0;
end:
	if (offsetfile)
		pfree(offsetfile);

	if (schemahistoryfile)
		pfree(schemahistoryfile);

	pfree(offsetstr);
	(*env)->DeleteLocalRef(env, joffsetstr);
	(*env)->DeleteLocalRef(env, jdb);
	(*env)->DeleteLocalRef(env, jfile);

	return ret;
}

/*
 * dbz_read_snapshot_state
 *
 * This function determines if initial snapshot has been completed based
 * on offset information
 *
 * @param connectorType: The type of connector
 * @param offset: current offset of the connector
 * @param isSnapshotDone: whether or not snapshot has been done - set and returned as output
 */
static bool
dbz_read_snapshot_state(ConnectorType type, const char * offset)
{
	if (!offset)
		return false;

	switch(type)
	{
		case TYPE_ORACLE:
		{
			if (offset && (!strcasecmp(offset, "no offset") ||
					!strcasecmp(offset, "offset file not flushed yet")))
			{
				/* no offset available - assuming snapshot is not done */
				elog(WARNING, "offset file absent. Assuming snapshot is not done...");
				return false;
			}
			else
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
			}
			break;
		}
		case TYPE_MYSQL:
		case TYPE_SQLSERVER:
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
static void
main_loop(ConnectorType connectorType, ConnectionInfo *connInfo, char * snapshotMode)
{
	ConnectorState currstate;
	bool dbzExitSignal = false;
	BatchInfo myBatchInfo = {0};
	SynchdbStatistics myBatchStats = {0};
	orascn dbz_ora_resume_scn = 0;	/* used by FDW based snapshot for oracle connector */

	elog(LOG, "Main LOOP ENTER ");
	while (!ShutdownRequestPending)
	{
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (dbzExitSignal)
		{
			elog(WARNING, "dbz shutdown signal received. Exit now...");
			break;
		}

		CHECK_FOR_INTERRUPTS();

		processRequestInterrupt(connInfo, connectorType, myConnectorId);

		currstate = get_shm_connector_state_enum(myConnectorId);
		switch (currstate)
		{
			case STATE_SYNCING:
			{
				if (connectorType != TYPE_OLR)
				{
					if (((connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE) ||
						(connInfo->flag & CONNFLAG_INITIAL_SNAPSHOT_MODE)) &&
						connInfo->snapengine == ENGINE_FDW &&
						connectorType == TYPE_ORACLE)
					{
						/*
						 * FDW based snapshot and schema only sync processing logics here.
						 * Only Oracle connector is supported for now. May extend to support
						 * other connector types in the future.
						 */
						orascn scn_req = 0;
						orascn scn_res = 0;
						int ret = -1, ntables = 0;
						char * tbl_list = NULL;

						dbz_ora_resume_scn = 0;
						ret = ra_get_fdw_snapshot_err_table_list(connInfo->name, &tbl_list, &ntables, &scn_req);

					    if (ntables > 0 && tbl_list != NULL)
					    {
					    	elog(WARNING, "Retry on failed snapshot tables %s as of %llu for %s",
					    			tbl_list, scn_req, connInfo->name);
					    }

						if (connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE)
						{
							if (get_shm_connector_stage_enum(myConnectorId) != STAGE_SCHEMA_SYNC)
								set_shm_connector_stage(myConnectorId, STAGE_SCHEMA_SYNC);
						}
						else
						{
							if (get_shm_connector_stage_enum(myConnectorId) != STAGE_INITIAL_SNAPSHOT)
								set_shm_connector_stage(myConnectorId, STAGE_INITIAL_SNAPSHOT);
						}

						/*
						 * the fdw initial snapshot scripts are written according to pg parsing
						 * standard, so we need to temporarily change to pg compatible mode for
						 * the duration of the fdw snapshot
						 */
						if (connInfo->isOraCompat)
						{
							SetConfigOption("ivorysql.compatible_mode", "pg", PGC_USERSET, PGC_S_OVERRIDE);
						}

						/* invoke initial snapshot or schema sync PL/pgSQL workflow with schema history requested */
					    scn_res = ra_run_orafdw_initial_snapshot_spi(connInfo, connInfo->flag, tbl_list,
					    		scn_req, synchdb_fdw_use_subtx, true, snapshotMode);

					    if (connInfo->isOraCompat)
					    {
					    	SetConfigOption("ivorysql.compatible_mode", "oracle", PGC_USERSET, PGC_S_OVERRIDE);
					    }

						if (scn_res > 0)
						{
							/* initial snapshot is considered completed */
							elog(WARNING, "oracle_fdw based snapshot is done with scn %lld", scn_res);

							/* clean tbl_list if needed */
							if (tbl_list != NULL)
							{
								pfree(tbl_list);
								tbl_list = NULL;
							}

							ret = ra_get_fdw_snapshot_err_table_list(connInfo->name, &tbl_list, &ntables, &scn_req);
							if (ret != 0 || ntables == 0 || tbl_list == NULL)
							{
								/*
								 * all tables succeeded! remember the value of scn_res, this value is required to
								 * build debezium metadata to resume CDC after FDW snapshot has been done.
								 */
								dbz_ora_resume_scn = scn_res;

								/* change the state to STATE_SCHEMA_SYNC_DONE to handle the transition */
								set_shm_connector_state(myConnectorId, STATE_SCHEMA_SYNC_DONE);
							}
							else
							{
								/* some tables have failed... */
								elog(WARNING, "oracle_fdw based snapshot is done with errors: failed tables are: %s",
										tbl_list);

								set_shm_connector_state(myConnectorId, STATE_PAUSED);
								set_shm_connector_errmsg(myConnectorId, "some tables have failed the snapshot. "
										"Check synchdb_fdw_snapshot_errors_x tables for datails. "
										"Resume connector to try again");

								if (tbl_list != NULL)
									pfree(tbl_list);
							}
						}
						else
						{
							elog(WARNING, "oracle_fdw based snapshot is not successfully done");

							/* clean tbl_list if needed */
							if (tbl_list != NULL)
								pfree(tbl_list);

							set_shm_connector_state(myConnectorId, STATE_PAUSED);
							set_shm_connector_errmsg(myConnectorId, "oracle_fdw based snapshot "
									"is not successfully done - connector paused for troubleshooting. "
									"Send resume command to try again");
						}
					}
					else
					{
						/* Debezium based snapshot, schema and CDC processing logics here */

						myBatchInfo.batchId = SYNCHDB_INVALID_BATCH_ID;
						myBatchInfo.batchSize = 0;
						memset(&myBatchStats, 0, sizeof(myBatchStats));

						dbz_engine_get_change(jvm, env, &cls, &obj, myConnectorId, &dbzExitSignal,
								&myBatchInfo, &myBatchStats,
								connInfo->flag);

						/*
						 * if a valid batchid is set by dbz_engine_get_change(), it means we have
						 * successfully completed a batch change request and we shall notify dbz
						 * that it's been completed.
						 */
						if (myBatchInfo.batchId != SYNCHDB_INVALID_BATCH_ID)
						{
							dbz_mark_batch_complete(myBatchInfo.batchId);

							/* increment batch connector statistics */
							increment_connector_statistics(&myBatchStats, STATS_BATCH_COMPLETION, 1);

							/* update the batch statistics to shared memory */
							set_shm_connector_statistics(myConnectorId, &myBatchStats);

							/* update offset for displaying to user */
							set_shm_dbz_offset(myConnectorId);
						}
					}
				}
#ifdef WITH_OLR
				else
				{
					if ((connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE) ||
						(connInfo->flag & CONNFLAG_INITIAL_SNAPSHOT_MODE))
					{
						/* initial snapshot processing logics here */

						if (connInfo->snapengine == ENGINE_DEBEZIUM)
						{
							/* continuously poll changes from debezium engine */
							myBatchInfo.batchId = SYNCHDB_INVALID_BATCH_ID;
							myBatchInfo.batchSize = 0;
							memset(&myBatchStats, 0, sizeof(myBatchStats));

							dbz_engine_get_change(jvm, env, &cls, &obj, myConnectorId, &dbzExitSignal,
									&myBatchInfo, &myBatchStats, connInfo->flag);

							/*
							 * if a valid batchid is set by dbz_engine_get_change(), it means we have
							 * successfully completed a batch change request and we shall notify dbz
							 * that it's been completed.
							 */
							if (myBatchInfo.batchId != SYNCHDB_INVALID_BATCH_ID)
							{
								dbz_mark_batch_complete(myBatchInfo.batchId);

								/* increment batch connector statistics */
								increment_connector_statistics(&myBatchStats, STATS_BATCH_COMPLETION, 1);

								/* update the batch statistics to shared memory */
								set_shm_connector_statistics(myConnectorId, &myBatchStats);
							}
						}
						else if (connInfo->snapengine == ENGINE_FDW)
						{
							orascn scn_req = 0;
							orascn scn_res = 0;
							int ret = -1, ntables = 0;
							char * tbl_list = NULL;

							ret = ra_get_fdw_snapshot_err_table_list(connInfo->name, &tbl_list, &ntables, &scn_req);

						    if (ntables > 0 && tbl_list != NULL)
						    {
						    	elog(WARNING, "Retry on failed snapshot tables %s as of %llu for %s",
						    			tbl_list, scn_req, connInfo->name);
						    }

							if (connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE)
							{
								if (get_shm_connector_stage_enum(myConnectorId) != STAGE_SCHEMA_SYNC)
									set_shm_connector_stage(myConnectorId, STAGE_SCHEMA_SYNC);
							}
							else
							{
								if (get_shm_connector_stage_enum(myConnectorId) != STAGE_INITIAL_SNAPSHOT)
									set_shm_connector_stage(myConnectorId, STAGE_INITIAL_SNAPSHOT);
							}

							/*
							 * the fdw initial snapshot scripts are written according to pg parsing
							 * standard, so we need to temporarily change to pg compatible mode for
							 * the duration of the fdw snapshot
							 */
							if (connInfo->isOraCompat)
							{
								SetConfigOption("ivorysql.compatible_mode", "pg", PGC_USERSET, PGC_S_OVERRIDE);
							}

							/* invoke initial snapshot or schema sync PL/pgSQL workflow without schema history*/
						    scn_res = ra_run_orafdw_initial_snapshot_spi(connInfo, connInfo->flag, tbl_list,
						    		scn_req, synchdb_fdw_use_subtx, false, snapshotMode);

						    if (connInfo->isOraCompat)
						    {
								SetConfigOption("ivorysql.compatible_mode", "oracle", PGC_USERSET, PGC_S_OVERRIDE);
						    }

							if (scn_res > 0)
							{
								/* initial snapshot is considered completed */
								elog(WARNING, "oracle_fdw based snapshot is done with scn %lld", scn_res);

								/* clean tbl_list if needed */
								if (tbl_list != NULL)
								{
									pfree(tbl_list);
									tbl_list = NULL;
								}

								ret = ra_get_fdw_snapshot_err_table_list(connInfo->name, &tbl_list, &ntables, &scn_req);
								if (ret != 0 || ntables == 0 || tbl_list == NULL)
								{
									/* all tables succeeded, proceed to next step... */

									/* set scn to OLR client so it knows where to resume CDC */
									olr_client_set_scns(scn_res, scn_res, 0);

									/* change the state to STATE_SCHEMA_SYNC_DONE to handle the transition */
									set_shm_connector_state(myConnectorId, STATE_SCHEMA_SYNC_DONE);
								}
								else
								{
									/* some tables have failed... */
									elog(WARNING, "oracle_fdw based snapshot is done with errors: failed tables are: %s",
											tbl_list);

									set_shm_connector_state(myConnectorId, STATE_PAUSED);
									set_shm_connector_errmsg(myConnectorId, "some tables have failed the snapshot. "
											"Check synchdb_fdw_snapshot_errors_x tables for datails. "
											"Resume connector to try again");

									if (tbl_list != NULL)
										pfree(tbl_list);
								}
							}
							else
							{
								elog(WARNING, "oracle_fdw based snapshot is not successfully done");

								/* clean tbl_list if needed */
								if (tbl_list != NULL)
									pfree(tbl_list);

								set_shm_connector_state(myConnectorId, STATE_PAUSED);
								set_shm_connector_errmsg(myConnectorId, "oracle_fdw based snapshot "
										"is not successfully done - connector paused for troubleshooting. "
										"Send resume command to try again");
							}
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

								/*
								 * flush scn if needed - if a flush happens, we also set it to
								 * shared memory to display to user
								 */
								if (olr_client_write_scn_state(connectorType, connInfo->name,
										connInfo->dstdb, false))
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
				}
#else
				else
				{
					set_shm_connector_errmsg(myConnectorId, "OLR connector is not enabled in this synchdb build");
					elog(ERROR, "OLR connector is not enabled in this synchdb build");
				}
#endif
				break;
			}
			case STATE_PAUSED:
			{
				/* Do nothing when paused */
				break;
			}
			case STATE_SCHEMA_SYNC_DONE:
			{
#ifdef WITH_OLR
				if (connectorType == TYPE_OLR)
				{
					/* exit schema sync mode if set and enter pause state - takes precedence */
					if ((connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE))
					{
						connInfo->flag &= ~CONNFLAG_SCHEMA_SYNC_MODE;
						set_shm_connector_state(myConnectorId, STATE_PAUSED);

						/* remove the snapshot flag as well if set, so cdc can commence when resumed */
						if ((connInfo->flag & CONNFLAG_INITIAL_SNAPSHOT_MODE))
							connInfo->flag &= ~CONNFLAG_INITIAL_SNAPSHOT_MODE;

						/* set the stage to change data capture */
						set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
					}
					else
					{
						/* exit initial snapshot mode if set and resume syncing state */
						if ((connInfo->flag & CONNFLAG_INITIAL_SNAPSHOT_MODE))
						{
							connInfo->flag &= ~CONNFLAG_INITIAL_SNAPSHOT_MODE;
							set_shm_connector_state(myConnectorId, STATE_SYNCING);
						}

						/* set the stage to change data capture */
						set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
					}

					/* update schema history metadata file to indicate we have completed a snapshot */
					if (!olr_client_write_snapshot_state(connectorType, connInfo->name,
							connInfo->dstdb, true))
					{
						elog(WARNING, "failed to write snapshot state...");
					}

					/* shutdown debezium only if we use debezium engine for snapshot */
					if (connInfo->snapengine == ENGINE_DEBEZIUM)
					{
						elog(WARNING, "shut down dbz engine...");
						if (dbz_engine_stop())
						{
							elog(WARNING, "failed to stop dbz engine...");
						}

						/* destroy JVM */
						elog(WARNING, "destroying jvm...");
						(*jvm)->DetachCurrentThread(jvm);
						(*jvm)->DestroyJavaVM(jvm);

						/* destroy the data cache created during dbz based snapshot */
						fc_resetDataCache();
					}

					/* change snapshot mode back to normal */
					if (snapshotMode)
					{
						pfree(snapshotMode);
						snapshotMode = pstrdup("initial");
					}

					/* clear error messages */
					set_shm_connector_errmsg(myConnectorId, NULL);

					/*
					 * if cdc_start_delay_ms is set > 0, we need to delay here before the
					 * next iteration, which will begin the CDC process
					 */
					if (cdc_start_delay_ms > 0)
						usleep(cdc_start_delay_ms * 1000);
				}
				else
#endif
				{
					/* exit schema sync mode if set and enter pause state - takes precedence */
					if ((connInfo->flag & CONNFLAG_SCHEMA_SYNC_MODE))
					{
						connInfo->flag &= ~CONNFLAG_SCHEMA_SYNC_MODE;
						set_shm_connector_state(myConnectorId, STATE_PAUSED);

						/* remove the snapshot flag as well if set, so cdc can commence when resumed */
						if ((connInfo->flag & CONNFLAG_INITIAL_SNAPSHOT_MODE))
							connInfo->flag &= ~CONNFLAG_INITIAL_SNAPSHOT_MODE;

						/* set the stage to change data capture */
						set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);

						/*
						 * when schema sync is done, we will put connector into pause state so the user
						 * can review the table schema and attribute mappings before proceeding.
						 */
						elog(DEBUG1, "shut down dbz engine...");
						if (dbz_engine_stop())
						{
							elog(WARNING, "failed to stop dbz engine...");
						}

						/*
						 * if this round of schema sync is done via the FDW, we need to populate
						 * the metadata for debezium to resume CDC
						 */
						if (connInfo->snapengine == ENGINE_FDW &&
								connectorType == TYPE_ORACLE)
						{
							populate_debezium_metadata(connInfo->name, connectorType, dbz_ora_resume_scn,
														connInfo->dstdb, connInfo->srcdb);
						}
					}
					else
					{
						/* exit initial snapshot mode if set and resume syncing state */
						if ((connInfo->flag & CONNFLAG_INITIAL_SNAPSHOT_MODE))
						{
							connInfo->flag &= ~CONNFLAG_INITIAL_SNAPSHOT_MODE;
							set_shm_connector_state(myConnectorId, STATE_SYNCING);

							/* post fdw-snapshot: populate metadata for debezium to resume CDC */
							populate_debezium_metadata(connInfo->name, connectorType, dbz_ora_resume_scn,
									connInfo->dstdb, connInfo->srcdb);

							/* change snapshot mode back to normal */
							if (snapshotMode)
							{
								pfree(snapshotMode);
								snapshotMode = pstrdup("initial");
							}

							/* clear error messages */
							set_shm_connector_errmsg(myConnectorId, NULL);

							/*
							 * if cdc_start_delay_ms is set > 0, we need to delay here before the
							 * next iteration, which will begin the CDC process
							 */
							if (cdc_start_delay_ms > 0)
								usleep(cdc_start_delay_ms * 1000);

							/* start debezium now */
							start_debezium_engine(connectorType, connInfo, snapshotMode);
						}

						/* set the stage to change data capture */
						set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
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
}

/*
 * cleanup - Cleanup routine
 *
 * This function shuts down Debezium runner, JVM and cleans up format converter
 * resources
 *
 * @param connectorType: The type of connector being started
 */
static void
cleanup(ConnectorType connectorType)
{
	int ret;

	elog(WARNING, "synchdb_engine_main shutting down");

#ifdef WITH_OLR
	if (connectorType == TYPE_OLR)
	{
		/* force flush scn file before shutdown */
		elog(WARNING, "force flushing scn file prior to shutdown");
		olr_client_write_scn_state(connectorType,
				sdb_state->connectors[myConnectorId].conninfo.name,
				sdb_state->connectors[myConnectorId].conninfo.dstdb,
				true);
	}
	else
#endif
	{
		ret = dbz_engine_stop();
		if (ret)
		{
			elog(DEBUG1, "Failed to call dbz engine stop method");
		}

		if (jvm != NULL)
		{
			(*jvm)->DestroyJavaVM(jvm);
			jvm = NULL;
			env = NULL;
		}
	}
	fc_deinitFormatConverter(connectorType);
	fc_deinitDataCache();
}

/*
 * assign_connector_id - Requests a new connector ID
 *
 * This function returns a free connector ID associated with the given name
 *
 * @param name: The name of the connector to request a connector ID
 *
 * @return the connector ID associated with the given name, -1 if
 * no free connector ID is available.
 */
static int
assign_connector_id(const char * name, const char * dstdb)
{
	int i = 0;

	/*
	 * first, check if "name" has been used in one of the connector slots.
	 * If yes, return its index
	 */
	for (i = 0; i < synchdb_max_connector_workers; i++)
	{
		if (!strcasecmp(sdb_state->connectors[i].conninfo.name, name) &&
			!strcasecmp(sdb_state->connectors[i].conninfo.dstdb, dstdb))
		{
			return i;
		}
	}

	/* if not, find the next unnamed free slot */
	for (i = 0; i < synchdb_max_connector_workers; i++)
	{
		if (sdb_state->connectors[i].state == STATE_UNDEF &&
				strlen(sdb_state->connectors[i].conninfo.name) == 0 &&
				strlen(sdb_state->connectors[i].conninfo.dstdb) == 0)
		{
			return i;
		}
	}

	/* if not, find the next free slot */
	for (i = 0; i < synchdb_max_connector_workers; i++)
	{
		if (sdb_state->connectors[i].state == STATE_UNDEF)
		{
			return i;
		}
	}
	return -1;
}

/*
 * get_shm_connector_id_by_name - get connector ID from given name
 *
 * This function returns a connector ID associated with the given name
 *
 * @param name: The name of the connector to request a connector ID
 *
 * @return: The connector ID associated with the given name, -1 if
 * no free connector ID is available.
 */
static int
get_shm_connector_id_by_name(const char * name, const char * dstdb)
{
	int i = 0;

	if (!sdb_state)
		return -1;

	for (i = 0; i < synchdb_max_connector_workers; i++)
	{
		if (!strcmp(sdb_state->connectors[i].conninfo.name, name) &&
			!strcmp(sdb_state->connectors[i].conninfo.dstdb, dstdb))
		{
			return i;
		}
	}
	return -1;
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
	jthrowable exception;
	jboolean jmarkall = JNI_TRUE;

	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
		return -1;
	}

	if (!env)
	{
		elog(WARNING, "jvm env not initialized");
		return -1;
	}

	/* Find the markBatchComplete method if needed */
	if (!markBatchComplete)
	{
		markBatchComplete = (*env)->GetMethodID(env, cls, "markBatchComplete",
										 "(IZII)V");
		if (markBatchComplete == NULL)
		{
			elog(WARNING, "Failed to find markBatchComplete method");
			return -1;
		}
	}

	/* Call the Java method */
	(*env)->CallVoidMethod(env, obj, markBatchComplete, batchid, jmarkall, -1, -1);

	/* Check for exceptions */
	exception = (*env)->ExceptionOccurred(env);
	if (exception)
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while calling markBatchComplete");
		return -1;
	}
	return 0;
}

static void
remove_dbz_metadata_files(const char * name)
{
	struct dirent *entry;
	char filepath[SYNCHDB_METADATA_PATH_SIZE] = {0};
	char keyword[SYNCHDB_CONNINFO_NAME_SIZE + 2] = {0};

	DIR *dir = opendir(SYNCHDB_METADATA_DIR);
	if (!dir)
		elog(ERROR, "failed to open synchdb metadata dir %s : %m",
				SYNCHDB_METADATA_DIR);

	snprintf(keyword, SYNCHDB_CONNINFO_NAME_SIZE + 2, "_%s_", name);

	while ((entry = readdir(dir)) != NULL)
	{
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		if (strstr(entry->d_name, keyword) != NULL)
		{
			memset(filepath, 0, SYNCHDB_METADATA_PATH_SIZE);
			snprintf(filepath, SYNCHDB_METADATA_PATH_SIZE, "%s/%s",
					SYNCHDB_METADATA_DIR, entry->d_name);
			if (remove(filepath) != 0)
				elog(ERROR, "Failed to delete metadata file %s %m",
						filepath);
		}
	}
	closedir(dir);
}

/*
 * synchdb_auto_launcher_main - auto connector launcher main routine
 *
 * This is the main routine of auto connector launcher
 *
 * @param main_arg: not used
 */
void
synchdb_auto_launcher_main(Datum main_arg)
{
	int ret = -1, numout = 0, i = 0;
	char ** out;

	/* Establish signal handlers; once that's done, unblock signals. */
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();

	elog(DEBUG1, "start synchdb_auto_launcher_main");
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	/*
	 * todo: this auto launcher worker currently assumes that synchdb
	 * extension is created at the default postgres database. So it connects
	 * there and try to look up the entries in synchdb_conninfo table in
	 * public schema. If synchdb is created at another database or schema, then
	 * it would fail to look up the retries, thus not starting any connector
	 * workers.
	 */

	out = palloc0(sizeof(char *) * synchdb_max_connector_workers);
	ret = ra_listConnInfoNames(out, &numout);
	if (ret == 0)
	{
		for (i = 0; i < (numout > synchdb_max_connector_workers ?
				synchdb_max_connector_workers : numout); i++)
		{
			elog(WARNING, "launching %s...", out[i]);
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());

			DirectFunctionCall1(synchdb_start_engine_bgw, CStringGetTextDatum(out[i]));

			PopActiveSnapshot();
			CommitTransactionCommand();
			sleep(2);
		}
	}
	pfree(out);
	elog(DEBUG1, "stop synchdb_auto_launcher_main");
}

/*
 * synchdb_start_leader_worker
 *
 * Helper function to start a auto connector launcher background worker
 */
static void
synchdb_start_leader_worker(void)
{
	BackgroundWorker worker;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy(worker.bgw_library_name, "synchdb");
	strcpy(worker.bgw_function_name, "synchdb_auto_launcher_main");
	strcpy(worker.bgw_name, "synchdb auto launcher");
	strcpy(worker.bgw_type, "synchdb auto launcher");

	RegisterBackgroundWorker(&worker);
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
	if (!sdb_state)
		return InvalidPid;

	return sdb_state->connectors[connectorId].pid;
}

/*
 * set_shm_connector_pid
 *
 * This function sets pid of the given connectorID
 *
 * @param connectorId: Connector ID of interest
 * @param pid: New pid value
 */
void
set_shm_connector_pid(int connectorId, pid_t pid)
{
	if (!sdb_state)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	sdb_state->connectors[connectorId].pid = pid;
	LWLockRelease(&sdb_state->lock);
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
	if (!sdb_state)
		return "no error";

	return (sdb_state->connectors[connectorId].errmsg[0] != '\0') ?
			sdb_state->connectors[connectorId].errmsg : "no error";
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
	if (!sdb_state)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	strlcpy(sdb_state->connectors[connectorId].errmsg, err ? err : "", SYNCHDB_ERRMSG_SIZE);
	LWLockRelease(&sdb_state->lock);
}

/*
 * get_shm_connector_stage - Get the current connector stage
 *
 * This function gets current connector stage based on the change request received.
 *
 * @param connectorId: Connector ID of interest
 *
 * @return connector stage in string
 */
static const char *
get_shm_connector_stage(int connectorId)
{
	ConnectorStage stage;

	if (!sdb_state)
		return "unknown";

	/*
	 * We're only reading, so shared lock is sufficient.
	 * This ensures thread-safety without blocking other readers.
	 */
	LWLockAcquire(&sdb_state->lock, LW_SHARED);
	stage = sdb_state->connectors[connectorId].stage;
	LWLockRelease(&sdb_state->lock);

	switch(stage)
	{
		case STAGE_INITIAL_SNAPSHOT:
		{
			return "initial snapshot";
			break;
		}
		case STAGE_CHANGE_DATA_CAPTURE:
		{
			return "change data capture";
			break;
		}
		case STAGE_SCHEMA_SYNC:
		{
			return "schema sync";
			break;
		}
		case STAGE_UNDEF:
		default:
		{
			break;
		}
	}
	return "unknown";
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
	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	/* CDC stats */
	sdb_state->connectors[connectorId].stats.cdcstats.stats_ddl +=
			stats->cdcstats.stats_ddl;
	sdb_state->connectors[connectorId].stats.cdcstats.stats_dml +=
			stats->cdcstats.stats_dml;
	sdb_state->connectors[connectorId].stats.cdcstats.stats_create +=
			stats->cdcstats.stats_create;
	sdb_state->connectors[connectorId].stats.cdcstats.stats_update +=
			stats->cdcstats.stats_update;
	sdb_state->connectors[connectorId].stats.cdcstats.stats_delete +=
			stats->cdcstats.stats_delete;
	sdb_state->connectors[connectorId].stats.cdcstats.stats_tx +=
			stats->cdcstats.stats_tx;
	sdb_state->connectors[connectorId].stats.cdcstats.stats_truncate +=
			stats->cdcstats.stats_truncate;

	/* General stats */
	sdb_state->connectors[connectorId].stats.genstats.stats_bad_change_event +=
			stats->genstats.stats_bad_change_event;
	sdb_state->connectors[connectorId].stats.genstats.stats_total_change_event +=
			stats->genstats.stats_total_change_event;
	sdb_state->connectors[connectorId].stats.genstats.stats_batch_completion +=
			stats->genstats.stats_batch_completion;
	/* the following should be overwritten \n */
	sdb_state->connectors[connectorId].stats.genstats.stats_first_src_ts =
			stats->genstats.stats_first_src_ts;
	sdb_state->connectors[connectorId].stats.genstats.stats_first_pg_ts =
			stats->genstats.stats_first_pg_ts;
	sdb_state->connectors[connectorId].stats.genstats.stats_last_src_ts =
			stats->genstats.stats_last_src_ts;
	sdb_state->connectors[connectorId].stats.genstats.stats_last_pg_ts =
			stats->genstats.stats_last_pg_ts;

	/* Snapshot stats */
	sdb_state->connectors[connectorId].stats.snapstats.snapstats_tables +=
			stats->snapstats.snapstats_tables;
	sdb_state->connectors[connectorId].stats.snapstats.snapstats_rows +=
			stats->snapstats.snapstats_rows;

	/* the following should be overwritten \n */
	if (stats->snapstats.snapstats_begintime_ts > 0)
	{
		sdb_state->connectors[connectorId].stats.snapstats.snapstats_begintime_ts =
				stats->snapstats.snapstats_begintime_ts;
		/*
		 * when begintime_ts is set, we assume it is the beginning of a snapshot, so
		 * we set endtime_ts to 0 to indicate a fresh start.
		 */
		sdb_state->connectors[connectorId].stats.snapstats.snapstats_endtime_ts = 0;
	}
	if (stats->snapstats.snapstats_endtime_ts > 0)
		sdb_state->connectors[connectorId].stats.snapstats.snapstats_endtime_ts =
				stats->snapstats.snapstats_endtime_ts;
	LWLockRelease(&sdb_state->lock);
}

static void
set_shm_connector_snapshot_statistics(int connectorId, SnapshotStatistics * snapstats)
{
	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	sdb_state->connectors[connectorId].stats.snapstats.snapstats_tables +=
				snapstats->snapstats_tables;
	sdb_state->connectors[connectorId].stats.snapstats.snapstats_rows +=
				snapstats->snapstats_rows;
	if (snapstats->snapstats_begintime_ts > 0)
	{
		sdb_state->connectors[connectorId].stats.snapstats.snapstats_begintime_ts =
				snapstats->snapstats_begintime_ts;
		/*
		 * when begintime_ts is set, we assume it is the beginning of a snapshot, so
		 * we set endtime_ts to 0 to indicate a fresh start.
		 */
		sdb_state->connectors[connectorId].stats.snapstats.snapstats_endtime_ts = 0;
	}
	if (snapstats->snapstats_endtime_ts > 0)
		sdb_state->connectors[connectorId].stats.snapstats.snapstats_endtime_ts =
				snapstats->snapstats_endtime_ts;
	LWLockRelease(&sdb_state->lock);
}

static void
is_snapshot_cdc_needed(const char* snapshotMode, bool isSnapshotDone, bool * snapshot, bool * cdc)
{
	if (!strcasecmp(snapshotMode, "initial"))
	{
		*snapshot = isSnapshotDone ? false : true;
		*cdc = true;
	}
	else if (!strcasecmp(snapshotMode, "initial_only"))
	{
		*snapshot = isSnapshotDone ? false : true;
		*cdc = false;
	}
	else if (!strcasecmp(snapshotMode, "no_data"))
	{
		*snapshot = isSnapshotDone ? false : true;
		*cdc = true;
	}
	else if (!strcasecmp(snapshotMode, "always"))
	{
		*snapshot = true;
		*cdc = true;
	}
	else if (!strcasecmp(snapshotMode, "never"))
	{
		*snapshot = false;
		*cdc = true;
	}
	else
	{
		elog(WARNING, "snapshot mode %s not handled for OLR connector. Default to initial",
				snapshotMode);
		*snapshot = isSnapshotDone ? false : true;
		*cdc = true;
	}
}
#ifdef WITH_OLR
static void
try_reconnect_olr(ConnectionInfo * connInfo)
{
	int ret = -1;

	if (olr_client_get_connect_status())
	{
		elog(WARNING, "already connected.");
		return;
	}

	elog(WARNING, "reconnecting...");
	if (olr_client_init(connInfo->olr.olr_host, connInfo->olr.olr_port))
		elog(DEBUG1, "failed to reconnect");
	else
	{
		elog(WARNING, "reconnected, request replication...");

		/* connInfo.srcdb is used as data source for OLR */
		ret = olr_client_start_or_cont_replication(connInfo->olr.olr_source, true);
		if (ret == -1)
		{
			set_shm_connector_errmsg(myConnectorId, "failed to start replication with olr server");
			elog(ERROR, "failed to start replication with olr server");
		}

		if (ret == RES_ALREADY_STARTED || ret == RES_STARTING)
		{
			elog(WARNING, "replication already started or starting - sending continue");
			ret = olr_client_start_or_cont_replication(connInfo->olr.olr_source, false);
			if (ret == -1)
			{
				set_shm_connector_errmsg(myConnectorId, "failed to start replication with olr server");
				elog(ERROR, "failed to start replication with olr server");
			}
		}
		if (ret == RES_REPLICATE)
			elog(WARNING, "replication requested...");
		else
		{
			set_shm_connector_errmsg(myConnectorId, "openlog replicator is not ready to replicate after reconnection");
			elog(ERROR, "openlog replicator is not ready to replicate, response code = %d", ret);
		}
	}
}

static int
olr_set_offset_from_raw(char * offsetdata)
{
	orascn scn = 0, c_scn = 0, c_idx = 0;
	const char *scn_pos = strstr(offsetdata, "\"scn\":");
	const char *c_scn_pos = strstr(offsetdata, "\"c_scn\":");
	const char *c_idx_pos = strstr(offsetdata, "\"c_idx\":");

	if (scn_pos)
	{
		sscanf(scn_pos, "\"scn\":%llu", &scn);
	}
	else
	{
		elog(WARNING, "bad offset string: scn missing...");
		return -1;
	}
	if (c_scn_pos)
	{
		sscanf(c_scn_pos, "\"c_scn\":%llu", &c_scn);
	}
	else
	{
		elog(WARNING, "bad offset string: c_scn missing...");
		return -1;
	}
	if (c_idx_pos)
	{
		sscanf(c_scn_pos, "\"c_idx\":%llu", &c_idx);
	}
	else
	{
		elog(WARNING, "bad offset string: c_idx missing...");
		return -1;
	}
	elog(WARNING, "new offset: scn:%llu c_scn:%llu c_idx:%llu",
			scn, c_scn, c_idx);
	olr_client_set_scns(scn, c_scn, c_idx);
	return 0;
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
                        synchdb_init_shmem();

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
	ConnectorType type;

	if (!sdb_state)
		return TYPE_UNDEF;

	/*
	 * We're only reading, so shared lock is sufficient.
	 * This ensures thread-safety without blocking other readers.
	 */
	LWLockAcquire(&sdb_state->lock, LW_SHARED);
	type = sdb_state->connectors[connectorId].type;
	LWLockRelease(&sdb_state->lock);

	return type;
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
	ConnectorStage stage;

	if (!sdb_state)
		return STAGE_UNDEF;

	/*
	 * We're only reading, so shared lock is sufficient.
	 * This ensures thread-safety without blocking other readers.
	 */
	LWLockAcquire(&sdb_state->lock, LW_SHARED);
	stage = sdb_state->connectors[connectorId].stage;
	LWLockRelease(&sdb_state->lock);

	return stage;
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
	if (!sdb_state)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	sdb_state->connectors[connectorId].stage = stage;
	LWLockRelease(&sdb_state->lock);
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
	ConnectorState state;

	if (!sdb_state)
		return "stopped";

	/*
	 * We're only reading, so shared lock is sufficient.
	 * This ensures thread-safety without blocking other readers.
	 */
	LWLockAcquire(&sdb_state->lock, LW_SHARED);
	state = sdb_state->connectors[connectorId].state;
	LWLockRelease(&sdb_state->lock);

	return connectorStateAsString(state);
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
	ConnectorState state;

	if (!sdb_state)
		return STATE_UNDEF;

	/*
	 * We're only reading, so shared lock is sufficient.
	 * This ensures thread-safety without blocking other readers.
	 */
	LWLockAcquire(&sdb_state->lock, LW_SHARED);
	state = sdb_state->connectors[connectorId].state;
	LWLockRelease(&sdb_state->lock);

	return state;
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
	if (!sdb_state)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	sdb_state->connectors[connectorId].state = state;
	LWLockRelease(&sdb_state->lock);
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

	if (!sdb_state)
		return;

#ifdef WITH_OLR
	if (sdb_state->connectors[connectorId].type == TYPE_OLR)
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

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	strlcpy(sdb_state->connectors[connectorId].dbzoffset, offset, SYNCHDB_ERRMSG_SIZE);
	LWLockRelease(&sdb_state->lock);

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
	if (!sdb_state)
		return "n/a";

	return (sdb_state->connectors[connectorId].dbzoffset[0] != '\0') ?
			sdb_state->connectors[connectorId].dbzoffset : "no offset";
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
	if (!sdb_state)
		return "n/a";

	return (sdb_state->connectors[connectorId].conninfo.name[0] != '\0') ?
			sdb_state->connectors[connectorId].conninfo.name : "no name";
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
	if (!sdb_state)
		return "n/a";

	return (sdb_state->connectors[connectorId].conninfo.user[0] != '\0') ?
			sdb_state->connectors[connectorId].conninfo.user : "no user";
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
	if (!sdb_state)
		return false;

	return sdb_state->connectors[connectorId].conninfo.isOraCompat;
}

/*
 * _PG_init - Initialize the SynchDB extension
 */
void
_PG_init(void)
{
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
							2,
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

	DefineCustomIntVariable("synchdb.max_connector_workers",
							"max number of connector workers that can be run at any time. Higher number would occupy more"
							"shared memory space",
							NULL,
							&synchdb_max_connector_workers,
							30,
							1,
							65535,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

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

	/* initialize data type mapping engine for all connectors */
	fc_initFormatConverter(TYPE_MYSQL);
	fc_initFormatConverter(TYPE_SQLSERVER);
	fc_initFormatConverter(TYPE_ORACLE);

	if (process_shared_preload_libraries_in_progress)
	{
		/* can't define PGC_POSTMASTER variable after startup */
		DefineCustomBoolVariable("synchdb.synchdb_auto_launcher",
								 "option to automatic launch connector workers at server restarts. This option "
								 "only works when synchdb is included in shared_preload_library option. Default true",
								 NULL,
								 &synchdb_auto_launcher,
								 true,
								 PGC_POSTMASTER,
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
	if (synchdb_auto_launcher && process_shared_preload_libraries_in_progress)
	{
		synchdb_start_leader_worker();
	}

 	prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = synchdb_ProcessUtility;
}

/*
 * synchdb_engine_main - Main entry point for the SynchDB background worker
 */
void
synchdb_engine_main(Datum main_arg)
{
	ConnectorType connectorType;
	ConnectionInfo connInfo = {0};
	char * snapshotMode = NULL;

	/* extract connectorId from main_arg */
	myConnectorId = DatumGetUInt32(main_arg);

	/* Set up signal handlers, initialize shared memory and obtain connInfo*/
	setup_environment(&connectorType, &connInfo, &snapshotMode);

	/* Initialize the connector state */
	set_shm_connector_state(myConnectorId, STATE_INITIALIZING);

	/*
	 * Initialize the stage to be CDC and it may get changed when the connector
	 * detects that it is in initial snapshot or other stages
	 */
	set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
	set_shm_connector_errmsg(myConnectorId, NULL);

	/* initialize format converter and its cache */
	fc_initFormatConverter(connectorType);
	fc_initDataCache();

	/* load custom object mappings */
	fc_load_objmap(connInfo.name, connectorType);

	if (connectorType != TYPE_OLR)
	{
		/* Initialize JVM */
		initialize_jvm(&connInfo.jmx);

		/* read current offset and update shm */
		memset(sdb_state->connectors[myConnectorId].dbzoffset, 0, SYNCHDB_ERRMSG_SIZE);
		set_shm_dbz_offset(myConnectorId);

		/* set the desired snapshot engine */
		connInfo.snapengine = synchdb_snapshot_engine;

		if (connInfo.snapengine == ENGINE_DEBEZIUM)
		{
			/*
			 * All debezium based connectors support debezium-based snapshot,
			 * so, just launch debezium and let it take care of everything in
			 * the main_loop. Debezium knows whether or not a snapshot has
			 * been done before or needs to do again.
			 */
			start_debezium_engine(connectorType, &connInfo, snapshotMode);
		}
		else if (connInfo.snapengine == ENGINE_FDW)
		{
			/*
			 * Only Oracle and OLR connectors support FDW based snapshot at this
			 * moment. We may expand to support other connectors in the future.
			 * The FDW based snapshot is handled entirely by synchdb so we need
			 * to find out if a snapshot has been done before, either via FDW or
			 * via Debezium, so we can signal main_loop correctly if FDW snapshot
			 * is required.
			 */
			if (connectorType != TYPE_ORACLE)
			{
				/* none-Oracle connectors, just start Debezium */
				start_debezium_engine(connectorType, &connInfo, snapshotMode);
			}
			else
			{
				/* Oracle connectors with FDW snapshot. Check if it is required. */
				const char * curroffset = get_shm_dbz_offset(myConnectorId);
				bool isSnapshotDone = false;
				bool snapshot = false, cdc = false;;

				isSnapshotDone = dbz_read_snapshot_state(connectorType, curroffset);
				is_snapshot_cdc_needed(snapshotMode, isSnapshotDone, &snapshot, &cdc);

				elog(WARNING,"snapshot mode %s: isSnapshotDone %d, snapshot %d, cdc %d",
						snapshotMode, isSnapshotDone, snapshot, cdc);

				/* xxx set no cdc flag if requested - does it apply here?? */
				if (!cdc)
					connInfo.flag |= CONNFLAG_NO_CDC_MODE;

				if (snapshot)
				{
					/*
					 * indicate to main_loop that we want to do initial snapshot
					 * via FDW by ourselves and not use debezium engine.
					 */
					connInfo.flag |= CONNFLAG_INITIAL_SNAPSHOT_MODE;

					/* set to the right state */
					set_shm_connector_state(myConnectorId, STATE_SYNCING);
				}
				else
				{
					/* snapshot already done, start debezium and resume CDC normally */
					start_debezium_engine(connectorType, &connInfo, snapshotMode);
				}
			}
		}

		elog(LOG, "Going to main loop .... ");
		main_loop(connectorType, &connInfo, snapshotMode);
	}
#ifdef WITH_OLR
	else
	{
		bool isSnapshotDone = false;
		bool snapshot = false, cdc = false;

		/* set the desired snapshot engine */
		connInfo.snapengine = synchdb_snapshot_engine;

		if (!olr_client_read_snapshot_state(connectorType, connInfo.name,
				connInfo.dstdb, &isSnapshotDone))
		{
			elog(WARNING, "Snapshot file absent. Assuming snapshot is not done...");
		}

		is_snapshot_cdc_needed(snapshotMode, isSnapshotDone, &snapshot, &cdc);

		elog(WARNING,"snapshot mode %s: isSnapshotDone %d, snapshot %d, cdc %d",
				snapshotMode, isSnapshotDone, snapshot, cdc);

		/* set no cdc flag if requested */
		if (!cdc)
			connInfo.flag |= CONNFLAG_NO_CDC_MODE;

		if (snapshot)
		{
			/* indicate to main_loop that we need to do initial snapshot */
			connInfo.flag |= CONNFLAG_INITIAL_SNAPSHOT_MODE;

			/* start JVM if needed */
			if (connInfo.snapengine == ENGINE_DEBEZIUM)
			{
				/* Initialize JVM */
				initialize_jvm(&connInfo.jmx);

				/* start Debezium engine */
				start_debezium_engine(connectorType, &connInfo, snapshotMode);
			}
			else if (connInfo.snapengine == ENGINE_FDW)
			{
				/* no preparation needed, just set the state */
				set_shm_connector_state(myConnectorId, STATE_SYNCING);
			}
			else
			{
				set_shm_connector_errmsg(myConnectorId, "unsupported snapshot engine");
				elog(ERROR, "unsupported snapshot engine %d", connInfo.snapengine);
			}

			elog(LOG, "Going to main loop .... ");
			main_loop(connectorType, &connInfo, snapshotMode);
		}
		else
		{
			int ret = -1;

			if (connInfo.flag & CONNFLAG_NO_CDC_MODE)
			{
				elog(WARNING, "CDC is not requested in snapshot mode '%s'. Exit...", snapshotMode);
				set_shm_connector_errmsg(myConnectorId, "CDC not requested");
				proc_exit(0);
			}
			/* read resume scn if exists */
			if (!olr_client_init_scn_state(connectorType, connInfo.name, connInfo.dstdb))
				elog(WARNING, "scn file not flushed yet");
			else
			{
				/* set current scn offset in shared memory for state display */
				memset(sdb_state->connectors[myConnectorId].dbzoffset, 0, SYNCHDB_ERRMSG_SIZE);
				set_shm_dbz_offset(myConnectorId);
			}

			ret = olr_client_init(connInfo.olr.olr_host, connInfo.olr.olr_port);
			if (ret)
			{
				set_shm_connector_errmsg(myConnectorId, "failed to init and connect to olr server");
				elog(ERROR, "failed to init and connect to olr server");
			}

			/* connInfo.srcdb is used as data source for OLR */
			ret = olr_client_start_or_cont_replication(connInfo.olr.olr_source, true);
			if (ret == -1)
			{
				set_shm_connector_errmsg(myConnectorId, "failed to start replication with olr server");
				elog(ERROR, "failed to start replication with olr server");
			}

			if (ret == RES_ALREADY_STARTED || ret == RES_STARTING)
			{
				elog(WARNING, "replication already started or starting - sending continue");
				ret = olr_client_start_or_cont_replication(connInfo.olr.olr_source, false);
				if (ret == -1)
				{
					set_shm_connector_errmsg(myConnectorId, "failed to start replication with olr server");
					elog(ERROR, "failed to start replication with olr server");
				}
			}

			if (ret == RES_REPLICATE)
			{
				set_shm_connector_state(myConnectorId, STATE_SYNCING);

				elog(LOG, "Going to main loop .... ");
				main_loop(connectorType, &connInfo, snapshotMode);
			}
			else if (ret == RES_INVALID_DATABASE)
			{
				set_shm_connector_errmsg(myConnectorId, "invalid data source requested");
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid data source requested"),
						 errhint("make sure srcdb matches data source set in openlog replicator")));
			}
			else if (ret == RES_INVALID_COMMAND)
			{
				set_shm_connector_errmsg(myConnectorId, "invalid OLR command");
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid OLR command"),
						 errhint("check the openlog replicator protocol version for potential protocol "
								 "incompatibility")));
			}
			else
			{
				set_shm_connector_errmsg(myConnectorId, "openlog replicator is not ready to replicate");
				elog(ERROR, "openlog replicator is not ready to replicate, response code = %d", ret);
			}
			olr_client_shutdown();
		}
	}
#else
	else
	{
		set_shm_connector_errmsg(myConnectorId, "OLR connector is not enabled in this synchdb build.");
		elog(ERROR, "OLR connector is not enabled in this synchdb build.");
	}
#endif
	if (snapshotMode)
		pfree(snapshotMode);

	elog(LOG, "synchdb worker shutting down .... ");
	proc_exit(0);
}

/*
 * synchdb_start_engine_bgw_snapshot_mode
 *
 * This function starts a connector with a custom snapshot mode
 */
Datum
synchdb_start_engine_bgw_snapshot_mode(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t pid;
	ConnectionInfo connInfo = {0};
	char *connector = NULL;
	int ret = -1, connectorid = -1;
	StringInfoData strinfo;
	char * _snapshotMode = "initial";

	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);
	Name snapshotmode = PG_GETARG_NAME(1);

	ret = ra_getConninfoByName(NameStr(*name), &connInfo, &connector);
	if (ret)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connection name does not exist"),
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
	/*
	 * attach or initialize synchdb shared memory area so we can assign
	 * a connector ID for this worker
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorid = assign_connector_id(connInfo.name, connInfo.dstdb);
	if (connectorid == -1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max number of connectors reached"),
				 errhint("use synchdb_stop_engine_bgw to stop some active connectors")));

	/* prepare background worker */
	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	if (synchdb_error_strategy == STRAT_RETRY_ON_ERROR)
		worker.bgw_restart_time = 5;
	else
		worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_notify_pid = MyProcPid;

	strcpy(worker.bgw_library_name, "synchdb");
	strcpy(worker.bgw_function_name, "synchdb_engine_main");

	prepare_bgw(&worker, &connInfo, connector, connectorid, _snapshotMode);

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));

	/*
	 * mark this conninfo as active so it can automatically resume running at
	 * postgresql server restarts given that synchdb is included in
	 * shared_preload_library GUC
	 */
	initStringInfo(&strinfo);
	appendStringInfo(&strinfo, "UPDATE synchdb_conninfo set isactive = true "
			"WHERE name = '%s'", NameStr(*name));

	ra_executeCommand(strinfo.data);

	PG_RETURN_INT32(0);
}

/*
 * synchdb_start_engine_bgw
 *
 * This function starts a connector with the default initial snapshot mode
 */
Datum
synchdb_start_engine_bgw(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t pid;
	ConnectionInfo connInfo = {0};
	char *connector = NULL;
	int ret = -1, connectorid = -1;
	StringInfoData strinfo;
	/* By default, we use snapshot mode = initial */
	char * snapshotMode = "initial";

	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);

	ret = ra_getConninfoByName(NameStr(*name), &connInfo, &connector);
	if (ret)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connection name does not exist"),
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
	/*
	 * attach or initialize synchdb shared memory area so we can assign
	 * a connector ID for this worker
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorid = assign_connector_id(connInfo.name, connInfo.dstdb);
	if (connectorid == -1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max number of connectors reached"),
				 errhint("use synchdb_stop_engine_bgw to stop some active connectors")));

	/* prepare background worker */
	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	if (synchdb_error_strategy == STRAT_RETRY_ON_ERROR)
		worker.bgw_restart_time = 5;
	else
		worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_notify_pid = MyProcPid;

	strcpy(worker.bgw_library_name, "synchdb");
	strcpy(worker.bgw_function_name, "synchdb_engine_main");

	prepare_bgw(&worker, &connInfo, connector, connectorid, snapshotMode);

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));

	/*
	 * mark this conninfo as active so it can automatically resume running at
	 * postgresql server restarts given that synchdb is included in
	 * shared_preload_library GUC
	 */
	initStringInfo(&strinfo);
	appendStringInfo(&strinfo, "UPDATE synchdb_conninfo set isactive = true "
			"WHERE name = '%s'", NameStr(*name));

	ra_executeCommand(strinfo.data);

	PG_RETURN_INT32(0);
}

/*
 * synchdb_stop_engine_bgw
 *
 * This function stops a running connector
 */
Datum
synchdb_stop_engine_bgw(PG_FUNCTION_ARGS)
{
	int connectorId;
	pid_t pid;
	StringInfoData strinfo;

	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(NameStr(*name), get_database_name(MyDatabaseId));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	/*
	 * mark this conninfo as inactive so it will not automatically resume running
	 * at postgresql server restarts given that synchdb is included in
	 * shared_preload_library GUC
	 */
	initStringInfo(&strinfo);
	appendStringInfo(&strinfo, "UPDATE synchdb_conninfo set isactive = false "
			"WHERE name = '%s'", NameStr(*name));

	ra_executeCommand(strinfo.data);

	pid = get_shm_connector_pid(connectorId);
	if (pid != InvalidPid)
	{
		elog(WARNING, "terminating dbz connector (%s) with pid %d. Shutdown timeout: %d ms",
				NameStr(*name), (int)pid, DEBEZIUM_SHUTDOWN_TIMEOUT_MSEC);
		DirectFunctionCall2(pg_terminate_backend, UInt32GetDatum(pid), Int64GetDatum(DEBEZIUM_SHUTDOWN_TIMEOUT_MSEC));
		set_shm_connector_pid(connectorId, InvalidPid);

	}
	else
	{
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not running", NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));
	}
	PG_RETURN_INT32(0);
}

/*
 * synchdb_get_state
 *
 * This function dumps the states of all connectors
 */
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
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

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

	while (*idx < count_active_connectors())
	{
		Datum values[7];
		bool nulls[7] = {0};
		HeapTuple tuple;

		/* we only want to show the connectors created in current database */
		if (strcasecmp(sdb_state->connectors[*idx].conninfo.dstdb,
				get_database_name(MyDatabaseId)))
		{
	        (*idx)++;
	        continue;
		}

		LWLockAcquire(&sdb_state->lock, LW_SHARED);
		values[0] = CStringGetTextDatum(sdb_state->connectors[*idx].conninfo.name);
		values[1] = CStringGetTextDatum(get_shm_connector_name(sdb_state->connectors[*idx].type));
		values[2] = Int32GetDatum((int)get_shm_connector_pid(*idx));
		values[3] = CStringGetTextDatum(get_shm_connector_stage(*idx));
		values[4] = CStringGetTextDatum(get_shm_connector_state(*idx));
		values[5] = CStringGetTextDatum(get_shm_connector_errmsg(*idx));
		values[6] = CStringGetTextDatum(get_shm_dbz_offset(*idx));
		LWLockRelease(&sdb_state->lock);

		*idx += 1;

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
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

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

	while (*idx < count_active_connectors())
	{
		Datum values[20];
		bool nulls[20] = {0};
		HeapTuple tuple;

		/* we only want to show the connectors created in current database */
		if (strcasecmp(sdb_state->connectors[*idx].conninfo.dstdb,
				get_database_name(MyDatabaseId)))
		{
	        (*idx)++;
	        continue;
		}

		LWLockAcquire(&sdb_state->lock, LW_SHARED);
		values[0] = CStringGetTextDatum(sdb_state->connectors[*idx].conninfo.name);
		/* cdc stats */
		values[1] = Int64GetDatum(sdb_state->connectors[*idx].stats.cdcstats.stats_ddl);
		values[2] = Int64GetDatum(sdb_state->connectors[*idx].stats.cdcstats.stats_dml);
		values[3] = Int64GetDatum(sdb_state->connectors[*idx].stats.cdcstats.stats_create);
		values[4] = Int64GetDatum(sdb_state->connectors[*idx].stats.cdcstats.stats_update);
		values[5] = Int64GetDatum(sdb_state->connectors[*idx].stats.cdcstats.stats_delete);
		values[6] = Int64GetDatum(sdb_state->connectors[*idx].stats.cdcstats.stats_tx);
		values[7] = Int64GetDatum(sdb_state->connectors[*idx].stats.cdcstats.stats_truncate);

		/* general stats */
		values[8] = Int64GetDatum(sdb_state->connectors[*idx].stats.genstats.stats_bad_change_event);
		values[9] = Int64GetDatum(sdb_state->connectors[*idx].stats.genstats.stats_total_change_event);
		values[10] = Int64GetDatum(sdb_state->connectors[*idx].stats.genstats.stats_batch_completion);
		values[11] = sdb_state->connectors[*idx].stats.genstats.stats_batch_completion > 0?
					Int64GetDatum(sdb_state->connectors[*idx].stats.genstats.stats_total_change_event /
							sdb_state->connectors[*idx].stats.genstats.stats_batch_completion) :
					Int64GetDatum(0);
		values[12] = Int64GetDatum(sdb_state->connectors[*idx].stats.genstats.stats_first_src_ts);
		values[13] = Int64GetDatum(sdb_state->connectors[*idx].stats.genstats.stats_first_pg_ts);
		values[14] = Int64GetDatum(sdb_state->connectors[*idx].stats.genstats.stats_last_src_ts);
		values[15] = Int64GetDatum(sdb_state->connectors[*idx].stats.genstats.stats_last_pg_ts);

		/* snapshot stats */
		values[16] = Int64GetDatum(sdb_state->connectors[*idx].stats.snapstats.snapstats_tables);
		values[17] = Int64GetDatum(sdb_state->connectors[*idx].stats.snapstats.snapstats_rows);
		values[18] = Int64GetDatum(sdb_state->connectors[*idx].stats.snapstats.snapstats_begintime_ts);
		values[19] = Int64GetDatum(sdb_state->connectors[*idx].stats.snapstats.snapstats_endtime_ts);
		LWLockRelease(&sdb_state->lock);

		*idx += 1;

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
	int connectorId;

	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(NameStr(*name), get_database_name(MyDatabaseId));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	memset(&sdb_state->connectors[connectorId].stats, 0, sizeof(SynchdbStatistics));
	LWLockRelease(&sdb_state->lock);

	PG_RETURN_INT32(0);
}

/*
 * synchdb_pause_engine
 *
 * This function pauses a running connector
 */
Datum
synchdb_pause_engine(PG_FUNCTION_ARGS)
{
	int connectorId = -1;
	pid_t pid;
	SynchdbRequest *req;

	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(NameStr(*name), get_database_name(MyDatabaseId));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not running", NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector %s",
						NameStr(*name)),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_PAUSED;
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent pause request interrupt to dbz connector %s (%d)",
			NameStr(*name), connectorId);
	PG_RETURN_INT32(0);
}

/*
 * synchdb_resume_engine
 *
 * This function resumes a paused connector
 */
Datum
synchdb_resume_engine(PG_FUNCTION_ARGS)
{
	int connectorId = -1;
	pid_t pid;
	SynchdbRequest *req;

	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(NameStr(*name), get_database_name(MyDatabaseId));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector id (%s) is not running", NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector id %s",
						NameStr(*name)),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_SYNCING;
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent resume request interrupt to dbz connector (%s)",
			NameStr(*name));
	PG_RETURN_INT32(0);
}

/*
 * synchdb_set_offset
 *
 * This function sets the given offset to debezium's offset file
 */
Datum
synchdb_set_offset(PG_FUNCTION_ARGS)
{
	pid_t pid;
	int connectorId = -1;
	char *offsetstr;
	SynchdbRequest *req;
	ConnectorState currstate;

	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);
	offsetstr = text_to_cstring(PG_GETARG_TEXT_P(1));

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(NameStr(*name), get_database_name(MyDatabaseId));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not running",
						NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	currstate = get_shm_connector_state_enum(connectorId);
	if (currstate != STATE_PAUSED)
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not in paused state.",
						NameStr(*name)),
				 errhint("use synchdb_pause_engine() to pause the worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector %s",
						NameStr(*name)),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_OFFSET_UPDATE;
	strncpy(req->reqdata, offsetstr, SYNCHDB_ERRMSG_SIZE);
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent update offset request interrupt to dbz connector (%s)",
			NameStr(*name));
	PG_RETURN_INT32(0);
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
	text *dst_db_text = PG_GETARG_TEXT_PP(6);
	text *table_text = PG_GETARG_TEXT_PP(7);
	text *snapshottable_text = PG_GETARG_TEXT_PP(8);
	text *connector_text = PG_GETARG_TEXT_PP(9);
	char *connector = NULL;

	ConnectionInfo connInfo = {0};
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	strlcpy(connInfo.name, NameStr(*name), SYNCHDB_CONNINFO_NAME_SIZE);

	/* Sanity check on input arguments */
	if (VARSIZE(hostname_text) - VARHDRSZ == 0 ||
			VARSIZE(hostname_text) - VARHDRSZ > SYNCHDB_CONNINFO_HOSTNAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("hostname cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_HOSTNAME_SIZE)));
	}
	strlcpy(connInfo.hostname, text_to_cstring(hostname_text), SYNCHDB_CONNINFO_HOSTNAME_SIZE);

	connInfo.port = port;
	if (connInfo.port == 0 || connInfo.port > 65535)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid port number")));
	}

	if (VARSIZE(user_text) - VARHDRSZ == 0 ||
			VARSIZE(user_text) - VARHDRSZ > SYNCHDB_CONNINFO_USERNAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("username cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_USERNAME_SIZE)));
	}
	strlcpy(connInfo.user, text_to_cstring(user_text), SYNCHDB_CONNINFO_USERNAME_SIZE);

	if (VARSIZE(pwd_text) - VARHDRSZ == 0 ||
			VARSIZE(pwd_text) - VARHDRSZ > SYNCHDB_CONNINFO_PASSWORD_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("password cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_PASSWORD_SIZE)));
	}
	strlcpy(connInfo.pwd, text_to_cstring(pwd_text), SYNCHDB_CONNINFO_PASSWORD_SIZE);

	/* source database can be empty or NULL */
	if (VARSIZE(src_db_text) - VARHDRSZ == 0)
		strlcpy(connInfo.srcdb, "null", SYNCHDB_CONNINFO_DB_NAME_SIZE);
	else if (VARSIZE(src_db_text) - VARHDRSZ > SYNCHDB_CONNINFO_DB_NAME_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("source database cannot be longer than %d",
						 SYNCHDB_CONNINFO_DB_NAME_SIZE)));
	else
		strlcpy(connInfo.srcdb, text_to_cstring(src_db_text), SYNCHDB_CONNINFO_DB_NAME_SIZE);

	if (VARSIZE(dst_db_text) - VARHDRSZ == 0 ||
			VARSIZE(dst_db_text) - VARHDRSZ > SYNCHDB_CONNINFO_DB_NAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("destination database cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_DB_NAME_SIZE)));
	}

	if (strcasecmp(text_to_cstring(dst_db_text), get_database_name(MyDatabaseId)))
	{
		elog(WARNING, "adjusting destination database from %s to the current database %s",
				text_to_cstring(dst_db_text),
				get_database_name(MyDatabaseId));
		strlcpy(connInfo.dstdb, get_database_name(MyDatabaseId), SYNCHDB_CONNINFO_DB_NAME_SIZE);
	}
	else
		strlcpy(connInfo.dstdb, text_to_cstring(dst_db_text), SYNCHDB_CONNINFO_DB_NAME_SIZE);

	/* table can be empty or NULL */
	if (VARSIZE(table_text) - VARHDRSZ == 0)
		strlcpy(connInfo.table, "null", SYNCHDB_CONNINFO_TABLELIST_SIZE);
	else if (VARSIZE(table_text) - VARHDRSZ > SYNCHDB_CONNINFO_TABLELIST_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("table list cannot be longer than %d",
						 SYNCHDB_CONNINFO_TABLELIST_SIZE)));
	else
		strlcpy(connInfo.table, text_to_cstring(table_text), SYNCHDB_CONNINFO_TABLELIST_SIZE);

	/* snapshot table can be empty or NULL */
	if (VARSIZE(snapshottable_text) - VARHDRSZ == 0)
		strlcpy(connInfo.snapshottable, "null", SYNCHDB_CONNINFO_TABLELIST_SIZE);
	else if (VARSIZE(snapshottable_text) - VARHDRSZ > SYNCHDB_CONNINFO_TABLELIST_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("snapshot table cannot be longer than %d",
						 SYNCHDB_CONNINFO_TABLELIST_SIZE)));
	else
		strlcpy(connInfo.snapshottable, text_to_cstring(snapshottable_text),
				SYNCHDB_CONNINFO_TABLELIST_SIZE);

	if (VARSIZE(connector_text) - VARHDRSZ == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connector type cannot be empty")));
	}
	connector = text_to_cstring(connector_text);

#ifdef WITH_OLR
	if (strcasecmp(connector, "mysql") && strcasecmp(connector, "sqlserver")
			&& strcasecmp(connector, "oracle") && strcasecmp(connector, "olr"))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported connector")));
#else
	if (strcasecmp(connector, "mysql") && strcasecmp(connector, "sqlserver")
			&& strcasecmp(connector, "oracle"))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported connector")));
#endif

	appendStringInfo(&strinfo, "INSERT INTO %s (name, isactive, data)"
			" VALUES ('%s', %s, jsonb_build_object("
			"'hostname', '%s', "
			"'port', %d, "
			"'user', '%s', "
			"'pwd', pgp_sym_encrypt('%s', '%s'), "
			"'srcdb', '%s', "
			"'dstdb', '%s', "
			"'table', (CASE WHEN '%s' = 'null' THEN null ELSE '%s' END), "
			"'snapshottable', (CASE WHEN '%s' = 'null' THEN null ELSE '%s' END), "
			"'connector', '%s'));",
			SYNCHDB_CONNINFO_TABLE,
			connInfo.name,
			"false",
			connInfo.hostname,
			connInfo.port,
			connInfo.user,
			connInfo.pwd,
			SYNCHDB_SECRET,
			connInfo.srcdb,
			connInfo.dstdb,
			connInfo.table,
			connInfo.table,
			connInfo.snapshottable,
			connInfo.snapshottable,
			connector);

	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
}

/*
 * synchdb_restart_connector
 *
 * This function restarts a connector with given snapshot mode
 */
Datum
synchdb_restart_connector(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	Name snapshot_mode = PG_GETARG_NAME(1);
	ConnectionInfo connInfo;
	int ret = -1, connectorId = -1;
	pid_t pid;
	char * _snapshot_mode;
	char *connector = NULL;

	SynchdbRequest *req;

	/* Sanity check on input arguments */
	if (VARSIZE(name_text) - VARHDRSZ == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connection name cannot be empty")));
	}

	/* snapshot_mode can be empty or NULL */
	if (strlen(NameStr(*snapshot_mode)) == 0)
		_snapshot_mode = "null";
	else
		_snapshot_mode = NameStr(*snapshot_mode);

	ret = ra_getConninfoByName(NameStr(*name), &connInfo, &connector);
	if (ret)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connection name does not exist"),
				 errhint("use synchdb_add_conninfo to add one first")));

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(NameStr(*name), get_database_name(MyDatabaseId));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not running",
						NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector %s",
						NameStr(*name)),
				 errhint("wait for it to finish and try again later")));

	/*
	 * connector info may have been changed, so let's pass the latest conninfo
	 * to the connector worker in the same way as synchdb_start_engine_bgw()
	 */
	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_RESTARTING;

	/* reqdata contains snapshot mode */
	memset(req->reqdata, 0, SYNCHDB_ERRMSG_SIZE);
	snprintf(req->reqdata, SYNCHDB_ERRMSG_SIZE,"%s", _snapshot_mode);

	/*
	 * reqconninfo contains a copy of conninfo recently read in case it has been
	 * changed since connector start
	 */
	memcpy(&req->reqconninfo, &connInfo, sizeof(ConnectionInfo));
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent restart request interrupt to dbz connector (%s)",
			NameStr(*name));
	PG_RETURN_INT32(0);
}

/*
 * synchdb_log_jvm_meminfo
 *
 * This function dumps JVM memory usage info in PostgreSQL log
 */
Datum
synchdb_log_jvm_meminfo(PG_FUNCTION_ARGS)
{
	int connectorId = -1;
	pid_t pid;
	SynchdbRequest *req;

	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(NameStr(*name), get_database_name(MyDatabaseId));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not running", NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector %s",
						NameStr(*name)),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_MEMDUMP;
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent memdump request interrupt to dbz connector %s (%d)",
			NameStr(*name), connectorId);
	PG_RETURN_INT32(0);
}

Datum
synchdb_add_objmap(PG_FUNCTION_ARGS)
{
	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);
	Name objtype = PG_GETARG_NAME(1);
	Name srcobj = PG_GETARG_NAME(2);
	text * dstobj = PG_GETARG_TEXT_PP(3);

	StringInfoData strinfo;
	initStringInfo(&strinfo);
	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	if (strcasecmp(NameStr(*objtype), "table") && strcasecmp(NameStr(*objtype), "column") &&
		strcasecmp(NameStr(*objtype), "datatype") && strcasecmp(NameStr(*objtype), "transform"))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unsupported object type %s", NameStr(*objtype))));
	}
	appendStringInfo(&strinfo, "INSERT INTO %s (name, objtype, enabled, srcobj, dstobj)"
			" VALUES (trim(lower('%s')), trim(lower('%s')), true, trim(lower('%s')), '%s')",
			SYNCHDB_OBJECT_MAPPING_TABLE,
			NameStr(*name),
			NameStr(*objtype),
			NameStr(*srcobj),
			escapeSingleQuote(text_to_cstring(dstobj), false));

	appendStringInfo(&strinfo, " ON CONFLICT(name, objtype, srcobj) "
			"DO UPDATE SET "
			"enabled = EXCLUDED.enabled,"
			"dstobj = EXCLUDED.dstobj;");

	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
}

/*
 * synchdb_reload_objmap
 *
 * This function forces a connector to reload objmap table entries
 */
Datum
synchdb_reload_objmap(PG_FUNCTION_ARGS)
{
	int connectorId = -1;
	pid_t pid;
	SynchdbRequest *req;

	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(NameStr(*name), get_database_name(MyDatabaseId));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not running", NameStr(*name)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector %s",
						NameStr(*name)),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_RELOAD_OBJMAP;
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent reload objmap request interrupt to dbz connector %s (%d)",
			NameStr(*name), connectorId);
	PG_RETURN_INT32(0);
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
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	strlcpy(extraconninfo.ssl_mode, NameStr(*sslmode), SYNCHDB_CONNINFO_NAME_SIZE);

	if (VARSIZE(ssl_keystore_text) - VARHDRSZ == 0)
		strlcpy(extraconninfo.ssl_keystore, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
	else if (VARSIZE(ssl_keystore_text) - VARHDRSZ > SYNCHDB_CONNINFO_KEYSTORE_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_keystore cannot be longer than %d",
						 SYNCHDB_CONNINFO_KEYSTORE_SIZE)));
	else
		strlcpy(extraconninfo.ssl_keystore, text_to_cstring(ssl_keystore_text), SYNCHDB_CONNINFO_KEYSTORE_SIZE);

	if (VARSIZE(ssl_keystore_pass_text) - VARHDRSZ == 0)
		strlcpy(extraconninfo.ssl_keystore_pass, "null", SYNCHDB_CONNINFO_PASSWORD_SIZE);
	else if (VARSIZE(ssl_keystore_pass_text) - VARHDRSZ > SYNCHDB_CONNINFO_PASSWORD_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_keystore_pass cannot be longer than %d",
						 SYNCHDB_CONNINFO_PASSWORD_SIZE)));
	else
		strlcpy(extraconninfo.ssl_keystore_pass, text_to_cstring(ssl_keystore_pass_text), SYNCHDB_CONNINFO_PASSWORD_SIZE);

	if (VARSIZE(ssl_truststore_text) - VARHDRSZ == 0)
		strlcpy(extraconninfo.ssl_truststore, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
	else if (VARSIZE(ssl_truststore_text) - VARHDRSZ > SYNCHDB_CONNINFO_KEYSTORE_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_truststore cannot be longer than %d",
						 SYNCHDB_CONNINFO_KEYSTORE_SIZE)));
	else
		strlcpy(extraconninfo.ssl_truststore, text_to_cstring(ssl_truststore_text), SYNCHDB_CONNINFO_KEYSTORE_SIZE);

	if (VARSIZE(ssl_truststore_pass_text) - VARHDRSZ == 0)
		strlcpy(extraconninfo.ssl_truststore_pass, "null", SYNCHDB_CONNINFO_PASSWORD_SIZE);
	else if (VARSIZE(ssl_truststore_pass_text) - VARHDRSZ > SYNCHDB_CONNINFO_PASSWORD_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ssl_truststore_pass cannot be longer than %d",
						 SYNCHDB_CONNINFO_PASSWORD_SIZE)));
	else
		strlcpy(extraconninfo.ssl_truststore_pass, text_to_cstring(ssl_truststore_pass_text), SYNCHDB_CONNINFO_PASSWORD_SIZE);

	appendStringInfo(&strinfo, "UPDATE %s SET data = data || json_build_object("
			"'ssl_mode', '%s', "
			"'ssl_keystore',  (CASE WHEN '%s' = 'null' THEN null ELSE '%s' END), "
			"'ssl_keystore_pass', (CASE WHEN '%s' = 'null' THEN null ELSE pgp_sym_encrypt('%s', '%s') END), "
			"'ssl_truststore', (CASE WHEN '%s' = 'null' THEN null ELSE '%s' END), "
			"'ssl_truststore_pass', (CASE WHEN '%s' = 'null' THEN null ELSE pgp_sym_encrypt('%s', '%s') END))::jsonb "
			"WHERE name = '%s'",
			SYNCHDB_CONNINFO_TABLE,
			extraconninfo.ssl_mode,
			extraconninfo.ssl_keystore,
			extraconninfo.ssl_keystore,
			extraconninfo.ssl_keystore_pass,
			extraconninfo.ssl_keystore_pass,
			SYNCHDB_SECRET,
			extraconninfo.ssl_truststore,
			extraconninfo.ssl_truststore,
			extraconninfo.ssl_truststore_pass,
			extraconninfo.ssl_truststore_pass,
			SYNCHDB_SECRET,
			NameStr(*name));

	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
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
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	appendStringInfo(&strinfo, "UPDATE %s SET data = data - ARRAY["
			"'ssl_mode', "
			"'ssl_keystore', "
			"'ssl_keystore_pass', "
			"'ssl_truststore', "
			"'ssl_truststore_pass'] "
			"WHERE name = '%s'",
			SYNCHDB_CONNINFO_TABLE,
			NameStr(*name));
	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
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
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	if (VARSIZE(jmx_listenaddr_text) - VARHDRSZ == 0 ||
			VARSIZE(jmx_listenaddr_text) - VARHDRSZ > SYNCHDB_CONNINFO_HOSTNAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jmx listen address cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_HOSTNAME_SIZE)));
	}
	strlcpy(jmxconninfo.jmx_listenaddr, text_to_cstring(jmx_listenaddr_text), SYNCHDB_CONNINFO_HOSTNAME_SIZE);

	jmxconninfo.jmx_port = jmx_port;
	if (jmxconninfo.jmx_port == 0 || jmxconninfo.jmx_port > 65535)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid port number")));
	}

	if (VARSIZE(jmx_rmiserver_text) - VARHDRSZ == 0 ||
			VARSIZE(jmx_rmiserver_text) - VARHDRSZ > SYNCHDB_CONNINFO_HOSTNAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jmx rmi server address cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_HOSTNAME_SIZE)));
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
		if (VARSIZE(jmx_passwdfile_text) - VARHDRSZ == 0 ||
				VARSIZE(jmx_passwdfile_text) - VARHDRSZ > SYNCHDB_METADATA_PATH_SIZE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("jmx auth password file cannot be empty or longer than %d",
							 SYNCHDB_METADATA_PATH_SIZE)));
		}
		strlcpy(jmxconninfo.jmx_auth_passwdfile, text_to_cstring(jmx_passwdfile_text), SYNCHDB_METADATA_PATH_SIZE);

		if (VARSIZE(jmx_accessfile_text) - VARHDRSZ == 0 ||
				VARSIZE(jmx_accessfile_text) - VARHDRSZ > SYNCHDB_METADATA_PATH_SIZE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("jmx auth password file cannot be empty or longer than %d",
							 SYNCHDB_METADATA_PATH_SIZE)));
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
		if (VARSIZE(jmx_keystore_text) - VARHDRSZ == 0)
			strlcpy(jmxconninfo.jmx_ssl_keystore, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
		else if (VARSIZE(jmx_keystore_text) - VARHDRSZ > SYNCHDB_CONNINFO_KEYSTORE_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ssl_keystore cannot be longer than %d",
							 SYNCHDB_CONNINFO_KEYSTORE_SIZE)));
		else
			strlcpy(jmxconninfo.jmx_ssl_keystore, text_to_cstring(jmx_keystore_text), SYNCHDB_CONNINFO_KEYSTORE_SIZE);

		if (VARSIZE(jmx_keystore_pass_text) - VARHDRSZ == 0)
			strlcpy(jmxconninfo.jmx_ssl_keystore_pass, "null", SYNCHDB_CONNINFO_PASSWORD_SIZE);
		else if (VARSIZE(jmx_keystore_pass_text) - VARHDRSZ > SYNCHDB_CONNINFO_PASSWORD_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ssl_keystore_pass cannot be longer than %d",
							 SYNCHDB_CONNINFO_PASSWORD_SIZE)));
		else
			strlcpy(jmxconninfo.jmx_ssl_keystore_pass, text_to_cstring(jmx_keystore_pass_text), SYNCHDB_CONNINFO_PASSWORD_SIZE);

		if (VARSIZE(jmx_truststore_text) - VARHDRSZ == 0)
			strlcpy(jmxconninfo.jmx_ssl_truststore, "null", SYNCHDB_CONNINFO_KEYSTORE_SIZE);
		else if (VARSIZE(jmx_truststore_text) - VARHDRSZ > SYNCHDB_CONNINFO_KEYSTORE_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ssl_truststore cannot be longer than %d",
							 SYNCHDB_CONNINFO_KEYSTORE_SIZE)));
		else
			strlcpy(jmxconninfo.jmx_ssl_truststore, text_to_cstring(jmx_truststore_text), SYNCHDB_CONNINFO_KEYSTORE_SIZE);

		if (VARSIZE(jmx_truststore_pass_text) - VARHDRSZ == 0)
			strlcpy(jmxconninfo.jmx_ssl_truststore_pass, "null", SYNCHDB_CONNINFO_PASSWORD_SIZE);
		else if (VARSIZE(jmx_truststore_pass_text) - VARHDRSZ > SYNCHDB_CONNINFO_PASSWORD_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ssl_truststore_pass cannot be longer than %d",
							 SYNCHDB_CONNINFO_PASSWORD_SIZE)));
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

	appendStringInfo(&strinfo, "UPDATE %s SET data = data || json_build_object("
			"'jmx_listenaddr', '%s', "
			"'jmx_port', %u, "
			"'jmx_rmi_address', '%s', "
			"'jmx_rmi_port', %u, "
			"'jmx_auth', %s, "
			"'jmx_auth_passwdfile', (CASE WHEN '%s' = 'null' THEN null ELSE '%s' END), "
			"'jmx_auth_accessfile', (CASE WHEN '%s' = 'null' THEN null ELSE '%s' END), "
			"'jmx_ssl', %s, "
			"'jmx_ssl_keystore', (CASE WHEN '%s' = 'null' THEN null ELSE '%s' END), "
			"'jmx_ssl_keystore_pass', (CASE WHEN '%s' = 'null' THEN null ELSE pgp_sym_encrypt('%s', '%s') END), "
			"'jmx_ssl_truststore', (CASE WHEN '%s' = 'null' THEN null ELSE '%s' END), "
			"'jmx_ssl_truststore_pass', (CASE WHEN '%s' = 'null' THEN null ELSE pgp_sym_encrypt('%s', '%s') END))::jsonb "
			"WHERE name = '%s'",
			SYNCHDB_CONNINFO_TABLE,
			jmxconninfo.jmx_listenaddr,
			jmxconninfo.jmx_port,
			jmxconninfo.jmx_rmiserveraddr,
			jmxconninfo.jmx_rmiport,
			jmxconninfo.jmx_auth ? "true" : "false",
			jmxconninfo.jmx_auth_passwdfile,
			jmxconninfo.jmx_auth_passwdfile,
			jmxconninfo.jmx_auth_accessfile,
			jmxconninfo.jmx_auth_accessfile,
			jmxconninfo.jmx_ssl ? "true" : "false",
			jmxconninfo.jmx_ssl_keystore,
			jmxconninfo.jmx_ssl_keystore,
			jmxconninfo.jmx_ssl_keystore_pass,
			jmxconninfo.jmx_ssl_keystore_pass,
			SYNCHDB_SECRET,
			jmxconninfo.jmx_ssl_truststore,
			jmxconninfo.jmx_ssl_truststore,
			jmxconninfo.jmx_ssl_truststore_pass,
			jmxconninfo.jmx_ssl_truststore_pass,
			SYNCHDB_SECRET,
			NameStr(*name));

	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
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
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	appendStringInfo(&strinfo, "UPDATE %s SET data = data - ARRAY["
			"'jmx_listenaddr', "
			"'jmx_port', "
			"'jmx_rmi_address', "
			"'jmx_rmi_port', "
			"'jmx_auth', "
			"'jmx_auth_passwdfile', "
			"'jmx_auth_accessfile', "
			"'jmx_ssl', "
			"'jmx_ssl_keystore', "
			"'jmx_ssl_keystore_pass', "
			"'jmx_ssl_truststore', "
			"'jmx_ssl_truststore_pass', "
			"'jmx_exporter', "
			"'jmx_exporter_port', "
			"'jmx_exporter_conf'] "
			"WHERE name = '%s'",
			SYNCHDB_CONNINFO_TABLE,
			NameStr(*name));
	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
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
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	/* Sanity check on input arguments */
	if (VARSIZE(jmx_exporterpath_text) - VARHDRSZ == 0 ||
			VARSIZE(jmx_exporterpath_text) - VARHDRSZ > SYNCHDB_METADATA_PATH_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jmx exporter jar library path cannot be empty or longer than %d",
						 SYNCHDB_METADATA_PATH_SIZE)));
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

	if (VARSIZE(jmx_exporterconf_text) - VARHDRSZ == 0 ||
			VARSIZE(jmx_exporterconf_text) - VARHDRSZ > SYNCHDB_METADATA_PATH_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jmx exporter conf file path cannot be empty or longer than %d",
						 SYNCHDB_METADATA_PATH_SIZE)));
	}
	strlcpy(jmxconninfo.jmx_exporter_conf, text_to_cstring(jmx_exporterconf_text),
			SYNCHDB_METADATA_PATH_SIZE);


	appendStringInfo(&strinfo, "UPDATE %s SET data = data || json_build_object("
			"'jmx_exporter', '%s', "
			"'jmx_exporter_port', %u, "
			"'jmx_exporter_conf', '%s')::jsonb "
			"WHERE name = '%s'",
			SYNCHDB_CONNINFO_TABLE,
			jmxconninfo.jmx_exporter,
			jmxconninfo.jmx_exporter_port,
			jmxconninfo.jmx_exporter_conf,
			NameStr(*name));

	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
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
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	appendStringInfo(&strinfo, "UPDATE %s SET data = data - ARRAY["
			"'jmx_exporter', "
			"'jmx_exporter_port', "
			"'jmx_exporter_conf'] "
			"WHERE name = '%s'",
			SYNCHDB_CONNINFO_TABLE,
			NameStr(*name));
	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
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
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	if (VARSIZE(olr_host_text) - VARHDRSZ == 0 ||
			VARSIZE(olr_host_text) - VARHDRSZ > SYNCHDB_CONNINFO_HOSTNAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Openlog Replicator host address cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_HOSTNAME_SIZE)));
	}
	strlcpy(olrconninfo.olr_host, text_to_cstring(olr_host_text), SYNCHDB_CONNINFO_HOSTNAME_SIZE);

	olrconninfo.olr_port = olr_port;
	if (olrconninfo.olr_port == 0 || olrconninfo.olr_port > 65535)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid port number")));
	}

	if (VARSIZE(olr_source_text) - VARHDRSZ == 0 ||
			VARSIZE(olr_source_text) - VARHDRSZ > SYNCHDB_CONNINFO_NAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Openlog Replicator data source cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_NAME_SIZE)));
	}
	strlcpy(olrconninfo.olr_source, text_to_cstring(olr_source_text), SYNCHDB_CONNINFO_NAME_SIZE);

	appendStringInfo(&strinfo, "UPDATE %s SET data = data || json_build_object("
			"'olr_host', '%s', "
			"'olr_port', %u, "
			"'olr_source', '%s')::jsonb "
			"WHERE name = '%s'",
			SYNCHDB_CONNINFO_TABLE,
			olrconninfo.olr_host,
			olrconninfo.olr_port,
			olrconninfo.olr_source,
			NameStr(*name));

	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
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
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	appendStringInfo(&strinfo, "UPDATE %s SET data = data - ARRAY["
			"'olr_host', "
			"'olr_port', "
			"'olr_source'] "
			"WHERE name = '%s'",
			SYNCHDB_CONNINFO_TABLE,
			NameStr(*name));
	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
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
	int connectorId = -1;
	pid_t pid = InvalidPid;
	StringInfoData strinfo;
	initStringInfo(&strinfo);
	synchdb_init_shmem();

	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	/* if connector is running, we will attemppt to shut it down */
	connectorId = get_shm_connector_id_by_name(NameStr(*name), get_database_name(MyDatabaseId));
	if (connectorId >= 0)
	{
		pid = get_shm_connector_pid(connectorId);
		if (pid != InvalidPid)
		{
			elog(WARNING, "terminating dbz connector (%s) with pid %d. Shutdown timeout: %d ms",
					NameStr(*name), (int)pid, DEBEZIUM_SHUTDOWN_TIMEOUT_MSEC);
			DirectFunctionCall2(pg_terminate_backend, UInt32GetDatum(pid), Int64GetDatum(DEBEZIUM_SHUTDOWN_TIMEOUT_MSEC));
			set_shm_connector_pid(connectorId, InvalidPid);

		}
	}

	/* remove the connector info record */
	appendStringInfo(&strinfo, "DELETE FROM %s WHERE name = '%s';"
			"DELETE FROM %s WHERE name = '%s';"
			"DELETE FROM %s WHERE name = '%s';",
			SYNCHDB_CONNINFO_TABLE,
			NameStr(*name),
			SYNCHDB_ATTRIBUTE_TABLE,
			NameStr(*name),
			SYNCHDB_OBJECT_MAPPING_TABLE,
			NameStr(*name));

	ra_executeCommand(strinfo.data);
	remove_dbz_metadata_files(NameStr(*name));

	PG_RETURN_INT32(0);
}

/*
 * synchdb_del_objmap
 *
 * This function marks a objmap rule as disabled
 */
Datum
synchdb_del_objmap(PG_FUNCTION_ARGS)
{
	/* Parse input arguments */
	Name name = PG_GETARG_NAME(0);
	Name objtype = PG_GETARG_NAME(1);
	Name srcobj = PG_GETARG_NAME(2);

	StringInfoData strinfo;
	initStringInfo(&strinfo);

	if (strcasecmp(NameStr(*objtype), "table") && strcasecmp(NameStr(*objtype), "column") &&
		strcasecmp(NameStr(*objtype), "datatype") && strcasecmp(NameStr(*objtype), "transform"))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unsupported object type %s", NameStr(*objtype))));
	}

	appendStringInfo(&strinfo, "UPDATE %s SET "
			"enabled = false WHERE "
			"name = '%s' AND objtype = trim(lower('%s')) AND "
			"srcobj = trim(lower('%s'));",
			SYNCHDB_OBJECT_MAPPING_TABLE,
			NameStr(*name),
			NameStr(*objtype),
			NameStr(*srcobj));

	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
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
	unsigned int memorySize = PG_GETARG_UINT32(2);

	IspnInfo ispninfo = {0};
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	/* only embedded infinispan is supported as of now */
	strlcpy(ispninfo.ispn_cache_type, "embedded", INFINISPAN_TYPE_SIZE);

	if (strcasecmp(NameStr(*memoryType), "heap") && strcasecmp(NameStr(*memoryType), "off heap"))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid infinispan memory type: expect 'heap' or 'off heap'")));
	}
	strlcpy(ispninfo.ispn_memory_type, NameStr(*memoryType), INFINISPAN_TYPE_SIZE);

	if (memorySize == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid infinispan memory size")));
	}
	ispninfo.ispn_memory_size = memorySize;

	appendStringInfo(&strinfo, "UPDATE %s SET data = data || json_build_object("
			"'ispn_cache_type', '%s', "	/* only embedded infinispan is supported */
			"'ispn_memory_type', '%s', "
			"'ispn_memory_size', %u)::jsonb "
			"WHERE name = '%s'",
			SYNCHDB_CONNINFO_TABLE,
			ispninfo.ispn_cache_type,
			ispninfo.ispn_memory_type,
			ispninfo.ispn_memory_size,
			NameStr(*name));

	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
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
	/* Parse input arguments */
	int connectorId;
	pid_t pid;
	char * ispn_dir = NULL;
	Name name = PG_GETARG_NAME(0);

	StringInfoData strinfo;
	initStringInfo(&strinfo);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(NameStr(*name), get_database_name(MyDatabaseId));
	if (connectorId > 0)
	{
		pid = get_shm_connector_pid(connectorId);
		if (pid != InvalidPid)
		{
			/* still running, check the connector state */
			ereport(ERROR,
					(errmsg("the connector needs to be in stopped state before infinispan "
							"settings can be removed"),
					errhint("use synchdb_stop_engine_bgw() to stop it first")));
		}
	}

	/* remove on-disk cache and metadata created by infinispan */
	ispn_dir = psprintf(SYNCHDB_INFINISPAN_DIR, NameStr(*name), get_database_name(MyDatabaseId));

	if (!rmtree(ispn_dir, true))
		elog(WARNING, "some infinispan metadata files may be left behind in %s",
				ispn_dir);

	appendStringInfo(&strinfo, "UPDATE %s SET data = data - ARRAY["
			"'ispn_cache_type', "
			"'ispn_memory_type', "
			"'ispn_memory_size'] "
			"WHERE name = '%s'",
			SYNCHDB_CONNINFO_TABLE,
			NameStr(*name));
	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
}

Datum
synchdb_translate_datatype(PG_FUNCTION_ARGS)
{
	Name type = PG_GETARG_NAME(0);
	Name ext_datatype = PG_GETARG_NAME(1);
	int ext_datatype_len = PG_GETARG_INT32(2);
	int ext_datatype_scale = PG_GETARG_INT32(3);
	int ext_datatype_precision = PG_GETARG_INT32(4);

	char * pg_datatype = NULL;
	int pg_datatype_len = 0;
	StringInfoData strinfo;
	ConnectorType connectorType = TYPE_UNDEF;

	(void)ext_datatype_precision;
	initStringInfo(&strinfo);

	connectorType = stringToConnectorType(NameStr(*type));
	if (connectorType == TYPE_UNDEF)
	{
		elog(WARNING, "unsupported connector type %s", NameStr(*type));
		PG_RETURN_TEXT_P(cstring_to_text("text"));
	}

	if (fc_translate_datatype(connectorType, NameStr(*ext_datatype),
			ext_datatype_len, ext_datatype_scale,
			&pg_datatype, &pg_datatype_len))
	{
		appendStringInfo(&strinfo, "%s", pg_datatype);

		/* pg_datatype_len == -1 means to use original length */
		if (pg_datatype_len == -1)
			pg_datatype_len = ext_datatype_len;

		if (pg_datatype_len > 0 && ext_datatype_scale > 0)
		{
			appendStringInfo(&strinfo, "(%d, %d)",
					pg_datatype_len, ext_datatype_scale);
			PG_RETURN_TEXT_P(cstring_to_text(strinfo.data));
		}

		if (pg_datatype_len > 0 && (ext_datatype_scale == 0 || ext_datatype_scale == -1))
		{
			appendStringInfo(&strinfo, "(%d)",
					pg_datatype_len);
			PG_RETURN_TEXT_P(cstring_to_text(strinfo.data));
		}

		PG_RETURN_TEXT_P(cstring_to_text(strinfo.data));
	}

	/* no mapping available, default to text */
	PG_RETURN_TEXT_P(cstring_to_text("text"));
}

Datum
synchdb_set_snapstats(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	int64 tables_inc = PG_GETARG_INT64(1);
	int64 rows_inc = PG_GETARG_INT64(2);
	int64 begintime = PG_GETARG_INT64(3);
	int64 endtime = PG_GETARG_INT64(4);
	SnapshotStatistics mysnapstats;
	int connectorId = -1;

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(NameStr(*name), get_database_name(MyDatabaseId));
	if (connectorId < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("no connector id assigned for connector %s", NameStr(*name))));
	}

	mysnapstats.snapstats_tables = (unsigned long long) tables_inc;
	mysnapstats.snapstats_rows = (unsigned long long) rows_inc;
	mysnapstats.snapstats_begintime_ts = (unsigned long long) begintime;
	mysnapstats.snapstats_endtime_ts = (unsigned long long) endtime;

	set_shm_connector_snapshot_statistics(connectorId, &mysnapstats);

	PG_RETURN_VOID();
}
