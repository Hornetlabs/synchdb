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
#include "utils/builtins.h"
#include <jni.h>
#include <unistd.h>
#include "format_converter.h"
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
#include "synchdb.h"
#include "replication_agent.h"
#include "access/xact.h"
#include "utils/snapmgr.h"

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

/* Constants */
#define SYNCHDB_METADATA_DIR "pg_synchdb"
#define DBZ_ENGINE_JAR_FILE "dbz-engine-1.0.0.jar"
#define MAX_PATH_LENGTH 1024
#define MAX_JAVA_OPTION_LENGTH 256

/* Global variables */
SynchdbSharedState *sdb_state = NULL; /* Pointer to shared-memory state. */
int myConnectorId = -1;	/* Global index number to SynchdbSharedState in shared memory - global per worker */
BatchInfo myBatchInfo;
ExtraConnectionInfo extraConnInfo = {0}; /* global extra connector parameters read from rule file */

/* GUC variables */
int synchdb_worker_naptime = 500;
bool synchdb_dml_use_spi = false;
bool synchdb_auto_launcher = true;
int dbz_batch_size = 2048;
int dbz_queue_size = 8192;
char * dbz_skipped_operations = "t";
int dbz_connect_timeout_ms = 30000;
int dbz_query_timeout_ms = 600000;
int jvm_max_heap_size = 0;
int dbz_snapshot_thread_num = 2;
int dbz_snapshot_fetch_size = 0; /* 0: auto */
int dbz_snapshot_min_row_to_stream_results = 0; /* 0: always stream */
int dbz_incremental_snapshot_chunk_size = 2048;
char * dbz_incremental_snapshot_watermarking_strategy = "insert_insert";
int dbz_offset_flush_interval_ms = 60000;
bool dbz_capture_only_selected_table_ddl = true;

/* JNI-related variables */
static JavaVM *jvm = NULL; /* represents java vm instance */
static JNIEnv *env = NULL; /* represents JNI run-time environment */
static jclass cls;		   /* represents debezium runner java class */
static jobject obj;		   /* represents debezium runner java class object */

/* Function declarations */
PGDLLEXPORT void synchdb_engine_main(Datum main_arg);
PGDLLEXPORT void synchdb_auto_launcher_main(Datum main_arg);

/* Static function prototypes */
static int dbz_engine_stop(void);
static int dbz_engine_init(JNIEnv *env, jclass *cls, jobject *obj);
static int dbz_engine_get_change(JavaVM *jvm, JNIEnv *env, jclass *cls, jobject *obj, int myConnectorId, bool * dbzExitSignal, BatchInfo * batchinfo);
static int dbz_engine_start(const ConnectionInfo *connInfo, ConnectorType connectorType, const char * snapshotMode);
static char *dbz_engine_get_offset(int connectorId);
static int dbz_mark_batch_complete(int batchid);
static TupleDesc synchdb_state_tupdesc(void);
static void synchdb_init_shmem(void);
static void synchdb_detach_shmem(int code, Datum arg);
static void prepare_bgw(BackgroundWorker *worker, const ConnectionInfo *connInfo, const char *connector, int connectorid, const char * snapshotMode);
static const char *connectorStateAsString(ConnectorState state);
static void reset_shm_request_state(int connectorId);
static int dbz_engine_set_offset(ConnectorType connectorType, char *db, char *offset, char *file);
static void processRequestInterrupt(const ConnectionInfo *connInfo, ConnectorType type, int connectorId, const char * snapshotMode);
static void setup_environment(ConnectorType * connectorType, ConnectionInfo *conninfo, char ** snapshotMode);
static void initialize_jvm(void);
static void start_debezium_engine(ConnectorType connectorType, const ConnectionInfo *connInfo, const char * snapshotMode);
static void main_loop(ConnectorType connectorType, const ConnectionInfo *connInfo, const char * snapshotMode);
static void cleanup(ConnectorType connectorType);
static void set_extra_dbz_parameters(jobject myParametersObj, jclass myParametersClass);

/*
 * set_extra_dbz_parameters - configures extra paramters for Debezium runner
 *
 * This function builds myParametersObj with extra parameters to be passed
 * to the Debezium Java side.
 *
 * @return: void
 */
static void set_extra_dbz_parameters(jobject myParametersObj, jclass myParametersClass)
{
	jmethodID setBatchSize, setQueueSize, setSkippedOperations, setConnectTimeout, setQueryTimeout;
	jmethodID setSnapshotThreadNum, setSnapshotFetchSize, setSnapshotMinRowToStreamResults;
	jmethodID setIncrementalSnapshotChunkSize, setIncrementalSnapshotWatermarkingStrategy;
	jmethodID setOffsetFlushIntervalMs, setCaptureOnlySelectedTableDDL;
	jmethodID setSslmode, setSslKeystore, setSslKeystorePass, setSslTruststore, setSslTruststorePass;
	jstring jdbz_skipped_operations, jdbz_watermarking_strategy;
	jstring jdbz_sslmode, jdbz_sslkeystore, jdbz_sslkeystorepass, jdbz_ssltruststore, jdbz_ssltruststorepass;

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

	if (extraConnInfo.ssl_mode != NULL)
	{
		jdbz_sslmode = (*env)->NewStringUTF(env, extraConnInfo.ssl_mode);

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

	if (extraConnInfo.ssl_keystore != NULL)
	{
		jdbz_sslkeystore = (*env)->NewStringUTF(env, extraConnInfo.ssl_keystore);

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

	if (extraConnInfo.ssl_keystore_pass != NULL)
	{
		jdbz_sslkeystorepass = (*env)->NewStringUTF(env, extraConnInfo.ssl_keystore_pass);

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

	if (extraConnInfo.ssl_truststore != NULL)
	{
		jdbz_ssltruststore = (*env)->NewStringUTF(env, extraConnInfo.ssl_truststore);

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

	if (extraConnInfo.ssl_truststore_pass != NULL)
	{
		jdbz_ssltruststorepass = (*env)->NewStringUTF(env, extraConnInfo.ssl_truststore_pass);

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

static void
processCompletionMessage(const char * eventStr, int myConnectorId, bool * dbzExitSignal)
{
	char * msgcopy = pstrdup(eventStr + 2); /* skip K- */
	char * successflag = NULL;
	char * message = NULL;

	elog(DEBUG1, "completion message: %s", msgcopy);

	/*
	 * success flag indicates if worker exits successfully or due to an error.
	 * Currently not used
	 */
	successflag = strtok(msgcopy, ";");

	/* remove compiler warning */
	if (successflag && strlen(successflag) > 0)
	{
		/* presence of successflag indicates that the DBZ connector in java has exited */
		elog(DEBUG1, "successflag = %s", successflag);
		*dbzExitSignal = true;
	}

	/* message is the last error or exit message the connector produced before
	 * it exited
	 */
	message = strtok(NULL, ";");
	if (message)
	{
		/* save it to shm for display to use */
		set_shm_connector_errmsg(myConnectorId, message);
	}
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
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_get_change(JavaVM *jvm, JNIEnv *env, jclass *cls, jobject *obj, int myConnectorId, bool * dbzExitSignal, BatchInfo * batchinfo)
{
	jmethodID getChangeEvents, sizeMethod, getMethod;
	jobject changeEventsList;
	jint size;
	jclass listClass;
	jobject event;
	const char *eventStr;

	/* Validate input parameters */
	if (!jvm || !env || !cls || !obj)
	{
		elog(WARNING, "dbz_engine_get_change: Invalid input parameters");
		return -1;
	}

	/* Get the getChangeEvents method */
	getChangeEvents = (*env)->GetMethodID(env, *cls, "getChangeEvents", "()Ljava/util/List;");
	if (getChangeEvents == NULL)
	{
		elog(WARNING, "Failed to find getChangeEvents method");
		return -1;
	}

	/* Call getChangeEvents method */
	changeEventsList = (*env)->CallObjectMethod(env, *obj, getChangeEvents);

	if ((*env)->ExceptionCheck(env))
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while calling getChangeEvents");
		return -1;
	}

	if (changeEventsList == NULL)
	{
		elog(WARNING, "dbz_engine_get_change: getChangeEvents returned null");
		return -1;
	}

	/* Get List class and methods */
	listClass = (*env)->FindClass(env, "java/util/List");
	if (listClass == NULL)
	{
		elog(WARNING, "Failed to find java list class");
		(*env)->DeleteLocalRef(env, changeEventsList);
		return -1;
	}

	sizeMethod = (*env)->GetMethodID(env, listClass, "size", "()I");
	if (sizeMethod == NULL)
	{
		elog(WARNING, "Failed to find java list.size method");
		(*env)->DeleteLocalRef(env, changeEventsList);
		(*env)->DeleteLocalRef(env, listClass);
		return -1;
	}

	getMethod = (*env)->GetMethodID(env, listClass, "get", "(I)Ljava/lang/Object;");
	if (getMethod == NULL)
	{
		elog(WARNING, "Failed to find java list.get method");
		return -1;
	}

	/* Process change events */
	size = (*env)->CallIntMethod(env, changeEventsList, sizeMethod);
	elog(DEBUG1, "dbz_engine_get_change: Retrieved %d change events", size);

	if (size == 0 || size < 0)
	{
		/* nothing to process, set current stage to CDC and return */
		if (get_shm_connector_stage_enum(myConnectorId) != STAGE_CHANGE_DATA_CAPTURE)
		{
			set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
		}
		(*env)->DeleteLocalRef(env, changeEventsList);
		(*env)->DeleteLocalRef(env, listClass);
		return -1;
	}
	batchinfo->batchSize = size - 1;	/* minus the metadata record */

	/* fetch special metadata element at index 0 and convert it to string */
	event = (*env)->CallObjectMethod(env, changeEventsList, getMethod, 0);
	if (event == NULL)
	{
		elog(WARNING, "dbz_engine_get_change: missing metadata element at index 0");
		(*env)->DeleteLocalRef(env, changeEventsList);
		(*env)->DeleteLocalRef(env, listClass);
		return -1;
	}
	eventStr = (*env)->GetStringUTFChars(env, (jstring)event, 0);
	if (eventStr == NULL)
	{
		elog(WARNING, "dbz_engine_get_change: Failed to convert metadata element to string");
		(*env)->DeleteLocalRef(env, event);
		(*env)->DeleteLocalRef(env, changeEventsList);
		(*env)->DeleteLocalRef(env, listClass);
		return -1;
	}

	/* check if it is a completion message */
	if (eventStr[0] == 'K' && eventStr[1] == '-')
	{
		/*
		 * connector completion/error message, consume it right here
		 * This may also indicates that the dbz connector on java side
		 * has exited and we may need to exit later as well.
		 */
		processCompletionMessage(eventStr, myConnectorId, dbzExitSignal);
        (*env)->ReleaseStringUTFChars(env, (jstring)event, eventStr);
        (*env)->DeleteLocalRef(env, event);
        (*env)->DeleteLocalRef(env, changeEventsList);
        (*env)->DeleteLocalRef(env, listClass);
		return 0;
	}
	/* check if it is a batch change request */
	else if (eventStr[0] == 'B' && eventStr[1] == '-')
	{
		/*
		 * obtain the batch id as we will need it to commit debezium offsets
		 * as we process the batch
		 */
		batchinfo->batchId = atoi(&eventStr[2]);
		elog(WARNING, "Synchdb received batchid(%d) with size(%d)", batchinfo->batchId, size-1);

		/* free reference to metadata element at index 0 */
		(*env)->ReleaseStringUTFChars(env, (jstring)event, eventStr);
		(*env)->DeleteLocalRef(env, event);

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		/* now process the rest of the changes in the batch */
		for (int i = 1; i < size; i++)
		{
			event = (*env)->CallObjectMethod(env, changeEventsList, getMethod, i);
			if (event == NULL)
			{
				elog(WARNING, "dbz_engine_get_change: Received NULL event at index %d", i);
				continue;
			}

			eventStr = (*env)->GetStringUTFChars(env, (jstring)event, 0);
			if (eventStr == NULL)
			{
				elog(WARNING, "dbz_engine_get_change: Failed to get string for event at index %d", i);
				(*env)->DeleteLocalRef(env, event);
				continue;
			}

			elog(DEBUG1, "Processing DBZ Event: %s", eventStr);
			/* change event message, send to format converter */
			if (fc_processDBZChangeEvent(eventStr) != 0)
			{
				elog(DEBUG1, "dbz_engine_get_change: Failed to process event at index %d", i);
			}

			(*env)->ReleaseStringUTFChars(env, (jstring)event, eventStr);
			(*env)->DeleteLocalRef(env, event);
		}

		PopActiveSnapshot();
		CommitTransactionCommand();

		/* read offset currently flushed to file for displaying to user */
		set_shm_dbz_offset(myConnectorId);
	}
	else
	{
		elog(WARNING, "unknown change request");
	}

	(*env)->DeleteLocalRef(env, changeEventsList);
	(*env)->DeleteLocalRef(env, listClass);
	return 0;
}

/*
 * dbz_engine_start - Start the Debezium engine
 *
 * This function starts the Debezium engine with the provided connection information.
 *
 * @param connInfo: Pointer to the ConnectionInfo structure containing connection details
 * @param connectorType: The type of connector to start
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_start(const ConnectionInfo *connInfo, ConnectorType connectorType, const char * snapshotMode)
{
	jmethodID mid, paramConstruct;
	jstring jHostname, jUser, jPassword, jDatabase, jTable, jName, jSnapshot;
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
			"Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
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
	jName = (*env)->NewStringUTF(env, connInfo->name);
	jSnapshot = (*env)->NewStringUTF(env, snapshotMode);

	myParametersObj = (*env)->NewObject(env, myParametersClass, paramConstruct, obj,
			jName, connectorType, jHostname, connInfo->port, jUser, jPassword,
			jDatabase, jTable, jSnapshot);
	if (!myParametersObj)
	{
		elog(WARNING, "failed to create MyParameters object");
		return -1;
	}

	/* set extra parameters */
	set_extra_dbz_parameters(myParametersObj, myParametersClass);

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

	return exception ? -1 : 0;
}

/*
 * dbz_engine_get_offset - Get the current offset from the Debezium engine
 *
 * This function retrieves the current offset for a specific connector type
 * from the Debezium engine.
 *
 * @param connectorType: The type of connector to get the offset for
 *
 * @return: The offset as a string (caller must free), or NULL on failure
 */
static char *
dbz_engine_get_offset(int connectorId)
{
	jmethodID getoffsets;
	jstring jdb, result, jName;
	char *resultStr = NULL;
	char *db = NULL, *name = NULL;
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

	/* Get the unique name */
	name = sdb_state->connectors[connectorId].conninfo.name;
	if (!name)
	{
		elog(WARNING, "Unique name not set for connector type: %d",
				sdb_state->connectors[connectorId].type);
		return NULL;
	}

	getoffsets = (*env)->GetMethodID(env, cls, "getConnectorOffset",
									 "(ILjava/lang/String;Ljava/lang/String;)Ljava/lang/String;");
	if (getoffsets == NULL)
	{
		elog(WARNING, "Failed to find getConnectorOffset method");
		return NULL;
	}

	jdb = (*env)->NewStringUTF(env, db);
	jName = (*env)->NewStringUTF(env, name);

	result = (jstring)(*env)->CallObjectMethod(env, obj, getoffsets,
			(int)sdb_state->connectors[connectorId].type, jdb, jName);
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
 * dbz_engine_memory_dump - prints current heap and non-heap memory allocated
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
 * Returns: A blessed TupleDesc, or NULL on failure
 */
static TupleDesc
synchdb_state_tupdesc(void)
{
	TupleDesc tupdesc;
	AttrNumber attrnum = 8;
	AttrNumber a = 0;

	tupdesc = CreateTemplateTupleDesc(attrnum);

	/* todo: add more columns here per connector if needed */
	TupleDescInitEntry(tupdesc, ++a, "id", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "connector", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "conninfo_name", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "pid", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "stage", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "state", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "err", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "last_dbz_offset", TEXTOID, -1, 0);

	Assert(a == maxattr);
	return BlessTupleDesc(tupdesc);
}

/*
 * synchdb_init_shmem - Initialize or attach to synchdb shared memory
 *
 * Allocate and initialize synchdb related shared memory, if not already
 * done, and set up backend-local pointer to that state.
 *
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
		for (i = 0; i < SYNCHDB_MAX_ACTIVE_CONNECTORS; i++)
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

	elog(LOG, "synchdb detach shm ... myConnectorId %d, code %d, myBatchId %d",
			DatumGetUInt32(arg), code, myBatchInfo.batchId);

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
 * @param snapshotMode: Snapshot mode to use to start Debezium engine
 */
static void
prepare_bgw(BackgroundWorker *worker, const ConnectionInfo *connInfo, const char *connector, int connectorid, const char * snapshotMode)

{
	ConnectorType type = fc_get_connector_type(connector);

	worker->bgw_main_arg = UInt32GetDatum(connectorid);
	snprintf(worker->bgw_name, BGW_MAXLEN, "synchdb engine: %s@%s:%u", connector, connInfo->hostname, connInfo->port);
	snprintf(worker->bgw_type, BGW_MAXLEN, "synchdb engine: %s", connector);

	/* append destination database to worker->bgw_name for clarity */
	strcat(worker->bgw_name, " -> ");
	strcat(worker->bgw_name, connInfo->dstdb);

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
	LWLockRelease(&sdb_state->lock);
}

/*
 * connectorStateAsString - Convert ConnectorState to string representation
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
	}
	return "UNKNOWN";
}

/*
 * reset_shm_request_state - Reset the shared memory request state for a connector
 *
 * This function resets the request state and clears the request data for a
 * specific connector type in shared memory.
 *
 * @param type: The type of connector whose request state should be reset
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

/**
 * processRequestInterrupt - Handles state transition requests for SynchDB connectors
 *
 * This function processes requests to change the state of a SynchDB connector,
 * such as pausing, resuming, or updating offsets.
 *
 * @param connInfo: Pointer to the connection information
 * @param type: The type of connector being processed
 */
static void
processRequestInterrupt(const ConnectionInfo *connInfo, ConnectorType type, int connectorId, const char * snapshotMode)
{
	SynchdbRequest *req, *reqcopy;
	ConnectorState *currstate, *currstatecopy;
	char offsetfile[SYNCHDB_JSON_PATH_SIZE] = {0};
	char *srcdb;
	int ret;

	if (!sdb_state)
		return;

	req = &(sdb_state->connectors[connectorId].req);
	currstate = &(sdb_state->connectors[connectorId].state);
	srcdb = sdb_state->connectors[connectorId].conninfo.srcdb;

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
	if (reqcopy->reqstate == STATE_UNDEF)
	{
		/* no requests, do nothing */
	}
	else if (reqcopy->reqstate == STATE_PAUSED && *currstatecopy == STATE_SYNCING)
	{
		/* we can only transition to STATE_PAUSED from STATE_SYNCING */
		elog(LOG, "Pausing %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));

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
	else if (reqcopy->reqstate == STATE_SYNCING && *currstatecopy == STATE_PAUSED)
	{
		/* Handle resume request, we can only transition to STATE_SYNCING from STATE_PAUSED */
		elog(LOG, "Resuming %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));

		/* restart dbz engine */
		elog(DEBUG1, "restart dbz engine...");

		ret = dbz_engine_start(connInfo, type, snapshotMode);
		if (ret < 0)
		{
			elog(WARNING, "Failed to restart dbz engine");
			reset_shm_request_state(connectorId);
			pfree(reqcopy);
			pfree(currstatecopy);
			return;
		}
		set_shm_connector_state(connectorId, STATE_SYNCING);
	}
	else if (reqcopy->reqstate == STATE_OFFSET_UPDATE && *currstatecopy == STATE_PAUSED)
	{
		/* Handle offset update request */
		elog(LOG, "Updating offset for %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));

		/* derive offset file*/
		snprintf(offsetfile, SYNCHDB_JSON_PATH_SIZE, SYNCHDB_OFFSET_FILE_PATTERN,
				get_shm_connector_name(type), connInfo->name);

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
	else if (reqcopy->reqstate == STATE_RESTARTING && *currstatecopy == STATE_SYNCING)
	{
		ConnectionInfo newConnInfo = {0};

		elog(WARNING, "got a restart request: %s", reqcopy->reqdata);
		set_shm_connector_state(connectorId, STATE_RESTARTING);

		/* get a copy of more recent conninfo from reqdata */
		memcpy(&newConnInfo, &(reqcopy->reqconninfo), sizeof(ConnectionInfo));

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
				"dst_db %s, table %s, rulefile %s snapshotMode %s",
				newConnInfo.hostname, newConnInfo.port, newConnInfo.user,
				newConnInfo.srcdb ? newConnInfo.srcdb : "N/A",
				newConnInfo.dstdb,
				newConnInfo.table ? newConnInfo.table : "N/A",
				newConnInfo.rulefile, reqcopy->reqdata);

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
	else if (reqcopy->reqstate == STATE_MEMDUMP)
	{
		/* Handle offset update request */
		elog(LOG, "Requesting memdump for %s connector",
				connInfo->name);

		set_shm_connector_state(connectorId, STATE_MEMDUMP);
		/* todo: call the java routine to dump jvm heap memory summary in log */

		dbz_engine_memory_dump();

		/* after new offset is set, change state back to STATE_PAUSED */
		set_shm_connector_state(connectorId, STATE_SYNCING);
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

/**
 * setup_environment - Prepares the environment for the SynchDB background worker
 *
 * This function sets up signal handlers, initializes the database connection,
 * sets up shared memory, and checks for existing worker processes.
 *
 * @param connectorType: The type of connector being set up
 * @param dst_db: The name of the destination database to connect to
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

	elog(LOG, "obtained conninfo from shm: myConnectorId %d, name %s, host %s, port %u, "
			"user %s, src_db %s, dst_db %s, table %s, connectorType %u (%s), conninfo_name %s"
			" rulefile %s snapshotMode %s",
			myConnectorId, conninfo->name,
			conninfo->hostname, conninfo->port, conninfo->user,
			conninfo->srcdb ? conninfo->srcdb : "N/A",
			conninfo->dstdb,
			conninfo->table ? conninfo->table : "N/A",
			*connectorType, connectorTypeToString(*connectorType),
			conninfo->name, conninfo->rulefile, *snapshotMode);

	elog(LOG, "Environment setup completed for SynchDB %s worker (type %u)",
		 connectorTypeToString(*connectorType), *connectorType);
}

/**
 * initialize_jvm - Initialize the Java Virtual Machine and Debezium engine
 *
 * This function sets up the Java environment, locates the Debezium engine JAR file,
 * creates a Java VM, and initializes the Debezium engine.
 *
 * @param connectorType: The type of connector being initialized
 */
static void
initialize_jvm(void)
{
	JavaVMInitArgs vm_args;
	JavaVMOption options[3];
	char javaopt[MAX_JAVA_OPTION_LENGTH] = {0};
	char jvmheapmax[MAX_JAVA_OPTION_LENGTH] = {0};
	char jar_path[MAX_PATH_LENGTH] = {0};
	const char *dbzpath = getenv("DBZ_ENGINE_DIR");
	int ret;

	/* Determine the path to the Debezium engine JAR file */
	if (dbzpath)
	{
		snprintf(jar_path, sizeof(jar_path), "%s/%s", dbzpath, DBZ_ENGINE_JAR_FILE);
	}
	else
	{
		snprintf(jar_path, sizeof(jar_path), "%s/dbz_engine/%s", pkglib_path, DBZ_ENGINE_JAR_FILE);
	}

	/* Check if the JAR file exists */
	if (access(jar_path, F_OK) == -1)
	{
		set_shm_connector_errmsg(myConnectorId, "Cannot find DBZ engine jar file");
		elog(ERROR, "Cannot find DBZ engine jar file at %s", jar_path);
	}

	/* Set up Java classpath and heap size */
	snprintf(javaopt, sizeof(javaopt), "-Djava.class.path=%s", jar_path);
	if (jvm_max_heap_size == 0)
		snprintf(jvmheapmax, sizeof(javaopt), "-Xmx0");
	else
		snprintf(jvmheapmax, sizeof(javaopt), "-Xmx%dm", jvm_max_heap_size);

	elog(WARNING, "Initializing JVM with options: -Xrs %s %s", javaopt, jvmheapmax);

	/* Configure JVM options */
	options[0].optionString = javaopt;
	options[1].optionString = "-Xrs"; // Reduce use of OS signals by JVM
	options[2].optionString = jvmheapmax;
	vm_args.version = JNI_VERSION_10;
	vm_args.nOptions = 3;
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
}

/**
 * start_debezium_engine - Starts the Debezium engine for a given connector
 *
 * This function initiates the Debezium engine using the provided connection
 * information and sets the connector state to SYNCING upon successful start.
 *
 * @param connectorType: The type of connector being started
 * @param connInfo: Pointer to the ConnectionInfo structure containing connection details
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

static void
main_loop(ConnectorType connectorType, const ConnectionInfo *connInfo, const char * snapshotMode)
{
	ConnectorState currstate;
	bool dbzExitSignal = false;

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

		processRequestInterrupt(connInfo, connectorType, myConnectorId, snapshotMode);

		currstate = get_shm_connector_state_enum(myConnectorId);
		switch (currstate)
		{
		case STATE_SYNCING:
			dbz_engine_get_change(jvm, env, &cls, &obj, myConnectorId, &dbzExitSignal, &myBatchInfo);

			/*
			 * if a valid batchid is set by dbz_engine_get_change(), it means we have
			 * successfully completed a batch change request and we shall notify dbz
			 * that it's been completed.
			 */
			if (myBatchInfo.batchId != SYNCHDB_INVALID_BATCH_ID)
				dbz_mark_batch_complete(myBatchInfo.batchId);

			/* we are done with a batch, make it invalid */
			myBatchInfo.batchId = SYNCHDB_INVALID_BATCH_ID;
			break;
		case STATE_PAUSED:
			/* Do nothing when paused */
			break;
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

static void
cleanup(ConnectorType connectorType)
{
	int ret;

	elog(WARNING, "synchdb_engine_main shutting down");

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

	fc_deinitFormatConverter(connectorType);
}

static int
assign_connector_id(char * name)
{
	int i = 0;

	/*
	 * first, check if "name" has been used in one of the connector slots.
	 * If yes, return its index
	 */
	for (i = 0; i < SYNCHDB_MAX_ACTIVE_CONNECTORS; i++)
	{
		if (!strcasecmp(sdb_state->connectors[i].conninfo.name, name))
		{
			return i;
		}
	}

	/* if not, find the next unnamed free slot */
	for (i = 0; i < SYNCHDB_MAX_ACTIVE_CONNECTORS; i++)
	{
		if (sdb_state->connectors[i].state == STATE_UNDEF &&
				strlen(sdb_state->connectors[i].conninfo.name) == 0)
		{
			return i;
		}
	}

	/* if not, find the next free slot */
	for (i = 0; i < SYNCHDB_MAX_ACTIVE_CONNECTORS; i++)
	{
		if (sdb_state->connectors[i].state == STATE_UNDEF)
		{
			return i;
		}
	}
	return -1;
}

static int
get_shm_connector_id_by_name(const char * name)
{
	int i = 0;

	if (!sdb_state)
		return -1;

	for (i = 0; i < SYNCHDB_MAX_ACTIVE_CONNECTORS; i++)
	{
		if (!strcmp(sdb_state->connectors[i].conninfo.name, name))
		{
			return i;
		}
	}
	return -1;
}

static int
dbz_mark_batch_complete(int batchid)
{
	jmethodID markBatchComplete;
	jthrowable exception;
	jboolean jmarkall = JNI_TRUE;

	/*
	 * todo: markfrom and markto are no longer supported because we no longer
	 * support partial batch completion. To be deleted later.
	 */
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

	/* Find the markBatchComplete method */
	markBatchComplete = (*env)->GetMethodID(env, cls, "markBatchComplete",
									 "(IZII)V");
	if (markBatchComplete == NULL)
	{
		elog(WARNING, "Failed to find markBatchComplete method");
		return -1;
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
	elog(DEBUG1, "Synchdb -> Debezium: sent batchid(%d) completion request", batchid);
	return 0;
}

void
synchdb_auto_launcher_main(Datum main_arg)
{
	int ret = -1, numout = 0, i = 0;
	char * out[SYNCHDB_MAX_ACTIVE_CONNECTORS];

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
	ret = ra_listConnInfoNames(out, &numout);
	if (ret == 0)
	{
		for (i = 0; i < (numout > SYNCHDB_MAX_ACTIVE_CONNECTORS ?
				SYNCHDB_MAX_ACTIVE_CONNECTORS : numout); i++)
		{
			elog(WARNING, "launching %s...", out[i]);
			DirectFunctionCall1(synchdb_start_engine_bgw, CStringGetTextDatum(out[i]));
			sleep(2);
		}
	}
	elog(DEBUG1, "stop synchdb_auto_launcher_main");
}

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
	default:
		return "UNKNOWN";
	}
}

/* public functions to access / change synchdb parameters in shared memory */
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
	/* todo: support more dbz connector types here */
	default:
		return "null";
	}
}

pid_t
get_shm_connector_pid(int connectorId)
{
	if (!sdb_state)
		return InvalidPid;

	return sdb_state->connectors[connectorId].pid;
}

void
set_shm_connector_pid(int connectorId, pid_t pid)
{
	if (!sdb_state)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	sdb_state->connectors[connectorId].pid = pid;
	LWLockRelease(&sdb_state->lock);
}

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
 * @param type: The type of connector for which to set the error message
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

static const char *
get_shm_connector_stage(int connectorId)
{
	ConnectorStage stage;

	if (!sdb_state)
		return STATE_UNDEF;

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
		case STAGE_UNDEF:
		default:
		{
			break;
		}
	}
	return "unknown";
}

ConnectorStage
get_shm_connector_stage_enum(int connectorId)
{
	ConnectorStage stage;

	if (!sdb_state)
		return STATE_UNDEF;

	/*
	 * We're only reading, so shared lock is sufficient.
	 * This ensures thread-safety without blocking other readers.
	 */
	LWLockAcquire(&sdb_state->lock, LW_SHARED);
	stage = sdb_state->connectors[connectorId].stage;
	LWLockRelease(&sdb_state->lock);

	return stage;
}

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
 * @param type: The type of connector for which to get the state
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
 * @param type: The type of connector for which to get the state
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
 * @param type: The type of connector for which to set the state
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

/* TODO: set_shm_dbz_offset:
 * This method reads from dbz's offset file per connector type, which does not
 * reflect the real-time offset of dbz engine. If we were to resume from this point
 * due to an error, there may be duplicate values after the resume in which we must
 * handle. In the future, we will need to explore a more accurate way to find out
 * the offset managed within dbz so we could freely resume from any reference not
 * just at the flushed locations
 */
void
set_shm_dbz_offset(int connectorId)
{
	char *offset;

	if (!sdb_state)
		return;

	offset = dbz_engine_get_offset(connectorId);
	if (!offset)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	strlcpy(sdb_state->connectors[connectorId].dbzoffset, offset, SYNCHDB_ERRMSG_SIZE);
	LWLockRelease(&sdb_state->lock);

	pfree(offset);
}

const char *
get_shm_dbz_offset(int connectorId)
{
	if (!sdb_state)
		return "n/a";

	return (sdb_state->connectors[connectorId].dbzoffset[0] != '\0') ?
			sdb_state->connectors[connectorId].dbzoffset : "no offset";
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
							100,
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
}

/*
 * Finalization function
 */
void
_PG_fini(void)
{
	elog(DEBUG1," shutdown synchdb");
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

	/* initialize batchinfo */
	myBatchInfo.batchId = SYNCHDB_INVALID_BATCH_ID;
	myBatchInfo.batchSize = 0;

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

	/* initialize format converter */
	fc_initFormatConverter(connectorType);

	/* load custom rules + additional connector parameters if applicable */
	if (connInfo.rulefile && strlen(connInfo.rulefile) > 0 && strcasecmp(connInfo.rulefile, "null"))
		fc_load_rules(connectorType, connInfo.rulefile);

	/* Initialize JVM */
	initialize_jvm();

	/* read current offset and update shm */
	memset(sdb_state->connectors[myConnectorId].dbzoffset, 0, SYNCHDB_ERRMSG_SIZE);
	set_shm_dbz_offset(myConnectorId);

	/* start Debezium engine */
	start_debezium_engine(connectorType, &connInfo, snapshotMode);

	elog(LOG, "Going to main loop .... ");
	/* Main processing loop */
	main_loop(connectorType, &connInfo, snapshotMode);

	elog(LOG, "synchdb worker shutting down .... ");
	if (snapshotMode)
		pfree(snapshotMode);

	proc_exit(0);
}

Datum
synchdb_start_engine_bgw_snapshot_mode(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t pid;
	ConnectionInfo connInfo;
	char *connector = NULL;
	int ret = -1, connectorid = -1;
	StringInfoData strinfo;
	char * snapshotMode = "initial";

	/* Parse input arguments */
	text *name_text = PG_GETARG_TEXT_PP(0);
	text *snapshotmode_text = PG_GETARG_TEXT_PP(1);

	/* Sanity check on input arguments */
	if (VARSIZE(name_text) - VARHDRSZ == 0 ||
			VARSIZE(name_text) - VARHDRSZ > SYNCHDB_CONNINFO_NAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connection name cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_NAME_SIZE)));
	}


	if (VARSIZE(snapshotmode_text) - VARHDRSZ == 0 ||
			VARSIZE(snapshotmode_text) - VARHDRSZ > SYNCHDB_SNAPSHOT_MODE_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("snapshot mode cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_NAME_SIZE)));
	}

	ret = ra_getConninfoByName(text_to_cstring(name_text), &connInfo, &connector);
	if (ret)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connection name does not exist"),
				 errhint("use synchdb_add_conninfo to add one first")));

	snapshotMode = text_to_cstring(snapshotmode_text);

	/*
	 * attach or initialize synchdb shared memory area so we can assign
	 * a connector ID for this worker
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorid = assign_connector_id(connInfo.name);
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
			"WHERE name = '%s'", text_to_cstring(name_text));

	ra_executeCommand(strinfo.data);

	PG_RETURN_INT32(0);
}

Datum
synchdb_start_engine_bgw(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t pid;
	ConnectionInfo connInfo;
	char *connector = NULL;
	int ret = -1, connectorid = -1;
	StringInfoData strinfo;
	/* By default, we use snapshot mode = initial */
	char * snapshotMode = "initial";

	/* Parse input arguments */
	text *name_text = PG_GETARG_TEXT_PP(0);

	/* Sanity check on input arguments */
	if (VARSIZE(name_text) - VARHDRSZ == 0 ||
			VARSIZE(name_text) - VARHDRSZ > SYNCHDB_CONNINFO_NAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connection name cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_NAME_SIZE)));
	}

	ret = ra_getConninfoByName(text_to_cstring(name_text), &connInfo, &connector);
	if (ret)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connection name does not exist"),
				 errhint("use synchdb_add_conninfo to add one first")));

	/*
	 * attach or initialize synchdb shared memory area so we can assign
	 * a connector ID for this worker
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorid = assign_connector_id(connInfo.name);
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
			"WHERE name = '%s'", text_to_cstring(name_text));

	ra_executeCommand(strinfo.data);

	PG_RETURN_INT32(0);
}

Datum
synchdb_stop_engine_bgw(PG_FUNCTION_ARGS)
{
	int connectorId;
	pid_t pid;
	StringInfoData strinfo;

	/* Parse input arguments */
	text *name_text = PG_GETARG_TEXT_PP(0);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(text_to_cstring(name_text));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	/*
	 * mark this conninfo as inactive so it will not automatically resume running
	 * at postgresql server restarts given that synchdb is included in
	 * shared_preload_library GUC
	 */
	initStringInfo(&strinfo);
	appendStringInfo(&strinfo, "UPDATE synchdb_conninfo set isactive = false "
			"WHERE name = '%s'", text_to_cstring(name_text));

	ra_executeCommand(strinfo.data);

	pid = get_shm_connector_pid(connectorId);
	if (pid != InvalidPid)
	{
		elog(WARNING, "terminating dbz connector (%s) with pid %d. Shutdown timeout: %d ms",
				text_to_cstring(name_text), (int)pid, DEBEZIUM_SHUTDOWN_TIMEOUT_MSEC);
		DirectFunctionCall2(pg_terminate_backend, UInt32GetDatum(pid), Int64GetDatum(DEBEZIUM_SHUTDOWN_TIMEOUT_MSEC));
		set_shm_connector_pid(connectorId, InvalidPid);

	}
	else
	{
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not running", text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));
	}
	PG_RETURN_INT32(0);
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

	/*
	 * todo: put 3 to a dynamic MACRO as the number of connector synchdb currently
	 * can support
	 */
	if (*idx < SYNCHDB_MAX_ACTIVE_CONNECTORS)
	{
		Datum values[8];
		bool nulls[8] = {0};
		HeapTuple tuple;

		LWLockAcquire(&sdb_state->lock, LW_SHARED);
		values[0] = Int32GetDatum(*idx);
		values[1] = CStringGetTextDatum(get_shm_connector_name(sdb_state->connectors[*idx].type));
		values[2] = CStringGetTextDatum(sdb_state->connectors[*idx].conninfo.name);
		values[3] = Int32GetDatum((int)get_shm_connector_pid(*idx));
		values[4] = CStringGetTextDatum(get_shm_connector_stage(*idx));
		values[5] = CStringGetTextDatum(get_shm_connector_state(*idx));
		values[6] = CStringGetTextDatum(get_shm_connector_errmsg(*idx));
		values[7] = CStringGetTextDatum(get_shm_dbz_offset(*idx));
		LWLockRelease(&sdb_state->lock);

		*idx += 1;

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	SRF_RETURN_DONE(funcctx);
}

Datum
synchdb_pause_engine(PG_FUNCTION_ARGS)
{
	int connectorId = -1;
	pid_t pid;
	SynchdbRequest *req;

	/* Parse input arguments */
	text *name_text = PG_GETARG_TEXT_PP(0);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(text_to_cstring(name_text));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not running", text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector %s",
						text_to_cstring(name_text)),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_PAUSED;
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent pause request interrupt to dbz connector %s (%d)",
			text_to_cstring(name_text), connectorId);
	PG_RETURN_INT32(0);
}

Datum
synchdb_resume_engine(PG_FUNCTION_ARGS)
{
	int connectorId = -1;
	pid_t pid;
	SynchdbRequest *req;

	/* Parse input arguments */
	text *name_text = PG_GETARG_TEXT_PP(0);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(text_to_cstring(name_text));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector id (%s) is not running", text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector id %s",
						text_to_cstring(name_text)),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_SYNCING;
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent resume request interrupt to dbz connector (%s)",
			text_to_cstring(name_text));
	PG_RETURN_INT32(0);
}

Datum
synchdb_set_offset(PG_FUNCTION_ARGS)
{
	pid_t pid;
	int connectorId = -1;
	char *offsetstr;
	SynchdbRequest *req;
	ConnectorState currstate;

	/* Parse input arguments */
	text *name_text = PG_GETARG_TEXT_PP(0);
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

	connectorId = get_shm_connector_id_by_name(text_to_cstring(name_text));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not running",
						text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	currstate = get_shm_connector_state_enum(connectorId);
	if (currstate != STATE_PAUSED)
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not in paused state.",
						text_to_cstring(name_text)),
				 errhint("use synchdb_pause_engine() to pause the worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector %s",
						text_to_cstring(name_text)),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_OFFSET_UPDATE;
	strncpy(req->reqdata, offsetstr, SYNCHDB_ERRMSG_SIZE);
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent update offset request interrupt to dbz connector (%s)",
			text_to_cstring(name_text));
	PG_RETURN_INT32(0);
}

Datum
synchdb_add_conninfo(PG_FUNCTION_ARGS)
{
	text *name_text = PG_GETARG_TEXT_PP(0);
	text *hostname_text = PG_GETARG_TEXT_PP(1);
	unsigned int port = PG_GETARG_UINT32(2);
	text *user_text = PG_GETARG_TEXT_PP(3);
	text *pwd_text = PG_GETARG_TEXT_PP(4);
	text *src_db_text = PG_GETARG_TEXT_PP(5);
	text *dst_db_text = PG_GETARG_TEXT_PP(6);
	text *table_text = PG_GETARG_TEXT_PP(7);
	text *connector_text = PG_GETARG_TEXT_PP(8);
	text *rulefile_text = PG_GETARG_TEXT_PP(9);
	char *connector = NULL;

	ConnectionInfo connInfo = {0};
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	/* Sanity check on input arguments */
	if (VARSIZE(name_text) - VARHDRSZ == 0 ||
			VARSIZE(name_text) - VARHDRSZ > SYNCHDB_CONNINFO_NAME_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("name cannot be empty or longer than %d",
						 SYNCHDB_CONNINFO_NAME_SIZE)));
	}
	strlcpy(connInfo.name, text_to_cstring(name_text), SYNCHDB_CONNINFO_NAME_SIZE);

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

	if (VARSIZE(connector_text) - VARHDRSZ == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connector type cannot be empty")));
	}
	connector = text_to_cstring(connector_text);

	/* todo: check more */
	if (strcasecmp(connector, "mysql") && strcasecmp(connector, "sqlserver"))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported connector")));

	/* rulefile can be empty or NULL */
	if (VARSIZE(rulefile_text) - VARHDRSZ == 0)
		strlcpy(connInfo.rulefile, "null", SYNCHDB_CONNINFO_RULEFILENAME_SIZE);
	else if (VARSIZE(rulefile_text) - VARHDRSZ > SYNCHDB_CONNINFO_RULEFILENAME_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("rule filename cannot be longer than %d",
						 SYNCHDB_CONNINFO_RULEFILENAME_SIZE)));
	else
		strlcpy(connInfo.rulefile, text_to_cstring(rulefile_text), SYNCHDB_CONNINFO_RULEFILENAME_SIZE);

	appendStringInfo(&strinfo, "INSERT INTO %s (name, isactive, data)"
			" VALUES ('%s', %s, jsonb_build_object('hostname', '%s', "
			"'port', %d, 'user', '%s', 'pwd', pgp_sym_encrypt('%s', '%s'), "
			"'srcdb', '%s', 'dstdb', '%s', 'table', '%s', 'connector', '%s',"
			"'rule_file', '%s') );",
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
			connector,
			connInfo.rulefile);

	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
}

Datum
synchdb_restart_connector(PG_FUNCTION_ARGS)
{
	text *name_text = PG_GETARG_TEXT_PP(0);
	text *snapshot_mode_text = PG_GETARG_TEXT_PP(1);
	ConnectionInfo connInfo;
	int ret = -1, connectorId = -1;
	pid_t pid;
	char * snapshot_mode;
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
	if (VARSIZE(snapshot_mode_text) - VARHDRSZ == 0)
		snapshot_mode = "null";
	else
		snapshot_mode = text_to_cstring(snapshot_mode_text);

	ret = ra_getConninfoByName(text_to_cstring(name_text), &connInfo, &connector);
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

	connectorId = get_shm_connector_id_by_name(text_to_cstring(name_text));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not running",
						text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector %s",
						text_to_cstring(name_text)),
				 errhint("wait for it to finish and try again later")));

	/*
	 * connector info may have been changed, so let's pass the latest conninfo
	 * to the connector worker in the same way as synchdb_start_engine_bgw()
	 */
	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_RESTARTING;

	/* reqdata contains snapshot mode */
	memset(req->reqdata, 0, SYNCHDB_ERRMSG_SIZE);
	snprintf(req->reqdata, SYNCHDB_ERRMSG_SIZE,"%s", snapshot_mode);

	/*
	 * reqconninfo contains a copy of conninfo recently read in case it has been
	 * changed since connector start
	 */
	memcpy(&req->reqconninfo, &connInfo, sizeof(ConnectionInfo));
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent restart request interrupt to dbz connector (%s)",
			text_to_cstring(name_text));
	PG_RETURN_INT32(0);
}

Datum
synchdb_log_jvm_meminfo(PG_FUNCTION_ARGS)
{
	int connectorId = -1;
	pid_t pid;
	SynchdbRequest *req;

	/* Parse input arguments */
	text *name_text = PG_GETARG_TEXT_PP(0);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorId = get_shm_connector_id_by_name(text_to_cstring(name_text));
	if (connectorId < 0)
		ereport(ERROR,
				(errmsg("dbz connector (%s) does not have connector ID assigned",
						text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to assign one first")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector (%s) is not running", text_to_cstring(name_text)),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector %s",
						text_to_cstring(name_text)),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_MEMDUMP;
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent memdump request interrupt to dbz connector %s (%d)",
			text_to_cstring(name_text), connectorId);
	PG_RETURN_INT32(0);
}
