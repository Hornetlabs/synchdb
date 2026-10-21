/* Construction, control and offset JNI calls, independent of worker globals. */
#include "postgres.h"

#include "runtime/java_jni.h"
#include "runtime/java_operations.h"
#include "runtime/worker_runtime.h"
#include "snapshot/artifact_store.h"
#include "synchdb/synchdb.h"

#define PARAMETERS_RESULT "Lcom/example/DebeziumRunner$MyParameters;"

static bool
java_operation_live(const SynchdbJavaRuntime *java)
{
	return java != NULL && java->creation_succeeded && !java->destroy_attempted &&
		java->vm != NULL && java->env != NULL && java->runner_bound &&
		java->cls != NULL && java->obj != NULL && !java->stop_failed &&
		(!java->stop_requested || java->stop_confirmed);
}

static bool
java_batch_settled(const SynchdbJavaRuntime *java)
{
	return java->frame_ref == NULL &&
		(java->batch_phase == SYNCHDB_JAVA_BATCH_IDLE ||
		 java->batch_phase == SYNCHDB_JAVA_BATCH_ACK_ACCEPTED ||
		 java->batch_phase == SYNCHDB_JAVA_BATCH_COMPLETED);
}

static jmethodID
java_method(JNIEnv *env, jclass cls, const char *name, const char *signature)
{
	jmethodID method = (*env)->GetMethodID(env, cls, name, signature);

	if (synchdb_java_exception(env) || method == NULL)
	{
		elog(WARNING, "failed to resolve Java method %s", name);
		return NULL;
	}
	return method;
}

static bool
java_set_parameter(JNIEnv *env, jclass cls, jobject parameters,
				   const char *name, const char *signature, const jvalue *arguments)
{
	jmethodID method = java_method(env, cls, name, signature);
	jobject returned;
	jboolean same;

	if (method == NULL)
		return false;
	returned = (*env)->CallObjectMethodA(env, parameters, method, arguments);
	if (synchdb_java_exception(env) || returned == NULL)
	{
		elog(WARNING, "failed to apply Java parameter %s", name);
		return false;
	}
	/* This protocol uses mutable fluent setters. A replacement object is not
	 * silently discarded, nor can it replace the constructor's original owner. */
	same = (*env)->IsSameObject(env, returned, parameters);
	(*env)->DeleteLocalRef(env, returned);
	if (synchdb_java_exception(env) || same != JNI_TRUE)
	{
		elog(WARNING, "Java parameter %s did not return its original object", name);
		return false;
	}
	return true;
}

static bool
java_set_integer(JNIEnv *env, jclass cls, jobject parameters, const char *name, int value)
{
	jvalue argument = {.i = (jint) value};

	return java_set_parameter(env, cls, parameters, name, "(I)" PARAMETERS_RESULT, &argument);
}

static bool
java_set_string(JNIEnv *env, jclass cls, jobject parameters, const char *name, const char *value)
{
	jstring text = synchdb_java_string(env, value);
	jvalue argument = {.l = text};
	bool result;

	if (text == NULL)
		return false;
	result = java_set_parameter(env, cls, parameters, name,
								"(Ljava/lang/String;)" PARAMETERS_RESULT, &argument);
	(*env)->DeleteLocalRef(env, text);
	return result;
}

static bool
java_set_artifact_parameters(JNIEnv *env, jclass cls, jobject parameters,
							const SynchdbArtifactPaths *artifacts)
{
	jvalue resume = {.z = artifacts->resume_existing ? JNI_TRUE : JNI_FALSE};
	jvalue legacy[2] = {0};
	bool result;

	if (!java_set_string(env, cls, parameters, "setArtifactBase", artifacts->base) ||
		!java_set_parameter(env, cls, parameters, "setArtifactResume", "(Z)" PARAMETERS_RESULT, &resume))
		return false;
	if (artifacts->postgres_legacy_slot[0] == '\0')
		return true;
	legacy[0].l = synchdb_java_string(env, artifacts->postgres_legacy_slot);
	if (legacy[0].l == NULL)
		return false;
	legacy[1].l = synchdb_java_string(env, artifacts->postgres_legacy_publication);
	if (legacy[1].l == NULL)
	{
		(*env)->DeleteLocalRef(env, legacy[0].l);
		return false;
	}
	result = java_set_parameter(env, cls, parameters, "setPostgresLegacyIdentity",
		"(Ljava/lang/String;Ljava/lang/String;)" PARAMETERS_RESULT, legacy);
	(*env)->DeleteLocalRef(env, legacy[0].l);
	(*env)->DeleteLocalRef(env, legacy[1].l);
	return result;
}

static bool
java_set_extra_parameters(JNIEnv *env, jclass cls, jobject parameters,
						  const ConnectionInfo *connection)
{
	const SynchdbRunOptions *options = &connection->run_options;
	const ExtraConnectionInfo *extra = &connection->extra;
	const char *ssl_names[] = {"setSslmode", "setSslKeystore", "setSslKeystorePass",
		"setSslTruststore", "setSslTruststorePass"};
	const char *ssl_values[] = {extra->ssl_mode, extra->ssl_keystore,
		extra->ssl_keystore_pass, extra->ssl_truststore, extra->ssl_truststore_pass};
	jvalue boolean_argument = {.z = options->dbz_capture_only_selected_table_ddl ? JNI_TRUE : JNI_FALSE};
	int index;

	if (!java_set_integer(env, cls, parameters, "setBatchSize", options->dbz_batch_size) ||
		!java_set_integer(env, cls, parameters, "setQueueSize", options->dbz_queue_size) ||
		!java_set_integer(env, cls, parameters, "setConnectTimeout", options->dbz_connect_timeout_ms) ||
		!java_set_integer(env, cls, parameters, "setQueryTimeout", options->dbz_query_timeout_ms) ||
		!java_set_string(env, cls, parameters, "setSkippedOperations", options->dbz_skipped_operations) ||
		!java_set_integer(env, cls, parameters, "setSnapshotThreadNum", options->dbz_snapshot_thread_num) ||
		!java_set_integer(env, cls, parameters, "setSnapshotFetchSize", options->dbz_snapshot_fetch_size) ||
		!java_set_integer(env, cls, parameters, "setSnapshotMinRowToStreamResults", options->dbz_snapshot_min_row_to_stream_results) ||
		!java_set_integer(env, cls, parameters, "setIncrementalSnapshotChunkSize", options->dbz_incremental_snapshot_chunk_size) ||
		!java_set_string(env, cls, parameters, "setIncrementalSnapshotWatermarkingStrategy", options->dbz_incremental_snapshot_watermarking_strategy) ||
		!java_set_integer(env, cls, parameters, "setOffsetFlushIntervalMs", options->dbz_offset_flush_interval_ms) ||
		!java_set_parameter(env, cls, parameters, "setCaptureOnlySelectedTableDDL",
							"(Z)" PARAMETERS_RESULT, &boolean_argument))
		return false;

	for (index = 0; index < (int) lengthof(ssl_names); index++)
		if (strcasecmp(ssl_values[index], "null") != 0 &&
			!java_set_string(env, cls, parameters, ssl_names[index], ssl_values[index]))
			return false;
	if (!java_set_integer(env, cls, parameters, "setLogLevel", options->dbz_log_level))
		return false;

	if (strcasecmp(connection->olr.olr_host, "null") != 0 && connection->olr.olr_port != 0)
	{
		jvalue arguments[3] = {0};

		arguments[0].l = synchdb_java_string(env, connection->olr.olr_host);
		if (arguments[0].l == NULL)
			return false;
		arguments[1].i = (jint) connection->olr.olr_port;
		arguments[2].l = synchdb_java_string(env, connection->olr.olr_source);
		if (arguments[2].l == NULL ||
			!java_set_parameter(env, cls, parameters, "setOlr",
								"(Ljava/lang/String;ILjava/lang/String;)" PARAMETERS_RESULT, arguments))
			return false;
		(*env)->DeleteLocalRef(env, arguments[0].l);
		(*env)->DeleteLocalRef(env, arguments[2].l);
	}
	if (strcasecmp(connection->ispn.ispn_cache_type, "null") != 0)
	{
		jvalue arguments[3] = {0};

		arguments[0].l = synchdb_java_string(env, connection->ispn.ispn_cache_type);
		if (arguments[0].l == NULL)
			return false;
		arguments[1].l = synchdb_java_string(env, connection->ispn.ispn_memory_type);
		if (arguments[1].l == NULL)
			return false;
		arguments[2].i = (jint) connection->ispn.ispn_memory_size;
		if (!java_set_parameter(env, cls, parameters, "setIspn",
								"(Ljava/lang/String;Ljava/lang/String;I)" PARAMETERS_RESULT, arguments))
			return false;
		(*env)->DeleteLocalRef(env, arguments[0].l);
		(*env)->DeleteLocalRef(env, arguments[1].l);
	}
	/* Preserve the existing adapter selection; committed-mode policy is separate. */
	if (options->dbz_logminer_stream_mode != LOGMINER_MODE_UNDEF &&
		!java_set_string(env, cls, parameters, "setLogminerStreamMode", "logminer"))
		return false;
	return java_set_integer(env, cls, parameters, "setCdcDelay", options->cdc_start_delay_ms);
}

static void
java_prepare_artifacts(const ConnectionInfo *connection, int connector_type,
					   SynchdbArtifactPaths *artifacts)
{
	bool previous = synchdb_worker_sql_interruptible();

	PG_TRY();
	{
		synchdb_worker_sql_enter();
		synchdb_artifact_store_prepare(connection, connector_type, artifacts);
	}
	PG_FINALLY();
	{
		synchdb_worker_sql_restore(previous);
	}
	PG_END_TRY();
}

int
synchdb_java_start(SynchdbJavaRuntime *java, const ConnectionInfo *connection,
				   int connector_type, const char *snapshot_mode)
{
	JNIEnv *env;
	SynchdbArtifactPaths artifacts;
	volatile int result = -1;

	if (!java_operation_live(java) || connection == NULL || snapshot_mode == NULL ||
		connection->run_options.version != SYNCHDB_RUN_OPTIONS_VERSION || !java_batch_settled(java) ||
		(java->engine_start_attempted && !java->stop_confirmed))
	{
		elog(WARNING, "Java session is not ready to start an engine");
		return -1;
	}
	/* Persist ownership before a throwing Java call can create engine threads.
	 * This transaction never publishes an FDW seed or rewrites a live offset. */
	java_prepare_artifacts(connection, connector_type, &artifacts);
	env = java->env;
	if (!synchdb_java_local_begin(env, 32))
		return -1;
	PG_TRY();
	{
		do
		{
			jclass cls;
			jmethodID constructor;
			jmethodID start;
			jobject parameters;
			jvalue arguments[13] = {0};
			const char *texts[] = {connection->name, connection->hostname, connection->user,
				connection->pwd, connection->srcdb,
				strncmp(connection->table, "file:", 5) == 0 ? psprintf("file:%s", artifacts.table_filter) : connection->table,
				strncmp(connection->snapshottable, "file:", 5) == 0 ? psprintf("file:%s", artifacts.snapshot_filter) : connection->snapshottable,
				snapshot_mode, connection->dstdb, connection->srcschema};
			const int positions[] = {1, 3, 5, 6, 7, 8, 9, 10, 11, 12};
			bool strings_ready = true;
			int index;

			cls = (*env)->FindClass(env, "com/example/DebeziumRunner$MyParameters");
			if (synchdb_java_exception(env) || cls == NULL)
				break;
			constructor = java_method(env, cls, "<init>",
				"(Lcom/example/DebeziumRunner;Ljava/lang/String;ILjava/lang/String;ILjava/lang/String;"
				"Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;"
				"Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
			if (constructor == NULL)
				break;
			arguments[0].l = java->obj;
			arguments[2].i = (jint) connector_type;
			arguments[4].i = (jint) connection->port;
			for (index = 0; index < (int) lengthof(texts); index++)
			{
				arguments[positions[index]].l = synchdb_java_string(env, texts[index]);
				if (arguments[positions[index]].l == NULL)
				{
					strings_ready = false;
					break;
				}
			}
			if (!strings_ready)
				break;
			parameters = (*env)->NewObjectA(env, cls, constructor, arguments);
			if (synchdb_java_exception(env) || parameters == NULL ||
				!java_set_extra_parameters(env, cls, parameters, connection) ||
				!java_set_artifact_parameters(env, cls, parameters, &artifacts))
				break;
			start = java_method(env, java->cls, "startEngine", "(" PARAMETERS_RESULT ")V");
			if (start == NULL || !synchdb_java_batch_reset(java))
				break;
			arguments[0].l = parameters;
			/* A throwing start may already have created engine threads. */
			java->engine_start_attempted = true;
			(*env)->CallVoidMethodA(env, java->obj, start, arguments);
			if (synchdb_java_exception(env))
			{
				java->batch_phase = SYNCHDB_JAVA_BATCH_FAILED;
				break;
			}
			result = 0;
		} while (false);
	}
	PG_FINALLY();
	{
		(*env)->PopLocalFrame(env, NULL);
	}
	PG_END_TRY();
	return result;
}

static int
java_write_offset(SynchdbJavaRuntime *java, int connector_type, const char *database,
				  const char *offset, const char *file, const char *method_name)
{
	JNIEnv *env;
	volatile int result = -1;

	if (!java_operation_live(java) || !java_batch_settled(java) ||
		(java->engine_start_attempted && !java->stop_confirmed) ||
		database == NULL || offset == NULL || file == NULL)
		return -1;
	env = java->env;
	if (!synchdb_java_local_begin(env, 8))
		return -1;
	PG_TRY();
	{
		do
		{
			jmethodID method = java_method(env, java->cls, method_name,
										"(Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;)V");
			jvalue arguments[4] = {0};

			if (method == NULL)
				break;
			arguments[0].l = synchdb_java_string(env, file);
			if (arguments[0].l == NULL)
				break;
			arguments[1].i = (jint) connector_type;
			arguments[2].l = synchdb_java_string(env, database);
			if (arguments[2].l == NULL)
				break;
			arguments[3].l = synchdb_java_string(env, offset);
			if (arguments[3].l == NULL)
				break;
			(*env)->CallVoidMethodA(env, java->obj, method, arguments);
			if (!synchdb_java_exception(env))
				result = 0;
		} while (false);
	}
	PG_FINALLY();
	{
		(*env)->PopLocalFrame(env, NULL);
	}
	PG_END_TRY();
	return result;
}

int
synchdb_java_set_offset(SynchdbJavaRuntime *java, int connector_type,
						const char *database, const char *offset, const char *file)
{
	return java_write_offset(java, connector_type, database, offset, file, "setConnectorOffset");
}

int
synchdb_java_create_offset(SynchdbJavaRuntime *java, int connector_type,
						   const char *database, const char *offset, const char *file)
{
	return java_write_offset(java, connector_type, database, offset, file, "createOffsetFile");
}

static int
java_diagnostic(SynchdbJavaRuntime *java, const char *name, const char *signature, const jvalue *arguments)
{
	JNIEnv *env;
	volatile int result = -1;

	if (!java_operation_live(java))
		return -1;
	env = java->env;
	if (!synchdb_java_local_begin(env, 2))
		return -1;
	PG_TRY();
	{
		jmethodID method = java_method(env, java->cls, name, signature);

		if (method != NULL)
		{
			(*env)->CallVoidMethodA(env, java->obj, method, arguments);
			if (!synchdb_java_exception(env))
				result = 0;
		}
	}
	PG_FINALLY();
	{
		(*env)->PopLocalFrame(env, NULL);
	}
	PG_END_TRY();
	return result;
}

int
synchdb_java_memory_dump(SynchdbJavaRuntime *java)
{
	return java_diagnostic(java, "jvmMemDump", "()V", NULL);
}

int
synchdb_java_set_loglevel(SynchdbJavaRuntime *java, int level)
{
	jvalue argument = {.i = (jint) level};

	return java_diagnostic(java, "changeLogLevel", "(I)V", &argument);
}
