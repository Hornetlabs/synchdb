/* JNI transport ownership and the target-commit/acknowledgement boundary. */
#include "postgres.h"

#include "runtime/java_runtime.h"
#include "runtime/java_jni.h"
#include "utils/memutils.h"

static bool
java_batch_live(const SynchdbJavaRuntime *java)
{
	return java != NULL && java->creation_succeeded && !java->destroy_attempted &&
		java->vm != NULL && java->env != NULL && java->runner_bound &&
		java->cls != NULL && java->obj != NULL;
}

static bool
java_batch_operable(const SynchdbJavaRuntime *java)
{
	return java_batch_live(java) && !java->stop_requested && !java->stop_failed;
}


void
synchdb_java_release_frame(SynchdbJavaRuntime *java)
{
	if (java == NULL || java->frame_ref == NULL)
		return;
	/* A failed creation/destruction may have left unusable interface pointers. */
	if (!java->creation_succeeded || java->destroy_attempted ||
		java->vm == NULL || java->env == NULL)
		return;
	(*java->env)->DeleteGlobalRef(java->env, java->frame_ref);
	java->frame_ref = NULL;
}

bool
synchdb_java_poll_frame(SynchdbJavaRuntime *java, SynchdbDbzFrame *frame)
{
	jobject volatile local_buffer = NULL;
	volatile bool found = false;

	if (java == NULL || frame == NULL)
		elog(ERROR, "invalid Java frame poll arguments");
	if (java->frame_ref != NULL ||
		(java->batch_phase != SYNCHDB_JAVA_BATCH_IDLE &&
		 java->batch_phase != SYNCHDB_JAVA_BATCH_ACK_ACCEPTED))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Java batch session is not ready for another poll")));
	MemSet(frame, 0, sizeof(*frame));
	java->batch_phase = SYNCHDB_JAVA_BATCH_IDLE;
	java->batch_id = -1;
	PG_TRY();
	{
		JNIEnv *env;
		jmethodID method;
		jlong capacity;
		const unsigned char *data;

		if (!java_batch_operable(java))
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Java batch session has no live runner")));
		env = java->env;
		if (synchdb_java_exception(env))
			elog(ERROR, "Java exception was pending before polling Debezium");
		if (java->get_change_events == NULL)
		{
			method = (*env)->GetMethodID(env, java->cls, "getChangeEvents", "()Ljava/nio/ByteBuffer;");
			if (synchdb_java_exception(env) || method == NULL)
				elog(ERROR, "Failed to find getChangeEvents method");
			java->get_change_events = method;
		}
		local_buffer = (*env)->CallObjectMethod(env, java->obj, java->get_change_events);
		if (synchdb_java_exception(env))
			elog(ERROR, "Exception occurred while calling getChangeEvents");
		if (local_buffer != NULL)
		{
			/* The runtime owns the root before any operation can raise ERROR. */
			java->frame_ref = (*env)->NewGlobalRef(env, local_buffer);
			(*env)->DeleteLocalRef(env, local_buffer);
			local_buffer = NULL;
			if (synchdb_java_exception(env) || java->frame_ref == NULL)
				elog(ERROR, "Failed to retain Debezium direct buffer");
			capacity = (*env)->GetDirectBufferCapacity(env, java->frame_ref);
			if (synchdb_java_exception(env))
				elog(ERROR, "Failed to read Debezium direct buffer capacity");
			if (capacity <= 0 || (uint64) capacity > (uint64) MaxAllocSize)
				ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
								errmsg("invalid Debezium direct buffer capacity")));
			data = (*env)->GetDirectBufferAddress(env, java->frame_ref);
			if (synchdb_java_exception(env))
				elog(ERROR, "Failed to read Debezium direct buffer address");
			if (data == NULL)
				ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
								errmsg("invalid Debezium direct buffer address")));
			synchdb_dbz_frame_decode(data, (Size) capacity, frame);
			if (frame->kind == SYNCHDB_DBZ_BATCH)
			{
				java->batch_phase = SYNCHDB_JAVA_BATCH_DELIVERED;
				java->batch_id = frame->batch_id;
			}
			else
				java->batch_phase = frame->success ?
					SYNCHDB_JAVA_BATCH_COMPLETED : SYNCHDB_JAVA_BATCH_FAILED;
			found = true;
		}
	}
	PG_CATCH();
	{
		if (java_batch_live(java))
			(void) synchdb_java_exception(java->env);
		if (local_buffer != NULL)
			(*java->env)->DeleteLocalRef(java->env, local_buffer);
		java->batch_phase = SYNCHDB_JAVA_BATCH_FAILED;
		java->batch_id = -1;
		synchdb_java_release_frame(java);
		MemSet(frame, 0, sizeof(*frame));
		PG_RE_THROW();
	}
	PG_END_TRY();
	return found;
}

void
synchdb_java_batch_committed(SynchdbJavaRuntime *java, int32 batch_id)
{
	if (java == NULL || batch_id < 0 ||
		java->batch_phase != SYNCHDB_JAVA_BATCH_DELIVERED || java->batch_id != batch_id)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Java batch does not match the committed target batch")));
	java->batch_phase = SYNCHDB_JAVA_BATCH_TARGET_COMMITTED;
}

void
synchdb_java_batch_failed(SynchdbJavaRuntime *java, bool commit_unknown)
{
	if (java == NULL)
		return;
	if (java->batch_phase == SYNCHDB_JAVA_BATCH_DELIVERED)
		java->batch_phase = commit_unknown ?
			SYNCHDB_JAVA_BATCH_COMMIT_UNKNOWN : SYNCHDB_JAVA_BATCH_APPLY_ABORTED;
	else if (java->batch_phase == SYNCHDB_JAVA_BATCH_TARGET_COMMITTED)
		java->batch_phase = SYNCHDB_JAVA_BATCH_ACK_UNKNOWN;
}

int
synchdb_java_ack_batch(SynchdbJavaRuntime *java, int32 batch_id)
{
	JNIEnv *env;
	jmethodID method;

	/* A rejected identity must not poison the batch that can still be confirmed. */
	if (java == NULL || batch_id < 0 || java->batch_id != batch_id)
		return -1;
	if (!java_batch_operable(java))
	{
		if (java->batch_phase == SYNCHDB_JAVA_BATCH_TARGET_COMMITTED)
			java->batch_phase = SYNCHDB_JAVA_BATCH_ACK_FAILED;
		return -1;
	}
	if (java->batch_phase == SYNCHDB_JAVA_BATCH_ACK_ACCEPTED)
		return 0;
	if (java->batch_phase != SYNCHDB_JAVA_BATCH_TARGET_COMMITTED)
		return -1;
	/* Until invocation starts, failure is known to have sent no confirmation. */
	java->batch_phase = SYNCHDB_JAVA_BATCH_ACK_FAILED;
	env = java->env;
	if (synchdb_java_exception(env))
		return -1;
	if (java->mark_batch_complete == NULL)
	{
		method = (*env)->GetMethodID(env, java->cls, "markBatchComplete", "(IZII)V");
		if (synchdb_java_exception(env) || method == NULL)
			return -1;
		java->mark_batch_complete = method;
	}
	java->batch_phase = SYNCHDB_JAVA_BATCH_ACK_UNKNOWN;
	(*env)->CallVoidMethod(env, java->obj, java->mark_batch_complete,
						  batch_id, JNI_TRUE, -1, -1);
	if (synchdb_java_exception(env))
		return -1;
	java->batch_phase = SYNCHDB_JAVA_BATCH_ACK_ACCEPTED;
	return 0;
}

bool
synchdb_java_batch_reset(SynchdbJavaRuntime *java)
{
	if (!java_batch_live(java) || java->frame_ref != NULL ||
		java->stop_failed || java->stop_requested != java->stop_confirmed ||
		(java->batch_phase != SYNCHDB_JAVA_BATCH_IDLE &&
		 java->batch_phase != SYNCHDB_JAVA_BATCH_ACK_ACCEPTED &&
		 java->batch_phase != SYNCHDB_JAVA_BATCH_COMPLETED))
		return false;
	if (synchdb_java_exception(java->env))
		return false;
	java->batch_phase = SYNCHDB_JAVA_BATCH_IDLE;
	java->batch_id = -1;
	java->stop_requested = false;
	java->stop_confirmed = false;
	java->stop_failed = false;
	return true;
}

char *
synchdb_java_get_offset(SynchdbJavaRuntime *java, int connector_type,
						 const char *database, const char *name, const char *destination)
{
	JNIEnv *env;
	char *volatile output = NULL;

	/* A confirmed pause may inspect the persisted offset without restarting
	 * delivery. An unresolved or failed close still rejects source I/O. */
	if (!java_batch_live(java) || java->stop_failed ||
		(java->stop_requested && !java->stop_confirmed) ||
		database == NULL || name == NULL || destination == NULL)
		return NULL;
	env = java->env;
	if (!synchdb_java_local_begin(env, 8))
		return NULL;
	PG_TRY();
	{
		do
		{
			jstring jdatabase;
			jstring jname;
			jstring jdestination;
			jstring result;

			if (java->get_offsets == NULL)
			{
				jmethodID method = (*env)->GetMethodID(env, java->cls, "getConnectorOffset",
					"(ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;");

				if (synchdb_java_exception(env) || method == NULL)
					break;
				java->get_offsets = method;
			}
			jdatabase = synchdb_java_string(env, database);
			if (jdatabase == NULL)
				break;
			jname = synchdb_java_string(env, name);
			if (jname == NULL)
				break;
			jdestination = synchdb_java_string(env, destination);
			if (jdestination == NULL)
				break;
			result = (jstring) (*env)->CallObjectMethod(env, java->obj, java->get_offsets,
													connector_type, jdatabase, jname, jdestination);
			if (synchdb_java_exception(env) || result == NULL)
				break;
			output = synchdb_java_string_copy(env, result);
			if (output != NULL && output[0] == '\0')
			{
				pfree(output);
				output = pstrdup("no offset");
			}
		} while (false);
	}
	PG_FINALLY();
	{
		(*env)->PopLocalFrame(env, NULL);
	}
	PG_END_TRY();
	return output;
}
