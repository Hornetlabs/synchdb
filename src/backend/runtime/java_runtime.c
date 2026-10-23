/* Process-owned JVM and persistent Debezium runner references. */
#include "postgres.h"

#include "miscadmin.h"
#include "portability/instr_time.h"
#include "runtime/java_runtime.h"
#include "runtime/java_jni.h"
#include "storage/latch.h"
#include "utils/wait_event.h"

#define SYNCHDB_JAVA_STOP_TIMEOUT_MS 5000
#define SYNCHDB_JAVA_STOP_POLL_MS 10


static void
java_clear_binding(SynchdbJavaRuntime *java)
{
	java->cls = NULL;
	java->obj = NULL;
	java->get_change_events = NULL;
	java->mark_batch_complete = NULL;
	java->get_offsets = NULL;
	java->request_stop = NULL;
	java->stop_status = NULL;
	java->runner_bound = false;
	java->engine_start_attempted = false;
}

static void
java_clear_stop(SynchdbJavaRuntime *java)
{
	java->stop_requested = false;
	java->stop_confirmed = false;
	java->stop_failed = false;
}

static void
java_clear_batch(SynchdbJavaRuntime *java)
{
	java->frame_ref = NULL;
	java->batch_phase = SYNCHDB_JAVA_BATCH_IDLE;
	java->batch_id = -1;
}

int
synchdb_java_bind(SynchdbJavaRuntime *java)
{
	JNIEnv *env;
	jclass local_class = NULL;
	jobject local_object = NULL;
	jmethodID constructor;
	jmethodID protocol_version;
	jint version;
	const char *failure;

	if (java == NULL || !java->creation_succeeded || java->vm == NULL ||
		java->env == NULL || java->destroy_attempted)
	{
		elog(WARNING, "cannot bind Debezium runner without a live Java VM");
		return -1;
	}
	if (java->runner_bound)
		return 0;
	/* A failed binding owns its partial GlobalRefs until session cleanup. */
	if (java->cls != NULL || java->obj != NULL)
	{
		elog(WARNING, "partial Debezium runner binding requires cleanup");
		return -1;
	}
	env = java->env;
	(void) synchdb_java_exception(env);

	local_class = (*env)->FindClass(env, "com/example/DebeziumRunner");
	if (synchdb_java_exception(env) || local_class == NULL)
	{
		failure = "Failed to find com.example.DebeziumRunner class";
		goto failed;
	}
	/* Persistent handles must survive operation-local JNI frames. */
	java->cls = (jclass) (*env)->NewGlobalRef(env, local_class);
	(*env)->DeleteLocalRef(env, local_class);
	local_class = NULL;
	if (synchdb_java_exception(env) || java->cls == NULL)
	{
		failure = "Failed to retain Debezium runner class";
		goto failed;
	}

	constructor = (*env)->GetMethodID(env, java->cls, "<init>", "()V");
	if (synchdb_java_exception(env) || constructor == NULL)
	{
		failure = "Failed to find Debezium runner constructor";
		goto failed;
	}
	local_object = (*env)->NewObject(env, java->cls, constructor);
	if (synchdb_java_exception(env) || local_object == NULL)
	{
		failure = "Failed to construct Debezium runner object";
		goto failed;
	}
	java->obj = (*env)->NewGlobalRef(env, local_object);
	(*env)->DeleteLocalRef(env, local_object);
	local_object = NULL;
	if (synchdb_java_exception(env) || java->obj == NULL)
	{
		failure = "Failed to retain Debezium runner object";
		goto failed;
	}
	protocol_version = (*env)->GetMethodID(env, java->cls, "getProtocolVersion", "()I");
	if (synchdb_java_exception(env) || protocol_version == NULL)
	{
		failure = "Debezium runner does not support the required JNI protocol";
		goto failed;
	}
	version = (*env)->CallIntMethod(env, java->obj, protocol_version);
	if (synchdb_java_exception(env) || version != 3)
	{
		failure = "Debezium runner JNI protocol version must be 3";
		goto failed;
	}
	java->runner_bound = true;
	return 0;

failed:
	if (local_object != NULL)
		(*env)->DeleteLocalRef(env, local_object);
	if (local_class != NULL)
		(*env)->DeleteLocalRef(env, local_class);
	elog(WARNING, "%s", failure);
	return -1;
}

SynchdbJavaStopStatus
synchdb_java_poll_stop(SynchdbJavaRuntime *java)
{
	JNIEnv *env;
	jmethodID method;
	jint status;

	if (java == NULL || java->vm == NULL)
		return SYNCHDB_JAVA_STOP_CLOSED;
	if (!java->creation_succeeded || java->destroy_attempted || java->env == NULL)
	{
		java->stop_failed = true;
		return SYNCHDB_JAVA_STOP_FAILED;
	}
	if (!java->runner_bound)
	{
		/* A partial binding has never been allowed to start an engine. */
		java->stop_confirmed = true;
		return SYNCHDB_JAVA_STOP_CLOSED;
	}
	if (java->stop_confirmed)
		return SYNCHDB_JAVA_STOP_CLOSED;
	if (!java->stop_requested || java->cls == NULL || java->obj == NULL)
	{
		java->stop_failed = true;
		return SYNCHDB_JAVA_STOP_FAILED;
	}
	env = java->env;
	if (synchdb_java_exception(env))
		java->stop_failed = true;
	if (java->stop_status == NULL)
	{
		method = (*env)->GetMethodID(env, java->cls, "stopStatus", "()I");
		if (synchdb_java_exception(env) || method == NULL)
		{
			java->stop_failed = true;
			return SYNCHDB_JAVA_STOP_FAILED;
		}
		java->stop_status = method;
	}
	status = (*env)->CallIntMethod(env, java->obj, java->stop_status);
	if (synchdb_java_exception(env))
	{
		java->stop_failed = true;
		return SYNCHDB_JAVA_STOP_FAILED;
	}
	if (status == SYNCHDB_JAVA_STOP_CLOSED)
	{
		java->stop_confirmed = true;
		return SYNCHDB_JAVA_STOP_CLOSED;
	}
	if (status != SYNCHDB_JAVA_STOP_PENDING)
	{
		java->stop_failed = true;
		return SYNCHDB_JAVA_STOP_FAILED;
	}
	return SYNCHDB_JAVA_STOP_PENDING;
}

SynchdbJavaStopStatus
synchdb_java_begin_stop(SynchdbJavaRuntime *java)
{
	JNIEnv *env;
	jmethodID method;

	if (java == NULL || java->vm == NULL)
		return SYNCHDB_JAVA_STOP_CLOSED;
	if (java->stop_requested)
		return synchdb_java_poll_stop(java);
	/* Once closing begins, no ordinary JNI operation may use this session. */
	java->stop_requested = true;
	if (!java->creation_succeeded || java->destroy_attempted || java->env == NULL)
	{
		java->stop_failed = true;
		return SYNCHDB_JAVA_STOP_FAILED;
	}
	synchdb_java_release_frame(java);
	if (!java->runner_bound)
	{
		java->stop_confirmed = true;
		return SYNCHDB_JAVA_STOP_CLOSED;
	}
	if (java->cls == NULL || java->obj == NULL)
	{
		java->stop_failed = true;
		return SYNCHDB_JAVA_STOP_FAILED;
	}
	env = java->env;
	if (synchdb_java_exception(env))
		java->stop_failed = true;
	if (java->request_stop == NULL)
	{
		method = (*env)->GetMethodID(env, java->cls, "requestStop", "()V");
		if (synchdb_java_exception(env) || method == NULL)
		{
			java->stop_failed = true;
			return SYNCHDB_JAVA_STOP_FAILED;
		}
		java->request_stop = method;
	}
	(*env)->CallVoidMethod(env, java->obj, java->request_stop);
	if (synchdb_java_exception(env))
		java->stop_failed = true;
	/* Even an exceptional call may have scheduled a successful close. */
	return synchdb_java_poll_stop(java);
}

int
synchdb_java_stop(SynchdbJavaRuntime *java)
{
	instr_time started;
	SynchdbJavaStopStatus status;

	INSTR_TIME_SET_CURRENT(started);
	status = synchdb_java_begin_stop(java);
	/* A failed attempt gets one late-status probe, never another wait budget. */
	while (status == SYNCHDB_JAVA_STOP_PENDING && !java->stop_failed)
	{
		instr_time elapsed;

		INSTR_TIME_SET_CURRENT(elapsed);
		INSTR_TIME_SUBTRACT(elapsed, started);
		if (INSTR_TIME_GET_MILLISEC(elapsed) >= SYNCHDB_JAVA_STOP_TIMEOUT_MS)
		{
			java->stop_failed = true;
			elog(WARNING, "timed out waiting for Debezium runner shutdown; Java VM remains owned");
			return -1;
		}
		if (MyLatch != NULL)
		{
			int events;

			ResetLatch(MyLatch);
			events = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   SYNCHDB_JAVA_STOP_POLL_MS, PG_WAIT_EXTENSION);
			if (events & WL_POSTMASTER_DEATH)
			{
				java->stop_failed = true;
				return -1;
			}
		}
		else
			pg_usleep(SYNCHDB_JAVA_STOP_POLL_MS * 1000L);
		CHECK_FOR_INTERRUPTS();
		status = synchdb_java_poll_stop(java);
	}
	return status == SYNCHDB_JAVA_STOP_CLOSED && (java == NULL || !java->stop_failed) ? 0 : -1;
}

bool
synchdb_java_destroy(SynchdbJavaRuntime *java)
{
	JNIEnv *env;
	jint result;

	if (java == NULL)
		return true;
	if (java->vm == NULL)
	{
		java_clear_batch(java);
		java_clear_binding(java);
		java_clear_stop(java);
		java->env = NULL;
		return true;
	}
	if (java->destroy_attempted)
		return false;
	if (!java->creation_succeeded || java->env == NULL)
	{
		java->destroy_attempted = true;
		elog(WARNING, "cannot destroy Java VM without a successful creation result");
		return false;
	}
	/* Unknown engine shutdown must not turn into an unbounded VM join. */
	if (java->runner_bound && !java->stop_confirmed)
	{
		elog(WARNING, "cannot destroy Java VM before Debezium runner shutdown is confirmed");
		return false;
	}
	env = java->env;
	(void) synchdb_java_exception(env);
	synchdb_java_release_frame(java);
	java->destroy_attempted = true;
	/* GlobalRefs require explicit disposal while their JNI environment is live. */
	if (java->obj != NULL)
	{
		(*env)->DeleteGlobalRef(env, java->obj);
		java->obj = NULL;
	}
	if (java->cls != NULL)
	{
		(*env)->DeleteGlobalRef(env, java->cls);
		java->cls = NULL;
	}
	java_clear_binding(java);
	result = (*java->vm)->DestroyJavaVM(java->vm);
	if (result != JNI_OK)
	{
		elog(WARNING, "failed to destroy connector JVM (return code: %d)", result);
		return false;
	}
	java->vm = NULL;
	java->env = NULL;
	java_clear_batch(java);
	java_clear_stop(java);
	return true;
}
