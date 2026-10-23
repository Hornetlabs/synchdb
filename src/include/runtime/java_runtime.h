/* Process-owned Java resources; session close is distinct from worker exit. */
#ifndef SYNCHDB_JAVA_RUNTIME_H
#define SYNCHDB_JAVA_RUNTIME_H

#include "postgres.h"
#include <jni.h>
#include "source/debezium_frame.h"

typedef enum SynchdbJavaBatchPhase
{
	SYNCHDB_JAVA_BATCH_IDLE = 0,
	SYNCHDB_JAVA_BATCH_DELIVERED,
	SYNCHDB_JAVA_BATCH_TARGET_COMMITTED,
	SYNCHDB_JAVA_BATCH_ACK_ACCEPTED,
	SYNCHDB_JAVA_BATCH_APPLY_ABORTED,
	SYNCHDB_JAVA_BATCH_COMMIT_UNKNOWN,
	SYNCHDB_JAVA_BATCH_ACK_FAILED,
	SYNCHDB_JAVA_BATCH_ACK_UNKNOWN,
	SYNCHDB_JAVA_BATCH_COMPLETED,
	SYNCHDB_JAVA_BATCH_FAILED
} SynchdbJavaBatchPhase;

typedef enum SynchdbJavaStopStatus
{
	SYNCHDB_JAVA_STOP_PENDING = 0,
	SYNCHDB_JAVA_STOP_CLOSED,
	SYNCHDB_JAVA_STOP_FAILED
} SynchdbJavaStopStatus;

typedef struct SynchdbJavaRuntime
{
	JavaVM *vm;
	JNIEnv *env;
	jclass cls;
	jobject obj;
	jmethodID get_change_events;
	jmethodID mark_batch_complete;
	jmethodID get_offsets;
	jmethodID request_stop;
	jmethodID stop_status;
	/* The GlobalRef roots DirectBuffer bytes until explicit release. */
	jobject frame_ref;
	SynchdbJavaBatchPhase batch_phase;
	int32 batch_id;
	bool creation_attempted;
	bool creation_succeeded;
	bool runner_bound;
	bool engine_start_attempted;
	bool destroy_attempted;
	bool stop_requested;
	bool stop_confirmed;
	bool stop_failed;
	/* OLR claims its generation before proving the external capture fence. */
	struct SynchdbArtifactPaths *prepared_artifacts;
} SynchdbJavaRuntime;

/* Bind persistent GlobalRefs. A partial binding remains owned for cleanup. */
extern int synchdb_java_bind(SynchdbJavaRuntime *java);
/* Begin once and poll without blocking in Java's engine.close(). */
extern SynchdbJavaStopStatus synchdb_java_begin_stop(SynchdbJavaRuntime *java);
extern SynchdbJavaStopStatus synchdb_java_poll_stop(SynchdbJavaRuntime *java);
/* Bounded runner close; pause/restart retain this VM and its method cache. */
extern int synchdb_java_stop(SynchdbJavaRuntime *java);
/* Confirmed runner close is required. DestroyJavaVM itself has no timeout;
 * release refs/cache and destroy once, retaining the VM on failure. */
extern bool synchdb_java_destroy(SynchdbJavaRuntime *java);

/* A returned view is valid only until release_frame; release never ACKs. */
extern bool synchdb_java_poll_frame(SynchdbJavaRuntime *java, SynchdbDbzFrame *frame);
extern void synchdb_java_release_frame(SynchdbJavaRuntime *java);
extern void synchdb_java_batch_committed(SynchdbJavaRuntime *java, int32 batch_id);
extern void synchdb_java_batch_failed(SynchdbJavaRuntime *java, bool commit_unknown);
/* Only TARGET_COMMITTED may confirm; repeats after success never repeat JNI. */
extern int synchdb_java_ack_batch(SynchdbJavaRuntime *java, int32 batch_id);
/* Normal engine restarts only; frozen/unfinished sessions cannot be reset. */
extern bool synchdb_java_batch_reset(SynchdbJavaRuntime *java);
/* Caller-owned copy, with JNI UTF resources released even on PostgreSQL ERROR. */
extern char *synchdb_java_get_offset(SynchdbJavaRuntime *java, int connector_type,
									   const char *database, const char *name,
									   const char *destination);

#endif /* SYNCHDB_JAVA_RUNTIME_H */
