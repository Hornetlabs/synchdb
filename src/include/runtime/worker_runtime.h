/* A worker's private, process-lifetime state, independent of its C stack. */
#ifndef SYNCHDB_WORKER_RUNTIME_H
#define SYNCHDB_WORKER_RUNTIME_H

#include "runtime/run_registry.h"
#include "runtime/java_runtime.h"
#include "executor/batch_apply.h"

#define SYNCHDB_WORKER_STARTUP_VERSION 3
typedef struct SynchdbWorkerStartup
{
	uint32 version;
	SynchdbRunHandle run;
	SynchdbConnectorKey key;
	int64 config_revision;
	Oid execution_role;
	uint64 controller_generation;
} SynchdbWorkerStartup;

typedef struct SynchdbWorkerRun
{
	SynchdbRunSpec spec;
	ConnectionInfo execution;
	char snapshot_mode[SYNCHDB_SNAPSHOT_MODE_SIZE];
} SynchdbWorkerRun;

typedef enum SynchdbWorkerCleanupPhase
{
	SYNCHDB_CLEANUP_NEW = 0,
	SYNCHDB_CLEANUP_RUNNING,
	SYNCHDB_CLEANUP_COMPLETE,
	SYNCHDB_CLEANUP_FAILED
} SynchdbWorkerCleanupPhase;

typedef struct SynchdbWorkerContext SynchdbWorkerContext;

/* Native transport/formatting adapters remain outside the lifetime module. */
typedef struct SynchdbWorkerResourceOps
{
	bool (*prepare_close) (SynchdbWorkerContext *worker);
	bool (*close_local) (SynchdbWorkerContext *worker);
} SynchdbWorkerResourceOps;

struct SynchdbWorkerContext
{
	SynchdbSharedState *registry;
	SynchdbWorkerOwner owner;
	SynchdbWorkerRun run;
	SynchdbJavaRuntime java;
	SynchdbBatchApplyProgress apply;
	struct SynchdbBootstrapResult *bootstrap;
	uint64 command_id;
	const SynchdbWorkerResourceOps *resources;
	bool converter_live;
	bool cache_live;
	SynchdbWorkerCleanupPhase cleanup_phase;
	bool cleanup_succeeded;
	bool resources_closed;
	char cleanup_error[SYNCHDB_ERRMSG_SIZE];
};

/* Initialize once from a zero-initialized context. Context and ops must outlive
 * ERROR stack unwinding and exit callbacks; an initialized context is not reset. */
extern void synchdb_worker_init(SynchdbWorkerContext *worker, SynchdbSharedState *registry,
							   const SynchdbWorkerResourceOps *resources);
extern bool synchdb_worker_claim(SynchdbWorkerContext *worker,
								const SynchdbWorkerStartup *startup, int slot);
extern SynchdbRunGate synchdb_worker_gate(SynchdbWorkerContext *worker);
extern bool synchdb_worker_publish(SynchdbWorkerContext *worker, const SynchdbRunSpec *spec,
								  const SynchdbRequest *consumed);
extern bool synchdb_worker_poll(SynchdbWorkerContext *worker, SynchdbRequest *request,
							   ConnectorState *state);
extern bool synchdb_worker_ack(SynchdbWorkerContext *worker);
/* SIGTERM may enter PostgreSQL shutdown only while the backend owns a SQL
 * operation. Save the previous flag before enter and restore it in FINALLY;
 * JNI calls keep the flag disabled and use their own bounded close protocol. */
extern bool synchdb_worker_sql_interruptible(void);
extern void synchdb_worker_sql_enter(void);
extern void synchdb_worker_sql_restore(bool previous);
/* Repeated cleanup reports its cached outcome without repeating resource calls. */
extern bool synchdb_worker_cleanup(SynchdbWorkerContext *worker);
/* Must not throw a resource cleanup ERROR out of an on_shmem_exit callback. */
extern void synchdb_worker_finalize(SynchdbWorkerContext *worker, int exitcode);

#endif /* SYNCHDB_WORKER_RUNTIME_H */
