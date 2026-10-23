/* Private process state and ordered, repeatable worker shutdown. */
#include "postgres.h"

#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "runtime/worker_runtime.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"

static volatile sig_atomic_t sql_interruptible;

bool
synchdb_worker_sql_interruptible(void)
{
	return sql_interruptible != 0;
}

void
synchdb_worker_sql_enter(void)
{
	sql_interruptible = true;
	/* A stop received in JNI must also abort a subsequent SQL lock wait. */
	if (ShutdownRequestPending)
		die(SIGTERM);
	CHECK_FOR_INTERRUPTS();
}

void
synchdb_worker_sql_restore(bool previous)
{
	/* Restoring a FINALLY boundary must never raise a second ERROR. */
	sql_interruptible = previous;
}

typedef enum WorkerCloseStep
{
	WORKER_CLOSE_PREPARE,
	WORKER_CLOSE_JAVA_STOP,
	WORKER_CLOSE_JAVA_DESTROY,
	WORKER_CLOSE_LOCAL
} WorkerCloseStep;

static void
worker_remember_cleanup_error(SynchdbWorkerContext *worker, const char *message)
{
	if (worker->cleanup_error[0] == '\0')
		strlcpy(worker->cleanup_error, message, sizeof(worker->cleanup_error));
}

void
synchdb_worker_init(SynchdbWorkerContext *worker, SynchdbSharedState *registry,
					const SynchdbWorkerResourceOps *resources)
{
	if (worker == NULL)
		elog(ERROR, "worker context must not be null");
	if (worker->registry || worker->resources || worker->command_id != 0 ||
		worker->owner.run.incarnation != 0 || worker->owner.run.generation != 0 ||
		worker->owner.run.slot != 0 || worker->owner.attempt != 0 || worker->owner.pid != 0 ||
		worker->java.vm || worker->java.env || worker->java.cls || worker->java.obj ||
		worker->java.get_change_events || worker->java.mark_batch_complete ||
		worker->java.get_offsets || worker->java.request_stop || worker->java.stop_status ||
		worker->java.frame_ref || worker->java.stop_requested ||
		worker->java.stop_confirmed || worker->java.stop_failed ||
		worker->java.batch_phase != SYNCHDB_JAVA_BATCH_IDLE ||
		worker->apply.phase != SYNCHDB_APPLY_NEW || worker->bootstrap != NULL || worker->java.creation_attempted ||
		worker->java.creation_succeeded || worker->java.runner_bound ||
		worker->java.engine_start_attempted ||
		worker->java.destroy_attempted || worker->converter_live || worker->cache_live ||
		worker->cleanup_phase != SYNCHDB_CLEANUP_NEW || worker->resources_closed ||
		worker->cleanup_succeeded || worker->cleanup_error[0] != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("worker context already owns resources or cleanup state")));
	MemSet(worker, 0, sizeof(*worker));
	worker->registry = registry;
	worker->resources = resources;
	worker->owner.run.slot = -1;
	worker->owner.pid = InvalidPid;
	worker->java.batch_id = SYNCHDB_INVALID_BATCH_ID;
}

bool
synchdb_worker_claim(SynchdbWorkerContext *worker,
					 const SynchdbWorkerStartup *startup, int slot)
{
	SynchdbAcceptedRun accepted;

	if (worker == NULL || startup == NULL || worker->owner.attempt != 0 ||
		worker->cleanup_phase != SYNCHDB_CLEANUP_NEW ||
		startup->version != SYNCHDB_WORKER_STARTUP_VERSION ||
		!synchdb_connector_key_is_valid(&startup->key) || startup->config_revision <= 0 ||
		startup->run.incarnation == 0 || startup->run.generation == 0 ||
		startup->run.slot != slot || slot < 0)
		return false;
	if (!synchdb_registry_claim(worker->registry, &startup->run, &startup->key,
								startup->config_revision, &worker->owner, &accepted))
		return false;
	/* The owner is already durable in this context if validation raises ERROR. */
	worker->run.spec.legacy_type = accepted.type;
	memcpy(&worker->run.spec.connection, &accepted.connection, sizeof(worker->run.spec.connection));
	memcpy(&worker->run.execution, &accepted.connection, sizeof(worker->run.execution));
	memcpy(worker->run.snapshot_mode, accepted.snapshot_mode, sizeof(worker->run.snapshot_mode));
	synchdb_run_spec_init(&worker->run.spec, accepted.type,
						  &accepted.connection, accepted.snapshot_mode);
	memcpy(&worker->run.execution, &worker->run.spec.connection, sizeof(worker->run.execution));
	memcpy(worker->run.snapshot_mode, worker->run.spec.snapshot_mode, sizeof(worker->run.snapshot_mode));
	worker->command_id = 0;
	return true;
}

SynchdbRunGate
synchdb_worker_gate(SynchdbWorkerContext *worker)
{
	return worker && worker->cleanup_phase == SYNCHDB_CLEANUP_NEW ?
		synchdb_registry_gate(worker->registry, &worker->owner) : SYNCHDB_GATE_STOP;
}

bool
synchdb_worker_publish(SynchdbWorkerContext *worker, const SynchdbRunSpec *spec,
					   const SynchdbRequest *consumed)
{
	if (worker == NULL || worker->cleanup_phase != SYNCHDB_CLEANUP_NEW ||
		spec == NULL || consumed == NULL ||
		!synchdb_registry_publish_run(worker->registry, &worker->owner, spec, consumed))
		return false;
	memmove(&worker->run.spec, spec, sizeof(worker->run.spec));
	memcpy(&worker->run.execution, &worker->run.spec.connection, sizeof(worker->run.execution));
	memcpy(worker->run.snapshot_mode, worker->run.spec.snapshot_mode, sizeof(worker->run.snapshot_mode));
	if (worker->command_id == consumed->command_id &&
		consumed->target_generation == worker->owner.run.generation &&
		consumed->target_attempt == worker->owner.attempt)
		worker->command_id = 0;
	return true;
}

bool
synchdb_worker_poll(SynchdbWorkerContext *worker, SynchdbRequest *request,
					ConnectorState *state)
{
	bool found;

	if (worker == NULL || worker->cleanup_phase != SYNCHDB_CLEANUP_NEW)
	{
		if (worker)
			worker->command_id = 0;
		if (request)
			MemSet(request, 0, sizeof(*request));
		if (state)
			*state = STATE_UNDEF;
		return false;
	}
	found = synchdb_registry_poll_request(worker->registry, &worker->owner, request, state);
	worker->command_id = found ? request->command_id : 0;
	return found;
}

bool
synchdb_worker_ack(SynchdbWorkerContext *worker)
{
	bool acknowledged;

	if (worker == NULL || worker->cleanup_phase != SYNCHDB_CLEANUP_NEW || worker->command_id == 0)
		return false;
	acknowledged = synchdb_registry_ack_request(worker->registry, &worker->owner, worker->command_id);
	worker->command_id = 0;
	return acknowledged;
}

/* Each resource step gets its own exception boundary and restores its caller. */
static bool
worker_close_step(SynchdbWorkerContext *worker, WorkerCloseStep step, MemoryContext caller)
{
	static const char *const failures[] = {
		"failed to prepare worker resource cleanup",
		"failed to stop the Debezium runner during cleanup",
		"failed to destroy the worker JVM during cleanup",
		"failed to close worker local resources"
	};
	volatile bool succeeded = false;

	PG_TRY();
	{
		switch (step)
		{
			case WORKER_CLOSE_PREPARE:
				succeeded = !worker->resources || !worker->resources->prepare_close ||
					worker->resources->prepare_close(worker);
				break;
			case WORKER_CLOSE_JAVA_STOP:
				succeeded = synchdb_java_stop(&worker->java) == 0;
				break;
			case WORKER_CLOSE_JAVA_DESTROY:
				succeeded = synchdb_java_destroy(&worker->java);
				break;
			case WORKER_CLOSE_LOCAL:
				succeeded = worker->resources && worker->resources->close_local &&
					worker->resources->close_local(worker);
				break;
		}
	}
	PG_CATCH();
	{
		ErrorData *error;

		MemoryContextSwitchTo(caller);
		error = CopyErrorData();
		worker_remember_cleanup_error(worker, error->message ? error->message : failures[step]);
		FreeErrorData(error);
		FlushErrorState();
		succeeded = false;
	}
	PG_END_TRY();
	MemoryContextSwitchTo(caller);
	if (!succeeded)
		worker_remember_cleanup_error(worker, failures[step]);
	return succeeded;
}

bool
synchdb_worker_cleanup(SynchdbWorkerContext *worker)
{
	MemoryContext caller;
	bool succeeded = true;
	bool local_closed;

	if (worker == NULL || worker->cleanup_phase == SYNCHDB_CLEANUP_RUNNING)
		return false;
	if (worker->cleanup_phase == SYNCHDB_CLEANUP_COMPLETE ||
		worker->cleanup_phase == SYNCHDB_CLEANUP_FAILED)
		return worker->cleanup_succeeded;
	caller = CurrentMemoryContext;
	worker->cleanup_phase = SYNCHDB_CLEANUP_RUNNING;
	succeeded &= worker_close_step(worker, WORKER_CLOSE_PREPARE, caller);
	succeeded &= worker_close_step(worker, WORKER_CLOSE_JAVA_STOP, caller);
	/* Destroy refuses an unconfirmed runner close and preserves VM ownership. */
	succeeded &= worker_close_step(worker, WORKER_CLOSE_JAVA_DESTROY, caller);
	local_closed = worker_close_step(worker, WORKER_CLOSE_LOCAL, caller);
	succeeded &= local_closed;
	worker->resources_closed = worker->java.vm == NULL && local_closed;
	worker->cleanup_succeeded = succeeded;
	worker->cleanup_phase = worker->resources_closed ?
		SYNCHDB_CLEANUP_COMPLETE : SYNCHDB_CLEANUP_FAILED;
	MemoryContextSwitchTo(caller);
	return succeeded;
}

void
synchdb_worker_finalize(SynchdbWorkerContext *worker, int exitcode)
{
	MemoryContext caller;

	if (worker == NULL || worker->owner.attempt == 0)
		return;
	caller = CurrentMemoryContext;
	PG_TRY();
	{
		(void) synchdb_worker_cleanup(worker);
	}
	PG_CATCH();
	{
		/* Even error-report allocation can fail; this fallback allocates nothing. */
		MemoryContextSwitchTo(caller);
		worker_remember_cleanup_error(worker, "unexpected error while cleaning worker resources");
		FlushErrorState();
		worker->cleanup_succeeded = false;
		worker->resources_closed = false;
		worker->cleanup_phase = SYNCHDB_CLEANUP_FAILED;
	}
	PG_END_TRY();
	MemoryContextSwitchTo(caller);
	if (!worker->cleanup_succeeded || !worker->resources_closed || worker->java.vm != NULL)
	{
		synchdb_registry_cleanup_failed(worker->registry, &worker->owner,
			worker->cleanup_error[0] ? worker->cleanup_error : "worker resources remain open after cleanup");
	}
	if (!worker->resources_closed || worker->java.vm != NULL)
	{
		return;
	}
	/* Postmaster, not this module, decides whether the real exit status retries. */
	synchdb_registry_release(worker->registry, &worker->owner, exitcode);
}
