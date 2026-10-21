/*
 * run_registry.c
 *
 * Atomic reservation, process claims, and conditional release of shared slots.
 * Preload reserves the registry in main shared memory. Explicit manual starts
 * use a pinned DSM segment; a small main-memory anchor is the sole rendezvous.
 */
#include "postgres.h"

#include "miscadmin.h"
#include "runtime/run_registry_internal.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "utils/memutils.h"

#define REGISTRY_MAGIC ((uint32) 0x53444252)
#define REGISTRY_LAYOUT 3
#define REGISTRY_ANCHOR_NAME "synchdb runtime anchor v3"
#define REGISTRY_SHMEM_NAME "synchdb runtime registry v3"

typedef struct RegistryAnchor
{
	uint32 magic;
	uint32 layout_version;
	dsm_handle handle;
	int capacity;
	uint64 incarnation;
} RegistryAnchor;

static shmem_request_hook_type previous_request_hook;
static shmem_startup_hook_type previous_startup_hook;
static int registry_capacity;
static SynchdbSharedState *registry_shared;

static int
registry_controller_capacity(void)
{
	/* Controllers consume PostgreSQL worker slots, not connector run slots.
	 * Keep one inert lease when workers are disabled; registration still fails
	 * through PostgreSQL's normal max_worker_processes check in that case. */
	return Max(max_worker_processes, 1);
}

static Size
registry_aligned_size(Size size)
{
	return add_size(size, PG_CACHE_LINE_SIZE - 1) & ~((Size) PG_CACHE_LINE_SIZE - 1);
}

static Size
registry_memory_size(int capacity)
{
	if (capacity <= 0 || capacity > 65535)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid connector registry capacity")));
	return add_size(add_size(registry_aligned_size(sizeof(SynchdbSharedState)),
					registry_aligned_size(mul_size(sizeof(ActiveConnectors), (Size) capacity))),
					registry_aligned_size(mul_size(sizeof(SynchdbControllerLease),
													 (Size) registry_controller_capacity())));
}

static void
registry_initialize(SynchdbSharedState *state, int capacity)
{
	int i;

	MemSet(state, 0, registry_memory_size(capacity));
	if (!pg_strong_random(&state->incarnation, sizeof(state->incarnation)) ||
		state->incarnation == 0)
		elog(ERROR, "could not generate connector registry incarnation");
	state->magic = REGISTRY_MAGIC;
	state->layout_version = REGISTRY_LAYOUT;
	state->allocated_slots = capacity;
	state->controller_capacity = registry_controller_capacity();
	state->byte_size = registry_memory_size(capacity);
	LWLockInitialize(&state->lock, LWLockNewTrancheId());
	for (i = 0; i < capacity; i++)
	{
		ActiveConnectors *entry = &synchdb_registry_entries(state)[i];

		entry->pid = InvalidPid;
		entry->state = STATE_UNDEF;
		entry->type = TYPE_UNDEF;
	}
}

static void
registry_validate(SynchdbSharedState *state, int capacity)
{
	if (state->magic != REGISTRY_MAGIC || state->layout_version != REGISTRY_LAYOUT ||
		state->incarnation == 0 || state->allocated_slots != capacity ||
		state->controller_capacity != registry_controller_capacity() ||
		state->byte_size != registry_memory_size(capacity))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("connector registry does not match its startup allocation"),
				 errhint("Use the same synchdb.max_connector_workers in all sessions; restart PostgreSQL after changing its value.")));
}

static void
registry_request_shmem(void)
{
	if (previous_request_hook)
		previous_request_hook();
	RequestAddinShmemSpace(registry_memory_size(registry_capacity));
}

static void
registry_startup_shmem(void)
{
	SynchdbSharedState *state;
	bool found;

	if (previous_startup_hook)
		previous_startup_hook();
	/* Also run after postmaster crash recovery: never reuse an incarnation. */
	registry_shared = NULL;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	state = ShmemInitStruct(REGISTRY_SHMEM_NAME,
							registry_memory_size(registry_capacity), &found);
	if (!found)
		registry_initialize(state, registry_capacity);
	else
		registry_validate(state, registry_capacity);
	registry_shared = state;
	LWLockRelease(AddinShmemInitLock);
	LWLockRegisterTranche(state->lock.tranche, "synchdb");
}

void
synchdb_registry_register_shmem(int capacity)
{
	if (!process_shared_preload_libraries_in_progress)
		return;
	(void) registry_memory_size(capacity);
	if (registry_capacity != 0)
	{
		if (registry_capacity != capacity)
			elog(ERROR, "connector registry capacity was registered twice with different values");
		return;
	}
	registry_capacity = capacity;
	previous_request_hook = shmem_request_hook;
	shmem_request_hook = registry_request_shmem;
	previous_startup_hook = shmem_startup_hook;
	shmem_startup_hook = registry_startup_shmem;
}

/* Caller holds AddinShmemInitLock, including initialization of a new anchor.
 * A late-loaded extension cannot reserve main shared memory in advance. Only
 * this small anchor uses the remaining space; failure leaves no DSM/worker.
 * Never fall back to a second rendezvous when that space is exhausted. */
static RegistryAnchor *
registry_anchor(void)
{
	RegistryAnchor *anchor;
	bool found;
	MemoryContext caller = CurrentMemoryContext;

	PG_TRY();
	{
		anchor = ShmemInitStruct(REGISTRY_ANCHOR_NAME, sizeof(*anchor), &found);
	}
	PG_CATCH();
	{
		ErrorData *error;

		MemoryContextSwitchTo(caller);
		error = CopyErrorData();
		if (error->sqlerrcode != ERRCODE_OUT_OF_MEMORY)
			PG_RE_THROW();
		FlushErrorState();
		FreeErrorData(error);
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("not enough main shared memory for the synchdb runtime anchor"),
						errhint("Add synchdb to shared_preload_libraries and restart PostgreSQL to reserve runtime memory at startup.")));
	}
	PG_END_TRY();
	if (!found)
	{
		MemSet(anchor, 0, sizeof(*anchor));
		anchor->magic = REGISTRY_MAGIC;
		anchor->layout_version = REGISTRY_LAYOUT;
	}
	if (anchor->magic != REGISTRY_MAGIC || anchor->layout_version != REGISTRY_LAYOUT)
		elog(ERROR, "incompatible synchdb runtime anchor");
	return anchor;
}

static SynchdbSharedState *
registry_attach(int capacity, bool create)
{
	(void) registry_memory_size(capacity);
	if (registry_shared == NULL && registry_capacity == 0)
	{
		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
		PG_TRY();
		{
			RegistryAnchor *anchor = registry_anchor();
			dsm_segment *segment;
			SynchdbSharedState *state;

			if (anchor->handle != DSM_HANDLE_INVALID)
			{
				if (anchor->capacity != capacity)
					ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("connector registry capacity differs from the active runtime"),
						 errhint("Use the same synchdb.max_connector_workers in all sessions; restart PostgreSQL to change it.")));
				segment = dsm_find_mapping(anchor->handle);
				if (segment == NULL)
					segment = dsm_attach(anchor->handle);
				if (segment == NULL || dsm_segment_map_length(segment) < registry_memory_size(capacity))
					elog(ERROR, "could not attach to the published synchdb runtime registry");
				state = dsm_segment_address(segment);
				registry_validate(state, capacity);
				if (state->incarnation != anchor->incarnation)
					elog(ERROR, "synchdb runtime registry incarnation mismatch");
				dsm_pin_mapping(segment);
				registry_shared = state;
			}
			else if (create)
			{
				segment = dsm_create(registry_memory_size(capacity), 0);
				state = dsm_segment_address(segment);
				registry_initialize(state, capacity);
				/* Publish only a fully initialized segment. No error-prone work
				 * follows pinning before its one authoritative handle is stored. */
				dsm_pin_segment(segment);
				dsm_pin_mapping(segment);
				anchor->capacity = capacity;
				anchor->incarnation = state->incarnation;
				anchor->handle = dsm_segment_handle(segment);
				registry_shared = state;
			}
			LWLockRelease(AddinShmemInitLock);
		}
		PG_CATCH();
		{
			/* ERROR resets InterruptHoldoffCount before longjmp. Releasing
			 * an LWLock normally in FINALLY would assert/mask that error. */
			LWLockReleaseAll();
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	if (registry_shared)
	{
		registry_validate(registry_shared, capacity);
		LWLockRegisterTranche(registry_shared->lock.tranche, "synchdb");
	}
	else if (create)
		elog(ERROR, "connector runtime shared memory is not initialized");
	return registry_shared;
}

SynchdbSharedState *
synchdb_registry_attach(int capacity)
{
	return registry_attach(capacity, true);
}

SynchdbSharedState *
synchdb_registry_try_attach(int capacity)
{
	return registry_attach(capacity, false);
}

bool
synchdb_registry_is_preloaded(void)
{
	return registry_capacity != 0;
}

bool
synchdb_registry_capacity_compatible(int capacity)
{
	return registry_shared == NULL || registry_shared->allocated_slots == capacity;
}

uint64
synchdb_registry_incarnation(const SynchdbSharedState *state)
{
	return state ? state->incarnation : 0;
}

bool
synchdb_registry_valid_slot(const SynchdbSharedState *state, int slot)
{
	return state != NULL && synchdb_registry_entries(state) != NULL && state->incarnation != 0 &&
		state->allocated_slots > 0 && slot >= 0 && slot < state->allocated_slots;
}

bool
synchdb_registry_occupied(const ActiveConnectors *entry)
{
	return entry != NULL && entry->lifecycle != SYNCHDB_RUN_FREE;
}

SynchdbRunHandle
synchdb_registry_handle_locked(const SynchdbSharedState *state, int slot)
{
	SynchdbRunHandle handle = {0, 0, -1};

	if (synchdb_registry_valid_slot(state, slot) &&
		synchdb_registry_occupied(&synchdb_registry_entries(state)[slot]) &&
		synchdb_registry_entries(state)[slot].generation != 0)
	{
		handle.incarnation = state->incarnation;
		handle.generation = synchdb_registry_entries(state)[slot].generation;
		handle.slot = slot;
	}
	return handle;
}

bool
synchdb_registry_matches_locked(const SynchdbSharedState *state,
								const SynchdbRunHandle *handle)
{
	return handle != NULL && synchdb_registry_valid_slot(state, handle->slot) &&
		handle->incarnation == state->incarnation && handle->generation != 0 &&
		handle->generation == synchdb_registry_entries(state)[handle->slot].generation &&
		synchdb_registry_occupied(&synchdb_registry_entries(state)[handle->slot]);
}

bool
synchdb_registry_owned_locked(const SynchdbSharedState *state,
							  const SynchdbWorkerOwner *owner)
{
	return owner != NULL && synchdb_registry_matches_locked(state, &owner->run) &&
		owner->attempt != 0 && owner->pid > 0 &&
		synchdb_registry_entries(state)[owner->run.slot].attempt == owner->attempt &&
		synchdb_registry_entries(state)[owner->run.slot].pid == owner->pid;
}

SynchdbRunHandle
synchdb_registry_reserve(SynchdbSharedState *state, const SynchdbRunSpec *spec, bool retry)
{
	SynchdbRunSpec accepted;
	SynchdbRunHandle handle = {0, 0, -1};
	ActiveConnectors *entry;
	int available = -1;
	int matching = -1;
	int i;
	bool in_use = false;

	if (state == NULL || synchdb_registry_entries(state) == NULL || state->incarnation == 0 ||
		state->allocated_slots <= 0 || spec == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("connector run registry is not initialized")));
	/* Validate before taking a lock: validation errors must not retain it. */
	synchdb_run_spec_init(&accepted, spec->legacy_type, &spec->connection, spec->snapshot_mode);
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	for (i = 0; i < state->allocated_slots; i++)
	{
		entry = &synchdb_registry_entries(state)[i];
		if (synchdb_registry_occupied(entry))
		{
			if (synchdb_connector_key_equal(&entry->conninfo.key, &accepted.connection.key) ||
				(entry->conninfo.key.database_oid == accepted.connection.key.database_oid &&
				 strcmp(entry->conninfo.name, accepted.connection.name) == 0))
				in_use = true;
		}
		else
		{
			if (available < 0)
				available = i;
			if (synchdb_connector_key_equal(&entry->conninfo.key, &accepted.connection.key))
				matching = i;
		}
	}
	if (in_use || available < 0 || state->next_generation == PG_UINT64_MAX)
	{
		bool exhausted = state->next_generation == PG_UINT64_MAX;

		LWLockRelease(&state->lock);
		if (in_use)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("connector identity or name already has a reserved or running worker")));
		if (available < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					 errmsg("maximum number of connector runs reached")));
		if (exhausted)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("connector run generation counter is exhausted")));
	}
	handle.incarnation = state->incarnation;
	handle.generation = ++state->next_generation;
	handle.slot = matching >= 0 ? matching : available;
	entry = &synchdb_registry_entries(state)[handle.slot];
	MemSet(entry, 0, sizeof(*entry));
	entry->generation = handle.generation;
	entry->pid = InvalidPid;
	entry->lifecycle = SYNCHDB_RUN_RESERVED;
	entry->retry = retry;
	entry->type = accepted.legacy_type;
	entry->state = STATE_UNDEF;
	memcpy(&entry->conninfo, &accepted.connection, sizeof(entry->conninfo));
	memcpy(entry->snapshotMode, accepted.snapshot_mode, sizeof(entry->snapshotMode));
	LWLockRelease(&state->lock);
	return handle;
}

bool
synchdb_registry_claim(SynchdbSharedState *state, const SynchdbRunHandle *handle,
					   const SynchdbConnectorKey *key, int64 minimum_revision,
					   SynchdbWorkerOwner *owner, SynchdbAcceptedRun *accepted)
{
	ActiveConnectors *entry;

	if (owner == NULL || accepted == NULL)
		return false;
	MemSet(owner, 0, sizeof(*owner));
	owner->run.slot = -1;
	MemSet(accepted, 0, sizeof(*accepted));
	if (state == NULL || synchdb_registry_entries(state) == NULL || handle == NULL ||
		!synchdb_connector_key_is_valid(key) || minimum_revision <= 0 || MyProcPid <= 0)
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (!synchdb_registry_matches_locked(state, handle))
	{
		LWLockRelease(&state->lock);
		return false;
	}
	entry = &synchdb_registry_entries(state)[handle->slot];
	if ((entry->lifecycle != SYNCHDB_RUN_RESERVED && entry->lifecycle != SYNCHDB_RUN_RETRY_WAIT) ||
		entry->pid != InvalidPid ||
		!synchdb_connector_key_equal(&entry->conninfo.key, key) ||
		entry->conninfo.config_revision < minimum_revision)
	{
		LWLockRelease(&state->lock);
		return false;
	}
	if (entry->attempt == PG_UINT64_MAX)
	{
		LWLockRelease(&state->lock);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("connector worker attempt counter is exhausted")));
	}
	entry->attempt++;
	entry->pid = MyProcPid;
	entry->state = STATE_INITIALIZING;
	entry->lifecycle = SYNCHDB_RUN_RUNNING;
	MemSet(&entry->req, 0, sizeof(entry->req));
	owner->run = *handle;
	owner->attempt = entry->attempt;
	owner->pid = MyProcPid;
	memcpy(&accepted->connection, &entry->conninfo, sizeof(accepted->connection));
	accepted->type = entry->type;
	memcpy(accepted->snapshot_mode, entry->snapshotMode, sizeof(accepted->snapshot_mode));
	LWLockRelease(&state->lock);
	return true;
}

bool
synchdb_registry_release(SynchdbSharedState *state, const SynchdbWorkerOwner *owner,
						 int exitcode)
{
	ActiveConnectors *entry;

	if (state == NULL || synchdb_registry_entries(state) == NULL || owner == NULL)
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (!synchdb_registry_owned_locked(state, owner))
	{
		LWLockRelease(&state->lock);
		return false;
	}
	entry = &synchdb_registry_entries(state)[owner->run.slot];
	entry->lifecycle = exitcode != 0 && entry->retry && entry->lifecycle != SYNCHDB_RUN_STOPPING ?
		SYNCHDB_RUN_RETRY_WAIT : SYNCHDB_RUN_FREE;
	entry->pid = InvalidPid;
	entry->state = STATE_UNDEF;
	entry->exit_code = exitcode;
	MemSet(&entry->req, 0, sizeof(entry->req));
	LWLockRelease(&state->lock);
	return true;
}

bool
synchdb_registry_cancel(SynchdbSharedState *state, const SynchdbRunHandle *handle)
{
	ActiveConnectors *entry;
	pid_t pid;

	if (state == NULL || synchdb_registry_entries(state) == NULL || handle == NULL)
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (!synchdb_registry_matches_locked(state, handle))
	{
		LWLockRelease(&state->lock);
		return false;
	}
	entry = &synchdb_registry_entries(state)[handle->slot];
	pid = entry->pid;
	entry->lifecycle = SYNCHDB_RUN_STOPPING;
	MemSet(&entry->req, 0, sizeof(entry->req));
	if (entry->pid == InvalidPid)
	{
		entry->lifecycle = SYNCHDB_RUN_FREE;
		entry->state = STATE_UNDEF;
	}
	LWLockRelease(&state->lock);
	if (pid > 0)
	{
		PGPROC *proc = BackendPidGetProc(pid);

		if (proc)
			SetLatch(&proc->procLatch);
	}
	return true;
}

bool
synchdb_registry_commit_start(SynchdbSharedState *state, const SynchdbRunHandle *run)
{
	pid_t pid = InvalidPid;
	bool committed = false;

	if (!state || !synchdb_registry_entries(state))
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (synchdb_registry_matches_locked(state, run) &&
		synchdb_registry_entries(state)[run->slot].lifecycle != SYNCHDB_RUN_STOPPING)
	{
		synchdb_registry_entries(state)[run->slot].start_committed = true;
		pid = synchdb_registry_entries(state)[run->slot].pid;
		committed = true;
	}
	LWLockRelease(&state->lock);
	if (pid > 0)
	{
		PGPROC *proc = BackendPidGetProc(pid);

		if (proc)
			SetLatch(&proc->procLatch);
	}
	return committed;
}

bool
synchdb_registry_find_run(SynchdbSharedState *state, const SynchdbConnectorKey *key,
						SynchdbRunHandle *run)
{
	int i;
	bool found = false;

	if (run == NULL)
		return false;
	MemSet(run, 0, sizeof(*run));
	run->slot = -1;
	if (!state || !synchdb_registry_entries(state) || !synchdb_connector_key_is_valid(key))
		return false;
	LWLockAcquire(&state->lock, LW_SHARED);
	for (i = 0; i < state->allocated_slots; i++)
		if (synchdb_registry_occupied(&synchdb_registry_entries(state)[i]) &&
			synchdb_connector_key_equal(&synchdb_registry_entries(state)[i].conninfo.key, key))
		{
			*run = synchdb_registry_handle_locked(state, i);
			found = true;
			break;
		}
	LWLockRelease(&state->lock);
	return found;
}

bool
synchdb_registry_run_active(SynchdbSharedState *state, const SynchdbRunHandle *run)
{
	bool active;

	if (!state || !synchdb_registry_entries(state))
		return false;
	LWLockAcquire(&state->lock, LW_SHARED);
	active = synchdb_registry_matches_locked(state, run);
	LWLockRelease(&state->lock);
	return active;
}

bool
synchdb_registry_run_unchanged(SynchdbSharedState *state, const SynchdbRunHandle *run,
							 const SynchdbConnectorKey *key)
{
	bool unchanged;

	if (!state || !synchdb_registry_entries(state) || !run || !synchdb_connector_key_is_valid(key))
		return false;
	LWLockAcquire(&state->lock, LW_SHARED);
	unchanged = synchdb_registry_valid_slot(state, run->slot) &&
		state->incarnation == run->incarnation && run->generation != 0 &&
		synchdb_registry_entries(state)[run->slot].generation == run->generation &&
		synchdb_connector_key_equal(&synchdb_registry_entries(state)[run->slot].conninfo.key, key);
	LWLockRelease(&state->lock);
	return unchanged;
}

bool
synchdb_registry_owned(SynchdbSharedState *state, const SynchdbWorkerOwner *owner)
{
	bool owned;

	if (!state || !synchdb_registry_entries(state))
		return false;
	LWLockAcquire(&state->lock, LW_SHARED);
	owned = synchdb_registry_owned_locked(state, owner);
	LWLockRelease(&state->lock);
	return owned;
}

SynchdbRunGate
synchdb_registry_gate(SynchdbSharedState *state, const SynchdbWorkerOwner *owner)
{
	SynchdbRunGate gate = SYNCHDB_GATE_STOP;

	if (!state || !synchdb_registry_entries(state))
		return gate;
	LWLockAcquire(&state->lock, LW_SHARED);
	if (synchdb_registry_owned_locked(state, owner) &&
		synchdb_registry_entries(state)[owner->run.slot].lifecycle != SYNCHDB_RUN_STOPPING)
	{
		ActiveConnectors *entry = &synchdb_registry_entries(state)[owner->run.slot];
		bool controller_live = entry->controller_generation == 0;
		int i;

		for (i = 0; !controller_live && i < state->controller_capacity; ++i)
			controller_live = synchdb_registry_controllers(state)[i].database_oid == entry->conninfo.key.database_oid &&
				synchdb_registry_controllers(state)[i].generation == entry->controller_generation &&
				synchdb_registry_controllers(state)[i].pid > 0;
		if (controller_live)
			gate = entry->start_committed ? SYNCHDB_GATE_READY : SYNCHDB_GATE_WAIT;
	}
	LWLockRelease(&state->lock);
	return gate;
}

void
synchdb_registry_cleanup_failed(SynchdbSharedState *state,
								const SynchdbWorkerOwner *owner, const char *message)
{
	if (!state || !synchdb_registry_entries(state))
		return;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (synchdb_registry_owned_locked(state, owner))
	{
		ActiveConnectors *entry = &synchdb_registry_entries(state)[owner->run.slot];

		entry->lifecycle = SYNCHDB_RUN_STOPPING;
		/* Preserve the original apply/source error when cleanup also fails.
		 * Exit status records the real process exit, independently of this. */
		if (entry->failure_sqlstate == 0)
		{
			entry->failure_sqlstate = ERRCODE_EXTERNAL_ROUTINE_EXCEPTION;
			strlcpy(entry->errmsg, message ? message : "worker resource cleanup failed",
					sizeof(entry->errmsg));
		}
	}
	LWLockRelease(&state->lock);
}
