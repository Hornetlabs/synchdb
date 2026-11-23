/* Private to runtime registry implementations and fault-injection fixtures. */
#ifndef SYNCHDB_RUN_REGISTRY_INTERNAL_H
#define SYNCHDB_RUN_REGISTRY_INTERNAL_H

#include "runtime/run_registry.h"
#include "storage/lwlock.h"

/**
 *  Structure holding state information for connectors
 */
typedef struct _ActiveConnectors
{
	pid_t pid;
	ConnectorState state;
	ConnectorStage stage;
	ConnectorType type;
	SynchdbRequest req;
	char errmsg[SYNCHDB_ERRMSG_SIZE];
	char dbzoffset[SYNCHDB_OFFSET_SIZE];
	char snapshotMode[SYNCHDB_SNAPSHOT_MODE_SIZE];
	ConnectionInfo conninfo;
	SynchdbStatistics stats;
	uint64 generation;
	uint64 attempt;
	uint64 next_command_id;
	SynchdbRunLifecycle lifecycle;
	bool retry;
	bool start_committed;
	uint64 controller_generation;
	uint64 completed_command_id;
	bool command_succeeded;
	int command_sqlstate;
	int bootstrap_status;
	bool source_ready;
	int failure_sqlstate;
	int exit_code;
} ActiveConnectors;

typedef struct SynchdbControllerLease
{
	Oid database_oid;
	pid_t pid;
	uint64 generation;
} SynchdbControllerLease;

/**
 * SynchdbSharedState - Shared state information for synchdb background worker
 */
struct _SynchdbSharedState
{
	LWLock		lock;		/* mutual exclusion */
	uint32 magic;
	uint32 layout_version;
	Size byte_size;
	uint64 incarnation;
	uint64 next_generation;
	int allocated_slots;
	int controller_capacity;
	uint64 next_controller_generation;
};

/* DSM mappings need not have the same virtual address in different backends. */
static inline ActiveConnectors *
synchdb_registry_entries(const SynchdbSharedState *state)
{
	return state ? (ActiveConnectors *) ((char *) state +
		CACHELINEALIGN(sizeof(SynchdbSharedState))) : NULL;
}

static inline SynchdbControllerLease *
synchdb_registry_controllers(const SynchdbSharedState *state)
{
	return state ? (SynchdbControllerLease *) ((char *) synchdb_registry_entries(state) +
		CACHELINEALIGN(sizeof(ActiveConnectors) * (Size) state->allocated_slots)) : NULL;
}

extern bool synchdb_registry_occupied(const ActiveConnectors *entry);
extern SynchdbRunHandle synchdb_registry_handle_locked(const SynchdbSharedState *state, int slot);
extern bool synchdb_registry_matches_locked(const SynchdbSharedState *state, const SynchdbRunHandle *handle);
extern bool synchdb_registry_owned_locked(const SynchdbSharedState *state, const SynchdbWorkerOwner *owner);
extern bool synchdb_registry_target_matches_locked(const SynchdbSharedState *state,
                                                  const SynchdbControllerTarget *target,
                                                  const SynchdbWorkerOwner *caller, bool running);

#endif /* SYNCHDB_RUN_REGISTRY_INTERNAL_H */
