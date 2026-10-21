/*
 * run_registry_control.c
 *
 * Atomic controller commands and fences around catalog writes.  Catalog,
 * engine and filesystem work belongs to callers outside the registry lock.
 */
#include "postgres.h"

#include "miscadmin.h"
#include "runtime/run_registry_internal.h"

static void
registry_copy_target_locked(const SynchdbSharedState *state, int slot,
						   SynchdbControllerTarget *target)
{
	const ActiveConnectors *entry = &synchdb_registry_entries(state)[slot];

	MemSet(target, 0, sizeof(*target));
	target->key = entry->conninfo.key;
	target->run.incarnation = state->incarnation;
	target->run.generation = entry->generation;
	target->run.slot = slot;
	target->snapshot.pid = entry->pid;
	target->snapshot.state = entry->state;
	target->snapshot.stage = entry->stage;
	target->snapshot.type = entry->type;
	/* Preserve the complete accepted value used by restart's final comparison. */
	memcpy(&target->snapshot.conninfo, &entry->conninfo, sizeof(entry->conninfo));
	memcpy(target->snapshot.snapshotMode, entry->snapshotMode, sizeof(entry->snapshotMode));
	target->snapshot.attempt = entry->attempt;
	target->snapshot.lifecycle = entry->lifecycle;
}

/* FREE display rows remain valid targets for external statistics SQL calls. */
bool
synchdb_registry_target_matches_locked(const SynchdbSharedState *state,
									 const SynchdbControllerTarget *target,
									 const SynchdbWorkerOwner *caller, bool running)
{
	const ActiveConnectors *entry;
	int slot;

	if (target == NULL || state == NULL || synchdb_registry_entries(state) == NULL)
		return false;
	slot = target->run.slot;
	if (target->run.incarnation != state->incarnation ||
		slot < 0 || slot >= state->allocated_slots)
		return false;
	entry = &synchdb_registry_entries(state)[slot];
	/* Worker-issued SQL cannot resolve a successor's name into a fresh target. */
	if (caller != NULL &&
		(caller->run.slot != slot || !synchdb_registry_owned_locked(state, caller)))
		return false;
	return entry->generation == target->run.generation &&
		synchdb_connector_key_equal(&entry->conninfo.key, &target->key) &&
		entry->attempt == target->snapshot.attempt &&
		entry->pid == target->snapshot.pid &&
		entry->lifecycle == target->snapshot.lifecycle &&
		(!running || (entry->lifecycle == SYNCHDB_RUN_RUNNING && entry->pid > 0));
}

bool
synchdb_registry_capture(SynchdbSharedState *state, const SynchdbConnectorKey *key,
						 bool running, SynchdbControllerTarget *target)
{
	SynchdbConnectorKey requested;
	int i;

	if (target == NULL)
		return false;
	if (key == NULL)
	{
		MemSet(target, 0, sizeof(*target));
		target->run.slot = -1;
		return false;
	}
	requested = *key;
	MemSet(target, 0, sizeof(*target));
	target->run.slot = -1;
	if (state == NULL || synchdb_registry_entries(state) == NULL ||
		!synchdb_connector_key_is_valid(&requested))
		return false;
	LWLockAcquire(&state->lock, LW_SHARED);
	for (i = 0; i < state->allocated_slots; i++)
	{
		const ActiveConnectors *entry = &synchdb_registry_entries(state)[i];

		if (!synchdb_connector_key_equal(&entry->conninfo.key, &requested))
			continue;
		/* Prefer the current registration to a retained, free display record. */
		if (target->run.slot >= 0 && !synchdb_registry_occupied(entry))
			continue;
		registry_copy_target_locked(state, i, target);
		if (synchdb_registry_occupied(entry))
			break;
	}
	LWLockRelease(&state->lock);
	return target->run.slot >= 0 &&
		(!running || (target->snapshot.lifecycle == SYNCHDB_RUN_RUNNING &&
					  target->snapshot.pid > 0));
}

void
synchdb_registry_submit(SynchdbSharedState *state, const SynchdbControllerTarget *target,
						const SynchdbWorkerOwner *caller, SynchdbRequest *request)
{
	ActiveConnectors *entry;
	bool state_ok = true;

	if (state == NULL || synchdb_registry_entries(state) == NULL || request == NULL)
		elog(ERROR, "invalid connector registry command arguments");
	switch (request->reqstate)
	{
		case STATE_PAUSED:
		case STATE_SYNCING:
		case STATE_OFFSET_UPDATE:
		case STATE_RESTARTING:
		case STATE_MEMDUMP:
		case STATE_RELOAD_OBJMAP:
		case STATE_DBZ_LOGLEVEL_UPDATE:
			break;
		default:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid connector command state")));
	}
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (!synchdb_registry_target_matches_locked(state, target, caller, true))
	{
		LWLockRelease(&state->lock);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
						errmsg("connector worker changed during command submission")));
	}
	entry = &synchdb_registry_entries(state)[target->run.slot];
	if (request->reqstate == STATE_PAUSED)
		state_ok = entry->state == STATE_SYNCING;
	else if (request->reqstate == STATE_SYNCING || request->reqstate == STATE_OFFSET_UPDATE)
		state_ok = entry->state == STATE_PAUSED;
	else if (request->reqstate == STATE_RESTARTING)
		state_ok = (entry->state == STATE_SYNCING || entry->state == STATE_PAUSED) &&
			entry->type == target->snapshot.type &&
			memcmp(&entry->conninfo, &target->snapshot.conninfo, sizeof(entry->conninfo)) == 0 &&
			memcmp(entry->snapshotMode, target->snapshot.snapshotMode, sizeof(entry->snapshotMode)) == 0;
	if (!state_ok || entry->req.reqstate != STATE_UNDEF)
	{
		LWLockRelease(&state->lock);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
						errmsg("connector state or pending command changed during command submission"),
						errhint("Wait for the current operation and retry.")));
	}
	if (entry->next_command_id == PG_UINT64_MAX)
	{
		LWLockRelease(&state->lock);
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("connector command identifier space is exhausted")));
	}
	request->command_id = ++entry->next_command_id;
	request->target_generation = target->run.generation;
	request->target_attempt = target->snapshot.attempt;
	memcpy(&entry->req, request, sizeof(entry->req));
	LWLockRelease(&state->lock);
}

void
synchdb_registry_reset_stats(SynchdbSharedState *state, const SynchdbControllerTarget *target,
							 const SynchdbWorkerOwner *caller)
{
	if (state == NULL || synchdb_registry_entries(state) == NULL)
		elog(ERROR, "connector registry is not initialized");
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (!synchdb_registry_target_matches_locked(state, target, caller, false))
	{
		LWLockRelease(&state->lock);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
						errmsg("connector worker changed during statistics reset")));
	}
	MemSet(&synchdb_registry_entries(state)[target->run.slot].stats, 0, sizeof(SynchdbStatistics));
	LWLockRelease(&state->lock);
}

void
synchdb_registry_add_snapstats(SynchdbSharedState *state, const SynchdbControllerTarget *target,
							   const SynchdbWorkerOwner *caller, int64 tables, int64 rows,
							   int64 begin, int64 end)
{
	SnapshotStatistics *stats;

	if (tables < 0 || rows < 0 || begin < 0 || end < 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("snapshot statistics must not be negative")));
	if (state == NULL || synchdb_registry_entries(state) == NULL)
		elog(ERROR, "connector registry is not initialized");
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (!synchdb_registry_target_matches_locked(state, target, caller, false))
	{
		LWLockRelease(&state->lock);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
						errmsg("connector worker changed during snapshot statistics update")));
	}
	stats = &synchdb_registry_entries(state)[target->run.slot].stats.snapstats;
	if ((uint64) tables > PG_UINT64_MAX - stats->snapstats_tables ||
		(uint64) rows > PG_UINT64_MAX - stats->snapstats_rows)
	{
		LWLockRelease(&state->lock);
		ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						errmsg("snapshot statistics counter overflow")));
	}
	stats->snapstats_tables += (uint64) tables;
	stats->snapstats_rows += (uint64) rows;
	if (begin > 0)
	{
		stats->snapstats_begintime_ts = (uint64) begin;
		stats->snapstats_endtime_ts = 0;
	}
	if (end > 0)
		stats->snapstats_endtime_ts = (uint64) end;
	LWLockRelease(&state->lock);
}

void
synchdb_registry_catalog_prepare(SynchdbSharedState *state, const SynchdbConnectorKey *key,
								 const char *name, SynchdbCatalogFence *fence)
{
	SynchdbConnectorKey requested;
	char requested_name[SYNCHDB_CONNINFO_NAME_SIZE];
	bool busy = false;
	int i;

	if (state == NULL || synchdb_registry_entries(state) == NULL || fence == NULL ||
		!synchdb_connector_key_is_valid(key) || name == NULL ||
		strlen(name) >= sizeof(requested_name))
		elog(ERROR, "invalid connector registry catalog fence arguments");
	requested = *key;
	strlcpy(requested_name, name, sizeof(requested_name));
	MemSet(fence, 0, sizeof(*fence));
	fence->key = requested;
	strlcpy(fence->name, requested_name, sizeof(fence->name));
	fence->target.run.slot = -1;
	LWLockAcquire(&state->lock, LW_SHARED);
	fence->incarnation = state->incarnation;
	for (i = 0; i < state->allocated_slots; i++)
	{
		const ActiveConnectors *entry = &synchdb_registry_entries(state)[i];

		if (entry->conninfo.key.database_oid == requested.database_oid &&
			strcmp(entry->conninfo.name, requested_name) == 0 &&
			synchdb_registry_occupied(entry))
			busy = true;
		if (!synchdb_connector_key_equal(&entry->conninfo.key, &requested))
			continue;
		if (fence->captured || synchdb_registry_occupied(entry))
			busy = true;
		registry_copy_target_locked(state, i, &fence->target);
		fence->captured = true;
	}
	LWLockRelease(&state->lock);
	if (busy)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
						errmsg("connector \"%s\" must have no reserved, running or retrying run before changing its catalog resources", requested_name)));
}

bool
synchdb_registry_catalog_finish(SynchdbSharedState *state, const SynchdbCatalogFence *fence,
								const SynchdbWorkerOwner *caller, bool forget)
{
	bool changed;
	int current = -1;
	int i;

	if (state == NULL || synchdb_registry_entries(state) == NULL || fence == NULL ||
		!synchdb_connector_key_is_valid(&fence->key))
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	changed = state->incarnation != fence->incarnation;
	for (i = 0; i < state->allocated_slots; i++)
	{
		ActiveConnectors *entry = &synchdb_registry_entries(state)[i];

		if (entry->conninfo.key.database_oid == fence->key.database_oid &&
			strcmp(entry->conninfo.name, fence->name) == 0 &&
			synchdb_registry_occupied(entry))
			changed = true;
		if (synchdb_connector_key_equal(&entry->conninfo.key, &fence->key))
		{
			if (current >= 0)
				changed = true;
			current = i;
		}
	}
	if ((fence->captured && current != fence->target.run.slot) ||
		(!fence->captured && current >= 0))
		changed = true;
	if (!changed && fence->captured &&
		(!synchdb_registry_target_matches_locked(state, &fence->target, caller, false) ||
		 synchdb_registry_occupied(&synchdb_registry_entries(state)[current])))
		changed = true;
	if (!changed && forget && current >= 0)
	{
		ActiveConnectors *entry = &synchdb_registry_entries(state)[current];
		uint64 generation = entry->generation;
		uint64 attempt = entry->attempt;
		uint64 command_id = entry->next_command_id;

		MemSet(entry, 0, sizeof(*entry));
		entry->pid = InvalidPid;
		entry->generation = generation;
		entry->attempt = attempt;
		entry->next_command_id = command_id;
	}
	LWLockRelease(&state->lock);
	return !changed;
}
