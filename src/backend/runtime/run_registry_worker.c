/* Worker reports and copied views of the runtime registry. */
#include "postgres.h"

#include "miscadmin.h"
#include "runtime/run_registry_internal.h"

static void
registry_copy_view(const SynchdbSharedState *state, int slot, SynchdbRunView *view)
{
	const ActiveConnectors *entry = &synchdb_registry_entries(state)[slot];

	view->key = entry->conninfo.key;
	view->run.incarnation = state->incarnation;
	view->run.generation = entry->generation;
	view->run.slot = slot;
	view->pid = entry->pid;
	view->attempt = entry->attempt;
	view->lifecycle = entry->lifecycle;
	view->state = entry->state;
	view->stage = entry->stage;
	view->type = entry->type;
	memcpy(view->name, entry->conninfo.name, sizeof(view->name));
	memcpy(view->user, entry->conninfo.user, sizeof(view->user));
	memcpy(view->errmsg, entry->errmsg, sizeof(view->errmsg));
	memcpy(view->dbzoffset, entry->dbzoffset, sizeof(view->dbzoffset));
	view->isOraCompat = entry->conninfo.isOraCompat;
	view->retry = entry->retry;
	view->start_committed = entry->start_committed;
	view->controller_generation = entry->controller_generation;
	view->completed_command_id = entry->completed_command_id;
	view->command_succeeded = entry->command_succeeded;
	view->command_sqlstate = entry->command_sqlstate;
	view->bootstrap_status = entry->bootstrap_status;
	view->source_ready = entry->source_ready;
	view->failure_sqlstate = entry->failure_sqlstate;
	view->exit_code = entry->exit_code;
	memcpy(&view->stats, &entry->stats, sizeof(view->stats));
}

static void
registry_empty_view(SynchdbRunView *view)
{
	MemSet(view, 0, sizeof(*view));
	view->run.slot = -1;
	view->pid = InvalidPid;
}

bool
synchdb_registry_read_slot(SynchdbSharedState *state, int slot,
						   const SynchdbWorkerOwner *owner, SynchdbRunView *view)
{
	bool found = false;

	if (view == NULL)
		return false;
	registry_empty_view(view);
	if (!synchdb_registry_valid_slot(state, slot))
		return false;
	LWLockAcquire(&state->lock, LW_SHARED);
	if (owner == NULL || (owner->run.slot == slot && synchdb_registry_owned_locked(state, owner)))
	{
		registry_copy_view(state, slot, view);
		found = true;
	}
	LWLockRelease(&state->lock);
	return found;
}

bool
synchdb_registry_next_view(SynchdbSharedState *state, Oid database_oid,
						   int *cursor, SynchdbRunView *view)
{
	bool found = false;

	if (view == NULL)
		return false;
	registry_empty_view(view);
	if (!state || !synchdb_registry_entries(state) || cursor == NULL)
		return false;
	if (*cursor < 0)
		*cursor = 0;
	LWLockAcquire(&state->lock, LW_SHARED);
	while (*cursor < state->allocated_slots)
	{
		int slot = (*cursor)++;
		const ActiveConnectors *entry = &synchdb_registry_entries(state)[slot];

		if (entry->conninfo.name[0] != '\0' && entry->conninfo.key.database_oid == database_oid)
		{
			registry_copy_view(state, slot, view);
			found = true;
			break;
		}
	}
	LWLockRelease(&state->lock);
	return found;
}

bool
synchdb_registry_any_active(SynchdbSharedState *state, Oid database_oid,
							char *name, Size name_size)
{
	int i;
	bool active = false;

	if (name && name_size > 0)
		name[0] = '\0';
	if (!state || !synchdb_registry_entries(state))
		return false;
	LWLockAcquire(&state->lock, LW_SHARED);
	for (i = 0; i < state->allocated_slots; i++)
	{
		const ActiveConnectors *entry = &synchdb_registry_entries(state)[i];

		if (synchdb_registry_occupied(entry) && entry->conninfo.key.database_oid == database_oid)
		{
			if (name && name_size > 0)
				strlcpy(name, entry->conninfo.name, name_size);
			active = true;
			break;
		}
	}
	LWLockRelease(&state->lock);
	return active;
}

bool
synchdb_registry_report_state(SynchdbSharedState *state, const SynchdbWorkerOwner *owner,
							  ConnectorState value)
{
	bool owned;

	if (!state || !synchdb_registry_entries(state))
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	owned = synchdb_registry_owned_locked(state, owner);
	if (owned)
		synchdb_registry_entries(state)[owner->run.slot].state = value;
	LWLockRelease(&state->lock);
	return owned;
}

bool
synchdb_registry_report_stage(SynchdbSharedState *state, const SynchdbWorkerOwner *owner,
							  ConnectorStage value)
{
	bool owned;

	if (!state || !synchdb_registry_entries(state))
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	owned = synchdb_registry_owned_locked(state, owner);
	if (owned)
		synchdb_registry_entries(state)[owner->run.slot].stage = value;
	LWLockRelease(&state->lock);
	return owned;
}

bool
synchdb_registry_report_error(SynchdbSharedState *state, const SynchdbWorkerOwner *owner,
							  const char *value)
{
	bool owned;

	if (!state || !synchdb_registry_entries(state))
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	owned = synchdb_registry_owned_locked(state, owner);
	if (owned)
		strlcpy(synchdb_registry_entries(state)[owner->run.slot].errmsg, value ? value : "",
				sizeof(synchdb_registry_entries(state)[owner->run.slot].errmsg));
	LWLockRelease(&state->lock);
	return owned;
}

bool
synchdb_registry_report_offset(SynchdbSharedState *state, const SynchdbWorkerOwner *owner,
							   const char *value)
{
	bool owned;

	if (!state || !synchdb_registry_entries(state))
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	owned = synchdb_registry_owned_locked(state, owner);
	if (owned)
		strlcpy(synchdb_registry_entries(state)[owner->run.slot].dbzoffset, value ? value : "",
				sizeof(synchdb_registry_entries(state)[owner->run.slot].dbzoffset));
	LWLockRelease(&state->lock);
	return owned;
}

bool
synchdb_registry_report_batch_completion(SynchdbSharedState *state,
										const SynchdbWorkerOwner *owner)
{
	bool owned;

	if (!state || !synchdb_registry_entries(state))
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	owned = synchdb_registry_owned_locked(state, owner);
	if (owned)
		synchdb_registry_entries(state)[owner->run.slot].stats.genstats.stats_batch_completion++;
	LWLockRelease(&state->lock);
	return owned;
}

bool
synchdb_registry_report_stats(SynchdbSharedState *state, const SynchdbWorkerOwner *owner,
							  const SynchdbStatistics *delta)
{
	SynchdbStatistics *stats;

	if (!state || !synchdb_registry_entries(state) || !delta)
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (!synchdb_registry_owned_locked(state, owner))
	{
		LWLockRelease(&state->lock);
		return false;
	}
	stats = &synchdb_registry_entries(state)[owner->run.slot].stats;
#define ADD_FIELD(group, field) stats->group.field += delta->group.field
	ADD_FIELD(cdcstats, stats_ddl);
	ADD_FIELD(cdcstats, stats_dml);
	ADD_FIELD(cdcstats, stats_create);
	ADD_FIELD(cdcstats, stats_update);
	ADD_FIELD(cdcstats, stats_delete);
	ADD_FIELD(cdcstats, stats_tx);
	ADD_FIELD(cdcstats, stats_truncate);
	ADD_FIELD(genstats, stats_bad_change_event);
	ADD_FIELD(genstats, stats_total_change_event);
	ADD_FIELD(genstats, stats_batch_completion);
	ADD_FIELD(snapstats, snapstats_tables);
	ADD_FIELD(snapstats, snapstats_rows);
#undef ADD_FIELD
	stats->genstats.stats_first_src_ts = delta->genstats.stats_first_src_ts;
	stats->genstats.stats_first_pg_ts = delta->genstats.stats_first_pg_ts;
	stats->genstats.stats_last_src_ts = delta->genstats.stats_last_src_ts;
	stats->genstats.stats_last_pg_ts = delta->genstats.stats_last_pg_ts;
	if (delta->snapstats.snapstats_begintime_ts > 0)
	{
		stats->snapstats.snapstats_begintime_ts = delta->snapstats.snapstats_begintime_ts;
		stats->snapstats.snapstats_endtime_ts = 0;
	}
	if (delta->snapstats.snapstats_endtime_ts > 0)
		stats->snapstats.snapstats_endtime_ts = delta->snapstats.snapstats_endtime_ts;
	LWLockRelease(&state->lock);
	return true;
}

static bool
registry_request_matches(const ActiveConnectors *entry, const SynchdbWorkerOwner *owner,
						 uint64 command_id)
{
	return command_id != 0 && entry->req.command_id == command_id &&
		entry->req.target_generation == owner->run.generation &&
		entry->req.target_attempt == owner->attempt;
}

bool
synchdb_registry_poll_request(SynchdbSharedState *state, const SynchdbWorkerOwner *owner,
							  SynchdbRequest *request, ConnectorState *current_state)
{
	const ActiveConnectors *entry;
	bool found = false;

	if (!request || !current_state)
		return false;
	MemSet(request, 0, sizeof(*request));
	*current_state = STATE_UNDEF;
	if (!state || !synchdb_registry_entries(state))
		return false;
	LWLockAcquire(&state->lock, LW_SHARED);
	if (synchdb_registry_owned_locked(state, owner))
	{
		entry = &synchdb_registry_entries(state)[owner->run.slot];
		if (entry->lifecycle != SYNCHDB_RUN_STOPPING && entry->req.reqstate != STATE_UNDEF &&
			registry_request_matches(entry, owner, entry->req.command_id))
		{
			memcpy(request, &entry->req, sizeof(*request));
			*current_state = entry->state;
			found = true;
		}
	}
	LWLockRelease(&state->lock);
	return found;
}

bool
synchdb_registry_ack_request(SynchdbSharedState *state, const SynchdbWorkerOwner *owner,
							 uint64 command_id)
{
	return synchdb_registry_complete_request(state, owner, command_id, true, 0, NULL);
}

bool
synchdb_registry_complete_request(SynchdbSharedState *state, const SynchdbWorkerOwner *owner,
							 uint64 command_id, bool succeeded, int sqlstate, const char *message)
{
	bool matched = false;

	if (!state || !synchdb_registry_entries(state))
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (synchdb_registry_owned_locked(state, owner) &&
		registry_request_matches(&synchdb_registry_entries(state)[owner->run.slot], owner, command_id))
	{
		ActiveConnectors *entry = &synchdb_registry_entries(state)[owner->run.slot];

		entry->completed_command_id = command_id;
		entry->command_succeeded = succeeded;
		entry->command_sqlstate = succeeded ? 0 : sqlstate;
		if (!succeeded)
		{
			strlcpy(entry->errmsg, message ? message : "connector command failed", sizeof(entry->errmsg));
		}
		MemSet(&synchdb_registry_entries(state)[owner->run.slot].req, 0, sizeof(SynchdbRequest));
		matched = true;
	}
	LWLockRelease(&state->lock);
	return matched;
}

void
synchdb_registry_report_bootstrap(SynchdbSharedState *state, const SynchdbWorkerOwner *owner, int status)
{
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (synchdb_registry_owned_locked(state, owner))
		synchdb_registry_entries(state)[owner->run.slot].bootstrap_status = status;
	LWLockRelease(&state->lock);
}

void
synchdb_registry_report_ready(SynchdbSharedState *state, const SynchdbWorkerOwner *owner, bool ready)
{
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (synchdb_registry_owned_locked(state, owner))
		synchdb_registry_entries(state)[owner->run.slot].source_ready = ready;
	LWLockRelease(&state->lock);
}

void
synchdb_registry_report_failure(SynchdbSharedState *state, const SynchdbWorkerOwner *owner,
								int sqlstate, const char *message)
{
	if (!state || !synchdb_registry_entries(state))
		return;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (synchdb_registry_owned_locked(state, owner))
	{
		ActiveConnectors *entry = &synchdb_registry_entries(state)[owner->run.slot];

		entry->failure_sqlstate = sqlstate;
		strlcpy(entry->errmsg, message ? message : "connector failed", sizeof(entry->errmsg));
	}
	LWLockRelease(&state->lock);
}

bool
synchdb_registry_publish_run(SynchdbSharedState *state, const SynchdbWorkerOwner *owner,
							 const SynchdbRunSpec *spec, const SynchdbRequest *consumed_request)
{
	ActiveConnectors *entry;

	if (!state || !synchdb_registry_entries(state) || !spec || !consumed_request)
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	if (!synchdb_registry_owned_locked(state, owner))
	{
		LWLockRelease(&state->lock);
		return false;
	}
	entry = &synchdb_registry_entries(state)[owner->run.slot];
	if (entry->lifecycle == SYNCHDB_RUN_STOPPING ||
		!synchdb_connector_key_equal(&entry->conninfo.key, &spec->connection.key) ||
		spec->connection.config_revision < entry->conninfo.config_revision)
	{
		LWLockRelease(&state->lock);
		return false;
	}
	entry->type = spec->legacy_type;
	memcpy(&entry->conninfo, &spec->connection, sizeof(entry->conninfo));
	memcpy(entry->snapshotMode, spec->snapshot_mode, sizeof(entry->snapshotMode));
	if (consumed_request->target_generation == owner->run.generation &&
		consumed_request->target_attempt == owner->attempt &&
		registry_request_matches(entry, owner, consumed_request->command_id))
		MemSet(&entry->req, 0, sizeof(entry->req));
	LWLockRelease(&state->lock);
	return true;
}

bool
synchdb_registry_publish_loglevel(SynchdbSharedState *state,
								 const SynchdbWorkerOwner *owner, int level)
{
	bool owned;

	if (!state || !synchdb_registry_entries(state) || level < LOG_LEVEL_ALL || level > LOG_LEVEL_TRACE)
		return false;
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);
	owned = synchdb_registry_owned_locked(state, owner);
	if (owned)
		synchdb_registry_entries(state)[owner->run.slot].conninfo.run_options.dbz_log_level = level;
	LWLockRelease(&state->lock);
	return owned;
}
