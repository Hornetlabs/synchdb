/* Controller leases fence attempts; only the native owner proves registration exit. */
#include "postgres.h"
#include "miscadmin.h"
#include "runtime/run_registry_internal.h"
#include "storage/procarray.h"

bool
synchdb_registry_controller_present(SynchdbSharedState *state, Oid database_oid)
{
    pid_t pid = 0;
    int i;

    if (!state || !OidIsValid(database_oid))
        return false;
    LWLockAcquire(&state->lock, LW_SHARED);
    for (i = 0; i < state->controller_capacity; ++i)
        if (synchdb_registry_controllers(state)[i].database_oid == database_oid)
        {
            pid = synchdb_registry_controllers(state)[i].pid;
            break;
        }
    LWLockRelease(&state->lock);
    return pid > 0 && BackendPidGetProc(pid) != NULL;
}

uint64
synchdb_registry_controller_claim(SynchdbSharedState *state, Oid database_oid)
{
    int slot = -1;
    int i;
    uint64 generation;

    if (!state || !synchdb_registry_controllers(state) || !OidIsValid(database_oid))
        elog(ERROR, "controller registry is not initialized");
    /* The caller also owns this database's session advisory lock. */
    LWLockAcquire(&state->lock, LW_EXCLUSIVE);
    for (i = 0; i < state->controller_capacity; ++i)
    {
        SynchdbControllerLease *entry = &synchdb_registry_controllers(state)[i];

        if (entry->database_oid == database_oid)
        {
            slot = i;
            break;
        }
        if (slot < 0 && entry->pid == 0)
            slot = i;
    }
    if (slot < 0 || state->next_controller_generation == PG_UINT64_MAX)
    {
        LWLockRelease(&state->lock);
        ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                        errmsg("maximum number of database controllers reached")));
    }
    generation = ++state->next_controller_generation;
    synchdb_registry_controllers(state)[slot].database_oid = database_oid;
    synchdb_registry_controllers(state)[slot].generation = generation;
    synchdb_registry_controllers(state)[slot].pid = MyProcPid;
    /* An unclaimed old registration is fenced before its slot can be reused. */
    for (i = 0; i < state->allocated_slots; ++i)
    {
        ActiveConnectors *entry = &synchdb_registry_entries(state)[i];

        if (entry->conninfo.key.database_oid != database_oid ||
            !synchdb_registry_occupied(entry))
            continue;
        entry->retry = false;
        entry->lifecycle = entry->pid > 0 ? SYNCHDB_RUN_STOPPING : SYNCHDB_RUN_FREE;
        MemSet(&entry->req, 0, sizeof(entry->req));
    }
    LWLockRelease(&state->lock);
    return generation;
}

void
synchdb_registry_controller_release(SynchdbSharedState *state, Oid database_oid, uint64 generation)
{
    int i;

    if (!state || !synchdb_registry_controllers(state))
        return;
    LWLockAcquire(&state->lock, LW_EXCLUSIVE);
    for (i = 0; i < state->controller_capacity; ++i)
        if (synchdb_registry_controllers(state)[i].database_oid == database_oid &&
            synchdb_registry_controllers(state)[i].generation == generation &&
            synchdb_registry_controllers(state)[i].pid == MyProcPid)
            synchdb_registry_controllers(state)[i].pid = 0;
    LWLockRelease(&state->lock);
}

bool
synchdb_registry_controller_valid(SynchdbSharedState *state, Oid database_oid, uint64 generation)
{
    bool valid = false;
    int i;

    if (!state || !synchdb_registry_controllers(state) || generation == 0)
        return false;
    LWLockAcquire(&state->lock, LW_SHARED);
    for (i = 0; i < state->controller_capacity; ++i)
        if (synchdb_registry_controllers(state)[i].database_oid == database_oid &&
            synchdb_registry_controllers(state)[i].generation == generation && synchdb_registry_controllers(state)[i].pid > 0)
            valid = true;
    LWLockRelease(&state->lock);
    return valid;
}

bool
synchdb_registry_assign_controller(SynchdbSharedState *state, const SynchdbRunHandle *run,
                                  uint64 generation)
{
    bool assigned = false;

    LWLockAcquire(&state->lock, LW_EXCLUSIVE);
    if (synchdb_registry_matches_locked(state, run))
    {
        synchdb_registry_entries(state)[run->slot].controller_generation = generation;
        assigned = true;
    }
    LWLockRelease(&state->lock);
    return assigned;
}

bool
synchdb_registry_reap(SynchdbSharedState *state, const SynchdbRunHandle *run, bool proven_stopped)
{
    SynchdbRunView view;
    bool reaped = false;

    if (!synchdb_registry_read_slot(state, run->slot, NULL, &view) ||
        view.run.incarnation != run->incarnation || view.run.generation != run->generation)
        return false;
    /* No handle after controller recovery: an unknown live PID is retained. */
    if (!proven_stopped && view.pid > 0 && BackendPidGetProc(view.pid) != NULL)
        return false;
    LWLockAcquire(&state->lock, LW_EXCLUSIVE);
    if (synchdb_registry_matches_locked(state, run) &&
        synchdb_registry_entries(state)[run->slot].pid == view.pid &&
        (proven_stopped || view.controller_generation != 0))
    {
        ActiveConnectors *entry = &synchdb_registry_entries(state)[run->slot];

        if (entry->failure_sqlstate == 0 &&
            (entry->lifecycle == SYNCHDB_RUN_RUNNING || entry->lifecycle == SYNCHDB_RUN_RESERVED))
        {
            entry->failure_sqlstate = ERRCODE_CRASH_SHUTDOWN;
            entry->exit_code = 1;
            strlcpy(entry->errmsg, "connector process exited without reporting cleanup", sizeof(entry->errmsg));
        }
        entry->pid = InvalidPid;
        entry->lifecycle = SYNCHDB_RUN_FREE;
        entry->state = STATE_UNDEF;
        entry->retry = false;
        MemSet(&entry->req, 0, sizeof(entry->req));
        reaped = true;
    }
    LWLockRelease(&state->lock);
    return reaped;
}
