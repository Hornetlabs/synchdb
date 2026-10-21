/*
 * One controller per database. SQL commits intent; this process owns native
 * registrations and reconciles durable commands with generation-fenced runs.
 */
#include "postgres.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/control_store.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "executor/pg_compat.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "runtime/controller.h"
#include "runtime/config.h"
#include "snapshot/artifact_store.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/procsignal.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

#ifndef SYNCHDB_LIBRARY_NAME
#define SYNCHDB_LIBRARY_NAME "synchdb"
#endif

typedef struct ManagedWorker
{
    SynchdbConnectorKey key;
    SynchdbRunHandle run;
    BackgroundWorkerHandle *handle;
    int64 operation_id;
    bool startup_receipt_failed;
    struct ManagedWorker *next;
} ManagedWorker;

typedef struct DatabaseWorker
{
    Oid database_oid;
    BackgroundWorkerHandle *handle;
    struct DatabaseWorker *next;
} DatabaseWorker;

typedef struct ManualControllerStartup
{
    uint32 version;
    TransactionId creator_xid;
    uint64 registry_incarnation;
} ManualControllerStartup;

StaticAssertDecl(sizeof(ManualControllerStartup) <= BGW_EXTRALEN,
                 "manual controller startup data must fit bgw_extra");

static SynchdbSharedState *controller_registry;
static uint64 controller_generation;
static ManagedWorker *managed_workers;

/* Retain a publication fence outside the per-cycle context. If persisting a
 * runtime receipt fails, even a second catalog error must not make this same
 * controller publish the non-transactional operation again. A replacement
 * controller uses the durable EXECUTING/controller_generation fence instead. */
typedef struct PublishedCommand
{
    SynchdbConnectorKey key;
    int64 operation_id;
    bool published;
    struct PublishedCommand *next;
} PublishedCommand;

static PublishedCommand *published_commands;

static PublishedCommand *
command_publication(const SynchdbControlRow *row, bool create)
{
    PublishedCommand **link=&published_commands;

    while (*link)
    {
        PublishedCommand *entry=*link;

        if (synchdb_connector_key_equal(&entry->key,&row->key))
        {
            if (entry->operation_id==row->operation_id)
                return entry;
            *link=entry->next;
            pfree(entry);
            break;
        }
        link=&entry->next;
    }
    if (create)
    {
        PublishedCommand *entry=MemoryContextAllocZero(TopMemoryContext,sizeof(*entry));

        entry->key=row->key;
        entry->operation_id=row->operation_id;
        entry->next=published_commands;
        published_commands=entry;
        return entry;
    }
    return NULL;
}

static void
forget_command_publication(const SynchdbControlRow *row)
{
    PublishedCommand **link=&published_commands;

    while (*link)
    {
        PublishedCommand *entry=*link;

        if (synchdb_connector_key_equal(&entry->key,&row->key) && entry->operation_id==row->operation_id)
        {
            *link=entry->next;
            pfree(entry);
            return;
        }
        link=&entry->next;
    }
}
static DatabaseWorker *database_workers;

static void
begin_control_transaction(void)
{
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
}

static void
commit_control_transaction(void)
{
    PopActiveSnapshot();
    CommitTransactionCommand();
}

static void
controller_exit(int code, Datum arg)
{
    ManagedWorker *entry;

    for (entry=managed_workers; entry; entry=entry->next)
    {
        if (entry->handle)
            TerminateBackgroundWorker(entry->handle);
        (void) synchdb_registry_cancel(controller_registry,&entry->run);
    }
    synchdb_registry_controller_release(controller_registry,MyDatabaseId,controller_generation);
}

static void
supervisor_exit(int code, Datum arg)
{
    DatabaseWorker *entry;

    for (entry=database_workers;entry;entry=entry->next)
        if (entry->handle)
            TerminateBackgroundWorker(entry->handle);
}

static ManagedWorker *
find_managed(const SynchdbConnectorKey *key)
{
    ManagedWorker *entry;

    for (entry=managed_workers; entry; entry=entry->next)
        if (synchdb_connector_key_equal(&entry->key,key))
            return entry;
    return NULL;
}

static void
forget_managed(ManagedWorker *entry)
{
    ManagedWorker **link=&managed_workers;

    while (*link && *link!=entry)
        link=&(*link)->next;
    if (*link)
        *link=entry->next;
    if (entry->handle)
        pfree(entry->handle);
    pfree(entry);
}

static bool
retryable(int sqlstate)
{
    int category=ERRCODE_TO_CATEGORY(sqlstate);

    return category==ERRCODE_CONNECTION_EXCEPTION ||
           sqlstate==ERRCODE_T_R_SERIALIZATION_FAILURE ||
           sqlstate==ERRCODE_T_R_DEADLOCK_DETECTED ||
           category==ERRCODE_INSUFFICIENT_RESOURCES ||
           sqlstate==ERRCODE_CANNOT_CONNECT_NOW;
}

static void
update_row(const SynchdbControlRow *row, const char *actual, const char *desired,
           const char *status, int code, const char *message, int attempt, TimestampTz retry_at)
{
    begin_control_transaction();
    synchdb_control_store_update(row,actual,desired,status,code,message,attempt,retry_at);
    commit_control_transaction();
}

static void
fail_row(const SynchdbControlRow *row, int code, const char *message, bool may_retry)
{
    bool retry=may_retry && row->retry_enabled && row->retry_attempt<8 && retryable(code);
    int attempt=row->retry_attempt+1;
    int delay=Min(60000,1000 << Min(attempt-1,6));

    update_row(row,retry ? "RETRY_WAIT" : "FAILED",row->desired,
               retry ? NULL : "FAILED",code,message,attempt,
               retry ? TimestampTzPlusMilliseconds(GetCurrentTimestamp(),delay) : DT_NOBEGIN);
}

static bool
current_view(const SynchdbControlRow *row, SynchdbRunView *view, ManagedWorker **managed,
             bool *native_stopped)
{
    SynchdbControllerTarget target;
    ManagedWorker *entry=find_managed(&row->key);

    *managed=entry;
    *native_stopped=false;
    if (entry)
    {
        pid_t pid;
        BgwHandleStatus status=GetBackgroundWorkerPid(entry->handle,&pid);

        *native_stopped=status==BGWH_STOPPED;
        if (*native_stopped)
            (void) synchdb_registry_reap(controller_registry,&entry->run,true);
        if (synchdb_registry_read_slot(controller_registry,entry->run.slot,NULL,view) &&
            view->run.generation==entry->run.generation &&
            view->run.incarnation==entry->run.incarnation)
            return view->lifecycle!=SYNCHDB_RUN_FREE;
        MemSet(view,0,sizeof(*view));
        view->run=entry->run;
        view->failure_sqlstate=ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN;
        strlcpy(view->errmsg,"worker display generation changed before its terminal result was collected",sizeof(view->errmsg));
        return false;
    }
    if (synchdb_registry_capture(controller_registry,&row->key,false,&target))
    {
        (void) synchdb_registry_reap(controller_registry,&target.run,false);
        if (synchdb_registry_read_slot(controller_registry,target.run.slot,NULL,view))
            return view->lifecycle!=SYNCHDB_RUN_FREE;
    }
    MemSet(view,0,sizeof(*view));
    view->run.slot=-1;
    return false;
}

static ConnectorState
request_state(const char *operation)
{
    if (strcmp(operation,"PAUSE")==0) return STATE_PAUSED;
    if (strcmp(operation,"RESUME")==0) return STATE_SYNCING;
    if (strcmp(operation,"RELOAD")==0) return STATE_RELOAD_OBJMAP;
    if (strcmp(operation,"MEMDUMP")==0) return STATE_MEMDUMP;
    if (strcmp(operation,"LOGLEVEL")==0) return STATE_DBZ_LOGLEVEL_UPDATE;
    if (strcmp(operation,"OFFSET")==0) return STATE_OFFSET_UPDATE;
    return STATE_UNDEF;
}

static bool
maintenance_operation(const SynchdbControlRow *row)
{
    return strcmp(row->operation,"RESET_STATS")==0 ||
        (request_state(row->operation)!=STATE_UNDEF && strcmp(row->operation,"PAUSE")!=0 &&
         strcmp(row->operation,"RESUME")!=0);
}

static void
submit_request(SynchdbControlRow *row, const SynchdbRunView *view, ConnectorState state)
{
    SynchdbControllerTarget target;
    SynchdbRequest request={0};
    PublishedCommand *publication=command_publication(row,true);

    if (publication->published)
        ereport(ERROR,(errcode(ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN),
            errmsg("runtime command was already published without a durable receipt")));
    if (!synchdb_registry_capture(controller_registry,&row->key,true,&target) ||
        target.run.generation!=view->run.generation)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_IN_USE),errmsg("connector changed before command execution")));
    request.reqstate=state;
    strlcpy(request.reqdata,row->payload,sizeof(request.reqdata));
    synchdb_registry_submit(controller_registry,&target,NULL,&request);
    publication->published=true;
    begin_control_transaction();
    synchdb_control_store_request(row,controller_generation,request.command_id,target.run.generation,
                                 strcmp(row->status,"EXECUTING")==0);
    commit_control_transaction();
    forget_command_publication(row);
}

static void
start_worker(SynchdbControlRow *row)
{
    SynchdbRunSpec spec;
    ManagedWorker *entry=MemoryContextAllocZero(TopMemoryContext,sizeof(*entry));
    MemoryContext caller=CurrentMemoryContext;

    entry->key=row->key;
    entry->run.slot=-1;
    entry->operation_id=row->operation_id;
    entry->next=managed_workers;
    managed_workers=entry;
    begin_control_transaction();
    synchdb_control_build_spec(row->name,row->mode,row->options,row->snapshot_engine,
                              row->schema_sync,row->oracle_compatible,&spec);
    if (!synchdb_connector_key_equal(&spec.connection.key,&row->key) ||
        spec.connection.config_revision!=row->revision)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("accepted connector revision changed before execution")));
    /* Revalidate the accepted role and connection privilege before forking. */
    synchdb_pg_worker_check_connection(row->key.database_oid,row->role);
    MemoryContextSwitchTo(caller);
    commit_control_transaction();
    PG_TRY();
    {
        entry->handle=synchdb_control_start_worker(&spec,row->role,controller_generation,&entry->run);
    }
    PG_CATCH();
    {
        forget_managed(entry);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PG_TRY();
    {
        begin_control_transaction();
        synchdb_control_store_request(row,controller_generation,0,entry->run.generation,
                                     strcmp(row->status,"EXECUTING")==0);
        synchdb_control_store_update(row,"STARTING",row->desired,NULL,0,NULL,row->retry_attempt,DT_NOBEGIN);
        commit_control_transaction();
    }
    PG_CATCH();
    {
        entry->startup_receipt_failed=true;
        (void) synchdb_registry_cancel(controller_registry,&entry->run);
        TerminateBackgroundWorker(entry->handle);
        PG_RE_THROW();
    }
    PG_END_TRY();
    (void) synchdb_registry_commit_start(controller_registry,&entry->run);
}

static void
reconcile(SynchdbControlRow *row)
{
    ManagedWorker *managed;
    SynchdbRunView view;
    bool stopped;
    bool active=current_view(row,&view,&managed,&stopped);
    bool pending=strcmp(row->status,"PENDING")==0;
    bool executing=strcmp(row->status,"EXECUTING")==0;
    bool restart=strcmp(row->operation,"RESTART")==0 && (pending || executing);
    bool maintenance=maintenance_operation(row);
    ConnectorState requested=request_state(row->operation);
    PublishedCommand *publication=command_publication(row,false);

    if (publication && publication->published)
    {
        if (pending || executing || strcmp(row->actual,"FAILED")!=0)
            fail_row(row,ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN,
                "runtime command was published before its receipt could commit; inspect its effects before retry",false);
        forget_command_publication(row);
        return;
    }
    /* A FREE report can precede actual process/registration exit. */
    if (!active && managed && !stopped)
        return;

    if (pending)
    {
        bool claimed;

        begin_control_transaction();
        claimed=synchdb_control_store_claim(row,controller_generation);
        commit_control_transaction();
        if (!claimed)
            return;
        row->status="EXECUTING";
        row->controller_generation=controller_generation;
        executing=true;
    }
    if (executing && row->controller_generation!=controller_generation)
    {
        /* A native request can have escaped before its receipt was durable. */
        if (maintenance)
        {
            update_row(row,"RECOVERING",row->desired,"FAILED",ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN,
                       "controller changed while the command was executing; inspect its effects before retry",0,DT_NOBEGIN);
            return;
        }
        row->request_id=0;
        {
            bool claimed;

            begin_control_transaction();
            claimed=synchdb_control_store_claim(row,controller_generation);
            commit_control_transaction();
            if (!claimed)
                return;
        }
        row->controller_generation=controller_generation;
    }

    if (strcmp(row->desired,"STOPPED")==0 ||
        (restart && active && (!managed || managed->operation_id!=row->operation_id)))
    {
        bool collected_worker=managed!=NULL;

        if (active)
        {
            /* Keep the stopped run's identity through controller recovery.
             * A retained historical FREE row is not this STOP's outcome. */
            if (strcmp(row->desired,"STOPPED")==0 && row->run_generation!=view.run.generation)
            {
                begin_control_transaction();
                synchdb_control_store_request(row,controller_generation,0,view.run.generation,executing);
                commit_control_transaction();
                row->run_generation=view.run.generation;
            }
            (void) synchdb_registry_cancel(controller_registry,&view.run);
            if (managed && managed->handle)
                TerminateBackgroundWorker(managed->handle);
            update_row(row,"STOPPING",row->desired,NULL,0,NULL,0,DT_NOBEGIN);
            return;
        }
        /* The registration can outlive its released shared display row. */
        if (managed && !stopped)
            return;
        if (managed)
            forget_managed(managed);
        if (strcmp(row->desired,"STOPPED")==0)
        {
            if (view.failure_sqlstate!=0 && (collected_worker ||
                (row->run_generation!=0 && row->run_generation==view.run.generation)))
                update_row(row,"STOPPED","STOPPED","FAILED",view.failure_sqlstate,
                    view.errmsg[0] ? view.errmsg : "connector shutdown failed",0,DT_NOBEGIN);
            else if (executing && maintenance)
                update_row(row,"STOPPED","STOPPED","FAILED",ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "maintenance command requires a running worker",0,DT_NOBEGIN);
            else
                update_row(row,"STOPPED","STOPPED","SUCCEEDED",0,"stopped",0,DT_NOBEGIN);
            return;
        }
        managed=NULL;
    }
    if (active && view.controller_generation!=controller_generation)
    {
        (void) synchdb_registry_cancel(controller_registry,&view.run);
        update_row(row,"RECOVERING",row->desired,NULL,0,
                   "waiting for the previous controller's worker to exit",0,DT_NOBEGIN);
        return;
    }
    if (managed && stopped)
    {
        bool restarting=restart && managed->operation_id!=row->operation_id;
        bool startup_receipt_failed=managed->startup_receipt_failed;

        forget_managed(managed);
        managed=NULL;
        /* The failed controller transaction already recorded the outcome;
         * successful cancellation of its gated process must not erase it. */
        if (startup_receipt_failed)
            return;
        if (!restarting)
        {
            if (executing && maintenance && view.exit_code==0 && view.failure_sqlstate==0)
            {
                if (row->request_id && view.run.generation==row->run_generation &&
                    view.completed_command_id==row->request_id)
                    update_row(row,"STOPPED","STOPPED",view.command_succeeded ? "SUCCEEDED" : "FAILED",
                        view.command_succeeded ? 0 : (view.command_sqlstate ? view.command_sqlstate : ERRCODE_INTERNAL_ERROR),
                        view.command_succeeded ? "command completed before worker exit" : view.errmsg,0,DT_NOBEGIN);
                else
                    update_row(row,"STOPPED","STOPPED","FAILED",row->request_id ?
                        ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN : ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                        "worker exited without a matching maintenance command completion",0,DT_NOBEGIN);
                return;
            }
            if (view.exit_code!=0 || view.failure_sqlstate!=0)
                fail_row(row,view.failure_sqlstate ? view.failure_sqlstate : ERRCODE_INTERNAL_ERROR,
                         view.errmsg[0] ? view.errmsg : "connector worker failed",true);
            else
                update_row(row,"STOPPED","STOPPED","SUCCEEDED",0,"connector completed",0,DT_NOBEGIN);
            return;
        }
    }
    /* A failed transition is a completed command, not permission to publish
     * it again while reconciling the still-unreached desired state. */
    if (!pending && !executing && strcmp(row->status,"FAILED")==0 &&
        (requested==STATE_PAUSED || requested==STATE_SYNCING))
        return;
    if (active)
    {
        if (view.lifecycle==SYNCHDB_RUN_STOPPING)
            return;
        if (executing && row->request_id)
        {
            if (view.run.generation!=row->run_generation)
                fail_row(row,ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN,"command's worker generation changed",false);
            else if (view.completed_command_id==row->request_id)
            {
                const char *desired=row->desired;

                if (!view.command_succeeded && row->snapshot_engine==ENGINE_FDW)
                    desired="PAUSED";
                update_row(row,view.state==STATE_PAUSED ? "PAUSED" : view.source_ready ? "RUNNING" : "STARTING",desired,
                    view.command_succeeded ? "SUCCEEDED" : "FAILED",view.command_succeeded ? 0 :
                    (view.command_sqlstate ? view.command_sqlstate : ERRCODE_INTERNAL_ERROR),
                    view.command_succeeded ? "command completed" : view.errmsg,0,DT_NOBEGIN);
            }
            return;
        }
        if (row->snapshot_engine==ENGINE_FDW && view.bootstrap_status==0 &&
            (strcmp(row->operation,"START")==0 || strcmp(row->operation,"RESTART")==0) && executing)
        {
            if (strcmp(row->actual,"BOOTSTRAPPING")!=0)
                update_row(row,"BOOTSTRAPPING",row->desired,NULL,0,
                           "waiting for the committed bootstrap result",row->retry_attempt,DT_NOBEGIN);
            return;
        }
        if (view.state==STATE_PAUSED && (view.bootstrap_status>0 || row->schema_sync) &&
            !(executing && maintenance) && strcmp(row->operation,"RESUME")!=0 && strcmp(row->desired,"RUNNING")==0)
        {
            bool complete=view.bootstrap_status<=1;

            update_row(row,"PAUSED","PAUSED",complete ? "SUCCEEDED" : "FAILED",
                complete ? 0 : ERRCODE_EXTERNAL_ROUTINE_EXCEPTION,
                complete ? "bootstrap completed; CDC resume requires a verified handoff" : view.errmsg,
                0,DT_NOBEGIN);
            return;
        }
        if (executing && maintenance)
        {
            if (strcmp(row->operation,"RESET_STATS")==0)
            {
                SynchdbControllerTarget target;
                PublishedCommand *reset=command_publication(row,true);

                if (!synchdb_registry_capture(controller_registry,&row->key,false,&target) ||
                    target.run.generation!=view.run.generation)
                    ereport(ERROR,(errcode(ERRCODE_OBJECT_IN_USE),errmsg("connector changed before statistics reset")));
                synchdb_registry_reset_stats(controller_registry,&target,NULL);
                reset->published=true;
                update_row(row,view.state==STATE_PAUSED ? "PAUSED" : view.source_ready ? "RUNNING" : "STARTING",row->desired,
                           "SUCCEEDED",0,"statistics reset",0,DT_NOBEGIN);
                forget_command_publication(row);
            }
            else if (view.state==STATE_SYNCING || view.state==STATE_PAUSED)
                submit_request(row,&view,requested);
            return;
        }
        if (strcmp(row->desired,"PAUSED")==0 && view.state==STATE_SYNCING)
        {
            submit_request(row,&view,STATE_PAUSED);
            return;
        }
        if (strcmp(row->desired,"RUNNING")==0 && view.state==STATE_PAUSED)
        {
            submit_request(row,&view,STATE_SYNCING);
            return;
        }
        if (view.state==STATE_SYNCING || view.state==STATE_PAUSED)
        {
            const char *actual=view.state==STATE_PAUSED ? "PAUSED" : view.source_ready ? "RUNNING" : "STARTING";

            if (executing || strcmp(row->actual,actual)!=0 ||
                (view.source_ready && row->retry_attempt!=0))
                update_row(row,actual,row->desired,"SUCCEEDED",0,
                    view.state==STATE_PAUSED || view.source_ready ? "desired state reached" :
                    "engine launched; waiting to observe a committed source batch",
                    view.source_ready ? 0 : row->retry_attempt,DT_NOBEGIN);
        }
        return;
    }
    if (executing && maintenance)
    {
        fail_row(row,ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,"maintenance command requires a running worker",false);
        return;
    }
    if (strcmp(row->desired,"PAUSED")==0)
    {
        update_row(row,"PAUSED","PAUSED","SUCCEEDED",0,"paused without an active worker",0,DT_NOBEGIN);
        return;
    }
    if (strcmp(row->operation,"RESUME")==0 && row->snapshot_engine==ENGINE_FDW)
    {
        update_row(row,"PAUSED","PAUSED","FAILED",ERRCODE_FEATURE_NOT_SUPPORTED,
                   "FDW bootstrap has no verified CDC handoff; submit an explicit new bootstrap",0,DT_NOBEGIN);
        return;
    }
    if (!pending && strcmp(row->actual,"FAILED")==0)
        return;
    if (strcmp(row->actual,"RETRY_WAIT")==0 && row->retry_at>GetCurrentTimestamp())
        return;
    if (requested!=STATE_UNDEF && executing && strcmp(row->operation,"RESUME")!=0)
    {
        fail_row(row,ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,"command requires a running worker",false);
        return;
    }
    start_worker(row);
}

static void
reconcile_one(SynchdbControlRow *row, MemoryContext context)
{
    PG_TRY();
    {
        reconcile(row);
    }
    PG_CATCH();
    {
        ErrorData *error;
        ManagedWorker *managed=find_managed(&row->key);
        PublishedCommand *publication=command_publication(row,false);

        MemoryContextSwitchTo(context);
        error=CopyErrorData();
        AbortCurrentTransaction();
        FlushErrorState();
        if (managed && managed->handle==NULL)
            forget_managed(managed);
        if (publication && publication->published)
        {
            fail_row(row,ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN,
                "runtime command was published before its receipt could commit; inspect its effects before retry",false);
            forget_command_publication(row);
        }
        else
        {
            forget_command_publication(row);
            fail_row(row,error->sqlerrcode,error->message,true);
        }
        FreeErrorData(error);
    }
    PG_END_TRY();
}

void
synchdb_control_ensure_manual_controller(SynchdbSharedState *registry)
{
    LOCKTAG tag;
    volatile bool locked=false;

    if (synchdb_registry_is_preloaded())
        return;
    /* Serialize registrations only until the database controller has claimed
     * its shared identity, not until the caller's possibly long transaction
     * commits. The controller's separate session lock still fences ownership. */
    SET_LOCKTAG_ADVISORY(tag,MyDatabaseId,0x53594e43,0,4);
    PG_TRY();
    {
        BackgroundWorkerHandle *handle=NULL;

        (void) LockAcquire(&tag,ExclusiveLock,false,false);
        locked=true;
        if (!synchdb_registry_controller_present(registry,MyDatabaseId))
        {
            BackgroundWorker worker={0};
            ManualControllerStartup startup={0};
            TimestampTz deadline=TimestampTzPlusMilliseconds(GetCurrentTimestamp(),10000);

            startup.version=1;
            startup.creator_xid=GetTopTransactionId();
            startup.registry_incarnation=synchdb_registry_incarnation(registry);
            worker.bgw_flags=BGWORKER_SHMEM_ACCESS|BGWORKER_BACKEND_DATABASE_CONNECTION;
            worker.bgw_start_time=BgWorkerStart_ConsistentState;
            /* A manual controller recovers its admitted intents after a local
             * controller failure, but is never registered at postmaster boot. */
            worker.bgw_restart_time=1;
            worker.bgw_main_arg=ObjectIdGetDatum(MyDatabaseId);
            worker.bgw_notify_pid=MyProcPid;
            memcpy(worker.bgw_extra,&startup,sizeof(startup));
            strlcpy(worker.bgw_library_name,SYNCHDB_LIBRARY_NAME,sizeof(worker.bgw_library_name));
            strlcpy(worker.bgw_function_name,"synchdb_control_database_main",sizeof(worker.bgw_function_name));
            snprintf(worker.bgw_name,BGW_MAXLEN,"synchdb controller %u",MyDatabaseId);
            strlcpy(worker.bgw_type,"synchdb controller",sizeof(worker.bgw_type));
            if (!RegisterDynamicBackgroundWorker(&worker,&handle))
                ereport(ERROR,(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                    errmsg("could not register the synchdb database controller"),
                    errhint("Increase max_worker_processes or stop an unused background worker.")));
            while (!synchdb_registry_controller_present(registry,MyDatabaseId))
            {
                pid_t pid;
                BgwHandleStatus status=GetBackgroundWorkerPid(handle,&pid);

                if (status==BGWH_STOPPED || status==BGWH_POSTMASTER_DIED)
                    ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("synchdb database controller exited before claiming its identity")));
                if (GetCurrentTimestamp()>=deadline)
                    ereport(ERROR,(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                        errmsg("timed out starting the synchdb database controller")));
                CHECK_FOR_INTERRUPTS();
                (void) WaitLatch(MyLatch,WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH,10L,PG_WAIT_EXTENSION);
                ResetLatch(MyLatch);
            }
            pfree(handle);
        }
    }
    PG_FINALLY();
    {
        if (locked)
            (void) LockRelease(&tag,ExclusiveLock,false);
    }
    PG_END_TRY();
}

/* A dynamic registration restarts after every nonzero exit. A deleted
 * database is permanent for this OID, so let that registration retire rather
 * than retry InitPostgres forever before reaching our incarnation fence.
 * InitPostgres reports this condition as FATAL, outside PG_CATCH handling. */
static void
retire_missing_database_controller(void *arg)
{
    MemoryContext caller;
    ErrorData *error;
    bool missing;

    if (geterrcode()!=ERRCODE_UNDEFINED_DATABASE)
        return;
    caller=MemoryContextSwitchTo(TopMemoryContext);
    error=CopyErrorData();
    missing=error->elevel==FATAL;
    FreeErrorData(error);
    if (missing)
        proc_exit(0);
    MemoryContextSwitchTo(caller);
}

static void
connect_controller_database(Oid database_oid, bool manual)
{
    ErrorContextCallback callback;

    if (!manual)
    {
        BackgroundWorkerInitializeConnectionByOid(database_oid,InvalidOid,0);
        return;
    }
    callback.callback=retire_missing_database_controller;
    callback.arg=NULL;
    callback.previous=error_context_stack;
    PG_TRY();
    {
        error_context_stack=&callback;
        BackgroundWorkerInitializeConnectionByOid(database_oid,InvalidOid,0);
    }
    PG_FINALLY();
    {
        error_context_stack=callback.previous;
    }
    PG_END_TRY();
}

void
synchdb_control_database_main(Datum arg)
{
    Oid database_oid=DatumGetObjectId(arg);
    LOCKTAG tag;
    MemoryContext loop_context;
    bool manual=!synchdb_registry_is_preloaded();
    ManualControllerStartup startup={0};
    volatile bool extension_seen=false;
    volatile bool creator_finished=false;

    if (manual)
    {
        memcpy(&startup,MyBgworkerEntry->bgw_extra,sizeof(startup));
        if (startup.version!=1 || !TransactionIdIsValid(startup.creator_xid) ||
            startup.registry_incarnation==0)
            ereport(FATAL,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid manual synchdb controller startup identity")));
    }

    /* SPI and relation-lock waits service ProcDiePending, not the simple
     * postmaster-loop shutdown flag. die() also wakes an idle controller. */
    pqsignal(SIGTERM,die);
    pqsignal(SIGHUP,SignalHandlerForConfigReload);
    pqsignal(SIGUSR1,procsignal_sigusr1_handler);
    BackgroundWorkerUnblockSignals();
    connect_controller_database(database_oid,manual);
    SET_LOCKTAG_ADVISORY(tag,database_oid,0x53594e43,0,3);
    if (LockAcquire(&tag,ExclusiveLock,true,true)==LOCKACQUIRE_NOT_AVAIL)
        proc_exit(0);
    if (!manual)
    {
        begin_control_transaction();
        if (!OidIsValid(get_extension_oid("synchdb",true)))
        {
            commit_control_transaction();
            proc_exit(0);
        }
        commit_control_transaction();
    }
    /* Claim before looking for an extension created by the still-uncommitted
     * explicit START transaction. No source runs until its incarnation-bearing
     * control row is visible after commit. */
    controller_registry=manual ? synchdb_registry_try_attach(synchdb_max_connector_workers) :
                                 synchdb_registry_attach(synchdb_max_connector_workers);
    /* Dynamic registrations survive a postmaster PANIC recovery. Their old
     * manual admission does not: only a new explicit START may create the new
     * runtime. Local controller restarts still attach to the original DSM. */
    if (manual && (!controller_registry ||
        startup.registry_incarnation!=synchdb_registry_incarnation(controller_registry)))
        proc_exit(0);
    controller_generation=synchdb_registry_controller_claim(controller_registry,database_oid);
    /* DSM mappings disappear before on_shmem_exit callbacks. */
    before_shmem_exit(controller_exit,(Datum)0);
    loop_context=AllocSetContextCreate(TopMemoryContext,"SynchdbControllerCycle",ALLOCSET_DEFAULT_SIZES);
    while (!ShutdownRequestPending)
    {
        MemoryContext caller;

        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();
        if (ConfigReloadPending)
        {
            ConfigReloadPending=false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        MemoryContextReset(loop_context);
        caller=MemoryContextSwitchTo(loop_context);
        PG_TRY();
        {
            List *rows;
            ListCell *cell;

            begin_control_transaction();
            if (!OidIsValid(get_extension_oid("synchdb",true)))
            {
                commit_control_transaction();
                if (!manual || extension_seen || creator_finished)
                    proc_exit(0);
                /* The creator may keep CREATE EXTENSION + START uncommitted
                 * for any duration. Once it ends, give committed catalog data
                 * one more fresh transaction before treating absence as final. */
                if (!TransactionIdIsInProgress(startup.creator_xid))
                    creator_finished=true;
            }
            else
            {
                extension_seen=true;
                rows=synchdb_control_store_list(loop_context);
                commit_control_transaction();
                foreach(cell,rows)
                {
                    SynchdbControlRow *row=lfirst(cell);

                    CHECK_FOR_INTERRUPTS();
                    if (manual && row->runtime_incarnation!=synchdb_registry_incarnation(controller_registry))
                        continue;
                    reconcile_one(row,loop_context);
                }
                synchdb_artifact_store_process_purges();
            }
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(loop_context);
            AbortCurrentTransaction();
            EmitErrorReport();
            FlushErrorState();
        }
        PG_END_TRY();
        MemoryContextSwitchTo(caller);
        (void) WaitLatch(MyLatch,WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH,250L,PG_WAIT_EXTENSION);
    }
    proc_exit(0);
}

void
synchdb_control_supervisor_main(Datum arg)
{
    pqsignal(SIGTERM,die);
    pqsignal(SIGHUP,SignalHandlerForConfigReload);
    pqsignal(SIGUSR1,procsignal_sigusr1_handler);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(NULL,NULL,0);
    on_shmem_exit(supervisor_exit,(Datum)0);
    while (!ShutdownRequestPending)
    {
        MemoryContext cycle=AllocSetContextCreate(TopMemoryContext,"SynchdbLauncherCycle",ALLOCSET_DEFAULT_SIZES);
        MemoryContext previous=MemoryContextSwitchTo(cycle);

        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();
        PG_TRY();
        {
            List *oids=NIL;
            ListCell *cell;
            Relation relation;
            TableScanDesc scan;
            HeapTuple tuple;

            begin_control_transaction();
            /* SQL parsing needs database-local pg_class; the launcher has no
             * database connection. pg_database's critical shared cache is safe. */
            relation=table_open(DatabaseRelationId,AccessShareLock);
            scan=table_beginscan_catalog(relation,0,NULL);
            while (HeapTupleIsValid(tuple=heap_getnext(scan,ForwardScanDirection)))
            {
                Form_pg_database database=(Form_pg_database)GETSTRUCT(tuple);

                if (database->datallowconn && !database->datistemplate)
                {
                    MemoryContext saved=MemoryContextSwitchTo(cycle);

                    oids=lappend_oid(oids,database->oid);
                    MemoryContextSwitchTo(saved);
                }
            }
            table_endscan(scan);
            table_close(relation,NoLock);
            commit_control_transaction();
            foreach(cell,oids)
            {
                Oid oid=lfirst_oid(cell);
                DatabaseWorker *entry;
                BackgroundWorker worker={0};
                pid_t pid;

                for (entry=database_workers;entry && entry->database_oid!=oid;entry=entry->next)
                    ;
                if (entry && entry->handle && GetBackgroundWorkerPid(entry->handle,&pid)!=BGWH_STOPPED)
                    continue;
                if (!entry)
                {
                    entry=MemoryContextAllocZero(TopMemoryContext,sizeof(*entry));
                    entry->database_oid=oid;
                    entry->next=database_workers;
                    database_workers=entry;
                }
                if (entry->handle)
                    pfree(entry->handle);
                entry->handle=NULL;
                worker.bgw_flags=BGWORKER_SHMEM_ACCESS|BGWORKER_BACKEND_DATABASE_CONNECTION;
                worker.bgw_start_time=BgWorkerStart_ConsistentState;
                worker.bgw_restart_time=BGW_NEVER_RESTART;
                worker.bgw_main_arg=ObjectIdGetDatum(oid);
                worker.bgw_notify_pid=MyProcPid;
                strlcpy(worker.bgw_library_name,SYNCHDB_LIBRARY_NAME,BGW_MAXLEN);
                strlcpy(worker.bgw_function_name,"synchdb_control_database_main",BGW_MAXLEN);
                snprintf(worker.bgw_name,BGW_MAXLEN,"synchdb controller %u",oid);
                strlcpy(worker.bgw_type,"synchdb controller",BGW_MAXLEN);
                MemoryContextSwitchTo(TopMemoryContext);
                if (!RegisterDynamicBackgroundWorker(&worker,&entry->handle))
                    ereport(WARNING,(errmsg("database %u is waiting for a synchdb controller worker slot",oid)));
                MemoryContextSwitchTo(cycle);
            }
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(cycle);
            AbortCurrentTransaction();
            EmitErrorReport();
            FlushErrorState();
        }
        PG_END_TRY();
        MemoryContextSwitchTo(previous);
        MemoryContextDelete(cycle);
        (void) WaitLatch(MyLatch,WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH,1000L,PG_WAIT_EXTENSION);
    }
    proc_exit(0);
}

void
synchdb_control_register_launcher(void)
{
    BackgroundWorker worker={0};

    worker.bgw_flags=BGWORKER_SHMEM_ACCESS|BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time=BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time=1;
    strlcpy(worker.bgw_library_name,SYNCHDB_LIBRARY_NAME,BGW_MAXLEN);
    strlcpy(worker.bgw_function_name,"synchdb_control_supervisor_main",BGW_MAXLEN);
    strlcpy(worker.bgw_name,"synchdb controller launcher",BGW_MAXLEN);
    strlcpy(worker.bgw_type,"synchdb controller launcher",BGW_MAXLEN);
    RegisterBackgroundWorker(&worker);
}
