/* Transactional SQL API: writing a receipt does not execute its operation. */
#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "catalog/catalog_access.h"
#include "catalog/connector_store.h"
#include "catalog/pg_authid_d.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "runtime/controller.h"
#include "runtime/config.h"
#include "storage/latch.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

PG_FUNCTION_INFO_V1(synchdb_control);
PG_FUNCTION_INFO_V1(synchdb_operation);
PG_FUNCTION_INFO_V1(synchdb_runtime_admitted);

/* Passive status projection: a durable RUNNING receipt from a previous manual
 * server runtime is not evidence that its source is currently running. */
Datum
synchdb_runtime_admitted(PG_FUNCTION_ARGS)
{
    SynchdbSharedState *registry;

    if (synchdb_registry_is_preloaded())
        PG_RETURN_BOOL(true);
    registry=synchdb_registry_try_attach(synchdb_max_connector_workers);
    PG_RETURN_BOOL(registry && (uint64) PG_GETARG_INT64(0)==synchdb_registry_incarnation(registry));
}

/* No SQL entry point: only the typed, fixed ledger statements below enter this
 * scope. Configuration/RLS checks and connector writes stay with the invoker.
 * PG_FINALLY also restores the role when the caller catches a subtransaction
 * error, so an unsuccessful enqueue cannot leave elevated session privileges. */
static int
control_ledger_execute_with_args(const char *sql, int nargs, Oid *types,
                                Datum *values, const char *nulls,
                                bool read_only, long count)
{
    Oid caller;
    int security_context;
    int result = 0;

    GetUserIdAndSecContext(&caller,&security_context);
    PG_TRY();
    {
        SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,security_context |
                              SECURITY_LOCAL_USERID_CHANGE | SECURITY_RESTRICTED_OPERATION);
        result = SPI_execute_with_args(sql,nargs,types,values,nulls,read_only,count);
    }
    PG_FINALLY();
    {
        SetUserIdAndSecContext(caller,security_context);
    }
    PG_END_TRY();
    return result;
}

static bool
operation_is(const char *value, const char *expected)
{
    return pg_strcasecmp(value, expected) == 0;
}

static char *
control_loglevel_payload(const char *payload)
{
    static const struct
    {
        const char *name;
        int level;
    } levels[] = {
        {"all",LOG_LEVEL_ALL}, {"debug",LOG_LEVEL_DEBUG},
        {"info",LOG_LEVEL_INFO}, {"warn",LOG_LEVEL_WARN},
        {"error",LOG_LEVEL_ERROR}, {"fatal",LOG_LEVEL_FATAL},
        {"off",LOG_LEVEL_OFF}, {"trace",LOG_LEVEL_TRACE}
    };
    int level=LOG_LEVEL_UNDEF;
    size_t index;

    if (!payload || !payload[0])
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("LOGLEVEL requires a log level payload")));
    for (index=0;index<lengthof(levels);++index)
        if (operation_is(payload,levels[index].name))
        {
            level=levels[index].level;
            break;
        }
    /* Compatibility SQL functions already submit numeric enum values. */
    if (level==LOG_LEVEL_UNDEF)
        level=pg_strtoint32(payload);
    if (level<LOG_LEVEL_ALL || level>LOG_LEVEL_TRACE)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("log level is outside the supported range"),
                        errhint("Valid levels: all, trace, debug, info, warn, error, fatal, off.")));
    return psprintf("%d",level);
}

static void
control_check_offset_payload(const char *payload)
{
    Jsonb *value;

    if (!payload || !payload[0])
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("OFFSET requires a non-empty JSON object payload")));
    value=DatumGetJsonbP(DirectFunctionCall1(jsonb_in,CStringGetDatum(payload)));
    if (!JB_ROOT_IS_OBJECT(value) || JB_ROOT_COUNT(value)==0)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("OFFSET requires a non-empty JSON object payload")));
    /* Preserve the authored bytes. Connector-specific fields and coordinate
     * semantics remain the source adapter's responsibility. */
    pfree(value);
}

int32
synchdb_control_receipt(int64 operation_id)
{
    if (operation_id <= 0 || operation_id > PG_INT32_MAX)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("operation ID exceeds the compatibility API range"),
                        errhint("Use synchdb_control(), which returns bigint operation IDs.")));
    return (int32) operation_id;
}

int64
synchdb_control_enqueue(const char *name, const char *operation,
                        const SynchdbRunSpec *spec, const char *payload)
{
    static const char *operations[] = {"START","STOP","PAUSE","RESUME","RESTART",
                                      "RELOAD","MEMDUMP","LOGLEVEL","OFFSET","RESET_STATS"};
    const char *op = NULL;
    SynchdbConnectorKey key;
    int64 revision;
    char *states;
    char *commands;
    char *connectors;
    char *old_desired = NULL;
    int64 operation_id;
    int i;
    bool isnull;
    bool new_run;
    bool stop_existing_state=false;
    bool preloaded;
    bool admitted=false;
    bool synchronous_stop;
    SynchdbSharedState *registry;
    uint64 runtime_incarnation;
    int path_level;
    Datum values[13];
    Oid types[13] = {UUIDOID,TEXTOID,INT8OID,OIDOID,TEXTOID,TEXTOID,INT4OID,BOOLOID,JSONBOID,BOOLOID,TEXTOID,BOOLOID,INT8OID};
    char nulls[13] = {' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' '};

    for (i = 0; i < lengthof(operations); ++i)
        if (operation_is(operation,operations[i]))
            op = operations[i];
    if (!op || !name || !name[0])
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid connector control operation")));
    new_run = operation_is(op,"START") || operation_is(op,"RESTART");
    preloaded=synchdb_registry_is_preloaded();
    /* Only an explicit new run may initialize a manual registry. Catalog
     * queries, maintenance and a cold STOP never start a dispatcher. */
    if (!synchdb_connector_store_get_identity(name,&key,&revision))
        ereport(ERROR,(errcode(ERRCODE_UNDEFINED_OBJECT),errmsg("connector \"%s\" does not exist",name)));
    if (new_run && (!spec || !synchdb_connector_key_equal(&key,&spec->connection.key) ||
                    revision != spec->connection.config_revision))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("connector configuration changed before control submission")));
    if (payload && strlen(payload) >= SYNCHDB_ERRMSG_SIZE)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("control payload is too long")));
    if (operation_is(op,"LOGLEVEL"))
        payload=control_loglevel_payload(payload);
    else if (operation_is(op,"OFFSET"))
        control_check_offset_payload(payload);
    else if (payload!=NULL)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("control operation %s does not accept a payload",op)));

    registry=new_run ? synchdb_registry_attach(synchdb_max_connector_workers) :
                      synchdb_registry_try_attach(synchdb_max_connector_workers);
    runtime_incarnation=registry ? synchdb_registry_incarnation(registry) : 0;

    synchdb_store_connect();
    path_level=synchdb_store_begin_path();
    connectors = synchdb_store_relation("synchdb_conninfo",RowExclusiveLock);
    states = synchdb_store_control_relation("synchdb_control_state",RowExclusiveLock);
    commands = synchdb_store_control_relation("synchdb_control_commands",RowExclusiveLock);
    values[0]=UUIDPGetDatum(&key.connector_id);
    /* A running FDW bootstrap holds FOR SHARE on its accepted configuration.
     * An existing STOP takes only KEY SHARE on that row, before locking its
     * state, so deletion cannot orphan the receipt or invert the lock order. */
    if (operation_is(op,"STOP"))
    {
        if (control_ledger_execute_with_args(psprintf("SELECT 1 FROM %s WHERE connector_id=$1",states),
            1,types,values,NULL,false,1)!=SPI_OK_SELECT)
            elog(ERROR,"could not inspect existing STOP state");
        stop_existing_state=SPI_processed==1;
    }
    /* A first STOP retains the exclusive connector lock used by START. */
    if (SPI_execute_with_args(psprintf("SELECT config_revision FROM %s WHERE connector_id=$1 %s",connectors,
        stop_existing_state ? "FOR KEY SHARE" : "FOR UPDATE"),
        1,types,values,NULL,false,1) != SPI_OK_SELECT || SPI_processed != 1)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("connector changed during control submission")));
    if (DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull))!=revision)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("connector revision changed while waiting for control submission")));
    if (control_ledger_execute_with_args(psprintf("SELECT desired_state,execution_role,config_revision,snapshot_mode,"
        "snapshot_engine,schema_sync,run_options,oracle_compatible,runtime_incarnation FROM %s WHERE connector_id=$1 FOR UPDATE",states),
        1,types,values,NULL,false,1) != SPI_OK_SELECT)
        elog(ERROR,"could not lock desired control state");
    if (SPI_processed)
    {
        SynchdbRunHandle existing;
        Oid execution_role = DatumGetObjectId(SPI_getbinval(
            SPI_tuptable->vals[0],SPI_tuptable->tupdesc,2,&isnull));

        old_desired = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull));
        admitted=preloaded || (registry && runtime_incarnation==(uint64) DatumGetInt64(
            SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,9,&isnull)));
        /* Maintenance and RESUME retain the accepted execution identity. Only
         * a caller allowed to assume that role may drive its existing worker.
         * STOP remains available to a permitted connector operator for cleanup;
         * START/RESTART capture the caller as the new execution identity. */
        if (!new_run && !operation_is(op,"STOP") &&
            !member_can_set_role(GetUserId(),execution_role))
            ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("cannot control a connector running as another role"),
                errhint("Use the accepted execution role or a role allowed to SET ROLE to it.")));
        if ((operation_is(op,"OFFSET") || operation_is(op,"MEMDUMP") || operation_is(op,"LOGLEVEL")) &&
            DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,5,&isnull))==ENGINE_FDW)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("FDW bootstrap has no Debezium runtime to maintain")));
        if (operation_is(op,"START") && synchdb_registry_find_run(
            registry,&key,&existing))
        {
            HeapTuple tuple=SPI_tuptable->vals[0];
            TupleDesc desc=SPI_tuptable->tupdesc;
            bool same=DatumGetObjectId(SPI_getbinval(tuple,desc,2,&isnull))==GetUserId() &&
                DatumGetInt64(SPI_getbinval(tuple,desc,3,&isnull))==revision &&
                strcmp(TextDatumGetCString(SPI_getbinval(tuple,desc,4,&isnull)),spec->snapshot_mode)==0 &&
                DatumGetInt32(SPI_getbinval(tuple,desc,5,&isnull))==spec->connection.snapengine &&
                DatumGetBool(SPI_getbinval(tuple,desc,6,&isnull))==((spec->connection.flag & CONNFLAG_SCHEMA_SYNC_MODE)!=0) &&
                DatumGetBool(SPI_getbinval(tuple,desc,8,&isnull))==spec->connection.isOraCompat &&
                DatumGetBool(DirectFunctionCall2(jsonb_eq,SPI_getbinval(tuple,desc,7,&isnull),
                    JsonbPGetDatum(synchdb_run_options_to_json(&spec->connection.run_options))));

            if (!same)
                ereport(ERROR,(errcode(ERRCODE_OBJECT_IN_USE),
                    errmsg("START cannot replace an active connector's accepted configuration"),
                    errhint("Use RESTART to apply a different configuration, execution role or mode.")));
        }
    }
    /* KEY SHARE permits non-key configuration updates while state locking
     * waits. Recheck the captured identity/revision after that wait too. */
    if (stop_existing_state)
    {
        if (SPI_execute_with_args(psprintf("SELECT config_revision FROM %s WHERE connector_id=$1",connectors),
            1,types,values,NULL,false,1)!=SPI_OK_SELECT || SPI_processed!=1 ||
            DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull))!=revision)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                           errmsg("connector changed while waiting for STOP state")));
    }
    if (!new_run && !operation_is(op,"STOP") && !old_desired)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("connector has no accepted run configuration")));
    if (!new_run && !preloaded && !admitted && !operation_is(op,"STOP"))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("connector has not been started in this server runtime"),
            errhint("Commit an explicit START or RESTART before controlling this connector.")));
    if (new_run && !preloaded && !admitted)
    {
        /* A full server restart deliberately does not resume a manual run or
         * its unfinished receipt. A newly accepted START supersedes those old
         * commands transactionally, including when this submission rolls back. */
        if (control_ledger_execute_with_args(psprintf("UPDATE %s SET status='CANCELLED',"
            "message='superseded by an explicit start in a new server runtime',finished_at=clock_timestamp() "
            "WHERE connector_id=$1 AND status IN ('PENDING','EXECUTING')",commands),
            1,types,values,NULL,false,0) != SPI_OK_UPDATE)
            elog(ERROR,"could not supersede commands from the previous manual runtime");
    }
    if (control_ledger_execute_with_args(psprintf("SELECT operation_id FROM %s WHERE connector_id=$1 "
        "AND status IN ('PENDING','EXECUTING')",commands),1,types,values,NULL,false,1) != SPI_OK_SELECT)
        elog(ERROR,"could not inspect pending control command");
    if (SPI_processed)
    {
        if (!operation_is(op,"STOP"))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_IN_USE),errmsg("connector already has a pending control command"),
                errhint("Inspect synchdb_control_commands or submit STOP to cancel it.")));
        if (control_ledger_execute_with_args(psprintf("UPDATE %s SET status='CANCELLED',message='superseded by STOP',"
            "finished_at=clock_timestamp() WHERE connector_id=$1 AND status IN ('PENDING','EXECUTING')",commands),
            1,types,values,NULL,false,0) != SPI_OK_UPDATE)
            elog(ERROR,"could not supersede control command");
    }
    values[1]=CStringGetTextDatum(op);
    values[2]=Int64GetDatum(revision);
    values[3]=ObjectIdGetDatum(GetUserId());
    values[4]=CStringGetTextDatum(operation_is(op,"STOP") ? "STOPPED" :
        operation_is(op,"PAUSE") ? "PAUSED" :
        (new_run || operation_is(op,"RESUME")) ? "RUNNING" : old_desired);
    values[5]=CStringGetTextDatum(spec ? spec->snapshot_mode : "initial");
    values[6]=Int32GetDatum(spec ? spec->connection.snapengine : ENGINE_DEBEZIUM);
    values[7]=BoolGetDatum(spec && (spec->connection.flag & CONNFLAG_SCHEMA_SYNC_MODE));
    values[8]=spec ? JsonbPGetDatum(synchdb_run_options_to_json(&spec->connection.run_options)) :
        DirectFunctionCall1(jsonb_in,CStringGetDatum("{}"));
    values[9]=BoolGetDatum(synchdb_error_strategy == STRAT_RETRY_ON_ERROR);
    values[10]=CStringGetTextDatum(payload ? payload : "");
    values[11]=BoolGetDatum(spec && spec->connection.isOraCompat);
    values[12]=Int64GetDatum((int64) runtime_incarnation);
    if (control_ledger_execute_with_args(psprintf("INSERT INTO %s(connector_id,requested_by,operation,config_revision,payload) "
        "VALUES($1,$4,$2,$3,$11) RETURNING operation_id",commands),13,types,values,nulls,false,1) != SPI_OK_INSERT_RETURNING ||
        SPI_processed != 1)
        elog(ERROR,"could not record control command");
    operation_id=DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull));
    if (control_ledger_execute_with_args(psprintf("INSERT INTO %s AS s(connector_id,connector_name,config_revision,"
        "execution_role,desired_state,snapshot_mode,snapshot_engine,schema_sync,run_options,retry_enabled,oracle_compatible,"
        "runtime_incarnation,last_operation) "
        "VALUES($1,%s,$3,$4,$5,$6,$7,$8,$9,$10,$12,CASE WHEN $2 IN ('START','RESTART') THEN $13 ELSE 0 END,"
        INT64_FORMAT ") ON CONFLICT(connector_id) DO UPDATE SET "
        "desired_state=$5,last_operation=" INT64_FORMAT ",retry_attempt=0,retry_at=NULL,last_error=NULL,last_sqlstate=NULL,"
        "config_revision=CASE WHEN $2 IN ('START','RESTART') THEN $3 ELSE s.config_revision END,"
        "execution_role=CASE WHEN $2 IN ('START','RESTART') THEN $4 ELSE s.execution_role END,"
        "snapshot_mode=CASE WHEN $2 IN ('START','RESTART') THEN $6 ELSE s.snapshot_mode END,"
        "snapshot_engine=CASE WHEN $2 IN ('START','RESTART') THEN $7 ELSE s.snapshot_engine END,"
        "schema_sync=CASE WHEN $2 IN ('START','RESTART') THEN $8 ELSE s.schema_sync END,"
        "oracle_compatible=CASE WHEN $2 IN ('START','RESTART') THEN $12 ELSE s.oracle_compatible END,"
        "run_options=CASE WHEN $2 IN ('START','RESTART') THEN $9 ELSE s.run_options END,"
        "retry_enabled=CASE WHEN $2 IN ('START','RESTART') THEN $10 ELSE s.retry_enabled END,"
        "runtime_incarnation=CASE WHEN $2 IN ('START','RESTART') THEN $13 ELSE s.runtime_incarnation END,"
        "actual_state=CASE WHEN $2 IN ('START','RESTART') THEN 'PENDING' ELSE s.actual_state END,"
        "updated_at=clock_timestamp()",states,quote_literal_cstr(name),operation_id,operation_id),
        13,types,values,nulls,false,0) != SPI_OK_INSERT)
        elog(ERROR,"could not persist connector desired state");
    synchronous_stop=!preloaded && !admitted && operation_is(op,"STOP");
    if ((!stop_existing_state || synchronous_stop) &&
        SPI_execute_with_args(psprintf("UPDATE %s SET isactive=($5 <> 'STOPPED') WHERE connector_id=$1",connectors),
        13,types,values,nulls,false,0) != SPI_OK_UPDATE)
        elog(ERROR,"could not persist connector active intent");
    if (synchronous_stop)
    {
        /* No worker can belong to an unadmitted manual incarnation. Complete
         * only this STOP in SQL; creating a controller here would be permission
         * to process unrelated old desired states after a cold server start. */
        if (control_ledger_execute_with_args(psprintf("UPDATE %s SET actual_state='STOPPED',updated_at=clock_timestamp() "
            "WHERE connector_id=$1",states),1,types,values,NULL,false,0)!=SPI_OK_UPDATE || SPI_processed!=1)
            elog(ERROR,"could not complete an inactive manual STOP state");
        if (control_ledger_execute_with_args(psprintf("UPDATE %s SET status='SUCCEEDED',"
            "message='stopped; no worker was admitted in this server runtime',"
            "started_at=clock_timestamp(),finished_at=clock_timestamp() WHERE operation_id=" INT64_FORMAT,
            commands,operation_id),0,NULL,NULL,NULL,false,0)!=SPI_OK_UPDATE || SPI_processed!=1)
            elog(ERROR,"could not complete an inactive manual STOP receipt");
    }
    synchdb_store_end_path(path_level);
    synchdb_store_finish();
    if (new_run && !preloaded)
        synchdb_control_ensure_manual_controller(registry);
    return operation_id;
}

void
synchdb_control_check_delete(const SynchdbConnectorKey *key)
{
    char *states;
    char *commands;
    Oid type=UUIDOID;
    Datum value=UUIDPGetDatum(&key->connector_id);
    int path_level;

    synchdb_store_connect();
    path_level=synchdb_store_begin_path();
    {
        char *connectors=synchdb_store_relation("synchdb_conninfo",RowExclusiveLock);

        if (SPI_execute_with_args(psprintf("SELECT 1 FROM %s WHERE connector_id=$1 FOR UPDATE",connectors),
            1,&type,&value,NULL,false,1)!=SPI_OK_SELECT)
            elog(ERROR,"could not lock connector for maintenance");
    }
    states=synchdb_store_control_relation("synchdb_control_state",RowExclusiveLock);
    commands=synchdb_store_control_relation("synchdb_control_commands",AccessShareLock);
    if (control_ledger_execute_with_args(psprintf("SELECT 1 FROM %s s WHERE connector_id=$1 AND "
        "(desired_state <> 'STOPPED' OR actual_state NOT IN ('STOPPED','FAILED') OR "
        "EXISTS (SELECT 1 FROM %s o WHERE o.operation_id=s.last_operation AND "
        "o.status IN ('PENDING','EXECUTING'))) FOR UPDATE OF s",states,commands),
        1,&type,&value,NULL,false,1) != SPI_OK_SELECT)
        elog(ERROR,"could not inspect connector control intent");
    if (SPI_processed)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_IN_USE),errmsg("connector still has an active control intent"),
                        errhint("Commit STOP and wait for it to finish before deleting the configuration.")));
    synchdb_store_end_path(path_level);
    synchdb_store_finish();
}

Datum
synchdb_control(PG_FUNCTION_ARGS)
{
    Name name;
    char *op;
    char *payload;
    SynchdbRunSpec spec;
    SynchdbRunSpec *accepted=NULL;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("connector and operation are required")));
    name=PG_GETARG_NAME(0);
    op=text_to_cstring(PG_GETARG_TEXT_PP(1));
    payload=PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(2));
    if (operation_is(op,"START") || operation_is(op,"RESTART"))
    {
        synchdb_control_capture_spec(NameStr(*name),payload ? payload : "initial",&spec);
        accepted=&spec;
        payload=NULL;
    }
    PG_RETURN_INT64(synchdb_control_enqueue(NameStr(*name),op,accepted,payload));
}

Datum
synchdb_operation(PG_FUNCTION_ARGS)
{
    int64 operation_id;
    int timeout;
    TimestampTz deadline;
    Jsonb *result=NULL;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("operation ID and timeout are required")));
    operation_id=PG_GETARG_INT64(0);
    timeout=PG_GETARG_INT32(1);
    if (operation_id<=0 || timeout<0 || timeout>60000)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid operation ID or timeout (0..60000 ms)")));
    if (timeout>0 && IsolationUsesXactSnapshot())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("operation waiting requires READ COMMITTED isolation")));
    deadline=TimestampTzPlusMilliseconds(GetCurrentTimestamp(),timeout);
    do
    {
        char *commands;
        Oid type=INT8OID;
        Datum value=Int64GetDatum(operation_id);
        bool isnull;
        bool own_transaction;
        char *status;
        MemoryContext caller=CurrentMemoryContext;
        int path_level;

        synchdb_store_connect();
        path_level=synchdb_store_begin_path();
        commands=synchdb_store_relation("synchdb_control_commands",AccessShareLock);
        if (SPI_execute_with_args(psprintf("SELECT to_jsonb(o),status,o.xmin FROM %s o "
            "WHERE operation_id=$1",commands),1,&type,&value,NULL,false,1) != SPI_OK_SELECT)
            elog(ERROR,"could not read operation result");
        if (!SPI_processed)
            ereport(ERROR,(errcode(ERRCODE_UNDEFINED_OBJECT),errmsg("control operation does not exist")));
        status=SPI_getvalue(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,2);
        /* xmin belongs to the inserting subtransaction. PostgreSQL checks the
         * complete current transaction tree, including released savepoints. */
        own_transaction=TransactionIdIsCurrentTransactionId(DatumGetTransactionId(
            SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,3,&isnull)));
        if (timeout>0 && own_transaction)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                          errmsg("cannot wait for a control operation before its transaction commits")));
        MemoryContextSwitchTo(caller);
        if (result)
            pfree(result);
        result=DatumGetJsonbPCopy(SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull));
        timeout=(strcmp(status,"PENDING")==0 || strcmp(status,"EXECUTING")==0) ? timeout : 0;
        synchdb_store_end_path(path_level);
        synchdb_store_finish();
        if (!timeout || GetCurrentTimestamp()>=deadline)
            break;
        ResetLatch(MyLatch);
        (void) WaitLatch(MyLatch,WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH,10L,PG_WAIT_EXTENSION);
        CHECK_FOR_INTERRUPTS();
    } while (true);
    PG_RETURN_JSONB_P(result);
}
