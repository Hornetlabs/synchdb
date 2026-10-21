/* Catalog-only control ledger operations. Caller owns transaction/snapshot. */
#include "postgres.h"
#include "catalog/control_store.h"
#include "catalog/catalog_access.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/uuid.h"

static Datum
column(HeapTuple tuple, TupleDesc desc, int number)
{
    bool isnull;
    Datum value = SPI_getbinval(tuple, desc, number, &isnull);

    if (isnull)
        elog(ERROR, "unexpected null in persistent control ledger");
    return value;
}

List *
synchdb_control_store_list(MemoryContext result_context)
{
    List *rows = NIL;
    char *states;
    char *commands;
    char *connectors;
    uint64 i;
    MemoryContext previous;
    int path_level;

    synchdb_store_connect();
    path_level=synchdb_store_begin_path();
    states = synchdb_store_control_relation("synchdb_control_state", AccessShareLock);
    commands = synchdb_store_control_relation("synchdb_control_commands", AccessShareLock);
    connectors = synchdb_store_relation("synchdb_conninfo", AccessShareLock);
    if (SPI_execute(psprintf(
        "SELECT s.connector_id,c.name::text,s.config_revision,s.execution_role,s.desired_state,"
        "s.snapshot_mode,s.snapshot_engine,s.schema_sync,s.run_options,s.retry_enabled,"
        "s.retry_attempt,coalesce(s.retry_at,'-infinity'::timestamptz),s.actual_state,"
        "s.last_operation,o.operation,o.status,coalesce(o.payload,''),"
        "coalesce(o.controller_generation,0),coalesce(o.request_id,0),coalesce(o.run_generation,0),s.oracle_compatible,"
        "s.runtime_incarnation "
        "FROM %s s JOIN %s c USING(connector_id) JOIN %s o ON o.operation_id=s.last_operation "
        "WHERE s.desired_state <> 'STOPPED' OR o.status IN ('PENDING','EXECUTING') "
        "ORDER BY s.last_operation", states, connectors, commands), true, 0) != SPI_OK_SELECT)
        elog(ERROR, "could not read control ledger");
    previous = MemoryContextSwitchTo(result_context);
    for (i = 0; i < SPI_processed; ++i)
    {
        HeapTuple tuple = SPI_tuptable->vals[i];
        TupleDesc desc = SPI_tuptable->tupdesc;
        SynchdbControlRow *row = palloc0(sizeof(*row));

        row->key.database_oid = MyDatabaseId;
        memcpy(&row->key.connector_id, DatumGetUUIDP(column(tuple,desc,1)), sizeof(pg_uuid_t));
        row->name = TextDatumGetCString(column(tuple,desc,2));
        row->revision = DatumGetInt64(column(tuple,desc,3));
        row->role = DatumGetObjectId(column(tuple,desc,4));
        row->desired = TextDatumGetCString(column(tuple,desc,5));
        row->mode = TextDatumGetCString(column(tuple,desc,6));
        row->snapshot_engine = DatumGetInt32(column(tuple,desc,7));
        row->schema_sync = DatumGetBool(column(tuple,desc,8));
        row->options = DatumGetJsonbPCopy(column(tuple,desc,9));
        row->retry_enabled = DatumGetBool(column(tuple,desc,10));
        row->retry_attempt = DatumGetInt32(column(tuple,desc,11));
        row->retry_at = DatumGetTimestampTz(column(tuple,desc,12));
        row->actual = TextDatumGetCString(column(tuple,desc,13));
        row->operation_id = DatumGetInt64(column(tuple,desc,14));
        row->operation = TextDatumGetCString(column(tuple,desc,15));
        row->status = TextDatumGetCString(column(tuple,desc,16));
        row->payload = TextDatumGetCString(column(tuple,desc,17));
        row->controller_generation = DatumGetInt64(column(tuple,desc,18));
        row->request_id = DatumGetInt64(column(tuple,desc,19));
        row->run_generation = DatumGetInt64(column(tuple,desc,20));
        row->oracle_compatible = DatumGetBool(column(tuple,desc,21));
        row->runtime_incarnation = (uint64) DatumGetInt64(column(tuple,desc,22));
        rows = lappend(rows, row);
    }
    MemoryContextSwitchTo(previous);
    synchdb_store_end_path(path_level);
    synchdb_store_finish();
    return rows;
}

bool
synchdb_control_store_claim(const SynchdbControlRow *row, uint64 generation)
{
    char *commands;
    Oid types[3] = {INT8OID,INT8OID,INT8OID};
    Datum values[3] = {Int64GetDatum(row->operation_id),Int64GetDatum((int64)generation),
                       Int64GetDatum((int64)row->controller_generation)};
    bool claimed;
    int path_level;

    synchdb_store_connect();
    path_level=synchdb_store_begin_path();
    commands = synchdb_store_control_relation("synchdb_control_commands", RowExclusiveLock);
    if (SPI_execute_with_args(psprintf("UPDATE %s SET status='EXECUTING',controller_generation=$2,"
        "started_at=clock_timestamp(),request_id=NULL WHERE operation_id=$1 AND "
        "(status='PENDING' OR (status='EXECUTING' AND controller_generation=$3))",commands),
        3,types,values,NULL,false,0) != SPI_OK_UPDATE)
        elog(ERROR,"could not claim control command");
    claimed = SPI_processed == 1;
    synchdb_store_end_path(path_level);
    synchdb_store_finish();
    return claimed;
}

void
synchdb_control_store_request(const SynchdbControlRow *row, uint64 generation,
                             uint64 request_id, uint64 run_generation,
                             bool require_receipt)
{
    Oid types[8] = {INT8OID,INT8OID,INT8OID,INT8OID,UUIDOID,INT8OID,TEXTOID,TEXTOID};
    Datum values[8] = {Int64GetDatum(row->operation_id),Int64GetDatum((int64)generation),
                      Int64GetDatum((int64)request_id),Int64GetDatum((int64)run_generation),
                      UUIDPGetDatum(&row->key.connector_id),Int64GetDatum(row->revision),
                      CStringGetTextDatum(row->desired),CStringGetTextDatum(row->status)};
    char *states;
    char *commands;
    int path_level;

    synchdb_store_connect();
    path_level=synchdb_store_begin_path();
    states = synchdb_store_control_relation("synchdb_control_state", RowExclusiveLock);
    commands = synchdb_store_control_relation("synchdb_control_commands", RowExclusiveLock);
    /* An accepted command can be superseded by STOP after the controller read
     * its cycle snapshot. Keep this row lock until the receipt transaction
     * commits, so a zero-row update cannot release an unrecorded native start. */
    if (SPI_execute_with_args(psprintf("SELECT 1 FROM %s WHERE connector_id=$5 "
        "AND last_operation=$1 AND config_revision=$6 AND desired_state=$7 FOR UPDATE",states),
        8,types,values,NULL,false,1) != SPI_OK_SELECT)
        elog(ERROR,"could not validate runtime request intent");
    if (SPI_processed != 1)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("accepted control intent changed before runtime request publication")));
    if (require_receipt)
    {
        if (SPI_execute_with_args(psprintf("UPDATE %s SET request_id=$3,run_generation=$4 "
            "WHERE operation_id=$1 AND connector_id=$5 AND controller_generation=$2 "
            "AND status='EXECUTING'",commands),8,types,values,NULL,false,0) != SPI_OK_UPDATE)
            elog(ERROR,"could not record runtime request identity");
        if (SPI_processed != 1)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                           errmsg("executing control receipt changed before runtime request publication")));
    }
    else
    {
        /* Controller recovery may restore RUNNING after the last operation
         * finished. Its old receipt records that operation, not this process. */
        if (SPI_execute_with_args(psprintf("SELECT 1 FROM %s WHERE operation_id=$1 "
            "AND connector_id=$5 AND status=$8 AND status IN ('SUCCEEDED','FAILED')",commands),
            8,types,values,NULL,true,1) != SPI_OK_SELECT)
            elog(ERROR,"could not validate terminal control receipt");
        if (SPI_processed != 1)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                           errmsg("terminal control receipt changed before desired-state recovery")));
    }
    synchdb_store_end_path(path_level);
    synchdb_store_finish();
}

void
synchdb_control_store_update(const SynchdbControlRow *row, const char *actual,
                            const char *desired, const char *status, int sqlstate,
                            const char *message, int retry_attempt, TimestampTz retry_at)
{
    Oid types[9] = {INT8OID,TEXTOID,TEXTOID,TEXTOID,TEXTOID,TEXTOID,INT4OID,TIMESTAMPTZOID,UUIDOID};
    Datum values[9] = {Int64GetDatum(row->operation_id),CStringGetTextDatum(actual),
        CStringGetTextDatum(desired),CStringGetTextDatum(status ? status : row->status),
        CStringGetTextDatum(sqlstate ? unpack_sql_state(sqlstate) : "00000"),
        CStringGetTextDatum(message ? message : ""),Int32GetDatum(retry_attempt),
        TimestampTzGetDatum(retry_at),UUIDPGetDatum(&row->key.connector_id)};
    char *states;
    char *commands;
    char *connectors = NULL;
    bool stopped = strcmp(actual,"STOPPED") == 0 && strcmp(desired,"STOPPED") == 0;
    int path_level;

    synchdb_store_connect();
    path_level=synchdb_store_begin_path();
    /* A bootstrap may retain a share lock on its accepted connector row.
     * Runtime progress must remain publishable while cancellation stops that
     * worker. Only a confirmed stop retires the compatibility active flag.
     * Lock that row first, matching START's connector/state/command order. */
    if (stopped)
    {
        connectors = synchdb_store_relation("synchdb_conninfo",RowExclusiveLock);
        if (SPI_execute_with_args(psprintf("SELECT 1 FROM %s WHERE connector_id=$9 FOR UPDATE",connectors),
            9,types,values,NULL,false,1) != SPI_OK_SELECT)
            elog(ERROR,"could not lock stopped connector intent");
    }
    states = synchdb_store_control_relation("synchdb_control_state",RowExclusiveLock);
    commands = synchdb_store_control_relation("synchdb_control_commands",RowExclusiveLock);
    if (SPI_execute_with_args(psprintf("UPDATE %s SET actual_state=$2,desired_state=$3,"
        "last_sqlstate=nullif($5,'00000'),last_error=nullif($6,''),retry_attempt=$7,"
        "retry_at=CASE WHEN $8='-infinity'::timestamptz THEN NULL ELSE $8 END,"
        "updated_at=clock_timestamp() WHERE connector_id=$9 AND last_operation=$1",states),
        9,types,values,NULL,false,0) != SPI_OK_UPDATE)
        elog(ERROR,"could not update control state");
    if (SPI_processed == 1)
    {
        if (SPI_execute_with_args(psprintf("UPDATE %s SET status=$4,sqlstate=nullif($5,'00000'),"
            "message=nullif($6,''),finished_at=CASE WHEN $4 IN ('SUCCEEDED','FAILED','CANCELLED') "
            "THEN clock_timestamp() ELSE NULL END WHERE operation_id=$1 "
            "AND status IN ('PENDING','EXECUTING')",commands),9,types,values,NULL,false,0) != SPI_OK_UPDATE)
            elog(ERROR,"could not finish control command");
        if (stopped && SPI_execute_with_args(psprintf("UPDATE %s SET isactive=false "
            "WHERE connector_id=$9 AND isactive IS DISTINCT FROM false",connectors),
            9,types,values,NULL,false,0) != SPI_OK_UPDATE)
            elog(ERROR,"could not publish control intent");
    }
    synchdb_store_end_path(path_level);
    synchdb_store_finish();
}
