#ifndef SYNCHDB_CONTROL_STORE_H
#define SYNCHDB_CONTROL_STORE_H

#include "runtime/controller.h"
#include "nodes/pg_list.h"
#include "utils/timestamp.h"

typedef struct SynchdbControlRow
{
    SynchdbConnectorKey key;
    char *name;
    int64 revision;
    Oid role;
    uint64 runtime_incarnation;
    char *desired;
    char *mode;
    int snapshot_engine;
    bool schema_sync;
    bool oracle_compatible;
    Jsonb *options;
    bool retry_enabled;
    int retry_attempt;
    TimestampTz retry_at;
    char *actual;
    int64 operation_id;
    char *operation;
    char *status;
    char *payload;
    uint64 controller_generation;
    uint64 request_id;
    uint64 run_generation;
} SynchdbControlRow;

extern List *synchdb_control_store_list(MemoryContext result_context);
extern bool synchdb_control_store_claim(const SynchdbControlRow *row, uint64 generation);
/* Lock and revalidate the accepted desired-state row until the caller commits.
 * A command receipt must match one EXECUTING operation. Restoring terminal
 * desired state validates its immutable receipt without rewriting history. */
extern void synchdb_control_store_request(const SynchdbControlRow *row, uint64 generation,
                                         uint64 request_id, uint64 run_generation,
                                         bool require_receipt);
extern void synchdb_control_store_update(const SynchdbControlRow *row, const char *actual,
                                        const char *desired, const char *status, int sqlstate,
                                        const char *message, int retry_attempt, TimestampTz retry_at);
#endif
