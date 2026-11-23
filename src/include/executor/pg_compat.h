/* PostgreSQL version adapters and allocation-free metadata invalidation. */
#ifndef SYNCHDB_PG_COMPAT_H
#define SYNCHDB_PG_COMPAT_H

#include "postgres.h"
#include "executor/executor.h"
#include "utils/relcache.h"

extern int synchdb_pg_base64_decode(const char *source, int length,
									  unsigned char *destination, int capacity);
extern Oid synchdb_pg_primary_key_index(Relation relation, bool allow_deferrable);
extern void synchdb_pg_init_range_table(EState *estate, List *range_table,
									   List *permissions);
extern void synchdb_pg_close_trigger_relations(EState *estate);
extern Expr *synchdb_pg_null_default(Oid type, int32 typmod, Oid collation,
									int length, bool by_value);
extern bool synchdb_pg_before_update(EState *estate, EPQState *epq,
									ResultRelInfo *result, ItemPointer tuple_id,
									TupleTableSlot *new_slot);
extern bool synchdb_pg_before_delete(EState *estate, EPQState *epq,
									ResultRelInfo *result, ItemPointer tuple_id);
/* PG16 retains the server's LOGIN requirement for the execution role. */
extern void synchdb_pg_worker_initialize_connection(Oid database_oid, Oid role_oid);
/* Call in the controller's transaction before registering a worker. */
extern void synchdb_pg_worker_check_connection(Oid database_oid, Oid role_oid);

/* Register once in a normal backend entry point, never from a callback. */
extern void synchdb_pg_metadata_init(void);
/* Callbacks only advance this epoch: no SPI, allocation, cache reset or ERROR. */
extern uint64 synchdb_pg_metadata_epoch(void);
extern bool synchdb_pg_metadata_stale(uint64 observed_epoch);
extern void synchdb_pg_metadata_mark_dirty(void);

#endif /* SYNCHDB_PG_COMPAT_H */
