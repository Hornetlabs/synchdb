/* Shared SPI and extension-owned relation access for SynchDB stores. */
#ifndef SYNCHDB_CATALOG_ACCESS_H
#define SYNCHDB_CATALOG_ACCESS_H

#include "storage/lockdefs.h"

/* Caller owns an active transaction and snapshot; errors propagate unchanged. */
extern void synchdb_store_connect(void);
extern void synchdb_store_finish(void);
/*
 * Resolve an extension member by its fixed relation name, lock it until the
 * caller's transaction ends, and return a quoted schema-qualified SQL name.
 * The returned string belongs to the current memory context (normally SPI).
 * No relation OID/schema cache or permission elevation is used.
 */
extern char *synchdb_store_relation(const char *relation_name, LOCKMODE lockmode);
/* Trusted control ledgers additionally reject custom execution hooks, RLS,
 * inheritance and mutable ownership before either the API or controller SPI. */
extern char *synchdb_store_control_relation(const char *relation_name, LOCKMODE lockmode);
/* Scoped trusted namespace for internal SQL. Restore with end_path on success;
 * an ERROR is restored by the caller's transaction/subtransaction abort. */
extern int synchdb_store_begin_path(void);
extern void synchdb_store_end_path(int level);

#endif /* SYNCHDB_CATALOG_ACCESS_H */
