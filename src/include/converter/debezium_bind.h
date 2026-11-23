/* Bind decoded source tables and rows to current PostgreSQL target metadata. */
#ifndef SYNCHDB_DEBEZIUM_BIND_H
#define SYNCHDB_DEBEZIUM_BIND_H

#include "executor/target_change.h"
#include "source/debezium_row.h"
#include "synchdb/synchdb.h"
#include "utils/relcache.h"

/* Exact source identity and the same table rules for rows and TRUNCATE.
 * The caller closes the relation with NoLock to retain the transaction lock. */
extern Relation synchdb_bind_source_table(const char *database,
		const char *source_schema, const char *source_table, LOCKMODE lockmode);

/* The returned target change and its Datums belong to CurrentMemoryContext. */
extern SynchdbTargetChange *synchdb_bind_dbz_row(const SynchdbDbzRow *row,
												ConnectorType type);

#endif /* SYNCHDB_DEBEZIUM_BIND_H */
