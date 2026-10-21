/* Bind decoded source rows to current PostgreSQL target metadata. */
#ifndef SYNCHDB_DEBEZIUM_BIND_H
#define SYNCHDB_DEBEZIUM_BIND_H

#include "executor/target_change.h"
#include "source/debezium_row.h"
#include "synchdb/synchdb.h"

/* The returned target change and its Datums belong to CurrentMemoryContext. */
extern SynchdbTargetChange *synchdb_bind_dbz_row(const SynchdbDbzRow *row,
												ConnectorType type);

#endif /* SYNCHDB_DEBEZIUM_BIND_H */
