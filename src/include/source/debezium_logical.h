/* Validation of known Debezium logical schemas and their source values. */
#ifndef SYNCHDB_DEBEZIUM_LOGICAL_H
#define SYNCHDB_DEBEZIUM_LOGICAL_H

#include "source/debezium_row.h"

extern void synchdb_dbz_validate_logical_schema(const SynchdbDbzField *field,
												 JsonbContainer *schema);
extern void synchdb_dbz_validate_logical_value(const SynchdbDbzField *field);

#endif /* SYNCHDB_DEBEZIUM_LOGICAL_H */
