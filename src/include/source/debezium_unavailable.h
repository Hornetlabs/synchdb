/* Recognize only the configured source placeholder representations. */
#ifndef SYNCHDB_DEBEZIUM_UNAVAILABLE_H
#define SYNCHDB_DEBEZIUM_UNAVAILABLE_H

#include "source/debezium_row.h"

extern bool synchdb_dbz_value_unavailable(const SynchdbDbzField *field,
										const char *connector, char operation);

#endif
