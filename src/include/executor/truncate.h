/* Table-wide target work belongs to the caller's batch transaction. */
#ifndef SYNCHDB_TRUNCATE_H
#define SYNCHDB_TRUNCATE_H

#include "utils/relcache.h"

/* Consumes the relation reference, retaining its lock until transaction end. */
extern void synchdb_execute_truncate(Relation relation);

#endif /* SYNCHDB_TRUNCATE_H */
