/* Apply one bound source event as one target statement. */
#ifndef SYNCHDB_TUPLE_WRITER_H
#define SYNCHDB_TUPLE_WRITER_H

#include "executor/target_change.h"
#include "executor/writer_result.h"

/* Caller owns the transaction and must roll it back on ERROR. */
extern SynchdbRowApplyResult synchdb_tuple_writer_apply(const SynchdbTargetChange *change);

#endif /* SYNCHDB_TUPLE_WRITER_H */
