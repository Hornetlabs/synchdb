/* Target row counts are distinct from the legacy event status (zero = OK). */
#ifndef SYNCHDB_WRITER_RESULT_H
#define SYNCHDB_WRITER_RESULT_H

#include "postgres.h"

typedef struct SynchdbRowApplyResult
{
	uint64 affected_rows;
} SynchdbRowApplyResult;

/* No source delivery currently proves that a zero-row write is safe to ACK. */
static inline SynchdbRowApplyResult
synchdb_row_apply_require_one(uint64 affected_rows)
{
	SynchdbRowApplyResult result;

	if (affected_rows > 1)
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("source row change affected more than one target row")));
	if (affected_rows == 0)
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("source row change did not affect a target row")));
	result.affected_rows = affected_rows;
	return result;
}

#endif /* SYNCHDB_WRITER_RESULT_H */
