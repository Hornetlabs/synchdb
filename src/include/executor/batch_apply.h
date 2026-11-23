/* The target transaction coordinator; source settlement remains separate. */
#ifndef SYNCHDB_BATCH_APPLY_H
#define SYNCHDB_BATCH_APPLY_H

#include "source/debezium_frame.h"

typedef enum SynchdbBatchApplyPhase
{
	SYNCHDB_APPLY_NEW = 0,
	SYNCHDB_APPLY_RUNNING,
	SYNCHDB_APPLY_COMMITTING,
	SYNCHDB_APPLY_COMMITTED,
	SYNCHDB_APPLY_ABORTED,
	SYNCHDB_APPLY_COMMIT_UNKNOWN
} SynchdbBatchApplyPhase;

typedef struct SynchdbBatchApplyProgress
{
	SynchdbBatchApplyPhase phase;
	int32 batch_id;
	int32 record_count;
	int32 records_applied;
} SynchdbBatchApplyProgress;

typedef void (*SynchdbBatchRecordApply) (const char *json, int32 index,
										 bool first, bool last, void *argument);

/* Caller owns progress outside transaction contexts and ERROR-unwound frames.
 * Requires an idle backend: this function owns the transaction and snapshot.
 * ERROR is rethrown for the backend to abort; never settles source records. */
extern void synchdb_batch_apply(const SynchdbDbzFrame *frame,
							   SynchdbBatchApplyProgress *progress,
							   SynchdbBatchRecordApply apply_record, void *argument);

#endif /* SYNCHDB_BATCH_APPLY_H */
