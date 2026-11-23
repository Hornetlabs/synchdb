/* Apply one validated source delivery in one owned target transaction. */
#include "postgres.h"

#include "access/xact.h"
#include "executor/batch_apply.h"
#include "utils/snapmgr.h"

void
synchdb_batch_apply(const SynchdbDbzFrame *frame,
					SynchdbBatchApplyProgress *progress,
					SynchdbBatchRecordApply apply_record, void *argument)
{
	SynchdbDbzRecordCursor cursor;

	if (frame == NULL || progress == NULL || apply_record == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("batch apply requires a frame, progress and record callback")));
	if (IsTransactionOrTransactionBlock() || ActiveSnapshotSet())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("batch apply requires an idle backend without an active snapshot")));
	if (frame->kind != SYNCHDB_DBZ_BATCH || progress->phase != SYNCHDB_APPLY_NEW)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("batch apply requires a new progress record and a batch frame")));

	/* The frame and progress are caller-owned and must survive transaction end. */
	synchdb_dbz_record_cursor(frame, &cursor);
	progress->batch_id = frame->batch_id;
	progress->record_count = frame->record_count;
	progress->records_applied = 0;
	progress->phase = SYNCHDB_APPLY_RUNNING;
	PG_TRY();
	{
		const char *json;
		Size length;

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		while (synchdb_dbz_record_next(&cursor, &json, &length))
		{
			int32 index = cursor.index - 1;

			apply_record(json, index, index == 0,
						 index == frame->record_count - 1, argument);
			progress->records_applied++;
		}
		PopActiveSnapshot();
		/* Source acknowledgement can outlive a target crash. Do not inherit an
		 * asynchronous commit setting and confirm rows whose WAL is still only
		 * buffered locally. This forces local WAL flush, not synchronous standby
		 * acknowledgement or a durability claim with fsync disabled. */
		ForceSyncCommit();
		progress->phase = SYNCHDB_APPLY_COMMITTING;
		CommitTransactionCommand();
		progress->phase = SYNCHDB_APPLY_COMMITTED;
	}
	PG_CATCH();
	{
		progress->phase = progress->phase == SYNCHDB_APPLY_COMMITTING ?
			SYNCHDB_APPLY_COMMIT_UNKNOWN : SYNCHDB_APPLY_ABORTED;
		/* The enclosing backend error boundary owns transaction abort. */
		PG_RE_THROW();
	}
	PG_END_TRY();
}
