/* Results describe target work; only Batch Apply may commit or acknowledge. */
#ifndef SYNCHDB_EVENT_RESULT_H
#define SYNCHDB_EVENT_RESULT_H

typedef enum SynchdbEventOutcome
{
	SYNCHDB_EVENT_FAILED = -1,
	SYNCHDB_EVENT_UNSUPPORTED = -2,
	SYNCHDB_EVENT_APPLIED = 0,
	SYNCHDB_EVENT_CONTROL,
	SYNCHDB_EVENT_NOOP,
	/* Reserved for an explicit policy with event subtransaction recovery. */
	SYNCHDB_EVENT_SKIPPED
} SynchdbEventOutcome;

#endif
