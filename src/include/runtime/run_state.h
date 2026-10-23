/* Run identity values; independent of shared-memory layout and connection types. */
#ifndef SYNCHDB_RUN_STATE_H
#define SYNCHDB_RUN_STATE_H

#include "postgres.h"
#include "runtime/connector_identity.h"

typedef enum SynchdbRunLifecycle
{
	SYNCHDB_RUN_FREE = 0,
	SYNCHDB_RUN_RESERVED,
	SYNCHDB_RUN_RUNNING,
	SYNCHDB_RUN_RETRY_WAIT,
	SYNCHDB_RUN_STOPPING
} SynchdbRunLifecycle;

typedef struct SynchdbRunHandle
{
	uint64 incarnation;
	uint64 generation;
	int slot;
} SynchdbRunHandle;

typedef struct SynchdbWorkerOwner
{
	SynchdbRunHandle run;
	uint64 attempt;
	pid_t pid;
} SynchdbWorkerOwner;

#endif /* SYNCHDB_RUN_STATE_H */
