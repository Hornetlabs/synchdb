/* Local FDW bootstrap and the legacy, unverified CDC continuation. */
#ifndef SYNCHDB_BOOTSTRAP_H
#define SYNCHDB_BOOTSTRAP_H

#include "runtime/run_spec.h"
#include "utils/jsonb.h"

typedef enum SynchdbBootstrapStatus
{
	SYNCHDB_BOOTSTRAP_COMPLETE,
	SYNCHDB_BOOTSTRAP_PARTIAL,
	SYNCHDB_BOOTSTRAP_FAILED
} SynchdbBootstrapStatus;

typedef struct SynchdbBootstrapResult
{
	SynchdbBootstrapStatus status;
	bool stop_after;
	bool continue_cdc;
	bool cdc_eligible;
	Jsonb *resume_material;
	uint64 completed_tables;
	uint64 failed_tables;
	Jsonb *detail;
} SynchdbBootstrapResult;

extern void synchdb_bootstrap_require_mode(const char *mode, bool schema_only);
/* Active transaction/snapshot belong to the caller; result belongs to context. */
extern SynchdbBootstrapResult *synchdb_bootstrap_execute(
	const SynchdbRunSpec *spec, MemoryContext context);
extern SynchdbBootstrapResult *synchdb_bootstrap_parse_result(
	Jsonb *json, MemoryContext context);

#endif
