/* Source protocol policy is distinct from an observed snapshot/CDC handoff. */
#ifndef SYNCHDB_SOURCE_CONSISTENCY_H
#define SYNCHDB_SOURCE_CONSISTENCY_H

#include "utils/jsonb.h"

struct SynchdbRunSpec;

/* Pure configuration validation, before registering a worker or opening files.
 * This does not validate remote privileges/log retention or manufacture proof. */
extern void synchdb_source_consistency_validate_run(const struct SynchdbRunSpec *spec);

/* A versioned declaration of implementation, prerequisites and limits. The
 * returned document never advertises cdc_eligible or a completed handoff. */
extern Jsonb *synchdb_source_consistency_describe(const struct SynchdbRunSpec *spec,
                                                MemoryContext context);

#endif
