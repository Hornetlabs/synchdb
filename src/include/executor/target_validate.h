#ifndef SYNCHDB_TARGET_VALIDATE_H
#define SYNCHDB_TARGET_VALIDATE_H

#include "executor/target_change.h"
#include "utils/relcache.h"

/* Both checks use the caller's live relation and retain no cached metadata. */
extern void synchdb_target_validate_relation(Relation relation);
/* Check mapped column metadata and privileges before evaluating values. */
extern void synchdb_target_validate_binding(Relation relation,
										  const SynchdbTargetChange *change);
extern void synchdb_target_validate_change(Relation relation,
										 const SynchdbTargetChange *change);

#endif /* SYNCHDB_TARGET_VALIDATE_H */
