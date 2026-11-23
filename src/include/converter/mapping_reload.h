/* Apply enabled mapping overrides to already bound physical target objects. */
#ifndef SYNCHDB_MAPPING_RELOAD_H
#define SYNCHDB_MAPPING_RELOAD_H

#include "converter/rule_snapshot.h"

/* Caller owns the transaction. An error rolls back the entire reload; publish
 * the candidate rules only after committing these DDL changes. */
extern void synchdb_mapping_reload_targets(const char *connector,
		ConnectorType type, const ObjectMap *rules, int count);

#endif
