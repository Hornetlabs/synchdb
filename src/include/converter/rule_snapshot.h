/* Immutable connector overrides; builtin type rules are separate const data. */
#ifndef SYNCHDB_RULE_SNAPSHOT_H
#define SYNCHDB_RULE_SNAPSHOT_H

#include "postgres.h"
#include "catalog/mapping_store.h"
#include "utils/memutils.h"

typedef struct SynchdbRuleSnapshot SynchdbRuleSnapshot;

typedef struct SynchdbRuleTypeMapping
{
	const char *name;
	int length;
} SynchdbRuleTypeMapping;

/* Caller owns an active transaction/snapshot for PostgreSQL parser metadata.
 * Build owns a new child of parent; failure never changes the active snapshot.
 * A run-owned parent lets a candidate survive commit until idle activation. */
extern SynchdbRuleSnapshot *synchdb_rule_snapshot_build(const char *connector,
		ConnectorType type, const ObjectMap *rules, int count, MemoryContext parent);
/* Idle batch boundary only. Generations are backend-local, not migration state. */
extern uint64 synchdb_rule_snapshot_activate(SynchdbRuleSnapshot *candidate);
extern void synchdb_rule_snapshot_destroy(SynchdbRuleSnapshot *snapshot);
extern void synchdb_rule_snapshot_clear(void);
extern const SynchdbRuleSnapshot *synchdb_rule_snapshot_current(void);
extern uint64 synchdb_rule_snapshot_generation(const SynchdbRuleSnapshot *snapshot);
extern const char *synchdb_rule_snapshot_connector(const SynchdbRuleSnapshot *snapshot);
extern ConnectorType synchdb_rule_snapshot_type(const SynchdbRuleSnapshot *snapshot);

/* Returned values are borrowed immutable snapshot/builtin storage. */
extern const char *synchdb_rule_object(const SynchdbRuleSnapshot *snapshot,
		SynchdbMappingKind kind, const char *source);
extern const char *synchdb_rule_transform(const SynchdbRuleSnapshot *snapshot,
		const char *source_column);
extern const SynchdbRuleTypeMapping *synchdb_rule_type(
		const SynchdbRuleSnapshot *snapshot, ConnectorType type,
		const char *source_column, const char *source_type, bool auto_incremented);
extern const SynchdbRuleTypeMapping *synchdb_rule_builtin_type(ConnectorType type,
		const char *source_type, bool auto_incremented);
/* Caller transaction/snapshot required for parse_ident's builtin metadata.
 * Syntax only, preserving bytes before capacity checks. A bare table retains
 * the established public schema default; database is NULL unless supplied. */
extern void synchdb_rule_parse_target_name(const char *mapped, char **database,
		char **schema, char **table);

#endif /* SYNCHDB_RULE_SNAPSHOT_H */
