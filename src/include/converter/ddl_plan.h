/* Validated target DDL and source bindings share the caller's transaction. */
#ifndef SYNCHDB_DDL_PLAN_H
#define SYNCHDB_DDL_PLAN_H

#include "postgres.h"
#include "converter/rule_snapshot.h"
#include "source/debezium_schema.h"

typedef struct SynchdbDdlPlan SynchdbDdlPlan;

typedef struct SynchdbDdlApplyResult
{
	uint64 tables_changed;
	uint64 tables_created;
	uint64 statements;
} SynchdbDdlApplyResult;

/* Build checks every source table before any DDL/catalog mutation. Existing
 * target relations remain locked until caller transaction end. Source default
 * expressions are deliberately target-managed, never translated to NULL.
 * The source image and rules must remain alive until destroy. */
extern SynchdbDdlPlan *synchdb_ddl_plan_build(const char *connector,
		ConnectorType type, const SynchdbRuleSnapshot *rules,
		const SynchdbDbzSchemaChange *source, MemoryContext parent);
extern SynchdbDdlApplyResult synchdb_ddl_plan_apply(SynchdbDdlPlan *plan);
extern void synchdb_ddl_plan_destroy(SynchdbDdlPlan *plan);
/* Shared with FDW's frozen-rule planner; caller owns transaction/snapshot.
 * Resolves target OID/typmod without evaluating a source value/default. */
extern char *synchdb_ddl_render_column_type(const char *source_table,
		const SynchdbDbzSchemaColumn *column, ConnectorType type,
		const SynchdbRuleSnapshot *rules);

#endif /* SYNCHDB_DDL_PLAN_H */
