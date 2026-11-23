#include "postgres.h"

#include "access/xact.h"
#include "access/table.h"
#include "commands/tablecmds.h"
#include "executor/target_validate.h"
#include "executor/truncate.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

void
synchdb_execute_truncate(Relation relation)
{
	TruncateStmt *statement = makeNode(TruncateStmt);
	RangeVar *target;

	if (!IsTransactionState() || !ActiveSnapshotSet())
		elog(ERROR, "truncate apply requires a transaction and active snapshot");
	synchdb_target_validate_relation(relation);
	target = makeRangeVar(get_namespace_name(RelationGetNamespace(relation)),
						  pstrdup(RelationGetRelationName(relation)), -1);
	/* Source events name captured tables individually. Never widen that set to
	 * local inheritance children or FK dependants, or reset unrelated sequences.
	 * The PostgreSQL command checks TRUNCATE privileges, FKs and fires triggers. */
	target->inh = false;
	statement->relations = list_make1(target);
	statement->restart_seqs = false;
	statement->behavior = DROP_RESTRICT;
	/* ExecuteTruncate opens the table itself and rejects extra active relation
	 * references. Keep the lock, but release our binding reference first. */
	table_close(relation, NoLock);
	ExecuteTruncate(statement);
	CommandCounterIncrement();
}
