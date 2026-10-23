/*
 * Tuple Writer: one target statement and trigger scope per source event.
 *
 * Use the table/index APIs directly: ExecSimpleRelation* has no statement
 * triggers and queues AFTER ROW events without transition capture.
 */
#include "postgres.h"
#include "executor/pg_compat.h"

#include "executor/target_validate.h"

#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/row_identity.h"
#include "executor/tuple_writer.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "parser/parse_relation.h"
#include "replication/logicalrelation.h"
#include "rewrite/rewriteHandler.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

typedef struct TupleStatement
{
	Relation relation;
	EState *estate;
	ResultRelInfo *result;
	RTEPermissionInfo *permissions;
	TupleTableSlot *newslot;
	TupleTableSlot *searchslot;
	TupleTableSlot *oldslot;
	TransitionCaptureState *transition;
	EPQState epq;
	bool epq_initialized;
	uint64 affected_rows;
} TupleStatement;

/*
 * Use the same selected identity as binding and SPI. PostgreSQL's index lookup
 * compares the entire row when passed an index other than its chosen identity,
 * so a sparse search slot must always use the resolver's selected index.
 */
static Oid
tuple_identity_index(Relation relation, const SynchdbTargetChange *change)
{
	SynchdbRowIdentity *identity = synchdb_resolve_row_identity(relation);
	Bitmapset *provided = NULL;
	ListCell *cell;

	if (change->identity_index_oid != identity->index_oid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("target row identity changed after binding")));
	foreach(cell, change->columnValuesBefore)
	{
		const SynchdbTargetColumn *column = lfirst(cell);
		int member = column->position - FirstLowInvalidHeapAttributeNumber;

		if (column->is_null || !bms_is_member(member, identity->attributes))
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("target identity must contain only nonnull key fields")));
		provided = bms_add_member(provided, member);
	}
	if (bms_is_empty(identity->attributes) ||
		!bms_equal(identity->attributes, provided))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("source row does not provide the complete target identity")));
	bms_free(provided);
	return identity->index_oid;
}

/*
 * Apply only provided fields. SQL NULL is a separate bit, independent of the
 * Datum bits; non-null Datums already passed target input and typmod checks.
 * The returned bitmap also distinguishes an explicit NULL from an omitted
 * column when inserting defaults.
 */
static Bitmapset *
store_tuple_columns(TupleTableSlot *slot, List *columns)
{
	TupleDesc descriptor = slot->tts_tupleDescriptor;
	Bitmapset *provided = NULL;
	ListCell *cell;

	foreach(cell, columns)
	{
		SynchdbTargetColumn *column = lfirst(cell);
		Form_pg_attribute attribute;
		int index;

		if (column == NULL || column->position <= 0 ||
			column->position > descriptor->natts)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid bound target column position")));
		index = column->position - 1;
		attribute = TupleDescAttr(descriptor, index);
		if (bms_is_member(index, provided))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_COLUMN),
					 errmsg("target column \"%s\" is provided more than once",
							NameStr(attribute->attname))));
		if (attribute->attisdropped || attribute->attgenerated ||
			attribute->attidentity == ATTRIBUTE_IDENTITY_ALWAYS)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot provide a dropped, generated or identity-always target column")));
		if (!OidIsValid(column->datatype) ||
			column->datatype != attribute->atttypid ||
			column->typmod != attribute->atttypmod ||
			column->typbyval != attribute->attbyval ||
			column->name == NULL ||
			strcmp(column->name, NameStr(attribute->attname)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("bound type for target column \"%s\" does not match its current type",
							NameStr(attribute->attname))));
		if (!column->is_null && !column->typbyval &&
			DatumGetPointer(column->value) == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("non-null target value has no Datum storage")));

		provided = bms_add_member(provided, index);
		slot->tts_isnull[index] = column->is_null;
		slot->tts_values[index] = column->is_null ? (Datum) 0 : column->value;
	}
	return provided;
}

/* Follow logical replication's slot_fill_defaults for omitted target fields. */
static void
fill_tuple_defaults(Relation relation, EState *estate, TupleTableSlot *slot,
					const Bitmapset *provided)
{
	TupleDesc descriptor = slot->tts_tupleDescriptor;
	MemoryContext old_context;
	ExprContext *expression_context;
	ExprState **expressions;
	int *positions;
	int count = 0;
	int index;

	old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	expression_context = GetPerTupleExprContext(estate);
	expressions = palloc(descriptor->natts * sizeof(ExprState *));
	positions = palloc(descriptor->natts * sizeof(int));
	for (index = 0; index < descriptor->natts; index++)
	{
		Form_pg_attribute attribute = TupleDescAttr(descriptor, index);
		Expr *expression;

		if (attribute->attisdropped || attribute->attgenerated ||
			bms_is_member(index, provided))
			continue;
		expression = (Expr *) build_column_default(relation, index + 1);
		if (expression == NULL)
			/* Match the ordinary INSERT planner: an implicit NULL must still
			 * pass any domain constraints before reaching row triggers. */
			expression = synchdb_pg_null_default(attribute->atttypid,
														   attribute->atttypmod,
														   attribute->attcollation,
														   attribute->attlen,
														   attribute->attbyval);
		expression = expression_planner(expression);
		expressions[count] = ExecInitExpr(expression, NULL);
		positions[count++] = index;
	}
	for (index = 0; index < count; index++)
		slot->tts_values[positions[index]] =
			ExecEvalExpr(expressions[index], expression_context,
						 &slot->tts_isnull[positions[index]]);
	MemoryContextSwitchTo(old_context);
}


/* Preserve the statement CID even when BEFORE triggers execute nested SQL. */
static bool
check_tuple_write_result(TM_Result status, const TM_FailureData *failure,
						 CommandId command_id)
{
	switch (status)
	{
		case TM_Ok:
			return true;
		case TM_SelfModified:
			if (failure->cmax != command_id)
				ereport(ERROR,
						(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
						 errmsg("tuple to be modified was already modified by an operation triggered by the current command"),
						 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
			/* ModifyTable also treats an already handled command tuple as
			 * zero rows; finish statement triggers before enforcing policy. */
			return false;
		case TM_Updated:
		case TM_Deleted:
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("target row changed after tuple writer locked it")));
			break;
		default:
			elog(ERROR, "unexpected tuple writer result: %d", status);
	}
	pg_unreachable();
}

/* Do not alter the copied TriggerDesc: generated columns depend on its flags. */
static bool
run_before_row(TupleStatement *statement, CmdType operation)
{
	EState *estate = statement->estate;
	ResultRelInfo *result = statement->result;
	TriggerDesc *triggers = result->ri_TrigDesc;
	bool proceed = true;

	if (triggers == NULL)
		return true;
	switch (operation)
	{
		case CMD_INSERT:
			if (triggers->trig_insert_before_row)
				proceed = ExecBRInsertTriggers(estate, result, statement->newslot);
			break;
		case CMD_UPDATE:
			if (triggers->trig_update_before_row)
				proceed = synchdb_pg_before_update(estate, &statement->epq, result,
												  &statement->oldslot->tts_tid, statement->newslot);
			break;
		case CMD_DELETE:
			if (triggers->trig_delete_before_row)
				proceed = synchdb_pg_before_delete(estate, &statement->epq, result,
												  &statement->oldslot->tts_tid);
			break;
		default:
			elog(ERROR, "invalid tuple writer operation: %d", operation);
	}
	return proceed;
}

static void
prepare_new_tuple(TupleStatement *statement, CmdType operation)
{
	Relation relation = statement->relation;
	TupleTableSlot *slot = statement->newslot;

	/* Triggers and expressions may read this before the physical write. */
	slot->tts_tableOid = RelationGetRelid(relation);
	if (relation->rd_att->constr &&
		relation->rd_att->constr->has_generated_stored)
		ExecComputeStoredGenerated(statement->result, statement->estate,
								   slot, operation);
	if (relation->rd_att->constr)
		ExecConstraints(statement->result, slot, statement->estate);
	if (relation->rd_rel->relispartition)
		ExecPartitionCheck(statement->result, slot, statement->estate, true);
}

static void
insert_tuple(TupleStatement *statement)
{
	List *recheck_indexes = NIL;

	if (!run_before_row(statement, CMD_INSERT))
		return;
	prepare_new_tuple(statement, CMD_INSERT);
	table_tuple_insert(statement->relation, statement->newslot,
					   statement->estate->es_output_cid, 0, NULL);
	statement->affected_rows++;
	if (statement->result->ri_NumIndices > 0)
		recheck_indexes = ExecInsertIndexTuples(statement->result,
											   statement->newslot, statement->estate,
											   false, false, NULL, NIL, false);
	ExecARInsertTriggers(statement->estate, statement->result,
						 statement->newslot, recheck_indexes,
						 statement->transition);
	list_free(recheck_indexes);
}

static void
update_tuple(TupleStatement *statement)
{
	TM_FailureData failure;
	TM_Result status;
	LockTupleMode lockmode;
	TU_UpdateIndexes update_indexes;
	List *recheck_indexes = NIL;

	if (!run_before_row(statement, CMD_UPDATE))
		return;
	prepare_new_tuple(statement, CMD_UPDATE);
	status = table_tuple_update(statement->relation,
								&statement->oldslot->tts_tid, statement->newslot,
								statement->estate->es_output_cid,
								statement->estate->es_snapshot, InvalidSnapshot,
								true, &failure, &lockmode, &update_indexes);
	if (!check_tuple_write_result(status, &failure, statement->estate->es_output_cid))
		return;
	statement->affected_rows++;
	if (statement->result->ri_NumIndices > 0 && update_indexes != TU_None)
		recheck_indexes = ExecInsertIndexTuples(statement->result,
											   statement->newslot, statement->estate,
											   true, false, NULL, NIL,
											   update_indexes == TU_Summarizing);
	ExecARUpdateTriggers(statement->estate, statement->result, NULL, NULL,
						 &statement->oldslot->tts_tid, NULL, statement->newslot,
						 recheck_indexes, statement->transition, false);
	list_free(recheck_indexes);
}

static void
delete_tuple(TupleStatement *statement)
{
	TM_FailureData failure;
	TM_Result status;

	if (!run_before_row(statement, CMD_DELETE))
		return;
	status = table_tuple_delete(statement->relation,
								&statement->oldslot->tts_tid,
								statement->estate->es_output_cid,
								statement->estate->es_snapshot, InvalidSnapshot,
								true, &failure, false);
	if (!check_tuple_write_result(status, &failure, statement->estate->es_output_cid))
		return;
	statement->affected_rows++;
	ExecARDeleteTriggers(statement->estate, statement->result,
						 &statement->oldslot->tts_tid, NULL,
						 statement->transition, false);
}

static TupleTableSlot *
new_virtual_slot(TupleStatement *statement)
{
	TupleTableSlot *slot;
	int index;

	slot = ExecInitExtraTupleSlot(statement->estate,
								  RelationGetDescr(statement->relation), &TTSOpsVirtual);
	for (index = 0; index < slot->tts_tupleDescriptor->natts; index++)
		slot->tts_isnull[index] = true;
	slot->tts_tableOid = RelationGetRelid(statement->relation);
	return slot;
}

static void
init_tuple_statement(TupleStatement *statement, const SynchdbTargetChange *change,
					 CmdType operation)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	List *permissions = NIL;
	Bitmapset *provided;
	int member = -1;

	statement->relation = table_open(change->tableoid, RowExclusiveLock);
	synchdb_target_validate_change(statement->relation, change);
	if (!isTempNamespace(RelationGetNamespace(statement->relation)))
		PreventCommandIfReadOnly("tuple writer");
	PreventCommandIfParallelMode("tuple writer");
	CheckCmdReplicaIdentity(statement->relation, operation);

	statement->estate = CreateExecutorState();
	MemoryContextSwitchTo(statement->estate->es_query_cxt);
	statement->estate->es_snapshot = GetActiveSnapshot();
	statement->estate->es_output_cid = GetCurrentCommandId(true);
	rte->rtekind = RTE_RELATION;
	rte->relid = change->tableoid;
	rte->relkind = statement->relation->rd_rel->relkind;
	rte->rellockmode = RowExclusiveLock;
	statement->permissions = addRTEPermissionInfo(&permissions, rte);
	synchdb_pg_init_range_table(statement->estate, list_make1(rte), permissions);
	statement->result = makeNode(ResultRelInfo);
	InitResultRelInfo(statement->result, statement->relation, 1, NULL, 0);
	statement->estate->es_opened_result_relations =
		lappend(statement->estate->es_opened_result_relations, statement->result);

	if (operation != CMD_DELETE)
	{
		statement->newslot = new_virtual_slot(statement);
		provided = store_tuple_columns(statement->newslot, change->columnValuesAfter);
		/* UPDATE OF and generated dependencies are fixed before BS triggers. */
		if (operation == CMD_UPDATE)
			while ((member = bms_next_member(provided, member)) >= 0)
				statement->permissions->updatedCols =
					bms_add_member(statement->permissions->updatedCols,
								   member + 1 - FirstLowInvalidHeapAttributeNumber);
		bms_free(provided);
	}
	if (operation != CMD_INSERT)
	{
		statement->searchslot = new_virtual_slot(statement);
		bms_free(store_tuple_columns(statement->searchslot, change->columnValuesBefore));
		ExecStoreVirtualTuple(statement->searchslot);
		statement->oldslot = table_slot_create(statement->relation,
											 &statement->estate->es_tupleTable);
		EvalPlanQualInit(&statement->epq, statement->estate, NULL, NIL, -1, NIL);
		statement->epq_initialized = true;
	}
	AfterTriggerBeginQuery();
	statement->transition = MakeTransitionCaptureState(statement->result->ri_TrigDesc,
														 change->tableoid, operation);
}

static void
run_tuple_statement(TupleStatement *statement, const SynchdbTargetChange *change,
					CmdType operation)
{
	Oid identity;
	Bitmapset *provided;
	bool found = true;

	/* Match ModifyTable: BS precedes defaults, tuple lookup and all row work. */
	switch (operation)
	{
		case CMD_INSERT:
			ExecBSInsertTriggers(statement->estate, statement->result);
			provided = store_tuple_columns(statement->newslot, change->columnValuesAfter);
			fill_tuple_defaults(statement->relation, statement->estate,
								statement->newslot, provided);
			bms_free(provided);
			ExecStoreVirtualTuple(statement->newslot);
			break;
		case CMD_UPDATE:
			ExecBSUpdateTriggers(statement->estate, statement->result);
			break;
		case CMD_DELETE:
			ExecBSDeleteTriggers(statement->estate, statement->result);
			break;
		default:
			elog(ERROR, "invalid tuple writer operation: %d", operation);
	}

	ExecOpenIndices(statement->result, false);
	if (operation != CMD_INSERT)
	{
		identity = tuple_identity_index(statement->relation, change);
		found = RelationFindReplTupleByIndex(statement->relation, identity,
										   LockTupleExclusive, statement->searchslot,
										   statement->oldslot);
	}

	switch (operation)
	{
		case CMD_INSERT:
			insert_tuple(statement);
			ExecASInsertTriggers(statement->estate, statement->result,
								 statement->transition);
			break;
		case CMD_UPDATE:
			if (found)
			{
				/* OLD is the live target row; AFTER only overlays SET fields. */
				slot_getallattrs(statement->oldslot);
				memcpy(statement->newslot->tts_values, statement->oldslot->tts_values,
					   change->natts * sizeof(Datum));
				memcpy(statement->newslot->tts_isnull, statement->oldslot->tts_isnull,
					   change->natts * sizeof(bool));
				bms_free(store_tuple_columns(statement->newslot, change->columnValuesAfter));
				ExecStoreVirtualTuple(statement->newslot);
				EvalPlanQualSetSlot(&statement->epq, statement->newslot);
				update_tuple(statement);
			}
			ExecASUpdateTriggers(statement->estate, statement->result,
								 statement->transition);
			break;
		case CMD_DELETE:
			if (found)
			{
				EvalPlanQualSetSlot(&statement->epq, statement->oldslot);
				delete_tuple(statement);
			}
			ExecASDeleteTriggers(statement->estate, statement->result,
								 statement->transition);
			break;
		default:
			elog(ERROR, "invalid tuple writer operation: %d", operation);
	}
}

SynchdbRowApplyResult
synchdb_tuple_writer_apply(const SynchdbTargetChange *change)
{
	MemoryContext caller_context = CurrentMemoryContext;
	MemoryContext statement_context;
	TupleStatement *statement;
	SynchdbRowApplyResult result;
	CmdType operation;

	if (change == NULL || !IsTransactionState())
		elog(ERROR, "tuple writer requires a bound row and an active transaction");
	switch (change->op)
	{
		case 'c':
		case 'r':
			operation = CMD_INSERT;
			break;
		case 'u':
			operation = CMD_UPDATE;
			break;
		case 'd':
			operation = CMD_DELETE;
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported target row operation: %c", change->op)));
	}
	/*
	 * A failed executor can still have ResourceOwner refs and deferred trigger
	 * state. Keep its memory under the transaction, not the event scratch
	 * context, until the caller rolls back.
	 */
	statement_context = AllocSetContextCreate(CurTransactionContext,
												 "Synchdb tuple statement",
												 ALLOCSET_DEFAULT_SIZES);
	statement = MemoryContextAllocZero(statement_context, sizeof(*statement));
	PG_TRY();
	{
		MemoryContextSwitchTo(statement_context);
		PreventCommandIfParallelMode("tuple writer");
		/* Match a mutating SPI statement's command and fresh snapshot boundary. */
		CommandCounterIncrement();
		PushActiveSnapshot(GetTransactionSnapshot());
		init_tuple_statement(statement, change, operation);
		run_tuple_statement(statement, change, operation);

		/* Match SQL even for a missing/suppressed row: statement triggers run
		 * before the exact-one policy rejects the completed zero-row write. */
		AfterTriggerEndQuery(statement->estate);
		ExecCloseIndices(statement->result);
		if (statement->epq_initialized)
			EvalPlanQualEnd(&statement->epq);
		synchdb_pg_close_trigger_relations(statement->estate);
		ExecResetTupleTable(statement->estate->es_tupleTable, false);
		MemoryContextSwitchTo(caller_context);
		FreeExecutorState(statement->estate);
		/* Retain the write lock until the enclosing transaction ends. */
		table_close(statement->relation, NoLock);
		CommandCounterIncrement();
		PopActiveSnapshot();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(caller_context);
		/*
		 * Nested trigger SQL can leave its own snapshot/query scope on ERROR.
		 * Transaction rollback owns all failure cleanup; in particular, do not
		 * run success callbacks through FreeExecutorState/AfterTriggerEndQuery.
		 */
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = synchdb_row_apply_require_one(statement->affected_rows);
	MemoryContextDelete(statement_context);
	return result;
}
