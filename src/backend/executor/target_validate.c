/* The two Writers accept the same target capabilities and column contract. */
#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/catalog.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class_d.h"
#include "executor/executor.h"
#include "executor/target_validate.h"
#include "miscadmin.h"
#include "parser/parse_relation.h"
#include "utils/rel.h"

void
synchdb_target_validate_relation(Relation relation)
{
	int index;

	if (relation == NULL || relation->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row apply requires an ordinary target table")));
	/* Source acknowledgement requires durable target rows. ForceSyncCommit
	 * cannot make unlogged or temporary table contents WAL-protected. */
	if (relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row apply requires a permanent WAL-logged target table")));
	if (IsCatalogRelation(relation))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row apply does not support system catalog targets")));
	if (relation->rd_rules != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row apply does not support target rewrite rules")));
	if (relation->rd_rel->relrowsecurity)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row apply does not support row security on its target")));
	for (index = 0; index < RelationGetDescr(relation)->natts; index++)
	{
		Form_pg_attribute attribute = TupleDescAttr(RelationGetDescr(relation), index);

		/* Virtual generated columns need executor projections not present in
		 * the physical Writer. Stored generated columns are fully supported. */
		if (!attribute->attisdropped && attribute->attgenerated == 'v')
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("row apply does not support virtual generated target columns")));
	}
}

static Bitmapset *
validate_columns(Relation relation, List *columns, bool check_values)
{
	TupleDesc descriptor = RelationGetDescr(relation);
	Bitmapset *provided = NULL;
	ListCell *cell;

	foreach(cell, columns)
	{
		const SynchdbTargetColumn *column = lfirst(cell);
		Form_pg_attribute attribute;
		int member;

		if (column == NULL || column->position <= 0 ||
			column->position > descriptor->natts)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("invalid bound target column position")));
		member = column->position - FirstLowInvalidHeapAttributeNumber;
		if (bms_is_member(member, provided))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_COLUMN),
					 errmsg("target column is provided more than once")));
		provided = bms_add_member(provided, member);
		attribute = TupleDescAttr(descriptor, column->position - 1);
		if (attribute->attisdropped || attribute->attgenerated ||
			attribute->attidentity == ATTRIBUTE_IDENTITY_ALWAYS)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("explicit values for dropped, generated or identity-always target columns are not supported")));
		if (column->name == NULL ||
			strcmp(column->name, NameStr(attribute->attname)) != 0 ||
			column->datatype != attribute->atttypid ||
			column->typmod != attribute->atttypmod ||
			column->typbyval != attribute->attbyval)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("target column changed after row binding")));
		if (check_values && !column->is_null && !column->typbyval &&
			DatumGetPointer(column->value) == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("non-null target value has no Datum storage")));
	}
	return provided;
}

static void
validate_change(Relation relation, const SynchdbTargetChange *change, bool check_values)
{
	RangeTblEntry *rte;
	RTEPermissionInfo *permissions;
	List *permission_list = NIL;
	Bitmapset *before;
	Bitmapset *after;

	synchdb_target_validate_relation(relation);
	if (change == NULL || change->tableoid != RelationGetRelid(relation))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("row apply requires a bound target relation")));
	if (change->natts < 0 || change->natts != RelationGetDescr(relation)->natts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("target table changed after row binding")));
	if ((change->op == 'c' || change->op == 'r') && change->columnValuesBefore != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target insert must not contain an identity predicate")));
	if (change->op == 'd' && change->columnValuesAfter != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target delete must not contain assigned fields")));
	if (change->op == 'u' && change->columnValuesAfter == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("update does not provide any target fields")));
	before = validate_columns(relation, change->columnValuesBefore, check_values);
	after = validate_columns(relation, change->columnValuesAfter, check_values);
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(relation);
	rte->relkind = relation->rd_rel->relkind;
	rte->rellockmode = RowExclusiveLock;
	permissions = addRTEPermissionInfo(&permission_list, rte);
	switch (change->op)
	{
		case 'c':
		case 'r':
			permissions->requiredPerms = ACL_INSERT;
			permissions->insertedCols = after;
			break;
		case 'u':
			permissions->requiredPerms = ACL_UPDATE | ACL_SELECT;
			permissions->updatedCols = after;
			permissions->selectedCols = before;
			break;
		case 'd':
			permissions->requiredPerms = ACL_DELETE | ACL_SELECT;
			permissions->selectedCols = before;
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported source row operation '%c'", change->op)));
	}
	ExecCheckPermissions(list_make1(rte), permission_list, true);
	bms_free(before);
	bms_free(after);
}

void
synchdb_target_validate_binding(Relation relation, const SynchdbTargetChange *change)
{
	validate_change(relation, change, false);
}

void
synchdb_target_validate_change(Relation relation, const SynchdbTargetChange *change)
{
	validate_change(relation, change, true);
}
