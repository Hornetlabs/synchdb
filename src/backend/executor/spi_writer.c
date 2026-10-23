/* Build target SQL from live metadata and keep row values outside SQL text. */
#include "postgres.h"

#include "executor/target_validate.h"

#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "executor/row_identity.h"
#include "executor/spi.h"
#include "executor/spi_writer.h"
#include "lib/stringinfo.h"
#include "parser/analyze.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

typedef struct SpiRowParameters
{
	int count;
	int capacity;
	Oid *types;
	Datum *values;
	char *nulls;
} SpiRowParameters;

typedef struct SpiIdentityParseGuard
{
	struct SpiIdentityParseGuard *previous;
	post_parse_analyze_hook_type previous_hook;
	post_parse_analyze_hook_type chained_hook;
	const char *query_text;
	const SynchdbRowIdentity *identity;
	Oid table_oid;
	CmdType command;
	int after_parameters;
	bool checked;
} SpiIdentityParseGuard;

/* Backend-local, and restored even when parsing or execution raises ERROR. */
static SpiIdentityParseGuard *identity_parse_guard = NULL;

static Node *
identity_argument(Node *node)
{
	while (node && (IsA(node, RelabelType) || IsA(node, CollateExpr)))
	{
		if (IsA(node, RelabelType))
			node = (Node *) ((RelabelType *) node)->arg;
		else
			node = (Node *) ((CollateExpr *) node)->arg;
	}
	return node;
}

static void
check_identity_quals(Node *node, const SpiIdentityParseGuard *guard,
					 Index result_relation, int *keyno)
{
	if (node && IsA(node, BoolExpr) &&
		((BoolExpr *) node)->boolop == AND_EXPR)
	{
		ListCell *cell;

		foreach(cell, ((BoolExpr *) node)->args)
			check_identity_quals(lfirst(cell), guard, result_relation, keyno);
		return;
	}
	if (node && IsA(node, OpExpr) && *keyno < guard->identity->nkeys)
	{
		const SynchdbIdentityKey *key = &guard->identity->keys[*keyno];
		OpExpr *operation = (OpExpr *) node;

		if (operation->opno == key->equality_operator &&
			operation->inputcollid == key->collation &&
			!operation->opretset && list_length(operation->args) == 2)
		{
			Node *left = identity_argument(linitial(operation->args));
			Node *right = identity_argument(lsecond(operation->args));

			if (left && IsA(left, Var) && right && IsA(right, Param))
			{
				Var *column = (Var *) left;
				Param *parameter = (Param *) right;

				if ((Index) column->varno == result_relation && column->varlevelsup == 0 &&
					column->varattno == key->position &&
					parameter->paramkind == PARAM_EXTERN &&
					parameter->paramid == guard->after_parameters + *keyno + 1)
				{
					(*keyno)++;
					return;
				}
			}
		}
	}
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("parsed row identity differs from its bound index")));
}

static void
spi_identity_parse_hook(ParseState *pstate, Query *query, JumbleState *jstate)
{
	SpiIdentityParseGuard *guard = identity_parse_guard;

	/* An inner writer inherits the external hook, never chains this hook to
	 * itself, and restores the outer writer's guard when it finishes. */
	if (guard->chained_hook)
		guard->chained_hook(pstate, query, jstate);
	for (guard = identity_parse_guard; guard; guard = guard->previous)
	{
		int keyno = 0;
		RangeTblEntry *target;

		if (pstate->p_sourcetext == NULL ||
			strcmp(pstate->p_sourcetext, guard->query_text) != 0)
			continue;
		if (query->commandType != guard->command ||
			query->resultRelation <= 0 ||
			query->resultRelation > list_length(query->rtable) ||
			query->jointree == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("parsed row change differs from its bound target")));
		target = list_nth_node(RangeTblEntry, query->rtable,
							   query->resultRelation - 1);
		if (target->rtekind != RTE_RELATION || target->relid != guard->table_oid)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("parsed row change differs from its bound target")));
		check_identity_quals(query->jointree->quals, guard,
							 query->resultRelation, &keyno);
		if (keyno != guard->identity->nkeys)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("parsed row identity does not contain every bound index key")));
		guard->checked = true;
		return;
	}
}

static int
execute_identity_guarded(const char *query, SpiRowParameters *parameters,
						 const SynchdbTargetChange *change, const SynchdbRowIdentity *identity)
{
	SpiIdentityParseGuard *guard = palloc0(sizeof(*guard));
	volatile int status = 0;

	guard->previous = identity_parse_guard;
	guard->previous_hook = post_parse_analyze_hook;
	guard->chained_hook = post_parse_analyze_hook == spi_identity_parse_hook ?
		guard->previous->chained_hook : post_parse_analyze_hook;
	guard->query_text = query;
	guard->identity = identity;
	guard->table_oid = change->tableoid;
	guard->command = change->op == 'u' ? CMD_UPDATE : CMD_DELETE;
	guard->after_parameters = list_length(change->columnValuesAfter);
	identity_parse_guard = guard;
	post_parse_analyze_hook = spi_identity_parse_hook;
	PG_TRY();
	{
		/* One-shot SPI analyzes the actual statement under this hook. After
		 * checking, OpExpr OIDs stay fixed even if another overload appears. */
		status = SPI_execute_with_args(query, parameters->count, parameters->types,
									   parameters->values, parameters->nulls, false, 0);
		if (!guard->checked)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("row identity was not checked during SQL analysis")));
	}
	PG_FINALLY();
	{
		post_parse_analyze_hook = guard->previous_hook;
		identity_parse_guard = guard->previous;
		pfree(guard);
	}
	PG_END_TRY();
	return status;
}

/* Validate the exact identity selected while the event was bound. */
static SynchdbRowIdentity *
validate_identity(Relation relation, const SynchdbTargetChange *change)
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
	return identity;
}

static void
append_parameter(StringInfo query, SpiRowParameters *parameters,
				 const SynchdbTargetColumn *column)
{
	int index = parameters->count;

	Assert(index < parameters->capacity);
	parameters->types[index] = column->datatype;
	parameters->values[index] = column->is_null ? (Datum) 0 : column->value;
	parameters->nulls[index] = column->is_null ? 'n' : ' ';
	parameters->count++;
	appendStringInfo(query, "$%d", index + 1);
}

/* Index comparisons use their opclass input type and index collation. */
static void
append_identity_predicate(StringInfo query, SpiRowParameters *parameters,
						  const SynchdbIdentityKey *key,
						  const SynchdbTargetColumn *column)
{
	char *input_type = format_type_be_qualified(key->input_type);
	char *collation = NULL;
	HeapTuple operator_tuple;
	Form_pg_operator operator;
	char *operator_schema;

	if (OidIsValid(key->collation))
	{
		HeapTuple collation_tuple =
			SearchSysCache1(COLLOID, ObjectIdGetDatum(key->collation));
		Form_pg_collation form;

		if (!HeapTupleIsValid(collation_tuple))
			elog(ERROR, "cache lookup failed for collation %u", key->collation);
		form = (Form_pg_collation) GETSTRUCT(collation_tuple);
		collation = quote_qualified_identifier(get_namespace_name(form->collnamespace),
											  NameStr(form->collname));
		ReleaseSysCache(collation_tuple);
	}
	operator_tuple = SearchSysCache1(OPEROID,
									 ObjectIdGetDatum(key->equality_operator));
	if (!HeapTupleIsValid(operator_tuple))
		elog(ERROR, "cache lookup failed for operator %u", key->equality_operator);
	operator = (Form_pg_operator) GETSTRUCT(operator_tuple);
	operator_schema = get_namespace_name(operator->oprnamespace);
	if (operator_schema == NULL)
		elog(ERROR, "cache lookup failed for namespace %u", operator->oprnamespace);
	appendStringInfo(query, "(%s::%s", quote_identifier(column->name), input_type);
	if (collation)
		appendStringInfo(query, " COLLATE %s", collation);
	appendStringInfo(query, ") OPERATOR(%s.%s) (",
					 quote_identifier(operator_schema), NameStr(operator->oprname));
	ReleaseSysCache(operator_tuple);
	append_parameter(query, parameters, column);
	appendStringInfo(query, "::%s", input_type);
	if (collation)
		appendStringInfo(query, " COLLATE %s", collation);
	appendStringInfoChar(query, ')');
	pfree(operator_schema);
	pfree(input_type);
	if (collation)
		pfree(collation);
}

SynchdbRowApplyResult
synchdb_spi_writer_apply(const SynchdbTargetChange *change)
{
	Relation relation;
	SynchdbRowIdentity *identity = NULL;
	StringInfoData query;
	SpiRowParameters parameters = {0};
	ListCell *cell;
	char *qualified;
	bool first;
	SynchdbRowApplyResult result;
	int status;

	if (change == NULL || !OidIsValid(change->tableoid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI row writer requires a bound target table")));
	relation = table_open(change->tableoid, RowExclusiveLock);
	synchdb_target_validate_change(relation, change);
	if (change->op == 'u' || change->op == 'd')
		identity = validate_identity(relation, change);
	parameters.capacity = list_length(change->columnValuesAfter) +
		(identity ? identity->nkeys : 0);
	if (parameters.capacity > 0)
	{
		parameters.types = palloc(sizeof(Oid) * parameters.capacity);
		parameters.values = palloc(sizeof(Datum) * parameters.capacity);
		parameters.nulls = palloc(sizeof(char) * parameters.capacity);
	}
	qualified = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(relation)),
											 RelationGetRelationName(relation));
	initStringInfo(&query);
	switch (change->op)
	{
		case 'c':
		case 'r':
			appendStringInfo(&query, "INSERT INTO %s", qualified);
			if (change->columnValuesAfter == NIL)
				appendStringInfoString(&query, " DEFAULT VALUES");
			else
			{
				appendStringInfoChar(&query, '(');
				first = true;
				foreach(cell, change->columnValuesAfter)
				{
					const SynchdbTargetColumn *column = lfirst(cell);

					appendStringInfo(&query, "%s%s", first ? "" : ",",
									 quote_identifier(column->name));
					first = false;
				}
				appendStringInfoString(&query, ") VALUES (");
				first = true;
				foreach(cell, change->columnValuesAfter)
				{
					if (!first)
						appendStringInfoChar(&query, ',');
					append_parameter(&query, &parameters, lfirst(cell));
					first = false;
				}
				appendStringInfoChar(&query, ')');
			}
			break;
		case 'u':
			if (change->columnValuesAfter == NIL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("update does not provide any target fields")));
			/* Binding names one relation OID; inheritance must not widen it. */
			appendStringInfo(&query, "UPDATE ONLY %s SET ", qualified);
			first = true;
			foreach(cell, change->columnValuesAfter)
			{
				const SynchdbTargetColumn *column = lfirst(cell);

				appendStringInfo(&query, "%s%s=", first ? "" : ",",
								 quote_identifier(column->name));
				append_parameter(&query, &parameters, column);
				first = false;
			}
			break;
		case 'd':
			/* Match the Tuple Writer's exact bound-relation scope. */
			appendStringInfo(&query, "DELETE FROM ONLY %s", qualified);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported source row operation '%c'", change->op)));
	}
	if (change->op == 'u' || change->op == 'd')
	{
		int key_index;

		appendStringInfoString(&query, " WHERE ");
		for (key_index = 0; key_index < identity->nkeys; key_index++)
		{
			const SynchdbIdentityKey *key = &identity->keys[key_index];
			const SynchdbTargetColumn *column = NULL;

			foreach(cell, change->columnValuesBefore)
			{
				const SynchdbTargetColumn *candidate = lfirst(cell);

				if (candidate->position == key->position)
				{
					column = candidate;
					break;
				}
			}
			if (column == NULL)
				elog(ERROR, "bound row does not contain identity attribute %d",
					 key->position);
			if (key_index > 0)
				appendStringInfoString(&query, " AND ");
			append_identity_predicate(&query, &parameters, key, column);
		}
	}
	appendStringInfoChar(&query, ';');
	if (identity)
		status = execute_identity_guarded(query.data, &parameters, change, identity);
	else
		status = SPI_execute_with_args(query.data, parameters.count, parameters.types,
									   parameters.values, parameters.nulls, false, 0);
	/* Capture the outer statement's count, excluding nested trigger SQL. */
	result.affected_rows = SPI_processed;
	if ((change->op == 'c' || change->op == 'r') ? status != SPI_OK_INSERT :
		change->op == 'u' ? status != SPI_OK_UPDATE : status != SPI_OK_DELETE)
		elog(ERROR, "SPI row execution failed: %d", status);
	result = synchdb_row_apply_require_one(result.affected_rows);
	table_close(relation, NoLock);
	return result;
}
