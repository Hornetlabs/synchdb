/*
 * Resolve one live indexed identity for binding and both Writers.
 *
 * Debezium's value-only protocol does not establish which non-key old fields
 * were actually provided. Keep the existing primary-key column contract;
 * an explicit RI index may use those same columns with its own comparisons.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type_d.h"
#include "executor/row_identity.h"
#include "nodes/makefuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "replication/logicalrelation.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

SynchdbRowIdentity *
synchdb_resolve_row_identity(Relation relation)
{
	SynchdbRowIdentity *identity;
	Relation index;
	Bitmapset *primary;
	TupleDesc descriptor;
	Oid selected;
	int keyno;

	if (!IsTransactionState())
		ereport(ERROR,
				(errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
				 errmsg("row identity binding requires an active transaction")));
	if (relation == NULL || relation->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("indexed row identity requires an ordinary target table")));
	selected = GetRelationIdentityOrPK(relation);
	if (!OidIsValid(selected))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row changes require an immediate primary key or equivalent replica identity index")));
	/* Retain this index lock as well as the caller's table lock. Concurrent
	 * index replacement must not release the binding before target commit. */
	index = index_open(selected, RowExclusiveLock);
	if (selected != GetRelationIdentityOrPK(relation))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("target identity index changed while binding the row")));
	if (index->rd_index->indrelid != RelationGetRelid(relation) ||
		index->rd_rel->relam != BTREE_AM_OID ||
		!index->rd_index->indisunique || !index->rd_index->indimmediate ||
		!index->rd_index->indisvalid || !index->rd_index->indisready ||
		!index->rd_index->indislive || index->rd_index->indisexclusion ||
		RelationGetIndexExpressions(index) != NIL ||
		RelationGetIndexPredicate(index) != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row identity requires a live immediate unique btree index without expressions or a predicate")));
	identity = palloc0(sizeof(*identity));
	identity->index_oid = selected;
	identity->nkeys = IndexRelationGetNumberOfKeyAttributes(index);
	if (identity->nkeys <= 0 || identity->nkeys > INDEX_MAX_KEYS)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("target identity index has no usable key columns")));
	identity->keys = palloc0(identity->nkeys * sizeof(SynchdbIdentityKey));
	descriptor = RelationGetDescr(relation);
	for (keyno = 0; keyno < identity->nkeys; keyno++)
	{
		SynchdbIdentityKey *key = &identity->keys[keyno];
		Form_pg_attribute attribute;
		Oid opclass;
		Oid family;
		Oid opclass_input;
		HeapTuple operator_tuple;
		Form_pg_operator operator;
		char *operator_schema;
		List *operator_name;

		key->position = index->rd_index->indkey.values[keyno];
		if (key->position <= 0 || key->position > descriptor->natts)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("target identity requires ordinary user columns")));
		attribute = TupleDescAttr(descriptor, key->position - 1);
		if (attribute->attisdropped || attribute->attgenerated ||
			!attribute->attnotnull)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("target identity requires nonnull, nongenerated key columns")));
		opclass = get_index_column_opclass(selected, keyno + 1);
		if (!get_opclass_opfamily_and_input_type(opclass, &family,
													&opclass_input))
			elog(ERROR, "could not read target identity operator class");
		if (!IsBinaryCoercible(attribute->atttypid, opclass_input))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("target identity operator class requires an unsupported input type"),
					 errdetail("Identity comparisons require a binary-compatible operator class input type.")));
		/* SQL cannot cast to anyenum/anyarray/record. Preserve the concrete
		 * column representation while resolving the original family operator. */
		key->input_type = get_typtype(opclass_input) == TYPTYPE_PSEUDO ?
			getBaseType(attribute->atttypid) : opclass_input;
		key->equality_operator = get_opfamily_member(family, opclass_input,
													 opclass_input, BTEqualStrategyNumber);
		if (!OidIsValid(key->equality_operator) ||
			!OidIsValid(get_opcode(key->equality_operator)))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("target identity operator class has no usable equality operator")));
		operator_tuple = SearchSysCache1(OPEROID,
										 ObjectIdGetDatum(key->equality_operator));
		if (!HeapTupleIsValid(operator_tuple))
			elog(ERROR, "cache lookup failed for identity operator %u",
				 key->equality_operator);
		operator = (Form_pg_operator) GETSTRUCT(operator_tuple);
		operator_schema = get_namespace_name(operator->oprnamespace);
		if (operator_schema == NULL)
			elog(ERROR, "cache lookup failed for identity operator namespace");
		operator_name = list_make2(makeString(operator_schema),
								  makeString(pstrdup(NameStr(operator->oprname))));
		ReleaseSysCache(operator_tuple);
		/* A concrete overload can shadow a polymorphic family operator even
		 * when its schema is qualified. Do not let the Writers disagree. */
		if (compatible_oper_opid(operator_name, key->input_type,
								 key->input_type, true) != key->equality_operator)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("target identity operator cannot be resolved consistently"),
					 errdetail("The concrete SQL comparison must resolve to the index operator family equality operator.")));
		key->collation = index->rd_indcollation[keyno];
		identity->attributes = bms_add_member(identity->attributes,
												 key->position - FirstLowInvalidHeapAttributeNumber);
	}
	primary = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_PRIMARY_KEY);
	identity->primary_equivalent = !bms_is_empty(primary) &&
		bms_equal(primary, identity->attributes);
	bms_free(primary);
	if (!identity->primary_equivalent)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("target identity must use the complete primary-key column set"),
				 errdetail("The source protocol does not establish availability of arbitrary old non-key values.")));
	/* Relation/index metadata can be invalidated while acquiring the locks. */
	if (selected != GetRelationIdentityOrPK(relation))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("target identity index changed while binding the row")));
	index_close(index, NoLock);
	return identity;
}
