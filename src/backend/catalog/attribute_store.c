/* Attribute binding catalog access; transactions belong to the caller. */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/attribute_store.h"
#include "catalog/catalog_access.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "synchdb/synchdb.h"
#include "utils/builtins.h"

#define ATTRIBUTE_STORE_TABLE "synchdb_attribute"

static void
attribute_store_name(const char *value, const char *field)
{
	if (!value || value[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("attribute binding field \"%s\" must not be empty", field)));
	if (strnlen(value, NAMEDATALEN) >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("attribute binding field \"%s\" exceeds the catalog NAME capacity", field),
				 errdetail("The maximum length is %d bytes.", NAMEDATALEN - 1)));
}

static void
attribute_store_scope(const char *name, const char *type)
{
	attribute_store_name(name, "name");
	attribute_store_name(type, "type");
}

static void
attribute_store_source(const char *value, const char *field)
{
	if (!value || value[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("attribute binding field \"%s\" must not be empty", field)));
	if (strnlen(value, SYNCHDB_SOURCE_ID_SIZE) > SYNCHDB_SOURCE_ID_MAX_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("attribute binding field \"%s\" exceeds the source ID capacity", field),
				 errdetail("The maximum length is %d bytes.", SYNCHDB_SOURCE_ID_MAX_BYTES)));
}

static char *
attribute_store_type(const char *type)
{
	char *normalized = pstrdup(type);
	char *p;

	/* Connector type identifiers are ASCII tokens; never fold source names. */
	for (p = normalized; *p; p++)
		if (*p >= 'A' && *p <= 'Z')
			*p += 'a' - 'A';
	return normalized;
}

static void
attribute_store_target(Oid relation, AttrNumber attnum)
{
	if (!OidIsValid(relation) || attnum <= 0 || attnum > MaxHeapAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("attribute binding requires a valid target relation and user attribute number")));
}

static List *
attribute_store_list(const char *name, const char *type,
										const char *source_table,
										Oid target_relation,
										MemoryContext result_context)
{
	Oid argtypes[3] = {TEXTOID, TEXTOID, TEXTOID};
	Datum values[3];
	char *relation;
	char *query;
	List *result = NIL;
	uint64 i;

	attribute_store_scope(name, type);
	if (source_table)
		attribute_store_source(source_table, "source_table");
	else
	{
		attribute_store_target(target_relation, 1);
		argtypes[2] = OIDOID;
	}
	if (result_context == NULL)
		elog(ERROR, "attribute binding list requires an owning result context");
	synchdb_store_connect();
	relation = synchdb_store_relation(ATTRIBUTE_STORE_TABLE, AccessShareLock);
	query = psprintf(
		"SELECT attrelid,attnum,ext_tbname,ext_attname,ext_atttypename FROM %s "
		"WHERE name::pg_catalog.text OPERATOR(pg_catalog.=) $1 "
		"AND type::pg_catalog.text OPERATOR(pg_catalog.=) $2 "
		"AND %s ORDER BY attrelid,attnum", relation,
		source_table ? "ext_tbname::pg_catalog.text COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) $3" :
		"attrelid OPERATOR(pg_catalog.=) $3");
	values[0] = CStringGetTextDatum(name);
	values[1] = CStringGetTextDatum(attribute_store_type(type));
	values[2] = source_table ? CStringGetTextDatum(source_table) : ObjectIdGetDatum(target_relation);
	/* A batch's entry snapshot predates bindings written by an earlier schema
	 * event. A current command snapshot is required even though this is SELECT. */
	if (SPI_execute_with_args(query, 3, argtypes, values, NULL, false, 0) != SPI_OK_SELECT)
		elog(ERROR, "could not read source-table attribute bindings");
	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple tuple = SPI_tuptable->vals[i];
		TupleDesc descriptor = SPI_tuptable->tupdesc;
		SynchdbAttributeBinding *binding;
		Datum value;
		bool isnull;
		MemoryContext previous = MemoryContextSwitchTo(result_context);

		binding = palloc0(sizeof(*binding));
		value = SPI_getbinval(tuple, descriptor, 1, &isnull);
		if (isnull)
			elog(ERROR, "attribute binding has a null target relation");
		binding->target_relation = DatumGetObjectId(value);
		value = SPI_getbinval(tuple, descriptor, 2, &isnull);
		if (isnull)
			elog(ERROR, "attribute binding has a null target attribute number");
		binding->target_attnum = DatumGetInt16(value);
		value = SPI_getbinval(tuple, descriptor, 3, &isnull);
		if (!isnull)
			binding->source_table = TextDatumGetCString(value);
		value = SPI_getbinval(tuple, descriptor, 4, &isnull);
		if (!isnull)
			binding->source_column = TextDatumGetCString(value);
		value = SPI_getbinval(tuple, descriptor, 5, &isnull);
		if (!isnull)
			binding->source_type = TextDatumGetCString(value);
		result = lappend(result, binding);
		MemoryContextSwitchTo(previous);
	}
	synchdb_store_finish();
	return result;
}

List *
synchdb_attribute_store_list_source_table(const char *name, const char *type,
										const char *source_table,
										MemoryContext result_context)
{
	attribute_store_source(source_table, "source_table");
	return attribute_store_list(name, type, source_table, InvalidOid, result_context);
}

List *
synchdb_attribute_store_list_target_relation(const char *name, const char *type,
										   Oid relation, MemoryContext result_context)
{
	return attribute_store_list(name, type, NULL, relation, result_context);
}

void
synchdb_attribute_store_upsert(const char *name, const char *type,
							  const SynchdbAttributeBinding *bindings, int count)
{
	Oid argtypes[7] = {TEXTOID, TEXTOID, OIDOID, INT2OID, TEXTOID, TEXTOID, TEXTOID};
	Datum values[7];
	char *relation;
	char *query;
	int i;
	int j;

	attribute_store_scope(name, type);
	if (!bindings || count <= 0 || count > MaxHeapAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("attribute upsert requires a nonempty supported binding list")));
	/* Validate the complete batch before the first catalog mutation. */
	for (i = 0; i < count; i++)
	{
		attribute_store_target(bindings[i].target_relation, bindings[i].target_attnum);
		attribute_store_source(bindings[i].source_table, "source_table");
		attribute_store_source(bindings[i].source_column, "source_column");
		attribute_store_source(bindings[i].source_type, "source_type");
		if (strlen(bindings[i].source_table) + 1 + strlen(bindings[i].source_column) >
			SYNCHDB_SOURCE_ID_MAX_BYTES)
			ereport(ERROR,
					(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
					 errmsg("qualified source column exceeds the source ID capacity"),
					 errdetail("The source table, dot and column together support at most %d bytes.",
							   SYNCHDB_SOURCE_ID_MAX_BYTES)));
		for (j = 0; j < i; j++)
			if (bindings[j].target_relation == bindings[i].target_relation &&
				bindings[j].target_attnum == bindings[i].target_attnum)
				ereport(ERROR,
						(errcode(ERRCODE_CARDINALITY_VIOLATION),
						 errmsg("multiple source columns bind the same target attribute")));
	}
	synchdb_store_connect();
	relation = synchdb_store_relation(ATTRIBUTE_STORE_TABLE, RowExclusiveLock);
	query = psprintf(
		"INSERT INTO %s(name,type,attrelid,attnum,ext_tbname,ext_attname,ext_atttypename) "
		"VALUES($1,$2,$3,$4,$5,$6,$7) "
		"ON CONFLICT(name,type,attrelid,attnum) DO UPDATE SET "
		"ext_tbname=EXCLUDED.ext_tbname,ext_attname=EXCLUDED.ext_attname,"
		"ext_atttypename=EXCLUDED.ext_atttypename", relation);
	values[0] = CStringGetTextDatum(name);
	values[1] = CStringGetTextDatum(attribute_store_type(type));
	for (i = 0; i < count; i++)
	{
		values[2] = ObjectIdGetDatum(bindings[i].target_relation);
		values[3] = Int16GetDatum(bindings[i].target_attnum);
		values[4] = CStringGetTextDatum(bindings[i].source_table);
		values[5] = CStringGetTextDatum(bindings[i].source_column);
		values[6] = CStringGetTextDatum(bindings[i].source_type);
		if (SPI_execute_with_args(query, 7, argtypes, values, NULL, false, 0) != SPI_OK_INSERT)
			elog(ERROR, "could not upsert attribute binding");
		if (SPI_processed != 1)
			elog(ERROR, "attribute binding upsert affected an unexpected number of rows");
	}
	synchdb_store_finish();
}

uint64
synchdb_attribute_store_delete_source_table(const char *name, const char *type,
										  const char *source_table)
{
	Oid argtypes[3] = {TEXTOID, TEXTOID, TEXTOID};
	Datum values[3];
	char *relation;
	char *query;
	uint64 affected;

	attribute_store_scope(name, type);
	attribute_store_source(source_table, "source_table");
	synchdb_store_connect();
	relation = synchdb_store_relation(ATTRIBUTE_STORE_TABLE, RowExclusiveLock);
	query = psprintf(
		"DELETE FROM %s WHERE name::pg_catalog.text OPERATOR(pg_catalog.=) $1 "
		"AND type::pg_catalog.text OPERATOR(pg_catalog.=) $2 "
		"AND ext_tbname::pg_catalog.text COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) $3", relation);
	values[0] = CStringGetTextDatum(name);
	values[1] = CStringGetTextDatum(attribute_store_type(type));
	values[2] = CStringGetTextDatum(source_table);
	if (SPI_execute_with_args(query, 3, argtypes, values, NULL, false, 0) != SPI_OK_DELETE)
		elog(ERROR, "could not delete source-table attribute bindings");
	affected = SPI_processed;
	synchdb_store_finish();
	return affected;
}

uint64
synchdb_attribute_store_mark_dropped(const char *name, const char *type,
								   const char *source_table, Oid target_relation,
								   AttrNumber target_attnum)
{
	Oid argtypes[6] = {TEXTOID, TEXTOID, OIDOID, INT2OID, TEXTOID, TEXTOID};
	Datum values[6];
	char *relation;
	char *query;
	uint64 affected;

	attribute_store_scope(name, type);
	attribute_store_source(source_table, "source_table");
	attribute_store_target(target_relation, target_attnum);
	synchdb_store_connect();
	relation = synchdb_store_relation(ATTRIBUTE_STORE_TABLE, RowExclusiveLock);
	query = psprintf(
		"UPDATE %s SET ext_attname=$6,ext_atttypename=NULL "
		"WHERE name::pg_catalog.text OPERATOR(pg_catalog.=) $1 "
		"AND type::pg_catalog.text OPERATOR(pg_catalog.=) $2 "
		"AND attrelid OPERATOR(pg_catalog.=) $3 "
		"AND attnum OPERATOR(pg_catalog.=) $4 "
		"AND ext_tbname::pg_catalog.text COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) $5", relation);
	values[0] = CStringGetTextDatum(name);
	values[1] = CStringGetTextDatum(attribute_store_type(type));
	values[2] = ObjectIdGetDatum(target_relation);
	values[3] = Int16GetDatum(target_attnum);
	values[4] = CStringGetTextDatum(source_table);
	values[5] = CStringGetTextDatum(psprintf("........synchdb.dropped.%d........",
										   target_attnum));
	if (SPI_execute_with_args(query, 6, argtypes, values, NULL, false, 0) != SPI_OK_UPDATE)
		elog(ERROR, "could not mark dropped attribute binding");
	if (SPI_processed > 1)
		elog(ERROR, "dropped attribute update affected multiple bindings");
	affected = SPI_processed;
	synchdb_store_finish();
	return affected;
}

uint64
synchdb_attribute_store_prune_missing(const char *name, const char *type)
{
	Oid argtypes[2] = {TEXTOID, TEXTOID};
	Datum values[2];
	char *relation;
	char *query;
	uint64 affected;

	attribute_store_scope(name, type);
	synchdb_store_connect();
	relation = synchdb_store_relation(ATTRIBUTE_STORE_TABLE, RowExclusiveLock);
	query = psprintf(
		"DELETE FROM %s AS binding "
		"WHERE binding.name::pg_catalog.text OPERATOR(pg_catalog.=) $1 "
		"AND binding.type::pg_catalog.text OPERATOR(pg_catalog.=) $2 "
		"AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_class AS target "
		"WHERE target.oid OPERATOR(pg_catalog.=) binding.attrelid)", relation);
	values[0] = CStringGetTextDatum(name);
	values[1] = CStringGetTextDatum(attribute_store_type(type));
	if (SPI_execute_with_args(query, 2, argtypes, values, NULL, false, 0) != SPI_OK_DELETE)
		elog(ERROR, "could not prune missing-relation attribute bindings");
	affected = SPI_processed;
	synchdb_store_finish();
	return affected;
}
