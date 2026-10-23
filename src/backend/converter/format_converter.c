/*-------------------------------------------------------------------------
 *
 * format_converter.c
 *    Conversion utilities for Debezium change events to PostgreSQL format
 *
 * This file contains functions to parse Debezium (DBZ) change events,
 * convert them to PostgreSQL-compatible DDL and DML operations, and
 * execute those operations. It handles CREATE, DROP, INSERT, UPDATE,
 * and DELETE operations from various source databases (currently 
 * MySQL, Oracle, and SQL Server) and converts them to equivalent 
 * PostgreSQL commands.
 *
 * The main entry point is fc_processDBZChangeEvent(), which takes a
 * Debezium change event as input, parses it, converts it, and executes
 * the resulting PostgreSQL operation.
 *
 * Key functions:
 * - parseDBZDDL(): Parses Debezium DDL events
 * - parseDBZDML(): Parses Debezium DML events
 * - convert2PGDDL(): Converts DBZ DDL to PostgreSQL DDL
 * - convert2PGDML(): Converts DBZ DML to PostgreSQL DML
 * - processDataByType(): Handles data type conversions
 *
 * Copyright (c) Hornetlabs Technology, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/inval.h"
#include "executor/target_validate.h"
#include "fmgr.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "replication/logicalrelation.h"
#include "access/table.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "executor/pg_compat.h"
#include "converter/rule_snapshot.h"
#include "source/debezium_schema.h"
#include "access/htup_details.h"
#include <time.h>
#include <sys/time.h>
#include <dlfcn.h>
#include "common/base64.h"
#include "port/pg_bswap.h"
#include "utils/datetime.h"
#include "utils/memutils.h"
#include "common/hashfn.h"
#include <stdarg.h>

/* synchdb includes */
#include "catalog/attribute_store.h"
#include "converter/format_converter.h"
#include "converter/value_codec.h"
#include "converter/transform_expression.h"
#include "executor/row_identity.h"
#include "runtime/config.h"
#include "converter/debezium_event_handler.h"
#ifdef WITH_OLR
#include "converter/olr_event_handler.h"
#endif
#include "synchdb/synchdb.h"

/* data transformation related hash tables */
HTAB * dataCacheHash = NULL;
static MemoryContext converterRuleContext = NULL;
static uint64 dataCacheEpoch = 0;

static uint32
source_column_hash(const void *key, Size keysize)
{
	const char *name = *(const char *const *) key;

	return hash_bytes((const unsigned char *) name, strlen(name));
}

static int
source_column_match(const void *left, const void *right, Size keysize)
{
	return strcmp(*(const char *const *) left, *(const char *const *) right);
}

SourceColumnCache *
fc_create_source_column_cache(MemoryContext parent)
{
	MemoryContext context;
	SourceColumnCache *cache;
	HASHCTL info = {0};

	if (!parent)
		elog(ERROR, "source column cache requires an owning memory context");
	context = AllocSetContextCreate(parent, "synchdb source columns", ALLOCSET_DEFAULT_SIZES);
	cache = MemoryContextAllocZero(context, sizeof(*cache));
	cache->context = context;
	info.keysize = sizeof(char *);
	info.entrysize = sizeof(NameJsonposEntry);
	info.hcxt = context;
	info.hash = source_column_hash;
	info.match = source_column_match;
	cache->hash = hash_create("source column metadata", 512, &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
	return cache;
}

NameJsonposEntry *
fc_source_column_lookup(SourceColumnCache *cache, const char *name)
{
	if (!cache || !name)
		return NULL;
	return hash_search(cache->hash, &name, HASH_FIND, NULL);
}

void
fc_source_column_insert(SourceColumnCache *cache, const char *name,
						int jsonpos, int dbztype, TimeRep timerep, int scale)
{
	NameJsonposEntry *entry;
	char *owned_name;

	if (!cache || !name || name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("source column cache requires a nonempty column name")));
	if (strlen(name) > SYNCHDB_SOURCE_ID_MAX_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("source column name exceeds %d bytes", SYNCHDB_SOURCE_ID_MAX_BYTES)));
	if (fc_source_column_lookup(cache, name))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_COLUMN),
				 errmsg("duplicate source column name in event schema")));
	owned_name = MemoryContextStrdup(cache->context, name);
	entry = hash_search(cache->hash, &owned_name, HASH_ENTER, NULL);
	entry->jsonpos = jsonpos;
	entry->dbztype = dbztype;
	entry->timerep = timerep;
	entry->scale = scale;
}

void
fc_destroy_source_column_cache(SourceColumnCache *cache)
{
	if (cache)
		MemoryContextDelete(cache->context);
}

static void remove_precision(char * str, bool * removed);
static int count_active_columns(TupleDesc tupdesc);
static long long derive_value_from_byte(const unsigned char * bytes, int len);
static void reverse_byte_array(unsigned char * array, int length);
static void trim_leading_zeros(char *str);
static void byte_to_binary(unsigned char byte, char * binary_str);
static void bytes_to_binary_string(const unsigned char * bytes,
		size_t len, char * binary_str, bool trim);
static char * transform_data_expression(const char * remoteObjid, const char * colname);
static void populate_primary_keys(StringInfoData * strinfo, const char * id,
		const char * jsonin, bool alter, bool isinline);
static void transformDDLColumns(const char * id, DBZ_DDL_COLUMN * col,
		ConnectorType conntype, bool datatypeonly, StringInfoData * strinfo,
		PG_DDL_COLUMN * pgcol);
static char * composeAlterColumnClauses(const char * objid, ConnectorType type,
		List * dbzcols, TupleDesc tupdesc, Relation rel, PG_DDL * pgddl);
static void expand_struct_value(char *in, DBZ_DML_COLUMN_VALUE *colval);
static char * handle_base64_to_numeric_with_scale(const char * in, int scale);
static char * handle_string_to_numeric(const char * in, bool addquote);
static char * handle_base64_to_bit(const char * in, bool addquote, int typemod, bool padzero);
static char * handle_string_to_bit(const char * in, bool addquote);
static char * handle_numeric_to_bit(const char * in, bool addquote);
static char * construct_datestr(long long input, bool addquote, int timerep);
static char * handle_base64_to_date(const char * in, bool addquote, int timerep);
static char * handle_numeric_to_date(const char * in, bool addquote, int timerep);
static char * handle_string_to_date(const char * in, bool addquote);
static char * construct_timestampstr(long long input, bool addquote, int timerep, int typemod);
static char * handle_base64_to_timestamp(const char * in, bool addquote, int timerep, int typemod);
static char * handle_numeric_to_timestamp(const char * in, bool addquote, int timerep, int typemod);
static char * handle_string_to_timestamp(const char * in, bool addquote, ConnectorType type);
static char * construct_timetr(long long input, bool addquote, int timerep, int typemod);
static char * handle_base64_to_time(const char * in, bool addquote, int timerep, int typemod);
static char * handle_numeric_to_time(const char * in, bool addquote, int timerep, int typemod);
static char * handle_string_to_time(const char * in, bool addquote);
static char * handle_base64_to_byte(const char * in, bool addquote);
static char * handle_string_to_byte(const char * in, bool addquote);
static char * handle_numeric_to_byte(const char * in, bool addquote);
static char * construct_intervalstr(long long input, bool addquote, int timerep, int typemod);
static char * handle_base64_to_interval(const char * in, bool addquote, int timerep, int typemod);
static char * handle_numeric_to_interval(const char * in, bool addquote, int timerep, int typemod);
static char * handle_string_to_interval(const char * in, bool addquote);
static char * handle_data_by_type_category(char * in, DBZ_DML_COLUMN_VALUE * colval,
		ConnectorType conntype, bool addquote);
static char * processDataByType(DBZ_DML_COLUMN_VALUE * colval, bool addquote,
		const char * remoteObjectId, ConnectorType type);

/*
 * remove_precision
 *
 * this helper function removes precision parameters enclosed in () from the input string
 */
static void
remove_precision(char * str, bool * removed) {
	char *openParen = strchr(str, '(');
	char *closeParen;

	if (removed == NULL)
		return;

	while (openParen != NULL)
	{
		closeParen = strchr(openParen, ')');
		if (closeParen != NULL)
		{
			memmove(openParen, closeParen + 1, strlen(closeParen));
			*removed = true;
		}
		else
			break;
		openParen = strchr(openParen, '(');
	}
}

/*
 * count_active_columns
 *
 * this helper function counts the number of active (not dropped) columns from given tupdesc
 */
static int
count_active_columns(TupleDesc tupdesc)
{
	int count = 0, i = 0;
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (!attr->attisdropped)
			count++;
	}
	return count;
}

/*
 * derive_value_from_byte
 *
 * Decodes a signed, big-endian integer and validates its width.
 */
static long long
derive_value_from_byte(const unsigned char * bytes, int len)
{
	return fc_value_int64_from_bytes(bytes, len);
}



/*
 * reverse_byte_array
 *
 * reverse the given byte array
 */
static void
reverse_byte_array(unsigned char * array, int length)
{
	size_t start = 0;
	size_t end = length - 1;
	while (start < end)
	{
		unsigned char temp = array[start];
		array[start] = array[end];
		array[end] = temp;
		start++;
		end--;
	}
}

/*
 * trim_leading_zeros
 *
 * trim the leading zeros from the given string
 */
static void
trim_leading_zeros(char *str)
{
	int i = 0, j = 0;
	while (str[i] == '0')
	{
		i++;
	}

	if (str[i] == '\0')
	{
		str[0] = '0';
		str[1] = '\0';
		return;
	}

	while (str[i] != '\0')
	{
		str[j++] = str[i++];
	}
	str[j] = '\0';
}

/*
 * byte_to_binary
 *
 * convert the given byte to a binary string with 1s and 0s
 */
static void
byte_to_binary(unsigned char byte, char * binary_str)
{
	for (int i = 7; i >= 0; i--)
	{
		binary_str[7 - i] = (byte & (1 << i)) ? '1' : '0';
	}
	binary_str[8] = '\0';
}

/*
 * bytes_to_binary_string
 *
 * convert the given bytes to a binary string with 1s and 0s
 */
static void
bytes_to_binary_string(const unsigned char * bytes, size_t len, char * binary_str, bool trim)
{
	char byte_str[9];
	size_t i = 0;
	binary_str[0] = '\0';

	for (i = 0; i < len; i++)
	{
		byte_to_binary(bytes[i], byte_str);
		strcat(binary_str, byte_str);
	}

	if (trim)
		trim_leading_zeros(binary_str);
}

/*
 * transform_data_expression
 *
 * return the expression to run on the given column name based on the transform
 * object rule definitions
 */
static char *
transform_data_expression(const char *remote_object, const char *column)
{
 const char *expression;
 char *source;
 char *result;

 if (!remote_object || !column)
  return NULL;
 source = synchdb_source_column_key(remote_object, column);
 expression = synchdb_rule_transform(synchdb_rule_snapshot_current(), source);
 result = expression ? pstrdup(expression) : NULL;
 pfree(source);
 return result;
}


/*
 * populate_primary_keys
 *
 * this function constructs primary key clauses based on jsonin. jsonin
 * is expected to be a json array with string element, for example:
 * ["col1","col2"]
 */
static void
populate_primary_keys(StringInfoData * strinfo, const char * id, const char * jsonin, bool alter, bool isinline)
{
	Datum jsonb_datum;
	Jsonb * jb;
	JsonbIterator *it;
	JsonbIteratorToken r;
	JsonbValue v;
	char * value = NULL;
	volatile bool isfirst = true;

	if (!jsonin)
	{
		elog(DEBUG1, "no valid primary key json");
		return;
	}

	/* Convert event string to JSONB */
	PG_TRY();
	{
		jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(jsonin));
		jb = DatumGetJsonbP(jsonb_datum);
	}
	PG_CATCH();
	{
		FlushErrorState();
		elog(WARNING, "bad primary key json message: %s", jsonin);
		return;
	}
	PG_END_TRY();

	it = JsonbIteratorInit(&jb->root);
	while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		switch (r)
		{
			case WJB_BEGIN_ARRAY:
				break;
			case WJB_END_ARRAY:
				/*
				 * if at least one primary key is appended, we need to remove the last comma
				 * and close parenthesis
				 */
				if (!isfirst)
				{
					strinfo->data[strinfo->len - 1] = '\0';
					strinfo->len = strinfo->len - 1;
					appendStringInfo(strinfo, ")");
				}
				break;
			case WJB_VALUE:
			case WJB_ELEM:
			{
				switch(v.type)
				{
					case jbvString:
					{
						char * mappedColumnName = NULL;
						StringInfoData colNameObjId;

						value = pnstrdup(v.val.string.val, v.val.string.len);
						elog(DEBUG1, "primary key column: %s", value);

						/* transform the column name if needed */
						initStringInfo(&colNameObjId);

						/* express a column name in fully qualified id */
						appendStringInfo(&colNameObjId, "%s.%s", id, value);
						mappedColumnName = transform_object_name(colNameObjId.data, "column");
						if (mappedColumnName)
						{
							/* replace the column name with looked up value here */
							pfree(value);
							value = pstrdup(mappedColumnName);
						}

						if(colNameObjId.data)
							pfree(colNameObjId.data);

						/* normalize the name if needed */
						fc_normalize_name(synchdb_letter_casing_strategy, value, strlen(value));
						if (alter)
						{
							if (isinline)
							{
								if (isfirst)
								{
									appendStringInfo(strinfo, ", ADD PRIMARY KEY(");
									appendStringInfo(strinfo, "\"%s\",", value);
									isfirst = false;
								}
								else
								{
									appendStringInfo(strinfo, "\"%s\",", value);
								}
							}
							else
							{
								if (isfirst)
								{
									appendStringInfo(strinfo, "PRIMARY KEY(");
									appendStringInfo(strinfo, "\"%s\",", value);
									isfirst = false;
								}
								else
								{
									appendStringInfo(strinfo, "\"%s\",", value);
								}
							}
						}
						else
						{
							if (isfirst)
							{
								appendStringInfo(strinfo, ", PRIMARY KEY(");
								appendStringInfo(strinfo, "\"%s\",", value);
								isfirst = false;
							}
							else
							{
								appendStringInfo(strinfo, "\"%s\",", value);
							}
						}
						pfree(value);
						break;
					}
					case jbvNull:
					case jbvNumeric:
					case jbvBool:
					case jbvBinary:
					default:
						elog(ERROR, "Unknown or unexpected value type: %d while "
								"parsing primaryKeyColumnNames", v.type);
						break;
				}
				break;
			}
			case WJB_BEGIN_OBJECT:
			case WJB_END_OBJECT:
			case WJB_KEY:
			default:
			{
				elog(ERROR, "Unknown or unexpected token: %d while "
						"parsing primaryKeyColumnNames", r);
				break;
			}
		}
	}
}

/*
 * transformDDLColumns
 *
 * Function to transform DDL columns, strinfo and pgcol will be filled according to transformation rules
 */
static void
transformDDLColumns(const char *id, DBZ_DDL_COLUMN *col, ConnectorType type,
                    bool datatypeonly, StringInfoData *sql, PG_DDL_COLUMN *target)
{
 const SynchdbRuleTypeMapping *mapping;
 char *source = synchdb_source_column_key(id, col->name);
 char *mapped = transform_object_name(source, "column");
 char *lookup = col->typeName;
 bool removed = false;

 target->attname = mapped ? mapped : pstrdup(col->name);
 if (!mapped)
  fc_normalize_name(synchdb_letter_casing_strategy, target->attname, strlen(target->attname));
 if (type == TYPE_ORACLE || type == TYPE_OLR)
 {
  remove_precision(col->typeName, &removed);
  if ((!pg_strcasecmp(col->typeName, "interval day to second") && col->scale > 0) || removed)
  {
   col->length = col->scale;
   col->scale = 0;
  }
  if (!pg_strcasecmp(col->typeName, "number") && col->scale == 0)
   lookup = psprintf("%s(%d,0)", col->typeName, col->length);
 }
 else if ((type == TYPE_MYSQL || type == TYPE_SQLSERVER) && col->length == 1 &&
          !pg_strcasecmp(col->typeName, "bit"))
  lookup = psprintf("%s(1)", col->typeName);
 mapping = synchdb_rule_type(synchdb_rule_snapshot_current(), type, source,
                            lookup, col->autoIncremented);
 if (!mapping && lookup != col->typeName)
  mapping = synchdb_rule_type(synchdb_rule_snapshot_current(), type, source,
                             col->typeName, col->autoIncremented);
 target->atttype = pstrdup(mapping ? mapping->name : col->typeName);
 if (mapping && mapping->length != -1)
  col->length = mapping->length;
 if (datatypeonly)
  appendStringInfo(sql, " %s ", target->atttype);
 else
  appendStringInfo(sql, " %s %s ", quote_identifier(target->attname), target->atttype);
 /* The explicit-delta OLR adapter retains its established size conventions.
  * Full Debezium images use ddl_plan.c's typed target diff instead. */
 if (type == TYPE_SQLSERVER && col->scale > 0 &&
     (find_exact_string_match(target->atttype, "timestamp") ||
      find_exact_string_match(target->atttype, "time") ||
      find_exact_string_match(target->atttype, "timestamptz")))
  appendStringInfo(sql, "(%d) ", Min(col->scale, 6));
 if (lookup != col->typeName)
  pfree(lookup);
 pfree(source);
}

/*
 * composeAlterColumnClauses
 *
 * This functions build the ALTER COLUM SQL clauses based on given inputs
 */
static char *
composeAlterColumnClauses(const char * objid, ConnectorType type, List * dbzcols, TupleDesc tupdesc, Relation rel, PG_DDL * pgddl)
{
	ListCell * cell;
	int attnum = 1;
	StringInfoData colNameObjId;
	StringInfoData strinfo;
	char * mappedColumnName = NULL;
	char * ret = NULL;
	bool found = false;
	Bitmapset * pkattrs;
	bool atleastone = false;
	PG_DDL_COLUMN * pgcol = NULL;

	initStringInfo(&colNameObjId);
	initStringInfo(&strinfo);
	pkattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_PRIMARY_KEY);

	foreach(cell, dbzcols)
	{
		DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);
		pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));

		resetStringInfo(&colNameObjId);
		appendStringInfo(&colNameObjId, "%s.%s", objid, col->name);
		mappedColumnName = transform_object_name(colNameObjId.data, "column");

		/* use the name as it came if no column name mapping found */
		if (!mappedColumnName)
		{
			mappedColumnName = pstrdup(col->name);

			/* we want to normalize according to letter casing strategy here */
			fc_normalize_name(synchdb_letter_casing_strategy, mappedColumnName, strlen(mappedColumnName));
		}

		found = false;
		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			/* skip dropped columns */
			if (attr->attisdropped)
				continue;

			/* found a matching column, build the alter column clauses */
			if (!strcmp(mappedColumnName, NameStr(attr->attname)))
			{
				found = true;

				/* skip ALTER on primary key columns */
				if (pkattrs != NULL && bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber, pkattrs))
					continue;

				/* check data type */
				appendStringInfo(&strinfo, "ALTER COLUMN \"%s\" SET DATA TYPE",
						mappedColumnName);
				transformDDLColumns(objid, col, type, true, &strinfo, pgcol);
				if (col->length > 0 && col->scale > 0)
				{
					appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
				}

				/* if a only length if specified, add it. For example VARCHAR(30)*/
				if (col->length > 0 && col->scale == 0)
				{
					/* make sure it does not exceed postgresql allowed maximum */
					if (col->length > MaxAttrSize)
						col->length = MaxAttrSize;
					appendStringInfo(&strinfo, "(%d) ", col->length);
				}

				/*
				 * todo: for complex data type transformation, postgresql requires
				 * the user to specify a function to cast existing values to the new
				 * data type via the USING clause. This is needed for INT -> TEXT,
				 * INT -> DATE or NUMERIC -> VARCHAR. We do not support USING now as
				 * we do not know what function the user wants to use for casting the
				 * values. Perhaps we can include these cast functions in the rule file
				 * as well, but for now this is not supported and PostgreSQL may complain
				 * if we attempt to do complex data type changes.
				 */
				appendStringInfo(&strinfo, ", ");

				if (col->defaultValueExpression)
				{
					/*
					 * synchdb can receive a default expression not supported in postgresql.
					 * so for now, we always set to default null. todo
					 */
					appendStringInfo(&strinfo, "ALTER COLUMN \"%s\" SET DEFAULT %s",
							mappedColumnName, "NULL");
				}
				else
				{
					/* remove default value */
					appendStringInfo(&strinfo, "ALTER COLUMN \"%s\" DROP DEFAULT",
							mappedColumnName);
				}

				appendStringInfo(&strinfo, ", ");

				/* check if nullable or not nullable */
				if (!col->optional)
				{
					appendStringInfo(&strinfo, "ALTER COLUMN \"%s\" SET NOT NULL",
							mappedColumnName);
				}
				else
				{
					appendStringInfo(&strinfo, "ALTER COLUMN \"%s\" DROP NOT NULL",
							mappedColumnName);
				}
				appendStringInfo(&strinfo, ",");
				atleastone = true;

				pgcol->position = attnum;
			}
		}
		if (!found)
		{
			/*
			 * todo: support renamed columns? The challenge is to find out which column
			 * got renamed on remote site because dbz did not tell us the old column name
			 * that was renamed. Only the new name is given to us to figure it out :(
			 */
			elog(WARNING, "column %s missing in PostgreSQL, indicating a renamed column?! -"
					"Not supported now",
					mappedColumnName);
		}
		pgddl->columns = lappend(pgddl->columns, pgcol);
	}

	if (atleastone)
	{
		/* No clause means an empty buffer, with no trailing comma to remove. */
		strinfo.data[strinfo.len - 1] = '\0';
		strinfo.len--;
		ret = pstrdup(strinfo.data);
	}

	if(strinfo.data)
		pfree(strinfo.data);

	if (pkattrs != NULL)
		bms_free(pkattrs);

	/* no column altered, return null */
	if (!atleastone)
		return NULL;

	return ret;
}

/* Source logical schemas are checked before binding. Read typed members again
 * here so a legacy/internal caller cannot turn missing values into real data. */
static void
expand_struct_value(char *in, DBZ_DML_COLUMN_VALUE *colval)
{
	Jsonb *object = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(in)));
	JsonbValue first_buffer;
	JsonbValue second_buffer;
	JsonbValue *first;
	JsonbValue *second;
	char *replacement;

	if (!JB_ROOT_IS_OBJECT(object))
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("source logical value must be an object")));
	if (colval->timerep == DATA_VARIABLE_SCALE &&
		(colval->typcategory == TYPCATEGORY_NUMERIC ||
		 colval->typcategory == TYPCATEGORY_BOOLEAN))
	{
		Datum integral;

		first = getKeyJsonValueFromContainer(&object->root, "scale", 5, &first_buffer);
		second = getKeyJsonValueFromContainer(&object->root, "value", 5, &second_buffer);
		if (first == NULL || first->type != jbvNumeric ||
			second == NULL || second->type != jbvString || second->val.string.len == 0)
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("variable-scale decimal requires an integer scale and nonempty bytes value")));
		colval->scale = DatumGetInt32(DirectFunctionCall1(numeric_int4,
										NumericGetDatum(first->val.numeric)));
		integral = DirectFunctionCall1(int4_numeric, Int32GetDatum(colval->scale));
		if (!DatumGetBool(DirectFunctionCall2(numeric_eq,
									NumericGetDatum(first->val.numeric), integral)))
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("variable-scale decimal scale must be an exact integer")));
		replacement = pnstrdup(second->val.string.val, second->val.string.len);
	}
	else if (colval->timerep == DATA_POINT && colval->datatype == POINTOID)
	{
		char *x;
		char *y;

		first = getKeyJsonValueFromContainer(&object->root, "x", 1, &first_buffer);
		second = getKeyJsonValueFromContainer(&object->root, "y", 1, &second_buffer);
		if (first == NULL || second == NULL ||
			(first->type != jbvNumeric && first->type != jbvString) ||
			(second->type != jbvNumeric && second->type != jbvString))
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("point requires both coordinates")));
		x = first->type == jbvNumeric ?
			DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(first->val.numeric))) :
			pnstrdup(first->val.string.val, first->val.string.len);
		y = second->type == jbvNumeric ?
			DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(second->val.numeric))) :
			pnstrdup(second->val.string.val, second->val.string.len);
		replacement = psprintf("(%s,%s)", x, y);
		pfree(x);
		pfree(y);
	}
	else
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("source logical struct has no codec for this target type")));
	pfree(object);
	pfree(colval->value);
	colval->value = replacement;
}
static char *
handle_base64_to_numeric_with_scale(const char * in, int scale)
{
	return fc_value_decimal_from_base64(in, scale);
}

static char *
handle_string_to_numeric(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to numeric. May fail to apply
	 * if it contains non-numeric characters
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
handle_base64_to_bit(const char * in, bool addquote, int typemod, bool padzero)
{
	Size input_length = strlen(in);
	int capacity;
	int tmpoutlen;
	unsigned char *tmpout;
	char *out;
	Size bits;

	if (input_length == 0 || input_length > MaxAllocSize / 8)
		ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
						errmsg("invalid base64 bit string length")));
	capacity = pg_b64_dec_len((int) input_length);
	tmpout = palloc(capacity);
	tmpoutlen = synchdb_pg_base64_decode(in, (int) input_length, tmpout, capacity);
	if (tmpoutlen <= 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
						errmsg("invalid base64 bit string")));
	/* Allocate for the entire decoded value before considering target width.
	 * A narrower target must never turn padding into a negative write offset. */
	out = palloc((Size) tmpoutlen * 8 + 1);
	reverse_byte_array(tmpout, tmpoutlen);
	bytes_to_binary_string(tmpout, tmpoutlen, out, !padzero);
	pfree(tmpout);
	bits = strlen(out);
	if (padzero && typemod > 0 && bits > (Size) typemod)
	{
		Size excess = bits - typemod;
		Size index;

		/* Non-byte-aligned MySQL BIT widths have zero high padding bits. */
		for (index = 0; index < excess; index++)
			if (out[index] != '0')
				ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
								errmsg("bit string value exceeds target width %d", typemod)));
		memmove(out, out + excess, (Size) typemod + 1);
	}
	else if (padzero && typemod > 0 && bits < (Size) typemod)
	{
		Size zeros = (Size) typemod - bits;
		char *padded = palloc((Size) typemod + 1);

		memset(padded, '0', zeros);
		memcpy(padded + zeros, out, bits + 1);
		pfree(out);
		out = padded;
	}
	if (addquote)
	{
		char *quoted = quote_literal_cstr(out);

		pfree(out);
		out = quoted;
	}
	return out;
}

static char *
handle_string_to_bit(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to bit. May fail to apply
	 * if it contains non-bit characters
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
handle_numeric_to_bit(const char * in, bool addquote)
{
	/*
	 * no special handling to convert numeric to bit. May fail to apply
	 * if it contains non-bit numerics
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
construct_datestr(long long input, bool addquote, int timerep)
{
	SynchdbValueTimeUnit unit;

	switch (timerep)
	{
		case TIME_DATE: unit = SYNCHDB_VALUE_DAYS; break;
		case TIME_TIMESTAMP: unit = SYNCHDB_VALUE_MILLISECONDS; break;
		case TIME_MICROTIMESTAMP: unit = SYNCHDB_VALUE_MICROSECONDS; break;
		case TIME_NANOTIMESTAMP: unit = SYNCHDB_VALUE_NANOSECONDS; break;
		default: elog(ERROR, "unsupported date representation: %d", timerep);
	}
	return fc_value_date(input, unit, addquote);
}

static char *
handle_base64_to_date(const char * in, bool addquote, int timerep)
{
	long long input = 0;
	int tmpoutlen = pg_b64_dec_len(strlen(in));
	unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen + 1);

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
	tmpoutlen = pg_b64_decode(in, strlen(in), tmpout, tmpoutlen);
#else
	tmpoutlen = pg_b64_decode(in, strlen(in), (char *)tmpout, tmpoutlen);
#endif
	input = derive_value_from_byte(tmpout, tmpoutlen);
	return construct_datestr(input, addquote, timerep);
}

static char *
handle_numeric_to_date(const char * in, bool addquote, int timerep)
{
	int64 input = pg_strtoint64(in);

	return construct_datestr(input, addquote, timerep);
}

static char *
handle_string_to_date(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to date. May fail to apply
	 * if it contains unsupported date format
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
construct_timestampstr(long long input, bool addquote, int timerep, int typemod)
{
	SynchdbValueTimeUnit unit;

	switch (timerep)
	{
		case TIME_TIMESTAMP: unit = SYNCHDB_VALUE_MILLISECONDS; break;
		case TIME_MICROTIMESTAMP: unit = SYNCHDB_VALUE_MICROSECONDS; break;
		case TIME_NANOTIMESTAMP: unit = SYNCHDB_VALUE_NANOSECONDS; break;
		default: elog(ERROR, "unsupported timestamp representation: %d", timerep);
	}
	return fc_value_timestamp(input, unit, typemod, addquote);
}

static char *
handle_base64_to_timestamp(const char * in, bool addquote, int timerep, int typemod)
{
	long long input = 0;
	int tmpoutlen = pg_b64_dec_len(strlen(in));
	unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen + 1);

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
	tmpoutlen = pg_b64_decode(in, strlen(in), tmpout, tmpoutlen);
#else
	tmpoutlen = pg_b64_decode(in, strlen(in), (char *)tmpout, tmpoutlen);
#endif
	input = derive_value_from_byte(tmpout, tmpoutlen);
	return construct_timestampstr(input, addquote, timerep, typemod);
}

static char *
handle_numeric_to_timestamp(const char * in, bool addquote, int timerep, int typemod)
{
	int64 input = pg_strtoint64(in);

	return construct_timestampstr(input, addquote, timerep, typemod);
}

static char *
handle_string_to_timestamp(const char * in, bool addquote, ConnectorType type)
{
    char * out = NULL;

    if (type == TYPE_OLR)
    {
        /*
         * openlog replicator expressed timestamp with time zone as a string
         * that looks like:
         *
         * 		"1706725800000000000,-08:00"
         *
         * this function will treat the first part of the string as nanosecond
         * since epoch, convert it to timestamp string and append the second
         * part (timezone) to the end of it.
         */
		long long ns;
		char tz[SYNCHDB_MAX_TZ_LEN];
		time_t seconds = 0;
		struct tm t;
		int offset_hours, offset_minutes;
		char sign;

		if (sscanf(in, "%lld,%15s", &ns, tz) != 2)
		{
			elog(WARNING, "invalid OLR timestamp string %s", in);
			return NULL;
		}

		if (sscanf(tz, "%c%2d:%2d", (char *)&sign, &offset_hours, &offset_minutes) != 3)
		{
			elog(WARNING, "invalid OLR timezone string %s", tz);
			return NULL;
		}

		if (sign != '+' && sign != '-')
		{
			elog(WARNING, "invalid timezone sign '%c'", sign);
			return NULL;
		}

		seconds = ns / 1000000000LL;

	    /* apply timezone offset to get local time */
		seconds = sign == '-' ?
				(seconds - (offset_hours * 3600 + offset_minutes * 60)) :
				(seconds + (offset_hours * 3600 + offset_minutes * 60));

		gmtime_r(&seconds, &t);

		out = palloc0(SYNCHDB_MAX_TIMESTAMP_LEN + 1);
		strftime(out, SYNCHDB_MAX_TIMESTAMP_LEN, "%Y-%m-%d %H:%M:%S", &t);
		strcat(out, tz);
    }
    else
    {
        size_t len = strlen(in);
        bool add_tz = (len > 0 && in[len - 1] == 'Z') ? true : false;
        size_t new_len = len + (add_tz ? 5 : 0);
        char * t = NULL;

        /* todo: we assume input is already in correct timestamp format.
         * We may need to check that is true prior to processing
         */
        if (addquote)
        {
            out = (char *) palloc0(new_len + 2 + 1);
            out[0] = '\'';
            strcpy(&out[1], in);
            t = strchr(out, 'T');
            if (t)
                *t = ' ';

            if (add_tz)
                strcpy(&out[len], "+00:00");
        }
        else
        {
            out = (char *) palloc0(new_len + 1);
            strcpy(out, in);
            t = strchr(out, 'T');
            if (t)
                *t = ' ';

            if (add_tz)
                strcpy(&out[len - 1], "+00:00");
        }
    }

    return out;
}

static char *
construct_timetr(long long input, bool addquote, int timerep, int typemod)
{
	SynchdbValueTimeUnit unit;

	switch (timerep)
	{
		case TIME_TIME: unit = SYNCHDB_VALUE_MILLISECONDS; break;
		case TIME_MICROTIME: unit = SYNCHDB_VALUE_MICROSECONDS; break;
		case TIME_NANOTIME: unit = SYNCHDB_VALUE_NANOSECONDS; break;
		default: elog(ERROR, "unsupported time representation: %d", timerep);
	}
	return fc_value_time(input, unit, typemod, addquote);
}

static char *
handle_base64_to_time(const char * in, bool addquote, int timerep, int typemod)
{
	long long input = 0;
	int tmpoutlen = pg_b64_dec_len(strlen(in));
	unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen + 1);

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
	tmpoutlen = pg_b64_decode(in, strlen(in), tmpout, tmpoutlen);
#else
	tmpoutlen = pg_b64_decode(in, strlen(in), (char *)tmpout, tmpoutlen);
#endif
	input = derive_value_from_byte(tmpout, tmpoutlen);
	return construct_timetr(input, addquote, timerep, typemod);
}

static char *
handle_numeric_to_time(const char * in, bool addquote, int timerep, int typemod)
{
	int64 input = pg_strtoint64(in);

	return construct_timetr(input, addquote, timerep, typemod);
}

static char *
handle_string_to_time(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to time. May fail to apply
	 * if it contains non-time characters
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
handle_base64_to_byte(const char *in, bool addquote)
{
	int length = strlen(in);
	int capacity = pg_b64_dec_len(length);
	unsigned char *bytes = palloc(capacity > 0 ? capacity : 1);
	int decoded;
	char *text;
	char *result;

	decoded = synchdb_pg_base64_decode(in, length, bytes, capacity);
	if (decoded < 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
						errmsg("invalid base64 bytea value")));
	if ((Size) decoded > (MaxAllocSize - 3) / 2)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("bytea input is too large")));
	text = palloc((Size) decoded * 2 + 3);
	text[0] = '\\';
	text[1] = 'x';
	hex_encode((const char *) bytes, decoded, text + 2);
	text[(Size) decoded * 2 + 2] = '\0';
	result = addquote ? quote_literal_cstr(text) : text;
	if (addquote)
		pfree(text);
	pfree(bytes);
	return result;
}

static char *
handle_string_to_byte(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to byte. May fail to apply
	 * if it contains non-byte characters
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
handle_numeric_to_byte(const char * in, bool addquote)
{
	/*
	 * no special handling to convert numeric to byte. May fail to apply
	 * if it contains non-byte numerics
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
construct_intervalstr(long long input, bool addquote, int timerep, int typemod)
{
	time_t seconds = 0, remains = 0;
	char inter[64 + 1] = {0};
	int fields;
	char * out = NULL;

	switch (timerep)
	{
		case TIME_MICRODURATION:
		{
			/* interval in microseconds - convert to seconds */
			seconds = (time_t)(input / 1000 / 1000);
			remains = input % 1000000;
			break;
		}
		default:
		{
			/* todo: intelligently figure out the right timerep based on input */
			set_shm_connector_errmsg(myConnectorId, "no interval representation available to"
					"process INTERVALOID value");
			elog(ERROR, "no interval representation available to process INTERVALOID value");
		}
	}

	/* we need to figure out interval's range and precision from colval->typemod */
	fields = INTERVAL_RANGE(typemod);
	switch (fields)
	{
		case INTERVAL_MASK(YEAR):	/* year */
			snprintf(inter, sizeof(inter), "%d years",
					(int) seconds / SECS_PER_YEAR);
			break;
		case INTERVAL_MASK(MONTH):	/* month */
			snprintf(inter, sizeof(inter), "%d months",
					(int) seconds / (SECS_PER_DAY * DAYS_PER_MONTH));
			break;
		case INTERVAL_MASK(DAY):	/* day */
			snprintf(inter, sizeof(inter), "%d days",
					(int) seconds / SECS_PER_DAY);
			break;
		case INTERVAL_MASK(HOUR):	/* hour */
			snprintf(inter, sizeof(inter), "%d hours",
					(int) seconds / SECS_PER_HOUR);
			break;
		case INTERVAL_MASK(MINUTE):	/* minute */
			snprintf(inter, sizeof(inter), "%d minutes",
					(int) seconds / SECS_PER_MINUTE);
			break;
		case INTERVAL_MASK(SECOND):	/* second */
			snprintf(inter, sizeof(inter), "%d seconds",
					(int) seconds);
			break;
		case INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH):	/* year to month */
			snprintf(inter, sizeof(inter), "%d years %d months",
					(int) seconds / SECS_PER_YEAR,
					(int) (seconds / (SECS_PER_DAY * DAYS_PER_MONTH)) % MONTHS_PER_YEAR);
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR):		/* day to hour */
			snprintf(inter, sizeof(inter), "%d days %d hours",
					(int) seconds / SECS_PER_DAY,
					(int) (seconds / SECS_PER_HOUR) % HOURS_PER_DAY);
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):	/* day to minute */
			snprintf(inter, sizeof(inter), "%d days %02d:%02d",
					(int) seconds / SECS_PER_DAY,
					(int) (seconds / SECS_PER_HOUR) % HOURS_PER_DAY,
					(int) (seconds / SECS_PER_MINUTE) % MINS_PER_HOUR);
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):	/* day to second */
			snprintf(inter, sizeof(inter), "%d days %02d:%02d:%02d.%06d",
					(int) seconds / SECS_PER_DAY,
					(int) (seconds / SECS_PER_HOUR) % HOURS_PER_DAY,
					(int) (seconds / SECS_PER_MINUTE) % MINS_PER_HOUR,
					(int) (seconds % SECS_PER_MINUTE) % SECS_PER_MINUTE,
					(int) remains);
			break;
		case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):	/* hour to minute */
			snprintf(inter, sizeof(inter), "%02d:%02d",
					(int) seconds / SECS_PER_HOUR,
					(int) (seconds / SECS_PER_MINUTE) % MINS_PER_HOUR);
			break;
		case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):	/* hour to second */
			snprintf(inter, sizeof(inter), "%02d:%02d:%02d.%06d",
					(int) (seconds / SECS_PER_HOUR),
					(int) (seconds / SECS_PER_MINUTE) % MINS_PER_HOUR,
					(int) (seconds % SECS_PER_MINUTE) % SECS_PER_MINUTE,
					(int) remains);
			break;
		case INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):	/* minute to second */
			snprintf(inter, sizeof(inter), "%02d:%02d.%06d",
					(int) seconds / SECS_PER_MINUTE,
					(int) (seconds % SECS_PER_MINUTE) % SECS_PER_MINUTE,
					(int) remains);
			break;
		case INTERVAL_FULL_RANGE:	/* full range */
			snprintf(inter, sizeof(inter), "%d years %d months %d days %02d:%02d:%02d.%06d",
					(int) seconds / SECS_PER_YEAR,
					(int) (seconds / (SECS_PER_DAY * DAYS_PER_MONTH)) % MONTHS_PER_YEAR,
					(int) (seconds / SECS_PER_DAY) % (SECS_PER_DAY * DAYS_PER_MONTH),
					(int) (seconds / SECS_PER_HOUR) % HOURS_PER_DAY,
					(int) (seconds / SECS_PER_MINUTE) % MINS_PER_HOUR,
					(int) (seconds % SECS_PER_MINUTE) % SECS_PER_MINUTE,
					(int) remains);
			break;
		default:
		{
			set_shm_connector_errmsg(myConnectorId, "invalid INTERVAL typmod");
			elog(ERROR, "invalid INTERVAL typmod: 0x%x", typemod);
			break;
		}
	}
	if (addquote)
	{
		out = escapeSingleQuote(inter, addquote);
	}
	else
	{
		out = (char *) palloc0(strlen(inter) + 1);
		memcpy(out, inter, strlen(inter));
	}
	return out;
}

static char *
handle_base64_to_interval(const char * in, bool addquote, int timerep, int typemod)
{
	int tmpoutlen = pg_b64_dec_len(strlen(in));
	unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen);
	long long input = 0;

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
	tmpoutlen = pg_b64_decode(in, strlen(in), tmpout, tmpoutlen);
#else
	tmpoutlen = pg_b64_decode(in, strlen(in), (char *)tmpout, tmpoutlen);
#endif
	input = derive_value_from_byte(tmpout, tmpoutlen);
	return construct_intervalstr(input, addquote, timerep, typemod);
}

static char *
handle_numeric_to_interval(const char * in, bool addquote, int timerep, int typemod)
{
	long long input = atoll(in);

	return construct_intervalstr(input, addquote, timerep, typemod);
}

static char *
handle_string_to_interval(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to interval. May fail to apply
	 * if it contains incorrect interval format
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
handle_data_by_type_category(char * in, DBZ_DML_COLUMN_VALUE * colval, ConnectorType conntype, bool addquote)
{
	char * out = NULL;
	elog(DEBUG1, "%s: col %s timerep %d dbztype %d category %c typname %s",__FUNCTION__,
			colval->name, colval->timerep, colval->dbztype, colval->typcategory, colval->typname);
	switch (colval->typcategory)
	{
		case TYPCATEGORY_BOOLEAN:
		case TYPCATEGORY_NUMERIC:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					out = handle_base64_to_numeric_with_scale(colval->value, colval->scale);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_numeric_with_scale(in, colval->scale);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				case DBZTYPE_STRING:
				{
					/* no special handling for now. todo */
					out = pstrdup(in);
					break;
				}
				default:
				{
					out = pstrdup(in);
					break;
				}
			}
			break;
		}
		case TYPCATEGORY_DATETIME:
		{
			/*
			 * DATE, TIME or IMESTAMP, we cannot possibly know at this point, so we assume the
			 * type name would contain descriptive words to help us identify. It may not be 100%
			 * accurate, so it is best to define a transform function after this stage to
			 * correctly process it. todo: we can expose a new GUC to allow user to configure
			 * non-native data type and tell synchdb how to process them
			 */
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					if (strstr(colval->typname, "date"))
						out = handle_base64_to_date(colval->value, addquote, colval->timerep);
					else if (strstr(colval->typname, "timestamp"))
						out = handle_base64_to_timestamp(colval->value, addquote, colval->timerep, colval->typemod);
					else
						out = handle_base64_to_time(colval->value, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					if (strstr(colval->typname, "date"))
						out = handle_base64_to_date(in, addquote, colval->timerep);
					else if (strstr(colval->typname, "timestamp"))
						out = handle_base64_to_timestamp(in, addquote, colval->timerep, colval->typemod);
					else
						out = handle_base64_to_time(in, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					if (strstr(colval->typname, "timestamp") || strstr(colval->typname, "timetz"))
					{
						out = handle_string_to_timestamp(in, addquote, conntype);
					}
					else
					{
						if (addquote)
							out = escapeSingleQuote(in, addquote);
						else
							out = pstrdup(in);
					}
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:
				{
					/* OLR represents date as nanoseconds since epoch*/
					if (strstr(colval->typname, "date"))
						out = handle_numeric_to_date(in, addquote, TIME_NANOTIMESTAMP);
					else if (strstr(colval->typname, "timestamp"))
						out = handle_numeric_to_timestamp(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					else
						out = handle_numeric_to_time(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					break;
				}
#endif
				default:
				{
					if (strstr(colval->typname, "date"))
						out = handle_numeric_to_date(in, addquote, colval->timerep);
					else if (strstr(colval->typname, "timestamp"))
						out = handle_numeric_to_timestamp(in, addquote, colval->timerep, colval->typemod);
					else
						out = handle_numeric_to_time(in, addquote, colval->timerep, colval->typemod);
					break;
				}
			}
			break;
		}
		case TYPCATEGORY_BITSTRING:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					out = handle_base64_to_bit(colval->value, addquote, colval->typemod,
							conntype == TYPE_MYSQL ? true : false);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_bit(in, addquote, colval->typemod,
							conntype == TYPE_MYSQL ? true : false);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				default:
				{
					if (addquote)
						out = escapeSingleQuote(in, addquote);
					else
						out = pstrdup(in);
					break;
				}
			}
			break;
		}
		case TYPCATEGORY_TIMESPAN:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					out = handle_base64_to_interval(colval->value, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_interval(in, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					if (addquote)
						out = escapeSingleQuote(in, addquote);
					else
						out = pstrdup(in);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:
				{
					/* oracle number represented as interval - usd nanoseconds */
					out = handle_numeric_to_interval(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					break;
				}
#endif
				default:
				{
					out = handle_numeric_to_interval(in, addquote, colval->timerep, colval->typemod);
					break;
				}
			}
			break;
		}
		case TYPCATEGORY_USER:
		case TYPCATEGORY_ENUM:
		case TYPCATEGORY_GEOMETRIC:
		case TYPCATEGORY_STRING:
		default:
		{
			/* todo */
			elog(DEBUG1, "no special handling for category %c", colval->typcategory);
			if (addquote)
			{
				out = escapeSingleQuote(in, addquote);
			}
			else
			{
				out = pstrdup(in);
			}
			break;
		}
	}
	return out;
}

/*
 * processDataByType
 *
 * this function performs necessary data conversions to convert input data
 * as string and output a processed string based on type
 */
static char *
processDataByType(DBZ_DML_COLUMN_VALUE * colval, bool addquote, const char * remoteObjectId, ConnectorType type)
{
	char * out = NULL;
	char * in = colval->value;
	char * transformExpression = NULL;
	DBZ_DML_COLUMN_VALUE converted;
	bool structured = colval->dbztype == DBZTYPE_STRUCT;
	bool base_type_changed;
	Jsonb *spatial = NULL;

	elog(DEBUG1, "%s: col %s typoid %d timerep %d dbztype %d category %c",__FUNCTION__,
			colval->name, colval->datatype, colval->timerep, colval->dbztype, colval->typcategory);

	if (colval->is_null)
	{
		elog(DEBUG1,"NULL input value, returning NULL");
		return NULL;
	}
	if (in == NULL)
		elog(ERROR, "non-null source value has no input text");
	/* The source codec follows the domain's base representation; final input
	 * still uses the original target domain and enforces all its constraints. */
	converted = *colval;
	converted.datatype = getBaseType(colval->datatype);
	base_type_changed = converted.datatype != colval->datatype;
	if (base_type_changed)
	{
		get_type_category_preferred(converted.datatype, &converted.typcategory,
								   &converted.typispreferred);
		converted.typname = format_type_be(converted.datatype);
	}
	/* Struct expansion owns its replacement, never the bound source image. */
	if (structured)
	{
		converted.value = pstrdup(in);
		in = converted.value;
	}
	colval = &converted;
	transformExpression = transform_data_expression(remoteObjectId, colval->remoteColumnName);
	if (transformExpression && structured &&
		(colval->timerep == DATA_GEOMETRY || colval->timerep == DATA_POINT))
		spatial = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(in)));

	if (colval->dbztype == DBZTYPE_ARRAY_TEXT)
		out = addquote ? escapeSingleQuote(in, true) : pstrdup(in);
	else switch(colval->datatype)
	{
		case BOOLOID:
		case INT8OID:
		case INT2OID:
		case INT4OID:
		case FLOAT8OID:
		case FLOAT4OID:
		case NUMERICOID:
		case MONEYOID:
		{
			/* special handling for MONEYOID to use scale 4 to account for cents */
			if (colval->datatype == MONEYOID && type != TYPE_POSTGRES)
				colval->scale = 4;

			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					out = handle_base64_to_numeric_with_scale(colval->value, colval->scale);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_numeric_with_scale(in, colval->scale);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_numeric(in, addquote);
					break;
				}
				default:
				{
					/* numeric to numeric, just use as is */
					out = pstrdup(in);
					break;
				}
			}
			break;
		}
		case BPCHAROID:
		case TEXTOID:
		case VARCHAROID:
		case CSTRINGOID:
		case JSONBOID:
		case UUIDOID:
		case INETOID:
		case CIDROID:
		case MACADDROID:
		case MACADDR8OID:
		case INT4RANGEOID:
		case INT8RANGEOID:
		case NUMRANGEOID:
		case DATERANGEOID:
		case TSRANGEOID:
		case TSTZRANGEOID:
		{
			if (addquote)
				out = escapeSingleQuote(in, addquote);
			else
				out = pstrdup(in);
			break;
		}
		case VARBITOID:
		case BITOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					out = handle_base64_to_bit(colval->value, addquote, colval->typemod,
							type == TYPE_MYSQL ? true : false);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_bit(in, addquote, colval->typemod,
							type == TYPE_MYSQL ? true : false);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_bit(in, addquote);
					break;
				}
				default:
				{
					out = handle_numeric_to_bit(in, addquote);
					break;
				}
			}
			break;
		}
		case DATEOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					out = handle_base64_to_date(colval->value, addquote, colval->timerep);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_date(in, addquote, colval->timerep);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_date(in, addquote);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:
				{
					out = handle_numeric_to_date(in, addquote, TIME_NANOTIMESTAMP);
					break;
				}
#endif
				default:
				{
					out = handle_numeric_to_date(in, addquote, colval->timerep);
					break;
				}
			}
			break;
		}
		case TIMESTAMPTZOID:
		case TIMESTAMPOID:
		case TIMETZOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					out = handle_base64_to_timestamp(colval->value, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_timestamp(in, addquote, colval->timerep,  colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_timestamp(in, addquote, type);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:

				{
					/* OLR represents date as nanoseconds since epoch*/
					out = handle_numeric_to_timestamp(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					break;
				}
#endif
				default:
				{
					out = handle_numeric_to_timestamp(in, addquote, colval->timerep, colval->typemod);
					break;
				}
			}
			break;
		}
		case TIMEOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					out = handle_base64_to_time(colval->value, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_time(in, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_time(in, addquote);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:
				{
					out = handle_numeric_to_time(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					break;
				}
#endif
				default:
				{
					out = handle_numeric_to_time(in, addquote, colval->timerep, colval->typemod);
					break;
				}
			}
			break;
		}
		case BYTEAOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					out = handle_base64_to_byte(colval->value, addquote);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_byte(in, addquote);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_byte(in, addquote);
					break;
				}
				default:
				{
					out = handle_numeric_to_byte(in, addquote);
					break;
				}
			}
			break;
		}
		case INTERVALOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					out = handle_base64_to_interval(colval->value, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_interval(in, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_interval(in, addquote);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:
				{
					/* oracle number represented as interval - usd nanoseconds */
					out = handle_numeric_to_interval(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					break;
				}
#endif
				default:
				{
					out = handle_numeric_to_interval(in, addquote, colval->timerep, colval->typemod);
					break;
				}
			}
			break;
		}
		case POINTOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval);
					out = pstrdup(colval->value);
					break;
				}
				case DBZTYPE_BYTES:
				{
					/* xxx: binary to point */
					elog(ERROR," binary to point not supported");
					break;
				}
				case DBZTYPE_STRING:
				{
					out = pstrdup(colval->value);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_STRING:
				case OLRTYPE_NUMBER:
#endif
				default:
				{
					elog(ERROR, "cannot convert source type %d to POINT type", colval->dbztype);
					break;
				}
			}
			break;
		}
		default:
		{
			/*
			 * if this column data type does not fall in the cases above, then we will try
			 * to process it based on its type category.
			 */
			out = handle_data_by_type_category(in, colval, type, addquote);
			break;
		}
	}

	if (out == NULL)
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("source value conversion did not produce target input")));
	/*
	 * after the data is prepared, we need to check if we need to transform the data
	 * with a user-defined expression by looking up against transformExpressionHash.
	 * Note, we have to use colval->remoteColumnName to look up because colval->name
	 * may have been transformed to something else.
	 */
	if (transformExpression)
	{
		char *wkb = NULL;
		char *srid = NULL;
		char *transformed;

		/* Geometry parameters come only from declared spatial structs. A JSON
		 * object or text containing "wkb" is ordinary user data. */
		if (spatial)
		{
			JsonbValue buffer;
			JsonbValue *member;

			member = getKeyJsonValueFromContainer(&spatial->root, "wkb", 3, &buffer);
			if (member && member->type != jbvNull)
			{
				if (member->type != jbvString)
					ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
									errmsg("spatial wkb must be bytes or null")));
				wkb = pnstrdup(member->val.string.val, member->val.string.len);
			}
			member = getKeyJsonValueFromContainer(&spatial->root, "srid", 4, &buffer);
			if (member && member->type != jbvNull)
			{
				if (member->type != jbvNumeric)
					ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
									errmsg("spatial srid must be an integer or null")));
				srid = DatumGetCString(DirectFunctionCall1(int4out,
					DirectFunctionCall1(numeric_int4, NumericGetDatum(member->val.numeric))));
			}
		}
		transformed = ra_transformDataExpression(out, wkb, srid,
											transformExpression, colval->datatype);
		pfree(out);
		out = transformed;
		if (wkb)
			pfree(wkb);
		if (srid)
			pfree(srid);
		pfree(transformExpression);
	}
	if (spatial)
		pfree(spatial);
	if (structured)
		pfree(converted.value);
	if (base_type_changed)
		pfree(converted.typname);

	elog(DEBUG1, "data processing completed. Returning output: ");
	elog(DEBUG1, "%s", out);
	return out;
}

/*
 * list_sort_cmp
 *
 * helper function to compare 2 ListCells for sorting
 */
int
list_sort_cmp(const ListCell *a, const ListCell *b)
{
	DBZ_DML_COLUMN_VALUE * colvala = (DBZ_DML_COLUMN_VALUE *) lfirst(a);
	DBZ_DML_COLUMN_VALUE * colvalb = (DBZ_DML_COLUMN_VALUE *) lfirst(b);

	if (colvala->position < colvalb->position)
		return -1;
	if (colvala->position > colvalb->position)
		return 1;
	return 0;
}

/*
 * convert2PGDML
 *
 * this function converts  DBZ_DML to PG_DML strucutre
 */
/* Perform final target input once, after any repair expression, for both Writers. */
char *
fc_convert_value_text(DBZ_DML_COLUMN_VALUE *source, ConnectorType type)
{
	return processDataByType(source, false, NULL, type);
}

SynchdbTargetColumn *
fc_convert_target_column(DBZ_DML_COLUMN_VALUE *source, const char *remote_object_id,
                         Relation relation, ConnectorType type)
{
	TupleDesc descriptor = RelationGetDescr(relation);
	Form_pg_attribute attribute;
	PG_DML_COLUMN_VALUE *target;
	Oid input_function;
	Oid input_parameter;
	char *input;

	if (source->position <= 0 || source->position > descriptor->natts)
		elog(ERROR, "invalid bound target column position: %d", source->position);
	attribute = TupleDescAttr(descriptor, source->position - 1);
	if (attribute->attisdropped || source->datatype != attribute->atttypid ||
		source->typemod != attribute->atttypmod ||
		strcmp(source->name, NameStr(attribute->attname)) != 0)
		ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
						errmsg("target column changed after row binding")));
	if (attribute->attgenerated || attribute->attidentity == ATTRIBUTE_IDENTITY_ALWAYS)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("explicit values for generated target columns are not supported")));
	if (!source->is_null && source->typcategory == TYPCATEGORY_ARRAY &&
		source->dbztype != DBZTYPE_ARRAY_TEXT)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("source values require an explicit PostgreSQL array codec")));
	target = palloc0(sizeof(*target));
	target->name = pstrdup(NameStr(attribute->attname));
	target->datatype = attribute->atttypid;
	target->typmod = attribute->atttypmod;
	target->typbyval = get_typbyval(attribute->atttypid);
	target->position = attribute->attnum;
	input = processDataByType(source, false, remote_object_id, type);
	target->is_null = input == NULL;
	getTypeInputInfo(target->datatype, &input_function, &input_parameter);
	/* domain_in is non-strict: NULL still has to satisfy domain constraints. */
	target->value = OidInputFunctionCall(input_function, input,
									   input_parameter, target->typmod);
	/* A type input function may return expanded storage. Writers borrow a
	 * single owned chunk; never pfree an expanded object's interior pointer. */
	if (!target->is_null && attribute->attlen == -1 &&
		VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(target->value)))
		target->value = datumCopy(target->value, false, -1);
	if (input)
		pfree(input);
	return target;
}

static List *
convert_dml_image(List *source, DBZ_DML *row, Relation relation, ConnectorType type)
{
	List *target = NIL;
	ListCell *cell;
	Bitmapset *provided = NULL;

	foreach(cell, source)
	{
		DBZ_DML_COLUMN_VALUE *column = lfirst(cell);

		if (column->position <= 0 || column->position > row->natts ||
			bms_is_member(column->position, provided))
			ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
							errmsg("invalid or repeated target column in a row image")));
		provided = bms_add_member(provided, column->position);
		target = lappend(target, fc_convert_target_column(column, row->remoteObjectId, relation, type));
	}
	bms_free(provided);
	return target;
}

/* Convert exactly the selected identity attributes, independently of index order. */
static List *
convert_dml_identity(DBZ_DML *row, Relation relation, ConnectorType type,
					 const SynchdbRowIdentity *binding)
{
	const Bitmapset *keys = binding->attributes;
	List *source = row->columnValuesBefore;
	List *identity = NIL;
	int attribute = -1;

	if (source == NIL && row->op == 'u' && type == TYPE_POSTGRES &&
		binding->primary_equivalent)
		source = row->columnValuesAfter;
	while ((attribute = bms_next_member(keys, attribute)) >= 0)
	{
		int position = attribute + FirstLowInvalidHeapAttributeNumber;
		DBZ_DML_COLUMN_VALUE *key = NULL;
		ListCell *cell;
		PG_DML_COLUMN_VALUE *converted;

		foreach(cell, source)
		{
			DBZ_DML_COLUMN_VALUE *column = lfirst(cell);

			if (column->position == position)
			{
				if (key != NULL)
					elog(ERROR, "duplicate source identity column");
				key = column;
			}
		}
		if (key == NULL || key->is_null)
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							errmsg("source row does not provide the complete non-null target primary key")));
		converted = fc_convert_target_column(key, row->remoteObjectId, relation, type);
		if (converted->is_null)
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							errmsg("target identity transformation returned NULL")));
		identity = lappend(identity, converted);
	}
	if (identity == NIL)
		elog(ERROR, "target index has no identity columns");
	return identity;
}

/* The legacy OLR adapter also checks mapped permissions before value code. */
static List *
convert_dml_metadata(List *source, Relation relation, const Bitmapset *keys)
{
	List *columns = NIL;
	ListCell *cell;

	foreach(cell, source)
	{
		DBZ_DML_COLUMN_VALUE *value = lfirst(cell);
		SynchdbTargetColumn *column;

		if (value == NULL || value->position <= 0 ||
			value->position > RelationGetDescr(relation)->natts)
			ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						errmsg("invalid bound target column position")));
		if (keys != NULL && !bms_is_member(value->position - FirstLowInvalidHeapAttributeNumber, keys))
			continue;
		column = palloc0(sizeof(*column));
		column->name = value->name;
		column->datatype = value->datatype;
		column->typmod = value->typemod;
		column->typbyval = get_typbyval(value->datatype);
		column->position = value->position;
		columns = lappend(columns, column);
	}
	return columns;
}

PG_DML *
convert2PGDML(DBZ_DML *row, ConnectorType type)
{
	PG_DML *target = palloc0(sizeof(*target));
	Relation relation = table_open(row->tableoid, RowExclusiveLock);
	SynchdbRowIdentity *identity = NULL;

	synchdb_target_validate_relation(relation);

	if (relation->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("row writer requires an ordinary target table")));
	if (row->natts != RelationGetDescr(relation)->natts)
		ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
						errmsg("target table changed after row binding")));
	target->op = row->op;
	target->tableoid = row->tableoid;
	target->natts = row->natts;
	if (row->op == 'u' || row->op == 'd')
	{
		identity = synchdb_resolve_row_identity(relation);

		if (row->identity_index_oid != InvalidOid &&
			row->identity_index_oid != identity->index_oid)
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("target identity index changed after source row binding")));
		target->identity_index_oid = identity->index_oid;
		target->columnValuesBefore = convert_dml_metadata(
			row->columnValuesBefore == NIL && row->op == 'u' && type == TYPE_POSTGRES ?
			row->columnValuesAfter : row->columnValuesBefore, relation, identity->attributes);
	}
	if (row->op == 'c' || row->op == 'r' || row->op == 'u')
		target->columnValuesAfter = convert_dml_metadata(row->columnValuesAfter, relation, NULL);
	synchdb_target_validate_binding(relation, target);
	list_free_deep(target->columnValuesBefore);
	list_free_deep(target->columnValuesAfter);
	target->columnValuesBefore = NIL;
	target->columnValuesAfter = NIL;
	if (identity != NULL)
		target->columnValuesBefore = convert_dml_identity(row, relation, type, identity);
	if (row->op == 'c' || row->op == 'r' || row->op == 'u')
		target->columnValuesAfter = convert_dml_image(row->columnValuesAfter, row, relation, type);
	if (row->op != 'c' && row->op != 'r' && row->op != 'u' && row->op != 'd')
		elog(ERROR, "unsupported row operation: %c", row->op);
	if (row->op == 'u' && target->columnValuesAfter == NIL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("update does not provide any target fields")));
	synchdb_target_validate_change(relation, target);
	table_close(relation, NoLock);
	return target;
}

/*
 * fc_get_connector_type
 *
 * this function takes connector type in string and returns a corresponding enum
 */
ConnectorType
fc_get_connector_type(const char * connector)
{
	if (!strcasecmp(connector, "mysql"))
		return TYPE_MYSQL;
	else if (!strcasecmp(connector, "oracle"))
		return TYPE_ORACLE;
	else if (!strcasecmp(connector, "sqlserver"))
		return TYPE_SQLSERVER;
	else if (!strcasecmp(connector, "olr"))
		return TYPE_OLR;
	else if (!strcasecmp(connector, "postgres") ||
			 !strcasecmp(connector, "postgresql"))
		return TYPE_POSTGRES;
	else
	{
		elog(ERROR, "failed to translate connector %s to a proper internal"
				" connector type. Does synchdb support this type yet?", connector);
		set_shm_connector_errmsg(myConnectorId, "failed to translate connector to a proper internal"
				" connector type. Does synchdb support this type yet?");
		return TYPE_UNDEF;
	}
}

/*
 * updateSynchdbAttribute
 *
 * update synchdb_attribute table based on debezium's DDL info (dbzddl) and the transformed pg
 * equivalent (pgddl).
 */
void
updateSynchdbAttribute(DBZ_DDL * dbzddl, PG_DDL * pgddl, ConnectorType conntype, const char * name)
{
	const char *type = connectorTypeToString(conntype);
	bool upsert;
	bool drop_column;
	Oid target_relation = InvalidOid;
	int target_natts = 0;
	ListCell *cell;
	ListCell *cell2;

	if (!pgddl || !dbzddl || !name || !dbzddl->id)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid attribute update inputs")));
	upsert = pgddl->type == DDL_CREATE_TABLE ||
		(pgddl->type == DDL_ALTER_TABLE && pgddl->subtype == SUBTYPE_ADD_COLUMN) ||
		(pgddl->type == DDL_ALTER_TABLE && pgddl->subtype == SUBTYPE_ALTER_COLUMN);
	drop_column = pgddl->type == DDL_ALTER_TABLE && pgddl->subtype == SUBTYPE_DROP_COLUMN;
	if (upsert || drop_column)
	{
		Oid schema;
		Relation relation;

		if (!pgddl->schema || !pgddl->tbname)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("attribute update requires an explicit target schema and table")));
		schema = get_namespace_oid(pgddl->schema, false);
		target_relation = get_relname_relid(pgddl->tbname, schema);
		if (!OidIsValid(target_relation))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("target relation \"%s\".\"%s\" is missing during attribute update",
							pgddl->schema, pgddl->tbname)));
		/* Retain the relation lock until the caller commits the DDL and bindings. */
		relation = table_open(target_relation, AccessShareLock);
		target_natts = RelationGetNumberOfAttributes(relation);
		table_close(relation, NoLock);
	}
	if (upsert)
	{
		SynchdbAttributeBinding *bindings;
		int source_count = list_length(dbzddl->columns);
		int target_count = list_length(pgddl->columns);
		int count = 0;

		if (source_count <= 0 || source_count != target_count ||
			source_count > MaxHeapAttributeNumber)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("attribute update requires equal nonempty source and target column lists")));
		bindings = palloc0(sizeof(*bindings) * source_count);
		forboth(cell, dbzddl->columns, cell2, pgddl->columns)
		{
			DBZ_DDL_COLUMN *source = lfirst(cell);
			PG_DDL_COLUMN *target = lfirst(cell2);
			AttrNumber attnum;

			if (!source || !target)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("attribute update contains a missing column entry")));
			/* ADD/ALTER keep aligned placeholders for columns not being changed. */
			if (target->attname == NULL && target->atttype == NULL)
				continue;
			if (target->attname == NULL || target->atttype == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("attribute update contains an incomplete target column")));
			/* CREATE IF NOT EXISTS and dropped slots invalidate predicted positions. */
			attnum = get_attnum(target_relation, target->attname);
			if (attnum <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("target column \"%s\" is missing during attribute update",
								target->attname)));
			bindings[count].target_relation = target_relation;
			bindings[count].target_attnum = attnum;
			bindings[count].source_table = dbzddl->id;
			bindings[count].source_column = source->name;
			bindings[count].source_type = source->typeName;
			count++;
		}
		if (count == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("attribute update contains no effective column bindings")));
		synchdb_attribute_store_upsert(name, type, bindings, count);
		pfree(bindings);
	}
	else if (pgddl->type == DDL_DROP_TABLE)
		(void) synchdb_attribute_store_delete_source_table(name, type, dbzddl->id);
	else if (drop_column)
	{
		if (list_length(pgddl->columns) <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("attribute drop update requires dropped target columns")));
		foreach(cell, pgddl->columns)
		{
			PG_DDL_COLUMN *target = lfirst(cell);

			if (!target || target->position <= 0 || target->position > target_natts)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("attribute drop update contains an invalid target attribute number")));
			/* Persisted target identity survives DROP and any source-to-target rename. */
			(void) synchdb_attribute_store_mark_dropped(name, type, dbzddl->id,
													  target_relation, (AttrNumber) target->position);
		}
	}
	else
	{
		elog(DEBUG1, "unknown ddl type %d. Skipping attribute update", pgddl->type);
		return;
	}
	/* Maintenance stays within this connector/type; other bindings are untouched. */
	(void) synchdb_attribute_store_prune_missing(name, type);
}

/*
 * fc_initFormatConverter
 *
 * main entry to initialize all hash tables for all supported database types
 */
void
fc_initFormatConverter(ConnectorType type)
{
 /* Builtins are const and available without backend-local hash setup. */
 (void) synchdb_rule_builtin_type(type, NULL, false);
 synchdb_pg_metadata_init();
 if (!converterRuleContext)
  converterRuleContext = AllocSetContextCreate(TopMemoryContext,
   "SynchDB converter rules owner", ALLOCSET_DEFAULT_SIZES);
}

void
fc_deinitFormatConverter(ConnectorType type)
{
 synchdb_rule_snapshot_clear();
 if (converterRuleContext)
  MemoryContextDelete(converterRuleContext);
 converterRuleContext = NULL;
#ifdef WITH_OLR
 if (type == TYPE_OLR)
  unload_oracle_parser();
#endif
}

static void
release_data_cache_entry(DataCacheEntry *entry)
{
	if (entry->typeidhash)
		hash_destroy(entry->typeidhash);
	entry->typeidhash = NULL;
	fc_destroy_source_column_cache(entry->namejsonposhash);
	entry->namejsonposhash = NULL;
}

void
fc_invalidate_data_cache(const DataCacheKey *key)
{
	DataCacheEntry *entry;

	if (!dataCacheHash)
		return;
	entry = hash_search(dataCacheHash, key, HASH_FIND, NULL);
	if (!entry)
		return;
	release_data_cache_entry(entry);
	(void) hash_search(dataCacheHash, key, HASH_REMOVE, NULL);
}

void
fc_initDataCache(void)
{
	/* init data cache hash */
	HASHCTL info = {0};

	if (dataCacheHash)
		fc_deinitDataCache();

	info.keysize = sizeof(DataCacheKey);
	info.entrysize = sizeof(DataCacheEntry);
	info.hcxt = TopMemoryContext;

	dataCacheHash = hash_create("data cache hash",
							 256,
							 &info,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	dataCacheEpoch = synchdb_pg_metadata_epoch();
}

void
fc_deinitDataCache(void)
{
	/* destroy data cache hash */
	if (dataCacheHash)
	{
		HASH_SEQ_STATUS scan;
		DataCacheEntry *entry;

		hash_seq_init(&scan, dataCacheHash);
		while ((entry = hash_seq_search(&scan)) != NULL)
			release_data_cache_entry(entry);
		hash_destroy(dataCacheHash);
		dataCacheHash = NULL;
	}
}

void
fc_resetDataCache(void)
{
	/* deinit followed by another init */
	fc_deinitDataCache();
	fc_initDataCache();
}

bool
fc_load_objmap(const char *name, ConnectorType type)
{
 MemoryContext caller = CurrentMemoryContext;
 MemoryContext scratch;
 ObjectMap *objects = NULL;
 int count = 0;
 SynchdbRuleSnapshot *volatile candidate = NULL;

 if (IsTransactionOrTransactionBlock() || ActiveSnapshotSet())
  ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                 errmsg("mapping reload requires an idle batch boundary")));
 fc_initFormatConverter(type);
 scratch = AllocSetContextCreate(converterRuleContext, "SynchDB mapping read",
                                ALLOCSET_DEFAULT_SIZES);
 PG_TRY();
 {
  /* Read rules only: no target binding and no target DDL during reload. */
  StartTransactionCommand();
  PushActiveSnapshot(GetTransactionSnapshot());
  synchdb_mapping_store_list_raw(name, &objects, &count, scratch);
  /* parse_ident's text-array construction pins pg_type metadata internally.
   * Compile while the read transaction still owns a ResourceOwner; only the
   * independent run-owned candidate survives that transaction. */
  MemoryContextSwitchTo(caller);
  candidate = synchdb_rule_snapshot_build(name, type, objects, count, converterRuleContext);
  PopActiveSnapshot();
  CommitTransactionCommand();
  MemoryContextSwitchTo(caller);
  (void) synchdb_rule_snapshot_activate((SynchdbRuleSnapshot *) candidate);
  candidate = NULL;
  synchdb_pg_metadata_mark_dirty();
 }
 PG_CATCH();
 {
  MemoryContextSwitchTo(caller);
  /* This idle-entry loader owns the transaction, including START/COMMIT
   * errors; AbortCurrentTransaction is also safe when already idle. */
  AbortCurrentTransaction();
  MemoryContextSwitchTo(caller);
  if (candidate)
   synchdb_rule_snapshot_destroy((SynchdbRuleSnapshot *) candidate);
  MemoryContextDelete(scratch);
  PG_RE_THROW();
 }
 PG_END_TRY();
 MemoryContextDelete(scratch);
 return true;
}

void
fc_ensureDataCache(void)
{
 synchdb_pg_metadata_init();
 AcceptInvalidationMessages();
 if (!dataCacheHash || synchdb_pg_metadata_stale(dataCacheEpoch))
  fc_resetDataCache();
}


/*
 * find_exact_string_match
 *
 * Function to find exact match from given line
 */
bool
find_exact_string_match(const char * line, const char * wordtofind)
{
	const char *p = strstr(line, wordtofind);
	if ((p == line) || (p != NULL && !isalnum((unsigned char)p[-1])))
	{
		p += strlen(wordtofind);
		if (!isalnum((unsigned char)*p))
			return true;
		else
			return false;
	}
	return false;
}

/*
 * remove_double_quotes
 *
 * Function to remove double quotes from a string
 */
void
remove_double_quotes(StringInfoData * str)
{
	char *src = str->data, *dst = str->data;
	int newlen = 0;

	while (*src)
	{
		if (*src != '"' && *src != '\\')
		{
			*dst++ = *src;
			newlen++;
		}
		src++;
	}
	*dst = '\0';
	str->len = newlen;
}

/*
 * escapeSingleQuote
 *
 * escape the single quotes in the given input and returns a palloc-ed string
 */
char *
escapeSingleQuote(const char * in, bool addquote)
{
	int i = 0, j = 0, outlen = 0;
	char * out = NULL;

	/* escape possible single quotes */
	for (i = 0; i < strlen(in); i++)
	{
		if (in[i] == '\'')
		{
			/* single quote will be escaped so +2 in size */
			outlen += 2;
		}
		else
		{
			outlen++;
		}
	}

	if (addquote)
		/* 2 more to account for open and closing quotes */
		out = (char *) palloc0(outlen + 1 + 2);
	else
		out = (char *) palloc0(outlen + 1);

	if (addquote)
		out[j++] = '\'';
	for (i = 0; i < strlen(in); i++)
	{
		if (in[i] == '\'')
		{
			out[j++] = '\'';
			out[j++] = '\'';
		}
		else
		{
			out[j++] = in[i];
		}
	}
	if (addquote)
		out[j++] = '\'';

	return out;
}

/*
 * transform_object_name
 *
 * transform the remote object name based on the object name rule file definitions
 */
char *
transform_object_name(const char *source, const char *kind)
{
 const char *destination;
 SynchdbMappingKind mapping_kind;

 if (!source || !kind)
  return NULL;
 if (strcmp(kind, "table") == 0)
  mapping_kind = SYNCHDB_MAPPING_TABLE;
 else if (strcmp(kind, "column") == 0)
  mapping_kind = SYNCHDB_MAPPING_COLUMN;
 else
  return NULL;
 destination = synchdb_rule_object(synchdb_rule_snapshot_current(), mapping_kind, source);
 return destination ? pstrdup(destination) : NULL;
}


/*
 * getPathElementString
 *
 * Function to get a string element from a JSONB path
 */
int
getPathElementString(Jsonb * jb, char * path, StringInfoData * strinfoout, bool removequotes)
{
	Datum * datum_elems = NULL;
	char * str_elems = NULL, * p = path;
	int numPaths = 0, curr = 0;
	char * pathcopy = path;
	Datum res;
	bool isnull;

	if (!strinfoout)
		return -1;

    /* Count the number of elements in the path */
	if (strchr(pathcopy, '.'))
	{
		while (*p != '\0')
		{
			if (*p == '.')
			{
				numPaths++;
			}
			p++;
		}
		numPaths++; /* Add the last one */
		pathcopy = pstrdup(path);
	}
	else
	{
		numPaths = 1;
	}

	datum_elems = palloc0(sizeof(Datum) * numPaths);

    /* Parse the path into elements */
	if (numPaths > 1)
	{
		str_elems= strtok(pathcopy, ".");
		if (str_elems)
		{
			datum_elems[curr] = CStringGetTextDatum(str_elems);
			curr++;
			while ((str_elems = strtok(NULL, ".")))
			{
				datum_elems[curr] = CStringGetTextDatum(str_elems);
				curr++;
			}
		}
	}
	else
	{
		/* only one level, just use pathcopy*/
		datum_elems[curr] = CStringGetTextDatum(pathcopy);
	}

    /* Get the element from JSONB */
    res = jsonb_get_element(jb, datum_elems, numPaths, &isnull, false);
    if (isnull)
    {
    	resetStringInfo(strinfoout);
    	pfree(datum_elems);
    	if (pathcopy != path)
    		pfree(pathcopy);
    	return -1;
    }
    else
    {
    	Jsonb *resjb = DatumGetJsonbP(res);
    	resetStringInfo(strinfoout);
		JsonbToCString(strinfoout, &resjb->root, VARSIZE(resjb));

		/*
		 * note: buf.data includes double quotes and escape char \.
		 * We need to remove them
		 */
		if (removequotes)
			remove_double_quotes(strinfoout);

		if (resjb)
			pfree(resjb);
    }

	pfree(datum_elems);
	if (pathcopy != path)
		pfree(pathcopy);
	return 0;
}

/*
 * splitIdString
 *
 * Function to split ID string into database, schema, and table.
 *
 * This function breaks down a fully qualified id string (database.
 * schema.table, schema.table, or database.table) into individual
 * components.
 */
void
splitIdString(char * id, char ** db, char ** schema, char ** table, bool usedb)
{
	int dotCount = 0;
	char *p = NULL;

	for (p = id; *p != '\0'; p++)
	{
		if (*p == '.')
			dotCount++;
	}

	if (dotCount == 1)
	{
		if (usedb)
		{
			/* treat it as database.table */
			*db = strtok(id, ".");
			*schema = NULL;
			*table = strtok(NULL, ".");
		}
		else
		{
			/* treat it as schema.table */
			*db = NULL;
			*schema = strtok(id, ".");
			*table = strtok(NULL, ".");
		}
	}
	else if (dotCount == 2)
	{
		/* treat it as database.schema.table */
		*db = strtok(id, ".");
		*schema = strtok(NULL, ".");
		*table = strtok(NULL, ".");
	}
	else if (dotCount == 0)
	{
		/* treat it as table */
		*db = NULL;
		*schema = NULL;
		*table = id;
	}
	else
	{
		elog(WARNING, "invalid ID string format %s", id);
		*db = NULL;
		*schema = NULL;
		*table = NULL;
	}
}

/*
 * convert2PGDDL
 *
 * This functions converts DBZ_DDL to PG_DDL structure
 */
PG_DDL *
convert2PGDDL(DBZ_DDL * dbzddl, ConnectorType type)
{
	PG_DDL * pgddl = (PG_DDL*) palloc0(sizeof(PG_DDL));
	ListCell * cell;
	StringInfoData strinfo;
	char * mappedObjName = NULL;
	char * db = NULL, * schema = NULL, * table = NULL;
	PG_DDL_COLUMN * pgcol = NULL;
	Relation rel = NULL;

	initStringInfo(&strinfo);

	pgddl->type = dbzddl->type;

	if (dbzddl->type == DDL_CREATE_TABLE)
	{
		int attnum = 1;
		mappedObjName = transform_object_name(dbzddl->id, "table");
		if (mappedObjName)
		{
			/*
			 * we will used the transformed object name here, but first, we will check if
			 * transformed name is valid. It should be expressed in one of the forms below:
			 * - schema.table
			 * - table
			 *
			 * then we check if schema is spplied. If yes, we need to add the CREATE SCHEMA
			 * clause as well.
			 */
			splitIdString(mappedObjName, &db, &schema, &table, false);
			if (!table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "transformed object ID is invalid: %s",
						mappedObjName);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			if (schema && table)
			{
				/* include create schema clause */
				appendStringInfo(&strinfo, "CREATE SCHEMA IF NOT EXISTS \"%s\"; ", schema);

				/* table stays as table under the schema */
				appendStringInfo(&strinfo, "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (", schema, table);
				pgddl->schema = pstrdup(schema);
				pgddl->tbname = pstrdup(table);
			}
			else if (!schema && table)
			{
				/* table stays as table but no schema */
				appendStringInfo(&strinfo, "CREATE TABLE IF NOT EXISTS \"%s\" (", table);
				pgddl->schema = pstrdup("public");
				pgddl->tbname = pstrdup(table);
			}
		}
		else
		{
			/*
			 * no object name mapping found. Transform it using default methods below:
			 *
			 * database.table:
			 * 	- database becomes schema in PG
			 * 	- table name stays
			 *
			 * database.schema.table:
			 * 	- database becomes schema in PG
			 * 	- schema is ignored
			 * 	- table name stays
			 */
			char * idcopy = pstrdup(dbzddl->id);

			/* we want to normalize according to letter casing strategy here */
			fc_normalize_name(synchdb_letter_casing_strategy, idcopy, strlen(idcopy));

			splitIdString(idcopy, &db, &schema, &table, true);

			/* database and table must be present. schema is optional */
			if (!db || !table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "malformed id field in dbz change event: %s",
						dbzddl->id);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			/* database mapped to schema */
			appendStringInfo(&strinfo, "CREATE SCHEMA IF NOT EXISTS \"%s\"; ", db);

			/* table stays as table, schema ignored */
			appendStringInfo(&strinfo, "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (", db, table);
			pgddl->schema = pstrdup(db);
			pgddl->tbname = pstrdup(table);

			pfree(idcopy);
		}

		foreach(cell, dbzddl->columns)
		{
			DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);
			pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));

			transformDDLColumns(dbzddl->id, col, type, false, &strinfo, pgcol);

			/* if both length and scale are specified, add them. For example DECIMAL(10,2) */
			if (col->length > 0 && col->scale > 0)
			{
				appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
			}

			/* if a only length if specified, add it. For example VARCHAR(30)*/
			if (col->length > 0 && col->scale == 0)
			{
				/* make sure it does not exceed postgresql allowed maximum */
				if (col->length > MaxAttrSize)
					col->length = MaxAttrSize;
				appendStringInfo(&strinfo, "(%d) ", col->length);
			}

			/* if there is UNSIGNED operator found in col->typeName, add CHECK constraint */
			if (strstr(col->typeName, "unsigned"))
			{
				appendStringInfo(&strinfo, "CHECK (%s >= 0) ", col->name);
			}

			/* is it optional? */
			if (!col->optional)
			{
				appendStringInfo(&strinfo, "NOT NULL ");
			}

			/* does it have defaults? */
			if (col->defaultValueExpression && strlen(col->defaultValueExpression) > 0
					&& !col->autoIncremented)
			{
				/* use DEFAULT NULL regardless of the value of col->defaultValueExpression
				 * because it may contain expressions not recognized by PostgreSQL. We could
				 * make this part more intelligent by checking if the given expression can
				 * be applied by PostgreSQL and use it only when it can. But for now, we will
				 * just put default null here. Todo
				 */
				appendStringInfo(&strinfo, "DEFAULT %s ", "NULL");
			}

			/* for create, the position(attnum) should be in the same order as they are created */
			pgcol->position = attnum++;

			appendStringInfo(&strinfo, ",");
			pgddl->columns =lappend(pgddl->columns, pgcol);
		}

		/* remove the last extra comma */
		strinfo.data[strinfo.len - 1] = '\0';
		strinfo.len = strinfo.len - 1;

		/*
		 * finally, declare primary keys if any. iterate dbzddl->primaryKeyColumnNames
		 * and build into primary key(x, y, z) clauses. todo
		 */
		populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, false, true);

		appendStringInfo(&strinfo, ");");
	}
	else if (dbzddl->type == DDL_DROP_TABLE)
	{
		DataCacheKey cachekey = {0};

		mappedObjName = transform_object_name(dbzddl->id, "table");
		if (mappedObjName)
		{
			/*
			 * we will used the transformed object name here, but first, we will check if
			 * transformed name is valid. It should be expressed in one of the forms below:
			 * - schema.table
			 * - table
			 */
			splitIdString(mappedObjName, &db, &schema, &table, false);
			if (!table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "transformed object ID is invalid: %s",
						mappedObjName);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			if (schema && table)
			{
				/* table stays as table under the schema */
				appendStringInfo(&strinfo, "DROP TABLE IF EXISTS \"%s\".\"%s\";", schema, table);
				pgddl->schema = pstrdup(schema);
				pgddl->tbname = pstrdup(table);
			}
			else if (!schema && table)
			{
				/* table stays as table but no schema */
				schema = pstrdup("public");
				appendStringInfo(&strinfo, "DROP TABLE IF EXISTS \"%s\";", table);
				pgddl->schema = pstrdup("public");
				pgddl->tbname = pstrdup(table);
			}
		}
		else
		{
			char * idcopy = pstrdup(dbzddl->id);

			/* we want to normalize according to letter casing strategy here */
			fc_normalize_name(synchdb_letter_casing_strategy, idcopy, strlen(idcopy));

			splitIdString(idcopy, &db, &schema, &table, true);

			/* database and table must be present. schema is optional */
			if (!db || !table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "malformed id field in dbz change event: %s",
						dbzddl->id);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}
			/* make schema points to db */
			schema = db;
			appendStringInfo(&strinfo, "DROP TABLE IF EXISTS \"%s\".\"%s\";", schema, table);
			pgddl->schema = pstrdup(schema);
			pgddl->tbname = pstrdup(table);
		}

		/* no column information needed for DROP */
		pgddl->columns = NULL;

		/* drop data cache for schema.table if exists */
		strlcpy(cachekey.schema, schema, SYNCHDB_CONNINFO_DB_NAME_SIZE);
		strlcpy(cachekey.table, table, SYNCHDB_CONNINFO_DB_NAME_SIZE);
		fc_invalidate_data_cache(&cachekey);

	}
	else if (dbzddl->type == DDL_ALTER_TABLE)
	{
		int attnum = 1, newcol = 0;
		Oid schemaoid = 0;
		Oid tableoid = 0;
		Oid pkoid = 0;
		bool found = false, altered = false;
		TupleDesc tupdesc;
		DataCacheKey cachekey = {0};
		StringInfoData colNameObjId;

		initStringInfo(&colNameObjId);

		mappedObjName = transform_object_name(dbzddl->id, "table");
		if (mappedObjName)
		{
			/*
			 * we will used the transformed object name here, but first, we will check if
			 * transformed name is valid. It should be expressed in one of the forms below:
			 * - schema.table
			 * - table
			 */
			splitIdString(mappedObjName, &db, &schema, &table, false);
			if (!table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "transformed object ID is invalid: %s",
						mappedObjName);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			if (schema && table)
			{
				/* table stays as table under the schema */
				appendStringInfo(&strinfo, "ALTER TABLE \"%s\".\"%s\" ", schema, table);
				pgddl->schema = pstrdup(schema);
				pgddl->tbname = pstrdup(table);
			}
			else if (!schema && table)
			{
				/* table stays as table but no schema */
				schema = pstrdup("public");
				appendStringInfo(&strinfo, "ALTER TABLE \"%s\" ", table);
				pgddl->schema = pstrdup("public");
				pgddl->tbname = pstrdup(table);
			}
		}
		else
		{
			/* by default, remote's db is mapped to schema in pg */
			char * idcopy = pstrdup(dbzddl->id);

			/* we want to normalize according to letter casing strategy here */
			fc_normalize_name(synchdb_letter_casing_strategy, idcopy, strlen(idcopy));

			splitIdString(idcopy, &db, &schema, &table, true);

			/* database and table must be present. schema is optional */
			if (!db || !table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "malformed id field in dbz change event: %s",
						dbzddl->id);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			/* make schema points to db */
			schema = db;
			appendStringInfo(&strinfo, "ALTER TABLE \"%s\".\"%s\" ", schema, table);
			pgddl->schema = pstrdup(schema);
			pgddl->tbname = pstrdup(table);
		}

		/* drop data cache for schema.table if exists */
		strlcpy(cachekey.schema, schema, SYNCHDB_CONNINFO_DB_NAME_SIZE);
		strlcpy(cachekey.table, table, SYNCHDB_CONNINFO_DB_NAME_SIZE);
		fc_invalidate_data_cache(&cachekey);

		/*
		 * For ALTER, we must obtain the current schema in PostgreSQL and identify
		 * which column is the new column added. We will first check if table exists
		 * and then add its column to a temporary hash table that we can compare
		 * with the new column list.
		 */
		schemaoid = get_namespace_oid(schema, false);
		if (!OidIsValid(schemaoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for schema '%s'", schema);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		tableoid = get_relname_relid(table, schemaoid);
		if (!OidIsValid(tableoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for table '%s'", table);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		elog(DEBUG1, "namespace \"%s\".\"%s\" has PostgreSQL OID %d", schema, table, tableoid);

		rel = table_open(tableoid, AccessShareLock);
		tupdesc = RelationGetDescr(rel);
#if SYNCHDB_PG_MAJOR_VERSION >= 1800
		pkoid = RelationGetPrimaryKeyIndex(rel, true);
#else
		pkoid = RelationGetPrimaryKeyIndex(rel);
#endif
		/* Keep the relation and descriptor valid while composing the ALTER. */

		if (type != TYPE_OLR)
		{
			/*
			 * DBZ supplies more columns than what PostreSQL have. This means ALTER
			 * TABLE ADD COLUMN operation. Let's find which one is to be added.
			 */
			if (list_length(dbzddl->columns) > count_active_columns(tupdesc))
			{
				altered = false;

				/* indicate pgddl that this is alter add column */
				pgddl->subtype = SUBTYPE_ADD_COLUMN;

				foreach(cell, dbzddl->columns)
				{
					DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);
					char * mappedColumnName = NULL;

					pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));

					/* express a column name in fully qualified id */
					resetStringInfo(&colNameObjId);
					appendStringInfo(&colNameObjId, "%s.%s", dbzddl->id, col->name);
					mappedColumnName = transform_object_name(colNameObjId.data, "column");
					if (mappedColumnName)
					{
						elog(DEBUG1, "transformed column object ID '%s'to '%s'",
								colNameObjId.data, mappedColumnName);
					}
					else
					{
						mappedColumnName = pstrdup(col->name);

						/* we want to normalize according to letter casing strategy here */
						fc_normalize_name(synchdb_letter_casing_strategy, mappedColumnName, strlen(mappedColumnName));
					}

					found = false;
					for (attnum = 1; attnum <= tupdesc->natts; attnum++)
					{
						Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

						/* skip special attname indicated a dropped column */
						if (strstr(NameStr(attr->attname), "pg.dropped"))
							continue;

						if (!strcmp(mappedColumnName, NameStr(attr->attname)))
						{
							found = true;
							break;
						}
					}
					if (!found)
					{
						elog(DEBUG1, "adding new column %s", mappedColumnName);
						altered = true;
						appendStringInfo(&strinfo, "ADD COLUMN IF NOT EXISTS ");

						transformDDLColumns(dbzddl->id, col, type, false, &strinfo, pgcol);

						/* if both length and scale are specified, add them. For example DECIMAL(10,2) */
						if (col->length > 0 && col->scale > 0)
						{
							appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
						}

						/* if a only length if specified, add it. For example VARCHAR(30)*/
						if (col->length > 0 && col->scale == 0)
						{
							/* make sure it does not exceed postgresql allowed maximum */
							if (col->length > MaxAttrSize)
								col->length = MaxAttrSize;
							appendStringInfo(&strinfo, "(%d) ", col->length);
						}

						/* if there is UNSIGNED operator found in col->typeName, add CHECK constraint */
						if (strstr(col->typeName, "unsigned"))
						{
							appendStringInfo(&strinfo, "CHECK (%s >= 0) ", pgcol->attname);
						}

						/* is it optional? */
						if (!col->optional)
						{
							appendStringInfo(&strinfo, "NOT NULL ");
						}

						/* does it have defaults? */
						if (col->defaultValueExpression && strlen(col->defaultValueExpression) > 0
								&& !col->autoIncremented)
						{
							/* use DEFAULT NULL regardless of the value of col->defaultValueExpression
							 * because it may contain expressions not recognized by PostgreSQL. We could
							 * make this part more intelligent by checking if the given expression can
							 * be applied by PostgreSQL and use it only when it can. But for now, we will
							 * just put default null here. Todo
							 */
							appendStringInfo(&strinfo, "DEFAULT %s ", "NULL");
						}
						appendStringInfo(&strinfo, ",");

						/* assign new attnum for this newly added column */
						pgcol->position = attnum + newcol;
						newcol++;
					}
					else
					{
						/* not a column to add, point data to null and act as a placeholder in the list */
						pgcol->attname = NULL;
						pgcol->atttype = NULL;
					}

					/*
					 * add to list regardless if this column is to be added or not so we keep both ddl->columns
					 * and pgddl->columns the same size
					 */
					pgddl->columns = lappend(pgddl->columns, pgcol);

					if(mappedColumnName)
						pfree(mappedColumnName);
				}

				if (altered)
				{
					/* something has been altered, continue to wrap up... */
					/* remove the last extra comma */
					strinfo.data[strinfo.len - 1] = '\0';
					strinfo.len = strinfo.len - 1;

					/*
					 * finally, declare primary keys if current table has no primary key index.
					 * Iterate dbzddl->primaryKeyColumnNames and build into primary key(x, y, z)
					 * clauses. We will not add new primary key if current table already has pk.
					 *
					 * If a primary key is added in the same clause as alter table add column, debezium
					 * may not be able to include the list of primary keys properly (observed in oracle
					 * connector). We need to ensure that it is done in 2 qureies; first to add a new
					 * column and second to add a new primary on the new column.
					 */
					if (pkoid == InvalidOid)
						populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, true, true);
				}
				else
				{
					elog(DEBUG1, "no column altered");
					destroyPGDDL(pgddl);
					if (rel)
						table_close(rel, NoLock);
					return NULL;
				}
			}
			else if (list_length(dbzddl->columns) < count_active_columns(tupdesc))
			{
				/*
				 * DBZ supplies less columns than what PostreSQL have. This means ALTER
				 * TABLE DROP COLUMN operation. Let's find which one is to be dropped.
				 */
				altered = false;

				/* indicate pgddl that this is alter drop column */
				pgddl->subtype = SUBTYPE_DROP_COLUMN;

				for (attnum = 1; attnum <= tupdesc->natts; attnum++)
				{
					Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
					found = false;

					/* skip special attname indicated a dropped column */
					if (strstr(NameStr(attr->attname), "pg.dropped"))
						continue;

					foreach(cell, dbzddl->columns)
					{
						DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);
						char * mappedColumnName = NULL;

						/* express a column name in fully qualified id */
						resetStringInfo(&colNameObjId);
						appendStringInfo(&colNameObjId, "%s.%s", dbzddl->id, col->name);
						mappedColumnName = transform_object_name(colNameObjId.data, "column");
						if (mappedColumnName)
						{
							elog(DEBUG1, "transformed column object ID '%s'to '%s'",
									colNameObjId.data, mappedColumnName);
						}
						else
						{
							mappedColumnName = pstrdup(col->name);

							/* we want to normalize according to letter casing strategy here */
							fc_normalize_name(synchdb_letter_casing_strategy, mappedColumnName, strlen(mappedColumnName));
						}

						if (!strcmp(mappedColumnName, NameStr(attr->attname)))
						{
							found = true;
							if (mappedColumnName)
								pfree(mappedColumnName);
							break;
						}
						if (mappedColumnName)
							pfree(mappedColumnName);
					}
					if (!found)
					{
						elog(DEBUG1, "dropping old column %s", NameStr(attr->attname));

						pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));
						altered = true;
						appendStringInfo(&strinfo, "DROP COLUMN IF EXISTS \"%s\",", NameStr(attr->attname));
						pgcol->attname = pstrdup(NameStr(attr->attname));
						pgcol->position = attnum;
						pgddl->columns = lappend(pgddl->columns, pgcol);
					}
				}
				if(altered)
				{
					/* something has been altered, continue to wrap up... */
					/* remove the last extra comma */
					strinfo.data[strinfo.len - 1] = '\0';
					strinfo.len = strinfo.len - 1;
				}
				else
				{
					elog(DEBUG1, "no column altered");
					destroyPGDDL(pgddl);
					if (rel)
						table_close(rel, NoLock);
					return NULL;
				}
			}
			else
			{
				/*
				 * DBZ supplies the same number of columns as what PostreSQL have. This means
				 * general ALTER TABLE operation.
				 */
				char * alterclause = NULL;

				/* indicate pgddl that this is alter column ddl*/
				pgddl->subtype = SUBTYPE_ALTER_COLUMN;

				alterclause = composeAlterColumnClauses(dbzddl->id, type, dbzddl->columns, tupdesc, rel, pgddl);
				if (alterclause)
				{
					appendStringInfo(&strinfo, "%s", alterclause);
					elog(DEBUG1, "alter clause: %s", strinfo.data);
					pfree(alterclause);
				}
				else
				{
					elog(DEBUG1, "no column altered");
					destroyPGDDL(pgddl);
					if (rel)
						table_close(rel, NoLock);
					return NULL;
				}
				/*
				 * finally, declare primary keys if current table has no primary key index.
				 * Iterate dbzddl->primaryKeyColumnNames and build into primary key(x, y, z)
				 * clauses. We will not add new primary key if current table already has pk.
				 */
				if (pkoid == InvalidOid)
					populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, true, true);
			}
		}
#ifdef WITH_OLR
		else
		{
			/* pass the subtype to pgddl */
			pgddl->subtype = dbzddl->subtype;

			/*
			 * OLR connector gives the added and dropped columns directly so there is no
			 * need to figure out that information
			 */
			if (dbzddl->subtype == SUBTYPE_ADD_COLUMN)
			{
				/* ADD COLUMN */
				if (list_length(dbzddl->columns) <= 0)
				{
					elog(WARNING, "no columns to add");
					destroyPGDDL(pgddl);
					if (rel)
						table_close(rel, NoLock);
					return NULL;
				}

				altered = false;
				foreach(cell, dbzddl->columns)
				{
					OLR_DDL_COLUMN * col = (OLR_DDL_COLUMN *) lfirst(cell);
					char * mappedColumnName = NULL;

					pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));

					/* express a column name in fully qualified id */
					resetStringInfo(&colNameObjId);
					appendStringInfo(&colNameObjId, "%s.%s", dbzddl->id, col->name);
					mappedColumnName = transform_object_name(colNameObjId.data, "column");
					if (mappedColumnName)
					{
						elog(DEBUG1, "transformed column object ID '%s'to '%s'",
								colNameObjId.data, mappedColumnName);
					}
					else
					{
						mappedColumnName = pstrdup(col->name);

						/* we want to normalize according to letter casing strategy here */
						fc_normalize_name(synchdb_letter_casing_strategy, mappedColumnName, strlen(mappedColumnName));
					}

					elog(DEBUG1, "adding new column %s", mappedColumnName);
					altered = true;
					appendStringInfo(&strinfo, "ADD COLUMN IF NOT EXISTS ");

					transformDDLColumns(dbzddl->id, col, type, false, &strinfo, pgcol);

					/* if both length and scale are specified, add them. For example DECIMAL(10,2) */
					if (col->length > 0 && col->scale > 0)
					{
						appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
					}

					/* if a only length if specified, add it. For example VARCHAR(30)*/
					if (col->length > 0 && col->scale == 0)
					{
						/* make sure it does not exceed postgresql allowed maximum */
						if (col->length > MaxAttrSize)
							col->length = MaxAttrSize;
						appendStringInfo(&strinfo, "(%d) ", col->length);
					}

					/* if there is UNSIGNED operator found in col->typeName, add CHECK constraint */
					if (strstr(col->typeName, "unsigned"))
					{
						appendStringInfo(&strinfo, "CHECK (%s >= 0) ", pgcol->attname);
					}

					/* is it optional? */
					if (!col->optional)
					{
						appendStringInfo(&strinfo, "NOT NULL ");
					}

					/* does it have defaults? */
					if (col->defaultValueExpression && strlen(col->defaultValueExpression) > 0
							&& !col->autoIncremented)
					{
						/* use DEFAULT NULL regardless of the value of col->defaultValueExpression
						 * because it may contain expressions not recognized by PostgreSQL. We could
						 * make this part more intelligent by checking if the given expression can
						 * be applied by PostgreSQL and use it only when it can. But for now, we will
						 * just put default null here. Todo
						 */
						appendStringInfo(&strinfo, "DEFAULT %s ", "NULL");
					}
					appendStringInfo(&strinfo, ",");

					newcol++;
					pgcol->position = tupdesc->natts + newcol;
					pgddl->columns = lappend(pgddl->columns, pgcol);

					if(mappedColumnName)
						pfree(mappedColumnName);
				}

				if (altered)
				{
					/* something has been altered, continue to wrap up... */
					/* remove the last extra comma */
					strinfo.data[strinfo.len - 1] = '\0';
					strinfo.len = strinfo.len - 1;

					/*
					 * finally, declare primary keys if current table has no primary key index.
					 * Iterate dbzddl->primaryKeyColumnNames and build into primary key(x, y, z)
					 * clauses. We will not add new primary key if current table already has pk.
					 *
					 * If a primary key is added in the same clause as alter table add column, debezium
					 * may not be able to include the list of primary keys properly (observed in oracle
					 * connector). We need to ensure that it is done in 2 qureies; first to add a new
					 * column and second to add a new primary on the new column.
					 */
					if (pkoid == InvalidOid)
						populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, true, true);
				}
				else
				{
					elog(DEBUG1, "no column altered");
					destroyPGDDL(pgddl);
					if (rel)
						table_close(rel, NoLock);
					return NULL;
				}
			}
			else if (dbzddl->subtype == SUBTYPE_DROP_COLUMN)
			{
				/* DROP COLUMN */
				if (list_length(dbzddl->columns) <= 0)
				{
					elog(WARNING, "no columns to drop");
					destroyPGDDL(pgddl);
					if (rel)
						table_close(rel, NoLock);
					return NULL;
				}

				altered = false;
				foreach(cell, dbzddl->columns)
				{
					OLR_DDL_COLUMN * col = (OLR_DDL_COLUMN *) lfirst(cell);
					char * mappedColumnName = NULL;

					/* express a column name in fully qualified id */
					resetStringInfo(&colNameObjId);
					appendStringInfo(&colNameObjId, "%s.%s", dbzddl->id, col->name);
					mappedColumnName = transform_object_name(colNameObjId.data, "column");
					if (mappedColumnName)
					{
						elog(DEBUG1, "transformed column object ID '%s'to '%s'",
								colNameObjId.data, mappedColumnName);
					}
					else
					{
						mappedColumnName = pstrdup(col->name);

						/* we want to normalize according to letter casing strategy here */
						fc_normalize_name(synchdb_letter_casing_strategy, mappedColumnName, strlen(mappedColumnName));
					}

					/*
					 * in order to update synchdb_attribute table correctly, we need to find
					 * this dropped column's attribute id from postgres
					 */
					for (attnum = 1; attnum <= tupdesc->natts; attnum++)
					{
						Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

						/* skip special attname indicated a dropped column */
						if (strstr(NameStr(attr->attname), "pg.dropped"))
							continue;

						if (!strcmp(mappedColumnName, NameStr(attr->attname)))
							break;
					}

					appendStringInfo(&strinfo, "DROP COLUMN IF EXISTS \"%s\",", mappedColumnName);

					elog(DEBUG1, "dropping old column %s", mappedColumnName);

					pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));
					altered = true;

					pgcol->attname = pstrdup(mappedColumnName);
					pgcol->position = attnum;
					pgddl->columns = lappend(pgddl->columns, pgcol);

					if (mappedColumnName)
						pfree(mappedColumnName);
				}
				if(altered)
				{
					/* something has been altered, continue to wrap up... */
					/* remove the last extra comma */
					strinfo.data[strinfo.len - 1] = '\0';
					strinfo.len = strinfo.len - 1;
				}
			}
			else if (dbzddl->subtype == SUBTYPE_ALTER_COLUMN)
			{
				/* ALTER COLUMN */
				if (list_length(dbzddl->columns) <= 0)
				{
					elog(WARNING, "no columns to alter");
					destroyPGDDL(pgddl);
					if (rel)
						table_close(rel, NoLock);
					return NULL;
				}

				altered = false;
				foreach(cell, dbzddl->columns)
				{
					char * mappedColumnName = NULL;
					OLR_DDL_COLUMN * col = (OLR_DDL_COLUMN *) lfirst(cell);
					pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));

					/* express a column name in fully qualified id */
					resetStringInfo(&colNameObjId);
					appendStringInfo(&colNameObjId, "%s.%s", dbzddl->id, col->name);
					mappedColumnName = transform_object_name(colNameObjId.data, "column");
					if (mappedColumnName)
					{
						elog(DEBUG1, "transformed column object ID '%s'to '%s'",
								colNameObjId.data, mappedColumnName);
					}
					else
					{
						mappedColumnName = pstrdup(col->name);

						/* we want to normalize according to letter casing strategy here */
						fc_normalize_name(synchdb_letter_casing_strategy, mappedColumnName, strlen(mappedColumnName));
					}

					/*
					 * in order to update synchdb_attribute table correctly, we need to find
					 * this dropped column's attribute id from postgres
					 */
					for (attnum = 1; attnum <= tupdesc->natts; attnum++)
					{
						Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

						/* skip special attname indicated a dropped column */
						if (strstr(NameStr(attr->attname), "pg.dropped"))
							continue;

						if (!strcmp(mappedColumnName, NameStr(attr->attname)))
							break;
					}

					/* check data type */
					elog(DEBUG1, "altering column %s", col->name);
					appendStringInfo(&strinfo, "ALTER COLUMN \"%s\" SET DATA TYPE",
							mappedColumnName);

					transformDDLColumns(dbzddl->id, col, type, true, &strinfo, pgcol);
					if (col->length > 0 && col->scale > 0)
					{
						appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
					}

					/* if a only length if specified, add it. For example VARCHAR(30)*/
					if (col->length > 0 && col->scale == 0)
					{
						/* make sure it does not exceed postgresql allowed maximum */
						if (col->length > MaxAttrSize)
							col->length = MaxAttrSize;
						appendStringInfo(&strinfo, "(%d) ", col->length);
					}

					/*
					 * todo: for complex data type transformation, postgresql requires
					 * the user to specify a function to cast existing values to the new
					 * data type via the USING clause. This is needed for INT -> TEXT,
					 * INT -> DATE or NUMERIC -> VARCHAR. We do not support USING now as
					 * we do not know what function the user wants to use for casting the
					 * values. Perhaps we can include these cast functions in the rule file
					 * as well, but for now this is not supported and PostgreSQL may complain
					 * if we attempt to do complex data type changes.
					 */
					appendStringInfo(&strinfo, ", ");

					if (col->defaultValueExpression)
					{
						/*
						 * synchdb can receive a default expression not supported in postgresql.
						 * so for now, we always set to default null. todo
						 */
						appendStringInfo(&strinfo, "ALTER COLUMN \"%s\" SET DEFAULT %s",
								mappedColumnName, "NULL");
					}
					else
					{
						/* remove default value */
						appendStringInfo(&strinfo, "ALTER COLUMN \"%s\" DROP DEFAULT",
								mappedColumnName);
					}

					appendStringInfo(&strinfo, ", ");

					/* check if nullable or not nullable */
					if (!col->optional)
					{
						appendStringInfo(&strinfo, "ALTER COLUMN \"%s\" SET NOT NULL",
								mappedColumnName);
					}
					else
					{
						appendStringInfo(&strinfo, "ALTER COLUMN \"%s\" DROP NOT NULL",
								mappedColumnName);
					}

					appendStringInfo(&strinfo, ",");

					/*
					 * finally, declare primary keys if current table has no primary key index.
					 * Iterate dbzddl->primaryKeyColumnNames and build into primary key(x, y, z)
					 * clauses. We will not add new primary key if current table already has pk.
					 */
					if (pkoid == InvalidOid)
						populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, true, true);

					pgcol->position = attnum;
					pgddl->columns = lappend(pgddl->columns, pgcol);
				}

				/* remove extra "," */
				strinfo.data[strinfo.len - 1] = '\0';
				strinfo.len = strinfo.len - 1;
			}
			else if (dbzddl->subtype == SUBTYPE_ADD_CONSTRAINT)
			{
				if (pkoid == InvalidOid)
				{
					appendStringInfo(&strinfo, "ADD CONSTRAINT \"%s\" ", dbzddl->constraintName);
					populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, true, false);
				}
				else
				{
					elog(WARNING, "relation already has primary key, skip adding...");
					destroyPGDDL(pgddl);
					if (rel)
						table_close(rel, NoLock);
					return NULL;
				}
			}
			else if (dbzddl->subtype == SUBTYPE_DROP_CONSTRAINT)
			{
				if (dbzddl->constraintName)
					appendStringInfo(&strinfo, "DROP CONSTRAINT \"%s\" ", dbzddl->constraintName);
				else
				{
					elog(WARNING, "constaint name to drop is NULL. Skipping...");
					destroyPGDDL(pgddl);
					if (rel)
						table_close(rel, NoLock);
					return NULL;
				}
			}
			else
			{
				elog(WARNING, "unsupported ALTER TABLE sub type %d", dbzddl->subtype);
				destroyPGDDL(pgddl);
				if (rel)
					table_close(rel, NoLock);
				return NULL;
			}
		}
#else
		else
		{
			set_shm_connector_errmsg(myConnectorId, "OLR connector is not enabled in this synchdb build");
			elog(ERROR, "OLR connector is not enabled in this synchdb build");
		}
#endif
	}
	else if (dbzddl->type == DDL_TRUNCATE_TABLE)
	{
		mappedObjName = transform_object_name(dbzddl->id, "table");
		if (mappedObjName)
		{
			/*
			 * we will used the transformed object name here, but first, we will check if
			 * transformed name is valid. It should be expressed in one of the forms below:
			 * - schema.table
			 * - table
			 */
			splitIdString(mappedObjName, &db, &schema, &table, false);
			if (!table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "transformed object ID is invalid: %s",
						mappedObjName);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			if (schema && table)
			{
				/* table stays as table under the schema */
				appendStringInfo(&strinfo, "TRUNCATE TABLE \"%s\".\"%s\";", schema, table);
				pgddl->schema = pstrdup(schema);
				pgddl->tbname = pstrdup(table);
			}
			else if (!schema && table)
			{
				/* table stays as table but no schema */
				schema = pstrdup("public");
				appendStringInfo(&strinfo, "TRUNCATE TABLE \"%s\";", table);
				pgddl->schema = pstrdup("public");
				pgddl->tbname = pstrdup(table);
			}
		}
		else
		{
			char * idcopy = pstrdup(dbzddl->id);
			splitIdString(idcopy, &db, &schema, &table, true);

			/* we want to normalize according to letter casing strategy here */
			fc_normalize_name(synchdb_letter_casing_strategy, idcopy, strlen(idcopy));

			/* database and table must be present. schema is optional */
			if (!db || !table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "malformed id field in dbz change event: %s",
						dbzddl->id);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}
			/* make schema points to db */
			schema = db;
			appendStringInfo(&strinfo, "TRUNCATE TABLE \"%s\".\"%s\";", schema, table);
			pgddl->schema = pstrdup(schema);
			pgddl->tbname = pstrdup(table);
		}
		/* no column information needed for TRUNCATE */
		pgddl->columns = NULL;
	}
	else
	{
		elog(WARNING, "unsupported conversion for DDL type %d", dbzddl->type);
		destroyPGDDL(pgddl);
		if (rel)
			table_close(rel, NoLock);
		return NULL;
	}

	pgddl->ddlquery = pstrdup(strinfo.data);

	/* free the data inside strinfo as we no longer needs it */
	pfree(strinfo.data);

	elog(DEBUG1, "pgsql: %s ", pgddl->ddlquery);
	if (rel)
		table_close(rel, NoLock);
	return pgddl;
}

bool
fc_translate_datatype_rules(const SynchdbRuleSnapshot *rules, ConnectorType type,
                           const char *source, int *length, int *scale,
                           char **target, int *target_length)
{
 const SynchdbRuleTypeMapping *mapping;
 char *lookup = pstrdup(source);
 bool removed = false;

 if ((type == TYPE_MYSQL || type == TYPE_SQLSERVER) && *length == 1 &&
     !pg_strcasecmp(source, "bit"))
 {
  pfree(lookup);
  lookup = psprintf("%s(1)", source);
 }
 else if (type == TYPE_ORACLE || type == TYPE_OLR)
 {
  remove_precision(lookup, &removed);
  if ((!pg_strcasecmp(lookup, "interval day to second") && *scale > 0) || removed)
  {
   *length = *scale;
   *scale = 0;
  }
  if (!pg_strcasecmp(lookup, "number") && *scale == 0)
  {
   char *numeric = psprintf("%s(%d,0)", lookup, *length);

   pfree(lookup);
   lookup = numeric;
  }
 }
 mapping = synchdb_rule_type(rules, type, NULL, lookup, false);
 if (!mapping)
  mapping = synchdb_rule_type(rules, type, NULL, source, false);
 pfree(lookup);
 if (!mapping)
  return false;
 *target = pstrdup(mapping->name);
 *target_length = mapping->length;
 return true;
}

bool
fc_translate_datatype(ConnectorType type, const char *source,
                      int *length, int *scale, char **target, int *target_length)
{
 /* The SQL helper has no connector identity. It uses readonly builtins;
  * FDW plans apply their own frozen overrides rather than worker globals. */
 return fc_translate_datatype_rules(NULL, type, source, length, scale,
                                    target, target_length);
}

void
fc_normalize_name(LetterCasingStrategy strategy, char * name, int len)
{
	int i = 0;

	if (!name || len == 0)
		return;

	switch(strategy)
	{
		case LCS_NORMALIZE_LOWERCASE:
		{
			for (i = 0; i < len; i++)
				name[i] = (char) pg_tolower((unsigned char) name[i]);
			break;
		}
		case LCS_NORMALIZE_UPPERCASE:
		{
			for (i = 0; i < len; i++)
				name[i] = (char) pg_toupper((unsigned char) name[i]);
			break;
		}
		default:
		case LCS_AS_IS:
		{
			break;
		}
	}
}
