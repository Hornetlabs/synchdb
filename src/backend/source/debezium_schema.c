/* Decode Debezium's relational schema images without guessing target changes. */
#include "postgres.h"

#include <ctype.h>

#include "lib/stringinfo.h"
#include "source/debezium_schema.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"

static void
schema_malformed(const char *field, const char *expected)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_EXCEPTION),
			 errmsg("malformed Debezium schema change: %s must be %s", field, expected)));
}

static JsonbValue *
schema_member(Jsonb *object, const char *key, JsonbValue *buffer)
{
	return object ? getKeyJsonValueFromContainer(&object->root, key, strlen(key), buffer) : NULL;
}

static Jsonb *
schema_object(const JsonbValue *value, const char *field, bool required)
{
	if (value == NULL || value->type == jbvNull)
	{
		if (required)
			schema_malformed(field, "an object");
		return NULL;
	}
	if (value->type != jbvBinary || !JsonContainerIsObject(value->val.binary.data))
		schema_malformed(field, "an object");
	return JsonbValueToJsonb((JsonbValue *) value);
}

static char *
schema_string(const JsonbValue *value, const char *field, bool required)
{
	if (value == NULL || value->type == jbvNull)
	{
		if (required)
			schema_malformed(field, "a nonempty string");
		return NULL;
	}
	if (value->type != jbvString || (required && value->val.string.len == 0))
		schema_malformed(field, required ? "a nonempty string" : "a string or null");
	return pnstrdup(value->val.string.val, value->val.string.len);
}

static bool
schema_boolean(const JsonbValue *value, const char *field, bool required,
			   bool missing_default)
{
	if (value == NULL || value->type == jbvNull)
	{
		if (required)
			schema_malformed(field, "a boolean");
		return missing_default;
	}
	if (value->type != jbvBool)
		schema_malformed(field, "a boolean");
	return value->val.boolean;
}

static int32
schema_integer(const JsonbValue *value, const char *field, bool required,
			   int32 missing_default, bool *present)
{
	Datum original;
	Datum integral;
	int64 number;

	if (present)
		*present = value && value->type != jbvNull;
	if (value == NULL || value->type == jbvNull)
	{
		if (required)
			schema_malformed(field, "an exact int32");
		return missing_default;
	}
	if (value->type != jbvNumeric)
		schema_malformed(field, "an exact int32");
	original = NumericGetDatum(value->val.numeric);
	/* Reject huge/fractional values through numeric comparison before casting. */
	if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, original,
			DirectFunctionCall1(int8_numeric, Int64GetDatum(PG_INT32_MIN)))) < 0 ||
		DatumGetInt32(DirectFunctionCall2(numeric_cmp, original,
			DirectFunctionCall1(int8_numeric, Int64GetDatum(PG_INT32_MAX)))) > 0)
		schema_malformed(field, "an exact int32");
	number = DatumGetInt64(DirectFunctionCall1(numeric_int8, original));
	integral = DirectFunctionCall1(int8_numeric, Int64GetDatum(number));
	if (!DatumGetBool(DirectFunctionCall2(numeric_eq, original, integral)))
		schema_malformed(field, "an exact int32");
	return (int32) number;
}

static void
schema_quote_component(StringInfo buffer, const char *value)
{
	const char *at;

	appendStringInfoChar(buffer, '"');
	for (at = value; *at; at++)
	{
		if (*at == '"')
			appendStringInfoChar(buffer, '"');
		appendStringInfoChar(buffer, *at);
	}
	appendStringInfoChar(buffer, '"');
}

static bool
schema_key_needs_quotes(const char *component)
{
	return component && (component[0] == '\0' || strchr(component, '.') || strchr(component, '"'));
}

char *
synchdb_source_table_key(const char *database, const char *schema, const char *table)
{
	StringInfoData key;
	bool quote;

	if (database == NULL || database[0] == '\0' || table == NULL || table[0] == '\0')
		schema_malformed("source table identity", "a database and table");
	quote = schema_key_needs_quotes(database) || schema_key_needs_quotes(schema) ||
		schema_key_needs_quotes(table);
	if (!quote)
		return schema ? psprintf("%s.%s.%s", database, schema, table) :
			psprintf("%s.%s", database, table);
	initStringInfo(&key);
	schema_quote_component(&key, database);
	appendStringInfoChar(&key, '.');
	if (schema)
	{
		schema_quote_component(&key, schema);
		appendStringInfoChar(&key, '.');
	}
	schema_quote_component(&key, table);
	return key.data;
}

char *
synchdb_source_column_key(const char *table_key, const char *column)
{
	StringInfoData key;

	if (table_key == NULL || column == NULL || column[0] == '\0')
		schema_malformed("source column identity", "a table key and column");
	if (!schema_key_needs_quotes(column))
		return psprintf("%s.%s", table_key, column);
	initStringInfo(&key);
	appendStringInfo(&key, "%s.", table_key);
	schema_quote_component(&key, column);
	return key.data;
}

/* TableId.toDoubleQuotedString doubles embedded quotes. Do not apply SQL
 * identifier case folding or PostgreSQL's 63-byte name truncation here. */
static void
schema_table_id(const char *serialized, const char *connector,
				const char *database_context, SynchdbDbzTableId *id)
{
	const char *at = serialized;
	char *parts[3] = {NULL, NULL, NULL};
	int count = 0;

	while (true)
	{
		StringInfoData component;

		while (isspace((unsigned char) *at))
			at++;
		if (count == 3 || *at == '\0')
			schema_malformed("tableChange.id", "a qualified source table identifier");
		initStringInfo(&component);
		if (*at == '"')
		{
			bool closed = false;

			at++;
			while (*at)
			{
				if (*at == '"')
				{
					if (at[1] != '"')
					{
						at++;
						closed = true;
						break;
					}
					at++;
				}
				appendStringInfoChar(&component, *at++);
			}
			if (!closed)
				schema_malformed("tableChange.id", "balanced double-quoted components");
		}
		else
		{
			const char *start = at;
			const char *end;

			while (*at && *at != '.')
			{
				if (*at == '"')
					schema_malformed("tableChange.id", "quoted or unquoted complete components");
				at++;
			}
			end = at;
			while (end > start && isspace((unsigned char) end[-1]))
				end--;
			appendBinaryStringInfo(&component, start, end - start);
		}
		parts[count++] = component.data;
		while (isspace((unsigned char) *at))
			at++;
		if (*at == '\0')
			break;
		if (*at++ != '.')
			schema_malformed("tableChange.id", "dot-separated source components");
	}
	if (parts[count - 1][0] == '\0' || (count == 3 && parts[0][0] == '\0'))
		schema_malformed("tableChange.id", "nonempty database and table components");
	id->serialized = pstrdup(serialized);
	id->table = parts[count - 1];
	if (count == 3)
	{
		id->database = parts[0];
		id->schema = parts[1];
	}
	else if (count == 2 && strcmp(connector, "mysql") == 0)
		id->database = parts[0];
	else
	{
		if (database_context == NULL || database_context[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("source table identity requires a database context")));
		id->database = pstrdup(database_context);
		if (count == 2)
			id->schema = parts[0];
	}
	id->key = synchdb_source_table_key(id->database, id->schema, id->table);
}

static List *
schema_string_array(const JsonbValue *value, const char *field)
{
	JsonbIterator *iterator;
	JsonbValue element;
	JsonbIteratorToken token;
	List *result = NIL;

	if (value == NULL || value->type == jbvNull)
		return NIL;
	if (value->type != jbvBinary || !JsonContainerIsArray(value->val.binary.data))
		schema_malformed(field, "an array of nonempty strings");
	iterator = JsonbIteratorInit(value->val.binary.data);
	while ((token = JsonbIteratorNext(&iterator, &element, true)) != WJB_DONE)
		if (token == WJB_ELEM)
			result = lappend(result, schema_string(&element, field, true));
	return result;
}

static SynchdbDbzSchemaColumn *
schema_column(Jsonb *document)
{
	SynchdbDbzSchemaColumn *column = palloc0(sizeof(*column));
	JsonbValue value;
	JsonbValue *member;

	column->document = document;
	column->name = schema_string(schema_member(document, "name", &value), "column.name", true);
	column->type_name = schema_string(schema_member(document, "typeName", &value), "column.typeName", true);
	column->type_expression = schema_string(schema_member(document, "typeExpression", &value),
										  "column.typeExpression", false);
	column->charset_name = schema_string(schema_member(document, "charsetName", &value),
									   "column.charsetName", false);
	column->position = schema_integer(schema_member(document, "position", &value),
									 "column.position", true, 0, NULL);
	if (column->position <= 0)
		schema_malformed("column.position", "a positive int32");
	column->length = schema_integer(schema_member(document, "length", &value),
								   "column.length", false, -1, &column->has_length);
	column->scale = schema_integer(schema_member(document, "scale", &value),
								  "column.scale", false, 0, &column->has_scale);
	column->optional = schema_boolean(schema_member(document, "optional", &value),
									 "column.optional", true, false);
	column->auto_incremented = schema_boolean(schema_member(document, "autoIncremented", &value),
											 "column.autoIncremented", false, false);
	column->generated = schema_boolean(schema_member(document, "generated", &value),
									  "column.generated", false, false);
	column->default_expression = schema_string(schema_member(document, "defaultValueExpression", &value),
											   "column.defaultValueExpression", false);
	member = schema_member(document, "hasDefaultValue", &value);
	column->has_default = schema_boolean(member, "column.hasDefaultValue", false,
									 column->default_expression != NULL);
	if (!column->has_default && column->default_expression != NULL)
		schema_malformed("column.hasDefaultValue", "true when a default expression is present");
	column->enum_values = schema_string_array(schema_member(document, "enumValues", &value), "column.enumValues");
	/* Keep otherwise optional source details typed, even though baseline DDL
	 * currently does not reproduce all vendor-specific metadata. */
	(void) schema_integer(schema_member(document, "jdbcType", &value), "column.jdbcType", false, 0, NULL);
	(void) schema_integer(schema_member(document, "nativeType", &value), "column.nativeType", false, 0, NULL);
	(void) schema_string(schema_member(document, "comment", &value), "column.comment", false);
	return column;
}

static int
schema_compare_column_name(const void *left, const void *right)
{
	return strcmp((*(const SynchdbDbzSchemaColumn *const *) left)->name,
				  (*(const SynchdbDbzSchemaColumn *const *) right)->name);
}

static int
schema_compare_column_position(const void *left, const void *right)
{
	int32 a = (*(const SynchdbDbzSchemaColumn *const *) left)->position;
	int32 b = (*(const SynchdbDbzSchemaColumn *const *) right)->position;

	return a < b ? -1 : a > b ? 1 : 0;
}

static int
schema_compare_strings(const void *left, const void *right)
{
	return strcmp(*(const char *const *) left, *(const char *const *) right);
}

static int
schema_compare_tables(const void *left, const void *right)
{
	return strcmp((*(const SynchdbDbzTableChange *const *) left)->id.key,
				  (*(const SynchdbDbzTableChange *const *) right)->id.key);
}

static void
schema_columns(SynchdbDbzTableChange *change, Jsonb *table)
{
	JsonbValue value;
	JsonbValue *member = schema_member(table, "columns", &value);
	JsonbIterator *iterator;
	JsonbIteratorToken token;
	SynchdbDbzSchemaColumn **columns;
	ListCell *cell;
	char **keys;
	int key_count;
	int count;
	int index = 0;

	if (member == NULL || member->type != jbvBinary ||
		!JsonContainerIsArray(member->val.binary.data) ||
		JsonContainerSize(member->val.binary.data) == 0)
		schema_malformed("table.columns", "a nonempty array");
	count = JsonContainerSize(member->val.binary.data);
	columns = palloc(mul_size(count, sizeof(*columns)));
	iterator = JsonbIteratorInit(member->val.binary.data);
	while ((token = JsonbIteratorNext(&iterator, &value, true)) != WJB_DONE)
		if (token == WJB_ELEM)
			columns[index++] = schema_column(schema_object(&value, "table.columns[]", true));
	qsort(columns, count, sizeof(*columns), schema_compare_column_name);
	for (index = 1; index < count; index++)
		if (strcmp(columns[index - 1]->name, columns[index]->name) == 0)
			schema_malformed("table.columns", "distinct column names");
	member = schema_member(table, "primaryKeyColumnNames", &value);
	change->primary_key_known = member != NULL && member->type != jbvNull;
	change->primary_key = schema_string_array(member,
											 "table.primaryKeyColumnNames");
	key_count = list_length(change->primary_key);
	keys = key_count ? palloc(mul_size(key_count, sizeof(*keys))) : NULL;
	index = 0;
	foreach(cell, change->primary_key)
	{
		const char *key = lfirst(cell);
		int lo = 0;
		int hi = count;

		keys[index++] = (char *) key;

		while (lo < hi)
		{
			int mid = lo + (hi - lo) / 2;
			int comparison = strcmp(key, columns[mid]->name);

			if (comparison > 0)
				lo = mid + 1;
			else
				hi = mid;
		}
		if (lo == count || strcmp(key, columns[lo]->name) != 0)
			schema_malformed("table.primaryKeyColumnNames", "names present in table.columns");
	}
	if (key_count > 1)
		qsort(keys, key_count, sizeof(*keys), schema_compare_strings);
	for (index = 1; index < key_count; index++)
		if (strcmp(keys[index - 1], keys[index]) == 0)
			schema_malformed("table.primaryKeyColumnNames", "distinct names");
	if (keys)
		pfree(keys);
	qsort(columns, count, sizeof(*columns), schema_compare_column_position);
	for (index = 0; index < count; index++)
	{
		if (index > 0 && columns[index - 1]->position == columns[index]->position)
			schema_malformed("table.columns", "distinct column positions");
		change->columns = lappend(change->columns, columns[index]);
	}
	pfree(columns);
}

void
synchdb_dbz_decode_schema(const SynchdbDbzEvent *event, SynchdbDbzSchemaChange *output)
{
	SynchdbDbzSchemaChange result = {0};
	JsonbValue value;
	JsonbValue *member;
	JsonbIterator *iterator;
	JsonbIteratorToken token;
	char *database;
	SynchdbDbzTableChange **tables;
	ListCell *cell;
	int count;
	int index;

	if (event == NULL || event->payload == NULL || event->connector == NULL || output == NULL)
		elog(ERROR, "schema decoding requires a source event and output");
	database = schema_string(schema_member(event->source, "db", &value), "source.db", false);
	if (database == NULL || database[0] == '\0')
		database = schema_string(schema_member(event->payload, "databaseName", &value),
								 "payload.databaseName", false);
	member = schema_member(event->payload, "tableChanges", &value);
	if (member == NULL || member->type != jbvBinary || !JsonContainerIsArray(member->val.binary.data))
		schema_malformed("payload.tableChanges", "an array");
	iterator = JsonbIteratorInit(member->val.binary.data);
	while ((token = JsonbIteratorNext(&iterator, &value, true)) != WJB_DONE)
	{
		SynchdbDbzTableChange *change;
		Jsonb *document;
		JsonbValue field;
		char *operation;
		char *identifier;

		if (token != WJB_ELEM)
			continue;
		document = schema_object(&value, "tableChanges[]", true);
		change = palloc0(sizeof(*change));
		change->document = document;
		operation = schema_string(schema_member(document, "type", &field), "tableChange.type", true);
		if (strcmp(operation, "CREATE") == 0 || strcmp(operation, "CREATE TABLE") == 0)
			change->operation = SYNCHDB_DBZ_SCHEMA_CREATE;
		else if (strcmp(operation, "ALTER") == 0 || strcmp(operation, "ALTER TABLE") == 0)
			change->operation = SYNCHDB_DBZ_SCHEMA_ALTER;
		else if (strcmp(operation, "DROP") == 0 || strcmp(operation, "DROP TABLE") == 0)
			change->operation = SYNCHDB_DBZ_SCHEMA_DROP;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported Debezium table change operation \"%s\"", operation)));
		identifier = schema_string(schema_member(document, "id", &field), "tableChange.id", true);
		schema_table_id(identifier, event->connector, database, &change->id);
		identifier = schema_string(schema_member(document, "previousId", &field),
									   "tableChange.previousId", false);
		if (identifier)
		{
			change->previous_id = palloc0(sizeof(*change->previous_id));
			schema_table_id(identifier, event->connector, database, change->previous_id);
			if (change->operation != SYNCHDB_DBZ_SCHEMA_ALTER)
				schema_malformed("tableChange.previousId", "present only for ALTER");
		}
		change->table_document = schema_object(schema_member(document, "table", &field),
											 "tableChange.table", change->operation != SYNCHDB_DBZ_SCHEMA_DROP);
		if (change->operation != SYNCHDB_DBZ_SCHEMA_DROP)
			schema_columns(change, change->table_document);
		result.tables = lappend(result.tables, change);
	}
	count = list_length(result.tables);
	tables = count ? palloc(mul_size(count, sizeof(*tables))) : NULL;
	index = 0;
	foreach(cell, result.tables)
		tables[index++] = lfirst(cell);
	if (count > 1)
		qsort(tables, count, sizeof(*tables), schema_compare_tables);
	for (index = 1; index < count; index++)
		if (strcmp(tables[index - 1]->id.key, tables[index]->id.key) == 0)
			schema_malformed("tableChanges", "distinct table identities in one schema image");
	if (tables)
		pfree(tables);
	*output = result;
}
