/*
 * A placeholder can collide with real data. Without source provenance it must
 * fail closed, including creates/deletes derived from a PostgreSQL PK change.
 * Do not search arbitrary JSON or recursively turn nested data into missing.
 */
#include "postgres.h"

#include "source/debezium_unavailable.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"

static bool
placeholder_string(const JsonbValue *value, const char *text)
{
	return value && value->type == jbvString &&
		(Size) value->val.string.len == strlen(text) &&
		memcmp(value->val.string.val, text, strlen(text)) == 0;
}

static bool
array_item_is(const SynchdbDbzField *field, const char *wire_type,
			  const char *logical_name)
{
	JsonbValue buffer;
	JsonbValue *member;

	/* Old generic JSON array schemas did not retain items. Do not let that
	 * ambiguity turn a known placeholder representation into target data. */
	if (field->item_schema == NULL)
		return true;
	member = getKeyJsonValueFromContainer(&field->item_schema->root, "type", 4, &buffer);
	if (!placeholder_string(member, wire_type))
		return false;
	member = getKeyJsonValueFromContainer(&field->item_schema->root, "name", 4, &buffer);
	return logical_name ? member && placeholder_string(member, logical_name) :
		member == NULL || member->type == jbvNull;
}

static bool
postgres_array_placeholder(const SynchdbDbzField *field)
{
	static const char marker[] = "__debezium_unavailable_value";
	JsonbContainer *array = field->value.raw.val.binary.data;
	unsigned int count = JsonContainerSize(array);
	unsigned int position;

	if (count == 1)
	{
		JsonbValue *value = getIthJsonbValueFromContainer(array, 0);

		if ((placeholder_string(value, marker) &&
			 (array_item_is(field, "string", NULL) ||
			  array_item_is(field, "string", "io.debezium.data.Json"))) ||
			(placeholder_string(value, "X19kZWJleml1bV91bmF2YWlsYWJsZV92YWx1ZQ==") &&
			 array_item_is(field, "bytes", NULL)) ||
			(placeholder_string(value, "b68a35a7-17ad-35b3-af2a-ae46edb4545a") &&
			 array_item_is(field, "string", "io.debezium.data.Uuid")))
			return true;
	}
	/* int4[], int8[] and date[] use the placeholder's signed UTF8 bytes.
	 * The configured marker is ASCII, so every byte here is nonnegative. */
	if (count != sizeof(marker) - 1)
		return false;
	if (!array_item_is(field, "int32", NULL) &&
		!array_item_is(field, "int32", "io.debezium.time.Date") &&
		!array_item_is(field, "int64", NULL))
		return false;
	for (position = 0; position < count; position++)
	{
		JsonbValue *value = getIthJsonbValueFromContainer(array, position);
		Datum expected = DirectFunctionCall1(int8_numeric,
										Int64GetDatum((unsigned char) marker[position]));

		if (value->type != jbvNumeric ||
			!DatumGetBool(DirectFunctionCall2(numeric_eq,
								NumericGetDatum(value->val.numeric), expected)))
			return false;
	}
	return true;
}

static bool
postgres_hstore_placeholder(const SynchdbDbzField *field)
{
	static const char marker[] = "__debezium_unavailable_value";
	JsonbValue buffer;
	JsonbValue *value;
	JsonbContainer *map;

	if (field->value.kind == SYNCHDB_DBZ_VALUE_OBJECT &&
		strcmp(field->wire_type, "map") == 0)
	{
		map = field->value.raw.val.binary.data;
		if (JsonContainerSize(map) != 1)
			return false;
		value = getKeyJsonValueFromContainer(map, marker, sizeof(marker) - 1, &buffer);
		return value && placeholder_string(value, marker);
	}
	/* The pinned converter uses a compact JsonGenerator for HSTORE JSON.
	 * Match its whole value; never parse or scan unrelated JSON content. */
	return field->logical_name &&
		strcmp(field->logical_name, "io.debezium.data.Json") == 0 &&
		placeholder_string(&field->value.raw,
			"{\"__debezium_unavailable_value\":\"__debezium_unavailable_value\"}");
}

bool
synchdb_dbz_value_unavailable(const SynchdbDbzField *field,
								const char *connector, char operation)
{
	bool postgres = strcmp(connector, "postgresql") == 0 ||
		strcmp(connector, "postgres") == 0;
	bool oracle = strcmp(connector, "oracle") == 0 ||
		strcmp(connector, "olr") == 0;

	if (postgres && (operation == 'c' || operation == 'u' || operation == 'd'))
	{
		if (postgres_hstore_placeholder(field))
			return true;
		if (field->value.kind == SYNCHDB_DBZ_VALUE_ARRAY &&
			strcmp(field->wire_type, "array") == 0)
			return postgres_array_placeholder(field);
		if (strcmp(field->wire_type, "string") == 0)
			return placeholder_string(&field->value.raw, "__debezium_unavailable_value");
		if (strcmp(field->wire_type, "bytes") == 0)
			return placeholder_string(&field->value.raw,
								  "X19kZWJleml1bV91bmF2YWlsYWJsZV92YWx1ZQ==");
	}
	else if (oracle && (operation == 'c' || operation == 'r' ||
						 operation == 'u' || operation == 'd'))
	{
		if (strcmp(field->wire_type, "string") == 0)
			return placeholder_string(&field->value.raw, "__synchdb_unavailable_value");
		if (strcmp(field->wire_type, "bytes") == 0)
			return placeholder_string(&field->value.raw,
								  "X19zeW5jaGRiX3VuYXZhaWxhYmxlX3ZhbHVl");
	}
	return false;
}
