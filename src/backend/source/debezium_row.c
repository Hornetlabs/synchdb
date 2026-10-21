/* Decode source row images without consulting target metadata or rules. */
#include "postgres.h"

#include "source/debezium_row.h"
#include "source/debezium_logical.h"
#include "source/debezium_unavailable.h"
#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

typedef struct DbzRowFieldEntry
{
	char *name;
	SynchdbDbzField *field;
} DbzRowFieldEntry;

static void
row_malformed(const char *field, const char *expected)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_EXCEPTION),
			 errmsg("malformed Debezium row: %s must be %s", field, expected)));
}

static JsonbValue *
row_member(JsonbContainer *object, const char *key, JsonbValue *value)
{
	return getKeyJsonValueFromContainer(object, key, strlen(key), value);
}

static JsonbContainer *
row_object(const JsonbValue *value, const char *field)
{
	if (value == NULL || value->type != jbvBinary ||
		!JsonContainerIsObject(value->val.binary.data))
		row_malformed(field, "an object");
	return value->val.binary.data;
}

static char *
row_string(const JsonbValue *value, const char *field, bool required)
{
	if (!required && (value == NULL || value->type == jbvNull))
		return NULL;
	if (value == NULL || value->type != jbvString ||
		(required && value->val.string.len == 0))
		row_malformed(field, required ? "a nonempty string" : "a string or null");
	return pnstrdup(value->val.string.val, value->val.string.len);
}

/* Pointer keys retain the complete, case-sensitive source spelling. */
static uint32
row_field_hash(const void *key, Size keysize)
{
	const char *name = *((const char *const *) key);

	return hash_bytes((const unsigned char *) name, strlen(name));
}

static int
row_field_compare(const void *left, const void *right, Size keysize)
{
	return strcmp(*((const char *const *) left),
				  *((const char *const *) right));
}

static HTAB *
row_field_index(const char *name, long count)
{
	HASHCTL control = {0};

	control.keysize = sizeof(char *);
	control.entrysize = sizeof(DbzRowFieldEntry);
	control.hash = row_field_hash;
	control.match = row_field_compare;
	control.hcxt = CurrentMemoryContext;
	return hash_create(name, Max(count, 16), &control,
					   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
}

static void
row_check_wire_type(const char *wire_type)
{
	static const char *const supported[] = {
		"string", "bytes", "boolean", "int8", "int16", "int32", "int64",
		"float32", "float64", "float", "double", "array", "map", "struct"
	};
	size_t index;

	for (index = 0; index < lengthof(supported); index++)
		if (strcmp(wire_type, supported[index]) == 0)
			return;
	row_malformed("column schema.type", "a supported Kafka Connect type");
}

static SynchdbDbzField *
row_decode_field(JsonbContainer *schema)
{
	SynchdbDbzField *field = palloc0(sizeof(*field));
	JsonbValue buffer;
	JsonbValue *member;

	field->name = row_string(row_member(schema, "field", &buffer),
							 "column schema.field", true);
	field->wire_type = row_string(row_member(schema, "type", &buffer),
								  "column schema.type", true);
	row_check_wire_type(field->wire_type);
	field->logical_name = row_string(row_member(schema, "name", &buffer),
									 "column schema.name", false);
	member = row_member(schema, "optional", &buffer);
	if (member != NULL)
	{
		if (member->type != jbvBool)
			row_malformed("column schema.optional", "a boolean");
		field->optional = member->val.boolean;
	}
	member = row_member(schema, "parameters", &buffer);
	if (member != NULL && member->type != jbvNull)
	{
		(void) row_object(member, "column schema.parameters");
		field->parameters = JsonbValueToJsonb(member);
	}
	if (strcmp(field->wire_type, "array") == 0)
	{
		member = row_member(schema, "items", &buffer);
		if (member != NULL && member->type != jbvNull)
		{
			(void) row_object(member, "column schema.items");
			field->item_schema = JsonbValueToJsonb(member);
		}
	}
	synchdb_dbz_validate_logical_schema(field, schema);
	return field;
}

static void
row_check_integer(const SynchdbDbzField *field, Numeric number)
{
	int64 minimum;
	int64 maximum;
	int64 integer;
	Datum original = NumericGetDatum(number);
	Datum integral;
	bool limited = true;

	if (strcmp(field->wire_type, "int8") == 0)
	{
		minimum = PG_INT8_MIN;
		maximum = PG_INT8_MAX;
	}
	else if (strcmp(field->wire_type, "int16") == 0)
	{
		minimum = PG_INT16_MIN;
		maximum = PG_INT16_MAX;
	}
	else if (strcmp(field->wire_type, "int32") == 0)
	{
		minimum = PG_INT32_MIN;
		maximum = PG_INT32_MAX;
	}
	else if (strcmp(field->wire_type, "int64") == 0)
	{
		minimum = PG_INT64_MIN;
		maximum = PG_INT64_MAX;
		limited = false;
	}
	else
		return;

	/* Keep narrow-wire overflows source data errors even beyond int64 range. */
	if (limited &&
		(DatumGetBool(DirectFunctionCall2(numeric_lt, original,
					 DirectFunctionCall1(int8_numeric, Int64GetDatum(minimum)))) ||
		 DatumGetBool(DirectFunctionCall2(numeric_gt, original,
					 DirectFunctionCall1(int8_numeric, Int64GetDatum(maximum))))))
		goto invalid;
	/* numeric_int8 rounds, so compare its round trip to reject fractions. */
	integer = DatumGetInt64(DirectFunctionCall1(numeric_int8, original));
	integral = DirectFunctionCall1(int8_numeric, Int64GetDatum(integer));
	if (DatumGetBool(DirectFunctionCall2(numeric_eq, original, integral)))
		return;

invalid:
	ereport(ERROR,
			(errcode(ERRCODE_DATA_EXCEPTION),
			 errmsg("malformed Debezium row: value of column \"%s\" must be an exact %s within its signed range",
					field->name, field->wire_type)));
}

static void
row_decode_value(SynchdbDbzField *field, const JsonbValue *raw,
				 const char *connector, char operation)
{
	SynchdbDbzValue *value = &field->value;
	const char *type = field->wire_type;
	bool compatible = false;

	value->raw = *raw;
	switch (raw->type)
	{
		case jbvNull:
			value->kind = SYNCHDB_DBZ_VALUE_NULL;
			return;
		case jbvString:
			value->kind = SYNCHDB_DBZ_VALUE_STRING;
			compatible = strcmp(type, "string") == 0 || strcmp(type, "bytes") == 0;
			/* Kafka's JSON converter quotes non-finite floating-point values. */
			if (!compatible && (strcmp(type, "float") == 0 ||
				strcmp(type, "double") == 0 || strcmp(type, "float32") == 0 ||
				strcmp(type, "float64") == 0))
			{
				static const char *const special[] = {"NaN", "Infinity", "-Infinity"};
				size_t index;

				for (index = 0; index < lengthof(special); index++)
					if ((Size) raw->val.string.len == strlen(special[index]) &&
						memcmp(raw->val.string.val, special[index], raw->val.string.len) == 0)
					{
						compatible = true;
						break;
					}
			}
			break;
		case jbvNumeric:
			value->kind = SYNCHDB_DBZ_VALUE_NUMBER;
			compatible = strcmp(type, "int8") == 0 || strcmp(type, "int16") == 0 ||
				strcmp(type, "int32") == 0 || strcmp(type, "int64") == 0 ||
				strcmp(type, "float32") == 0 || strcmp(type, "float64") == 0 ||
				strcmp(type, "float") == 0 || strcmp(type, "double") == 0;
			if (compatible)
				row_check_integer(field, raw->val.numeric);
			break;
		case jbvBool:
			value->kind = SYNCHDB_DBZ_VALUE_BOOLEAN;
			compatible = strcmp(type, "boolean") == 0;
			break;
		case jbvBinary:
			if (JsonContainerIsObject(raw->val.binary.data))
			{
				value->kind = SYNCHDB_DBZ_VALUE_OBJECT;
				compatible = strcmp(type, "struct") == 0 || strcmp(type, "map") == 0;
			}
			else if (JsonContainerIsArray(raw->val.binary.data) &&
					 !JsonContainerIsScalar(raw->val.binary.data))
			{
				value->kind = SYNCHDB_DBZ_VALUE_ARRAY;
				compatible = strcmp(type, "array") == 0 || strcmp(type, "map") == 0;
			}
			break;
		default:
			break;
	}
	if (!compatible)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("malformed Debezium row: value of column \"%s\" does not match schema type \"%s\"",
						field->name, field->wire_type)));
	if (synchdb_dbz_value_unavailable(field, connector, operation))
		value->kind = SYNCHDB_DBZ_VALUE_UNAVAILABLE;
	else
		synchdb_dbz_validate_logical_value(field);
}

static void
row_decode_image(Jsonb *payload, const char *name, JsonbContainer *schema,
				 SynchdbDbzImage *image, const char *connector, char operation)
{
	JsonbValue buffer;
	JsonbValue *member = row_member(&payload->root, name, &buffer);
	JsonbContainer *object = NULL;
	JsonbContainer *columns;
	HTAB *index;
	unsigned int position;
	JsonbIterator *iterator;
	JsonbIteratorToken token;
	JsonbValue raw;
	char *key = NULL;

	if (schema == NULL)
	{
		if (member != NULL)
			row_malformed(name, "described by its own source schema");
		return;
	}
	if (member != NULL && member->type == jbvNull)
		image->state = SYNCHDB_DBZ_IMAGE_NULL;
	else if (member != NULL)
	{
		object = row_object(member, name);
		image->state = SYNCHDB_DBZ_IMAGE_OBJECT;
	}
	member = row_member(schema, "fields", &buffer);
	if (member == NULL || member->type != jbvBinary ||
		!JsonContainerIsArray(member->val.binary.data) ||
		JsonContainerIsScalar(member->val.binary.data))
		row_malformed("row schema.fields", "an array");
	columns = member->val.binary.data;
	index = row_field_index("Debezium image fields", JsonContainerSize(columns));
	for (position = 0; position < JsonContainerSize(columns); position++)
	{
		JsonbValue *column = getIthJsonbValueFromContainer(columns, position);
		SynchdbDbzField *field = row_decode_field(row_object(column, "column schema"));
		DbzRowFieldEntry *entry;
		bool found;

		entry = hash_search(index, &field->name, HASH_ENTER, &found);
		if (found)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_COLUMN),
					 errmsg("duplicate Debezium source column \"%s\" in %s schema",
							field->name, name)));
		entry->field = field;
		if (image->state == SYNCHDB_DBZ_IMAGE_OBJECT)
			image->fields = lappend(image->fields, field);
	}
	/* Null and absent images still require any supplied schema to be valid. */
	if (image->state != SYNCHDB_DBZ_IMAGE_OBJECT)
	{
		hash_destroy(index);
		return;
	}
	iterator = JsonbIteratorInit(object);
	while ((token = JsonbIteratorNext(&iterator, &raw, true)) != WJB_DONE)
	{
		if (token == WJB_KEY)
		{
			if (key != NULL)
				pfree(key);
			key = pnstrdup(raw.val.string.val, raw.val.string.len);
		}
		else if (token == WJB_VALUE)
		{
			DbzRowFieldEntry *entry = hash_search(index, &key, HASH_FIND, NULL);

			if (entry == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("Debezium %s column \"%s\" is absent from its source schema",
								name, key)));
			row_decode_value(entry->field, &raw, connector, operation);
		}
	}
	if (key != NULL)
		pfree(key);
	hash_destroy(index);
}

void
synchdb_dbz_decode_row(const SynchdbDbzEvent *event, SynchdbDbzRow *row)
{
	SynchdbDbzRow decoded = {0};
	JsonbValue buffer;
	JsonbValue *member;
	JsonbContainer *schema;
	JsonbContainer *fields;
	JsonbContainer *before = NULL;
	JsonbContainer *after = NULL;
	HTAB *index;
	unsigned int position;

	if (event == NULL || row == NULL || event->kind != SYNCHDB_DBZ_EVENT_ROW ||
		event->document == NULL || event->payload == NULL || event->source == NULL)
		elog(ERROR, "Debezium row decoder requires a decoded row event");
	decoded.db = row_string(row_member(&event->source->root, "db", &buffer),
							"payload.source.db", true);
	decoded.schema = row_string(row_member(&event->source->root, "schema", &buffer),
								"payload.source.schema", false);
	decoded.table = row_string(row_member(&event->source->root, "table", &buffer),
							   "payload.source.table", true);
	decoded.op = event->operation;
	schema = row_object(row_member(&event->document->root, "schema", &buffer),
						"schema");
	member = row_member(schema, "fields", &buffer);
	if (member == NULL || member->type != jbvBinary ||
		!JsonContainerIsArray(member->val.binary.data) ||
		JsonContainerIsScalar(member->val.binary.data))
		row_malformed("schema.fields", "an array");
	fields = member->val.binary.data;
	index = row_field_index("Debezium envelope schema fields", JsonContainerSize(fields));
	for (position = 0; position < JsonContainerSize(fields); position++)
	{
		JsonbValue *item = getIthJsonbValueFromContainer(fields, position);
		JsonbContainer *field = row_object(item, "schema.fields[]");
		char *name = row_string(row_member(field, "field", &buffer),
								"schema.fields[].field", true);
		bool found;

		(void) hash_search(index, &name, HASH_ENTER, &found);
		if (found)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_COLUMN),
					 errmsg("duplicate Debezium envelope schema field \"%s\"", name)));
		if (strcmp(name, "before") == 0 || strcmp(name, "after") == 0)
		{
			SynchdbDbzField *definition = row_decode_field(field);

			if (strcmp(definition->wire_type, "struct") != 0)
				row_malformed("row schema.type", "struct");
			if (strcmp(name, "before") == 0)
				before = field;
			else
				after = field;
		}
	}
	hash_destroy(index);
	row_decode_image(event->payload, "before", before, &decoded.before,
					 event->connector, event->operation);
	row_decode_image(event->payload, "after", after, &decoded.after,
					 event->connector, event->operation);
	*row = decoded;
}
