/* Known logical values are checked before target binding or conversion. */
#include "postgres.h"
#include "executor/pg_compat.h"

#include "source/debezium_logical.h"
#include "common/base64.h"
#include "nodes/miscnodes.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"

typedef enum DbzLogicalKind
{
	DBZ_LOGICAL_OTHER,
	DBZ_LOGICAL_DECIMAL,
	DBZ_LOGICAL_VARIABLE_DECIMAL,
	DBZ_LOGICAL_POINT,
	DBZ_LOGICAL_GEOMETRY
} DbzLogicalKind;

typedef struct LogicalMember
{
	const char *name;
	const char *wire_type;
	bool optional;
} LogicalMember;

static const LogicalMember decimal_members[] = {
	{"scale", "int32", false}, {"value", "bytes", false}
};
static const LogicalMember point_members[] = {
	{"x", "double", false}, {"y", "double", false},
	{"wkb", "bytes", true}, {"srid", "int32", true}
};
static const LogicalMember geometry_members[] = {
	{"wkb", "bytes", false}, {"srid", "int32", true}
};

static void
logical_malformed(const SynchdbDbzField *field, const char *member,
				  const char *expected)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_EXCEPTION),
			 errmsg("malformed Debezium logical column \"%s\": %s must be %s",
					field->name, member, expected)));
}

static bool
logical_string_equals(const JsonbValue *value, const char *text)
{
	return value && value->type == jbvString &&
		(Size) value->val.string.len == strlen(text) &&
		memcmp(value->val.string.val, text, value->val.string.len) == 0;
}

static JsonbValue *
logical_member(JsonbContainer *object, const char *name, JsonbValue *buffer)
{
	return getKeyJsonValueFromContainer(object, name, strlen(name), buffer);
}

static DbzLogicalKind
logical_kind(const SynchdbDbzField *field)
{
	const char *name = field->logical_name;

	if (name == NULL)
		return DBZ_LOGICAL_OTHER;
	if (strcmp(name, "org.apache.kafka.connect.data.Decimal") == 0)
		return DBZ_LOGICAL_DECIMAL;
	if (strcmp(name, "io.debezium.data.VariableScaleDecimal") == 0)
		return DBZ_LOGICAL_VARIABLE_DECIMAL;
	if (strcmp(name, "io.debezium.data.geometry.Point") == 0)
		return DBZ_LOGICAL_POINT;
	if (strcmp(name, "io.debezium.data.geometry.Geometry") == 0 ||
		strcmp(name, "io.debezium.data.geometry.Geography") == 0)
		return DBZ_LOGICAL_GEOMETRY;
	return DBZ_LOGICAL_OTHER;
}

static const LogicalMember *
logical_members(DbzLogicalKind kind, int *count)
{
	if (kind == DBZ_LOGICAL_VARIABLE_DECIMAL)
	{
		*count = lengthof(decimal_members);
		return decimal_members;
	}
	if (kind == DBZ_LOGICAL_POINT)
	{
		*count = lengthof(point_members);
		return point_members;
	}
	*count = lengthof(geometry_members);
	return geometry_members;
}

/* Kafka's Decimal scale is an Integer.parseInt string, not SQL numeric text. */
static void
logical_scale_parameter(const SynchdbDbzField *field)
{
	JsonbValue buffer;
	JsonbValue *value = field->parameters ?
		logical_member(&field->parameters->root, "scale", &buffer) : NULL;
	const char *text;
	int length;
	int position = 0;
	uint32 limit = PG_INT32_MAX;
	uint32 parsed = 0;

	if (value == NULL || value->type != jbvString || value->val.string.len == 0)
		logical_malformed(field, "schema.parameters.scale", "an int32 decimal string");
	text = value->val.string.val;
	length = value->val.string.len;
	if (text[position] == '-' || text[position] == '+')
	{
		if (text[position] == '-')
			limit++;
		position++;
	}
	if (position == length)
		logical_malformed(field, "schema.parameters.scale", "an int32 decimal string");
	for (; position < length; position++)
	{
		uint32 digit;

		if (text[position] < '0' || text[position] > '9')
			logical_malformed(field, "schema.parameters.scale", "an int32 decimal string");
		digit = text[position] - '0';
		if (parsed > (limit - digit) / 10)
			logical_malformed(field, "schema.parameters.scale", "an int32 decimal string");
		parsed = parsed * 10 + digit;
	}
}

void
synchdb_dbz_validate_logical_schema(const SynchdbDbzField *field,
								  JsonbContainer *schema)
{
	DbzLogicalKind kind = logical_kind(field);
	const LogicalMember *members;
	JsonbValue buffer;
	JsonbValue *value;
	JsonbContainer *fields;
	unsigned int seen = 0;
	unsigned int position;
	int count;

	if (kind == DBZ_LOGICAL_OTHER)
		return;
	if (kind == DBZ_LOGICAL_DECIMAL)
	{
		if (strcmp(field->wire_type, "bytes") != 0)
			logical_malformed(field, "schema.type", "bytes for Connect Decimal");
		logical_scale_parameter(field);
		return;
	}
	if (strcmp(field->wire_type, "struct") != 0)
		logical_malformed(field, "schema.type", "struct for this logical type");
	members = logical_members(kind, &count);
	value = logical_member(schema, "fields", &buffer);
	if (value == NULL || value->type != jbvBinary ||
		!JsonContainerIsArray(value->val.binary.data) ||
		JsonContainerIsScalar(value->val.binary.data))
		logical_malformed(field, "schema.fields", "the complete logical field array");
	fields = value->val.binary.data;
	for (position = 0; position < JsonContainerSize(fields); position++)
	{
		JsonbValue *definition = getIthJsonbValueFromContainer(fields, position);
		JsonbContainer *object;
		JsonbValue *name;
		int member;

		if (definition->type != jbvBinary ||
			!JsonContainerIsObject(definition->val.binary.data))
			logical_malformed(field, "schema.fields[]", "an object");
		object = definition->val.binary.data;
		name = logical_member(object, "field", &buffer);
		for (member = 0; member < count; member++)
			if (logical_string_equals(name, members[member].name))
				break;
		if (member == count || (seen & (1U << member)) != 0)
			logical_malformed(field, "schema.fields", "known logical fields without duplicates");
		seen |= 1U << member;
		value = logical_member(object, "type", &buffer);
		if (!logical_string_equals(value, members[member].wire_type) &&
			!(strcmp(members[member].wire_type, "double") == 0 &&
			  logical_string_equals(value, "float64")))
			logical_malformed(field, members[member].name, "the declared logical wire type");
		value = logical_member(object, "optional", &buffer);
		if (value == NULL || value->type != jbvBool ||
			value->val.boolean != members[member].optional)
			logical_malformed(field, members[member].name, "the declared logical optional flag");
	}
	if (seen != (1U << count) - 1)
		logical_malformed(field, "schema.fields", "the complete logical field array");
}

static void
logical_int32(const SynchdbDbzField *field, const char *member,
			  const JsonbValue *value)
{
	Datum number;
	int32 integer;

	if (value->type != jbvNumeric)
		logical_malformed(field, member, "an exact int32 number");
	number = NumericGetDatum(value->val.numeric);
	if (DatumGetBool(DirectFunctionCall2(numeric_lt, number,
					 DirectFunctionCall1(int4_numeric, Int32GetDatum(PG_INT32_MIN)))) ||
		DatumGetBool(DirectFunctionCall2(numeric_gt, number,
					 DirectFunctionCall1(int4_numeric, Int32GetDatum(PG_INT32_MAX)))))
		logical_malformed(field, member, "an exact int32 number");
	integer = DatumGetInt32(DirectFunctionCall1(numeric_int4, number));
	if (!DatumGetBool(DirectFunctionCall2(numeric_eq, number,
						DirectFunctionCall1(int4_numeric, Int32GetDatum(integer)))))
		logical_malformed(field, member, "an exact int32 number");
}

static void
logical_bytes(const SynchdbDbzField *field, const char *member,
			  const JsonbValue *value, bool nonempty)
{
	unsigned char *decoded;
	int capacity;
	int length;

	if (value->type != jbvString)
		logical_malformed(field, member, "a base64 bytes string");
	capacity = pg_b64_dec_len(value->val.string.len);
	decoded = palloc(Max(capacity, 1));
	length = synchdb_pg_base64_decode(value->val.string.val, value->val.string.len,
									 decoded, capacity);
	pfree(decoded);
	if (length < 0 || (nonempty && length == 0))
		logical_malformed(field, member, nonempty ?
						  "a nonempty base64 bytes string" : "a base64 bytes string");
}

static void
logical_float64(const SynchdbDbzField *field, const char *member,
				const JsonbValue *value)
{
	if (value->type == jbvNumeric)
	{
		ErrorSaveContext errors = {.type = T_ErrorSaveContext};
		char *text = DatumGetCString(DirectFunctionCall1(numeric_out,
												 NumericGetDatum(value->val.numeric)));

		/* PostgreSQL rejects overflow/underflow, accepting finite subnormals.
		 * Keep malformed logical values source errors rather than target errors. */
		(void) float8in_internal(text, NULL, "double precision", text, (Node *) &errors);
		pfree(text);
		if (errors.error_occurred)
			logical_malformed(field, member, "a number within the float64 range");
		return;
	}
	if (logical_string_equals(value, "NaN") ||
		logical_string_equals(value, "Infinity") ||
		logical_string_equals(value, "-Infinity"))
		return;
	logical_malformed(field, member, "a float64 number or Kafka nonfinite string");
}

void
synchdb_dbz_validate_logical_value(const SynchdbDbzField *field)
{
	DbzLogicalKind kind = logical_kind(field);
	const LogicalMember *members;
	JsonbContainer *object;
	JsonbValue buffer;
	JsonbIterator *iterator;
	JsonbIteratorToken token;
	int count;
	int member;

	if (kind == DBZ_LOGICAL_OTHER || field->value.kind == SYNCHDB_DBZ_VALUE_NULL)
		return;
	if (kind == DBZ_LOGICAL_DECIMAL)
	{
		logical_bytes(field, "value", &field->value.raw, true);
		return;
	}
	if (field->value.kind != SYNCHDB_DBZ_VALUE_OBJECT ||
		field->value.raw.type != jbvBinary ||
		!JsonContainerIsObject(field->value.raw.val.binary.data))
		logical_malformed(field, "value", "a logical struct object");
	object = field->value.raw.val.binary.data;
	members = logical_members(kind, &count);
	iterator = JsonbIteratorInit(object);
	while ((token = JsonbIteratorNext(&iterator, &buffer, true)) != WJB_DONE)
	{
		if (token != WJB_KEY)
			continue;
		for (member = 0; member < count; member++)
			if (logical_string_equals(&buffer, members[member].name))
				break;
		if (member == count)
			logical_malformed(field, "value", "only declared logical members");
	}
	for (member = 0; member < count; member++)
	{
		const LogicalMember *definition = &members[member];
		JsonbValue *value = logical_member(object, definition->name, &buffer);

		if (value == NULL || value->type == jbvNull)
		{
			if (!definition->optional)
				logical_malformed(field, definition->name, "present and nonnull");
			continue;
		}
		if (strcmp(definition->wire_type, "int32") == 0)
			logical_int32(field, definition->name, value);
		else if (strcmp(definition->wire_type, "bytes") == 0)
			logical_bytes(field, definition->name, value,
						  kind == DBZ_LOGICAL_VARIABLE_DECIMAL);
		else
			logical_float64(field, definition->name, value);
	}
}
