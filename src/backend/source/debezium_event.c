/* Validate and classify source envelopes before target binding or execution. */
#include "postgres.h"

#include "source/debezium_event.h"
#include "source/debezium_schema.h"
#include "source/truncate_sql.h"
#include "executor/pg_compat.h"
#include "source/json_input.h"
#include "catalog/pg_type.h"
#include "common/base64.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"
#include "utils/memutils.h"

static void
dbz_malformed(const char *field, const char *expected)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_EXCEPTION),
			 errmsg("malformed Debezium event: %s must be %s", field, expected)));
}

static JsonbValue *
dbz_member(Jsonb *object, const char *key, JsonbValue *value)
{
	return object ? getKeyJsonValueFromContainer(&object->root, key, strlen(key), value) : NULL;
}

static bool
dbz_string_equals(const JsonbValue *value, const char *expected)
{
	return value != NULL && value->type == jbvString &&
		(Size) value->val.string.len == strlen(expected) &&
		memcmp(value->val.string.val, expected, value->val.string.len) == 0;
}

static Jsonb *
dbz_object(const JsonbValue *value, const char *field, bool required)
{
	if (value == NULL || value->type == jbvNull)
	{
		if (required)
			dbz_malformed(field, "an object");
		return NULL;
	}
	if (value->type != jbvBinary ||
		!JsonContainerIsObject(value->val.binary.data))
		dbz_malformed(field, "an object");
	return JsonbValueToJsonb((JsonbValue *) value);
}

static char *
dbz_string(const JsonbValue *value, const char *field, bool required)
{
	if (value == NULL || value->type == jbvNull)
	{
		if (required)
			dbz_malformed(field, "a nonempty string");
		return NULL;
	}
	if (value->type != jbvString || (required && value->val.string.len == 0))
		dbz_malformed(field, required ? "a nonempty string" : "a string or null");
	return pnstrdup(value->val.string.val, value->val.string.len);
}

/* numeric_int8 rounds; the round trip is required to reject fractional input. */
static int64
dbz_integer(const JsonbValue *value, const char *field, bool required,
			bool nonnegative)
{
	int64 result;
	Datum original;
	Datum integral;

	if (value == NULL || value->type == jbvNull)
	{
		if (required)
			dbz_malformed(field, "an integer");
		return 0;
	}
	if (value->type != jbvNumeric)
		dbz_malformed(field, "an integer");
	original = NumericGetDatum(value->val.numeric);
	result = DatumGetInt64(DirectFunctionCall1(numeric_int8, original));
	integral = DirectFunctionCall1(int8_numeric, Int64GetDatum(result));
	if (!DatumGetBool(DirectFunctionCall2(numeric_eq, original, integral)) ||
		(nonnegative && result < 0))
		dbz_malformed(field, nonnegative ? "a nonnegative integer" : "an integer");
	return result;
}

static SynchdbDbzSnapshot
dbz_snapshot(const JsonbValue *value)
{
	static const char *const names[] = {
		"false", "true", "first", "first_in_data_collection",
		"last_in_data_collection", "last", "incremental"
	};
	size_t index;

	/* Missing/null is the source schema's optional default, false. */
	if (value == NULL || value->type == jbvNull)
		return SYNCHDB_DBZ_SNAPSHOT_FALSE;
	if (value->type == jbvBool)
		return value->val.boolean ? SYNCHDB_DBZ_SNAPSHOT_TRUE : SYNCHDB_DBZ_SNAPSHOT_FALSE;
	for (index = 0; index < lengthof(names); index++)
		if (dbz_string_equals(value, names[index]))
			return (SynchdbDbzSnapshot) index;
	dbz_malformed("payload.source.snapshot", "a supported snapshot string or boolean");
	return SYNCHDB_DBZ_SNAPSHOT_FALSE;
}

static void
dbz_optional_integer(Jsonb *object, const char *key, bool nonnegative)
{
	JsonbValue value;

	(void) dbz_integer(dbz_member(object, key, &value), key, false, nonnegative);
}

static void
dbz_transaction(Jsonb *payload)
{
	JsonbValue value;
	JsonbValue *member;
	Jsonb *collections;
	JsonbIterator *iterator;
	JsonbIteratorToken token;

	member = dbz_member(payload, "status", &value);
	if (!dbz_string_equals(member, "BEGIN") && !dbz_string_equals(member, "END"))
		dbz_malformed("payload.status", "BEGIN or END");
	(void) dbz_string(dbz_member(payload, "id", &value), "payload.id", true);
	(void) dbz_integer(dbz_member(payload, "ts_ms", &value),
					   "payload.ts_ms", true, true);
	dbz_optional_integer(payload, "event_count", true);
	member = dbz_member(payload, "data_collections", &value);
	if (member == NULL || member->type == jbvNull)
		return;
	if (member->type != jbvBinary ||
		!JsonContainerIsArray(member->val.binary.data))
		dbz_malformed("payload.data_collections", "an array or null");
	collections = JsonbValueToJsonb(member);
	iterator = JsonbIteratorInit(&collections->root);
	while ((token = JsonbIteratorNext(&iterator, &value, true)) != WJB_DONE)
	{
		Jsonb *collection;
		JsonbValue field;

		if (token != WJB_ELEM)
			continue;
		collection = dbz_object(&value, "payload.data_collections[]", true);
		(void) dbz_string(dbz_member(collection, "data_collection", &field),
						 "payload.data_collections[].data_collection", true);
		(void) dbz_integer(dbz_member(collection, "event_count", &field),
						  "payload.data_collections[].event_count", true, true);
	}
}

static void
dbz_image(Jsonb *payload, const char *key, bool required)
{
	JsonbValue value;
	Jsonb *image = dbz_object(dbz_member(payload, key, &value), key, required);

	if (image != NULL && JsonContainerSize(&image->root) == 0)
		dbz_malformed(key, "a nonempty row object");
}

static void
dbz_truncate_payload(SynchdbDbzEvent *event)
{
	JsonbValue value;
	JsonbValue *member;
	static const char *const row_fields[] = {"before", "after"};
	int index;

	for (index = 0; index < lengthof(row_fields); index++)
	{
		member = dbz_member(event->payload, row_fields[index], &value);
		if (member != NULL && member->type != jbvNull)
			dbz_malformed(row_fields[index], "null or absent for a truncate");
	}
	if (event->snapshot != SYNCHDB_DBZ_SNAPSHOT_FALSE)
		dbz_malformed("payload.source.snapshot", "false for a truncate");
	if (event->database == NULL || event->database[0] == '\0' ||
		event->table == NULL || event->table[0] == '\0' ||
		(strcmp(event->connector, "mysql") != 0 &&
		 (event->schema == NULL || event->schema[0] == '\0')))
		dbz_malformed("truncate source identity", "a database, table and connector schema");
	event->kind = SYNCHDB_DBZ_EVENT_TRUNCATE;
}

static void
dbz_truncate_component(char **component, const char *candidate)
{
	if (candidate == NULL)
		return;
	if (*component != NULL && strcmp(*component, candidate) != 0)
		dbz_malformed("truncate source identity", "consistent across SQL and metadata");
	if (*component == NULL)
		*component = pstrdup(candidate);
}

/* Compatibility input stays source-only and produces the same typed event as
 * op=t. In particular, an empty tableChanges carrying TRUNCATE is not a no-op. */
static bool
dbz_schema_truncate(SynchdbDbzEvent *event)
{
	JsonbValue value;
	JsonbValue *member;
	Jsonb *change = NULL;
	SynchdbSourceName sql_name;
	SynchdbSourceName id_name;
	char *sql = dbz_string(dbz_member(event->payload, "ddl", &value), "payload.ddl", false);
	bool sql_truncate = synchdb_source_parse_truncate(sql, event->connector, &sql_name);
	bool explicit_truncate = false;
	char *identifier = NULL;
	int count;
	JsonbIterator *iterator;
	JsonbIteratorToken token;

	member = dbz_member(event->payload, "tableChanges", &value);
	if (member == NULL || member->type == jbvNull)
	{
		count = 0;
		iterator = NULL;
	}
	else
	{
		if (member->type != jbvBinary || !JsonContainerIsArray(member->val.binary.data))
			dbz_malformed("payload.tableChanges", "an array");
		count = JsonContainerSize(member->val.binary.data);
		iterator = JsonbIteratorInit(member->val.binary.data);
	}
	while (iterator && (token = JsonbIteratorNext(&iterator, &value, true)) != WJB_DONE)
	{
		JsonbValue field;
		char *action;

		if (token != WJB_ELEM)
			continue;
		change = dbz_object(&value, "tableChanges[]", true);
		action = dbz_string(dbz_member(change, "type", &field), "tableChange.type", true);
		if (pg_strcasecmp(action, "TRUNCATE") == 0 ||
			pg_strcasecmp(action, "TRUNCATE TABLE") == 0)
			explicit_truncate = true;
		else if (sql_truncate)
			dbz_malformed("tableChange.type", "TRUNCATE when the DDL is TRUNCATE");
	}
	if (!sql_truncate && !explicit_truncate)
		return false;
	if (count > 1)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("a TRUNCATE schema event must name exactly one table")));
	if (sql != NULL && !sql_truncate)
		dbz_malformed("payload.ddl", "TRUNCATE when tableChange.type is TRUNCATE");
	event->database = dbz_string(dbz_member(event->source, "db", &value), "source.db", false);
	event->schema = dbz_string(dbz_member(event->source, "schema", &value), "source.schema", false);
	event->table = dbz_string(dbz_member(event->source, "table", &value), "source.table", false);
	dbz_truncate_component(&event->database,
		dbz_string(dbz_member(event->payload, "databaseName", &value), "databaseName", false));
	dbz_truncate_component(&event->schema,
		dbz_string(dbz_member(event->payload, "schemaName", &value), "schemaName", false));
	if (change)
		identifier = dbz_string(dbz_member(change, "id", &value), "tableChange.id", true);
	if (identifier)
	{
		/* Debezium's serialized TableId already contains exact source case. */
		synchdb_source_parse_name(identifier, &id_name);
		dbz_truncate_component(&event->table, id_name.parts[id_name.count - 1]);
		if (id_name.count == 3)
		{
			dbz_truncate_component(&event->database, id_name.parts[0]);
			dbz_truncate_component(&event->schema, id_name.parts[1]);
		}
		else if (id_name.count == 2)
			dbz_truncate_component(strcmp(event->connector, "mysql") == 0 ?
				&event->database : &event->schema, id_name.parts[0]);
	}
	if (sql_truncate)
	{
		if (event->table == NULL)
			event->table = synchdb_source_name_component(&sql_name, sql_name.count - 1, event->connector);
		if (sql_name.count == 3)
		{
			if (event->database == NULL)
				event->database = synchdb_source_name_component(&sql_name, 0, event->connector);
			if (event->schema == NULL)
				event->schema = synchdb_source_name_component(&sql_name, 1, event->connector);
		}
		else if (sql_name.count == 2)
		{
			char **component = strcmp(event->connector, "mysql") == 0 ?
				&event->database : &event->schema;

			if (*component == NULL)
				*component = synchdb_source_name_component(&sql_name, 0, event->connector);
		}
		synchdb_source_check_truncate_name(&sql_name, event->connector,
			event->database, event->schema, event->table);
	}
	dbz_truncate_payload(event);
	return true;
}

static void
dbz_schema(SynchdbDbzEvent *event, const char *schema_name)
{
	JsonbValue value;
	JsonbValue *changes;
	int count;

	if (dbz_schema_truncate(event))
		return;
	changes = dbz_member(event->payload, "tableChanges", &value);
	if (changes == NULL || changes->type != jbvBinary ||
		!JsonContainerIsArray(changes->val.binary.data))
		dbz_malformed("payload.tableChanges", "an array");
	count = JsonContainerSize(changes->val.binary.data);
	if (count == 0)
	{
		char *expected = psprintf("io.debezium.connector.%s.SchemaChangeValue", event->connector);

		if (schema_name == NULL || strcmp(schema_name, expected) != 0 ||
			(strcmp(event->connector, "mysql") != 0 &&
			 strcmp(event->connector, "oracle") != 0 &&
			 strcmp(event->connector, "sqlserver") != 0))
		{
			event->unsupported_reason = "empty tableChanges lacks a matching schema-change envelope";
			return;
		}
		(void) dbz_integer(dbz_member(event->payload, "ts_ms", &value),
						   "payload.ts_ms", true, true);
		(void) dbz_string(dbz_member(event->payload, "databaseName", &value),
						  "payload.databaseName", false);
		(void) dbz_string(dbz_member(event->payload, "schemaName", &value),
						  "payload.schemaName", false);
		(void) dbz_string(dbz_member(event->payload, "ddl", &value), "payload.ddl", false);
		event->schema_empty = true;
		event->kind = SYNCHDB_DBZ_EVENT_SCHEMA;
		return;
	}
	{
		SynchdbDbzSchemaChange schema;

		/* The source-only decoder validates the complete list before binding. */
		synchdb_dbz_decode_schema(event, &schema);
	}
	event->kind = SYNCHDB_DBZ_EVENT_SCHEMA;
}

/* Normalize the project's logical DDL envelope, retaining the original source. */
static void
dbz_logical_schema(SynchdbDbzEvent *event, const char *content)
{
	int encoded_length = strlen(content);
	int capacity = pg_b64_dec_len(encoded_length);
	int length;
	char *bytes = palloc(capacity + 1);
	Jsonb *change;
	Jsonb *changes;
	JsonbParseState *state = NULL;
	JsonbValue element;
	JsonbValue *array;
	ArrayType *path;
	Datum keys[2] = {CStringGetTextDatum("payload"), CStringGetTextDatum("tableChanges")};
	JsonbValue buffer;

length = synchdb_pg_base64_decode(content, encoded_length,
									 (unsigned char *) bytes, capacity);
	if (length <= 0 || memchr(bytes, '\0', length) != NULL)
		dbz_malformed("payload.message.content", "base64 JSON without NUL bytes");
	bytes[length] = '\0';
	change = synchdb_source_json_from_utf8(bytes);
	if (!JB_ROOT_IS_OBJECT(change))
		dbz_malformed("payload.message.content", "a schema-change object");
	(void) pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
	element.type = jbvBinary;
	element.val.binary.data = &change->root;
	element.val.binary.len = VARSIZE_ANY_EXHDR(change);
	(void) pushJsonbValue(&state, WJB_ELEM, &element);
	array = pushJsonbValue(&state, WJB_END_ARRAY, NULL);
	changes = JsonbValueToJsonb(array);
	path = construct_array(keys, 2, TEXTOID, -1, false, TYPALIGN_INT);
	event->document = DatumGetJsonbP(DirectFunctionCall4(jsonb_set,
			JsonbPGetDatum(event->document), PointerGetDatum(path),
			JsonbPGetDatum(changes), BoolGetDatum(true)));
	event->payload = dbz_object(dbz_member(event->document, "payload", &buffer),
								 "payload", true);
	dbz_schema(event, NULL);
	if (event->kind == SYNCHDB_DBZ_EVENT_SCHEMA)
		event->kind = SYNCHDB_DBZ_EVENT_LOGICAL_MESSAGE;
}

void
synchdb_dbz_decode_event(const char *json, SynchdbDbzEvent *event)
{
	SynchdbDbzEvent decoded = {0};
	JsonbValue value;
	JsonbValue *member;
	Jsonb *schema;
	char *schema_name = NULL;
	char *operation;

	if (json == NULL || event == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Debezium event decoder requires JSON and an output")));
	decoded.kind = SYNCHDB_DBZ_EVENT_UNSUPPORTED;
	decoded.unsupported_reason = "unrecognized source event envelope";
	decoded.document = synchdb_source_json_from_utf8(json);
	if (!JB_ROOT_IS_OBJECT(decoded.document))
		dbz_malformed("event", "an object");
	decoded.payload = dbz_object(dbz_member(decoded.document, "payload", &value),
								 "payload", true);
	decoded.source = dbz_object(dbz_member(decoded.payload, "source", &value),
								"payload.source", false);
	schema = dbz_object(dbz_member(decoded.document, "schema", &value), "schema", false);
	if (schema != NULL)
		schema_name = dbz_string(dbz_member(schema, "name", &value), "schema.name", false);
	member = dbz_member(decoded.payload, "op", &value);
	if (member == NULL && schema_name != NULL &&
		(strcmp(schema_name, "io.debezium.connector.common.Heartbeat") == 0 ||
		 strcmp(schema_name, "io.debezium.connector.common.TransactionMetadataValue") == 0))
	{
		static const char *const row_fields[] = {
			"source", "before", "after", "tableChanges", "message"
		};
		size_t index;

		for (index = 0; index < lengthof(row_fields); index++)
			if (dbz_member(decoded.payload, row_fields[index], &value) != NULL)
				dbz_malformed("control payload", "free of row and schema-change fields");
		if (strcmp(schema_name, "io.debezium.connector.common.Heartbeat") == 0)
			decoded.kind = SYNCHDB_DBZ_EVENT_HEARTBEAT;
		else
		{
			dbz_transaction(decoded.payload);
			decoded.kind = SYNCHDB_DBZ_EVENT_TRANSACTION;
		}
		decoded.src_ts_ms = dbz_integer(dbz_member(decoded.payload, "ts_ms", &value),
										 "payload.ts_ms", true, true);
		decoded.unsupported_reason = NULL;
		*event = decoded;
		return;
	}
	if (decoded.source == NULL)
	{
		if (member != NULL || dbz_member(decoded.payload, "tableChanges", &value) != NULL)
			dbz_malformed("payload.source", "an object for a source change");
		*event = decoded;
		return;
	}
	decoded.connector = dbz_string(dbz_member(decoded.source, "connector", &value),
									 "payload.source.connector", true);
	if (strcmp(decoded.connector, "mysql") != 0 && strcmp(decoded.connector, "oracle") != 0 &&
		strcmp(decoded.connector, "sqlserver") != 0 && strcmp(decoded.connector, "postgresql") != 0)
	{
		decoded.unsupported_reason = "unsupported source connector";
		*event = decoded;
		return;
	}
	decoded.snapshot = dbz_snapshot(dbz_member(decoded.source, "snapshot", &value));
	decoded.src_ts_ms = dbz_integer(dbz_member(decoded.source, "ts_ms", &value),
										"payload.source.ts_ms", false, true);
	dbz_optional_integer(decoded.payload, "ts_ms", true);
	member = dbz_member(decoded.payload, "op", &value);
	if (member == NULL)
	{
		if (dbz_member(decoded.payload, "tableChanges", &value) != NULL ||
			dbz_member(decoded.payload, "ddl", &value) != NULL)
		{
			if (dbz_member(decoded.payload, "before", &value) != NULL ||
				dbz_member(decoded.payload, "after", &value) != NULL ||
				dbz_member(decoded.payload, "message", &value) != NULL ||
				dbz_member(decoded.payload, "status", &value) != NULL)
				dbz_malformed("schema-change payload", "free of row and control fields");
			dbz_schema(&decoded, schema_name);
		}
	}
	else
	{
		operation = dbz_string(member, "payload.op", true);
		if (strlen(operation) != 1)
			decoded.unsupported_reason = "unsupported source operation";
		else
		{
			decoded.operation = operation[0];
			switch (decoded.operation)
			{
				case 'c':
				case 'r':
				case 'u':
				case 'd':
					(void) dbz_string(dbz_member(decoded.source, "db", &value),
									  "payload.source.db", true);
					(void) dbz_string(dbz_member(decoded.source, "table", &value),
									  "payload.source.table", true);
					(void) dbz_string(dbz_member(decoded.source, "schema", &value),
									  "payload.source.schema", false);
					dbz_image(decoded.payload, "before", decoded.operation == 'd');
					dbz_image(decoded.payload, "after", decoded.operation != 'd');
					decoded.kind = SYNCHDB_DBZ_EVENT_ROW;
					break;
				case 'm':
				{
					Jsonb *message = dbz_object(dbz_member(decoded.payload, "message", &value),
												 "payload.message", true);
					char *prefix = dbz_string(dbz_member(message, "prefix", &value),
												 "payload.message.prefix", false);
					char *content = dbz_string(dbz_member(message, "content", &value),
												  "payload.message.content", false);

					if (strcmp(decoded.connector, "postgresql") == 0 &&
						prefix != NULL && strcmp(prefix, "synchdb_ddl") == 0)
					{
						if (content == NULL || content[0] == '\0')
							dbz_malformed("payload.message.content", "a nonempty base64 string");
						dbz_logical_schema(&decoded, content);
					}
					else
						decoded.unsupported_reason = "unsupported logical-message connector or prefix";
					break;
				}
				case 't':
					decoded.database = dbz_string(dbz_member(decoded.source, "db", &value),
												  "payload.source.db", true);
					decoded.schema = dbz_string(dbz_member(decoded.source, "schema", &value),
												"payload.source.schema", false);
					decoded.table = dbz_string(dbz_member(decoded.source, "table", &value),
											   "payload.source.table", true);
					dbz_truncate_payload(&decoded);
					break;
				default:
					decoded.unsupported_reason = "unsupported source operation";
					break;
			}
		}
	}
	if (decoded.kind != SYNCHDB_DBZ_EVENT_UNSUPPORTED)
		decoded.unsupported_reason = NULL;
	*event = decoded;
}
