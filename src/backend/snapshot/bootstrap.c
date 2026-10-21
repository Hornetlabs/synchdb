#include "postgres.h"
#include "fmgr.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type_d.h"
#include "commands/extension.h"
#include "converter/format_converter.h"
#include "converter/transform_expression.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "snapshot/bootstrap.h"
#include "source/debezium_schema.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

PG_FUNCTION_INFO_V1(synchdb_bootstrap_value);
PG_FUNCTION_INFO_V1(synchdb_bootstrap_validate_transform);
PG_FUNCTION_INFO_V1(synchdb_bootstrap_source_id);
PG_FUNCTION_INFO_V1(synchdb_bootstrap_column_id);
PG_FUNCTION_INFO_V1(synchdb_bootstrap_normalize_name);

Datum
synchdb_bootstrap_normalize_name(PG_FUNCTION_ARGS)
{
	char *name;
	char *strategy;
	int mode;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("name normalization requires a name and strategy")));
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	strategy = text_to_cstring(PG_GETARG_TEXT_PP(1));
	if (strcmp(strategy, "lower") == 0)
		mode = LCS_NORMALIZE_LOWERCASE;
	else if (strcmp(strategy, "upper") == 0)
		mode = LCS_NORMALIZE_UPPERCASE;
	else if (strcmp(strategy, "asis") == 0)
		PG_RETURN_TEXT_P(cstring_to_text(name));
	else
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid name normalization strategy")));
	fc_normalize_name(mode, name, strlen(name));
	PG_RETURN_TEXT_P(cstring_to_text(name));
}

Datum
synchdb_bootstrap_source_id(PG_FUNCTION_ARGS)
{
	char *database;
	char *schema;
	char *table;
	char *provider;
	char *key;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("source identity requires database, table and provider")));
	database = text_to_cstring(PG_GETARG_TEXT_PP(0));
	schema = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(1));
	table = text_to_cstring(PG_GETARG_TEXT_PP(2));
	provider = text_to_cstring(PG_GETARG_TEXT_PP(3));
	key = synchdb_source_table_key(database,
		pg_strcasecmp(provider, "mysql") == 0 ? NULL : schema, table);

	PG_RETURN_TEXT_P(cstring_to_text(key));
}

Datum
synchdb_bootstrap_column_id(PG_FUNCTION_ARGS)
{
	char *table;
	char *column;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("source column identity must not be NULL")));
	table = text_to_cstring(PG_GETARG_TEXT_PP(0));
	column = text_to_cstring(PG_GETARG_TEXT_PP(1));

	PG_RETURN_TEXT_P(cstring_to_text(synchdb_source_column_key(table, column)));
}

Datum
synchdb_bootstrap_validate_transform(PG_FUNCTION_ARGS)
{
	if (!PG_ARGISNULL(0))
	{
		char *expression = text_to_cstring(PG_GETARG_TEXT_PP(0));

		synchdb_transform_validate_fdw_template(expression);
		pfree(expression);
	}
	PG_RETURN_VOID();
}

/* The AS (...) descriptor supplies the real destination type and typmod.
 * Returning a typed column avoids explicit SQL casts silently truncating text.
 * NULL bypasses the expression, but still passes through domain_in. */
Datum
synchdb_bootstrap_value(PG_FUNCTION_ARGS)
{
	TupleDesc descriptor;
	Form_pg_attribute attribute;
	char *input = NULL;
	char *expression = NULL;
	Oid input_function;
	Oid input_parameter;
	Datum values[1];
	bool nulls[1];
	HeapTuple tuple;

	if (get_call_result_type(fcinfo, NULL, &descriptor) != TYPEFUNC_COMPOSITE ||
		descriptor->natts != 1)
		ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
						errmsg("bootstrap value requires exactly one typed output column")));
	attribute = TupleDescAttr(descriptor, 0);
	if (attribute->attisdropped)
		elog(ERROR, "bootstrap value output column is dropped");
	if (!PG_ARGISNULL(1))
	{
		expression = text_to_cstring(PG_GETARG_TEXT_PP(1));
		synchdb_transform_validate_fdw_template(expression);
	}
	if (!PG_ARGISNULL(0))
	{
		input = text_to_cstring(PG_GETARG_TEXT_PP(0));
		if (expression)
		{
			char *transformed = ra_transformDataExpression(input, NULL, NULL,
														 expression, attribute->atttypid);
			pfree(input);
			input = transformed;
		}
	}
	nulls[0] = input == NULL;
	getTypeInputInfo(attribute->atttypid, &input_function, &input_parameter);
	values[0] = OidInputFunctionCall(input_function, input, input_parameter,
								   attribute->atttypmod);
	tuple = heap_form_tuple(BlessTupleDesc(descriptor), values, nulls);
	if (input)
		pfree(input);
	if (expression)
		pfree(expression);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

void
synchdb_bootstrap_require_mode(const char *mode, bool schema_only)
{
	if (schema_only || (mode && pg_strcasecmp(mode, "initial_only") == 0))
		return;
	/* A source read position alone is not an engine resume boundary. */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("FDW bootstrap cannot establish a verified CDC handoff for mode %s",
					mode ? mode : "NULL"),
			 errhint("Use initial_only or schemasync; CDC handoff requires a provider-specific protocol.")));
}

static JsonbValue *
bootstrap_member(JsonbContainer *object, const char *name, enum jbvType type,
				 JsonbValue *value)
{
	JsonbValue *found = getKeyJsonValueFromContainer(object, name, strlen(name), value);

	if (found == NULL || found->type != type)
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("invalid bootstrap result member: %s", name)));
	return found;
}

static bool
bootstrap_string_is(JsonbValue *value, const char *text)
{
	return value->type == jbvString &&
		(Size) value->val.string.len == strlen(text) &&
		memcmp(value->val.string.val, text, value->val.string.len) == 0;
}

SynchdbBootstrapResult *
synchdb_bootstrap_parse_result(Jsonb *json, MemoryContext context)
{
	SynchdbBootstrapResult *result;
	JsonbValue value;
	JsonbValue tables;
	JsonbIterator *iterator;
	JsonbIteratorToken token;
	MemoryContext caller;

	if (json == NULL || !JB_ROOT_IS_OBJECT(json))
		elog(ERROR, "bootstrap did not return an object");
	bootstrap_member(&json->root, "version", jbvNumeric, &value);
	if (!DatumGetBool(DirectFunctionCall2(numeric_eq,
			NumericGetDatum(value.val.numeric), DirectFunctionCall1(int4_numeric, Int32GetDatum(1)))))
		elog(ERROR, "unsupported bootstrap result version");
	caller = MemoryContextSwitchTo(context);
	result = palloc0(sizeof(*result));
	result->detail = DatumGetJsonbPCopy(JsonbPGetDatum(json));
	MemoryContextSwitchTo(caller);
	bootstrap_member(&json->root, "status", jbvString, &value);
	if (bootstrap_string_is(&value, "COMPLETE"))
		result->status = SYNCHDB_BOOTSTRAP_COMPLETE;
	else if (bootstrap_string_is(&value, "PARTIAL"))
		result->status = SYNCHDB_BOOTSTRAP_PARTIAL;
	else if (bootstrap_string_is(&value, "FAILED"))
		result->status = SYNCHDB_BOOTSTRAP_FAILED;
	else
		elog(ERROR, "unknown bootstrap result status");
	bootstrap_member(&json->root, "after_bootstrap", jbvString, &value);
	result->stop_after = bootstrap_string_is(&value, "STOP");
	if (!result->stop_after && !bootstrap_string_is(&value, "PAUSE"))
		elog(ERROR, "unsupported bootstrap continuation");
	bootstrap_member(&json->root, "cdc_eligible", jbvBool, &value);
	if (value.val.boolean)
		elog(ERROR, "FDW bootstrap unexpectedly advertised CDC eligibility");
	bootstrap_member(&json->root, "cdc_resume_material", jbvNull, &value);
	bootstrap_member(&json->root, "tables", jbvBinary, &tables);
	if (!JsonContainerIsArray(tables.val.binary.data))
		elog(ERROR, "bootstrap tables must be an array");
	iterator = JsonbIteratorInit(tables.val.binary.data);
	while ((token = JsonbIteratorNext(&iterator, &value, true)) != WJB_DONE)
	{
		JsonbValue status;

		if (token != WJB_ELEM)
			continue;
		if (value.type != jbvBinary || !JsonContainerIsObject(value.val.binary.data))
			elog(ERROR, "invalid bootstrap table result");
		bootstrap_member(value.val.binary.data, "status", jbvString, &status);
		if (bootstrap_string_is(&status, "COMPLETE"))
			result->completed_tables++;
		else if (bootstrap_string_is(&status, "FAILED"))
			result->failed_tables++;
		else
			elog(ERROR, "invalid bootstrap table status");
	}
	if ((result->status == SYNCHDB_BOOTSTRAP_COMPLETE && result->failed_tables != 0) ||
		(result->status == SYNCHDB_BOOTSTRAP_PARTIAL &&
		 (result->completed_tables == 0 || result->failed_tables == 0)) ||
		(result->status == SYNCHDB_BOOTSTRAP_FAILED &&
		 (result->completed_tables != 0 || result->failed_tables == 0)))
		elog(ERROR, "bootstrap status disagrees with table outcomes");
	return result;
}

SynchdbBootstrapResult *
synchdb_bootstrap_execute(const SynchdbRunSpec *spec, MemoryContext context)
{
	const ConnectionInfo *connection = &spec->connection;
	bool schema_only = (connection->flag & CONNFLAG_SCHEMA_SYNC_MODE) != 0;
	StringInfoData request;
	char destination[SYNCHDB_CONNINFO_DB_NAME_SIZE];
	const char *source_db = connection->srcdb;
	const char *slash;
	Oid extension_oid;
	char *function;
	char *query;
	Oid types[1] = {JSONBOID};
	Datum values[1];
	Datum json;
	bool is_null;
	int status;
	SynchdbBootstrapResult *result;

	synchdb_bootstrap_require_mode(spec->snapshot_mode, schema_only);
	if (!IsTransactionState() || !ActiveSnapshotSet())
		elog(ERROR, "bootstrap requires a caller-owned transaction and snapshot");
	if ((spec->legacy_type == TYPE_ORACLE || spec->legacy_type == TYPE_OLR) &&
		(slash = strchr(source_db, '/')) != NULL)
		source_db = slash + 1;
	strlcpy(destination, source_db, sizeof(destination));
	fc_normalize_name(connection->run_options.synchdb_letter_casing_strategy,
					  destination, strlen(destination));
	initStringInfo(&request);
	appendStringInfoString(&request, "{\"version\":1,\"connector_name\":");
	escape_json(&request, connection->name);
	appendStringInfoString(&request, ",\"mode\":");
	escape_json(&request, schema_only ? "schemasync" : "initial_only");
	appendStringInfoString(&request, ",\"dest_schema\":");
	escape_json(&request, destination);
	appendStringInfoString(&request, ",\"connector_id\":");
	escape_json(&request, DatumGetCString(DirectFunctionCall1(uuid_out,
								 UUIDPGetDatum(&connection->key.connector_id))));
	appendStringInfo(&request, ",\"config_revision\":%lld",
					 (long long) connection->config_revision);
	appendStringInfoString(&request, ",\"failure_policy\":");
	escape_json(&request, connection->run_options.synchdb_fdw_use_subtx ?
				"continue_tables" : "fail_fast");
	appendStringInfoString(&request, ",\"target_policy\":\"error\",\"case_strategy\":");
	escape_json(&request, connection->run_options.synchdb_letter_casing_strategy == LCS_NORMALIZE_LOWERCASE ?
				"lower" : connection->run_options.synchdb_letter_casing_strategy == LCS_NORMALIZE_UPPERCASE ?
				"upper" : "asis");
	appendStringInfoChar(&request, '}');
	values[0] = DirectFunctionCall1(jsonb_in, CStringGetDatum(request.data));
	extension_oid = get_extension_oid("synchdb", false);
	function = quote_qualified_identifier(get_namespace_name(get_extension_schema(extension_oid)),
										  "synchdb_bootstrap");
	query = psprintf("SELECT %s($1::jsonb)", function);
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect SPI for bootstrap");
	status = SPI_execute_with_args(query, 1, types, values, NULL, false, 1);
	if (status != SPI_OK_SELECT || SPI_processed != 1 || SPI_tuptable == NULL)
		elog(ERROR, "bootstrap did not return exactly one result");
	json = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &is_null);
	if (is_null)
		elog(ERROR, "bootstrap returned NULL");
	result = synchdb_bootstrap_parse_result(DatumGetJsonbP(json), context);
	if (result->stop_after != !schema_only)
		elog(ERROR, "bootstrap continuation disagrees with the request");
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "could not finish SPI bootstrap");
	return result;
}
