/*
 * connector_store.c
 *
 * Connector catalog access.  Transaction ownership belongs to the caller.
 */
#include "postgres.h"

#include "catalog/connector_store.h"
#include "catalog/catalog_access.h"
#include "catalog/credentials.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

static char *
connector_store_text(HeapTuple tuple, TupleDesc tupdesc, int column,
					 const char *field, bool required)
{
	bool isnull;
	Datum value;
	char *field_text;

	if (column < 1 || column > tupdesc->natts ||
		SPI_gettypeid(tupdesc, column) != TEXTOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("connector catalog field \"%s\" must have type text", field)));
	value = SPI_getbinval(tuple, tupdesc, column, &isnull);
	if (isnull)
	{
		if (required)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("required connector field \"%s\" is NULL", field)));
		return NULL;
	}
	field_text = TextDatumGetCString(value);
	if (required && field_text[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("required connector field \"%s\" is empty", field)));
	return field_text;
}

static void
connector_store_copy(char *destination, Size capacity, const char *value,
					 const char *field)
{
	if (value == NULL)
		value = "null"; /* Legacy optional-text convention; migrate separately. */
	if (strlen(value) >= capacity)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("connector field \"%s\" exceeds its supported length", field),
				 errdetail("The maximum length is %zu bytes.", capacity - 1)));
	strlcpy(destination, value, capacity);
}

static void
connector_store_secret(char *destination, Size capacity,
					   HeapTuple tuple, TupleDesc tupdesc, int column,
					   const char *field, bool required,
					   SynchdbCredentials *credentials)
{
	bool isnull;
	Datum value;
	Jsonb *envelope;
	char *plaintext;

	if (SPI_gettypeid(tupdesc, column) != JSONBOID)
		elog(ERROR, "connector credential catalog field must have type jsonb");
	value = SPI_getbinval(tuple, tupdesc, column, &isnull);
	envelope = isnull ? NULL : DatumGetJsonbP(value);
	if (!isnull && JB_ROOT_IS_SCALAR(envelope))
	{
		JsonbValue scalar;

		JsonbExtractScalar(&envelope->root, &scalar);
		isnull = scalar.type == jbvNull;
	}
	if (isnull)
	{
		if (required)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("required connector credential is NULL")));
		connector_store_copy(destination, capacity, NULL, field);
		return;
	}
	plaintext = synchdb_credentials_decrypt(credentials, envelope);
	if (required && plaintext[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("required connector credential is empty")));
	connector_store_copy(destination, capacity, plaintext, field);
	explicit_bzero(plaintext, strlen(plaintext));
	pfree(plaintext);
}

static unsigned int
connector_store_unsigned(HeapTuple tuple, TupleDesc tupdesc, int column,
						 const char *field, bool required, uint32 maximum)
{
	char *text = connector_store_text(tuple, tupdesc, column, field, required);
	int64 value;

	/* Missing optional numbers historically mean disabled/unspecified (zero). */
	if (text == NULL || (!required && strcmp(text, "null") == 0))
		return 0;
	value = pg_strtoint64(text);
	if (value < 0 || (uint64) value > maximum || (required && value == 0))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("connector field \"%s\" is out of range", field)));
	return (unsigned int) value;
}

static bool
connector_store_bool(HeapTuple tuple, TupleDesc tupdesc, int column)
{
	bool isnull;
	Datum value;

	if (column < 1 || column > tupdesc->natts ||
		SPI_gettypeid(tupdesc, column) != BOOLOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("connector catalog boolean field has an invalid type")));
	value = SPI_getbinval(tuple, tupdesc, column, &isnull);
	/* Optional/missing boolean settings retain the legacy false default. */
	return !isnull && DatumGetBool(value);
}

static void
connector_store_identity(HeapTuple tuple, TupleDesc tupdesc,
						 int id_column, int revision_column,
						 SynchdbConnectorKey *key, int64 *revision)
{
	bool isnull;
	Datum value;

	if (id_column < 1 || id_column > tupdesc->natts ||
		revision_column < 1 || revision_column > tupdesc->natts ||
		SPI_gettypeid(tupdesc, id_column) != UUIDOID ||
		SPI_gettypeid(tupdesc, revision_column) != INT8OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("connector identity must have UUID and bigint catalog types")));
	value = SPI_getbinval(tuple, tupdesc, id_column, &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("connector identity must not be NULL")));
	key->database_oid = MyDatabaseId;
	memcpy(&key->connector_id, DatumGetUUIDP(value), sizeof(key->connector_id));
	value = SPI_getbinval(tuple, tupdesc, revision_column, &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("connector configuration revision must not be NULL")));
	*revision = DatumGetInt64(value);
	if (!synchdb_connector_key_is_valid(key) || *revision <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connector identity and revision must be valid and positive")));
}

bool
synchdb_connector_store_get(const char *name, ConnectionInfo *out,
							char **connector, MemoryContext result_context)
{
	ConnectionInfo loaded = {0};
	Oid argtypes[1] = {TEXTOID};
	Datum values[1];
	StringInfoData query;
	char *relation;
	SynchdbCredentials *credentials;
	char *type;
	char *type_result;
	HeapTuple tuple;
	TupleDesc tupdesc;
	int ret;

	if (!name || !out || !connector || !result_context)
		elog(ERROR, "invalid connector store get arguments");
	*connector = NULL;
	memset(out, 0, sizeof(*out));
	if (name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connector name must not be empty")));
	synchdb_store_connect();
	relation = synchdb_store_relation(SYNCHDB_CONNINFO_TABLE, AccessShareLock);
	initStringInfo(&query);
	appendStringInfo(&query,
			"SELECT pg_catalog.jsonb_extract_path_text(data, 'hostname'), pg_catalog.jsonb_extract_path_text(data, 'port'), pg_catalog.jsonb_extract_path_text(data, 'user'), "
			"pg_catalog.jsonb_extract_path(data, 'pwd'), "
			"pg_catalog.jsonb_extract_path_text(data, 'srcdb'), pg_catalog.jsonb_extract_path_text(data, 'dstdb'), pg_catalog.jsonb_extract_path_text(data, 'table'), "
			"pg_catalog.jsonb_extract_path_text(data, 'snapshottable'), pg_catalog.jsonb_extract_path_text(data, 'connector'), isactive, "
			"pg_catalog.jsonb_extract_path_text(data, 'ssl_mode'), pg_catalog.jsonb_extract_path_text(data, 'ssl_keystore'), "
			"pg_catalog.jsonb_extract_path(data, 'ssl_keystore_pass'), "
			"pg_catalog.jsonb_extract_path_text(data, 'ssl_truststore'), "
			"pg_catalog.jsonb_extract_path(data, 'ssl_truststore_pass'), "
			"pg_catalog.jsonb_extract_path_text(data, 'jmx_listenaddr'), pg_catalog.jsonb_extract_path_text(data, 'jmx_port'), "
			"pg_catalog.jsonb_extract_path_text(data, 'jmx_rmi_address'), pg_catalog.jsonb_extract_path_text(data, 'jmx_rmi_port'), "
			"(pg_catalog.jsonb_extract_path_text(data, 'jmx_auth'))::pg_catalog.bool, "
			"pg_catalog.jsonb_extract_path_text(data, 'jmx_auth_passwdfile'), pg_catalog.jsonb_extract_path_text(data, 'jmx_auth_accessfile'), "
			"(pg_catalog.jsonb_extract_path_text(data, 'jmx_ssl'))::pg_catalog.bool, pg_catalog.jsonb_extract_path_text(data, 'jmx_ssl_keystore'), "
			"pg_catalog.jsonb_extract_path(data, 'jmx_ssl_keystore_pass'), "
			"pg_catalog.jsonb_extract_path_text(data, 'jmx_ssl_truststore'), "
			"pg_catalog.jsonb_extract_path(data, 'jmx_ssl_truststore_pass'), "
			"pg_catalog.jsonb_extract_path_text(data, 'jmx_exporter'), pg_catalog.jsonb_extract_path_text(data, 'jmx_exporter_port'), "
			"pg_catalog.jsonb_extract_path_text(data, 'jmx_exporter_conf'), pg_catalog.jsonb_extract_path_text(data, 'olr_host'), "
			"pg_catalog.jsonb_extract_path_text(data, 'olr_port'), pg_catalog.jsonb_extract_path_text(data, 'olr_source'), "
			"pg_catalog.jsonb_extract_path_text(data, 'ispn_cache_type'), pg_catalog.jsonb_extract_path_text(data, 'ispn_memory_type'), "
			"pg_catalog.jsonb_extract_path_text(data, 'ispn_memory_size'), pg_catalog.jsonb_extract_path_text(data, 'srcschema'), "
			"pg_catalog.jsonb_extract_path_text(data, 'fdw_ssl_cert'), pg_catalog.jsonb_extract_path_text(data, 'fdw_ssl_key'), "
			"pg_catalog.jsonb_extract_path_text(data, 'fdw_ssl_rootcert'), "
			"pg_catalog.jsonb_extract_path(data, 'fdw_ssl_cipher'), "
			"connector_id, config_revision "
			"FROM %s WHERE name COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) $1 COLLATE pg_catalog.\"C\"",
			relation);
	values[0] = CStringGetTextDatum(name);
	ret = SPI_execute_with_args(query.data, 1, argtypes, values, NULL, true, 2);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "could not read connector catalog");
	if (SPI_processed == 0)
	{
		synchdb_store_finish();
		return false;
	}
	if (SPI_processed != 1 || SPI_tuptable->tupdesc->natts != 43)
		elog(ERROR, "invalid connector catalog result");
	tuple = SPI_tuptable->vals[0];
	tupdesc = SPI_tuptable->tupdesc;
	credentials = synchdb_credentials_open();

#define COPY_FIELD(member, column, field, required) \
	connector_store_copy(loaded.member, sizeof(loaded.member), \
		connector_store_text(tuple, tupdesc, column, field, required), field)
#define COPY_SECRET(member, column, field, required) \
	connector_store_secret(loaded.member, sizeof(loaded.member), tuple, tupdesc, \
		column, field, required, credentials)
	connector_store_copy(loaded.name, sizeof(loaded.name), name, "name");
	COPY_FIELD(hostname, 1, "hostname", true);
	loaded.port = connector_store_unsigned(tuple, tupdesc, 2, "port", true, 65535);
	COPY_FIELD(user, 3, "user", true);
	COPY_SECRET(pwd, 4, "pwd", true);
	COPY_FIELD(srcdb, 5, "srcdb", true);
	COPY_FIELD(dstdb, 6, "dstdb", true);
	COPY_FIELD(table, 7, "table", false);
	COPY_FIELD(snapshottable, 8, "snapshottable", false);
	type = connector_store_text(tuple, tupdesc, 9, "connector", true);
	loaded.active = connector_store_bool(tuple, tupdesc, 10);
	COPY_FIELD(extra.ssl_mode, 11, "ssl_mode", false);
	COPY_FIELD(extra.ssl_keystore, 12, "ssl_keystore", false);
	COPY_SECRET(extra.ssl_keystore_pass, 13, "ssl_keystore_pass", false);
	COPY_FIELD(extra.ssl_truststore, 14, "ssl_truststore", false);
	COPY_SECRET(extra.ssl_truststore_pass, 15, "ssl_truststore_pass", false);
	COPY_FIELD(jmx.jmx_listenaddr, 16, "jmx_listenaddr", false);
	loaded.jmx.jmx_port = connector_store_unsigned(tuple, tupdesc, 17,
													"jmx_port", false, 65535);
	COPY_FIELD(jmx.jmx_rmiserveraddr, 18, "jmx_rmi_address", false);
	loaded.jmx.jmx_rmiport = connector_store_unsigned(tuple, tupdesc, 19,
													   "jmx_rmi_port", false, 65535);
	loaded.jmx.jmx_auth = connector_store_bool(tuple, tupdesc, 20);
	COPY_FIELD(jmx.jmx_auth_passwdfile, 21, "jmx_auth_passwdfile", false);
	COPY_FIELD(jmx.jmx_auth_accessfile, 22, "jmx_auth_accessfile", false);
	loaded.jmx.jmx_ssl = connector_store_bool(tuple, tupdesc, 23);
	COPY_FIELD(jmx.jmx_ssl_keystore, 24, "jmx_ssl_keystore", false);
	COPY_SECRET(jmx.jmx_ssl_keystore_pass, 25, "jmx_ssl_keystore_pass", false);
	COPY_FIELD(jmx.jmx_ssl_truststore, 26, "jmx_ssl_truststore", false);
	COPY_SECRET(jmx.jmx_ssl_truststore_pass, 27, "jmx_ssl_truststore_pass", false);
	COPY_FIELD(jmx.jmx_exporter, 28, "jmx_exporter", false);
	loaded.jmx.jmx_exporter_port = connector_store_unsigned(tuple, tupdesc, 29,
															 "jmx_exporter_port", false, 65535);
	COPY_FIELD(jmx.jmx_exporter_conf, 30, "jmx_exporter_conf", false);
	COPY_FIELD(olr.olr_host, 31, "olr_host", false);
	loaded.olr.olr_port = connector_store_unsigned(tuple, tupdesc, 32,
													"olr_port", false, 65535);
	COPY_FIELD(olr.olr_source, 33, "olr_source", false);
	COPY_FIELD(ispn.ispn_cache_type, 34, "ispn_cache_type", false);
	COPY_FIELD(ispn.ispn_memory_type, 35, "ispn_memory_type", false);
	loaded.ispn.ispn_memory_size = connector_store_unsigned(tuple, tupdesc, 36,
															 "ispn_memory_size", false, PG_UINT32_MAX);
	COPY_FIELD(srcschema, 37, "srcschema", false);
	COPY_FIELD(fdw.ssl_cert, 38, "fdw_ssl_cert", false);
	COPY_FIELD(fdw.ssl_key, 39, "fdw_ssl_key", false);
	COPY_FIELD(fdw.ssl_rootcert, 40, "fdw_ssl_rootcert", false);
	COPY_SECRET(fdw.ssl_cipher, 41, "fdw_ssl_cipher", false);
#undef COPY_FIELD
#undef COPY_SECRET
	connector_store_identity(tuple, tupdesc, 42, 43, &loaded.key, &loaded.config_revision);

	/* Publish only after all fields have been validated and copied. */
	type_result = MemoryContextStrdup(result_context, type);
	synchdb_credentials_close(credentials);
	synchdb_store_finish();
	memcpy(out, &loaded, sizeof(loaded));
	*connector = type_result;
	return true;
}

bool
synchdb_connector_store_get_identity(const char *name,
									SynchdbConnectorKey *key, int64 *revision)
{
	SynchdbConnectorKey loaded = {0};
	int64 loaded_revision = 0;
	Oid argtypes[1] = {TEXTOID};
	Datum values[1];
	StringInfoData query;
	char *relation;

	if (!name || !key || !revision)
		elog(ERROR, "invalid connector identity read arguments");
	MemSet(key, 0, sizeof(*key));
	*revision = 0;
	if (name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connector name must not be empty")));
	synchdb_store_connect();
	relation = synchdb_store_relation(SYNCHDB_CONNINFO_TABLE, AccessShareLock);
	initStringInfo(&query);
	appendStringInfo(&query,
		"SELECT connector_id, config_revision FROM %s "
		"WHERE name COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) $1 COLLATE pg_catalog.\"C\"",
		relation);
	values[0] = CStringGetTextDatum(name);
	if (SPI_execute_with_args(query.data, 1, argtypes, values, NULL, true, 2) != SPI_OK_SELECT)
		elog(ERROR, "could not read connector identity");
	if (SPI_processed == 0)
	{
		synchdb_store_finish();
		return false;
	}
	if (SPI_processed != 1 || SPI_tuptable->tupdesc->natts != 2)
		elog(ERROR, "invalid connector identity result");
	connector_store_identity(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
							 1, 2, &loaded, &loaded_revision);
	synchdb_store_finish();
	memcpy(key, &loaded, sizeof(loaded));
	*revision = loaded_revision;
	return true;
}

bool
synchdb_connector_store_set_active_identity(const SynchdbConnectorKey *key,
										  int64 expected_revision, bool active)
{
	Oid argtypes[3] = {UUIDOID, BOOLOID, INT8OID};
	Datum values[3];
	StringInfoData query;
	char *relation;
	bool found;

	if (!synchdb_connector_key_is_valid(key) || key->database_oid != MyDatabaseId ||
		expected_revision < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connector identity must belong to this database and revision must not be negative")));
	synchdb_store_connect();
	relation = synchdb_store_relation(SYNCHDB_CONNINFO_TABLE, RowExclusiveLock);
	initStringInfo(&query);
	appendStringInfo(&query,
		"UPDATE %s SET isactive = $2 WHERE connector_id OPERATOR(pg_catalog.=) $1",
		relation);
	if (expected_revision > 0)
		appendStringInfoString(&query, " AND config_revision OPERATOR(pg_catalog.=) $3");
	values[0] = UUIDPGetDatum(&key->connector_id);
	values[1] = BoolGetDatum(active);
	values[2] = Int64GetDatum(expected_revision);
	if (SPI_execute_with_args(query.data, expected_revision > 0 ? 3 : 2,
							  argtypes, values, NULL, false, 0) != SPI_OK_UPDATE)
		elog(ERROR, "could not update connector active state by identity");
	if (SPI_processed > 1)
		elog(ERROR, "connector identity update affected multiple configurations");
	found = SPI_processed == 1;
	synchdb_store_finish();
	return found;
}

void
synchdb_connector_store_list_active(char ***out, int *numout,
									MemoryContext result_context)
{
	StringInfoData query;
	char **names = NULL;
	char *relation;
	int count;
	int i;

	if (!out || !numout || !result_context)
		elog(ERROR, "invalid connector store list arguments");
	*out = NULL;
	*numout = 0;
	synchdb_store_connect();
	relation = synchdb_store_relation(SYNCHDB_CONNINFO_TABLE, AccessShareLock);
	initStringInfo(&query);
	appendStringInfo(&query, "SELECT name FROM %s WHERE isactive", relation);
	if (SPI_execute(query.data, true, 0) != SPI_OK_SELECT)
		elog(ERROR, "could not list active connector configurations");
	if (SPI_processed > INT_MAX || SPI_processed > MaxAllocSize / sizeof(char *))
		elog(ERROR, "too many active connector names");
	count = (int) SPI_processed;
	if (count > 0)
	{
		char **checked_names = palloc(count * sizeof(char *));

		/* Invalid later rows must not strand partial results in a long-lived
		 * caller context.  Validate the complete SPI-owned list first. */
		for (i = 0; i < count; i++)
		{
			char *name = connector_store_text(SPI_tuptable->vals[i],
												 SPI_tuptable->tupdesc, 1, "name", true);

			if (strlen(name) >= SYNCHDB_CONNINFO_NAME_SIZE)
				ereport(ERROR,
						(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
						 errmsg("connector name exceeds its supported length")));
			checked_names[i] = name;
		}
		names = MemoryContextAllocZero(result_context, count * sizeof(char *));
		for (i = 0; i < count; i++)
			names[i] = MemoryContextStrdup(result_context, checked_names[i]);
	}
	synchdb_store_finish();
	*out = names;
	*numout = count;
}

bool
synchdb_connector_store_set_active(const char *name, bool active)
{
	Oid argtypes[2] = {TEXTOID, BOOLOID};
	Datum values[2];
	StringInfoData query;
	char *relation;
	bool found;

	if (!name)
		elog(ERROR, "invalid connector store update arguments");
	synchdb_store_connect();
	relation = synchdb_store_relation(SYNCHDB_CONNINFO_TABLE, RowExclusiveLock);
	initStringInfo(&query);
	appendStringInfo(&query, "UPDATE %s SET isactive = $2 "
					 "WHERE name COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) $1 COLLATE pg_catalog.\"C\"", relation);
	values[0] = CStringGetTextDatum(name);
	values[1] = BoolGetDatum(active);
	if (SPI_execute_with_args(query.data, 2, argtypes, values, NULL, false, 0) != SPI_OK_UPDATE)
		elog(ERROR, "could not update connector active state");
	if (SPI_processed > 1)
		elog(ERROR, "connector active-state update affected multiple configurations");
	found = SPI_processed == 1;
	synchdb_store_finish();
	return found;
}

/* Private, fixed-field bindings shared by the typed write entry points. */
typedef struct ConnectorStoreField
{
	const char *key;
	Oid type;
	const char *string;
	uint64 number;
	bool isnull;
	bool encrypted;
} ConnectorStoreField;

static ConnectorStoreField
connector_store_string_field(const char *key, const char *string, Size capacity,
							 bool optional, bool encrypted)
{
	ConnectorStoreField field = {0};

	if (!string || strnlen(string, capacity) == capacity)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("connector field \"%s\" is not a terminated supported string", key)));
	if (!optional && string[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("required connector field \"%s\" is empty", key)));
	field.key = key;
	field.type = TEXTOID;
	field.string = string;
	field.isnull = optional && strcmp(string, "null") == 0;
	field.encrypted = encrypted;
	return field;
}

static ConnectorStoreField
connector_store_number_field(const char *key, uint64 number, uint64 maximum)
{
	ConnectorStoreField field = {0};

	if (number == 0 || number > maximum)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("connector field \"%s\" is out of range", key)));
	field.key = key;
	field.type = INT8OID;
	field.number = number;
	return field;
}

static ConnectorStoreField
connector_store_boolean_field(const char *key, bool value)
{
	ConnectorStoreField field = {0};

	field.key = key;
	field.type = BOOLOID;
	field.number = value ? 1 : 0;
	return field;
}

/*
 * Each public writer supplies its fixed field list.  Keys, values, connector
 * name are all SPI parameters. Secrets become versioned encrypted envelopes
 * before binding, and key material never enters SQL. No SQL/data fragment is
 * accepted from a caller, and this helper never owns the caller's transaction.
 */
static bool
connector_store_write_fields(const char *name, const ConnectorStoreField *fields,
							 int field_count, bool create)
{
	StringInfoData query;
	char *relation;
	SynchdbCredentials *credentials = NULL;
	bool encrypted = false;
	bool found;
	int nargs;
	Oid *argtypes;
	Datum *values;
	char *nulls;
	int i;
	int ret;

	if (!name)
		elog(ERROR, "invalid connector store write arguments");
	for (i = 0; i < field_count; i++)
		encrypted |= fields[i].encrypted;
	nargs = 1 + 2 * field_count;
	synchdb_store_connect();
	relation = synchdb_store_relation(SYNCHDB_CONNINFO_TABLE, RowExclusiveLock);
	if (encrypted)
		/* Lock before pinning a keyring, as the re-encryption operation does. */
		credentials = synchdb_credentials_open();
	argtypes = palloc(nargs * sizeof(Oid));
	values = palloc0(nargs * sizeof(Datum));
	nulls = palloc(nargs);
	memset(nulls, ' ', nargs);
	argtypes[0] = TEXTOID;
	values[0] = CStringGetTextDatum(name);
	initStringInfo(&query);
	if (create)
		appendStringInfo(&query, "INSERT INTO %s (name, isactive, data) "
						 "VALUES ($1, false, pg_catalog.jsonb_build_object(", relation);
	else
		appendStringInfo(&query, "UPDATE %s SET data = data OPERATOR(pg_catalog.||) "
						 "pg_catalog.jsonb_build_object(", relation);
	for (i = 0; i < field_count; i++)
	{
		const ConnectorStoreField *field = &fields[i];
		int key_index = 1 + 2 * i;
		int value_index = key_index + 1;

		argtypes[key_index] = TEXTOID;
		values[key_index] = CStringGetTextDatum(field->key);
		argtypes[value_index] = field->encrypted ? JSONBOID : field->type;
		if (field->isnull)
			nulls[value_index] = 'n';
		else if (field->encrypted)
			values[value_index] = JsonbPGetDatum(synchdb_credentials_encrypt(credentials, field->string));
		else if (field->type == TEXTOID)
			values[value_index] = CStringGetTextDatum(field->string);
		else if (field->type == INT8OID)
			values[value_index] = Int64GetDatum((int64) field->number);
		else if (field->type == BOOLOID)
			values[value_index] = BoolGetDatum(field->number != 0);
		else
			elog(ERROR, "unsupported connector catalog binding type");

		if (i > 0)
			appendStringInfoString(&query, ", ");
		appendStringInfo(&query, "$%d, ", key_index + 1);
		appendStringInfo(&query, "$%d", value_index + 1);
	}
	if (create)
		appendStringInfoString(&query, "))");
	else
		appendStringInfoString(&query, ") WHERE name COLLATE pg_catalog.\"C\" "
										"OPERATOR(pg_catalog.=) $1 COLLATE pg_catalog.\"C\"");
	ret = SPI_execute_with_args(query.data, nargs, argtypes, values, nulls, false, 0);
	if (ret != (create ? SPI_OK_INSERT : SPI_OK_UPDATE))
		elog(ERROR, "could not write connector configuration");
	if (SPI_processed > 1 || (create && SPI_processed != 1))
		elog(ERROR, "connector configuration write affected an unexpected number of rows");
	found = SPI_processed == 1;
	synchdb_credentials_close(credentials);
	synchdb_store_finish();
	return found;
}

#define STORE_STRING(key, value, optional, encrypted) \
	connector_store_string_field(key, value, sizeof(value), optional, encrypted)
#define STORE_PORT(key, value) connector_store_number_field(key, value, 65535)

void
synchdb_connector_store_create(const ConnectionInfo *config, const char *connector)
{
	ConnectorStoreField fields[10];

	if (!config || !connector || connector[0] == '\0')
		elog(ERROR, "invalid connector store creation arguments");
	/* Check the fixed-array name before passing it as a SQL text parameter. */
	(void) STORE_STRING("name", config->name, false, false);
	fields[0] = STORE_STRING("hostname", config->hostname, false, false);
	fields[1] = STORE_PORT("port", config->port);
	fields[2] = STORE_STRING("user", config->user, false, false);
	fields[3] = STORE_STRING("pwd", config->pwd, false, true);
	fields[4] = STORE_STRING("srcdb", config->srcdb, false, false);
	fields[5] = STORE_STRING("dstdb", config->dstdb, false, false);
	fields[6] = STORE_STRING("srcschema", config->srcschema, true, false);
	fields[7] = STORE_STRING("table", config->table, true, false);
	fields[8] = STORE_STRING("snapshottable", config->snapshottable, true, false);
	fields[9] = connector_store_string_field("connector", connector,
											  strlen(connector) + 1, false, false);
	(void) connector_store_write_fields(config->name, fields, lengthof(fields), true);
}

bool
synchdb_connector_store_set_extra(const char *name, const ExtraConnectionInfo *options)
{
	ConnectorStoreField fields[5];

	if (!options)
		elog(ERROR, "missing extra connector options");
	/* The legacy name argument allows an empty mode, and "null" is literal
	 * text here rather than the sentinel used by the other extra fields. */
	fields[0] = STORE_STRING("ssl_mode", options->ssl_mode, true, false);
	fields[0].isnull = false;
	fields[1] = STORE_STRING("ssl_keystore", options->ssl_keystore, true, false);
	fields[2] = STORE_STRING("ssl_keystore_pass", options->ssl_keystore_pass, true, true);
	fields[3] = STORE_STRING("ssl_truststore", options->ssl_truststore, true, false);
	fields[4] = STORE_STRING("ssl_truststore_pass", options->ssl_truststore_pass, true, true);
	return connector_store_write_fields(name, fields, lengthof(fields), false);
}

bool
synchdb_connector_store_set_fdw(const char *name, const FdwConnectionInfo *options)
{
	ConnectorStoreField fields[4];

	if (!options)
		elog(ERROR, "missing FDW connector options");
	fields[0] = STORE_STRING("fdw_ssl_cert", options->ssl_cert, true, false);
	fields[1] = STORE_STRING("fdw_ssl_key", options->ssl_key, true, false);
	fields[2] = STORE_STRING("fdw_ssl_rootcert", options->ssl_rootcert, true, false);
	fields[3] = STORE_STRING("fdw_ssl_cipher", options->ssl_cipher, true, true);
	return connector_store_write_fields(name, fields, lengthof(fields), false);
}

bool
synchdb_connector_store_set_jmx(const char *name, const JMXConnectionInfo *options)
{
	ConnectorStoreField fields[12];

	if (!options)
		elog(ERROR, "missing JMX connector options");
	fields[0] = STORE_STRING("jmx_listenaddr", options->jmx_listenaddr, false, false);
	fields[1] = STORE_PORT("jmx_port", options->jmx_port);
	fields[2] = STORE_STRING("jmx_rmi_address", options->jmx_rmiserveraddr, false, false);
	fields[3] = STORE_PORT("jmx_rmi_port", options->jmx_rmiport);
	fields[4] = connector_store_boolean_field("jmx_auth", options->jmx_auth);
	fields[5] = STORE_STRING("jmx_auth_passwdfile", options->jmx_auth_passwdfile, true, false);
	fields[6] = STORE_STRING("jmx_auth_accessfile", options->jmx_auth_accessfile, true, false);
	fields[7] = connector_store_boolean_field("jmx_ssl", options->jmx_ssl);
	fields[8] = STORE_STRING("jmx_ssl_keystore", options->jmx_ssl_keystore, true, false);
	fields[9] = STORE_STRING("jmx_ssl_keystore_pass", options->jmx_ssl_keystore_pass, true, true);
	fields[10] = STORE_STRING("jmx_ssl_truststore", options->jmx_ssl_truststore, true, false);
	fields[11] = STORE_STRING("jmx_ssl_truststore_pass", options->jmx_ssl_truststore_pass, true, true);
	return connector_store_write_fields(name, fields, lengthof(fields), false);
}

bool
synchdb_connector_store_set_jmx_exporter(const char *name, const JMXConnectionInfo *options)
{
	ConnectorStoreField fields[3];

	if (!options)
		elog(ERROR, "missing JMX exporter options");
	fields[0] = STORE_STRING("jmx_exporter", options->jmx_exporter, false, false);
	fields[1] = STORE_PORT("jmx_exporter_port", options->jmx_exporter_port);
	fields[2] = STORE_STRING("jmx_exporter_conf", options->jmx_exporter_conf, false, false);
	return connector_store_write_fields(name, fields, lengthof(fields), false);
}

bool
synchdb_connector_store_set_olr(const char *name, const OLRConnectionInfo *options)
{
	ConnectorStoreField fields[3];

	if (!options)
		elog(ERROR, "missing Openlog Replicator options");
	fields[0] = STORE_STRING("olr_host", options->olr_host, false, false);
	fields[1] = STORE_PORT("olr_port", options->olr_port);
	fields[2] = STORE_STRING("olr_source", options->olr_source, false, false);
	return connector_store_write_fields(name, fields, lengthof(fields), false);
}

bool
synchdb_connector_store_set_ispn(const char *name, const IspnInfo *options)
{
	ConnectorStoreField fields[3];

	if (!options)
		elog(ERROR, "missing Infinispan options");
	fields[0] = STORE_STRING("ispn_cache_type", options->ispn_cache_type, false, false);
	fields[1] = STORE_STRING("ispn_memory_type", options->ispn_memory_type, false, false);
	fields[2] = connector_store_number_field("ispn_memory_size",
											   options->ispn_memory_size, PG_UINT32_MAX);
	return connector_store_write_fields(name, fields, lengthof(fields), false);
}

#undef STORE_STRING
#undef STORE_PORT

bool
synchdb_connector_store_clear_options(const char *name, SynchdbConnectorOptionGroup group)
{
	static const char *const extra[] = {
		"ssl_mode", "ssl_keystore", "ssl_keystore_pass", "ssl_truststore", "ssl_truststore_pass"
	};
	static const char *const fdw[] = {
		"fdw_ssl_cert", "fdw_ssl_key", "fdw_ssl_rootcert", "fdw_ssl_cipher"
	};
	static const char *const jmx[] = {
		"jmx_listenaddr", "jmx_port", "jmx_rmi_address", "jmx_rmi_port", "jmx_auth",
		"jmx_auth_passwdfile", "jmx_auth_accessfile", "jmx_ssl", "jmx_ssl_keystore",
		"jmx_ssl_keystore_pass", "jmx_ssl_truststore", "jmx_ssl_truststore_pass"
	};
	static const char *const jmx_exporter[] = {
		"jmx_exporter", "jmx_exporter_port", "jmx_exporter_conf"
	};
	static const char *const olr[] = {"olr_host", "olr_port", "olr_source"};
	static const char *const ispn[] = {"ispn_cache_type", "ispn_memory_type", "ispn_memory_size"};
	const char *const *keys;
	int count;
	Oid argtypes[2] = {TEXTOID, TEXTARRAYOID};
	Datum values[2];
	Datum *elements;
	StringInfoData query;
	char *relation;
	bool found;
	int i;

	if (!name)
		elog(ERROR, "invalid connector store clear arguments");
	switch (group)
	{
		case SYNCHDB_CONNECTOR_OPTIONS_EXTRA:
			keys = extra; count = lengthof(extra); break;
		case SYNCHDB_CONNECTOR_OPTIONS_FDW:
			keys = fdw; count = lengthof(fdw); break;
		case SYNCHDB_CONNECTOR_OPTIONS_JMX:
			keys = jmx; count = lengthof(jmx); break;
		case SYNCHDB_CONNECTOR_OPTIONS_JMX_EXPORTER:
			keys = jmx_exporter; count = lengthof(jmx_exporter); break;
		case SYNCHDB_CONNECTOR_OPTIONS_OLR:
			keys = olr; count = lengthof(olr); break;
		case SYNCHDB_CONNECTOR_OPTIONS_ISPN:
			keys = ispn; count = lengthof(ispn); break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unknown connector option group")));
			pg_unreachable();
	}
	synchdb_store_connect();
	relation = synchdb_store_relation(SYNCHDB_CONNINFO_TABLE, RowExclusiveLock);
	elements = palloc(count * sizeof(Datum));
	for (i = 0; i < count; i++)
		elements[i] = CStringGetTextDatum(keys[i]);
	values[0] = CStringGetTextDatum(name);
	values[1] = PointerGetDatum(construct_array(elements, count, TEXTOID, -1, false, TYPALIGN_INT));
	initStringInfo(&query);
	appendStringInfo(&query, "UPDATE %s SET data = data OPERATOR(pg_catalog.-) $2 "
					 "WHERE name COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) $1 COLLATE pg_catalog.\"C\"", relation);
	if (SPI_execute_with_args(query.data, 2, argtypes, values, NULL, false, 0) != SPI_OK_UPDATE)
		elog(ERROR, "could not clear connector options");
	if (SPI_processed > 1)
		elog(ERROR, "clearing connector options affected multiple configurations");
	found = SPI_processed == 1;
	synchdb_store_finish();
	return found;
}

static bool
connector_store_delete_internal(const char *name, const SynchdbConnectorKey *key)
{
	static const char *const table_names[] = {
		SYNCHDB_CONNINFO_TABLE, SYNCHDB_ATTRIBUTE_TABLE, SYNCHDB_OBJECT_MAPPING_TABLE
	};
	char *relations[lengthof(table_names)];
	Oid argtypes[2] = {TEXTOID, UUIDOID};
	Datum values[2];
	bool found = false;
	int i;

	if (!name)
		elog(ERROR, "invalid connector store deletion arguments");
	if (key && (!synchdb_connector_key_is_valid(key) || key->database_oid != MyDatabaseId))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connector deletion identity must belong to this database")));
	synchdb_store_connect();
	/* Validate/lock all owned relations before the first mutation. */
	for (i = 0; i < (int) lengthof(table_names); i++)
		relations[i] = synchdb_store_relation(table_names[i], RowExclusiveLock);
	values[0] = CStringGetTextDatum(name);
	if (key)
		values[1] = UUIDPGetDatum(&key->connector_id);
	for (i = 0; i < (int) lengthof(table_names); i++)
	{
		StringInfoData query;

		initStringInfo(&query);
		appendStringInfo(&query, "DELETE FROM %s WHERE %s COLLATE pg_catalog.\"C\" "
						 "OPERATOR(pg_catalog.=) $1 COLLATE pg_catalog.\"C\"",
						 relations[i], i == 0 ? "name" : "name::pg_catalog.text");
		if (i == 0 && key)
			appendStringInfoString(&query, " AND connector_id OPERATOR(pg_catalog.=) $2");
		if (SPI_execute_with_args(query.data, i == 0 && key ? 2 : 1,
								  argtypes, values, NULL, false, 0) != SPI_OK_DELETE)
			elog(ERROR, "could not delete connector metadata");
		if (i == 0)
		{
			if (SPI_processed > 1)
				elog(ERROR, "connector deletion affected multiple configurations");
			found = SPI_processed == 1;
			/* An absent, filtered or trigger-vetoed parent is not permission
			 * to remove its dependent metadata or external resources. */
			if (!found)
			{
				synchdb_store_finish();
				return false;
			}
		}
	}
	synchdb_store_finish();
	return found;
}

bool
synchdb_connector_store_delete(const char *name)
{
	return connector_store_delete_internal(name, NULL);
}

bool
synchdb_connector_store_delete_identity(const char *name, const SynchdbConnectorKey *key)
{
	if (!key)
		elog(ERROR, "connector deletion requires an identity");
	return connector_store_delete_internal(name, key);
}
