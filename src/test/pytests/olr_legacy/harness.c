/* Source-free tests of production framing, replay filtering and cursor SQL.
 * Only catalog discovery is redirected to a private table; the UPDATE and its
 * transaction are production code. No source is contacted or acknowledged. */
#include "postgres.h"
#include "fmgr.h"
#include "catalog/catalog_access.h"
#include "executor/spi.h"
#include "olr/olr_handoff.h"
#include "runtime/run_spec.h"
#include "source/consistency.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"

static void
fixture_connect(void)
{
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "fixture could not connect SPI");
}

static void fixture_finish(void) { SPI_finish(); }
static int fixture_begin_path(void) { return 0; }
static void fixture_end_path(int level) { (void) level; }
static char *fixture_relation(const char *name, LOCKMODE mode)
{
	(void) name;
	(void) mode;
	return pstrdup("fixture.positions");
}

#define synchdb_store_connect fixture_connect
#define synchdb_store_finish fixture_finish
#define synchdb_store_begin_path fixture_begin_path
#define synchdb_store_end_path fixture_end_path
#define synchdb_store_relation fixture_relation
#include "../../../backend/olr/olr_handoff.c"
#undef synchdb_store_connect
#undef synchdb_store_finish
#undef synchdb_store_begin_path
#undef synchdb_store_end_path
#undef synchdb_store_relation
#include "../../../backend/olr/olr_client.c"

PG_FUNCTION_INFO_V1(olr_legacy_fixture_scope);
Datum
olr_legacy_fixture_scope(PG_FUNCTION_ARGS)
{
	SynchdbRunSpec spec = {0};
	bool fdw = PG_GETARG_BOOL(3);

	spec.source_dialect = SYNCHDB_SOURCE_ORACLE;
	spec.capture_backend = SYNCHDB_CAPTURE_OLR;
	spec.snapshot_provider = fdw ? SYNCHDB_SNAPSHOT_FDW : SYNCHDB_SNAPSHOT_DEBEZIUM;
	spec.connection.snapengine = fdw ? ENGINE_FDW : ENGINE_DEBEZIUM;
	spec.connection.run_options.dbz_snapshot_thread_num = 1;
	strlcpy(spec.snapshot_mode, text_to_cstring(PG_GETARG_TEXT_PP(0)), sizeof(spec.snapshot_mode));
	strlcpy(spec.connection.table, text_to_cstring(PG_GETARG_TEXT_PP(1)), sizeof(spec.connection.table));
	strlcpy(spec.connection.snapshottable, text_to_cstring(PG_GETARG_TEXT_PP(2)), sizeof(spec.connection.snapshottable));
	synchdb_source_consistency_validate_run(&spec);
	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(olr_legacy_fixture_allow_fragment);
Datum
olr_legacy_fixture_allow_fragment(PG_FUNCTION_ARGS)
{
	source_proof = PG_GETARG_JSONB_P(0);
	olr_client_set_scns(PG_GETARG_INT64(1), PG_GETARG_INT64(2), PG_GETARG_INT64(3));
	PG_RETURN_BOOL(legacy_leading_fragment_allowed(source_proof));
}

PG_FUNCTION_INFO_V1(olr_legacy_fixture_prefix);
Datum
olr_legacy_fixture_prefix(PG_FUNCTION_ARGS)
{
	Datum *records;
	bool *nulls;
	int count;
	int safe;
	int frames = 0;
	int offset = 0;

	deconstruct_array(PG_GETARG_ARRAYTYPE_P(0), TEXTOID, -1, false, TYPALIGN_INT,
					  &records, &nulls, &count);
	memset(&g_strinfo, 0, sizeof(g_strinfo));
	initStringInfo(&g_strinfo);
	g_legacy_leading_fragment = PG_GETARG_BOOL(1);
	for (int index = 0; index < count; index++)
	{
		char *record;
		uint32 size;

		if (nulls[index]) elog(ERROR, "fixture record must not be null");
		record = TextDatumGetCString(records[index]);
		size = strlen(record);
		appendBinaryStringInfo(&g_strinfo, (char *) &size, 4);
		appendBinaryStringInfo(&g_strinfo, record, size);
	}
	safe = complete_prefix();
	while (offset < safe)
	{
		uint32 size;

		memcpy(&size, g_strinfo.data + offset, 4);
		offset += 4 + size;
		frames++;
	}
	pfree(g_strinfo.data);
	memset(&g_strinfo, 0, sizeof(g_strinfo));
	PG_RETURN_INT32(frames);
}

static void
fixture_boundary(FunctionCallInfo fcinfo)
{
	source_proof = PG_GETARG_JSONB_P(0);
	snapshot_scn = PG_GETARG_INT64(1);
	olr_client_set_scns(PG_GETARG_INT64(2), PG_GETARG_INT64(3), PG_GETARG_INT64(4));
	active = true;
	memset(&key, 0, sizeof(key));
	memset(&paths, 0, sizeof(paths));
	key.database_oid = MyDatabaseId;
}

PG_FUNCTION_INFO_V1(olr_legacy_fixture_filter);
Datum
olr_legacy_fixture_filter(PG_FUNCTION_ARGS)
{
	Datum *records;
	bool *nulls;
	int count;
	StringInfoData result;

	fixture_boundary(fcinfo);
	deconstruct_array(PG_GETARG_ARRAYTYPE_P(5), JSONBOID, -1, false, TYPALIGN_INT,
					  &records, &nulls, &count);
	initStringInfo(&result);
	appendStringInfoString(&result, "{\"apply\":[");
	for (int index = 0; index < count; index++)
	{
		bool applied;

		if (nulls[index]) elog(ERROR, "fixture record must not be null");
		applied = olr_handoff_apply_record(DatumGetJsonbP(records[index]));
		appendStringInfo(&result, "%s%s", index ? "," : "", applied ? "true" : "false");
	}
	appendStringInfo(&result, "],\"scn\":%llu,\"c_scn\":%llu,\"c_idx\":%llu}",
		(unsigned long long) olr_client_get_scn(),
		(unsigned long long) olr_client_get_c_scn(),
		(unsigned long long) olr_client_get_c_idx());
	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(result.data)));
}

PG_FUNCTION_INFO_V1(olr_legacy_fixture_write);
Datum
olr_legacy_fixture_write(PG_FUNCTION_ARGS)
{
	Datum *records;
	bool *nulls;
	int count;

	fixture_boundary(fcinfo);
	deconstruct_array(PG_GETARG_ARRAYTYPE_P(5), JSONBOID, -1, false, TYPALIGN_INT,
					  &records, &nulls, &count);
	fixture_connect();
	for (int index = 0; index < count; index++)
	{
		Jsonb *record;

		if (nulls[index]) elog(ERROR, "fixture record must not be null");
		record = DatumGetJsonbP(records[index]);
		if (olr_handoff_apply_record(record))
		{
			Oid type[] = {INT8OID};
			Datum id[] = {Int64GetDatum(olr_json_uint(record, "rowid"))};

			if (SPI_execute_with_args("INSERT INTO fixture.items VALUES ($1)",
				1, type, id, NULL, false, 0) != SPI_OK_INSERT)
				elog(ERROR, "fixture row insert failed");
		}
	}
	fixture_finish();
	olr_handoff_store_position();
	if (PG_GETARG_BOOL(6)) elog(ERROR, "fixture failure after row and cursor writes");
	PG_RETURN_VOID();
}
