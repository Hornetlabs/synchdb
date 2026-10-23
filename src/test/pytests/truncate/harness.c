/* Test-only PostgreSQL module. Real parsers, converters and SPI executor;
 * connector shared memory and OLR checkpoint transport are replaced locally.
 * Including the converter lets tests install mappings without a running JVM.
 */
#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "utils/array.h"
#include "utils/json.h"
#include "utils/resowner.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"
#include "synchdb/synchdb.h"
#include "converter/debezium_event_handler.h"
#include "converter/format_converter.h"
#ifdef WITH_OLR
#include "converter/olr_event_handler.h"
#include "olr/olr_client.h"
#endif
#include "../../../backend/converter/format_converter.c"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(synchdb_test_batch);

int myConnectorId = 0;
bool synchdb_dml_use_spi = false;
bool synchdb_log_event_on_error = false;
char * g_eventStr = NULL;
char * dbz_skipped_operations = "t";
int synchdb_error_strategy = STRAT_EXIT_ON_ERROR;
int synchdb_letter_casing_strategy = LCS_NORMALIZE_LOWERCASE;
static ConnectorType test_type;
static ConnectorStage test_stage = STAGE_CHANGE_DATA_CAPTURE;
static orascn test_scn;

void set_shm_connector_state(int id, ConnectorState state) {}
void set_shm_connector_stage(int id, ConnectorStage stage) { test_stage = stage; }
void set_shm_connector_errmsg(int id, const char * err) {}
ConnectorType get_shm_connector_type_enum(int id) { return test_type; }
ConnectorStage get_shm_connector_stage_enum(int id) { return test_stage; }
const char * get_shm_connector_user_by_id(int id) { return "OWNER"; }
const char * connectorTypeToString(ConnectorType type)
{
	switch (type)
	{
		case TYPE_POSTGRES: return "postgresql";
		case TYPE_MYSQL: return "mysql";
		case TYPE_ORACLE: return "oracle";
		case TYPE_OLR: return "olr";
		default: return "unknown";
	}
}
#ifdef WITH_OLR
void olr_client_set_scns(orascn scn, orascn commit_scn, orascn index) { test_scn = commit_scn; }
#endif

/* Same local counter updates as the worker; no shared-memory publication. */
void increment_connector_statistics(SynchdbStatistics * s, ConnectorStatistics which, int n)
{
	switch (which)
	{
		case STATS_DDL: s->cdcstats.stats_ddl += n; break;
		case STATS_DML: s->cdcstats.stats_dml += n; break;
		case STATS_CREATE: s->cdcstats.stats_create += n; break;
		case STATS_UPDATE: s->cdcstats.stats_update += n; break;
		case STATS_DELETE: s->cdcstats.stats_delete += n; break;
		case STATS_TRUNCATE: s->cdcstats.stats_truncate += n; break;
		case STATS_TX: s->cdcstats.stats_tx += n; break;
		case STATS_ROWS: s->snapstats.snapstats_rows += n; break;
		case STATS_TABLES: s->snapstats.snapstats_tables += n; break;
		case STATS_BAD_CHANGE_EVENT: s->genstats.stats_bad_change_event += n; break;
		default: break;
	}
}

Datum synchdb_test_batch(PG_FUNCTION_ARGS)
{
	ArrayType * events = PG_GETARG_ARRAYTYPE_P(0);
	char * connector = text_to_cstring(PG_GETARG_TEXT_PP(1));
	Jsonb * mappings = PG_GETARG_JSONB_P(4);
	Datum * values;
	bool * nulls;
	int count, i;
	SynchdbStatistics * stats = palloc0(sizeof(SynchdbStatistics));
	MemoryContext saved = CurrentMemoryContext;
	ResourceOwner owner = CurrentResourceOwner;
	volatile int result = 0;
	volatile bool confirmed = false;
	char * error = NULL;
	StringInfoData output;
	HASHCTL ctl = {0};
	JsonbIterator * iter;
	JsonbValue value;
	JsonbIteratorToken token;
	ObjMapHashKey key = {0};

	test_type = fc_get_connector_type(connector);
	dbz_skipped_operations = text_to_cstring(PG_GETARG_TEXT_PP(2));
	synchdb_letter_casing_strategy = PG_GETARG_INT32(3);
	synchdb_dml_use_spi = PG_GETARG_BOOL(5);
	test_scn = 0;
	if (objectMappingHash)
		hash_destroy(objectMappingHash);
	ctl.keysize = sizeof(ObjMapHashKey);
	ctl.entrysize = sizeof(ObjMapHashEntry);
	ctl.hcxt = TopMemoryContext;
	objectMappingHash = hash_create("test mappings", 16, &ctl,
		HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	iter = JsonbIteratorInit(&mappings->root);
	while ((token = JsonbIteratorNext(&iter, &value, true)) != WJB_DONE)
	{
		if (token == WJB_KEY)
		{
			memset(&key, 0, sizeof(key));
			strlcpy(key.extObjType, "table", sizeof(key.extObjType));
			strlcpy(key.extObjName, pnstrdup(value.val.string.val, value.val.string.len), sizeof(key.extObjName));
		}
		else if (token == WJB_VALUE && value.type == jbvString)
		{
			ObjMapHashEntry * entry = hash_search(objectMappingHash, &key, HASH_ENTER, NULL);
			strlcpy(entry->pgsqlObjName, pnstrdup(value.val.string.val, value.val.string.len), sizeof(entry->pgsqlObjName));
		}
	}
	MemoryContextSwitchTo(TopMemoryContext);
	fc_resetDataCache();
	MemoryContextSwitchTo(saved);
	deconstruct_array(events, TEXTOID, -1, false, TYPALIGN_INT, &values, &nulls, &count);
	BeginInternalSubTransaction(NULL);
	PG_TRY();
	{
		for (i = 0; i < count; i++)
		{
			char * event = TextDatumGetCString(values[i]);
			bool sendconfirm = false;

			if (test_type == TYPE_OLR)
			{
#ifdef WITH_OLR
#if SYNCHDB_PG_MAJOR_VERSION >= 1700
				text * raw = cstring_to_text(event);
#else
				char * raw = event;
#endif
				result = fc_processOLRChangeEvent(raw, strlen(event), stats,
					"truncate_test", &sendconfirm, i == 0, i == count - 1);
#else
				elog(ERROR, "test module built without OLR");
#endif
			}
			else
			{
				result = fc_processDBZChangeEvent(event, stats, 0,
					"truncate_test", i == 0, i == count - 1);
				sendconfirm = result == 0;
			}
			if (result < 0)
				elog(ERROR, "event rejected; batch must not be acknowledged");
			confirmed = sendconfirm;
		}
		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(saved);
		CurrentResourceOwner = owner;
	}
	PG_CATCH();
	{
		ErrorData * edata;

		MemoryContextSwitchTo(saved);
		edata = CopyErrorData();
		error = pstrdup(edata->message);
		FreeErrorData(edata);
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(saved);
		CurrentResourceOwner = owner;
		result = -1;
		confirmed = false;
		test_scn = 0;
		/* Worker never publishes successful counters from an aborted batch. */
		memset(&stats->cdcstats, 0, sizeof(stats->cdcstats));

	}
	PG_END_TRY();
	initStringInfo(&output);
	appendStringInfo(&output,
		"{\"ret\":%d,\"confirmed\":%s,\"truncates\":%llu,\"ddls\":%llu,"
		"\"dmls\":%llu,\"creates\":%llu,\"updates\":%llu,\"deletes\":%llu,"
		"\"bad_events\":%llu,\"scn\":%llu,\"failed\":%s,\"error\":",
		result, confirmed ? "true" : "false", stats->cdcstats.stats_truncate,
		stats->cdcstats.stats_ddl, stats->cdcstats.stats_dml, stats->cdcstats.stats_create,
		stats->cdcstats.stats_update, stats->cdcstats.stats_delete,
		stats->genstats.stats_bad_change_event, test_scn, error ? "true" : "false");
	escape_json(&output, error ? error : "");
	appendStringInfoChar(&output, '}');
	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(output.data)));
}
