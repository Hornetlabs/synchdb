/* Test-only extension: production decoders, bindings, writers and GUCs.
 * These two fixtures stand in for an already accepted run and source boundary.
 * No production SQL entry points or source transport acknowledgements are added.
 */
#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type_d.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "synchdb/synchdb.h"
#include "source/debezium_event.h"
#include "source/debezium_schema.h"
#include "converter/debezium_event_handler.h"
#include "converter/format_converter.h"
#include "converter/rule_snapshot.h"

/* Activation normally occurs in an idle worker, outside a SQL transaction. */
#include "../../../backend/converter/rule_snapshot.c"

#ifdef WITH_OLR
#include "olr/olr_handoff.h"
#include "olr/olr_client.h"

static bool
fixture_handoff(Jsonb *record)
{
	/* Production shape/numeric checks, without a real snapshot proof. */
	(void) olr_json_payload(record);
	olr_client_set_scns(olr_json_uint(record, "scn"),
		olr_json_uint(record, "c_scn"), olr_json_uint(record, "c_idx"));
	return true;
}

#define olr_handoff_apply_record fixture_handoff
#include "../../../backend/converter/olr_event_handler.c"
#undef olr_handoff_apply_record
#endif

PG_FUNCTION_INFO_V1(truncate_fixture_batch);
Datum
truncate_fixture_batch(PG_FUNCTION_ARGS)
{
	Datum *records;
	bool *nulls;
	int count;
	int index;
	bool native = PG_GETARG_BOOL(1);
	char *mapped = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(2));
	SynchdbStatistics stats = {0};
	char *result;

	deconstruct_array(PG_GETARG_ARRAYTYPE_P(0), TEXTOID, -1, false, TYPALIGN_INT,
					  &records, &nulls, &count);
	for (index = 0; index < count; index++)
	{
		char *json;
		char *source;
		ConnectorType type;
		ObjectMap rule = {0};
		SynchdbRuleSnapshot *rules;
		int outcome = -1;

		if (nulls[index])
			elog(ERROR, "fixture records must not be null");
		json = TextDatumGetCString(records[index]);
		if (native)
		{
#ifdef WITH_OLR
			Jsonb *document = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(json)));
			Jsonb *payload = olr_json_payload(document);
			Datum path[] = {CStringGetTextDatum("schema")};
			Jsonb *schema = GET_JSONB_ELEM(payload, path, 1);

			source = synchdb_source_table_key(olr_json_text(document, "db"),
				olr_json_text(schema, "owner"), olr_json_text(schema, "table"));
			type = TYPE_OLR;
#else
			elog(ERROR, "fixture requires a WITH_OLR build");
#endif
		}
		else
		{
			SynchdbDbzEvent event;

			synchdb_dbz_decode_event(json, &event);
			source = event.kind == SYNCHDB_DBZ_EVENT_TRUNCATE ?
				synchdb_source_table_key(event.database, event.schema, event.table) :
				pstrdup("fixture.public.items");
			type = fc_get_connector_type(event.connector);
		}
		strlcpy(rule.objtype, "table", sizeof(rule.objtype));
		rule.srcobj = source;
		rule.enabled = true;
		if (mapped)
			strlcpy(rule.dstobj, mapped, sizeof(rule.dstobj));
		rules = synchdb_rule_snapshot_build("truncate_fixture", type,
			mapped ? &rule : NULL, mapped ? 1 : 0, TopMemoryContext);
		rules->generation = ++last_generation;
		active_snapshot = rules;
		PG_TRY();
		{
#ifdef WITH_OLR
			if (native)
			{
				bool confirm = false;
#if SYNCHDB_PG_MAJOR_VERSION >= 1700
				void *record = cstring_to_text(json);
#else
				void *record = json;
#endif

				outcome = fc_processOLRChangeEvent(record, &stats, "truncate_fixture",
					&confirm, index == 0, index == count - 1);
			}
			else
#endif
				outcome = fc_processDBZChangeEvent(json, &stats, 0, "truncate_fixture",
					index == 0, index == count - 1);
			if (outcome < 0)
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("fixture source event was rejected")));
		}
		PG_FINALLY();
		{
			synchdb_rule_snapshot_clear();
		}
		PG_END_TRY();
	}
	result = psprintf("{\"truncates\":%llu,\"ddls\":%llu,\"bad\":%llu}",
		stats.cdcstats.stats_truncate, stats.cdcstats.stats_ddl,
		stats.genstats.stats_bad_change_event);
	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(result)));
}
