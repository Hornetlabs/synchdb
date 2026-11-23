/*
 * debezium_event_handler.c
 *
 * contains routines to process change events originated from
 * debezium connectors.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "catalog/namespace.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include <time.h>
#include <sys/time.h>
#include <dlfcn.h>

#include "access/table.h"
#include "synchdb/synchdb.h"
#include "converter/debezium_event_handler.h"
#include "converter/format_converter.h"
#include "source/debezium_event.h"
#include "source/debezium_row.h"
#include "source/debezium_schema.h"
#include "converter/ddl_plan.h"
#include "converter/rule_snapshot.h"
#include "converter/debezium_bind.h"
#include "executor/truncate.h"
#include "runtime/config.h"
#include "port/pg_bswap.h"
#ifdef WITH_OLR
#include "olr/olr_client.h"
#endif

static bool isInSnapshot = false;

/* Legacy bind/convert/write adapters. Decoding does not access target metadata. */
static SynchdbEventOutcome
apply_dbz_schema(const SynchdbDbzEvent *event, SynchdbStatistics *statistics,
                 ConnectorType type, const char *name, bool snapshot,
                 bool first, bool last)
{
 SynchdbDbzSchemaChange source;
 SynchdbDdlPlan *plan;
 SynchdbDdlApplyResult result;
 const SynchdbRuleSnapshot *rules = synchdb_rule_snapshot_current();
 ConnectorType planning_type = type;
 ListCell *cell;
 List *tables = NIL;

 if (event->schema_empty)
  return SYNCHDB_EVENT_NOOP;
 set_shm_connector_state(myConnectorId, STATE_PARSING);
 synchdb_dbz_decode_schema(event, &source);
 /* Snapshot schema events describe a baseline, not source-side deletions.
  * MySQL emits synthetic DROP/CREATE pairs while rebuilding its schema
  * history. Applying that DROP would destroy an existing target before the
  * CREATE image can validate it. Keep targets and bindings intact; streamed
  * DROP events still pass through the ordinary DDL planner. */
 foreach(cell, source.tables)
 {
  const SynchdbDbzTableChange *table = lfirst(cell);

  if (!snapshot || table->operation != SYNCHDB_DBZ_SCHEMA_DROP)
   tables = lappend(tables, (void *) table);
 }
 if (tables == NIL)
  return SYNCHDB_EVENT_NOOP;
 source.tables = tables;
 set_shm_connector_state(myConnectorId, STATE_CONVERTING);
 /* An OLR run receives its initial relational image from Debezium Oracle. */
 if (rules && synchdb_rule_snapshot_type(rules) == TYPE_OLR && type == TYPE_ORACLE)
  planning_type = TYPE_OLR;
 plan = synchdb_ddl_plan_build(name, planning_type, rules,
                              &source, CurrentMemoryContext);
 set_shm_connector_state(myConnectorId, STATE_EXECUTING);
 result = synchdb_ddl_plan_apply(plan);
 synchdb_ddl_plan_destroy(plan);
 if (result.tables_changed > INT_MAX || result.tables_created > INT_MAX)
  elog(ERROR, "schema change statistics exceed the supported event count");
 if (!snapshot)
  increment_connector_statistics(statistics, STATS_DDL, (int) result.tables_changed);
 else
  increment_connector_statistics(statistics, STATS_TABLES, (int) result.tables_created);
 return result.tables_changed ? SYNCHDB_EVENT_APPLIED : SYNCHDB_EVENT_NOOP;
}

static SynchdbEventOutcome
apply_dbz_truncate(const SynchdbDbzEvent *event, SynchdbStatistics *statistics)
{
	Relation relation;

	if (synchdb_operation_skipped('t'))
	{
		elog(LOG, "skipping source TRUNCATE for %s.%s by configured operation policy",
			 event->database, event->table);
		return SYNCHDB_EVENT_NOOP;
	}
	set_shm_connector_state(myConnectorId, STATE_CONVERTING);
	relation = synchdb_bind_source_table(event->database, event->schema,
										event->table, AccessExclusiveLock);
	set_shm_connector_state(myConnectorId, STATE_EXECUTING);
	synchdb_execute_truncate(relation);
	increment_connector_statistics(statistics, STATS_DDL, 1);
	increment_connector_statistics(statistics, STATS_TRUNCATE, 1);
	return SYNCHDB_EVENT_APPLIED;
}

static SynchdbEventOutcome
apply_dbz_row(const SynchdbDbzEvent *event, SynchdbStatistics *statistics,
              ConnectorType type, bool snapshot)
{
	SynchdbDbzRow row;
	SynchdbTargetChange *target;
	SynchdbEventOutcome outcome = SYNCHDB_EVENT_FAILED;

	set_shm_connector_state(myConnectorId, STATE_PARSING);
	synchdb_dbz_decode_row(event, &row);
	set_shm_connector_state(myConnectorId, STATE_CONVERTING);
	target = synchdb_bind_dbz_row(&row, type);
	if (target != NULL)
	{
		set_shm_connector_state(myConnectorId, STATE_EXECUTING);
		if (ra_executePGDML(target, type, statistics, snapshot) == 0)
		{
			if (!snapshot)
				increment_connector_statistics(statistics, STATS_DML, 1);
			outcome = SYNCHDB_EVENT_APPLIED;
		}
		destroyPGDML(target);
	}
	return outcome;
}

static void
record_dbz_event_time(const SynchdbDbzEvent *event, SynchdbStatistics *statistics,
                      bool first, bool last)
{
	struct timeval time;
	uint64 now;

	if (!first && !last)
		return;
	gettimeofday(&time, NULL);
	now = (uint64) time.tv_sec * 1000 + time.tv_usec / 1000;
	if (first)
	{
		statistics->genstats.stats_first_src_ts = event->src_ts_ms;
		statistics->genstats.stats_first_pg_ts = now;
	}
	if (last)
	{
		statistics->genstats.stats_last_src_ts = event->src_ts_ms;
		statistics->genstats.stats_last_pg_ts = now;
	}
}

/* A heartbeat/transaction does not terminate a snapshot between source rows. */
static void
record_dbz_snapshot(const SynchdbDbzEvent *event, SynchdbStatistics *statistics,
                    int flags, bool snapshot)
{
	bool last = event->snapshot == SYNCHDB_DBZ_SNAPSHOT_LAST;
	struct timeval time;
	uint64 now;

	gettimeofday(&time, NULL);
	now = (uint64) time.tv_sec * 1000 + time.tv_usec / 1000;
	if (snapshot)
	{
		if (flags & CONNFLAG_SCHEMA_SYNC_MODE)
			set_shm_connector_stage(myConnectorId, STAGE_SCHEMA_SYNC);
		else if (flags & CONNFLAG_INITIAL_SNAPSHOT_MODE)
			set_shm_connector_stage(myConnectorId, STAGE_INITIAL_SNAPSHOT);
		if (!isInSnapshot)
		{
			statistics->snapstats.snapstats_begintime_ts = now;
			statistics->snapstats.snapstats_endtime_ts = 0;
		}
		if (last)
			statistics->snapstats.snapstats_endtime_ts = now;
	}
	else
		set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
	isInSnapshot = snapshot && !last;


	set_shm_connector_state(myConnectorId,
			last && (flags & (CONNFLAG_SCHEMA_SYNC_MODE | CONNFLAG_INITIAL_SNAPSHOT_MODE)) ?
			STATE_SCHEMA_SYNC_DONE : STATE_SYNCING);
}

/* The caller owns the batch transaction and retains the original record for ACK. */
SynchdbEventOutcome
fc_processDBZChangeEvent(const char *json, SynchdbStatistics *statistics,
                        int flags, const char *name, bool first, bool last)
{
	MemoryContext caller = CurrentMemoryContext;
	MemoryContext context;
	volatile SynchdbEventOutcome outcome = SYNCHDB_EVENT_FAILED;

	context = AllocSetContextCreate(caller, "Debezium event", ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(context);
	PG_TRY();
	{
		SynchdbDbzEvent event;

		synchdb_dbz_decode_event(json, &event);
		switch (event.kind)
		{
			case SYNCHDB_DBZ_EVENT_UNSUPPORTED:
				elog(WARNING, "unsupported Debezium event: %s", event.unsupported_reason);
				outcome = SYNCHDB_EVENT_UNSUPPORTED;
				break;
			case SYNCHDB_DBZ_EVENT_TRANSACTION:
				increment_connector_statistics(statistics, STATS_TX, 1);
				outcome = SYNCHDB_EVENT_CONTROL;
				break;
			case SYNCHDB_DBZ_EVENT_HEARTBEAT:
				outcome = SYNCHDB_EVENT_CONTROL;
				break;
			case SYNCHDB_DBZ_EVENT_ROW:
			case SYNCHDB_DBZ_EVENT_SCHEMA:
			case SYNCHDB_DBZ_EVENT_LOGICAL_MESSAGE:
			case SYNCHDB_DBZ_EVENT_TRUNCATE:
			{
				ConnectorType type = fc_get_connector_type(event.connector);
				const SynchdbRuleSnapshot *rules = synchdb_rule_snapshot_current();
				bool snapshot = event.snapshot != SYNCHDB_DBZ_SNAPSHOT_FALSE &&
					event.snapshot != SYNCHDB_DBZ_SNAPSHOT_INCREMENTAL;

				if (rules &&
					(strcmp(synchdb_rule_snapshot_connector(rules), name) != 0 ||
					 (synchdb_rule_snapshot_type(rules) != type &&
					  !(synchdb_rule_snapshot_type(rules) == TYPE_OLR && type == TYPE_ORACLE))))
					ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									errmsg("source event does not match the active connector rules")));

				if (event.kind == SYNCHDB_DBZ_EVENT_ROW)
					outcome = apply_dbz_row(&event, statistics, type, snapshot);
				else if (event.kind == SYNCHDB_DBZ_EVENT_TRUNCATE)
					outcome = apply_dbz_truncate(&event, statistics);
				else
					outcome = apply_dbz_schema(&event, statistics, type, name, snapshot, first, last);
				if (outcome == SYNCHDB_EVENT_APPLIED || outcome == SYNCHDB_EVENT_NOOP)
					record_dbz_snapshot(&event, statistics, flags, snapshot);
				else
					set_shm_connector_state(myConnectorId, STATE_SYNCING);
				break;
			}
			default:
				elog(ERROR, "unexpected Debezium event kind: %d", (int) event.kind);
		}
		if (outcome == SYNCHDB_EVENT_FAILED || outcome == SYNCHDB_EVENT_UNSUPPORTED)
			increment_connector_statistics(statistics, STATS_BAD_CHANGE_EVENT, 1);
		else
			record_dbz_event_time(&event, statistics, first, last);
	}
	PG_FINALLY();
	{
		MemoryContextSwitchTo(caller);
		MemoryContextDelete(context);
	}
	PG_END_TRY();
	return outcome;
}
