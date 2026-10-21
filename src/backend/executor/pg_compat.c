/* Keep major-version API differences out of row and schema semantics. */
#include "postgres.h"

#include "access/table.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "commands/trigger.h"
#include "common/base64.h"
#include "executor/pg_compat.h"
#include "postmaster/bgworker.h"
#include "utils/acl.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static bool metadata_callbacks_registered = false;
static uint64 metadata_epoch = 1;

void
synchdb_pg_worker_check_connection(Oid database_oid, Oid role_oid)
{
	HeapTuple tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(role_oid));
	Form_pg_authid role;
	bool is_superuser;

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				 errmsg("role with OID %u does not exist", role_oid)));
	role = (Form_pg_authid) GETSTRUCT(tuple);
#if PG_VERSION_NUM < 170000
	if (!role->rolcanlogin)
	{
		char *name = pstrdup(NameStr(role->rolname));

		ReleaseSysCache(tuple);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				 errmsg("role \"%s\" is not permitted to log in", name)));
	}
#endif
	is_superuser = role->rolsuper;
	ReleaseSysCache(tuple);
	if (!is_superuser &&
		object_aclcheck(DatabaseRelationId, database_oid, role_oid, ACL_CONNECT) != ACLCHECK_OK)
		aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_DATABASE, get_database_name(database_oid));
	/* Core checks these privileges again during initialization. Preflight
	 * preserves actionable receipt diagnostics for failures reported there
	 * as FATAL, which cannot be captured by the worker's PG_CATCH. */
}

void
synchdb_pg_worker_initialize_connection(Oid database_oid, Oid role_oid)
{
#if PG_VERSION_NUM >= 170000
	BackgroundWorkerInitializeConnectionByOid(database_oid, role_oid,
										 BGWORKER_BYPASS_ROLELOGINCHECK);
#else
	/* PG16 has no public API to bypass rolcanlogin. Preserve its connection
	 * checks, including database CONNECT and role settings, rather than
	 * connecting as the bootstrap superuser and switching identities later. */
	BackgroundWorkerInitializeConnectionByOid(database_oid, role_oid, 0);
#endif
}

int
synchdb_pg_base64_decode(const char *source, int length,
						unsigned char *destination, int capacity)
{
#if PG_VERSION_NUM >= 180000
	return pg_b64_decode(source, length, destination, capacity);
#else
	return pg_b64_decode(source, length, (char *) destination, capacity);
#endif
}

Oid
synchdb_pg_primary_key_index(Relation relation, bool allow_deferrable)
{
#if PG_VERSION_NUM >= 180000
	return RelationGetPrimaryKeyIndex(relation, allow_deferrable);
#else
	Oid constraint = InvalidOid;
	Bitmapset *attributes;

	if (!allow_deferrable)
		return RelationGetPrimaryKeyIndex(relation);
	attributes = get_primary_key_attnos(RelationGetRelid(relation), true, &constraint);
	bms_free(attributes);
	return OidIsValid(constraint) ? get_constraint_index(constraint) : InvalidOid;
#endif
}

void
synchdb_pg_init_range_table(EState *estate, List *range_table, List *permissions)
{
#if PG_VERSION_NUM >= 180000
	ExecInitRangeTable(estate, range_table, permissions, bms_make_singleton(1));
#else
	ExecInitRangeTable(estate, range_table, permissions);
#endif
}

void
synchdb_pg_close_trigger_relations(EState *estate)
{
#if PG_VERSION_NUM >= 180000
	ExecCloseTrigTargetRelations(estate);
#else
	ListCell *cell;

	/* PostgreSQL 16/17 originally keep this loop inside
	 * ExecCloseResultRelations. Only close the dummy trigger relations here;
	 * the estate context still owns this list and its ResultRelInfos. */
	foreach(cell, estate->es_trig_target_relations)
	{
		ResultRelInfo *result = lfirst(cell);

		Assert(result->ri_RangeTableIndex == 0);
		Assert(result->ri_NumIndices == 0);
		table_close(result->ri_RelationDesc, NoLock);
	}
#endif
}

bool
synchdb_pg_before_update(EState *estate, EPQState *epq, ResultRelInfo *result,
						ItemPointer tuple_id, TupleTableSlot *new_slot)
{
#if PG_VERSION_NUM >= 180000
	return ExecBRUpdateTriggers(estate, epq, result, tuple_id, NULL,
							   new_slot, NULL, NULL, false);
#else
	return ExecBRUpdateTriggers(estate, epq, result, tuple_id, NULL,
							   new_slot, NULL, NULL);
#endif
}

bool
synchdb_pg_before_delete(EState *estate, EPQState *epq, ResultRelInfo *result,
						ItemPointer tuple_id)
{
#if PG_VERSION_NUM >= 180000
	return ExecBRDeleteTriggers(estate, epq, result, tuple_id, NULL,
							   NULL, NULL, NULL, false);
#else
	return ExecBRDeleteTriggers(estate, epq, result, tuple_id, NULL,
							   NULL, NULL, NULL);
#endif
}

void
synchdb_pg_metadata_mark_dirty(void)
{
	/* At saturation all future observations remain stale, rather than reusing
	 * a wrapped generation. Callbacks must not raise even in that case. */
	if (metadata_epoch < PG_UINT64_MAX)
		metadata_epoch++;
}

static void
metadata_relcache_callback(Datum argument, Oid relation_id)
{
	synchdb_pg_metadata_mark_dirty();
}

static void
metadata_xact_callback(XactEvent event, void *argument)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
			synchdb_pg_metadata_mark_dirty();
			break;
		default:
			break;
	}
}

static void
metadata_subxact_callback(SubXactEvent event, SubTransactionId my_subid,
						 SubTransactionId parent_subid, void *argument)
{
	if (event == SUBXACT_EVENT_ABORT_SUB || event == SUBXACT_EVENT_COMMIT_SUB)
		synchdb_pg_metadata_mark_dirty();
}

void
synchdb_pg_metadata_init(void)
{
	if (metadata_callbacks_registered)
		return;
	CacheRegisterRelcacheCallback(metadata_relcache_callback, (Datum) 0);
	RegisterXactCallback(metadata_xact_callback, NULL);
	RegisterSubXactCallback(metadata_subxact_callback, NULL);
	metadata_callbacks_registered = true;
}

uint64
synchdb_pg_metadata_epoch(void)
{
	return metadata_epoch;
}

bool
synchdb_pg_metadata_stale(uint64 observed_epoch)
{
	return metadata_epoch == PG_UINT64_MAX || observed_epoch != metadata_epoch;
}
