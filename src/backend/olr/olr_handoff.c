/* A source-proven snapshot boundary and a target-transactional OLR cursor. */
#include "postgres.h"
#include "access/xact.h"
#include "catalog/catalog_access.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "olr/olr_client.h"
#include "olr/olr_handoff.h"
#include "runtime/java_operations.h"
#include "snapshot/artifact_store.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonfuncs.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

static SynchdbConnectorKey key;
static SynchdbArtifactPaths paths;
static Jsonb *source_proof;
static uint64 snapshot_scn;
static bool active;

char *
olr_json_text(Jsonb *object, const char *field)
{
	JsonbValue buffer;
	JsonbValue *value = getKeyJsonValueFromContainer(&object->root, field, strlen(field), &buffer);

	if (!value || value->type != jbvString)
		elog(ERROR, "OLR field %s must be a string", field);
	return pnstrdup(value->val.string.val, value->val.string.len);
}

uint64
olr_json_uint(Jsonb *object, const char *field)
{
	JsonbValue buffer;
	JsonbValue *value = getKeyJsonValueFromContainer(&object->root, field, strlen(field), &buffer);
	char *text;
	char *end;
	unsigned long long result;

	if (!value || (value->type != jbvNumeric && value->type != jbvString))
		elog(ERROR, "OLR field %s must be an unsigned integer", field);
	text = value->type == jbvString ? pnstrdup(value->val.string.val, value->val.string.len) :
		DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(value->val.numeric)));
	errno = 0;
	result = strtoull(text, &end, 10);
	if (errno || !text[0] || text[0] == '-' || *end || result > PG_INT64_MAX)
		elog(ERROR, "OLR field %s is outside the supported integer range", field);
	pfree(text);
	return (uint64) result;
}

Jsonb *
olr_json_payload(Jsonb *record)
{
	Datum path[] = {CStringGetTextDatum("payload")};
	Jsonb *array = GET_JSONB_ELEM(record, path, 1);
	Datum first[] = {CStringGetTextDatum("payload"), CStringGetTextDatum("0")};

	if (!array || !JB_ROOT_IS_ARRAY(array) || JsonContainerSize(&array->root) != 1)
		elog(ERROR, "OLR handoff requires one payload per message (format.message=2)");
	return GET_JSONB_ELEM(record, first, 2);
}

static void
key_values(Datum values[8])
{
	values[0] = ObjectIdGetDatum(key.database_oid);
	values[1] = UUIDPGetDatum(&key.connector_id);
	values[2] = UUIDPGetDatum(&paths.generation);
}

static void
store_state(bool create)
{
	Oid types[] = {OIDOID, UUIDOID, UUIDOID, JSONBOID, INT8OID, INT8OID, INT8OID, INT8OID};
	Datum values[8];
	char *catalog;
	char *query;
	int level;

	key_values(values);
	values[3] = JsonbPGetDatum(source_proof);
	values[4] = Int64GetDatum(snapshot_scn);
	values[5] = Int64GetDatum(olr_client_get_scn());
	values[6] = Int64GetDatum(olr_client_get_c_scn());
	values[7] = Int64GetDatum(olr_client_get_c_idx());
	synchdb_store_connect();
	level = synchdb_store_begin_path();
	catalog = synchdb_store_relation("synchdb_olr_positions", RowExclusiveLock);
	query = create ? psprintf("INSERT INTO %s (database_oid,connector_id,generation,source_proof,snapshot_scn,scn,c_scn,c_idx) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", catalog) :
		psprintf("UPDATE %s SET source_proof=$4,snapshot_scn=$5,scn=$6,c_scn=$7,c_idx=$8 WHERE database_oid=$1 AND connector_id=$2 AND generation=$3", catalog);
	if (SPI_execute_with_args(query, 8, types, values, NULL, false, 0) !=
		(create ? SPI_OK_INSERT : SPI_OK_UPDATE) || SPI_processed != 1)
		elog(ERROR, "could not persist OLR handoff position");
	synchdb_store_end_path(level);
	synchdb_store_finish();
	/* An ACK cannot outlive WAL durability even with synchronous_commit=off. */
	ForceSyncCommit();
}

static Jsonb *
preflight(const ConnectionInfo *connection, SynchdbJavaRuntime *java)
{
	StringInfoData request;
	const char *names[] = {"hostname", "user", "password", "database", "schema", "ssl_mode"};
	const char *values[] = {connection->hostname, connection->user, connection->pwd,
		connection->srcdb, connection->srcschema, connection->extra.ssl_mode};
	char *answer;
	Jsonb *proof;
	MemoryContext caller;
	int index;

	initStringInfo(&request);
	appendStringInfo(&request, "{\"port\":%u", connection->port);
	for (index = 0; index < lengthof(names); index++)
	{
		appendStringInfo(&request, ",\"%s\":", names[index]);
		escape_json(&request, values[index]);
	}
	if (source_proof)
		appendStringInfo(&request, ",\"previous\":%s", JsonbToCString(NULL, &source_proof->root, VARSIZE(source_proof)));
	appendStringInfoChar(&request, '}');
	PG_TRY();
	{
		answer = synchdb_java_string_operation(java, "prepareOlrHandoff", request.data);
	}
	PG_FINALLY();
	{
		/* The request contains credentials. Never include it in diagnostics. */
		explicit_bzero(request.data, request.len);
		pfree(request.data);
	}
	PG_END_TRY();
	caller = MemoryContextSwitchTo(TopMemoryContext);
	proof = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(answer)));
	MemoryContextSwitchTo(caller);
	pfree(answer);
	if (source_proof)
		pfree(source_proof);
	return proof;
}

bool
olr_handoff_prepare(const ConnectionInfo *connection, SynchdbJavaRuntime *java)
{
	Oid types[] = {OIDOID, UUIDOID, UUIDOID};
	Datum values[8];
	bool resume;
	bool isnull;
	char *catalog;
	char *query;
	int level;
	MemoryContext caller;

	key = connection->key;
	synchdb_artifact_store_prepare(connection, TYPE_OLR, &paths);
	caller = MemoryContextSwitchTo(TopMemoryContext);
	java->prepared_artifacts = palloc(sizeof(paths));
	*java->prepared_artifacts = paths;
	MemoryContextSwitchTo(caller);
	resume = paths.resume_existing;
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	synchdb_store_connect();
	level = synchdb_store_begin_path();
	catalog = synchdb_store_relation("synchdb_olr_positions", AccessShareLock);
	key_values(values);
	query = psprintf("SELECT source_proof,snapshot_scn,scn,c_scn,c_idx FROM %s WHERE database_oid=$1 AND connector_id=$2 AND generation=$3", catalog);
	if (SPI_execute_with_args(query, 3, types, values, NULL, true, 2) != SPI_OK_SELECT || SPI_processed > 1)
		elog(ERROR, "invalid OLR handoff catalog result");
	if (resume)
	{
		if (SPI_processed != 1)
			elog(ERROR, "existing OLR artifacts have no verified handoff; explicit target/artifact recovery is required");
		caller = MemoryContextSwitchTo(TopMemoryContext);
		source_proof = DatumGetJsonbPCopy(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
		MemoryContextSwitchTo(caller);
		snapshot_scn = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
		if (!snapshot_scn)
			elog(ERROR, "OLR snapshot did not complete; explicit target/artifact recovery is required");
		for (int index = 0; index < 3; index++)
			values[index] = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3 + index, &isnull);
		olr_client_set_scns(DatumGetInt64(values[0]), DatumGetInt64(values[1]), DatumGetInt64(values[2]));
	}
	synchdb_store_end_path(level);
	synchdb_store_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	source_proof = preflight(connection, java);
	if (!resume)
	{
		uint64 lower = olr_json_uint(source_proof, "start_scn");

		olr_client_set_scns(lower, 0, 0);
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		store_state(true);
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	active = true;
	olr_client_verified_connect(connection, source_proof, resume);
	if (!resume)
	{
		char *position = psprintf("{\"olr_first_data_scn\":\"%llu\"}",
			(unsigned long long) olr_client_get_source_boundary());
		Jsonb *data = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(position)));
		caller = MemoryContextSwitchTo(TopMemoryContext);

		source_proof = DatumGetJsonbP(DirectFunctionCall2(jsonb_concat, JsonbPGetDatum(source_proof), JsonbPGetDatum(data)));
		MemoryContextSwitchTo(caller);
	}
	return resume;
}

void
olr_handoff_reconnect(const ConnectionInfo *connection, SynchdbJavaRuntime *java)
{
	if (!active || !snapshot_scn)
		elog(ERROR, "OLR reconnect requires a completed handoff");
	source_proof = preflight(connection, java);
	olr_client_verified_connect(connection, source_proof, true);
}

void
olr_handoff_complete(const ConnectionInfo *connection, SynchdbJavaRuntime *java)
{
	char *offset = synchdb_java_get_offset(java, TYPE_OLR, connection->srcdb,
		connection->name, connection->dstdb);
	Jsonb *state;
	JsonbValue buffer;
	JsonbValue *completed;
	char *boundary_json;
	Jsonb *boundary;
	Jsonb *combined;
	MemoryContext caller;

	if (!active || !offset)
		elog(ERROR, "OLR handoff requires a completed native snapshot offset");
	state = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(offset)));
	completed = getKeyJsonValueFromContainer(&state->root, "snapshot", 8, &buffer);
	if (completed && completed->type != jbvNull &&
		!(completed->type == jbvBool && !completed->val.boolean) &&
		!(completed->type == jbvString && completed->val.string.len == 5 &&
		  memcmp(completed->val.string.val, "false", 5) == 0))
		elog(ERROR, "OLR snapshot offset is still marked as in progress");
	boundary_json = synchdb_java_string_operation(java, "getOlrSnapshotBoundary", "{}");
	boundary = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(boundary_json)));
	snapshot_scn = olr_json_uint(boundary, "scn");
	if (snapshot_scn != olr_json_uint(state, "scn"))
		elog(ERROR, "completed OLR snapshot offset disagrees with its native read-view witness");
	caller = MemoryContextSwitchTo(TopMemoryContext);
	combined = DatumGetJsonbP(DirectFunctionCall2(jsonb_concat, JsonbPGetDatum(source_proof), JsonbPGetDatum(boundary)));
	pfree(source_proof);
	source_proof = combined;
	MemoryContextSwitchTo(caller);
	if (snapshot_scn < olr_json_uint(source_proof, "fence_scn_upper_bound"))
		elog(ERROR, "OLR snapshot boundary precedes its verified source fence");
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	store_state(false);
	PopActiveSnapshot();
	CommitTransactionCommand();
	elog(LOG, "OLR snapshot handoff committed at SCN %llu", (unsigned long long) snapshot_scn);
}

bool
olr_handoff_active(void)
{
	return active;
}

void
olr_handoff_store_position(void)
{
	if (!active || !snapshot_scn)
		elog(ERROR, "OLR CDC cannot persist a cursor before its snapshot handoff");
	store_state(false);
}

bool
olr_handoff_apply_record(Jsonb *record)
{
	Jsonb *payload = olr_json_payload(record);
	char *op = olr_json_text(payload, "op");
	uint64 scn = olr_json_uint(record, "scn");
	uint64 c_scn = olr_json_uint(record, "c_scn");
	uint64 c_idx = olr_json_uint(record, "c_idx");
	bool apply = false;
	bool pending = false;
	Datum pending_path[] = {CStringGetTextDatum("snapshot_pending_xids")};
	Jsonb *pending_xids = GET_JSONB_ELEM(source_proof, pending_path, 1);
	elog(DEBUG2, "OLR boundary: op %s scn %llu baseline %llu cursor %llu/%llu previous %llu/%llu",
		op, (unsigned long long) scn, (unsigned long long) snapshot_scn,
		(unsigned long long) c_scn, (unsigned long long) c_idx,
		(unsigned long long) olr_client_get_c_scn(), (unsigned long long) olr_client_get_c_idx());

	if (!active || !snapshot_scn)
		elog(ERROR, "OLR change received without a completed snapshot handoff");
	if (c_scn < olr_client_get_c_scn() ||
		(c_scn == olr_client_get_c_scn() && c_idx <= olr_client_get_c_idx()))
		return false;
	if (strcmp(op, "c") == 0 || strcmp(op, "u") == 0 ||
		strcmp(op, "d") == 0 || strcmp(op, "ddl") == 0)
	{
		Datum path[] = {CStringGetTextDatum("schema")};
		Jsonb *schema = GET_JSONB_ELEM(payload, path, 1);
		char *owner;
		char *table;

		if (!schema)
			elog(ERROR, "OLR captured change has no schema");
		owner = olr_json_text(schema, "owner");
		table = olr_json_text(schema, "table");
		if (pending_xids)
			pending = DatumGetBool(DirectFunctionCall2(jsonb_exists, JsonbPGetDatum(pending_xids),
				CStringGetTextDatum(olr_json_text(record, "xid"))));
		apply = (scn > snapshot_scn || pending) && strcmp(owner, olr_json_text(source_proof, "owner")) == 0 &&
			strcmp(table, "SYNCHDB_OLR_FENCE") != 0 && strncmp(table, "LOG_MINING", 10) != 0;
	}
	else if (!strcmp(op, "begin") || !strcmp(op, "commit"))
		apply = true; /* Preserve transaction counts and batch timestamps. */
	else if (strcmp(op, "chkpt"))
		elog(ERROR, "unsupported OLR operation %s", op);
	/* Even excluded and already snapshotted records advance the real OLR
	 * coordinate. A new LWN is allowed to reset c_idx to zero. */
	olr_client_set_scns(scn, c_scn, c_idx);
	return apply;
}
