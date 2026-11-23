/* Source-fenced OLR transport and a target-transactional cursor. Native
 * snapshots carry a read-view proof; the FDW compatibility path does not. */
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

#define OLR_FDW_PROTOCOL "UNVERIFIED_FDW_CDC_HANDOFF_V1"
#define OLR_LEGACY_PROTOCOL "UNVERIFIED_LEGACY_OLR_CDC_V1"

static const char *
compatibility_protocol(Jsonb *proof)
{
	JsonbValue buffer;
	JsonbValue *value;

	if (!proof || !JB_ROOT_IS_OBJECT(proof))
		return NULL;
	value = getKeyJsonValueFromContainer(&proof->root, "protocol", 8, &buffer);
	if (value && value->type == jbvString)
	{
		if ((Size) value->val.string.len == strlen(OLR_FDW_PROTOCOL) &&
			memcmp(value->val.string.val, OLR_FDW_PROTOCOL, strlen(OLR_FDW_PROTOCOL)) == 0)
			return OLR_FDW_PROTOCOL;
		if ((Size) value->val.string.len == strlen(OLR_LEGACY_PROTOCOL) &&
			memcmp(value->val.string.val, OLR_LEGACY_PROTOCOL, strlen(OLR_LEGACY_PROTOCOL)) == 0)
			return OLR_LEGACY_PROTOCOL;
	}
	return NULL;
}

bool
olr_handoff_legacy_resume(void)
{
	const char *protocol = compatibility_protocol(source_proof);

	return protocol && strcmp(protocol, OLR_LEGACY_PROTOCOL) == 0;
}

char *
olr_json_text(Jsonb *object, const char *field)
{
	JsonbValue buffer;
	JsonbValue *value;

	if (object == NULL || !JB_ROOT_IS_OBJECT(object))
		elog(ERROR, "OLR field %s requires an object", field);
	value = getKeyJsonValueFromContainer(&object->root, field, strlen(field), &buffer);

	if (!value || value->type != jbvString)
		elog(ERROR, "OLR field %s must be a string", field);
	return pnstrdup(value->val.string.val, value->val.string.len);
}

uint64
olr_json_uint(Jsonb *object, const char *field)
{
	JsonbValue buffer;
	JsonbValue *value;
	char *text;
	char *end;
	unsigned long long result;

	if (object == NULL || !JB_ROOT_IS_OBJECT(object))
		elog(ERROR, "OLR field %s requires an object", field);
	value = getKeyJsonValueFromContainer(&object->root, field, strlen(field), &buffer);
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
	Jsonb *payload;

	if (!array || !JB_ROOT_IS_ARRAY(array) || JsonContainerSize(&array->root) != 1)
		elog(ERROR, "OLR handoff requires one payload per message (format.message=2)");
	payload = GET_JSONB_ELEM(record, first, 2);
	if (!payload || !JB_ROOT_IS_OBJECT(payload))
		elog(ERROR, "OLR payload must be an object");
	return payload;
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
	const char *protocol = compatibility_protocol(source_proof);

	initStringInfo(&request);
	appendStringInfo(&request, "{\"port\":%u", connection->port);
	for (index = 0; index < lengthof(names); index++)
	{
		appendStringInfo(&request, ",\"%s\":", names[index]);
		escape_json(&request, values[index]);
	}
	if (source_proof)
		appendStringInfo(&request, ",\"previous\":%s", JsonbToCString(NULL, &source_proof->root, VARSIZE(source_proof)));
	if (olr_handoff_legacy_resume())
		appendStringInfo(&request, ",\"resume_scn\":\"%llu\",\"resume_c_scn\":\"%llu\",\"resume_c_idx\":\"%llu\"",
			(unsigned long long) olr_client_get_scn(),
			(unsigned long long) olr_client_get_c_scn(),
			(unsigned long long) olr_client_get_c_idx());
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
	if (protocol)
	{
		char *qualification = psprintf("{\"protocol\":\"%s\","
			"\"handoff_verified\":false,\"cdc_verified\":false}", protocol);
		Jsonb *marker = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,
			CStringGetDatum(qualification)));
		Jsonb *combined = DatumGetJsonbP(DirectFunctionCall2(jsonb_concat,
			JsonbPGetDatum(proof), JsonbPGetDatum(marker)));

		/* A new source fence cannot verify an old FDW read or legacy baseline.
		 * Preserve that qualification across every restart/reconnect. */
		pfree(qualification);
		pfree(proof);
		pfree(marker);
		proof = combined;
	}
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
		if (!snapshot_scn && !olr_handoff_legacy_resume())
			elog(ERROR, "OLR snapshot did not complete; explicit target/artifact recovery is required");
		for (int index = 0; index < 3; index++)
			values[index] = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3 + index, &isnull);
		olr_client_set_scns(DatumGetInt64(values[0]), DatumGetInt64(values[1]), DatumGetInt64(values[2]));
	}
	/* no_data can mean continuing an imported old baseline, including its
	 * original table subset. Only that committed legacy protocol is exempt
	 * from the full-schema rule for a new native handoff. Check the actual
	 * generation here rather than querying catalogs in pure run validation. */
	if (connection->snapengine == ENGINE_DEBEZIUM && !(resume && olr_handoff_legacy_resume()) &&
		((strcmp(connection->table, "null") != 0 && connection->table[0] != '\0') ||
		 (strcmp(connection->snapshottable, "null") != 0 && connection->snapshottable[0] != '\0')))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("verified native OLR handoff currently requires the complete configured schema")));
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
	if (resume && olr_handoff_legacy_resume())
	{
		Jsonb *verified = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,
			CStringGetDatum("{\"identity_verified\":true,\"legacy_source_identity_verified\":false}")));
		Jsonb *combined;

		/* Bind the freshly verified Oracle identity before applying any CDC.
		 * It proves the source seen now, not the source behind an old file. */
		caller = MemoryContextSwitchTo(TopMemoryContext);
		combined = DatumGetJsonbP(DirectFunctionCall2(jsonb_concat,
			JsonbPGetDatum(source_proof), JsonbPGetDatum(verified)));
		pfree(source_proof);
		source_proof = combined;
		MemoryContextSwitchTo(caller);
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		store_state(false);
		PopActiveSnapshot();
		CommitTransactionCommand();
		elog(LOG, "OLR resumed imported legacy cursor %llu/%llu; legacy baseline consistency remains unverified",
			(unsigned long long) olr_client_get_c_scn(),
			(unsigned long long) olr_client_get_c_idx());
	}
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
	if (!active || (!snapshot_scn && !olr_handoff_legacy_resume()))
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

void
olr_handoff_complete_fdw(const ConnectionInfo *connection, Jsonb *material)
{
	Datum offset_path[] = {CStringGetTextDatum("offset")};
	Jsonb *offset;
	uint64 candidate;
	char *boundary_json;
	Jsonb *boundary;
	Jsonb *combined;
	MemoryContext caller;

	if (!connection || !active || !source_proof || !material || !JB_ROOT_IS_OBJECT(material))
		elog(ERROR, "OLR FDW compatibility handoff requires prepared source evidence");
	if (connection->key.database_oid != key.database_oid ||
		memcmp(&connection->key.connector_id, &key.connector_id, sizeof(pg_uuid_t)) != 0)
		elog(ERROR, "OLR FDW compatibility handoff connector identity changed");
	offset = GET_JSONB_ELEM(material, offset_path, 1);
	if (!offset || !JB_ROOT_IS_OBJECT(offset))
		elog(ERROR, "OLR FDW compatibility handoff requires a source SCN offset");
	candidate = olr_json_uint(offset, "scn");
	if (!candidate || candidate < olr_json_uint(source_proof, "fence_scn_upper_bound"))
		elog(ERROR, "OLR FDW compatibility boundary precedes its source fence");

	/* TODO: Bind FDW metadata/data reads to the OLR replay boundary, including
	 * transactions crossing the read SCN. The captured candidate only enables
	 * legacy CDC continuation; it is not a native snapshot witness. */
	boundary_json = psprintf("{\"protocol\":\"" OLR_FDW_PROTOCOL "\","
		"\"handoff_verified\":false,\"cdc_verified\":false,"
		"\"snapshot_pending_xids\":{},\"scn\":\"%llu\"}",
		(unsigned long long) candidate);
	boundary = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(boundary_json)));
	caller = MemoryContextSwitchTo(TopMemoryContext);
	combined = DatumGetJsonbP(DirectFunctionCall2(jsonb_concat,
		JsonbPGetDatum(source_proof), JsonbPGetDatum(boundary)));
	pfree(source_proof);
	source_proof = combined;
	snapshot_scn = candidate;
	MemoryContextSwitchTo(caller);
	pfree(boundary);
	pfree(boundary_json);
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	store_state(false);
	PopActiveSnapshot();
	CommitTransactionCommand();
	elog(LOG, "OLR FDW compatibility handoff committed at SCN %llu (snapshot-to-CDC consistency unverified)",
		 (unsigned long long) snapshot_scn);
}

bool
olr_handoff_active(void)
{
	return active;
}

void
olr_handoff_store_position(void)
{
	if (!active || (!snapshot_scn && !olr_handoff_legacy_resume()))
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

	if (!active || (!snapshot_scn && !olr_handoff_legacy_resume()))
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
		/* TODO: legacy snapshots did not establish the native read-view proof.
		 * Continue strictly after the imported OLR coordinate; using its Oracle
		 * SCN as a snapshot cutoff could discard later records at the same SCN. */
		apply = (olr_handoff_legacy_resume() || scn > snapshot_scn || pending) &&
			strcmp(owner, olr_json_text(source_proof, "owner")) == 0 &&
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
