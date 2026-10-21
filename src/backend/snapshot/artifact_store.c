/* Exact, persistent local engine artifact ownership. No FDW seed is created. */
#include "postgres.h"
#include "fmgr.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "catalog/catalog_access.h"
#include "catalog/connector_store.h"
#include "catalog/pg_authid_d.h"
#include "catalog/pg_type_d.h"
#include "common/file_perm.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "runtime/run_registry.h"
#include "runtime/controller.h"
#include "runtime/config.h"
#include "snapshot/artifact_store.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

PG_FUNCTION_INFO_V1(synchdb_artifact_manifest);
PG_FUNCTION_INFO_V1(synchdb_artifact_reset);
PG_FUNCTION_INFO_V1(synchdb_artifact_request_purge);
PG_FUNCTION_INFO_V1(synchdb_artifact_migrate_legacy);

static void artifact_copy(const char *source, const char *destination);

typedef struct ArtifactCreatedFile
{
	char *path;
	SubTransactionId subtransaction;
	struct ArtifactCreatedFile *next;
} ArtifactCreatedFile;

typedef struct ArtifactPurgeAttempt
{
	SynchdbConnectorKey key;
	pg_uuid_t generation;
	bool intent_committed;
} ArtifactPurgeAttempt;

static ArtifactCreatedFile *artifact_created_files;
static bool artifact_callbacks_registered;

static void
artifact_discard_created(ArtifactCreatedFile *file)
{
	/* Only O_EXCL-created, unpublished files enter this list. Abort cleanup
	 * never traverses a directory or removes an existing engine generation. */
	if (unlink(file->path) < 0 && errno != ENOENT)
		elog(WARNING, "could not remove unpublished artifact \"%s\": %m", file->path);
}

static void
artifact_files_xact(XactEvent event, void *argument)
{
	ArtifactCreatedFile *file;

	if (event == XACT_EVENT_PRE_PREPARE && artifact_created_files)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("artifact publication does not support prepared transactions")));
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		for (file = artifact_created_files; file; file = file->next)
			artifact_discard_created(file);
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT ||
		event == XACT_EVENT_COMMIT || event == XACT_EVENT_PARALLEL_COMMIT)
		artifact_created_files = NULL; /* TopTransactionContext releases the list. */
}

static void
artifact_files_subxact(SubXactEvent event, SubTransactionId current,
					  SubTransactionId parent, void *argument)
{
	ArtifactCreatedFile **link = &artifact_created_files;

	if (event != SUBXACT_EVENT_ABORT_SUB && event != SUBXACT_EVENT_COMMIT_SUB)
		return;
	while (*link)
	{
		ArtifactCreatedFile *file = *link;

		if (file->subtransaction == current)
		{
			if (event == SUBXACT_EVENT_ABORT_SUB)
			{
				artifact_discard_created(file);
				*link = file->next;
				continue;
			}
			file->subtransaction = parent;
		}
		link = &file->next;
	}
}

static ArtifactCreatedFile *
artifact_track_created(const char *path)
{
	ArtifactCreatedFile *file;
	MemoryContext caller;

	if (!artifact_callbacks_registered)
	{
		RegisterXactCallback(artifact_files_xact, NULL);
		RegisterSubXactCallback(artifact_files_subxact, NULL);
		artifact_callbacks_registered = true;
	}
	caller = MemoryContextSwitchTo(TopTransactionContext);
	file = palloc0(sizeof(*file));
	file->path = pstrdup(path);
	file->subtransaction = GetCurrentSubTransactionId();
	MemoryContextSwitchTo(caller);
	return file;
}

static void
artifact_admin(void)
{
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("artifact maintenance requires superuser")));
}

static void
artifact_key(const SynchdbConnectorKey *key)
{
	if (!synchdb_connector_key_is_valid(key) || key->database_oid != MyDatabaseId)
		elog(ERROR, "invalid artifact connector identity");
}

void
synchdb_artifact_paths(const SynchdbConnectorKey *key,
					   const pg_uuid_t *generation, SynchdbArtifactPaths *out)
{
	char *connector;
	char *version;
	pg_uuid_t accepted;
	static const pg_uuid_t zero = {{0}};

	if (!synchdb_connector_key_is_valid(key) || !generation || !out ||
		memcmp(generation, &zero, sizeof(zero)) == 0)
		elog(ERROR, "invalid artifact path identity");
	accepted = *generation;
	memset(out, 0, sizeof(*out));
	out->generation = accepted;
	connector = DatumGetCString(DirectFunctionCall1(uuid_out, UUIDPGetDatum(&key->connector_id)));
	version = DatumGetCString(DirectFunctionCall1(uuid_out, UUIDPGetDatum(&accepted)));
	snprintf(out->base, sizeof(out->base), "pg_synchdb/%u_%s_%s", key->database_oid, connector, version);
	snprintf(out->offset, sizeof(out->offset), "%s_offsets.dat", out->base);
	snprintf(out->history, sizeof(out->history), "%s_schemahistory.dat", out->base);
	snprintf(out->signal, sizeof(out->signal), "%s_signal.dat", out->base);
	snprintf(out->cache, sizeof(out->cache), "%s_ispn", out->base);
	snprintf(out->table_filter, sizeof(out->table_filter), "%s_tables.json", out->base);
	snprintf(out->snapshot_filter, sizeof(out->snapshot_filter), "%s_snapshot_tables.json", out->base);
	pfree(connector);
	pfree(version);
}

static void
artifact_directory(void)
{
	struct stat st;

	if (lstat("pg_synchdb", &st) < 0)
	{
		if (errno != ENOENT || MakePGDirectory("pg_synchdb") < 0)
			ereport(ERROR, (errcode_for_file_access(), errmsg("could not create artifact directory: %m")));
		fsync_fname(".", true);
		if (lstat("pg_synchdb", &st) < 0)
			ereport(ERROR, (errcode_for_file_access(), errmsg("could not inspect artifact directory: %m")));
	}
	if (!S_ISDIR(st.st_mode) || st.st_uid != geteuid() || (st.st_mode & 0022))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("artifact directory must be a server-owned directory without group/world write access")));
}

static bool
artifact_regular(const char *path, bool required)
{
	struct stat st;

	if (lstat(path, &st) < 0)
	{
		if (errno == ENOENT && !required)
			return false;
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not inspect engine artifact \"%s\": %m", path),
						errdetail("A previously engine-owned generation is never reset automatically.")));
	}
	if (!S_ISREG(st.st_mode) || st.st_nlink != 1 || st.st_uid != geteuid() ||
		(st.st_mode & 0022) || st.st_size > 64 * 1024 * 1024 || (required && st.st_size == 0))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("invalid engine artifact \"%s\"", path)));
	return true;
}

static void
artifact_require_fresh(const SynchdbArtifactPaths *paths)
{
	const char *files[] = {paths->offset, paths->history, paths->signal,
		paths->cache, paths->table_filter, paths->snapshot_filter};
	int index;

	for (index = 0; index < lengthof(files); index++)
	{
		struct stat st;

		if (lstat(files[index], &st) == 0)
			ereport(ERROR, (errcode(ERRCODE_DUPLICATE_FILE),
							errmsg("new artifact generation already has a filesystem object at \"%s\"", files[index])));
		if (errno != ENOENT)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not inspect new artifact generation path \"%s\": %m", files[index])));
	}
}

static void
artifact_stopped(const SynchdbConnectorKey *key)
{
	SynchdbSharedState *state = synchdb_registry_try_attach(synchdb_max_connector_workers);
	SynchdbRunHandle run;

	if (state && synchdb_registry_find_run(state, key, &run) &&
		synchdb_registry_run_active(state, &run))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
						errmsg("artifact maintenance requires a fully stopped connector")));
}

/* The connector row is shared with the committed controller's start fence.
 * A table lock also serializes maintenance after configuration deletion. */
static char *
artifact_catalog(const SynchdbConnectorKey *key, bool maintenance)
{
	char *catalog;
	char *connectors;
	char *query;
	Oid types[] = {UUIDOID};
	Datum values[] = {UUIDPGetDatum(&key->connector_id)};

	artifact_key(key);
	synchdb_store_connect();
	connectors = synchdb_store_relation("synchdb_conninfo", RowShareLock);
	query = psprintf("SELECT connector_id FROM %s WHERE connector_id OPERATOR(pg_catalog.=) $1 FOR UPDATE", connectors);
	if (SPI_execute_with_args(query, 1, types, values, NULL, false, 1) != SPI_OK_SELECT)
		elog(ERROR, "could not lock artifact connector");
	if (!maintenance && SPI_processed != 1)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("artifact connector configuration no longer exists")));
	if (maintenance)
		synchdb_control_check_delete(key);
	catalog = synchdb_store_relation("synchdb_artifacts", ShareRowExclusiveLock);
	if (maintenance)
		artifact_stopped(key);
	return catalog;
}

static void
artifact_require_revision(const ConnectionInfo *connection)
{
	Oid type = UUIDOID;
	Datum key = UUIDPGetDatum(&connection->key.connector_id);
	Datum revision;
	bool isnull;
	char *query = psprintf("SELECT config_revision FROM %s WHERE connector_id OPERATOR(pg_catalog.=) $1",
		synchdb_store_relation("synchdb_conninfo", AccessShareLock));

	if (SPI_execute_with_args(query, 1, &type, &key, NULL, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("accepted artifact configuration no longer exists")));
	revision = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull || DatumGetInt64(revision) != connection->config_revision)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("accepted artifact configuration revision is obsolete")));
}

static char *
artifact_selection_fingerprint(const char *path)
{
	int descriptor = OpenTransientFile(path, O_RDONLY | PG_BINARY | O_NOFOLLOW);
	struct stat st;
	StringInfoData contents;
	char buffer[8192];
	ssize_t count;

	if (descriptor < 0 || fstat(descriptor, &st) < 0 || !S_ISREG(st.st_mode))
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not read source selection file: %m")));
	initStringInfo(&contents);
	while ((count = read(descriptor, buffer, sizeof(buffer))) != 0)
	{
		CHECK_FOR_INTERRUPTS();
		if (count < 0)
		{
			if (errno == EINTR)
				continue;
			ereport(ERROR, (errcode_for_file_access(), errmsg("could not read source selection file: %m")));
		}
		if (contents.len + count > 64 * 1024 * 1024)
			ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("source selection file exceeds 64 MiB")));
		appendBinaryStringInfo(&contents, buffer, count);
	}
	if (CloseTransientFile(descriptor) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not close source selection file: %m")));
	return TextDatumGetCString(DirectFunctionCall2(binary_encode,
		DirectFunctionCall1(sha256_bytea,
			PointerGetDatum(cstring_to_text_with_len(contents.data, contents.len))),
		CStringGetTextDatum("hex")));
}

static Jsonb *
artifact_identity(const ConnectionInfo *connection, ConnectorType type)
{
	StringInfoData json;
	const char *names[] = {"hostname", "database", "schema", "tables", "snapshot_tables", "olr_host", "olr_source", "ispn_type", "source_user", "legacy_slot_alias", "legacy_slot_database"};
	const char *values[] = {connection->hostname, connection->srcdb, connection->srcschema,
		connection->table, connection->snapshottable, connection->olr.olr_host,
		connection->olr.olr_source, connection->ispn.ispn_cache_type, connection->user,
		connection->name, connection->dstdb};
	int index;

	initStringInfo(&json);
	appendStringInfo(&json, "{\"type\":%d,\"port\":%u,\"olr_port\":%u", (int) type,
		connection->port, connection->olr.olr_port);
	for (index = 0; index < lengthof(names); index++)
	{
		appendStringInfo(&json, ",\"%s\":", names[index]);
		escape_json(&json, values[index]);
	}
	for (index = 3; index <= 4; index++)
	{
		if (strncmp(values[index], "file:", 5) == 0)
		{
			char *digest;

			/* This is an external, caller-selected server path. Internal UUID
			 * copies are checked independently and do not grant file access. */
			if (!superuser() &&
				!has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("source selection files require pg_read_server_files")));
			digest = artifact_selection_fingerprint(values[index] + 5);

			appendStringInfo(&json, ",\"%s_file_fingerprint\":", names[index]);
			escape_json(&json, digest);
		}
	}
	appendStringInfoChar(&json, '}');
	return DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(json.data)));
}

static void
artifact_selections(const ConnectionInfo *connection, const SynchdbArtifactPaths *paths,
					Jsonb *identity, bool copy)
{
	const char *settings[] = {connection->table, connection->snapshottable};
	const char *destinations[] = {paths->table_filter, paths->snapshot_filter};
	const char *members[] = {"tables_file_fingerprint", "snapshot_tables_file_fingerprint"};
	int index;

	for (index = 0; index < lengthof(settings); index++)
	{
		JsonbValue value;
		JsonbValue *expected;
		char *actual;

		if (strncmp(settings[index], "file:", 5) != 0)
			continue;
		if (copy)
			artifact_copy(settings[index] + 5, destinations[index]);
		artifact_regular(destinations[index], true);
		actual = artifact_selection_fingerprint(destinations[index]);
		expected = getKeyJsonValueFromContainer(&identity->root, members[index], strlen(members[index]), &value);
		if (!expected || expected->type != jbvString || expected->val.string.len != strlen(actual) ||
			memcmp(expected->val.string.val, actual, strlen(actual)) != 0)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("frozen source selection file does not match its artifact generation")));
	}
}

static const char *
artifact_legacy_prefix(ConnectorType type)
{
	switch (type)
	{
		case TYPE_MYSQL: return "mysql";
		case TYPE_ORACLE: return "oracle";
		case TYPE_SQLSERVER: return "sqlserver";
		case TYPE_POSTGRES: return "postgres";
		case TYPE_OLR: return "openlog_replicator_d";
		default: elog(ERROR, "unsupported artifact connector type");
	}
	return NULL;
}

static char *
artifact_legacy_path(const ConnectionInfo *connection, ConnectorType type, const char *suffix)
{
	/* Legacy aliases could contain path separators. Never interpret them as a
	 * filesystem path; an operator must move those files explicitly while offline. */
	if (strchr(connection->name, '/') || strchr(connection->dstdb, '/'))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("legacy artifact alias contains a path separator")));
	return psprintf("pg_synchdb/%s_%s_%s_%s.dat", artifact_legacy_prefix(type),
		connection->name, connection->dstdb, suffix);
}

static void
artifact_new_generation(pg_uuid_t *generation)
{
	if (!pg_strong_random(generation->data, UUID_LEN))
		elog(ERROR, "could not generate artifact generation");
	generation->data[6] = (generation->data[6] & 0x0f) | 0x40;
	generation->data[8] = (generation->data[8] & 0x3f) | 0x80;
}

static void
artifact_insert(const char *catalog, const ConnectionInfo *connection,
				const SynchdbArtifactPaths *paths, Jsonb *identity, bool legacy)
{
	Oid types[] = {OIDOID, UUIDOID, UUIDOID, INT8OID, JSONBOID, TEXTOID};
	Datum values[] = {ObjectIdGetDatum(MyDatabaseId), UUIDPGetDatum(&connection->key.connector_id),
		UUIDPGetDatum(&paths->generation), Int64GetDatum(connection->config_revision),
		JsonbPGetDatum(identity), CStringGetTextDatum(legacy ? "LEGACY_COPY" : "NATIVE")};
	char *query = psprintf("INSERT INTO %s (database_oid,connector_id,generation,config_revision,source_identity,phase,active,origin) "
		"VALUES ($1,$2,$3,$4,$5,'ENGINE_OWNED',true,$6)", catalog);

	if (SPI_execute_with_args(query, 6, types, values, NULL, false, 0) != SPI_OK_INSERT || SPI_processed != 1)
		elog(ERROR, "could not persist engine artifact ownership");
}

static void
artifact_resume_metadata(SynchdbArtifactPaths *paths, HeapTuple tuple,
						TupleDesc descriptor, int origin_column, int identity_column)
{
	char *origin = SPI_getvalue(tuple, descriptor, origin_column);
	Datum identity_value;
	Jsonb *identity;
	JsonbValue value;
	JsonbValue *kind;
	JsonbValue *alias;
	JsonbValue *database;
	char *alias_text;
	char *database_text;
	bool isnull;
	int length;

	paths->resume_existing = true;
	if (origin && strcmp(origin, "NATIVE") == 0)
		return;
	if (!origin || strcmp(origin, "LEGACY_COPY") != 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid engine artifact origin")));
	identity_value = SPI_getbinval(tuple, descriptor, identity_column, &isnull);
	if (isnull)
		elog(ERROR, "engine artifact source identity is NULL");
	identity = DatumGetJsonbP(identity_value);
	kind = getKeyJsonValueFromContainer(&identity->root, "type", 4, &value);
	if (!kind || kind->type != jbvNumeric)
		elog(ERROR, "engine artifact source type is missing");
	if (DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(kind->val.numeric))) != TYPE_POSTGRES)
		return;
	alias = getKeyJsonValueFromContainer(&identity->root, "legacy_slot_alias", 17, &value);
	if (!alias || alias->type != jbvString)
		elog(ERROR, "legacy artifact slot alias is missing");
	alias_text = pnstrdup(alias->val.string.val, alias->val.string.len);
	database = getKeyJsonValueFromContainer(&identity->root, "legacy_slot_database", 20, &value);
	if (!database || database->type != jbvString)
		elog(ERROR, "legacy artifact destination database is missing");
	database_text = pnstrdup(database->val.string.val, database->val.string.len);
	/* Names are recovered from the committed migration record, never from a
	 * mutable alias. Truncating would silently select a different source object. */
	length = snprintf(paths->postgres_legacy_slot, sizeof(paths->postgres_legacy_slot),
		"%s_%s_synchdb_slot", alias_text, database_text);
	if (length < 0 || length >= sizeof(paths->postgres_legacy_slot))
		ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("legacy PostgreSQL slot identity exceeds 63 bytes")));
	length = snprintf(paths->postgres_legacy_publication, sizeof(paths->postgres_legacy_publication),
		"%s_%s_synchdb_pub", alias_text, database_text);
	if (length < 0 || length >= sizeof(paths->postgres_legacy_publication))
		ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("legacy PostgreSQL publication identity exceeds 63 bytes")));
}

bool
synchdb_artifact_store_lookup(const SynchdbConnectorKey *key, SynchdbArtifactPaths *out)
{
	Oid types[] = {OIDOID, UUIDOID};
	Datum values[] = {ObjectIdGetDatum(MyDatabaseId), UUIDPGetDatum(&key->connector_id)};
	char *catalog;
	char *query;
	bool found = false;
	bool isnull;

	artifact_key(key);
	synchdb_store_connect();
	catalog = synchdb_store_relation("synchdb_artifacts", AccessShareLock);
	query = psprintf("SELECT generation,origin,source_identity FROM %s WHERE database_oid OPERATOR(pg_catalog.=) $1 "
		"AND connector_id OPERATOR(pg_catalog.=) $2 AND active AND phase OPERATOR(pg_catalog.=) 'ENGINE_OWNED'", catalog);
	if (SPI_execute_with_args(query, 2, types, values, NULL, true, 2) != SPI_OK_SELECT || SPI_processed > 1)
		elog(ERROR, "invalid active artifact catalog result");
	if (SPI_processed == 1)
	{
		pg_uuid_t generation = *DatumGetUUIDP(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
		if (isnull)
			elog(ERROR, "artifact generation is NULL");
		synchdb_artifact_paths(key, &generation, out);
		artifact_resume_metadata(out, SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, 3);
		found = true;
	}
	synchdb_store_finish();
	return found;
}

void
synchdb_artifact_store_prepare(const ConnectionInfo *connection, ConnectorType type,
							   SynchdbArtifactPaths *out)
{
	MemoryContext caller = CurrentMemoryContext;
	Oid types[] = {OIDOID, UUIDOID, JSONBOID};
	Datum values[3];

	if (!connection || !out || IsTransactionState() || ActiveSnapshotSet())
		elog(ERROR, "artifact engine claim requires an idle worker");
	artifact_key(&connection->key);
	artifact_directory();
	PG_TRY();
	{
		char *catalog;
		char *query;
		Jsonb *identity;
		bool isnull;

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		catalog = artifact_catalog(&connection->key, false);
		/* A queued accepted configuration must not start after a concurrent
		 * catalog edit has replaced its persistent revision. */
		artifact_require_revision(connection);
		identity = artifact_identity(connection, type);
		values[0] = ObjectIdGetDatum(MyDatabaseId);
		values[1] = UUIDPGetDatum(&connection->key.connector_id);
		values[2] = JsonbPGetDatum(identity);
		/* PostgreSQL native source names come from the generation. Legacy
		 * names remain frozen metadata for explicit migrations, not a match
		 * against a later display alias or database name. Physical source and
		 * selection identity still have to agree exactly. Other providers
		 * retain their existing alias-dependent source identity contract. */
		query = psprintf("SELECT generation,%s,origin,source_identity FROM %s "
			"WHERE database_oid OPERATOR(pg_catalog.=) $1 AND connector_id OPERATOR(pg_catalog.=) $2 AND active FOR UPDATE",
			type == TYPE_POSTGRES ?
			"(source_identity OPERATOR(pg_catalog.-) ARRAY['legacy_slot_alias','legacy_slot_database']::pg_catalog.text[]) "
			"OPERATOR(pg_catalog.=) ($3 OPERATOR(pg_catalog.-) ARRAY['legacy_slot_alias','legacy_slot_database']::pg_catalog.text[])" :
			"source_identity OPERATOR(pg_catalog.=) $3", catalog);
		if (SPI_execute_with_args(query, 3, types, values, NULL, false, 2) != SPI_OK_SELECT || SPI_processed > 1)
			elog(ERROR, "invalid active artifact catalog result");
		if (SPI_processed == 1)
		{
			pg_uuid_t generation = *DatumGetUUIDP(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
			bool compatible = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));

			if (isnull || !compatible)
				ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								errmsg("source or selection changed for an engine-owned artifact generation"),
								errhint("Stop the connector and explicitly reset its artifact generation before selecting a new snapshot.")));
			synchdb_artifact_paths(&connection->key, &generation, out);
			artifact_resume_metadata(out, SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, 4);
			artifact_regular(out->offset, true);
			if (type != TYPE_POSTGRES)
				artifact_regular(out->history, true);
			artifact_selections(connection, out, identity, false);
		}
		else
		{
			pg_uuid_t generation;

			/* New generations are independent of display names and historical
			 * alias-based files. Only their exact UUID paths can collide. */
			artifact_new_generation(&generation);
			synchdb_artifact_paths(&connection->key, &generation, out);
			artifact_require_fresh(out);
			artifact_selections(connection, out, identity, true);
			artifact_insert(catalog, connection, out, identity, false);
		}
		synchdb_store_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		MemoryContextSwitchTo(caller);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(caller);
		AbortCurrentTransaction();
		MemoryContextSwitchTo(caller);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

Datum
synchdb_artifact_manifest(PG_FUNCTION_ARGS)
{
	SynchdbConnectorKey key = {MyDatabaseId, *PG_GETARG_UUID_P(0)};
	Oid types[] = {OIDOID, UUIDOID};
	Datum values[] = {ObjectIdGetDatum(MyDatabaseId), UUIDPGetDatum(&key.connector_id)};
	MemoryContext caller = CurrentMemoryContext;
	char *catalog;
	char *query;
	Jsonb *result;
	bool isnull;

	artifact_admin();
	artifact_key(&key);
	synchdb_store_connect();
	catalog = synchdb_store_relation("synchdb_artifacts", AccessShareLock);
	query = psprintf("SELECT pg_catalog.jsonb_build_object('version',1,'connector_id',$2,'database_oid',$1,"
		"'generations',COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(a) OPERATOR(pg_catalog.||) "
		"pg_catalog.jsonb_build_object('base_path',pg_catalog.format('pg_synchdb/%%s_%%s_%%s',database_oid,connector_id,generation)) "
		"ORDER BY created_at),'[]'::pg_catalog.jsonb)) "
		"FROM %s a WHERE database_oid OPERATOR(pg_catalog.=) $1 AND connector_id OPERATOR(pg_catalog.=) $2", catalog);
	if (SPI_execute_with_args(query, 2, types, values, NULL, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "could not inspect artifact manifest");
	MemoryContextSwitchTo(caller);
	result = DatumGetJsonbPCopy(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
	if (isnull)
		elog(ERROR, "artifact manifest is NULL");
	synchdb_store_finish();
	PG_RETURN_JSONB_P(result);
}

static int
artifact_retire(const SynchdbConnectorKey *key, bool purge)
{
	Oid types[] = {OIDOID, UUIDOID};
	Datum values[] = {ObjectIdGetDatum(MyDatabaseId), UUIDPGetDatum(&key->connector_id)};
	char *catalog = artifact_catalog(key, true);
	char *query = psprintf("UPDATE %s SET active=false,%supdated_at=pg_catalog.clock_timestamp() "
		"WHERE database_oid OPERATOR(pg_catalog.=) $1 AND connector_id OPERATOR(pg_catalog.=) $2 %s", catalog,
		purge ? "phase='PURGE_REQUESTED',last_error=NULL,last_sqlstate=NULL," : "",
		purge ? "AND phase OPERATOR(pg_catalog.<>) 'PURGED'" : "AND active");
	int changed;

	if (SPI_execute_with_args(query, 2, types, values, NULL, false, 0) != SPI_OK_UPDATE)
		elog(ERROR, "could not retire artifact generation");
	changed = (int) SPI_processed;
	synchdb_store_finish();
	return changed;
}

Datum
synchdb_artifact_reset(PG_FUNCTION_ARGS)
{
	SynchdbConnectorKey key = {MyDatabaseId, *PG_GETARG_UUID_P(0)};

	artifact_admin();
	PG_RETURN_INT32(artifact_retire(&key, false));
}

Datum
synchdb_artifact_request_purge(PG_FUNCTION_ARGS)
{
	SynchdbConnectorKey key = {MyDatabaseId, *PG_GETARG_UUID_P(0)};

	artifact_admin();
	PG_RETURN_INT32(artifact_retire(&key, true));
}

static void
artifact_copy(const char *source, const char *destination)
{
	int input;
	int output;
	char buffer[8192];
	ssize_t count;
	uint64 copied = 0;
	ArtifactCreatedFile *created;

	artifact_regular(source, true);
	/* Allocate/register before O_EXCL so an allocation failure cannot strand
	 * an untracked newly-created file. Link it only after creation succeeds. */
	created = artifact_track_created(destination);
	input = OpenTransientFile(source, O_RDONLY | PG_BINARY | O_NOFOLLOW);
	if (input < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not open legacy artifact: %m")));
	output = OpenTransientFilePerm(destination, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY | O_NOFOLLOW, pg_file_create_mode);
	if (output < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not create migrated artifact: %m")));
	created->next = artifact_created_files;
	artifact_created_files = created;
	while ((count = read(input, buffer, sizeof(buffer))) != 0)
	{
		ssize_t written = 0;

		CHECK_FOR_INTERRUPTS();
		if (count < 0)
		{
			if (errno == EINTR)
				continue;
			ereport(ERROR, (errcode_for_file_access(), errmsg("could not read legacy artifact: %m")));
		}
		copied += count;
		if (copied > 64 * 1024 * 1024)
			ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("legacy artifact grew beyond 64 MiB during migration")));
		while (written < count)
		{
			ssize_t n = write(output, buffer + written, count - written);

			if (n < 0 && errno == EINTR)
				continue;
			if (n <= 0)
				ereport(ERROR, (errcode_for_file_access(), errmsg("could not write migrated artifact: %m")));
			written += n;
		}
	}
	if (pg_fsync(output) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not sync migrated artifact: %m")));
	if (CloseTransientFile(input) < 0 || CloseTransientFile(output) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not close migrated artifact: %m")));
	fsync_fname("pg_synchdb", true);
}

Datum
synchdb_artifact_migrate_legacy(PG_FUNCTION_ARGS)
{
	MemoryContext caller = CurrentMemoryContext;
	ConnectionInfo connection;
	char *connector;
	char *catalog;
	ConnectorType type;
	SynchdbArtifactPaths paths;
	pg_uuid_t generation;
	char *offset;
	char *history;
	Jsonb *identity;
	pg_uuid_t *result;

	artifact_admin();
	if (!synchdb_connector_store_get(NameStr(*PG_GETARG_NAME(0)), &connection, &connector, CurrentMemoryContext))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("connector configuration does not exist")));
	if (strchr(connection.name, '_') || strchr(connection.dstdb, '_'))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("legacy underscore-separated artifact identity is ambiguous"),
						errdetail("Migration cannot prove which connector/database pair originally owned this filename. Original files are preserved.")));
	if (pg_strcasecmp(connector, "mysql") == 0) type = TYPE_MYSQL;
	else if (pg_strcasecmp(connector, "oracle") == 0) type = TYPE_ORACLE;
	else if (pg_strcasecmp(connector, "sqlserver") == 0) type = TYPE_SQLSERVER;
	else if (pg_strcasecmp(connector, "postgres") == 0) type = TYPE_POSTGRES;
	else
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("legacy migration does not support this capture backend")));
	artifact_directory();
	catalog = artifact_catalog(&connection.key, true);
	artifact_require_revision(&connection);
	if (synchdb_artifact_store_lookup(&connection.key, &paths))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("connector already has an active artifact generation")));
	if (pg_strcasecmp(connection.ispn.ispn_cache_type, "null") != 0)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("legacy Infinispan caches require dedicated offline migration")));
	offset = artifact_legacy_path(&connection, type, "offsets");
	history = artifact_legacy_path(&connection, type, "schemahistory");
	artifact_new_generation(&generation);
	synchdb_artifact_paths(&connection.key, &generation, &paths);
	identity = artifact_identity(&connection, type);
	/* Only exact files are copied; old files remain valid for explicit rollback.
	 * Abort callbacks remove unpublished files; abrupt process/server death may
	 * leave unreferenced UUID files, never a partly active catalog reference. */
	artifact_copy(offset, paths.offset);
	if (artifact_regular(history, type != TYPE_POSTGRES))
		artifact_copy(history, paths.history);
	artifact_selections(&connection, &paths, identity, true);
	artifact_insert(catalog, &connection, &paths, identity, true);
	MemoryContextSwitchTo(caller);
	result = palloc(sizeof(*result));
	*result = generation;
	synchdb_store_finish();
	PG_RETURN_UUID_P(result);
}

/* Never follow a cache symlink out of an exact generation-owned root. A false
 * result means the destructive-operation budget was exhausted; leave the
 * committed PURGING intent available for the next controller cycle. */
static bool
artifact_remove_tree(const char *path, unsigned int *remaining)
{
	struct stat st;

	CHECK_FOR_INTERRUPTS();
	check_stack_depth();
	if (*remaining == 0)
		return false;
	if (lstat(path, &st) < 0)
	{
		if (errno == ENOENT)
			return true;
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not inspect purge artifact: %m")));
	}
	if (st.st_uid != geteuid())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("purge artifact is not server-owned")));
	if (S_ISDIR(st.st_mode))
	{
		DIR *directory = AllocateDir(path);
		struct dirent *entry;

		if (!directory)
			ereport(ERROR, (errcode_for_file_access(), errmsg("could not open purge cache: %m")));
		while ((entry = ReadDir(directory, path)) != NULL)
		{
			char *child;
			bool removed;

			CHECK_FOR_INTERRUPTS();
			if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
				continue;
			child = psprintf("%s/%s", path, entry->d_name);
			removed = artifact_remove_tree(child, remaining);
			pfree(child);
			if (!removed)
			{
				FreeDir(directory);
				return false;
			}
		}
		FreeDir(directory);
		if (*remaining == 0)
			return false;
		if (rmdir(path) < 0)
			ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove purge cache: %m")));
	}
	else if (unlink(path) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove purge artifact: %m")));
	(*remaining)--;
	return true;
}

/* A caught filesystem/catalog error requires explicit operator retry. An
 * abrupt process exit leaves PURGING, which is safe for automatic crash replay.
 * Caller has aborted the failed transaction and preserved the original error. */
static void
artifact_purge_failed(const ArtifactPurgeAttempt *attempt, const ErrorData *failure)
{
	Oid types[] = {OIDOID, UUIDOID, UUIDOID, TEXTOID, TEXTOID};
	Datum values[] = {ObjectIdGetDatum(MyDatabaseId), UUIDPGetDatum(&attempt->key.connector_id),
		UUIDPGetDatum(&attempt->generation), CStringGetTextDatum(failure->message),
		CStringGetTextDatum(unpack_sql_state(failure->sqlerrcode))};
	char *catalog;
	char *query;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	synchdb_store_connect();
	catalog = synchdb_store_relation("synchdb_artifacts", RowExclusiveLock);
	query = psprintf("UPDATE %s SET phase='PURGE_FAILED',last_error=$4,last_sqlstate=$5,"
		"updated_at=pg_catalog.clock_timestamp() WHERE database_oid OPERATOR(pg_catalog.=) $1 "
		"AND connector_id OPERATOR(pg_catalog.=) $2 AND generation OPERATOR(pg_catalog.=) $3 "
		"AND phase OPERATOR(pg_catalog.=) 'PURGING' AND NOT active", catalog);
	if (SPI_execute_with_args(query, 5, types, values, NULL, false, 0) != SPI_OK_UPDATE)
		elog(ERROR, "could not persist artifact purge failure");
	/* Zero rows means a concurrent explicit request or successful sweep has
	 * already replaced this attempt; do not overwrite its newer intent. */
	synchdb_store_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}

static void
artifact_purge_failure_safely(const ArtifactPurgeAttempt *attempt,
							  const ErrorData *failure, MemoryContext caller)
{
	PG_TRY();
	{
		artifact_purge_failed(attempt, failure);
		MemoryContextSwitchTo(caller);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(caller);
		FlushErrorState();
		AbortCurrentTransaction();
		MemoryContextSwitchTo(caller);
		elog(WARNING, "could not journal artifact purge failure; its committed PURGING intent remains replayable");
	}
	PG_END_TRY();
}

int
synchdb_artifact_store_process_purges(void)
{
	MemoryContext caller = CurrentMemoryContext;
	SynchdbConnectorKey key;
	pg_uuid_t generation;
	SynchdbArtifactPaths paths;
	ArtifactPurgeAttempt *attempt;
	bool found = false;
	int completed = 0;

	if (IsTransactionState() || ActiveSnapshotSet())
		elog(ERROR, "artifact purge sweep requires an idle controller");
	attempt = MemoryContextAllocZero(caller, sizeof(*attempt));
	/* One generation and at most 256 unlink/rmdir operations per cycle leave
	 * STOP/PAUSE and other connector reconciliation work runnable each cycle. */
	PG_TRY();
	{
		char *catalog;
		char *query;
		Oid types[] = {OIDOID, UUIDOID, UUIDOID};
		Datum values[3];
		bool isnull;

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		synchdb_store_connect();
		catalog = synchdb_store_relation("synchdb_artifacts", ShareRowExclusiveLock);
		query = psprintf("SELECT connector_id,generation FROM %s WHERE database_oid OPERATOR(pg_catalog.=) %u "
			"AND (phase OPERATOR(pg_catalog.=) 'PURGE_REQUESTED' OR phase OPERATOR(pg_catalog.=) 'PURGING') "
			"ORDER BY created_at LIMIT 1 FOR UPDATE", catalog, MyDatabaseId);
		if (SPI_execute(query, false, 1) != SPI_OK_SELECT)
			elog(ERROR, "could not read artifact purge intent");
		if (SPI_processed == 1)
		{
			key.database_oid = MyDatabaseId;
			key.connector_id = *DatumGetUUIDP(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
			generation = *DatumGetUUIDP(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
			attempt->key = key;
			attempt->generation = generation;
			synchdb_artifact_paths(&key, &generation, &paths);
			values[0] = ObjectIdGetDatum(MyDatabaseId);
			values[1] = UUIDPGetDatum(&key.connector_id);
			values[2] = UUIDPGetDatum(&generation);
			query = psprintf("UPDATE %s SET phase='PURGING',active=false,last_error=NULL,last_sqlstate=NULL,"
				"updated_at=pg_catalog.clock_timestamp() "
				"WHERE database_oid OPERATOR(pg_catalog.=) $1 AND connector_id OPERATOR(pg_catalog.=) $2 "
				"AND generation OPERATOR(pg_catalog.=) $3", catalog);
			if (SPI_execute_with_args(query, 3, types, values, NULL, false, 0) != SPI_OK_UPDATE || SPI_processed != 1)
				elog(ERROR, "could not persist artifact purge phase");
			found = true;
		}
		synchdb_store_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		MemoryContextSwitchTo(caller);
		if (found)
		{
			unsigned int remaining = 256;
			bool all_removed;

			attempt->intent_committed = true;
			artifact_directory();
			all_removed = artifact_remove_tree(paths.offset, &remaining) &&
				artifact_remove_tree(paths.history, &remaining) &&
				artifact_remove_tree(paths.signal, &remaining) &&
				artifact_remove_tree(paths.cache, &remaining) &&
				artifact_remove_tree(paths.table_filter, &remaining) &&
				artifact_remove_tree(paths.snapshot_filter, &remaining);
			fsync_fname("pg_synchdb", true);
			if (all_removed)
			{
				StartTransactionCommand();
				PushActiveSnapshot(GetTransactionSnapshot());
				synchdb_store_connect();
				catalog = synchdb_store_relation("synchdb_artifacts", RowExclusiveLock);
				query = psprintf("UPDATE %s SET phase='PURGED',updated_at=pg_catalog.clock_timestamp() "
					"WHERE database_oid OPERATOR(pg_catalog.=) $1 AND connector_id OPERATOR(pg_catalog.=) $2 "
					"AND generation OPERATOR(pg_catalog.=) $3 AND phase OPERATOR(pg_catalog.=) 'PURGING' AND NOT active", catalog);
				if (SPI_execute_with_args(query, 3, types, values, NULL, false, 0) != SPI_OK_UPDATE || SPI_processed != 1)
					elog(ERROR, "could not complete artifact purge intent");
				synchdb_store_finish();
				PopActiveSnapshot();
				CommitTransactionCommand();
				MemoryContextSwitchTo(caller);
				completed++;
			}
		}
	}
	PG_CATCH();
	{
		ErrorData *failure;

		MemoryContextSwitchTo(caller);
		failure = CopyErrorData();
		FlushErrorState();
		AbortCurrentTransaction();
		MemoryContextSwitchTo(caller);
		if (attempt->intent_committed)
			artifact_purge_failure_safely(attempt, failure, caller);
		pfree(attempt);
		ReThrowError(failure);
	}
	PG_END_TRY();
	pfree(attempt);
	return completed;
}
