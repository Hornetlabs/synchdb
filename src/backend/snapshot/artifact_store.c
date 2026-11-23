/* Exact, persistent local engine ownership, including explicit FDW seeds. */
#include "postgres.h"
#include "fmgr.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog_access.h"
#include "catalog/connector_store.h"
#include "catalog/pg_authid_d.h"
#include "catalog/pg_type_d.h"
#include "common/file_perm.h"
#include "executor/spi.h"
#include "commands/dbcommands.h"
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
static void artifact_require_cache_layout(const char *path);

typedef struct ArtifactCreatedFile
{
	char *path;
	bool directory;
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
	if ((file->directory ? rmdir(file->path) : unlink(file->path)) < 0 && errno != ENOENT)
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
		/* The old C cursor and completion byte used olr_*. Java's
		 * openlog_replicator_d_* files describe its separate snapshot engine. */
		case TYPE_OLR: return "olr";
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
artifact_require_claim_result(const char *query, int count, Oid *types,
							  Datum *values, const char *message)
{
	bool isnull;
	Datum result;

	if (SPI_execute_with_args(query, count, types, values, NULL, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "could not check legacy artifact ownership inventory");
	result = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull || !DatumGetBool(result))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg_internal("%s", message)));
}

/* Legacy names can contain underscores. Only another actual database suffix
 * makes their split uncertain. An administrator can resolve that uncertainty
 * with a complete cluster inventory; the automatic upgrader collects it from
 * every database while all SynchDB writers are stopped. Never infer ownership
 * by splitting at the first or last underscore. */
static void
artifact_require_legacy_owner(const ConnectionInfo *connection, ConnectorType type, Jsonb *claims)
{
	Oid types[] = {JSONBOID, TEXTOID, OIDOID, TEXTOID};
	Datum values[4];
	char system_identifier[32];
	char *stem = psprintf("%s_%s", connection->name, connection->dstdb);
	char *connectors;
	char *query;
	int level = synchdb_store_begin_path();

	if (strcmp(connection->dstdb, get_database_name(MyDatabaseId)) != 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("legacy artifact destination does not match the current database")));
	/* Validate aliases even when a caller supplies an ownership inventory. */
	(void) artifact_legacy_path(connection, type, "offsets");
	if (type == TYPE_POSTGRES &&
		(strlen(stem) + strlen("_synchdb_slot") >= NAMEDATALEN ||
		 strlen(stem) + strlen("_synchdb_pub") >= NAMEDATALEN))
		ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
						errmsg("legacy PostgreSQL slot or publication identity exceeds the identifier limit")));
	if (!claims)
	{
		Oid plain_types[] = {TEXTOID, TEXTOID};
		Datum plain_values[] = {CStringGetTextDatum(connection->dstdb), CStringGetTextDatum(stem)};

		artifact_require_claim_result(
			"SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database "
			"WHERE datname<>$1 AND length($2)>length(datname)+1 "
			"AND right($2,length(datname)+1)='_'||datname)",
			2, plain_types, plain_values,
			"legacy artifact filename has another possible database owner; supply a complete cluster ownership inventory");
		synchdb_store_end_path(level);
		return;
	}
	snprintf(system_identifier, sizeof(system_identifier), UINT64_FORMAT, GetSystemIdentifier());
	values[0] = JsonbPGetDatum(claims);
	values[1] = CStringGetTextDatum(system_identifier);
	values[2] = ObjectIdGetDatum(MyDatabaseId);
	values[3] = CStringGetTextDatum(artifact_legacy_path(connection, type, "offsets"));
	artifact_require_claim_result(
		"SELECT jsonb_typeof($1)='object' AND jsonb_typeof($1->'databases')='array' "
		"AND jsonb_typeof($1->'system_identifier')='string' AND $1->>'system_identifier'=$2",
		2, types, values, "legacy ownership inventory has an invalid format or belongs to another PostgreSQL cluster");
	artifact_require_claim_result(
		"WITH declared AS (SELECT * FROM jsonb_to_recordset($1->'databases') "
		"AS d(oid oid,name text,connectors jsonb)), expected AS "
		"(SELECT oid,datname::text AS name FROM pg_catalog.pg_database WHERE datallowconn AND NOT datistemplate) "
		"SELECT NOT EXISTS (SELECT 1 FROM declared WHERE oid IS NULL OR name IS NULL "
		"OR jsonb_typeof(connectors) IS DISTINCT FROM 'array') "
		"AND (SELECT count(*)=count(DISTINCT oid) AND count(*)=count(DISTINCT name) FROM declared) "
		"AND NOT EXISTS ((SELECT oid,name FROM expected EXCEPT SELECT oid,name FROM declared) "
		"UNION ALL (SELECT oid,name FROM declared EXCEPT SELECT oid,name FROM expected))",
		1, types, values, "legacy ownership inventory is incomplete or database names have changed");
	artifact_require_claim_result(
		"WITH declared AS (SELECT d.oid,c.* FROM jsonb_to_recordset($1->'databases') "
		"AS d(oid oid,name text,connectors jsonb) CROSS JOIN LATERAL "
		"jsonb_to_recordset(d.connectors) AS c(name text,connector text,target_database text)) "
		"SELECT NOT EXISTS (SELECT 1 FROM declared WHERE name IS NULL OR name='' "
		"OR connector IS NULL OR connector='' OR target_database IS NULL OR target_database='') "
		"AND NOT EXISTS (SELECT 1 FROM declared GROUP BY oid,name HAVING count(*)<>1)",
		1, types, values, "legacy ownership inventory contains malformed or duplicate connector claims");
	/* The caller has audited the remote databases. Recheck the complete local
	 * inventory against persistent rows rather than trusting its own claim. */
	connectors = synchdb_store_relation("synchdb_conninfo", ShareLock);
	query = psprintf(
		"WITH declared AS (SELECT c.* FROM jsonb_to_recordset($1->'databases') "
		"AS d(oid oid,name text,connectors jsonb) CROSS JOIN LATERAL "
		"jsonb_to_recordset(d.connectors) AS c(name text,connector text,target_database text) WHERE d.oid=$3), "
		"actual AS (SELECT name::text,lower(data->>'connector') AS connector,"
		"coalesce(data->>'dstdb',current_database()) AS target_database FROM %s) "
		"SELECT NOT EXISTS ((SELECT name,lower(connector),target_database FROM declared "
		"EXCEPT SELECT * FROM actual) UNION ALL (SELECT * FROM actual EXCEPT "
		"SELECT name,lower(connector),target_database FROM declared))", connectors);
	artifact_require_claim_result(query, 3, types, values,
		"legacy ownership inventory no longer matches this database's connector configuration");
	/* Even an unavailable/template database can own an old suffix. A skipped
	 * database cannot be certified as empty merely because it was not scanned. */
	query = psprintf(
		"WITH declared AS (SELECT * FROM jsonb_to_recordset($1->'databases') "
		"AS d(oid oid,name text,connectors jsonb)) SELECT NOT EXISTS "
		"(SELECT 1 FROM pg_catalog.pg_database d WHERE length(%s)>length(d.datname)+1 "
		"AND right(%s,length(d.datname)+1)='_'||d.datname "
		"AND NOT EXISTS (SELECT 1 FROM declared x WHERE x.oid=d.oid))",
		quote_literal_cstr(stem), quote_literal_cstr(stem));
	artifact_require_claim_result(query, 1, types, values,
		"another possible legacy filename owner belongs to a database omitted from the ownership inventory");
	artifact_require_claim_result(
		"WITH declared AS (SELECT d.oid,c.* FROM jsonb_to_recordset($1->'databases') "
		"AS d(oid oid,name text,connectors jsonb) CROSS JOIN LATERAL "
		"jsonb_to_recordset(d.connectors) AS c(name text,connector text,target_database text)), "
		"paths AS (SELECT oid,name,target_database,'pg_synchdb/'||CASE lower(connector) "
		"WHEN 'mysql' THEN 'mysql' WHEN 'oracle' THEN 'oracle' WHEN 'sqlserver' THEN 'sqlserver' "
		"WHEN 'postgres' THEN 'postgres' WHEN 'postgresql' THEN 'postgres' WHEN 'olr' THEN 'olr' END "
		"||'_'||name||'_'||target_database||'_offsets.dat' AS path FROM declared) "
		"SELECT count(*)=1 AND bool_and(oid=$3) FROM paths WHERE path=$4",
		4, types, values, "multiple connector/database pairs claim the same legacy artifact filename");
	synchdb_store_end_path(level);
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
				const SynchdbArtifactPaths *paths, Jsonb *identity, const char *origin)
{
	Oid types[] = {OIDOID, UUIDOID, UUIDOID, INT8OID, JSONBOID, TEXTOID};
	Datum values[] = {ObjectIdGetDatum(MyDatabaseId), UUIDPGetDatum(&connection->key.connector_id),
		UUIDPGetDatum(&paths->generation), Int64GetDatum(connection->config_revision),
		JsonbPGetDatum(identity), CStringGetTextDatum(origin)};
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
	if (origin && strcmp(origin, "FDW_SEED") == 0)
	{
		paths->fdw_seed = true;
		return;
	}
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

static void
artifact_store_prepare(const ConnectionInfo *connection, ConnectorType type,
					   SynchdbArtifactPaths *out, bool fdw_seed)
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
			char *origin = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);

			if (isnull || !compatible)
				ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								errmsg("source or selection changed for an engine-owned artifact generation"),
								errhint("Stop the connector and explicitly reset its artifact generation before selecting a new snapshot.")));
			synchdb_artifact_paths(&connection->key, &generation, out);
			artifact_resume_metadata(out, SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, 4);
			/* FDW and imported legacy OLR resume from the position catalog,
			 * without a Java producer offset. Legacy copies are kept as evidence,
			 * not interpreted as serialized Debezium files. SQL Server may be
			 * stopped before recovery has built its connector-specific history. */
			if (!(type == TYPE_OLR && (out->fdw_seed || strcmp(origin, "LEGACY_COPY") == 0)))
			{
				artifact_regular(out->offset, true);
				if (type != TYPE_POSTGRES)
					artifact_regular(out->history, !(type == TYPE_SQLSERVER && out->fdw_seed));
			}
			if (type == TYPE_ORACLE && strcmp(origin, "LEGACY_COPY") == 0 &&
				pg_strcasecmp(connection->ispn.ispn_cache_type, "null") != 0)
				artifact_require_cache_layout(out->cache);
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
			out->fdw_seed = fdw_seed;
			artifact_insert(catalog, connection, out, identity, fdw_seed ? "FDW_SEED" : "NATIVE");
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

void
synchdb_artifact_store_prepare(const ConnectionInfo *connection, ConnectorType type,
							   SynchdbArtifactPaths *out)
{
	artifact_store_prepare(connection, type, out,
		type == TYPE_OLR && connection->snapengine == ENGINE_FDW);
}

void
synchdb_artifact_store_prepare_fdw(const ConnectionInfo *connection, ConnectorType type,
								 SynchdbArtifactPaths *out)
{
	artifact_store_prepare(connection, type, out, true);
	if (out->resume_existing)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("FDW bootstrap cannot overwrite an existing artifact generation"),
						errhint("Resume the existing generation, or stop and explicitly reset it.")));
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

static bool
artifact_same_file(const struct stat *before, const struct stat *after)
{
	return before->st_dev == after->st_dev && before->st_ino == after->st_ino &&
		before->st_size == after->st_size && before->st_mtime == after->st_mtime &&
		before->st_ctime == after->st_ctime;
}

static void
artifact_copy_file(const char *source, const char *destination, bool cache_file)
{
	int input;
	int output;
	char buffer[8192];
	ssize_t count;
	uint64 copied = 0;
	ArtifactCreatedFile *created;
	struct stat before;
	struct stat after;

	if (!cache_file)
		artifact_regular(source, true);
	/* Allocate/register before O_EXCL so an allocation failure cannot strand
	 * an untracked newly-created file. Link it only after creation succeeds. */
	created = artifact_track_created(destination);
	input = OpenTransientFile(source, O_RDONLY | PG_BINARY | O_NOFOLLOW);
	if (input < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not open legacy artifact: %m")));
	if (fstat(input, &before) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not inspect opened legacy artifact: %m")));
	if (!S_ISREG(before.st_mode) || before.st_nlink != 1 || before.st_uid != geteuid() ||
		(before.st_mode & 0022) || before.st_size < 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid legacy artifact file \"%s\"", source)));
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
		if (!cache_file && copied > 64 * 1024 * 1024)
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
	if (fstat(input, &after) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not recheck legacy artifact: %m")));
	if (!artifact_same_file(&before, &after) || copied != (uint64) before.st_size)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("legacy artifact changed during migration"),
						errdetail("Stop the old connector and PostgreSQL cleanly before copying its persisted state.")));
	if (pg_fsync(output) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not sync migrated artifact: %m")));
	if (CloseTransientFile(input) < 0 || CloseTransientFile(output) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not close migrated artifact: %m")));
	if (!cache_file)
		fsync_fname("pg_synchdb", true);
}

static void
artifact_copy(const char *source, const char *destination)
{
	artifact_copy_file(source, destination, false);
}

/* Store files can exceed the offset/history limit and can legitimately be
 * empty. Copy all metadata as well as data/index, preserving the old tree.
 * Each exclusive new path is tracked independently: abort removes children
 * before parents and never recursively visits an existing engine directory. */
static void
artifact_cache_stat(const char *path, struct stat *st)
{
	if (lstat(path, st) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not inspect Infinispan cache \"%s\": %m", path)));
	if (!S_ISDIR(st->st_mode) || st->st_uid != geteuid() || (st->st_mode & 0022))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("Infinispan cache directories must be server-owned directories without writable group/world access")));
}

static void
artifact_copy_cache_tree(const char *source, const char *destination, int depth, uint64 *entries)
{
	DIR *directory;
	struct dirent *entry;
	struct stat before;
	struct stat after;
	ArtifactCreatedFile *created;

	CHECK_FOR_INTERRUPTS();
	check_stack_depth();
	if (depth > 16 || ++(*entries) > 100000)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("legacy Infinispan cache exceeds migration directory limits")));
	artifact_cache_stat(source, &before);
	directory = AllocateDir(source);
	if (!directory)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not open legacy Infinispan cache directory: %m")));
	if (fstat(dirfd(directory), &after) < 0 || !artifact_same_file(&before, &after))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("legacy Infinispan cache directory changed before copying")));
	created = artifact_track_created(destination);
	created->directory = true;
	if (MakePGDirectory(destination) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not create migrated Infinispan cache directory: %m")));
	created->next = artifact_created_files;
	artifact_created_files = created;
	while ((entry = ReadDir(directory, source)) != NULL)
	{
		char *input;
		char *output;
		struct stat st;

		if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
			continue;
		CHECK_FOR_INTERRUPTS();
		input = psprintf("%s/%s", source, entry->d_name);
		output = psprintf("%s/%s", destination, entry->d_name);
		if (strlen(input) >= MAXPGPATH || strlen(output) >= MAXPGPATH)
			ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("legacy Infinispan cache path exceeds migration limits")));
		if (lstat(input, &st) < 0)
			ereport(ERROR, (errcode_for_file_access(), errmsg("could not inspect legacy Infinispan cache entry: %m")));
		if (S_ISDIR(st.st_mode))
			artifact_copy_cache_tree(input, output, depth + 1, entries);
		else if (S_ISREG(st.st_mode))
		{
			if (++(*entries) > 100000)
				ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("legacy Infinispan cache exceeds migration entry limits")));
			artifact_copy_file(input, output, true);
		}
		else
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("legacy Infinispan cache contains a symlink or special file")));
		pfree(input);
		pfree(output);
	}
	artifact_cache_stat(source, &after);
	if (!artifact_same_file(&before, &after))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("legacy Infinispan cache directory changed during migration")));
	FreeDir(directory);
	fsync_fname(destination, true);
}

static bool
artifact_legacy_cache_enabled(const ConnectionInfo *connection, ConnectorType type)
{
	if (pg_strcasecmp(connection->ispn.ispn_cache_type, "null") == 0)
		return false;
	if (type != TYPE_ORACLE || pg_strcasecmp(connection->ispn.ispn_cache_type, "embedded") != 0 ||
		(pg_strcasecmp(connection->ispn.ispn_memory_type, "heap") != 0 &&
		 pg_strcasecmp(connection->ispn.ispn_memory_type, "off heap") != 0) ||
		connection->ispn.ispn_memory_size == 0 || connection->ispn.ispn_memory_size > PG_INT32_MAX)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("legacy Infinispan migration requires Oracle embedded buffering with valid memory settings")));
	return true;
}

static void
artifact_require_legacy_cache_owner(const ConnectionInfo *connection, Jsonb *claims)
{
	Oid types[] = {JSONBOID, OIDOID, TEXTOID};
	Datum values[3];
	char *catalog;
	char *query;

	/* The single-argument filename check already excludes every possible
	 * alternative database suffix, independent of the capture backend. */
	if (!claims)
		return;
	values[0] = JsonbPGetDatum(claims);
	values[1] = ObjectIdGetDatum(MyDatabaseId);
	values[2] = CStringGetTextDatum(psprintf("ispn_%s_%s", connection->name, connection->dstdb));
	artifact_require_claim_result(
		"SELECT NOT EXISTS (SELECT 1 FROM jsonb_array_elements($1->'databases') d "
		"CROSS JOIN LATERAL jsonb_array_elements(d->'connectors') c "
		"WHERE jsonb_typeof(c->'ispn_cache_type') IS DISTINCT FROM 'string' "
		"OR c->>'ispn_cache_type'='')",
		1, types, values,
		"Infinispan migration requires an explicit cache type for every connector in the cluster ownership inventory");
	catalog = synchdb_store_relation("synchdb_conninfo", ShareLock);
	query = psprintf(
		"WITH declared AS (SELECT c.name,lower(c.ispn_cache_type) AS cache_type "
		"FROM jsonb_to_recordset($1->'databases') d(oid oid,name text,connectors jsonb) "
		"CROSS JOIN LATERAL jsonb_to_recordset(d.connectors) c(name text,ispn_cache_type text) WHERE d.oid=$2), "
		"actual AS (SELECT name::text,lower(coalesce(data->>'ispn_cache_type','null')) AS cache_type FROM %s) "
		"SELECT NOT EXISTS ((SELECT * FROM declared EXCEPT SELECT * FROM actual) "
		"UNION ALL (SELECT * FROM actual EXCEPT SELECT * FROM declared))", catalog);
	artifact_require_claim_result(query, 2, types, values,
		"legacy ownership inventory no longer matches this database's Infinispan configuration");
	artifact_require_claim_result(
		"WITH declared AS (SELECT d.oid,c.name,c.target_database,c.ispn_cache_type "
		"FROM jsonb_to_recordset($1->'databases') d(oid oid,name text,connectors jsonb) "
		"CROSS JOIN LATERAL jsonb_to_recordset(d.connectors) "
		"c(name text,target_database text,ispn_cache_type text)) "
		"SELECT count(*)=1 AND bool_and(oid=$2) FROM declared "
		"WHERE lower(ispn_cache_type)<>'null' AND 'ispn_'||name||'_'||target_database=$3",
		3, types, values,
		"multiple connector/database pairs claim the same legacy Infinispan cache directory");
}

static void
artifact_require_cache_layout(const char *source)
{
	const char *groups[] = {"events", "transactions", "processed", "schema"};
	struct stat st;
	int index;

	artifact_cache_stat(source, &st);
	/* A missing transaction store must not be treated as an empty buffer.
	 * Extra provider metadata, including a persisted rollbacks cache, is copied
	 * by the complete tree traversal rather than silently discarded. */
	for (index = 0; index < lengthof(groups); index++)
	{
		char *data = psprintf("%s/%s/data", source, groups[index]);
		char *indexes = psprintf("%s/%s/index", source, groups[index]);

		artifact_cache_stat(data, &st);
		artifact_cache_stat(indexes, &st);
		pfree(data);
		pfree(indexes);
	}
}

static void
artifact_import_legacy_cache(const ConnectionInfo *connection, const SynchdbArtifactPaths *paths)
{
	char *source = psprintf("pg_synchdb/ispn_%s_%s", connection->name, connection->dstdb);
	uint64 entries = 0;

	artifact_require_cache_layout(source);
	artifact_copy_cache_tree(source, paths->cache, 0, &entries);
	fsync_fname("pg_synchdb", true);
}

/* Old OLR wrote native-endian unsigned long long[3], and a separate byte
 * ('t' or 'f') for snapshot completion. Read the exclusive migration copies,
 * so the published cursor and the preserved evidence necessarily agree. */
static void
artifact_read_legacy_fixed(const char *path, void *buffer, Size size)
{
	int descriptor;
	struct stat st;
	Size total = 0;
	char extra;
	ssize_t count;

	artifact_regular(path, true);
	descriptor = OpenTransientFile(path, O_RDONLY | PG_BINARY | O_NOFOLLOW);
	if (descriptor < 0 || fstat(descriptor, &st) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not read legacy OLR artifact: %m")));
	if (!S_ISREG(st.st_mode) || st.st_nlink != 1 || st.st_uid != geteuid() ||
		(st.st_mode & 0022) || st.st_size != size)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("legacy OLR artifact has an invalid format"),
						errdetail("Expected exactly %zu bytes in \"%s\".", size, path)));
	while (total < size)
	{
		count = read(descriptor, (char *) buffer + total, size - total);
		if (count < 0 && errno == EINTR)
			continue;
		if (count < 0)
			ereport(ERROR, (errcode_for_file_access(), errmsg("could not read legacy OLR artifact: %m")));
		if (count == 0)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("legacy OLR artifact was truncated")));
		total += count;
	}
	do
	{
		count = read(descriptor, &extra, 1);
	} while (count < 0 && errno == EINTR);
	if (count < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not read legacy OLR artifact: %m")));
	if (count != 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("legacy OLR artifact changed during migration")));
	if (CloseTransientFile(descriptor) < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not close legacy OLR artifact: %m")));
}

static void
artifact_import_legacy_olr(const ConnectionInfo *connection, const SynchdbArtifactPaths *paths)
{
	uint64 cursor[3];
	unsigned char completed;
	Oid types[] = {OIDOID, UUIDOID, UUIDOID, JSONBOID, INT8OID, INT8OID, INT8OID};
	Datum values[7];
	Jsonb *proof;
	char *catalog;
	char *query;
	char *json;

	StaticAssertStmt(sizeof(uint64) == 8, "legacy OLR cursors require 64-bit integers");
	artifact_read_legacy_fixed(paths->offset, cursor, sizeof(cursor));
	artifact_read_legacy_fixed(paths->history, &completed, sizeof(completed));
	if (completed != 't')
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("legacy OLR snapshot has not completed"),
						errhint("Complete the snapshot and cleanly stop the old connector before migrating its cursor.")));
	if (!cursor[0] || !cursor[1] || cursor[0] > PG_INT64_MAX ||
		cursor[1] > PG_INT64_MAX || cursor[2] > PG_INT64_MAX)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("legacy OLR cursor is outside the supported range"),
						errdetail("SCN and c_scn must be positive, and all coordinates must fit a signed 64-bit integer.")));
	/* A legacy baseline is usable for compatibility, but has no native
	 * read-view proof. Never manufacture a snapshot_scn from its last DML:
	 * concurrent transactions may still contain earlier SCNs after this cursor.
	 * TODO: provide a source-fenced baseline validation protocol for upgrades. */
	json = psprintf("{\"protocol\":\"UNVERIFIED_LEGACY_OLR_CDC_V1\","
		"\"handoff_verified\":false,\"cdc_verified\":false,\"identity_verified\":false,"
		"\"legacy_scn\":\"" UINT64_FORMAT "\",\"legacy_c_scn\":\"" UINT64_FORMAT
		"\",\"legacy_c_idx\":\"" UINT64_FORMAT "\"}", cursor[0], cursor[1], cursor[2]);
	proof = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(json)));
	values[0] = ObjectIdGetDatum(connection->key.database_oid);
	values[1] = UUIDPGetDatum(&connection->key.connector_id);
	values[2] = UUIDPGetDatum(&paths->generation);
	values[3] = JsonbPGetDatum(proof);
	values[4] = Int64GetDatum(cursor[0]);
	values[5] = Int64GetDatum(cursor[1]);
	values[6] = Int64GetDatum(cursor[2]);
	catalog = synchdb_store_relation("synchdb_olr_positions", RowExclusiveLock);
	query = psprintf("INSERT INTO %s (database_oid,connector_id,generation,source_proof,snapshot_scn,scn,c_scn,c_idx) "
		"VALUES ($1,$2,$3,$4,0,$5,$6,$7)", catalog);
	if (SPI_execute_with_args(query, 7, types, values, NULL, false, 0) != SPI_OK_INSERT || SPI_processed != 1)
		elog(ERROR, "could not import legacy OLR cursor");
	ForceSyncCommit();
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
	bool cache_enabled;
	Jsonb *claims = PG_NARGS() > 1 && !PG_ARGISNULL(1) ? PG_GETARG_JSONB_P(1) : NULL;

	artifact_admin();
	if (!synchdb_connector_store_get(NameStr(*PG_GETARG_NAME(0)), &connection, &connector, CurrentMemoryContext))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("connector configuration does not exist")));
	if (pg_strcasecmp(connector, "mysql") == 0) type = TYPE_MYSQL;
	else if (pg_strcasecmp(connector, "oracle") == 0) type = TYPE_ORACLE;
	else if (pg_strcasecmp(connector, "sqlserver") == 0) type = TYPE_SQLSERVER;
	else if (pg_strcasecmp(connector, "postgres") == 0) type = TYPE_POSTGRES;
	else if (pg_strcasecmp(connector, "olr") == 0) type = TYPE_OLR;
	else
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("legacy migration does not support this capture backend")));
	artifact_directory();
	catalog = artifact_catalog(&connection.key, true);
	artifact_require_revision(&connection);
	artifact_require_legacy_owner(&connection, type, claims);
	if (synchdb_artifact_store_lookup(&connection.key, &paths))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("connector already has an active artifact generation")));
	cache_enabled = artifact_legacy_cache_enabled(&connection, type);
	if (cache_enabled)
		artifact_require_legacy_cache_owner(&connection, claims);
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
	if (cache_enabled)
		artifact_import_legacy_cache(&connection, &paths);
	artifact_selections(&connection, &paths, identity, true);
	artifact_insert(catalog, &connection, &paths, identity, "LEGACY_COPY");
	if (cache_enabled)
		ForceSyncCommit(); /* Publish the recovery set durably before CDC opens it. */
	if (type == TYPE_OLR)
		artifact_import_legacy_olr(&connection, &paths);
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
