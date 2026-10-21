/*
 * Versioned credentials.  The keyring is deliberately not stored in SQL or
 * cached across operations: atomic replacement changes the active key while
 * keys retained in the file continue to decode existing field envelopes.
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog/catalog_access.h"
#include "catalog/credentials.h"
#include "catalog/dependency.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type_d.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/uuid.h"

#define CREDENTIAL_KEYFILE_LIMIT (16 * 1024)
#define CREDENTIAL_KEY_LIMIT 64
#define CREDENTIAL_KEY_ID_LIMIT 63
#define CREDENTIAL_KEY_HEX_LENGTH 64
#define CREDENTIAL_CIPHERTEXT_LIMIT (512 * 1024)

typedef struct CredentialKey
{
	char id[CREDENTIAL_KEY_ID_LIMIT + 1];
	char secret[CREDENTIAL_KEY_HEX_LENGTH + 1];
} CredentialKey;

struct SynchdbCredentials
{
	MemoryContext context;
	MemoryContextCallback cleanup;
	Oid encrypt_oid;
	Oid decrypt_oid;
	int count;
	int active;
	CredentialKey keys[CREDENTIAL_KEY_LIMIT];
	char file_buffer[CREDENTIAL_KEYFILE_LIMIT + 1];
};

static char *credential_keyfile;
static const char *const credential_fields[] = {
	"pwd", "ssl_keystore_pass", "ssl_truststore_pass",
	"jmx_ssl_keystore_pass", "jmx_ssl_truststore_pass", "fdw_ssl_cipher"
};

PG_FUNCTION_INFO_V1(synchdb_connector_credential);
PG_FUNCTION_INFO_V1(synchdb_reencrypt_credentials);

void
synchdb_credentials_register_guc(void)
{
	DefineCustomStringVariable("synchdb.credential_keyfile",
							   "Absolute path to a server-owned connector credential keyring.",
							   NULL, &credential_keyfile, "", PGC_SIGHUP,
							   GUC_SUPERUSER_ONLY, NULL, NULL, NULL);
}

static void
credential_cleanup(void *argument)
{
	SynchdbCredentials *credentials = argument;

	explicit_bzero(credentials->keys, sizeof(credentials->keys));
	explicit_bzero(credentials->file_buffer, sizeof(credentials->file_buffer));
}

static void
credential_file_invalid(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid connector credential keyring"),
			 errhint("Use a version 1 keyring with one active key and distinct key identifiers.")));
}

static bool
credential_id_valid(const char *id)
{
	Size length = strlen(id);
	Size i;

	if (length == 0 || length > CREDENTIAL_KEY_ID_LIMIT)
		return false;
	for (i = 0; i < length; i++)
		if (!((id[i] >= 'a' && id[i] <= 'z') ||
			  (id[i] >= 'A' && id[i] <= 'Z') ||
			  (id[i] >= '0' && id[i] <= '9') ||
			  id[i] == '_' || id[i] == '-'))
			return false;
	return true;
}

static bool
credential_hex_valid(const char *value, Size length)
{
	Size i;

	for (i = 0; i < length; i++)
		if (!((value[i] >= '0' && value[i] <= '9') ||
			  (value[i] >= 'a' && value[i] <= 'f')))
			return false;
	return true;
}

static void
credential_read_keyring(SynchdbCredentials *credentials)
{
	struct stat before;
	struct stat after;
	int descriptor;
	Size length = 0;
	char active[CREDENTIAL_KEY_ID_LIMIT + 1] = {0};
	char *line;
	char *next;
	char *end;
	bool header = false;
	int i;

	if (!credential_keyfile || !is_absolute_path(credential_keyfile))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("connector credential keyring is not configured"),
				 errhint("Set synchdb.credential_keyfile to an absolute server-owned keyring path and reload configuration.")));
	descriptor = OpenTransientFile(credential_keyfile,
								 O_RDONLY | PG_BINARY | O_NOFOLLOW | O_NONBLOCK);
	if (descriptor < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open connector credential keyring: %m")));
	if (fstat(descriptor, &before) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not inspect connector credential keyring: %m")));
	if (!S_ISREG(before.st_mode) || before.st_uid != geteuid() ||
		(before.st_mode & 07777) != 0600 || before.st_size <= 0 ||
		before.st_size > CREDENTIAL_KEYFILE_LIMIT)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("connector credential keyring must be a server-owned regular file with mode 0600 and at most 16 KiB")));
	while (length < (Size) before.st_size)
	{
		ssize_t count;

		CHECK_FOR_INTERRUPTS();
		count = read(descriptor, credentials->file_buffer + length,
					 (Size) before.st_size - length);
		if (count < 0 && errno == EINTR)
			continue;
		if (count <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not read complete connector credential keyring")));
		length += count;
	}
	if (fstat(descriptor, &after) != 0 ||
		before.st_size != after.st_size || before.st_mode != after.st_mode ||
		before.st_uid != after.st_uid || before.st_mtime != after.st_mtime ||
		before.st_ctime != after.st_ctime)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("connector credential keyring changed while being read")));
	if (CloseTransientFile(descriptor) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close connector credential keyring: %m")));
	if (memchr(credentials->file_buffer, '\0', length) != NULL)
		credential_file_invalid();
	credentials->file_buffer[length] = '\0';
	line = credentials->file_buffer;
	end = line + length;
	while (line < end)
	{
		next = memchr(line, '\n', end - line);
		if (next)
			*next++ = '\0';
		else
			next = end;
		if (line[0] == '\0')
			credential_file_invalid();
		if (!header)
		{
			if (strcmp(line, "synchdb-keyring-v1") != 0)
				credential_file_invalid();
			header = true;
		}
		else if (strncmp(line, "active ", 7) == 0)
		{
			if (active[0] || !credential_id_valid(line + 7))
				credential_file_invalid();
			strlcpy(active, line + 7, sizeof(active));
		}
		else if (strncmp(line, "key ", 4) == 0)
		{
			char *secret = strchr(line + 4, ' ');
			CredentialKey *key;

			if (!secret || credentials->count == CREDENTIAL_KEY_LIMIT)
				credential_file_invalid();
			*secret++ = '\0';
			if (!credential_id_valid(line + 4) ||
				strlen(secret) != CREDENTIAL_KEY_HEX_LENGTH ||
				!credential_hex_valid(secret, CREDENTIAL_KEY_HEX_LENGTH))
				credential_file_invalid();
			for (i = 0; i < credentials->count; i++)
				if (strcmp(credentials->keys[i].id, line + 4) == 0 ||
					strcmp(credentials->keys[i].secret, secret) == 0)
					credential_file_invalid();
			key = &credentials->keys[credentials->count++];
			strlcpy(key->id, line + 4, sizeof(key->id));
			strlcpy(key->secret, secret, sizeof(key->secret));
		}
		else
			credential_file_invalid();
		line = next;
	}
	credentials->active = -1;
	for (i = 0; i < credentials->count; i++)
		if (strcmp(credentials->keys[i].id, active) == 0)
			credentials->active = i;
	if (!header || credentials->active < 0)
		credential_file_invalid();
	explicit_bzero(credentials->file_buffer, sizeof(credentials->file_buffer));
}

static Oid
credential_crypto_function(const char *name, Oid input_type)
{
	Oid extension_oid = get_extension_oid("pgcrypto", false);
	Oid namespace_oid = get_extension_schema(extension_oid);
	Oid argtypes[2] = {input_type, TEXTOID};
	char *namespace_name = get_namespace_name(namespace_oid);
	Oid function_oid = LookupFuncName(list_make2(makeString(namespace_name),
											 makeString(pstrdup(name))),
								2, argtypes, false);
	AclResult acl;

	if (getExtensionOfObject(ProcedureRelationId, function_oid) != extension_oid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("credential function is not owned by pgcrypto")));
	acl = object_aclcheck(NamespaceRelationId, namespace_oid, GetUserId(), ACL_USAGE);
	if (acl != ACLCHECK_OK)
		aclcheck_error(acl, OBJECT_SCHEMA, namespace_name);
	acl = object_aclcheck(ProcedureRelationId, function_oid, GetUserId(), ACL_EXECUTE);
	if (acl != ACLCHECK_OK)
		aclcheck_error(acl, OBJECT_FUNCTION, name);
	return function_oid;
}

SynchdbCredentials *
synchdb_credentials_open(void)
{
	MemoryContext context = AllocSetContextCreate(CurrentMemoryContext,
												 "SynchDB credential keyring",
												 ALLOCSET_SMALL_SIZES);
	MemoryContext prior = MemoryContextSwitchTo(context);
	SynchdbCredentials *credentials = palloc0(sizeof(*credentials));

	credentials->context = context;
	credentials->cleanup.func = credential_cleanup;
	credentials->cleanup.arg = credentials;
	MemoryContextRegisterResetCallback(context, &credentials->cleanup);
	credential_read_keyring(credentials);
	MemoryContextSwitchTo(prior);
	return credentials;
}

void
synchdb_credentials_close(SynchdbCredentials *credentials)
{
	if (credentials)
		MemoryContextDelete(credentials->context);
}

static void
credential_json_key(JsonbParseState **state, const char *key)
{
	JsonbValue value;

	value.type = jbvString;
	value.val.string.val = (char *) key;
	value.val.string.len = strlen(key);
	pushJsonbValue(state, WJB_KEY, &value);
}

static void
credential_json_string(JsonbParseState **state, const char *string)
{
	JsonbValue value;

	value.type = jbvString;
	value.val.string.val = (char *) string;
	value.val.string.len = strlen(string);
	pushJsonbValue(state, WJB_VALUE, &value);
}

/* Direct fmgr calls keep passphrases out of SPI SQL parameters and contexts. */
static Datum
credential_crypto_call(Oid function_oid, Datum input, CredentialKey *key)
{
	text *passphrase = cstring_to_text(key->secret);
	Datum result = (Datum) 0;

	PG_TRY();
	{
		result = OidFunctionCall2(function_oid, input, PointerGetDatum(passphrase));
	}
	PG_CATCH();
	{
		explicit_bzero(VARDATA(passphrase), VARSIZE(passphrase) - VARHDRSZ);
		PG_RE_THROW();
	}
	PG_END_TRY();
	explicit_bzero(VARDATA(passphrase), VARSIZE(passphrase) - VARHDRSZ);
	pfree(passphrase);
	return result;
}

Jsonb *
synchdb_credentials_encrypt(SynchdbCredentials *credentials, const char *plaintext)
{
	JsonbParseState *state = NULL;
	JsonbValue value;
	CredentialKey *key = &credentials->keys[credentials->active];
	text *input;
	bytea *encrypted = NULL;
	char *hex;

	if (!OidIsValid(credentials->encrypt_oid))
		credentials->encrypt_oid = credential_crypto_function("pgp_sym_encrypt", TEXTOID);
	input = cstring_to_text(plaintext);
	PG_TRY();
	{
		encrypted = DatumGetByteaP(credential_crypto_call(credentials->encrypt_oid,
															 PointerGetDatum(input), key));
	}
	PG_CATCH();
	{
		explicit_bzero(VARDATA(input), VARSIZE(input) - VARHDRSZ);
		PG_RE_THROW();
	}
	PG_END_TRY();
	explicit_bzero(VARDATA(input), VARSIZE(input) - VARHDRSZ);
	pfree(input);
	hex = palloc(2 * (VARSIZE_ANY_EXHDR(encrypted)) + 1);
	hex_encode(VARDATA_ANY(encrypted), VARSIZE_ANY_EXHDR(encrypted), hex);
	hex[2 * VARSIZE_ANY_EXHDR(encrypted)] = '\0';
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	credential_json_key(&state, "version");
	value.type = jbvNumeric;
	value.val.numeric = int64_to_numeric(1);
	pushJsonbValue(&state, WJB_VALUE, &value);
	credential_json_key(&state, "key_id");
	credential_json_string(&state, key->id);
	credential_json_key(&state, "ciphertext");
	credential_json_string(&state, hex);
	return JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
}

static JsonbValue *
credential_json_member(Jsonb *envelope, const char *name)
{
	JsonbValue key;

	key.type = jbvString;
	key.val.string.val = (char *) name;
	key.val.string.len = strlen(name);
	return findJsonbValueFromContainer(&envelope->root, JB_FOBJECT, &key);
}

char *
synchdb_credentials_decrypt(SynchdbCredentials *credentials, Jsonb *envelope)
{
	JsonbValue *version;
	JsonbValue *key_id;
	JsonbValue *ciphertext;
	CredentialKey *key = NULL;
	bytea *encrypted;
	text *decrypted;
	char *result;
	int i;

	if (!JB_ROOT_IS_OBJECT(envelope) || JB_ROOT_COUNT(envelope) != 3)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid connector credential envelope")));
	version = credential_json_member(envelope, "version");
	key_id = credential_json_member(envelope, "key_id");
	ciphertext = credential_json_member(envelope, "ciphertext");
	if (!version || version->type != jbvNumeric ||
		!DatumGetBool(DirectFunctionCall2(numeric_eq,
								 NumericGetDatum(version->val.numeric),
								 NumericGetDatum(int64_to_numeric(1)))) ||
		!key_id || key_id->type != jbvString ||
		!ciphertext || ciphertext->type != jbvString ||
		ciphertext->val.string.len == 0 ||
		ciphertext->val.string.len > CREDENTIAL_CIPHERTEXT_LIMIT ||
		(ciphertext->val.string.len % 2) != 0 ||
		!credential_hex_valid(ciphertext->val.string.val, ciphertext->val.string.len))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported or invalid connector credential envelope")));
	for (i = 0; i < credentials->count; i++)
		if ((Size) key_id->val.string.len == strlen(credentials->keys[i].id) &&
			memcmp(key_id->val.string.val, credentials->keys[i].id,
				   key_id->val.string.len) == 0)
			key = &credentials->keys[i];
	if (!key)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("connector credential requires a key absent from the configured keyring")));
	if (!OidIsValid(credentials->decrypt_oid))
		credentials->decrypt_oid = credential_crypto_function("pgp_sym_decrypt", BYTEAOID);
	encrypted = palloc(VARHDRSZ + ciphertext->val.string.len / 2);
	SET_VARSIZE(encrypted, VARHDRSZ + ciphertext->val.string.len / 2);
	hex_decode(ciphertext->val.string.val, ciphertext->val.string.len, VARDATA(encrypted));
	decrypted = DatumGetTextP(credential_crypto_call(credentials->decrypt_oid,
													 PointerGetDatum(encrypted), key));
	result = text_to_cstring(decrypted);
	explicit_bzero(VARDATA_ANY(decrypted), VARSIZE_ANY_EXHDR(decrypted));
	pfree(decrypted);
	return result;
}

Datum
synchdb_connector_credential(PG_FUNCTION_ARGS)
{
	char *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *field = text_to_cstring(PG_GETARG_TEXT_PP(1));
	MemoryContext caller = CurrentMemoryContext;
	SynchdbCredentials *credentials;
	Oid types[2] = {TEXTOID, TEXTOID};
	Datum values[2] = {CStringGetTextDatum(name), CStringGetTextDatum(field)};
	Datum value;
	char *relation;
	char *query;
	char *plaintext;
	text *result;
	bool isnull;
	bool allowed = false;
	int i;

	for (i = 0; i < lengthof(credential_fields); i++)
		allowed |= strcmp(field, credential_fields[i]) == 0;
	if (!allowed)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported connector credential field")));
	synchdb_store_connect();
	relation = synchdb_store_relation("synchdb_conninfo", AccessShareLock);
	query = psprintf("SELECT pg_catalog.jsonb_extract_path(data,$2) FROM %s "
					 "WHERE name COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) $1 COLLATE pg_catalog.\"C\"",
					 relation);
	if (SPI_execute_with_args(query, 2, types, values, NULL, true, 2) != SPI_OK_SELECT)
		elog(ERROR, "could not read connector credential");
	if (SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("connector credential requires exactly one connector")));
	value = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull || JB_ROOT_IS_SCALAR(DatumGetJsonbP(value)))
	{
		/* SQL NULL/missing and the JSON null optional-field representation. */
		if (!isnull)
		{
			JsonbValue scalar;

			JsonbExtractScalar(&DatumGetJsonbP(value)->root, &scalar);
			if (scalar.type != jbvNull)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid connector credential envelope")));
		}
		synchdb_store_finish();
		PG_RETURN_NULL();
	}
	credentials = synchdb_credentials_open();
	plaintext = synchdb_credentials_decrypt(credentials, DatumGetJsonbP(value));
	MemoryContextSwitchTo(caller);
	result = cstring_to_text(plaintext);
	explicit_bzero(plaintext, strlen(plaintext));
	synchdb_credentials_close(credentials);
	synchdb_store_finish();
	PG_RETURN_TEXT_P(result);
}

/* Atomically replace every present secret, preserving all other JSON fields. */
Datum
synchdb_reencrypt_credentials(PG_FUNCTION_ARGS)
{
	SynchdbCredentials *credentials;
	MemoryContext work;
	MemoryContext caller = CurrentMemoryContext;
	char *relation;
	char *query;
	char *update;
	SPIPlanPtr plan;
	Portal portal;
	int64 updated = 0;
	Oid types[2] = {UUIDOID, JSONBOID};

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("credential re-encryption requires superuser")));
	work = AllocSetContextCreate(caller, "SynchDB credential re-encryption row",
								 ALLOCSET_SMALL_SIZES);
	synchdb_store_connect();
	/* Serialize all connector writers until the caller commits or aborts. */
	relation = synchdb_store_relation("synchdb_conninfo", ShareRowExclusiveLock);
	/* Pin only after the same lock order used by ordinary secret writers. */
	credentials = synchdb_credentials_open();
	query = psprintf("SELECT connector_id,data FROM %s", relation);
	update = psprintf("UPDATE %s SET data=$2 WHERE connector_id OPERATOR(pg_catalog.=) $1", relation);
	plan = SPI_prepare(query, 0, NULL);
	if (!plan)
		elog(ERROR, "could not prepare credential re-encryption scan");
	portal = SPI_cursor_open(NULL, plan, NULL, NULL, false);
	for (;;)
	{
		HeapTuple tuple;
		TupleDesc descriptor;
		SPITupleTable *tuptable;
		Datum id;
		Datum data;
		bool isnull;
		Jsonb *document;
		JsonbParseState *state = NULL;
		Jsonb *patch;
		Jsonb *replacement;
		Datum values[2];
		bool changed = false;
		int i;

		CHECK_FOR_INTERRUPTS();
		SPI_cursor_fetch(portal, true, 1);
		if (SPI_processed == 0)
			break;
		tuple = SPI_tuptable->vals[0];
		descriptor = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		MemoryContextSwitchTo(work);
		id = SPI_getbinval(tuple, descriptor, 1, &isnull);
		if (isnull)
			elog(ERROR, "connector identity is NULL during credential re-encryption");
		values[0] = UUIDPGetDatum(palloc(sizeof(pg_uuid_t)));
		memcpy(DatumGetUUIDP(values[0]), DatumGetUUIDP(id), sizeof(pg_uuid_t));
		data = SPI_getbinval(tuple, descriptor, 2, &isnull);
		if (isnull)
			elog(ERROR, "connector data is NULL during credential re-encryption");
		document = DatumGetJsonbPCopy(data);
		SPI_freetuptable(tuptable);
		if (!JB_ROOT_IS_OBJECT(document))
			elog(ERROR, "connector data is not an object during credential re-encryption");
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		for (i = 0; i < lengthof(credential_fields); i++)
		{
			JsonbValue *secret = credential_json_member(document, credential_fields[i]);
			char *plaintext;
			Jsonb *encrypted;
			JsonbValue value;

			if (!secret || secret->type == jbvNull)
				continue;
			plaintext = synchdb_credentials_decrypt(credentials, JsonbValueToJsonb(secret));
			encrypted = synchdb_credentials_encrypt(credentials, plaintext);
			explicit_bzero(plaintext, strlen(plaintext));
			credential_json_key(&state, credential_fields[i]);
			JsonbToJsonbValue(encrypted, &value);
			pushJsonbValue(&state, WJB_VALUE, &value);
			changed = true;
		}
		patch = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
		if (changed)
		{
			replacement = DatumGetJsonbP(DirectFunctionCall2(jsonb_concat,
															 JsonbPGetDatum(document), JsonbPGetDatum(patch)));
			values[1] = JsonbPGetDatum(replacement);
			if (SPI_execute_with_args(update, 2, types, values, NULL, false, 0) != SPI_OK_UPDATE ||
				SPI_processed != 1)
				elog(ERROR, "credential re-encryption did not update exactly one connector");
			updated++;
		}
		MemoryContextSwitchTo(caller);
		MemoryContextReset(work);
	}
	SPI_cursor_close(portal);
	synchdb_credentials_close(credentials);
	synchdb_store_finish();
	MemoryContextDelete(work);
	PG_RETURN_INT64(updated);
}
