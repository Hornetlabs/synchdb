/*
 * Connector credentials retain their per-operation key collection and cleanup,
 * initialized in code with the 1.0 passphrase.  Write the 1.0 bytea JSON string
 * format as well, so both versions can read credentials written by the other.
 */
#include "postgres.h"

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
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/uuid.h"

#define CREDENTIAL_KEY_LIMIT 64
#define CREDENTIAL_KEY_ID_LIMIT 63
#define CREDENTIAL_KEY_SECRET_LIMIT 64
#define CREDENTIAL_CIPHERTEXT_LIMIT (512 * 1024)

/* The 1.0 SYNCHDB_SECRET, used verbatim as a text passphrase, not hex bytes. */
#define CREDENTIAL_LEGACY_SECRET "930e62fb8c40086c23f543357a023c0c"

typedef struct CredentialKey
{
	char id[CREDENTIAL_KEY_ID_LIMIT + 1];
	char secret[CREDENTIAL_KEY_SECRET_LIMIT + 1];
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
};

static const char *const credential_fields[] = {
	"pwd", "ssl_keystore_pass", "ssl_truststore_pass",
	"jmx_ssl_keystore_pass", "jmx_ssl_truststore_pass", "fdw_ssl_cipher"
};

PG_FUNCTION_INFO_V1(synchdb_connector_credential);
PG_FUNCTION_INFO_V1(synchdb_reencrypt_credentials);

static void
credential_cleanup(void *argument)
{
	SynchdbCredentials *credentials = argument;

	explicit_bzero(credentials->keys, sizeof(credentials->keys));
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
credential_initialize(SynchdbCredentials *credentials)
{
	credentials->count = 1;
	credentials->active = 0;
	strlcpy(credentials->keys[0].id, "legacy", sizeof(credentials->keys[0].id));
	strlcpy(credentials->keys[0].secret, CREDENTIAL_LEGACY_SECRET,
			sizeof(credentials->keys[0].secret));
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
												 "SynchDB credentials",
												 ALLOCSET_SMALL_SIZES);
	MemoryContext prior = MemoryContextSwitchTo(context);
	SynchdbCredentials *credentials = palloc0(sizeof(*credentials));

	credentials->context = context;
	credentials->cleanup.func = credential_cleanup;
	credentials->cleanup.arg = credentials;
	MemoryContextRegisterResetCallback(context, &credentials->cleanup);
	credential_initialize(credentials);
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
	JsonbValue value;
	Jsonb *result;
	CredentialKey *key = &credentials->keys[credentials->active];
	text *input;
	bytea *encrypted = NULL;
	char *encoded;

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
	/* Match jsonb_build_object(..., pgp_sym_encrypt(...)) in 1.0, including
	 * bytea_output=escape.  PGP ciphertext itself is randomized. */
	encoded = DatumGetCString(DirectFunctionCall1(byteaout, PointerGetDatum(encrypted)));
	value.type = jbvString;
	value.val.string.val = encoded;
	value.val.string.len = strlen(encoded);
	result = JsonbValueToJsonb(&value);
	pfree(encoded);
	pfree(encrypted);
	return result;
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

/* Retain the v1 reader for envelopes using a key in the current collection. */
static bytea *
credential_decode_envelope(SynchdbCredentials *credentials, Jsonb *envelope,
						   CredentialKey **key)
{
	JsonbValue *version;
	JsonbValue *key_id;
	JsonbValue *ciphertext;
	bytea *encrypted;
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
			*key = &credentials->keys[i];
	if (!*key)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("connector credential requires an unavailable key"),
				 errhint("Re-enter credentials previously encrypted with an external key.")));
	encrypted = palloc(VARHDRSZ + ciphertext->val.string.len / 2);
	SET_VARSIZE(encrypted, VARHDRSZ + ciphertext->val.string.len / 2);
	hex_decode(ciphertext->val.string.val, ciphertext->val.string.len, VARDATA(encrypted));
	return encrypted;
}

char *
synchdb_credentials_decrypt(SynchdbCredentials *credentials, Jsonb *stored)
{
	CredentialKey *key = NULL;
	bytea *encrypted;
	text *decrypted;
	char *result;

	if (JB_ROOT_IS_SCALAR(stored))
	{
		JsonbValue scalar;
		char *encoded;

		JsonbExtractScalar(&stored->root, &scalar);
		if (scalar.type != jbvString || scalar.val.string.len == 0 ||
			scalar.val.string.len > CREDENTIAL_CIPHERTEXT_LIMIT)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid connector credential ciphertext")));
		/* The 1.0 reader casts data->>'field' to bytea.  byteain accepts
		 * both hex and escape output without treating plaintext as a secret. */
		encoded = pnstrdup(scalar.val.string.val, scalar.val.string.len);
		encrypted = DatumGetByteaP(DirectFunctionCall1(byteain, CStringGetDatum(encoded)));
		pfree(encoded);
		key = &credentials->keys[credentials->active];
	}
	else
		encrypted = credential_decode_envelope(credentials, stored, &key);
	if (!OidIsValid(credentials->decrypt_oid))
		credentials->decrypt_oid = credential_crypto_function("pgp_sym_decrypt", BYTEAOID);
	decrypted = DatumGetTextP(credential_crypto_call(credentials->decrypt_oid,
													 PointerGetDatum(encrypted), key));
	result = text_to_cstring(decrypted);
	explicit_bzero(VARDATA_ANY(decrypted), VARSIZE_ANY_EXHDR(decrypted));
	pfree(decrypted);
	pfree(encrypted);
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
	bool absent;
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
	absent = isnull;
	if (!isnull && JB_ROOT_IS_SCALAR(DatumGetJsonbP(value)))
	{
		JsonbValue scalar;

		JsonbExtractScalar(&DatumGetJsonbP(value)->root, &scalar);
		absent = scalar.type == jbvNull;
	}
	/* SQL NULL/missing and the JSON null optional-field representation. */
	if (absent)
	{
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
