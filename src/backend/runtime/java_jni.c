/* JNI uses UTF-16 strings; modified UTF-8 is not PostgreSQL text. */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "runtime/java_jni.h"
#include "utils/memutils.h"

typedef struct SynchdbJavaEncoding
{
	MemoryContext context;
	int			encoding;
	FmgrInfo	to_utf8;
	FmgrInfo	from_utf8;
} SynchdbJavaEncoding;

/* Database encoding is fixed for a backend. No transaction-owned pointers. */
static SynchdbJavaEncoding *java_encoding;

bool
synchdb_java_exception(JNIEnv *env)
{
	if (env == NULL || !(*env)->ExceptionCheck(env))
		return false;
	(*env)->ExceptionDescribe(env);
	(*env)->ExceptionClear(env);
	return true;
}

bool
synchdb_java_local_begin(JNIEnv *env, jint capacity)
{
	jint		status;
	bool		exception;

	if (env == NULL || synchdb_java_exception(env) || capacity < 0)
		return false;
	status = (*env)->PushLocalFrame(env, capacity);
	exception = synchdb_java_exception(env);
	if (status != JNI_OK)
		return false;
	if (exception)
	{
		(*env)->PopLocalFrame(env, NULL);
		(void) synchdb_java_exception(env);
		return false;
	}
	return true;
}

/*
 * FindDefaultConversionProc/fmgr_info need a transaction, whereas worker JNI
 * operations often run between transactions. Prepare immutable function
 * metadata once, without starting or committing a caller's transaction. The
 * actual conversion below uses the cached FmgrInfo and performs no lookup.
 */
static void
java_prepare_encoding(int encoding)
{
	MemoryContext caller = CurrentMemoryContext;
	MemoryContext volatile context = NULL;
	SynchdbJavaEncoding *volatile prepared = NULL;
	bool		own_transaction;

	if (encoding == PG_UTF8 || encoding == PG_SQL_ASCII ||
		(java_encoding != NULL && java_encoding->encoding == encoding))
		return;
	own_transaction = !IsTransactionOrTransactionBlock();
	if (!own_transaction && !IsTransactionState())
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("cannot prepare Java text conversion in an aborted transaction")));

	PG_TRY();
	{
		Oid			to_utf8;
		Oid			from_utf8;
		SynchdbJavaEncoding *old;

		if (own_transaction)
			StartTransactionCommand();
		MemoryContextSwitchTo(caller);
		context = AllocSetContextCreate(TopMemoryContext,
										"synchdb Java encoding",
											ALLOCSET_SMALL_SIZES);
		prepared = MemoryContextAllocZero(context, sizeof(SynchdbJavaEncoding));
		prepared->context = context;
		prepared->encoding = encoding;
		to_utf8 = FindDefaultConversionProc(encoding, PG_UTF8);
		from_utf8 = FindDefaultConversionProc(PG_UTF8, encoding);
		if (!OidIsValid(to_utf8) || !OidIsValid(from_utf8))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("default Java text conversion for encoding \"%s\" does not exist",
							pg_encoding_to_char(encoding))));
		fmgr_info_cxt(to_utf8, &prepared->to_utf8, context);
		fmgr_info_cxt(from_utf8, &prepared->from_utf8, context);
		if (own_transaction)
			CommitTransactionCommand();
		MemoryContextSwitchTo(caller);
		old = java_encoding;
		java_encoding = prepared;
		context = NULL;
		if (old != NULL)
			MemoryContextDelete(old->context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(caller);
		if (own_transaction)
			AbortCurrentTransaction();
		MemoryContextSwitchTo(caller);
		if (context != NULL)
			MemoryContextDelete(context);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/* Result belongs to the operation's temporary context. */
static char *
java_convert(const char *value, Size length, bool to_utf8)
{
	char	   *result;
	Size		capacity;
	int			source;
	int			destination;
	FmgrInfo   *function;

	if (length > (MaxAllocSize - 1) / MAX_CONVERSION_GROWTH ||
		length > INT_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Java text is too long for encoding conversion")));
	capacity = length * MAX_CONVERSION_GROWTH + 1;
	result = palloc(capacity);
	source = to_utf8 ? java_encoding->encoding : PG_UTF8;
	destination = to_utf8 ? PG_UTF8 : java_encoding->encoding;
	function = to_utf8 ? &java_encoding->to_utf8 : &java_encoding->from_utf8;
	FunctionCall6(function,
				  Int32GetDatum(source),
				  Int32GetDatum(destination),
				  CStringGetDatum(value),
				  CStringGetDatum(result),
				  Int32GetDatum((int) length),
				  BoolGetDatum(false));
	return result;
}

jstring
synchdb_java_string(JNIEnv *env, const char *value)
{
	MemoryContext caller;
	MemoryContext scratch;
	jstring volatile result = NULL;
	int			encoding;
	Size		length;

	if (env == NULL || synchdb_java_exception(env) || value == NULL)
		return NULL;
	length = strlen(value);
	if (length > INT_MAX || length >= MaxAllocSize)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Java text is too long")));
	encoding = GetDatabaseEncoding();
	if (length > 0)
		java_prepare_encoding(encoding);
	caller = CurrentMemoryContext;
	scratch = AllocSetContextCreate(caller, "synchdb Java string",
								   ALLOCSET_SMALL_SIZES);
	PG_TRY();
	{
		const unsigned char *utf8;
		const unsigned char *cursor;
		const unsigned char *end;
		Size		units = 0;
		jchar	   *utf16;
		Size		written = 0;

		MemoryContextSwitchTo(scratch);
		if (encoding == PG_UTF8 || encoding == PG_SQL_ASCII || length == 0)
			utf8 = (const unsigned char *) value;
		else
		{
			(void) pg_verify_mbstr(encoding, value, (int) length, false);
			utf8 = (const unsigned char *) java_convert(value, length, true);
			length = strlen((const char *) utf8);
		}
		(void) pg_verify_mbstr(PG_UTF8, (const char *) utf8, (int) length, false);
		end = utf8 + length;
		for (cursor = utf8; cursor < end; cursor += pg_utf_mblen(cursor))
			units += utf8_to_unicode(cursor) > 0xFFFF ? 2 : 1;
		if (units > INT_MAX || units > MaxAllocSize / sizeof(jchar))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("Java UTF-16 string is too long")));
		utf16 = palloc(Max(units, (Size) 1) * sizeof(jchar));
		for (cursor = utf8; cursor < end; cursor += pg_utf_mblen(cursor))
		{
			pg_wchar	codepoint = utf8_to_unicode(cursor);

			if (codepoint <= 0xFFFF)
				utf16[written++] = (jchar) codepoint;
			else
			{
				codepoint -= 0x10000;
				utf16[written++] = (jchar) (0xD800 | (codepoint >> 10));
				utf16[written++] = (jchar) (0xDC00 | (codepoint & 0x3FF));
			}
		}
		result = (*env)->NewString(env, utf16, (jsize) units);
		if (synchdb_java_exception(env))
		{
			if (result != NULL)
				(*env)->DeleteLocalRef(env, result);
			result = NULL;
		}
	}
	PG_FINALLY();
	{
		MemoryContextSwitchTo(caller);
		MemoryContextDelete(scratch);
	}
	PG_END_TRY();
	return result;
}

char *
synchdb_java_string_copy(JNIEnv *env, jstring value)
{
	MemoryContext caller;
	MemoryContext scratch;
	const jchar *volatile chars = NULL;
	char *volatile result = NULL;
	jsize		length;
	int			encoding;

	if (env == NULL || synchdb_java_exception(env) || value == NULL)
		return NULL;
	encoding = GetDatabaseEncoding();
	length = (*env)->GetStringLength(env, value);
	if (synchdb_java_exception(env))
		return NULL;
	if (length < 0 || (Size) length > (MaxAllocSize - 1) / 3)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Java UTF-16 string is too long")));
	/* No JNI chars are held while a cache-preparation transaction is active. */
	if (length > 0)
		java_prepare_encoding(encoding);
	caller = CurrentMemoryContext;
	scratch = AllocSetContextCreate(caller, "synchdb Java string copy",
								   ALLOCSET_SMALL_SIZES);
	PG_TRY();
	{
		MemoryContextSwitchTo(scratch);
		chars = (*env)->GetStringChars(env, value, NULL);
		if (!synchdb_java_exception(env) && chars != NULL)
		{
			unsigned char *utf8 = palloc((Size) length * 3 + 1);
			unsigned char *cursor = utf8;
			char	   *converted;
			jsize		i;

			for (i = 0; i < length; ++i)
			{
				pg_wchar	codepoint = chars[i];

				if (codepoint == 0)
					ereport(ERROR,
							(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
							 errmsg("Java string contains a null character")));
				if (codepoint >= 0xD800 && codepoint <= 0xDBFF)
				{
					if (i + 1 >= length || chars[i + 1] < 0xDC00 ||
						chars[i + 1] > 0xDFFF)
						ereport(ERROR,
								(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
								 errmsg("Java string contains an unpaired UTF-16 surrogate")));
					codepoint = 0x10000 + ((codepoint - 0xD800) << 10) +
						(chars[++i] - 0xDC00);
				}
				else if (codepoint >= 0xDC00 && codepoint <= 0xDFFF)
					ereport(ERROR,
							(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
							 errmsg("Java string contains an unpaired UTF-16 surrogate")));
				unicode_to_utf8(codepoint, cursor);
				cursor += pg_utf_mblen(cursor);
			}
			*cursor = '\0';
			converted = (char *) utf8;
			if (length > 0 && encoding != PG_UTF8 && encoding != PG_SQL_ASCII)
				converted = java_convert(converted, cursor - utf8, false);
			MemoryContextSwitchTo(caller);
			result = pstrdup(converted);
		}
	}
	PG_FINALLY();
	{
		if (chars != NULL)
		{
			(*env)->ReleaseStringChars(env, value, chars);
			if (synchdb_java_exception(env) && result != NULL)
			{
				pfree(result);
				result = NULL;
			}
		}
		MemoryContextSwitchTo(caller);
		MemoryContextDelete(scratch);
	}
	PG_END_TRY();
	return result;
}
