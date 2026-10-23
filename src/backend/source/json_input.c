#include "postgres.h"

#include <limits.h>

#include "mb/pg_wchar.h"
#include "source/json_input.h"
#include "utils/builtins.h"

Jsonb *
synchdb_source_json_from_utf8(const char *json)
{
	Size length;
	char *server;
	Jsonb *result;

	if (json == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("source JSON input must not be NULL")));
	length = strlen(json);
	if (length > INT_MAX)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("source JSON input is too large")));
	(void) pg_verify_mbstr(PG_UTF8, json, length, false);
	server = pg_any_to_server(json, length, PG_UTF8);
	result = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(server)));
	if (server != json)
		pfree(server);
	return result;
}
