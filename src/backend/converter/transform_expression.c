/*
 * SQL templates contain SQL; source values are always SPI parameters.
 * PostgreSQL's scanner owns string, identifier and comment recognition.
 */
#include "postgres.h"

#include <limits.h>

#include "access/xact.h"
#include "catalog/pg_type_d.h"
#include "converter/transform_expression.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parser.h"
#include "parser/scanner.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

/* scanner.h guarantees this shared prefix of the core token numbering.
 * Verified against REL_16_STABLE, REL_17_STABLE and REL_18_STABLE gram.y.
 * gram.h is generated for the SQL parser and is not an installed header. */
enum TransformTokenCode
{
	TRANSFORM_IDENT = 258,
	TRANSFORM_UIDENT,
	TRANSFORM_FCONST,
	TRANSFORM_SCONST,
	TRANSFORM_USCONST,
	TRANSFORM_BCONST,
	TRANSFORM_XCONST,
	TRANSFORM_OP,
	TRANSFORM_ICONST,
	TRANSFORM_PARAM,
	TRANSFORM_LAST_SPECIAL = 274
};

typedef struct TransformToken
{
	int code;
	int start;
	int end;
	char *value;
} TransformToken;

typedef struct TransformParameters
{
	const char *data;
	const char *wkb;
	const char *srid;
	Oid target_type;
	Oid *types;
	Datum *values;
	char *nulls;
	int count;
	int capacity;
	int data_parameter;
	int wkb_parameter;
	int srid_parameter;
	bool syntax_only;
	bool reject_spatial;
} TransformParameters;

static void
transform_check_marker(const TransformParameters *parameters, char marker)
{
	if (parameters->reject_spatial && (marker == 'w' || marker == 's'))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("FDW transformations do not provide %%w or %%s parameters")));
}

static List *
transform_scan(const char *expression)
{
	core_yy_extra_type extra = {0};
	core_yyscan_t scanner;
	List *tokens = NIL;
	core_YYSTYPE value;
	YYLTYPE location;
	int code;

	scanner = scanner_init(expression, &extra, &ScanKeywords, ScanKeywordTokens);
	while ((code = core_yylex(&value, &location, scanner)) != 0)
	{
		TransformToken *token = palloc0(sizeof(*token));

		token->code = code;
		token->start = location;
		/* The scanner temporarily NUL-terminates the current token, including
		 * continued strings. Capture this before requesting any lookahead. */
		token->end = location + strlen(extra.scanbuf + location);
		if ((code >= TRANSFORM_IDENT && code <= TRANSFORM_OP) ||
			code > TRANSFORM_LAST_SPECIAL)
			token->value = pstrdup(value.str);
		if (code == TRANSFORM_PARAM)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("transformation templates must use %%d, %%w or %%s instead of SQL parameter references")));
		tokens = lappend(tokens, token);
	}
	scanner_finish(scanner);
	return tokens;
}

static bool
transform_has_marker(const char *text)
{
	const char *at;

	for (at = text; *at; at++)
		if (*at == '%' && (at[1] == 'd' || at[1] == 'w' ||
							 at[1] == 's' || at[1] == '%'))
			return true;
	return false;
}

static int
transform_parameter(TransformParameters *parameters, Oid type, Datum value,
					bool is_null)
{
	int index;

	if (parameters->count >= 65535)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("transformation expression has too many parameters")));
	if (parameters->count == parameters->capacity)
	{
		int capacity = parameters->capacity ? parameters->capacity * 2 : 8;

		if (parameters->capacity == 0)
		{
			parameters->types = palloc(sizeof(Oid) * capacity);
			parameters->values = palloc(sizeof(Datum) * capacity);
			parameters->nulls = palloc(sizeof(char) * capacity);
		}
		else
		{
			parameters->types = repalloc(parameters->types, sizeof(Oid) * capacity);
			parameters->values = repalloc(parameters->values, sizeof(Datum) * capacity);
			parameters->nulls = repalloc(parameters->nulls, sizeof(char) * capacity);
		}
		parameters->capacity = capacity;
	}
	index = parameters->count++;
	parameters->types[index] = type;
	parameters->values[index] = value;
	parameters->nulls[index] = is_null ? 'n' : ' ';
	return index + 1;
}

static int
transform_bare_parameter(TransformParameters *parameters, char marker)
{
	const char *text;
	Oid type;
	Oid input;
	Oid input_parameter;
	Datum value = (Datum) 0;
	int *cached;

	transform_check_marker(parameters, marker);
	switch (marker)
	{
		case 'd':
			text = parameters->data;
			type = parameters->syntax_only ? TEXTOID :
				getBaseType(parameters->target_type);
			cached = &parameters->data_parameter;
			break;
		case 'w':
			text = parameters->wkb;
			type = TEXTOID;
			cached = &parameters->wkb_parameter;
			break;
		case 's':
			text = parameters->srid;
			type = INT4OID;
			cached = &parameters->srid_parameter;
			break;
		default:
			elog(ERROR, "unknown transformation parameter");
			return 0;
	}
	if (*cached != 0)
		return *cached;
	if (!OidIsValid(type))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("transformation expression requires a valid target type")));
	if (text != NULL)
	{
		getTypeInputInfo(type, &input, &input_parameter);
		/* Column typmod/domain checks belong after transformation. */
		value = OidInputFunctionCall(input, (char *) text, input_parameter, -1);
	}
	*cached = transform_parameter(parameters, type, value, text == NULL);
	return *cached;
}

/* String contents have already been decoded by PostgreSQL, including escapes
 * and concatenation across newlines. They never become SQL source again. */
static int
transform_string_parameter(const char *text, TransformParameters *parameters)
{
	StringInfoData result;
	bool is_null = false;
	const char *at;

	initStringInfo(&result);
	for (at = text; *at; at++)
	{
		const char *replacement = NULL;
		bool substitute = false;

		if (*at == '%')
		{
			switch (at[1])
			{
				case 'd': replacement = parameters->data; substitute = true; break;
				case 'w': replacement = parameters->wkb; substitute = true; break;
				case 's': replacement = parameters->srid; substitute = true; break;
				case '%':
					appendStringInfoChar(&result, '%');
					at++;
					continue;
				default:
					break;
			}
		}
		if (substitute)
		{
			transform_check_marker(parameters, at[1]);
			if (replacement == NULL)
				is_null = true;
			else
				appendStringInfoString(&result, replacement);
			at++;
		}
		else
			appendStringInfoChar(&result, *at);
	}
	return transform_parameter(parameters, TEXTOID,
							   is_null ? (Datum) 0 : CStringGetTextDatum(result.data),
							   is_null);
}

/* Let the SQL parser decode U& strings/identifiers and any UESCAPE clause. */
static char *
transform_unicode_value(const char *expression, int start, int end,
						bool identifier)
{
	char *query = psprintf("SELECT %.*s", end - start, expression + start);
	List *statements = raw_parser(query, RAW_PARSE_DEFAULT);
	RawStmt *raw;
	SelectStmt *select;
	ResTarget *target;

	if (list_length(statements) != 1)
		elog(ERROR, "invalid Unicode transformation token");
	raw = linitial_node(RawStmt, statements);
	if (!IsA(raw->stmt, SelectStmt))
		elog(ERROR, "invalid Unicode transformation token");
	select = castNode(SelectStmt, raw->stmt);
	if (list_length(select->targetList) != 1)
		elog(ERROR, "invalid Unicode transformation token");
	target = linitial_node(ResTarget, select->targetList);
	if (identifier && IsA(target->val, ColumnRef))
	{
		ColumnRef *column = castNode(ColumnRef, target->val);

		if (list_length(column->fields) == 1 && IsA(linitial(column->fields), String))
			return pstrdup(strVal(linitial(column->fields)));
	}
	else if (!identifier && IsA(target->val, A_Const))
	{
		A_Const *constant = castNode(A_Const, target->val);

		if (!constant->isnull && IsA(&constant->val, String))
			return pstrdup(strVal(&constant->val));
	}
	elog(ERROR, "invalid Unicode transformation token");
	return NULL;
}

static char *
transform_compile(const char *expression, TransformParameters *parameters)
{
	List *tokens = transform_scan(expression);
	StringInfoData query;
	int copied = 0;
	int index;
	int count = list_length(tokens);

	initStringInfo(&query);
	appendStringInfoString(&query, "SELECT ");
	for (index = 0; index < count; index++)
	{
		TransformToken *token = list_nth(tokens, index);
		int end = token->end;
		char *text = token->value;
		bool replaced = false;
		int consumed = index;

		if (token->code == TRANSFORM_USCONST || token->code == TRANSFORM_UIDENT)
		{
			if (index + 2 < count)
			{
				TransformToken *escape = list_nth(tokens, index + 1);
				TransformToken *character = list_nth(tokens, index + 2);

				if (escape->value != NULL && pg_strcasecmp(escape->value, "uescape") == 0)
				{
					if (character->code != TRANSFORM_SCONST)
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
										errmsg("invalid Unicode escape character")));
					end = character->end;
					consumed = index + 2;
				}
			}
			text = transform_unicode_value(expression, token->start, end,
										   token->code == TRANSFORM_UIDENT);
		}
		if (token->code == TRANSFORM_SCONST || token->code == TRANSFORM_USCONST)
		{
			if (transform_has_marker(text))
			{
				int parameter = transform_string_parameter(text, parameters);

				appendBinaryStringInfo(&query, expression + copied, token->start - copied);
				appendStringInfo(&query, "($%d)", parameter);
				replaced = true;
			}
		}
		else if (token->code == TRANSFORM_IDENT || token->code == TRANSFORM_UIDENT ||
				 token->code == TRANSFORM_BCONST || token->code == TRANSFORM_XCONST)
		{
			char *original = pnstrdup(expression + token->start, token->end - token->start);

			/* Identifier decoding can truncate to NAMEDATALEN. Inspect the
			 * original spelling as well so a marker cannot disappear there. */
			if (transform_has_marker(text) || transform_has_marker(original))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("transformation placeholders are not supported in identifiers or bit strings")));
			pfree(original);
		}
		else if (token->code == '%' || token->code == TRANSFORM_OP)
		{
			int position;
			StringInfoData operator;

			initStringInfo(&operator);
			for (position = token->start; position < token->end; position++)
			{
				if (expression[position] == '%' && position + 1 < token->end &&
					expression[position + 1] == '%')
				{
					appendStringInfoChar(&operator, '%');
					position++;
					replaced = true;
				}
				else if (expression[position] == '%' && position + 1 == token->end &&
						 index + 1 < count)
				{
					TransformToken *next = list_nth(tokens, index + 1);
					char marker = expression[token->end];

					if (next->start == token->end &&
						(marker == 'd' || marker == 'w' || marker == 's'))
					{
						if (next->code != TRANSFORM_IDENT || next->end != next->start + 1)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("transformation placeholder must be a complete token")));
						appendStringInfo(&operator, "($%d)",
										 transform_bare_parameter(parameters, marker));
						end = next->end;
						consumed = index + 1;
						replaced = true;
					}
					else
						appendStringInfoChar(&operator, expression[position]);
				}
				else
					appendStringInfoChar(&operator, expression[position]);
			}
			if (replaced)
			{
				appendBinaryStringInfo(&query, expression + copied, token->start - copied);
				appendStringInfoString(&query, operator.data);
			}
			pfree(operator.data);
		}
		if (replaced)
			copied = end;
		index = consumed;
	}
	appendStringInfoString(&query, expression + copied);
	return query.data;
}

static bool
transform_has_write(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, InsertStmt) || IsA(node, UpdateStmt) ||
		IsA(node, DeleteStmt) || IsA(node, MergeStmt) ||
		(IsA(node, SelectStmt) && castNode(SelectStmt, node)->intoClause != NULL))
		return true;
	return raw_expression_tree_walker(node, transform_has_write, context);
}

static void
transform_check_query(const char *query)
{
	List *statements = raw_parser(query, RAW_PARSE_DEFAULT);
	RawStmt *raw;

	if (list_length(statements) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("data transformation must contain a single SELECT statement")));
	raw = linitial_node(RawStmt, statements);
	if (!IsA(raw->stmt, SelectStmt) || transform_has_write(raw->stmt, NULL))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("data transformation must contain a single SELECT statement")));
}

static void
transform_validate_template(const char *expression, bool reject_spatial)
{
	MemoryContext caller = CurrentMemoryContext;
	MemoryContext scratch;

	if (expression == NULL || expression[0] == '\0' || strlen(expression) > INT_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid data transformation expression")));
	scratch = AllocSetContextCreate(caller, "synchdb transform validation",
								   ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(scratch);
	PG_TRY();
	{
		TransformParameters parameters = {0};
		char *query;

		parameters.syntax_only = true;
		parameters.reject_spatial = reject_spatial;
		parameters.target_type = TEXTOID;
		query = transform_compile(expression, &parameters);
		transform_check_query(query);
	}
	PG_FINALLY();
	{
		MemoryContextSwitchTo(caller);
		MemoryContextDelete(scratch);
	}
	PG_END_TRY();
}

void
synchdb_transform_validate_template(const char *expression)
{
	transform_validate_template(expression, false);
}

void
synchdb_transform_validate_fdw_template(const char *expression)
{
	transform_validate_template(expression, true);
}

char *
ra_transformDataExpression(const char *data, const char *wkb, const char *srid,
						   const char *expression, Oid target_type)
{
	MemoryContext caller = CurrentMemoryContext;
	MemoryContext scratch;
	char *volatile result = NULL;
	volatile bool connected = false;

	if (!IsTransactionState())
		ereport(ERROR,
				(errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
				 errmsg("data transformation requires a caller-owned transaction")));
	if (expression == NULL || strlen(expression) > INT_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid data transformation expression")));
	scratch = AllocSetContextCreate(caller, "synchdb transform expression",
								   ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(scratch);
	PG_TRY();
	{
		TransformParameters parameters = {0};
		char *query;
		int status;
		char *value;

		parameters.data = data;
		parameters.wkb = wkb;
		parameters.srid = srid;
		parameters.target_type = target_type;
		query = transform_compile(expression, &parameters);
		transform_check_query(query);
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "could not connect to SPI for data transformation");
		connected = true;
		status = SPI_execute_with_args(query, parameters.count, parameters.types,
									   parameters.values, parameters.nulls, true, 2);
		if (status != SPI_OK_SELECT)
			elog(ERROR, "could not execute data transformation: %d", status);
		if (SPI_processed == 0)
			ereport(ERROR,
					(errcode(ERRCODE_NO_DATA_FOUND),
					 errmsg("data transform expression did not return a row")));
		if (SPI_processed != 1 || SPI_tuptable == NULL ||
			SPI_tuptable->tupdesc->natts != 1)
			ereport(ERROR,
					(errcode(ERRCODE_CARDINALITY_VIOLATION),
					 errmsg("data transform expression must return exactly one row and one column")));
		value = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		if (value != NULL)
			result = MemoryContextStrdup(caller, value);
		if (SPI_finish() != SPI_OK_FINISH)
			elog(ERROR, "could not finish SPI data transformation");
		connected = false;
	}
	PG_FINALLY();
	{
		if (connected)
			(void) SPI_finish();
		MemoryContextSwitchTo(caller);
		MemoryContextDelete(scratch);
	}
	PG_END_TRY();
	return result;
}
