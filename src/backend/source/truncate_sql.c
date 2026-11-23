/* A deliberately small source grammar, shared by Debezium and native OLR.
 * Never feed foreign SQL to PostgreSQL's identifier parser: doing so folds or
 * truncates source names before the mapping lookup can see their real bytes. */
#include "postgres.h"

#include <ctype.h>

#include "lib/stringinfo.h"
#include "source/truncate_sql.h"

static void
invalid_truncate(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unsupported source TRUNCATE statement"),
			 errdetail("One table and optional Oracle DROP [ALL] STORAGE or REUSE STORAGE are supported; multiple statements, CASCADE and RESTART IDENTITY are not supported.")));
}

static bool
skip_space(const char **cursor)
{
	for (;;)
	{
		while (isspace((unsigned char) **cursor))
			(*cursor)++;
		if (strncmp(*cursor, "--", 2) == 0)
		{
			while (**cursor && **cursor != '\n')
				(*cursor)++;
		}
		else if (strncmp(*cursor, "/*", 2) == 0)
		{
			int depth = 1;

			/* MySQL executable comments cannot be treated as whitespace. */
			if ((*cursor)[2] == '!')
				return false;
			*cursor += 2;
			while (depth && **cursor)
			{
				if (strncmp(*cursor, "/*", 2) == 0)
				{
					depth++;
					*cursor += 2;
				}
				else if (strncmp(*cursor, "*/", 2) == 0)
				{
					depth--;
					*cursor += 2;
				}
				else
					(*cursor)++;
			}
			if (depth)
				return false;
		}
		else
			return true;
	}
}

static bool
identifier_start(unsigned char value)
{
	return isalpha(value) || value == '_' || value >= 128;
}

static bool
identifier_continuation(unsigned char value)
{
	return identifier_start(value) || isdigit(value) || value == '$' || value == '#';
}

static bool
keyword(const char **cursor, const char *word)
{
	size_t length = strlen(word);

	if (!skip_space(cursor))
		return false;
	if (pg_strncasecmp(*cursor, word, length) != 0 ||
		identifier_continuation((unsigned char) (*cursor)[length]))
		return false;
	*cursor += length;
	return true;
}

static void
parse_name(const char **cursor, bool backticks, SynchdbSourceName *name)
{
	memset(name, 0, sizeof(*name));
	for (;;)
	{
		StringInfoData part;
		char quote;

		if (!skip_space(cursor))
			invalid_truncate();
		if (name->count == lengthof(name->parts))
			invalid_truncate();
		initStringInfo(&part);
		quote = **cursor;
		if (quote == '"' || (backticks && quote == '`'))
		{
			bool closed = false;

			name->quoted[name->count] = true;
			(*cursor)++;
			while (**cursor)
			{
				if (**cursor == quote)
				{
					(*cursor)++;
					if (**cursor != quote)
					{
						closed = true;
						break;
					}
				}
				appendStringInfoChar(&part, *(*cursor)++);
			}
			if (!closed)
				invalid_truncate();
		}
		else
		{
			if (!identifier_start((unsigned char) **cursor))
				invalid_truncate();
			while (identifier_continuation((unsigned char) **cursor))
				appendStringInfoChar(&part, *(*cursor)++);
		}
		if (part.len == 0)
			invalid_truncate();
		name->parts[name->count++] = part.data;
		if (!skip_space(cursor))
			invalid_truncate();
		if (**cursor != '.')
			return;
		(*cursor)++;
	}
}

void
synchdb_source_parse_name(const char *text, SynchdbSourceName *name)
{
	const char *cursor = text;

	if (text == NULL)
		invalid_truncate();
	parse_name(&cursor, true, name);
	if (*cursor)
		invalid_truncate();
}

bool
synchdb_source_parse_truncate(const char *sql, const char *connector,
							 SynchdbSourceName *name)
{
	const char *cursor = sql;
	bool oracle = strcmp(connector, "oracle") == 0;

	if (sql == NULL || !keyword(&cursor, "TRUNCATE"))
		return false;
	(void) keyword(&cursor, "TABLE");
	parse_name(&cursor, strcmp(connector, "mysql") == 0, name);
	if (name->count > 2 && strcmp(connector, "postgresql") != 0)
		invalid_truncate();
	if (oracle && keyword(&cursor, "DROP"))
	{
		(void) keyword(&cursor, "ALL");
		if (!keyword(&cursor, "STORAGE"))
			invalid_truncate();
	}
	else if (oracle && keyword(&cursor, "REUSE"))
	{
		if (!keyword(&cursor, "STORAGE"))
			invalid_truncate();
	}
	if (!skip_space(&cursor))
		invalid_truncate();
	if (*cursor == ';')
		cursor++;
	if (!skip_space(&cursor))
		invalid_truncate();
	if (*cursor)
		invalid_truncate();
	return true;
}

char *
synchdb_source_name_component(const SynchdbSourceName *name, int index,
							  const char *connector)
{
	char *value = pstrdup(name->parts[index]);
	char *at;

	if (!name->quoted[index])
		for (at = value; *at; at++)
		{
			if (strcmp(connector, "oracle") == 0)
				*at = pg_toupper((unsigned char) *at);
			else if (strcmp(connector, "postgresql") == 0)
				*at = pg_tolower((unsigned char) *at);
		}
	return value;
}

static void
check_component(const SynchdbSourceName *name, int index, const char *connector,
				const char *expected)
{
	char *actual = synchdb_source_name_component(name, index, connector);

	if (expected == NULL || strcmp(actual, expected) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("TRUNCATE SQL and source table metadata disagree")));
	pfree(actual);
}

void
synchdb_source_check_truncate_name(const SynchdbSourceName *name,
		const char *connector, const char *database, const char *schema,
		const char *table)
{
	check_component(name, name->count - 1, connector, table);
	if (name->count == 3)
	{
		check_component(name, 0, connector, database);
		check_component(name, 1, connector, schema);
	}
	else if (name->count == 2)
		check_component(name, 0, connector,
						strcmp(connector, "mysql") == 0 ? database : schema);
}
