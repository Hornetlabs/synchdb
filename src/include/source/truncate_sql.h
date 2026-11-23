/* Source SQL syntax only: no target lookup, execution or checkpoint changes. */
#ifndef SYNCHDB_SOURCE_TRUNCATE_SQL_H
#define SYNCHDB_SOURCE_TRUNCATE_SQL_H

#include "postgres.h"

typedef struct SynchdbSourceName
{
	int count;
	char *parts[3];
	bool quoted[3];
} SynchdbSourceName;

/* Preserve source identifier bytes, including names longer than NAMEDATALEN. */
extern void synchdb_source_parse_name(const char *text, SynchdbSourceName *name);
/* False means another command. A recognized but unsupported TRUNCATE errors. */
extern bool synchdb_source_parse_truncate(const char *sql, const char *connector,
										 SynchdbSourceName *name);
extern char *synchdb_source_name_component(const SynchdbSourceName *name,
										 int index, const char *connector);
extern void synchdb_source_check_truncate_name(const SynchdbSourceName *name,
		const char *connector, const char *database, const char *schema,
		const char *table);

#endif
