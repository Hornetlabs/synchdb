/*--------------------------------------------------------------------
 *
 * ora_compatible.h
 *
 * Definition enumeration structure is fro supporting different compatibility modes.
 *
 * Portions Copyright (c) 2023, IvorySQL
 *
 * src/include/utils/ora_compatible.h
 *
 * add the file for requirement "SQL PARSER"
 *
 *----------------------------------------------------------------------
 */

#ifndef ORA_COMPATIBLE_H
#define ORA_COMPATIBLE_H

#define ORA_SEARCH_PATH "sys,\"$user\", public"
#define DB_MODE_PARMATER "ivorysql.database_mode"

#define CHAR_TYPE_LENGTH_MAX    2000

typedef enum DBMode
{
	DB_PG = 0,
	DB_ORACLE
}DBMode;

typedef enum DBParser
{
	PG_PARSER = 0,
	ORA_PARSER
}DBParser;

typedef enum CaseSwitchMode
{
	NORMAL = 0,
	INTERCHANGE,
	LOWERCASE
}CaseSwitchMode;

typedef enum
{
	NLS_LENGTH_BYTE,
	NLS_LENGTH_CHAR
} NlsLengthSemantics;

/* oracle parser static parameters */
extern int compatible_db;
extern int nls_length_semantics;
extern bool identifier_case_from_pg_dump;
extern bool enable_case_switch;
extern bool enable_emptystring_to_NULL;
extern int identifier_case_switch;

char *identifier_case_transform(const char *ident, int len);
char *downcase_identifier(const char *ident, int len, bool warn, bool truncate);
char *upcase_identifier(const char *ident, int len, bool warn, bool truncate);
void truncate_identifier(char *ident, int len, bool warn);
bool is_all_upper(const char *src, int len);

#endif							/* ORA_COMPATIBLE_H */
