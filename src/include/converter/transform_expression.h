/* Parameterized scalar transformation expressions. */
#ifndef SYNCHDB_TRANSFORM_EXPRESSION_H
#define SYNCHDB_TRANSFORM_EXPRESSION_H

#include "postgres.h"

/* Compile template grammar without executing SQL or type input functions. */
extern void synchdb_transform_validate_template(const char *expression);
/* FDW text values carry no Debezium spatial struct parameters. */
extern void synchdb_transform_validate_fdw_template(const char *expression);

/* Caller owns the transaction. The result is caller-context text or SQL NULL. */
extern char *ra_transformDataExpression(const char *data, const char *wkb,
									   const char *srid, const char *expression,
										Oid target_type);

#endif /* SYNCHDB_TRANSFORM_EXPRESSION_H */
