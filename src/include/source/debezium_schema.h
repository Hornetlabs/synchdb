/* Source-only schema-change images, including every tableChanges element. */
#ifndef SYNCHDB_DEBEZIUM_SCHEMA_H
#define SYNCHDB_DEBEZIUM_SCHEMA_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "source/debezium_event.h"

typedef enum SynchdbDbzSchemaOperation
{
	SYNCHDB_DBZ_SCHEMA_CREATE,
	SYNCHDB_DBZ_SCHEMA_ALTER,
	SYNCHDB_DBZ_SCHEMA_DROP
} SynchdbDbzSchemaOperation;

typedef struct SynchdbDbzTableId
{
	char *serialized;
	char *database;
	char *schema;
	char *table;
	char *key;
} SynchdbDbzTableId;

typedef struct SynchdbDbzSchemaColumn
{
	char *name;
	char *type_name;
	char *type_expression;
	char *charset_name;
	int32 position;
	int32 length;
	int32 scale;
	bool has_length;
	bool has_scale;
	bool optional;
	bool auto_incremented;
	bool generated;
	bool has_default;
	char *default_expression;
	List *enum_values;
	Jsonb *document;
} SynchdbDbzSchemaColumn;

typedef struct SynchdbDbzTableChange
{
	SynchdbDbzSchemaOperation operation;
	SynchdbDbzTableId id;
	SynchdbDbzTableId *previous_id;
	List *columns;
	List *primary_key;
	bool primary_key_known;
	Jsonb *document;
	Jsonb *table_document;
} SynchdbDbzTableChange;

typedef struct SynchdbDbzSchemaChange
{
	List *tables;
} SynchdbDbzSchemaChange;

/* Preserve legacy plain keys when unambiguous; quote components containing
 * dots/quotes/empty schema so different structured identities never collide. */
extern char *synchdb_source_table_key(const char *database, const char *schema,
									const char *table);
extern char *synchdb_source_column_key(const char *table_key, const char *column);
/* No target/catalog/rules/transaction access. All results use current context. */
extern void synchdb_dbz_decode_schema(const SynchdbDbzEvent *event,
									SynchdbDbzSchemaChange *output);

#endif /* SYNCHDB_DEBEZIUM_SCHEMA_H */
