/* Source row images and values, before target binding or conversion. */
#ifndef SYNCHDB_DEBEZIUM_ROW_H
#define SYNCHDB_DEBEZIUM_ROW_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "source/debezium_event.h"
#include "utils/jsonb.h"

typedef enum SynchdbDbzValueKind
{
	SYNCHDB_DBZ_VALUE_MISSING,
	SYNCHDB_DBZ_VALUE_NULL,
	SYNCHDB_DBZ_VALUE_STRING,
	SYNCHDB_DBZ_VALUE_NUMBER,
	SYNCHDB_DBZ_VALUE_BOOLEAN,
	SYNCHDB_DBZ_VALUE_ARRAY,
	SYNCHDB_DBZ_VALUE_OBJECT,
	SYNCHDB_DBZ_VALUE_UNAVAILABLE
} SynchdbDbzValueKind;

typedef struct SynchdbDbzValue
{
	SynchdbDbzValueKind kind;
	/* Invalid for MISSING; nested values retain their original JSON structure. */
	JsonbValue raw;
} SynchdbDbzValue;

typedef struct SynchdbDbzField
{
	char *name;
	char *wire_type;
	char *logical_name;
	Jsonb *parameters;
	Jsonb *item_schema;	/* optional source array element schema */
	bool optional;
	SynchdbDbzValue value;
} SynchdbDbzField;

typedef enum SynchdbDbzImageState
{
	SYNCHDB_DBZ_IMAGE_MISSING,
	SYNCHDB_DBZ_IMAGE_NULL,
	SYNCHDB_DBZ_IMAGE_OBJECT
} SynchdbDbzImageState;

typedef struct SynchdbDbzImage
{
	SynchdbDbzImageState state;
	/* SynchdbDbzField*, in this image's source-schema order. */
	List *fields;
} SynchdbDbzImage;

typedef struct SynchdbDbzRow
{
	char *db;
	char *schema;
	char *table;
	char op;
	SynchdbDbzImage before;
	SynchdbDbzImage after;
} SynchdbDbzRow;

/*
 * The input must be a decoded row event. Allocations belong to the current
 * memory context. Raw field values borrow the event's JSON storage, which
 * must remain alive while the row is used. Missing and null values are never
 * replaced with defaults, and no target metadata or transformation is used.
 * Malformed row schemas or values raise ERROR; output is published on success.
 */
extern void synchdb_dbz_decode_row(const SynchdbDbzEvent *event,
								 SynchdbDbzRow *row);

/* Array elements use the same wire/logical validation as scalar fields.
 * Nested ordinary strings are data, not top-level unavailable placeholders. */
extern SynchdbDbzField *synchdb_dbz_array_element_field(const SynchdbDbzField *array);
extern void synchdb_dbz_decode_element_value(SynchdbDbzField *field,
										   const JsonbValue *raw);

#endif /* SYNCHDB_DEBEZIUM_ROW_H */
