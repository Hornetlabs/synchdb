/* Source-only Debezium envelope decoding. No target or connector state access. */
#ifndef SYNCHDB_DEBEZIUM_EVENT_H
#define SYNCHDB_DEBEZIUM_EVENT_H

#include "postgres.h"
#include "utils/jsonb.h"

typedef enum SynchdbDbzEventKind
{
	SYNCHDB_DBZ_EVENT_ROW,
	SYNCHDB_DBZ_EVENT_SCHEMA,
	SYNCHDB_DBZ_EVENT_TRANSACTION,
	SYNCHDB_DBZ_EVENT_HEARTBEAT,
	SYNCHDB_DBZ_EVENT_LOGICAL_MESSAGE,
	SYNCHDB_DBZ_EVENT_TRUNCATE,
	SYNCHDB_DBZ_EVENT_UNSUPPORTED
} SynchdbDbzEventKind;

typedef enum SynchdbDbzSnapshot
{
	SYNCHDB_DBZ_SNAPSHOT_FALSE,
	SYNCHDB_DBZ_SNAPSHOT_TRUE,
	SYNCHDB_DBZ_SNAPSHOT_FIRST,
	SYNCHDB_DBZ_SNAPSHOT_FIRST_IN_DATA_COLLECTION,
	SYNCHDB_DBZ_SNAPSHOT_LAST_IN_DATA_COLLECTION,
	SYNCHDB_DBZ_SNAPSHOT_LAST,
	SYNCHDB_DBZ_SNAPSHOT_INCREMENTAL
} SynchdbDbzSnapshot;

typedef struct SynchdbDbzEvent
{
	Jsonb *document;
	Jsonb *payload;
	Jsonb *source;
	char *connector;
	char operation;
	/* Exact source identity for a table-wide change, without row images. */
	char *database;
	char *schema;
	char *table;
	SynchdbDbzEventKind kind;
	SynchdbDbzSnapshot snapshot;
	uint64 src_ts_ms;
	bool schema_empty;
	const char *unsupported_reason;
} SynchdbDbzEvent;

/*
 * Input is wire UTF8; allocations/decoded strings use the database encoding.
 * Allocations belong to CurrentMemoryContext. A malformed envelope raises ERROR;
 * an understood but unsupported source event returns EVENT_UNSUPPORTED. The
 * output is only published after the complete envelope has been validated.
 */
extern void synchdb_dbz_decode_event(const char *json, SynchdbDbzEvent *event);

#endif /* SYNCHDB_DEBEZIUM_EVENT_H */
