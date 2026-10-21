/* Validated V1 wire views. All pointers borrow the caller's live frame bytes. */
#ifndef SYNCHDB_DEBEZIUM_FRAME_H
#define SYNCHDB_DEBEZIUM_FRAME_H

#include "postgres.h"

typedef enum SynchdbDbzFrameKind
{
	SYNCHDB_DBZ_BATCH,
	SYNCHDB_DBZ_COMPLETION
} SynchdbDbzFrameKind;

typedef struct SynchdbDbzFrame
{
	SynchdbDbzFrameKind kind;
	const unsigned char *data;
	Size length;
	int32 batch_id;
	int32 record_count; /* Transmitted records, after Java's tombstone filtering. */
	Size records_offset;
	bool success;
	const char *message;
	Size message_length;
} SynchdbDbzFrame;

typedef struct SynchdbDbzRecordCursor
{
	const SynchdbDbzFrame *frame;
	Size offset;
	int32 index;
} SynchdbDbzRecordCursor;

/* Validates the complete frame before publishing a view, without starting SQL. */
extern void synchdb_dbz_frame_decode(const unsigned char *data, Size length,
								   SynchdbDbzFrame *frame);
extern void synchdb_dbz_record_cursor(const SynchdbDbzFrame *frame,
									SynchdbDbzRecordCursor *cursor);
/* JSON is NUL terminated; length excludes that terminator. No allocation/copy. */
extern bool synchdb_dbz_record_next(SynchdbDbzRecordCursor *cursor,
								  const char **json, Size *length);

#endif /* SYNCHDB_DEBEZIUM_FRAME_H */
