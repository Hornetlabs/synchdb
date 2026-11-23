/* Debezium V1 wire validation; returned views borrow the complete frame. */
#include "postgres.h"

#include "miscadmin.h"
#include "port/pg_bswap.h"
#include "source/debezium_frame.h"
#include "utils/memutils.h"

#define DBZ_BATCH_HEADER_SIZE ((Size) (1 + 2 * sizeof(uint32)))
#define DBZ_MIN_RECORD_SIZE ((Size) (sizeof(uint32) + 2))

static int32
dbz_frame_int32(const unsigned char *data, Size length, Size *offset)
{
	uint32 value;

	if (*offset > length || length - *offset < sizeof(value))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("truncated Debezium frame header")));
	memcpy(&value, data + *offset, sizeof(value));
	*offset += sizeof(value);
	return (int32) pg_ntoh32(value);
}

static Size
dbz_frame_record(const unsigned char *data, Size length, Size *offset,
				 int32 index, int32 batch_id)
{
	int32 record_length = dbz_frame_int32(data, length, offset);

	CHECK_FOR_INTERRUPTS();
	if (record_length < 2 || (Size) record_length > length - *offset ||
		data[*offset + record_length - 1] != '\0' ||
		memchr(data + *offset, '\0', record_length - 1) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid Debezium record %d in batch %d", index, batch_id)));
	return (Size) record_length;
}

void
synchdb_dbz_frame_decode(const unsigned char *data, Size length,
						 SynchdbDbzFrame *frame)
{
	SynchdbDbzFrame decoded = {0};
	Size offset = 1;

	if (frame == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Debezium frame output must not be NULL")));
	if (data == NULL || length == 0 || length > MaxAllocSize)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid Debezium frame buffer")));
	decoded.data = data;
	decoded.length = length;
	if (data[0] == 'B')
	{
		int32 index;

		decoded.kind = SYNCHDB_DBZ_BATCH;
		decoded.batch_id = dbz_frame_int32(data, length, &offset);
		decoded.record_count = dbz_frame_int32(data, length, &offset);
		decoded.records_offset = offset;
		if (decoded.batch_id < 0 || decoded.record_count < 0 ||
			(Size) decoded.record_count > (length - offset) / DBZ_MIN_RECORD_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid Debezium batch header")));
		for (index = 0; index < decoded.record_count; index++)
		{
			Size record_length = dbz_frame_record(data, length, &offset,
												 index, decoded.batch_id);

			offset += record_length;
		}
		if (offset != length)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("trailing data in Debezium batch %d", decoded.batch_id)));
	}
	else if (data[0] == 'K')
	{
		int32 status_length = dbz_frame_int32(data, length, &offset);
		int32 message_length;

		decoded.kind = SYNCHDB_DBZ_COMPLETION;
		if (status_length < 0 || (Size) status_length > length - offset)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid Debezium completion status length")));
		if (status_length == 4 && memcmp(data + offset, "true", 4) == 0)
			decoded.success = true;
		else if (status_length == 5 && memcmp(data + offset, "false", 5) == 0)
			decoded.success = false;
		else
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid Debezium completion status")));
		offset += status_length;
		message_length = dbz_frame_int32(data, length, &offset);
		if (message_length < 0 || (Size) message_length != length - offset ||
			memchr(data + offset, '\0', message_length) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid Debezium completion message")));
		decoded.message = (const char *) (data + offset);
		decoded.message_length = (Size) message_length;
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unknown Debezium frame type 0x%02x", data[0])));

	/* Do not expose a partially validated frame to the caller. */
	*frame = decoded;
}

void
synchdb_dbz_record_cursor(const SynchdbDbzFrame *frame,
						  SynchdbDbzRecordCursor *cursor)
{
	Size offset = 1;
	int32 batch_id;
	int32 record_count;

	if (frame == NULL || cursor == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Debezium record cursor requires a frame and output")));
	if (frame->kind != SYNCHDB_DBZ_BATCH || frame->data == NULL ||
		frame->length < DBZ_BATCH_HEADER_SIZE || frame->length > MaxAllocSize ||
		frame->records_offset != DBZ_BATCH_HEADER_SIZE || frame->data[0] != 'B')
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid Debezium batch view")));
	batch_id = dbz_frame_int32(frame->data, frame->length, &offset);
	record_count = dbz_frame_int32(frame->data, frame->length, &offset);
	if (batch_id < 0 || record_count < 0 || batch_id != frame->batch_id ||
		record_count != frame->record_count ||
		(Size) record_count > (frame->length - offset) / DBZ_MIN_RECORD_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid Debezium batch view header")));
	cursor->frame = frame;
	cursor->offset = offset;
	cursor->index = 0;
}

bool
synchdb_dbz_record_next(SynchdbDbzRecordCursor *cursor,
						const char **json, Size *length)
{
	const SynchdbDbzFrame *frame;
	Size offset;
	Size record_length;

	if (cursor == NULL || cursor->frame == NULL || json == NULL || length == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Debezium record cursor arguments")));
	*json = NULL;
	*length = 0;
	frame = cursor->frame;
	if (frame->kind != SYNCHDB_DBZ_BATCH || frame->data == NULL ||
		frame->length < DBZ_BATCH_HEADER_SIZE || frame->length > MaxAllocSize ||
		frame->records_offset != DBZ_BATCH_HEADER_SIZE || frame->record_count < 0 ||
		cursor->index < 0 || cursor->index > frame->record_count ||
		cursor->offset < frame->records_offset || cursor->offset > frame->length)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid Debezium record cursor state")));
	if (cursor->index == frame->record_count)
	{
		if (cursor->offset != frame->length)
			ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("trailing data in Debezium batch %d", frame->batch_id)));
		return false;
	}

	offset = cursor->offset;
	record_length = dbz_frame_record(frame->data, frame->length, &offset,
									cursor->index, frame->batch_id);
	if (cursor->index == frame->record_count - 1 && offset + record_length != frame->length)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("trailing data in Debezium batch %d", frame->batch_id)));
	*json = (const char *) (frame->data + offset);
	*length = record_length - 1;
	cursor->offset = offset + record_length;
	cursor->index++;
	return true;
}
