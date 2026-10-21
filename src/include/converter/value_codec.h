/*
 * Value codecs independent of source events, mappings and target relations.
 */
#ifndef SYNCHDB_VALUE_CODEC_H
#define SYNCHDB_VALUE_CODEC_H

#include "postgres.h"

typedef enum SynchdbValueTimeUnit
{
	SYNCHDB_VALUE_DAYS,
	SYNCHDB_VALUE_MILLISECONDS,
	SYNCHDB_VALUE_MICROSECONDS,
	SYNCHDB_VALUE_NANOSECONDS
} SynchdbValueTimeUnit;

extern int64 fc_value_int64_from_bytes(const unsigned char *bytes, int len);
extern char *fc_value_decimal_from_base64(const char *input, int scale);
extern char *fc_value_date(int64 input, SynchdbValueTimeUnit unit, bool quote);
extern char *fc_value_timestamp(int64 input, SynchdbValueTimeUnit unit,
								int32 typmod, bool quote);
extern char *fc_value_time(int64 input, SynchdbValueTimeUnit unit,
						   int32 typmod, bool quote);

#endif
