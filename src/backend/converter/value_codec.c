/*
 * Source-independent numeric and temporal value conversion.
 *
 * Results belong to CurrentMemoryContext. SQL transaction ownership, source
 * metadata, mappings and connector error reporting remain with the caller.
 */
#include "postgres.h"
#include "executor/pg_compat.h"
#include "miscadmin.h"

#include <math.h>

#include "common/base64.h"
#include "common/int.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "converter/value_codec.h"

int64
fc_value_int64_from_bytes(const unsigned char *bytes, int len)
{
	uint64 bits;
	int64 result;
	int i;

	if (len < 1 || len > sizeof(result))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("invalid signed integer byte length: %d", len)));

	bits = (bytes[0] & 0x80) ? UINT64_MAX : 0;
	for (i = 0; i < len; i++)
		bits = (bits << 8) | bytes[i];
	memcpy(&result, &bits, sizeof(result));
	return result;
}

static char *
decimal_digits(const unsigned char *bytes, int len)
{
	unsigned char *work;
	char *digits;
	char *out;
	bool negative;
	int start = 0;
	int ndigits = 0;
	int i;
	int pos = 0;

	if (len < 1 || (Size) len > (MaxAllocSize - 2) / 3)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("invalid decimal byte length: %d", len)));

	negative = (bytes[0] & 0x80) != 0;
	work = palloc(len);
	memcpy(work, bytes, len);
	if (negative)
	{
		unsigned int carry = 1;

		for (i = len - 1; i >= 0; i--)
		{
			unsigned int v = (unsigned char) ~work[i] + carry;

			work[i] = v & 0xff;
			carry = v >> 8;
		}
	}
	while (start < len && work[start] == 0)
		start++;
	if (start == len)
	{
		pfree(work);
		return pstrdup("0");
	}

	digits = palloc((Size) len * 3 + 1);
	while (start < len)
	{
		unsigned int remainder = 0;

		for (i = start; i < len; i++)
		{
			unsigned int v = (remainder << 8) | work[i];

			work[i] = v / 10;
			remainder = v % 10;
		}
		digits[ndigits++] = '0' + remainder;
		while (start < len && work[start] == 0)
			start++;
	}
	out = palloc((Size) ndigits + (negative ? 1 : 0) + 1);
	if (negative)
		out[pos++] = '-';
	for (i = ndigits - 1; i >= 0; i--)
		out[pos++] = digits[i];
	out[pos] = '\0';
	pfree(work);
	pfree(digits);
	return out;
}

char *
fc_value_decimal_from_base64(const char *input, int scale)
{
	Size input_len = strlen(input);
	int capacity;
	int decoded_len;
	unsigned char *decoded;
	char *value;
	const char *digits;
	Size ndigits;
	Size zeros;
	Size size;
	char *out;
	char *p;
	bool negative;

	if (input_len == 0 || input_len > MaxAllocSize / 4)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("invalid base64 decimal length")));
	capacity = pg_b64_dec_len((int) input_len);
	decoded = palloc(capacity);
	decoded_len = synchdb_pg_base64_decode(input, (int) input_len, decoded, capacity);
	value = decimal_digits(decoded, decoded_len);
	pfree(decoded);
	negative = value[0] == '-';
	digits = value + (negative ? 1 : 0);
	ndigits = strlen(digits);
	zeros = scale < 0 ? (Size) -((int64) scale) : (Size) scale;
	size = ndigits + zeros + 4;
	if (!AllocSizeIsValid(size))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("decimal scale is too large: %d", scale)));
	out = palloc(size);
	p = out;
	if (negative)
		*p++ = '-';
	if (scale <= 0)
	{
		memcpy(p, digits, ndigits);
		p += ndigits;
		memset(p, '0', zeros);
		p += zeros;
	}
	else if (ndigits > (Size) scale)
	{
		Size whole = ndigits - scale;

		memcpy(p, digits, whole);
		p += whole;
		*p++ = '.';
		memcpy(p, digits + whole, scale);
		p += scale;
	}
	else
	{
		*p++ = '0';
		*p++ = '.';
		zeros = scale - ndigits;
		memset(p, '0', zeros);
		p += zeros;
		memcpy(p, digits, ndigits);
		p += ndigits;
	}
	*p = '\0';
	pfree(value);
	return out;
}

static int64
units_per_second(SynchdbValueTimeUnit unit)
{
	switch (unit)
	{
		case SYNCHDB_VALUE_MILLISECONDS:
			return 1000;
		case SYNCHDB_VALUE_MICROSECONDS:
			return 1000000;
		case SYNCHDB_VALUE_NANOSECONDS:
			return 1000000000;
		default:
			elog(ERROR, "invalid time unit: %d", unit);
	}
	pg_unreachable();
}

static int64
floor_divide(int64 value, int64 divisor)
{
	int64 result = value / divisor;

	if (value % divisor < 0)
		result--;
	return result;
}

static int64
epoch_microseconds(int64 input, SynchdbValueTimeUnit unit)
{
	int64 units = units_per_second(unit);
	int64 seconds = floor_divide(input, units);
	int64 remainder = input % units;
	int64 fraction;
	int64 result;

	if (remainder < 0)
		remainder += units;
	if (unit == SYNCHDB_VALUE_NANOSECONDS)
		fraction = (int64) rint((double) remainder / 1000.0);
	else
		fraction = remainder * (USECS_PER_SEC / units);
	if (pg_mul_s64_overflow(seconds, USECS_PER_SEC, &result) ||
		pg_add_s64_overflow(result, fraction, &result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("time value is out of range")));
	return result;
}

static char *
finish_value(const char *value, bool quote)
{
	return quote ? quote_literal_cstr(value) : pstrdup(value);
}

char *
fc_value_date(int64 input, SynchdbValueTimeUnit unit, bool quote)
{
	int64 days;
	int64 pg_days;
	struct pg_tm tm;
	char buf[MAXDATELEN + 1];

	if (unit == SYNCHDB_VALUE_DAYS)
		days = input;
	else
		days = floor_divide(input, units_per_second(unit) * SECS_PER_DAY);
	if (pg_sub_s64_overflow(days, POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE,
							&pg_days) || !IS_VALID_DATE(pg_days))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("date value is out of range")));
	memset(&tm, 0, sizeof(tm));
	j2date((int) (days + UNIX_EPOCH_JDATE),
		   &tm.tm_year, &tm.tm_mon, &tm.tm_mday);
	EncodeDateOnly(&tm, USE_ISO_DATES, buf);
	return finish_value(buf, quote);
}

char *
fc_value_timestamp(int64 input, SynchdbValueTimeUnit unit,
				   int32 typmod, bool quote)
{
	Timestamp ts = epoch_microseconds(input, unit);
	const int64 epoch_offset =
		(int64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY;
	struct pg_tm tm;
	fsec_t fsec;
	char buf[MAXDATELEN + 1];

	if (pg_sub_s64_overflow(ts, epoch_offset, &ts) ||
		!IS_VALID_TIMESTAMP(ts))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp value is out of range")));
	if (typmod < -1 || typmod > MAX_TIMESTAMP_PRECISION)
		elog(ERROR, "invalid timestamp precision: %d", typmod);
	ts = DatumGetTimestamp(DirectFunctionCall2(timestamp_scale,
											 TimestampGetDatum(ts),
											 Int32GetDatum(typmod)));
	if (timestamp2tm(ts, NULL, &tm, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp value is out of range")));
	EncodeDateTime(&tm, fsec, false, 0, NULL, USE_ISO_DATES, buf);
	return finish_value(buf, quote);
}

char *
fc_value_time(int64 input, SynchdbValueTimeUnit unit, int32 typmod, bool quote)
{
	TimeADT time;
	struct pg_tm tm;
	fsec_t fsec;
	char buf[MAXDATELEN + 1];
	int64 seconds;

	/* Validate the source range before nanosecond rounding can hide overflow. */
	if (input < 0 || input > units_per_second(unit) * SECS_PER_DAY)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("time of day is out of range")));
	time = epoch_microseconds(input, unit);
	if (typmod < -1 || typmod > MAX_TIME_PRECISION)
		elog(ERROR, "invalid time precision: %d", typmod);
	time = DatumGetTimeADT(DirectFunctionCall2(time_scale,
											 TimeADTGetDatum(time),
											 Int32GetDatum(typmod)));
	memset(&tm, 0, sizeof(tm));
	seconds = time / USECS_PER_SEC;
	tm.tm_hour = seconds / SECS_PER_HOUR;
	tm.tm_min = (seconds / SECS_PER_MINUTE) % MINS_PER_HOUR;
	tm.tm_sec = seconds % SECS_PER_MINUTE;
	fsec = time % USECS_PER_SEC;
	EncodeTimeOnly(&tm, fsec, false, 0, USE_ISO_DATES, buf);
	return finish_value(buf, quote);
}
