/*-------------------------------------------------------------------------
 *
 * format_converter.c
 *    Conversion utilities for Debezium change events to PostgreSQL format
 *
 * This file contains functions to parse Debezium (DBZ) change events,
 * convert them to PostgreSQL-compatible DDL and DML operations, and
 * execute those operations. It handles CREATE, DROP, INSERT, UPDATE,
 * and DELETE operations from various source databases (currently 
 * MySQL, Oracle, and SQL Server) and converts them to equivalent 
 * PostgreSQL commands.
 *
 * The main entry point is fc_processDBZChangeEvent(), which takes a
 * Debezium change event as input, parses it, converts it, and executes
 * the resulting PostgreSQL operation.
 *
 * Key functions:
 * - parseDBZDDL(): Parses Debezium DDL events
 * - parseDBZDML(): Parses Debezium DML events
 * - convert2PGDDL(): Converts DBZ DDL to PostgreSQL DDL
 * - convert2PGDML(): Converts DBZ DML to PostgreSQL DML
 * - processDataByType(): Handles data type conversions
 *
 * Copyright (c) Hornetlabs Technology, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "access/table.h"
#include <time.h>
#include <sys/time.h>
#include <dlfcn.h>
#include "common/base64.h"
#include "port/pg_bswap.h"
#include "utils/datetime.h"
#include "utils/memutils.h"

/* synchdb includes */
#include "converter/format_converter.h"
#include "converter/debezium_event_handler.h"
#ifdef WITH_OLR
#include "converter/olr_event_handler.h"
#endif
#include "synchdb/synchdb.h"

/* global external variables */
extern bool synchdb_dml_use_spi;
extern int myConnectorId;

/* data transformation related hash tables */
HTAB * dataCacheHash = NULL;
static HTAB * objectMappingHash = NULL;
static HTAB * transformExpressionHash = NULL;

/* data type mapping related hash tables */
static HTAB * mysqlDatatypeHash = NULL;
static HTAB * oracleDatatypeHash = NULL;
static HTAB * sqlserverDatatypeHash = NULL;

DatatypeHashEntry mysql_defaultTypeMappings[] =
{
	{{"int", true}, "serial", 0},
	{{"bigint", true}, "bigserial", 0},
	{{"smallint", true}, "smallserial", 0},
	{{"mediumint", true}, "serial", 0},
	{{"enum", false}, "text", 0},
	{{"set", false}, "text", 0},
	{{"bigint", false}, "bigint", 0},
	{{"bigint unsigned", false}, "numeric", -1},
	{{"numeric unsigned", false}, "numeric", -1},
	{{"dec", false}, "decimal", -1},
	{{"dec unsigned", false}, "decimal", -1},
	{{"decimal unsigned", false}, "decimal", -1},
	{{"fixed", false}, "decimal", -1},
	{{"fixed unsigned", false}, "decimal", -1},
	{{"bit(1)", false}, "boolean", 0},
	{{"bit", false}, "bit", -1},
	{{"bool", false}, "boolean", -1},
	{{"double", false}, "double precision", 0},
	{{"double precision", false}, "double precision", 0},
	{{"double precision unsigned", false}, "double precision", 0},
	{{"double unsigned", false}, "double precision", 0},
	{{"real", false}, "real", 0},
	{{"real unsigned", false}, "real", 0},
	{{"float", false}, "real", 0},
	{{"float unsigned", false}, "real", 0},
	{{"int", false}, "int", 0},
	{{"int unsigned", false}, "bigint", 0},
	{{"integer", false}, "int", 0},
	{{"integer unsigned", false}, "bigint", 0},
	{{"mediumint", false}, "int", 0},
	{{"mediumint unsigned", false}, "int", 0},
	{{"year", false}, "int", 0},
	{{"smallint", false}, "smallint", 0},
	{{"smallint unsigned", false}, "int", 0},
	{{"tinyint", false}, "smallint", 0},
	{{"tinyint unsigned", false}, "smallint", 0},
	{{"datetime", false}, "timestamp", -1},
	{{"timestamp", false}, "timestamptz", -1},
	{{"binary", false}, "bytea", 0},
	{{"varbinary", false}, "bytea", 0},
	{{"blob", false}, "bytea", 0},
	{{"mediumblob", false}, "bytea", 0},
	{{"longblob", false}, "bytea", 0},
	{{"tinyblob", false}, "bytea", 0},
	{{"long varchar", false}, "text", -1},
	{{"longtext", false}, "text", -1},
	{{"mediumtext", false}, "text", -1},
	{{"tinytext", false}, "text", -1},
	{{"json", false}, "jsonb", -1},
	{{"geometry", false}, "text", -1},
	{{"geometrycollection", false}, "text", -1},
	{{"geomcollection", false}, "text", -1},
	{{"linestring", false}, "text", -1},
	{{"multilinestring", false}, "text", -1},
	{{"multipoint", false}, "text", -1},
	{{"multipolygon", false}, "text", -1},
	{{"point", false}, "text", -1},
	{{"polygon", false}, "text", -1}
};
#define SIZE_MYSQL_DATATYPE_MAPPING (sizeof(mysql_defaultTypeMappings) / sizeof(DatatypeHashEntry))

DatatypeHashEntry oracle_defaultTypeMappings[] =
{
	{{"binary_double", false}, "double precision", 0},
	{{"binary_float", false}, "real", 0},
	{{"float", false}, "real", 0},
	{{"number(0,0)", false}, "numeric", -1},
	{{"number(1,0)", false}, "smallint", 0},
	{{"number(2,0)", false}, "smallint", 0},
	{{"number(3,0)", false}, "smallint", 0},
	{{"number(4,0)", false}, "smallint", 0},
	{{"number(5,0)", false}, "int", 0},
	{{"number(6,0)", false}, "int", 0},
	{{"number(7,0)", false}, "int", 0},
	{{"number(8,0)", false}, "int", 0},
	{{"number(9,0)", false}, "int", 0},
	{{"number(10,0)", false}, "bigint", 0},
	{{"number(11,0)", false}, "bigint", 0},
	{{"number(12,0)", false}, "bigint", 0},
	{{"number(13,0)", false}, "bigint", 0},
	{{"number(14,0)", false}, "bigint", 0},
	{{"number(15,0)", false}, "bigint", 0},
	{{"number(16,0)", false}, "bigint", 0},
	{{"number(17,0)", false}, "bigint", 0},
	{{"number(18,0)", false}, "bigint", 0},
	{{"number(19,0)", false}, "numeric", -1},
	{{"number(20,0)", false}, "numeric", -1},
	{{"number(21,0)", false}, "numeric", -1},
	{{"number(22,0)", false}, "numeric", -1},
	{{"number(23,0)", false}, "numeric", -1},
	{{"number(24,0)", false}, "numeric", -1},
	{{"number(25,0)", false}, "numeric", -1},
	{{"number(26,0)", false}, "numeric", -1},
	{{"number(27,0)", false}, "numeric", -1},
	{{"number(28,0)", false}, "numeric", -1},
	{{"number(29,0)", false}, "numeric", -1},
	{{"number(30,0)", false}, "numeric", -1},
	{{"number(31,0)", false}, "numeric", -1},
	{{"number(32,0)", false}, "numeric", -1},
	{{"number(33,0)", false}, "numeric", -1},
	{{"number(34,0)", false}, "numeric", -1},
	{{"number(35,0)", false}, "numeric", -1},
	{{"number(36,0)", false}, "numeric", -1},
	{{"number(37,0)", false}, "numeric", -1},
	{{"number(38,0)", false}, "numeric", -1},
	{{"number", false}, "numeric", -1},
	{{"numeric", false}, "numeric", -1},
	{{"date", false}, "timestamp", -1},
	{{"long", false}, "text", -1},
	{{"interval day to second", false}, "interval day to second", -1},
	{{"interval year to month", false}, "interval year to month", 0},
	{{"timestamp", false}, "timestamp", -1},
	{{"timestamp with local time zone", false}, "timestamptz", -1},
	{{"timestamp with time zone", false}, "timestamptz", -1},
	{{"date", false}, "date", -1},
	{{"char", false}, "char", -1},
	{{"nchar", false}, "char", -1},
	{{"nvarchar2", false}, "varchar", -1},
	{{"varchar", false}, "varchar", -1},
	{{"varchar2", false}, "varchar", -1},
	{{"long raw", false}, "bytea", 0},
	{{"raw", false}, "bytea", 0},
	{{"decimal", false}, "decimal", -1},
	{{"rowid", false}, "text", 0},
	{{"urowid", false}, "text", 0},
	{{"xmltype", false}, "text", 0},
	{{"bfile", false}, "text", 0},
	{{"blob", false}, "bytea", 0},
	{{"clob", false}, "text", 0},
	{{"nclob", false}, "text", 0},
	{{"sdo_geometry", false}, "text", 0},
	{{"sdo_topo_geometry", false}, "text", 0},
	{{"sdo_georaster", false}, "text", 0},
	{{"uritype", false}, "text", 0},
	{{"anytype", false}, "text", 0},
	{{"anydata", false}, "text", 0},
	{{"anydataset", false}, "text", 0},
};
#define SIZE_ORACLE_DATATYPE_MAPPING (sizeof(oracle_defaultTypeMappings) / sizeof(DatatypeHashEntry))

DatatypeHashEntry sqlserver_defaultTypeMappings[] =
{
	{{"int identity", true}, "serial", 0},
	{{"bigint identity", true}, "bigserial", 0},
	{{"smallint identity", true}, "smallserial", 0},
	{{"enum", false}, "text", 0},
	{{"int", false}, "int", 0},
	{{"bigint", false}, "bigint", 0},
	{{"smallint", false}, "smallint", 0},
	{{"tinyint", false}, "smallint", 0},
	{{"numeric", false}, "numeric", -1},
	{{"decimal", false}, "numeric", -1},
	{{"bit(1)", false}, "bool", 0},
	{{"bit", false}, "bit", 0},
	{{"money", false}, "money", 0},
	{{"smallmoney", false}, "money", 0},
	{{"real", false}, "real", 0},
	{{"float", false}, "real", 0},
	{{"date", false}, "date", 0},
	{{"time", false}, "time", 0},
	{{"datetime", false}, "timestamp", 0},
	{{"datetime2", false}, "timestamp", 0},
	{{"datetimeoffset", false}, "timestamptz", 0},
	{{"smalldatetime", false}, "timestamp", 0},
	{{"char", false}, "char", -1},
	{{"varchar", false}, "varchar", -1},
	{{"text", false}, "text", 0},
	{{"nchar", false}, "char", 0},
	{{"nvarchar", false}, "varchar", -1},
	{{"ntext", false}, "text", 0},
	{{"binary", false}, "bytea", 0},
	{{"varbinary", false}, "bytea", 0},
	{{"image", false}, "bytea", 0},
	{{"uniqueidentifier", false}, "uuid", 0},
	{{"xml", false}, "xml", 0},
	{{"json", false}, "jsonb", -1},
	{{"hierarchyid", false}, "text", 0},
	{{"vector", false}, "text", 0},
	{{"geometry", false}, "text", 0},
	{{"geography", false}, "text", 0},
};

#define SIZE_SQLSERVER_DATATYPE_MAPPING (sizeof(sqlserver_defaultTypeMappings) / sizeof(DatatypeHashEntry))

static void remove_precision(char * str, bool * removed);
static int count_active_columns(TupleDesc tupdesc);
static void bytearray_to_escaped_string(const unsigned char *byte_array,
		size_t length, char *output_string);
static long long derive_value_from_byte(const unsigned char * bytes, int len);
static char * derive_decimal_string_from_byte(const unsigned char *bytes, int len);
static void reverse_byte_array(unsigned char * array, int length);
static void trim_leading_zeros(char *str);
static void prepend_zeros(char *str, int num_zeros);
static void byte_to_binary(unsigned char byte, char * binary_str);
static void bytes_to_binary_string(const unsigned char * bytes,
		size_t len, char * binary_str);
static char * transform_data_expression(const char * remoteObjid, const char * colname);
static void populate_primary_keys(StringInfoData * strinfo, const char * id,
		const char * jsonin, bool alter, bool isinline);
static void init_mysql(void);
static void init_oracle(void);
static void init_sqlserver(void);
static int alter_tbname(const char * from, const char * to);
static int alter_attname(const char * tbname, const char * from,
		const char * to);
static int alter_atttype(const char * tbname, const char * from,
		const char * to, int typesz, const char * convertfunc);
static void transformDDLColumns(const char * id, DBZ_DDL_COLUMN * col,
		ConnectorType conntype, bool datatypeonly, StringInfoData * strinfo,
		PG_DDL_COLUMN * pgcol);
static char * composeAlterColumnClauses(const char * objid, ConnectorType type,
		List * dbzcols, TupleDesc tupdesc, Relation rel, PG_DDL * pgddl);
static void expand_struct_value(char * in, DBZ_DML_COLUMN_VALUE * colval,
		ConnectorType conntype);
static char * handle_base64_to_numeric_with_scale(const char * in, int scale);
static char * handle_string_to_numeric(const char * in, bool addquote);
static char * handle_base64_to_bit(const char * in, bool addquote, int typemod);
static char * handle_string_to_bit(const char * in, bool addquote);
static char * handle_numeric_to_bit(const char * in, bool addquote);
static char * construct_datestr(long long input, bool addquote, int timerep);
static char * handle_base64_to_date(const char * in, bool addquote, int timerep);
static char * handle_numeric_to_date(const char * in, bool addquote, int timerep);
static char * handle_string_to_date(const char * in, bool addquote);
static char * construct_timestampstr(long long input, bool addquote, int timerep, int typemod);
static char * handle_base64_to_timestamp(const char * in, bool addquote, int timerep, int typemod);
static char * handle_numeric_to_timestamp(const char * in, bool addquote, int timerep, int typemod);
static char * handle_string_to_timestamp(const char * in, bool addquote, ConnectorType type);
static char * construct_timetr(long long input, bool addquote, int timerep, int typemod);
static char * handle_base64_to_time(const char * in, bool addquote, int timerep, int typemod);
static char * handle_numeric_to_time(const char * in, bool addquote, int timerep, int typemod);
static char * handle_string_to_time(const char * in, bool addquote);
static char * handle_base64_to_byte(const char * in, bool addquote);
static char * handle_string_to_byte(const char * in, bool addquote);
static char * handle_numeric_to_byte(const char * in, bool addquote);
static char * construct_intervalstr(long long input, bool addquote, int timerep, int typemod);
static char * handle_base64_to_interval(const char * in, bool addquote, int timerep, int typemod);
static char * handle_numeric_to_interval(const char * in, bool addquote, int timerep, int typemod);
static char * handle_string_to_interval(const char * in, bool addquote);
static char * handle_data_by_type_category(char * in, DBZ_DML_COLUMN_VALUE * colval,
		ConnectorType conntype, bool addquote);
static char * processDataByType(DBZ_DML_COLUMN_VALUE * colval, bool addquote,
		char * remoteObjectId, ConnectorType type);

/*
 * remove_precision
 *
 * this helper function removes precision parameters enclosed in () from the input string
 */
static void
remove_precision(char * str, bool * removed) {
	char *openParen = strchr(str, '(');
	char *closeParen;

	if (removed == NULL)
		return;

	while (openParen != NULL)
	{
		closeParen = strchr(openParen, ')');
		if (closeParen != NULL)
		{
			memmove(openParen, closeParen + 1, strlen(closeParen));
			*removed = true;
		}
		else
			break;
		openParen = strchr(openParen, '(');
	}
}

/*
 * count_active_columns
 *
 * this helper function counts the number of active (not dropped) columns from given tupdesc
 */
static int
count_active_columns(TupleDesc tupdesc)
{
	int count = 0, i = 0;
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (!attr->attisdropped)
			count++;
	}
	return count;
}

/*
 * bytearray_to_escaped_string
 *
 * converts byte array to escaped string
 */
static void
bytearray_to_escaped_string(const unsigned char *byte_array, size_t length, char *output_string)
{
	char *ptr = NULL;

	if (!output_string)
		return;

	strcpy(output_string, "'\\x");
	ptr = output_string + 3; /* Skip "'\\x" */

	for (size_t i = 0; i < length; i++)
	{
		sprintf(ptr, "%02X", byte_array[i]);
		ptr += 2;
	}
	strcat(ptr, "'");
}

/*
 * derive_value_from_byte
 *
 * computes the int value from given byte. Safe for len < 7.
 */
static long long
derive_value_from_byte(const unsigned char * bytes, int len)
{
	long long value = 0;
	int i;

	/* Convert the byte array to an integer */
	for (i = 0; i < len; i++)
	{
		value = (value << 8) | bytes[i];
	}

	/*
	 * If the value is signed and the most significant bit (MSB) is set,
	 * sign-extend the value
	 */
	if ((bytes[0] & 0x80))
	{
		value |= -((long long) 1 << (len * 8));
	}
	return value;
}

/*
 * derive_decimal_string_from_byte
 *
 * Converts a big-endian two's-complement byte array to a decimal string.
 * No integer overflow, works for any byte length.
 */
static char *
derive_decimal_string_from_byte(const unsigned char *bytes, int len)
{
	bool neg;
	unsigned char *mag;
	int offset;
	int maglen;
	char digits[128];     /* enough for DECIMAL(38) and more */
	int nd;
	int i;                /* declare once and reuse */
	int outlen;
	char *out;
	char *p;

	neg = (bytes[0] & 0x80) != 0;
	mag = (unsigned char *) palloc(len);
	memcpy(mag, bytes, len);

	/* If negative, convert to magnitude via two's complement */
	if (neg)
	{
		for (i = 0; i < len; i++)
			mag[i] = (unsigned char) ~mag[i];

		for (i = len - 1; i >= 0; i--)
		{
			unsigned int v = (unsigned int) mag[i] + 1;
			mag[i] = (unsigned char) (v & 0xFF);
			if ((v & 0x100) == 0)
				break;
		}
	}

	/* strip leading zero bytes */
	offset = 0;
	while (offset < len && mag[offset] == 0)
		offset++;
	maglen = len - offset;
	mag += offset;

	/* if value is zero, just return "0" */
	if (maglen == 0)
	{
		out = pstrdup("0");
		return out;
	}

	nd = 0;

	/* working copy */
	{
		unsigned char *work = (unsigned char *) palloc(maglen);
		memcpy(work, mag, maglen);

		while (1)
		{
			unsigned remainder = 0;
			i = 0;
			while (i < maglen && work[i] == 0)
				i++;
			if (i == maglen)
				break;

			for (i = 0; i < maglen; i++)
			{
				unsigned acc = (remainder << 8) | work[i];
				work[i] = (unsigned char) (acc / 10);
				remainder = acc % 10;
			}
			digits[nd++] = (char) ('0' + remainder);
		}

		pfree(work);
	}

	/* allocate final string with sign if needed */
	outlen = nd + (neg ? 1 : 0);
	out = (char *) palloc(outlen + 1);
	p = out;

	if (neg)
		*p++ = '-';

	for (i = nd - 1; i >= 0; i--)
		*p++ = digits[i];

	*p = '\0';  /* fixed misleading indentation */

	return out;
}


/*
 * reverse_byte_array
 *
 * reverse the given byte array
 */
static void
reverse_byte_array(unsigned char * array, int length)
{
	size_t start = 0;
	size_t end = length - 1;
	while (start < end)
	{
		unsigned char temp = array[start];
		array[start] = array[end];
		array[end] = temp;
		start++;
		end--;
	}
}

/*
 * trim_leading_zeros
 *
 * trim the leading zeros from the given string
 */
static void
trim_leading_zeros(char *str)
{
	int i = 0, j = 0;
	while (str[i] == '0')
	{
		i++;
	}

	if (str[i] == '\0')
	{
		str[0] = '0';
		str[1] = '\0';
		return;
	}

	while (str[i] != '\0')
	{
		str[j++] = str[i++];
	}
	str[j] = '\0';
}

/*
 * prepend_zeros
 *
 * prepend zeros to the given string
 */
static void
prepend_zeros(char *str, int num_zeros)
{
    int original_len = strlen(str);
    int new_len = original_len + num_zeros;
    char * temp = palloc0(new_len + 1);

    for (int i = 0; i < num_zeros; i++)
    {
        temp[i] = '0';
    }

    for (int i = 0; i < original_len; i++)
    {
        temp[i + num_zeros] = str[i];
    }
    temp[new_len] = '\0';
    strcpy(str, temp);
    pfree(temp);
}

/*
 * byte_to_binary
 *
 * convert the given byte to a binary string with 1s and 0s
 */
static void
byte_to_binary(unsigned char byte, char * binary_str)
{
	for (int i = 7; i >= 0; i--)
	{
		binary_str[7 - i] = (byte & (1 << i)) ? '1' : '0';
	}
	binary_str[8] = '\0';
}

/*
 * bytes_to_binary_string
 *
 * convert the given bytes to a binary string with 1s and 0s
 */
static void
bytes_to_binary_string(const unsigned char * bytes, size_t len, char * binary_str)
{
	char byte_str[9];
	size_t i = 0;
	binary_str[0] = '\0';

	for (i = 0; i < len; i++)
	{
		byte_to_binary(bytes[i], byte_str);
		strcat(binary_str, byte_str);
	}
}

/*
 * transform_data_expression
 *
 * return the expression to run on the given column name based on the transform
 * object rule definitions
 */
static char *
transform_data_expression(const char * remoteObjid, const char * colname)
{
	TransformExpressionHashEntry * entry = NULL;
	TransformExpressionHashKey key = {0};
	bool found = false;
	char * res = NULL;

	/*
	 * return NULL immediately if objectMappingHash has not been initialized. Most
	 * likely the connector does not have a rule file specified
	 */
	if (!transformExpressionHash)
		return NULL;

	if (!remoteObjid || !colname)
		return NULL;

	/*
	 * expression lookup key consists of [remoteobjid].[colname] and remoteobjid consists of
	 * [database].[schema].[table] or [database].[table]
	 */
	snprintf(key.extObjName, sizeof(key.extObjName), "%s.%s", remoteObjid, colname);
	entry = (TransformExpressionHashEntry *) hash_search(transformExpressionHash, &key, HASH_FIND, &found);
	if (!found)
	{
		/* no object mapping found, so no transformation done */
		elog(DEBUG1, "no data transformation needed for %s", key.extObjName);
	}
	else
	{
		/* return the expression to run */
		elog(DEBUG1, "%s needs data transformation with expression '%s'",
				key.extObjName, entry->pgsqlTransExpress);
		res = pstrdup(entry->pgsqlTransExpress);
	}
	return res;
}


/*
 * populate_primary_keys
 *
 * this function constructs primary key clauses based on jsonin. jsonin
 * is expected to be a json array with string element, for example:
 * ["col1","col2"]
 */
static void
populate_primary_keys(StringInfoData * strinfo, const char * id, const char * jsonin, bool alter, bool isinline)
{
	Datum jsonb_datum;
	Jsonb * jb;
	JsonbIterator *it;
	JsonbIteratorToken r;
	JsonbValue v;
	char * value = NULL;
	bool isfirst = true;

	if (!jsonin)
	{
		elog(DEBUG1, "no valid primary key json");
		return;
	}

	/* Convert event string to JSONB */
	PG_TRY();
	{
		jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(jsonin));
		jb = DatumGetJsonbP(jsonb_datum);
	}
	PG_CATCH();
	{
		FlushErrorState();
		elog(WARNING, "bad primary key json message: %s", jsonin);
		return;
	}
	PG_END_TRY();

	it = JsonbIteratorInit(&jb->root);
	while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		switch (r)
		{
			case WJB_BEGIN_ARRAY:
				break;
			case WJB_END_ARRAY:
				/*
				 * if at least one primary key is appended, we need to remove the last comma
				 * and close parenthesis
				 */
				if (!isfirst)
				{
					strinfo->data[strinfo->len - 1] = '\0';
					strinfo->len = strinfo->len - 1;
					appendStringInfo(strinfo, ")");
				}
				break;
			case WJB_VALUE:
			case WJB_ELEM:
			{
				switch(v.type)
				{
					case jbvString:
					{
						char * mappedColumnName = NULL;
						StringInfoData colNameObjId;

						value = pnstrdup(v.val.string.val, v.val.string.len);
						elog(DEBUG1, "primary key column: %s", value);

						/* transform the column name if needed */
						initStringInfo(&colNameObjId);

						/* express a column name in fully qualified id */
						appendStringInfo(&colNameObjId, "%s.%s", id, value);
						mappedColumnName = transform_object_name(colNameObjId.data, "column");
						if (mappedColumnName)
						{
							/* replace the column name with looked up value here */
							pfree(value);
							value = pstrdup(mappedColumnName);
						}

						if(colNameObjId.data)
							pfree(colNameObjId.data);

						if (alter)
						{
							if (isinline)
							{
								if (isfirst)
								{
									appendStringInfo(strinfo, ", ADD PRIMARY KEY(");
									appendStringInfo(strinfo, "%s,", value);
									isfirst = false;
								}
								else
								{
									appendStringInfo(strinfo, "%s,", value);
								}
							}
							else
							{
								if (isfirst)
								{
									appendStringInfo(strinfo, "PRIMARY KEY(");
									appendStringInfo(strinfo, "%s,", value);
									isfirst = false;
								}
								else
								{
									appendStringInfo(strinfo, "%s,", value);
								}
							}
						}
						else
						{
							if (isfirst)
							{
								appendStringInfo(strinfo, ", PRIMARY KEY(");
								appendStringInfo(strinfo, "%s,", value);
								isfirst = false;
							}
							else
							{
								appendStringInfo(strinfo, "%s,", value);
							}
						}
						pfree(value);
						break;
					}
					case jbvNull:
					case jbvNumeric:
					case jbvBool:
					case jbvBinary:
					default:
						elog(ERROR, "Unknown or unexpected value type: %d while "
								"parsing primaryKeyColumnNames", v.type);
						break;
				}
				break;
			}
			case WJB_BEGIN_OBJECT:
			case WJB_END_OBJECT:
			case WJB_KEY:
			default:
			{
				elog(ERROR, "Unknown or unexpected token: %d while "
						"parsing primaryKeyColumnNames", r);
				break;
			}
		}
	}
}

/*
 * init_mysql
 *
 * initialize data type hash table for mysql database
 */
static void
init_mysql(void)
{
	HASHCTL	info;
	int i = 0;
	DatatypeHashEntry * entry;
	bool found = 0;

	info.keysize = sizeof(DatatypeHashKey);
	info.entrysize = sizeof(DatatypeHashEntry);
	info.hcxt = TopMemoryContext;

	mysqlDatatypeHash = hash_create("mysql datatype hash",
							 256,
							 &info,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	for (i = 0; i < SIZE_MYSQL_DATATYPE_MAPPING; i++)
	{
		entry = (DatatypeHashEntry *) hash_search(mysqlDatatypeHash, &(mysql_defaultTypeMappings[i].key), HASH_ENTER, &found);
		if (!found)
		{
			entry->key.autoIncremented = mysql_defaultTypeMappings[i].key.autoIncremented;
			memset(entry->key.extTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strncpy(entry->key.extTypeName,
					mysql_defaultTypeMappings[i].key.extTypeName,
					strlen(mysql_defaultTypeMappings[i].key.extTypeName));

			entry->pgsqlTypeLength = mysql_defaultTypeMappings[i].pgsqlTypeLength;
			memset(entry->pgsqlTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strncpy(entry->pgsqlTypeName,
					mysql_defaultTypeMappings[i].pgsqlTypeName,
					strlen(mysql_defaultTypeMappings[i].pgsqlTypeName));

			elog(DEBUG1, "Inserted mapping '%s' <-> '%s'", entry->key.extTypeName, entry->pgsqlTypeName);
		}
		else
		{
			elog(DEBUG1, "mapping exists '%s' <-> '%s'", entry->key.extTypeName, entry->pgsqlTypeName);
		}
	}
}

/*
 * init_oracle
 *
 * initialize data type hash table for oracle database
 */
static void
init_oracle(void)
{
	HASHCTL	info;
	int i = 0;
	DatatypeHashEntry * entry;
	bool found = 0;

	info.keysize = sizeof(DatatypeHashKey);
	info.entrysize = sizeof(DatatypeHashEntry);
	info.hcxt = TopMemoryContext;

	oracleDatatypeHash = hash_create("oracle datatype hash",
							 256,
							 &info,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	for (i = 0; i < SIZE_ORACLE_DATATYPE_MAPPING; i++)
	{
		entry = (DatatypeHashEntry *) hash_search(oracleDatatypeHash, &(oracle_defaultTypeMappings[i].key), HASH_ENTER, &found);
		if (!found)
		{
			entry->key.autoIncremented = oracle_defaultTypeMappings[i].key.autoIncremented;
			memset(entry->key.extTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strncpy(entry->key.extTypeName,
					oracle_defaultTypeMappings[i].key.extTypeName,
					strlen(oracle_defaultTypeMappings[i].key.extTypeName));

			entry->pgsqlTypeLength = oracle_defaultTypeMappings[i].pgsqlTypeLength;
			memset(entry->pgsqlTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strncpy(entry->pgsqlTypeName,
					oracle_defaultTypeMappings[i].pgsqlTypeName,
					strlen(oracle_defaultTypeMappings[i].pgsqlTypeName));

			elog(DEBUG1, "Inserted mapping '%s' <-> '%s'", entry->key.extTypeName, entry->pgsqlTypeName);
		}
		else
		{
			elog(DEBUG1, "mapping exists '%s' <-> '%s'", entry->key.extTypeName, entry->pgsqlTypeName);
		}
	}
}

/*
 * init_sqlserver
 *
 * initialize data type hash table for sqlserver database
 */
static void
init_sqlserver(void)
{
	HASHCTL	info;
	int i = 0;
	DatatypeHashEntry * entry;
	bool found = 0;

	info.keysize = sizeof(DatatypeHashKey);
	info.entrysize = sizeof(DatatypeHashEntry);
	info.hcxt = TopMemoryContext;

	sqlserverDatatypeHash = hash_create("sqlserver datatype hash",
							 256,
							 &info,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	for (i = 0; i < SIZE_SQLSERVER_DATATYPE_MAPPING; i++)
	{
		entry = (DatatypeHashEntry *) hash_search(sqlserverDatatypeHash, &(sqlserver_defaultTypeMappings[i].key), HASH_ENTER, &found);
		if (!found)
		{
			entry->key.autoIncremented = sqlserver_defaultTypeMappings[i].key.autoIncremented;
			memset(entry->key.extTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strncpy(entry->key.extTypeName,
					sqlserver_defaultTypeMappings[i].key.extTypeName,
					strlen(sqlserver_defaultTypeMappings[i].key.extTypeName));

			entry->pgsqlTypeLength = sqlserver_defaultTypeMappings[i].pgsqlTypeLength;
			memset(entry->pgsqlTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strncpy(entry->pgsqlTypeName,
					sqlserver_defaultTypeMappings[i].pgsqlTypeName,
					strlen(sqlserver_defaultTypeMappings[i].pgsqlTypeName));

			elog(DEBUG1, "Inserted mapping '%s' <-> '%s'", entry->key.extTypeName, entry->pgsqlTypeName);
		}
		else
		{
			elog(DEBUG1, "mapping exists '%s' <-> '%s'", entry->key.extTypeName, entry->pgsqlTypeName);
		}
	}
}

static int
alter_tbname(const char * from, const char * to)
{
	/* work on copies of the inputs */
	char * fromcopy = pstrdup(from);
	char * tocopy = pstrdup(to);
	char * db = NULL, * schema = NULL, * table = NULL;
	char * db2 = NULL, * schema2 = NULL, * table2 = NULL;
	StringInfoData strinfo;
	int ret = -1;

	if (!from || !to)
		return ret;

	initStringInfo(&strinfo);
	splitIdString(fromcopy, &db, &schema, &table, false);
	if (table && schema)
	{
		/* 'from' expressed as schema.table */
		splitIdString(tocopy, &db2, &schema2, &table2, false);
		if (table2 && schema2)
		{
			/* 'to' expressed as schema.table */
			appendStringInfo(&strinfo, "CREATE SCHEMA IF NOT EXISTS %s; "
					"ALTER TABLE %s RENAME TO %s; "
					"ALTER TABLE %s.%s SET SCHEMA %s;",
					schema2, from, table2, schema, table2, schema2);
		}
		else
		{
			/* 'to' expressed as table */
			appendStringInfo(&strinfo, "ALTER TABLE %s RENAME TO %s;"
					"ALTER TABLE %s.%s SET SCHEMA public;",
					from, table2, schema, table2);
		}
	}
	else
	{
		/* 'from' expressed as table */
		splitIdString(tocopy, &db2, &schema2, &table2, false);
		if (table2 && schema2)
		{
			/* 'to' expressed as schema.table */
			appendStringInfo(&strinfo, "CREATE SCHEMA IF NOT EXISTS %s; "
					"ALTER TABLE %s RENAME TO %s; "
					"ALTER TABLE %s SET SCHEMA %s;",
					schema2, from, table2, table2, schema2);
		}
		else
		{
			/* 'to' expressed as table */
			appendStringInfo(&strinfo, "ALTER TABLE %s RENAME TO %s;",
					from, table2);
		}
	}

	elog(WARNING, "renaming table from '%s' to '%s' with query: %s", from, to, strinfo.data);
	ret = ra_executeCommand(strinfo.data);

	pfree(fromcopy);
	pfree(tocopy);
	if (strinfo.data)
		pfree(strinfo.data);

	return ret;
}

static int
alter_attname(const char * tbname, const char * from, const char * to)
{
	int ret = -1;
	StringInfoData strinfo;

	if (!tbname || !from || !to)
		return ret;

	initStringInfo(&strinfo);
	appendStringInfo(&strinfo, "ALTER TABLE %s RENAME COLUMN %s TO %s;",
			tbname, from, to);

	elog(WARNING, "renaming table ('%s')'s column from '%s' to '%s' with query: %s",
			tbname, from, to, strinfo.data);
	ret = ra_executeCommand(strinfo.data);

	if (strinfo.data)
		pfree(strinfo.data);

	return ret;
}

static int
alter_atttype(const char * tbname, const char * from, const char * to, int typesz, const char * convertfunc)
{
	int ret = -1;
	StringInfoData strinfo;

	if (!tbname || !from || !to)
		return ret;

	initStringInfo(&strinfo);
	appendStringInfo(&strinfo, "ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s",
			tbname, from, to);

	if (typesz > 0)
	{
		appendStringInfo(&strinfo, "(%d)", typesz);
	}

	if (convertfunc)
		appendStringInfo(&strinfo, " USING %s::%s;", tbname, convertfunc);
	else
		appendStringInfo(&strinfo, ";");

	elog(WARNING, "alter data type for table ('%s') column ('%s') to '%s' with query: %s",
			tbname, from, to, strinfo.data);
	ret = ra_executeCommand(strinfo.data);

	if (strinfo.data)
		pfree(strinfo.data);

	return ret;
}

/*
 * transformDDLColumns
 *
 * Function to transform DDL columns, strinfo and pgcol will be filled according to transformation rules
 */
static void
transformDDLColumns(const char * id, DBZ_DDL_COLUMN * col, ConnectorType conntype,
					bool datatypeonly, StringInfoData * strinfo, PG_DDL_COLUMN * pgcol)
{
	/* transform the column name if needed */
	char * mappedColumnName = NULL;
	StringInfoData colNameObjId;

	initStringInfo(&colNameObjId);
	/* express a column name in fully qualified id */
	appendStringInfo(&colNameObjId, "%s.%s", id, col->name);

	mappedColumnName = transform_object_name(colNameObjId.data, "column");

	if (mappedColumnName)
	{
		elog(DEBUG1, "transformed column object ID '%s'to '%s'",
				colNameObjId.data, mappedColumnName);
		pgcol->attname = pstrdup(mappedColumnName);
	}
	else
		pgcol->attname = pstrdup(col->name);

	switch (conntype)
	{
		case TYPE_MYSQL:
		{
			DatatypeHashEntry * entry;
			DatatypeHashKey key = {0};
			bool found = 0;

			/*
			 * check if there is a translation rule applied specifically for this column using
			 * key format: [column object id]
			 */
			key.autoIncremented = col->autoIncremented;
			snprintf(key.extTypeName, sizeof(key.extTypeName), "%s", colNameObjId.data);

			entry = (DatatypeHashEntry *) hash_search(mysqlDatatypeHash, &key, HASH_FIND, &found);
			if (!found)
			{
				/*
				 * no mapping found, so no data type translation for this particular column.
				 * Now, check if there is a global data type translation rule
				 */
				memset(&key, 0, sizeof(DatatypeHashKey));
				if (!strcasecmp(col->typeName, "bit") && col->length == 1)
				{
					/* special lookup case: BIT with length 1 */
					key.autoIncremented = col->autoIncremented;
					snprintf(key.extTypeName, sizeof(key.extTypeName), "%s(%d)",
							col->typeName, col->length);
				}
				else
				{
					/* all other cases - no special handling */
					key.autoIncremented = col->autoIncremented;
					snprintf(key.extTypeName, sizeof(key.extTypeName), "%s",
							col->typeName);
				}
				entry = (DatatypeHashEntry *) hash_search(mysqlDatatypeHash, &key, HASH_FIND, &found);
				if (!found)
				{
					/* no mapping found, so no transformation done */
					elog(DEBUG1, "no transformation done for %s (autoincrement %d)",
							key.extTypeName, key.autoIncremented);
					if (datatypeonly)
						appendStringInfo(strinfo, " %s ", col->typeName);
					else
						appendStringInfo(strinfo, " %s %s ", pgcol->attname, col->typeName);

					pgcol->atttype = pstrdup(col->typeName);
				}
				else
				{
					/* use the mapped values and sizes */
					elog(DEBUG1, "transform %s (autoincrement %d) to %s with length %d",
							key.extTypeName, key.autoIncremented, entry->pgsqlTypeName,
							entry->pgsqlTypeLength);
					if (datatypeonly)
						appendStringInfo(strinfo, " %s ", entry->pgsqlTypeName);
					else
						appendStringInfo(strinfo, " %s %s ", pgcol->attname, entry->pgsqlTypeName);

					pgcol->atttype = pstrdup(entry->pgsqlTypeName);
					if (entry->pgsqlTypeLength != -1)
						col->length = entry->pgsqlTypeLength;
				}
			}
			else
			{
				/* use the mapped values and sizes */
				elog(DEBUG1, "transform %s (autoincrement %d) to %s with length %d",
						key.extTypeName, key.autoIncremented, entry->pgsqlTypeName,
						entry->pgsqlTypeLength);

				if (datatypeonly)
					appendStringInfo(strinfo, " %s ", entry->pgsqlTypeName);
				else
					appendStringInfo(strinfo, " %s %s ", pgcol->attname, entry->pgsqlTypeName);

				pgcol->atttype = pstrdup(entry->pgsqlTypeName);
				if (entry->pgsqlTypeLength != -1)
					col->length = entry->pgsqlTypeLength;
			}
			break;
		}
		case TYPE_ORACLE:
		case TYPE_OLR:
		{
			DatatypeHashEntry * entry;
			DatatypeHashKey key = {0};
			bool found = false, removed = false;

			/*
			 * oracle data type may contain length and scale information in the col->typeName,
			 * but these are also available in col->length and col->scale. We need to remove
			 * them here to ensure proper data type transforms. Known data type to have this
			 * addition is INTERVAL DAY(3) TO SECOND(6)
			 */
			remove_precision(col->typeName, &removed);

			/*
			 * for INTERVAL DAY TO SECOND or if precision operators have been removed previously,
			 * we need to make size = scale, and empty the scale to maintain compatibility in
			 * PostgreSQL.
			 */
			if ((!strcasecmp(col->typeName, "interval day to second") && col->scale > 0) || removed)
			{
				col->length = col->scale;
				col->scale = 0;
			}

			key.autoIncremented = col->autoIncremented;
			snprintf(key.extTypeName, sizeof(key.extTypeName), "%s",colNameObjId.data);

			entry = (DatatypeHashEntry *) hash_search(oracleDatatypeHash, &key, HASH_FIND, &found);
			if (!found)
			{
				/*
				 * no mapping found, so no data type translation for this particular column.
				 * Now, check if there is a global data type translation rule
				 */
				memset(&key, 0, sizeof(DatatypeHashKey));
				if (!strcasecmp(col->typeName, "number") && col->scale == 0)
				{
					/*
					 * special case: variable length NUMBER value - re-structure col->typeName so that
					 * it includes length and precision information before we do any data type mapping
					 * lookup. This ensures a more granular mappings. We only do this when col->scale
					 * is zero because it indicates an integer type, and PostgreSQL has different int
					 * types for different sizes.
					 */
					key.autoIncremented = col->autoIncremented;
					snprintf(key.extTypeName, sizeof(key.extTypeName), "%s(%d,%d)",
							col->typeName, col->length, col->scale);
				}
				else
				{
					/* all other cases - no special handling */
					key.autoIncremented = col->autoIncremented;
					snprintf(key.extTypeName, sizeof(key.extTypeName), "%s",
							col->typeName);
				}

				entry = (DatatypeHashEntry *) hash_search(oracleDatatypeHash, &key, HASH_FIND, &found);
				if (!found)
				{
					/* no mapping found, so no transformation done */
					elog(DEBUG1, "no transformation done for %s (autoincrement %d)",
							key.extTypeName, key.autoIncremented);
					if (datatypeonly)
						appendStringInfo(strinfo, " %s ", col->typeName);
					else
						appendStringInfo(strinfo, " %s %s ", pgcol->attname, col->typeName);

					pgcol->atttype = pstrdup(col->typeName);
				}
				else
				{
					/* use the mapped values and sizes */
					elog(DEBUG1, "transform %s (autoincrement %d) to %s with length %d",
							key.extTypeName, key.autoIncremented, entry->pgsqlTypeName,
							entry->pgsqlTypeLength);
					if (datatypeonly)
						appendStringInfo(strinfo, " %s ", entry->pgsqlTypeName);
					else
						appendStringInfo(strinfo, " %s %s ", pgcol->attname, entry->pgsqlTypeName);

					pgcol->atttype = pstrdup(entry->pgsqlTypeName);
					if (entry->pgsqlTypeLength != -1)
						col->length = entry->pgsqlTypeLength;
				}
			}
			else
			{
				/* use the mapped values and sizes */
				elog(DEBUG1, "transform %s (autoincrement %d) to %s with length %d",
						key.extTypeName, key.autoIncremented, entry->pgsqlTypeName,
						entry->pgsqlTypeLength);

				if (datatypeonly)
					appendStringInfo(strinfo, " %s ", entry->pgsqlTypeName);
				else
					appendStringInfo(strinfo, " %s %s ", pgcol->attname, entry->pgsqlTypeName);

				pgcol->atttype = pstrdup(entry->pgsqlTypeName);
				if (entry->pgsqlTypeLength != -1)
					col->length = entry->pgsqlTypeLength;
			}
			break;
		}
		case TYPE_SQLSERVER:
		{
			DatatypeHashEntry * entry;
			DatatypeHashKey key = {0};
			bool found = 0;

			/*
			 * check if there is a translation rule applied specifically for this column using
			 * key format: [column object id]
			 */
			key.autoIncremented = col->autoIncremented;
			snprintf(key.extTypeName, sizeof(key.extTypeName), "%s", colNameObjId.data);

			entry = (DatatypeHashEntry *) hash_search(sqlserverDatatypeHash, &key, HASH_FIND, &found);
			if (!found)
			{
				/*
				 * no mapping found, so no data type translation for this particular column.
				 * Now, check if there is a global data type translation rule
				 */
				memset(&key, 0, sizeof(DatatypeHashKey));
				if (!strcasecmp(col->typeName, "bit") && col->length == 1)
				{
					/* special lookup case: BIT with length 1 */
					key.autoIncremented = col->autoIncremented;
					snprintf(key.extTypeName, sizeof(key.extTypeName), "%s(%d)",
							col->typeName, col->length);
				}
				else
				{
					/* all other cases - no special handling */
					key.autoIncremented = col->autoIncremented;
					snprintf(key.extTypeName, sizeof(key.extTypeName), "%s",
							col->typeName);
				}
				entry = (DatatypeHashEntry *) hash_search(sqlserverDatatypeHash, &key, HASH_FIND, &found);
				if (!found)
				{
					/* no mapping found, so no transformation done */
					elog(DEBUG1, "no transformation done for %s (autoincrement %d)",
							key.extTypeName, key.autoIncremented);
					if (datatypeonly)
						appendStringInfo(strinfo, " %s ", col->typeName);
					else
						appendStringInfo(strinfo, " %s %s ", pgcol->attname, col->typeName);

					pgcol->atttype = pstrdup(col->typeName);
				}
				else
				{
					/* use the mapped values and sizes */
					elog(DEBUG1, "transform %s (autoincrement %d) to %s with length %d",
							key.extTypeName, key.autoIncremented, entry->pgsqlTypeName,
							entry->pgsqlTypeLength);
					if (datatypeonly)
						appendStringInfo(strinfo, " %s ", entry->pgsqlTypeName);
					else
						appendStringInfo(strinfo, " %s %s ", pgcol->attname, entry->pgsqlTypeName);

					pgcol->atttype = pstrdup(entry->pgsqlTypeName);
					if (entry->pgsqlTypeLength != -1)
						col->length = entry->pgsqlTypeLength;
				}
			}
			else
			{
				/* use the mapped values and sizes */
				elog(DEBUG1, "transform %s (autoincrement %d) to %s with length %d",
						key.extTypeName, key.autoIncremented, entry->pgsqlTypeName,
						entry->pgsqlTypeLength);

				if (datatypeonly)
					appendStringInfo(strinfo, " %s ", entry->pgsqlTypeName);
				else
					appendStringInfo(strinfo, " %s %s ", pgcol->attname, entry->pgsqlTypeName);

				pgcol->atttype = pstrdup(entry->pgsqlTypeName);
				if (entry->pgsqlTypeLength != -1)
					col->length = entry->pgsqlTypeLength;
			}

			/*
			 * special handling for sqlserver: the scale parameter for timestamp,
			 * and time date types are sent as "scale" not as "length" as in
			 * mysql case. So we need to use the scale value here
			 */
			if (col->scale > 0 && (find_exact_string_match(entry->pgsqlTypeName, "timestamp") ||
					find_exact_string_match(entry->pgsqlTypeName, "time") ||
					find_exact_string_match(entry->pgsqlTypeName, "timestamptz")))
			{
				/* postgresql can only support up to 6 */
				if (col->scale > 6)
					appendStringInfo(strinfo, "(6) ");
				else
					appendStringInfo(strinfo, "(%d) ", col->scale);
			}
			break;
		}
		default:
		{
			/* unknown type, no special handling - may error out later when applying to PostgreSQL */
			appendStringInfo(strinfo, " %s %s ", col->name, col->typeName);
			break;
		}
	}

	if (colNameObjId.data)
		pfree(colNameObjId.data);
}

/*
 * composeAlterColumnClauses
 *
 * This functions build the ALTER COLUM SQL clauses based on given inputs
 */
static char *
composeAlterColumnClauses(const char * objid, ConnectorType type, List * dbzcols, TupleDesc tupdesc, Relation rel, PG_DDL * pgddl)
{
	ListCell * cell;
	int attnum = 1;
	StringInfoData colNameObjId;
	StringInfoData strinfo;
	char * mappedColumnName = NULL;
	char * ret = NULL;
	bool found = false;
	Bitmapset * pkattrs;
	bool atleastone = false;
	PG_DDL_COLUMN * pgcol = NULL;

	initStringInfo(&colNameObjId);
	initStringInfo(&strinfo);
	pkattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_PRIMARY_KEY);

	foreach(cell, dbzcols)
	{
		DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);
		pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));

		resetStringInfo(&colNameObjId);
		appendStringInfo(&colNameObjId, "%s.%s", objid, col->name);
		mappedColumnName = transform_object_name(colNameObjId.data, "column");

		/* use the name as it came if no column name mapping found */
		if (!mappedColumnName)
			mappedColumnName = pstrdup(col->name);

		found = false;
		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			/* skip dropped columns */
			if (attr->attisdropped)
				continue;

			/* found a matching column, build the alter column clauses */
			if (!strcasecmp(mappedColumnName, NameStr(attr->attname)))
			{
				found = true;

				/* skip ALTER on primary key columns */
				if (pkattrs != NULL && bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber, pkattrs))
					continue;

				/* check data type */
				appendStringInfo(&strinfo, "ALTER COLUMN %s SET DATA TYPE",
						mappedColumnName);
				transformDDLColumns(objid, col, type, true, &strinfo, pgcol);
				if (col->length > 0 && col->scale > 0)
				{
					appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
				}

				/* if a only length if specified, add it. For example VARCHAR(30)*/
				if (col->length > 0 && col->scale == 0)
				{
					/* make sure it does not exceed postgresql allowed maximum */
					if (col->length > MaxAttrSize)
						col->length = MaxAttrSize;
					appendStringInfo(&strinfo, "(%d) ", col->length);
				}

				/*
				 * todo: for complex data type transformation, postgresql requires
				 * the user to specify a function to cast existing values to the new
				 * data type via the USING clause. This is needed for INT -> TEXT,
				 * INT -> DATE or NUMERIC -> VARCHAR. We do not support USING now as
				 * we do not know what function the user wants to use for casting the
				 * values. Perhaps we can include these cast functions in the rule file
				 * as well, but for now this is not supported and PostgreSQL may complain
				 * if we attempt to do complex data type changes.
				 */
				appendStringInfo(&strinfo, ", ");

				if (col->defaultValueExpression)
				{
					/*
					 * synchdb can receive a default expression not supported in postgresql.
					 * so for now, we always set to default null. todo
					 */
					appendStringInfo(&strinfo, "ALTER COLUMN %s SET DEFAULT %s",
							mappedColumnName, "NULL");
				}
				else
				{
					/* remove default value */
					appendStringInfo(&strinfo, "ALTER COLUMN %s DROP DEFAULT",
							mappedColumnName);
				}

				appendStringInfo(&strinfo, ", ");

				/* check if nullable or not nullable */
				if (!col->optional)
				{
					appendStringInfo(&strinfo, "ALTER COLUMN %s SET NOT NULL",
							mappedColumnName);
				}
				else
				{
					appendStringInfo(&strinfo, "ALTER COLUMN %s DROP NOT NULL",
							mappedColumnName);
				}
				appendStringInfo(&strinfo, ",");
				atleastone = true;

				pgcol->position = attnum;
			}
		}
		if (!found)
		{
			/*
			 * todo: support renamed columns? The challenge is to find out which column
			 * got renamed on remote site because dbz did not tell us the old column name
			 * that was renamed. Only the new name is given to us to figure it out :(
			 */
			elog(WARNING, "column %s missing in PostgreSQL, indicating a renamed column?! -"
					"Not supported now",
					mappedColumnName);
		}
		pgddl->columns = lappend(pgddl->columns, pgcol);
	}

	/* remove extra "," */
	strinfo.data[strinfo.len - 1] = '\0';
	strinfo.len = strinfo.len - 1;

	ret = pstrdup(strinfo.data);

	if(strinfo.data)
		pfree(strinfo.data);

	if (pkattrs != NULL)
		bms_free(pkattrs);

	/* no column altered, return null */
	if (!atleastone)
		return NULL;

	return ret;
}

/*
 * this function handles and expands a value with dbztype struct.
 * We currently assumes it has strucutre like this:
 *       "ID":
 *       {
 *			"scale": 0,
 *			"value": "AQ=="
 *		 }
 * in Oracle's variable scale case. There are also other cases
 * such as MySQL's geometry data, that we do not support yet.
 */
static void
expand_struct_value(char * in, DBZ_DML_COLUMN_VALUE * colval, ConnectorType conntype)
{
	StringInfoData strinfo;
	Datum jsonb_datum;
	Jsonb *jb;

	switch (conntype)
	{
		case TYPE_ORACLE:
		{
			/*
			 * variable scale struct in Oracle looks like:
			 * 	"ID":
			 *	{
			 *		"scale": 0,
			 *		"value": "AQ=="
			 *	}
			 */
			if (colval->timerep == DATA_VARIABLE_SCALE)
			{
				initStringInfo(&strinfo);

				PG_TRY();
				{
					jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(in));
					jb = DatumGetJsonbP(jsonb_datum);
				}
				PG_CATCH();
				{
					FlushErrorState();
					elog(WARNING, "bad json struct to expand: %s", in);
					return;
				}
				PG_END_TRY();

				getPathElementString(jb, "scale", &strinfo, true);
				if (!strcasecmp(strinfo.data, "null"))
					colval->scale = 0;
				else
					colval->scale = atoi(strinfo.data);

				elog(DEBUG1, "colval->scale is set to %d", colval->scale);
				getPathElementString(jb, "value", &strinfo, true);
				if (!strcasecmp(strinfo.data, "null"))
				{
					elog(WARNING, "JSON has scale but with no value");
				}
				else
				{
					/* replace colval->value with the new value derived from the JSON */
					pfree(colval->value);
					colval->value = pstrdup(strinfo.data);

					/* make "in" points to colval->value for subsequent processing */
					in = colval->value;
					elog(DEBUG1, "colval->value is set to %s", colval->value);
				}
				if(strinfo.data)
					pfree(strinfo.data);
			}
			break;
		}
		case TYPE_MYSQL:
		case TYPE_SQLSERVER:
		default:
		{
			/* todo */
			elog(WARNING, "struct expansion for mysql and sqlserver can be added here"
					"once we come across them");
			break;
		}
	}
}
static char *
handle_base64_to_numeric_with_scale(const char * in, int scale)
{
	int newlen = 0, decimalpos = 0;
	char * value = 0;
	char buffer[128] = {0};
	int tmpoutlen = pg_b64_dec_len(strlen(in));
	unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen + 1);
	char * out = NULL;
	bool isneg = false;

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
	tmpoutlen = pg_b64_decode(in, strlen(in), tmpout, tmpoutlen);
#else
	tmpoutlen = pg_b64_decode(in, strlen(in), (char *)tmpout, tmpoutlen);
#endif
	value = derive_decimal_string_from_byte(tmpout, tmpoutlen);
	/* if value is negative, we set isneg = true */
	if (value[0] == '-')
	{
		isneg = true;
		/* skip minus sign for processing below */
		snprintf(buffer, sizeof(buffer), "%s", value + 1);
	}
	else
		snprintf(buffer, sizeof(buffer), "%s", value);
	if (scale > 0)
	{
		if (strlen(buffer) > scale)
		{
			/* ex: 123 -> 1.23*/
			newlen = strlen(buffer) + 1 + (isneg ? 1 : 0);	/* plus 1 decimal and minus sign is needed */
			out = (char *) palloc0(newlen + 1);	/* plus 1 terminating null */

			decimalpos = strlen(buffer) - scale;
			strncpy(out + (isneg ? 1 : 0), buffer, decimalpos);
			out[decimalpos + (isneg ? 1 : 0)] = '.';
			strcpy(out + decimalpos + 1 + (isneg ? 1 : 0), buffer + decimalpos);
			if (isneg)
				out[0] = '-';
		}
		else if (strlen(buffer) == scale)
		{
			/* ex: 123 -> 0.123 */
			newlen = strlen(buffer) + 2 + (isneg ? 1 : 0);	/* plus 1 decimal, 1 zero and minus sign is needed*/
			out = (char *) palloc0(newlen + 1);	/* plus 1 terminating null */
			if (isneg)
				snprintf(out, newlen + 1, "-0.%s", buffer);
			else
				snprintf(out, newlen + 1, "0.%s", buffer);
		}
		else
		{
			/* ex: 1 -> 0.001 when scale = 3 */
			int i = 0;
			int val_len = strlen(buffer);
			int zero_count = scale - val_len;
			char *p;

			/* plus 1 decimal, 1 zero, minus sign if needed and leading zeros */
			newlen = val_len + zero_count + 2 + (isneg ? 1 : 0);
			out = (char *) palloc0(newlen + 1);  /* plus terminating null */

			p = out;
			if (isneg)
				*p++ = '-';

			*p++ = '0';
			*p++ = '.';

			/* insert leading zeros */
			for (i = 0; i < zero_count; i++)
				*p++ = '0';

			/* copy the digits from buffer */
			strcpy(p, buffer);
		}
	}
	else
	{
		newlen = strlen(buffer) + (isneg ? 1 : 0);	/* no decimal + minus sign if needed */
		out = (char *) palloc0(newlen + 1);
		if (isneg)
			snprintf(out, newlen + 1, "-%s", buffer);
		else
			strlcpy(out, buffer, newlen + 1);
	}
	return out;
}

static char *
handle_string_to_numeric(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to numeric. May fail to apply
	 * if it contains non-numeric characters
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
handle_base64_to_bit(const char * in, bool addquote, int typemod)
{
	int tmpoutlen = pg_b64_dec_len(strlen(in));
	unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen);
	char * out = NULL;

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
	tmpoutlen = pg_b64_decode(in, strlen(in), tmpout, tmpoutlen);
#else
	tmpoutlen = pg_b64_decode(in, strlen(in), (char*)tmpout, tmpoutlen);
#endif
	if (addquote)
	{
		/* 8 bits per byte + 2 single quotes + b + terminating null */
		char * tmp = NULL;
		out = (char *) palloc0((tmpoutlen * 8) + 2 + 1 + 1);
		tmp = out;
		reverse_byte_array(tmpout, tmpoutlen);
		strcat(tmp, "'b");
		tmp += 2;
		bytes_to_binary_string(tmpout, tmpoutlen, tmp);
		trim_leading_zeros(tmp);
		if (strlen(tmp) < typemod)
			prepend_zeros(tmp, typemod - strlen(tmp));

		strcat(tmp, "'");
	}
	else
	{
		/* 8 bits per byte + terminating null */
		out = (char *) palloc0(tmpoutlen * 8 + 1);
		reverse_byte_array(tmpout, tmpoutlen);
		bytes_to_binary_string(tmpout, tmpoutlen, out);
		trim_leading_zeros(out);
		if (strlen(out) < typemod)
			prepend_zeros(out, typemod - strlen(out));
	}
	pfree(tmpout);
	return out;
}

static char *
handle_string_to_bit(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to bit. May fail to apply
	 * if it contains non-bit characters
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
handle_numeric_to_bit(const char * in, bool addquote)
{
	/*
	 * no special handling to convert numeric to bit. May fail to apply
	 * if it contains non-bit numerics
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
construct_datestr(long long input, bool addquote, int timerep)
{
	char * out = NULL;
	time_t dayssinceepoch = 0;
	struct tm epoch = {0};
	time_t epoch_time, target_time;
	struct tm *target_date;
	char datestr[10 + 1]; /* YYYY-MM-DD */

	switch (timerep)
	{
		case TIME_DATE:
			/* number of days since epoch, no conversion needed */
			dayssinceepoch = (time_t) input;
			break;
		case TIME_TIMESTAMP:
			/* number of milliseconds since epoch - convert to days since epoch */
			dayssinceepoch = (time_t)(input / 86400000LL);
			break;
		case TIME_MICROTIMESTAMP:
			/* number of microseconds since epoch - convert to days since epoch */
			dayssinceepoch = (time_t)(input / 86400000000LL);
			break;
		case TIME_NANOTIMESTAMP:
			/* number of microseconds since epoch - convert to days since epoch */
			dayssinceepoch = (time_t)(input / 86400000000000LL);
			break;
		case TIME_UNDEF:
		default:
		{
			/* todo: intelligently figure out the right timerep based on input */
			set_shm_connector_errmsg(myConnectorId, "no time representation available to"
					"process DATEOID value");
			elog(ERROR, "no time representation available to process DATEOID value");
		}
	}

	/* since 1970-01-01 */
	epoch.tm_year = 70;
	epoch.tm_mon = 0;
	epoch.tm_mday = 1;

	epoch_time = mktime(&epoch);
	target_time = epoch_time + (dayssinceepoch * 24 * 60 * 60);

	/*
	 * Convert to struct tm in GMT timezone for now
	 * todo: convert to local timezone?
	 */
	target_date = gmtime(&target_time);
	strftime(datestr, sizeof(datestr), "%Y-%m-%d", target_date);

	if (addquote)
	{
		out = (char *) palloc0(strlen(datestr) + 2 + 1);
		snprintf(out, strlen(datestr) + 2 + 1, "'%s'", datestr);
	}
	else
	{
		out = (char *) palloc0(strlen(datestr) + 1);
		strcpy(out, datestr);
	}
	return out;
}

static char *
handle_base64_to_date(const char * in, bool addquote, int timerep)
{
	long long input = 0;
	int tmpoutlen = pg_b64_dec_len(strlen(in));
	unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen + 1);

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
	tmpoutlen = pg_b64_decode(in, strlen(in), tmpout, tmpoutlen);
#else
	tmpoutlen = pg_b64_decode(in, strlen(in), (char *)tmpout, tmpoutlen);
#endif
	input = derive_value_from_byte(tmpout, tmpoutlen);
	return construct_datestr(input, addquote, timerep);
}

static char *
handle_numeric_to_date(const char * in, bool addquote, int timerep)
{
	long long input = atoll(in);

	return construct_datestr(input, addquote, timerep);
}

static char *
handle_string_to_date(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to date. May fail to apply
	 * if it contains unsupported date format
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
construct_timestampstr(long long input, bool addquote, int timerep, int typemod)
{
	char * out = NULL;
	time_t seconds = 0, remains = 0;
	struct tm *tm_info;
	char timestamp[26 + 1] = {0};	/* yyyy-MM-dd hh:mm:ss.xxxxxx */

	switch (timerep)
	{
		case TIME_TIMESTAMP:
			/* milliseconds since epoch - convert to seconds since epoch */
			seconds = (time_t)(input / 1000);
			remains = input % 1000;
			break;
		case TIME_MICROTIMESTAMP:
			/* microseconds since epoch - convert to seconds since epoch */
			seconds = (time_t)(input / 1000 / 1000);
			remains = input % 1000000;
			break;
		case TIME_NANOTIMESTAMP:
			/* microseconds since epoch - convert to seconds since epoch */
			seconds = (time_t)(input / 1000 / 1000 / 1000);
			remains = input % 1000000000;
			break;
		case TIME_UNDEF:
		default:
		{
			/* todo: intelligently figure out the right timerep based on input */
			set_shm_connector_errmsg(myConnectorId, "no time representation available to"
					"process TIMESTAMPOID value");
			elog(ERROR, "no time representation available to process TIMESTAMPOID value");
		}
	}
	tm_info = gmtime(&seconds);

	if (typemod > 0)
	{
		/*
		 * it means we could include additional precision to timestamp. PostgreSQL
		 * supports up to 6 digits of precision. We always put 6, PostgreSQL will
		 * round it up or down as defined by table schema
		 */
		snprintf(timestamp, sizeof(timestamp), "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
				tm_info->tm_year + 1900,
				tm_info->tm_mon + 1,
				tm_info->tm_mday,
				tm_info->tm_hour,
				tm_info->tm_min,
				tm_info->tm_sec,
				remains);
	}
	else
	{
		snprintf(timestamp, sizeof(timestamp), "%04d-%02d-%02d %02d:%02d:%02d",
				tm_info->tm_year + 1900,
				tm_info->tm_mon + 1,
				tm_info->tm_mday,
				tm_info->tm_hour,
				tm_info->tm_min,
				tm_info->tm_sec);
	}

	if (addquote)
	{
		out = (char *) palloc0(strlen(timestamp) + 2 + 1);
		snprintf(out, strlen(timestamp) + 2 + 1, "'%s'", timestamp);
	}
	else
	{
		out = (char *) palloc0(strlen(timestamp) + 1);
		strcpy(out, timestamp);
	}
	return out;
}

static char *
handle_base64_to_timestamp(const char * in, bool addquote, int timerep, int typemod)
{
	long long input = 0;
	int tmpoutlen = pg_b64_dec_len(strlen(in));
	unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen + 1);

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
	tmpoutlen = pg_b64_decode(in, strlen(in), tmpout, tmpoutlen);
#else
	tmpoutlen = pg_b64_decode(in, strlen(in), (char *)tmpout, tmpoutlen);
#endif
	input = derive_value_from_byte(tmpout, tmpoutlen);
	return construct_timestampstr(input, addquote, timerep, typemod);
}

static char *
handle_numeric_to_timestamp(const char * in, bool addquote, int timerep, int typemod)
{
	long long input = atoll(in);

	return construct_timestampstr(input, addquote, timerep, typemod);
}

static char *
handle_string_to_timestamp(const char * in, bool addquote, ConnectorType type)
{
    char * out = NULL;

    if (type == TYPE_OLR)
    {
        /*
         * openlog replicator expressed timestamp with time zone as a string
         * that looks like:
         *
         * 		"1706725800000000000,-08:00"
         *
         * this function will treat the first part of the string as nanosecond
         * since epoch, convert it to timestamp string and append the second
         * part (timezone) to the end of it.
         */
		long long ns;
		char tz[SYNCHDB_MAX_TZ_LEN];
		time_t seconds = 0;
		struct tm t;
		int offset_hours, offset_minutes;
		char sign;

		if (sscanf(in, "%lld,%15s", &ns, tz) != 2)
		{
			elog(WARNING, "invalid OLR timestamp string %s", in);
			return NULL;
		}

		if (sscanf(tz, "%c%2d:%2d", (char *)&sign, &offset_hours, &offset_minutes) != 3)
		{
			elog(WARNING, "invalid OLR timezone string %s", tz);
			return NULL;
		}

		if (sign != '+' && sign != '-')
		{
			elog(WARNING, "invalid timezone sign '%c'", sign);
			return NULL;
		}

		seconds = ns / 1000000000LL;

	    /* apply timezone offset to get local time */
		seconds = sign == '-' ?
				(seconds - (offset_hours * 3600 + offset_minutes * 60)) :
				(seconds + (offset_hours * 3600 + offset_minutes * 60));

		gmtime_r(&seconds, &t);

		out = palloc0(SYNCHDB_MAX_TIMESTAMP_LEN + 1);
		strftime(out, SYNCHDB_MAX_TIMESTAMP_LEN, "%Y-%m-%d %H:%M:%S", &t);
		strcat(out, tz);
    }
    else
    {
        size_t len = strlen(in);
        bool add_tz = (len > 0 && in[len - 1] == 'Z') ? true : false;
        size_t new_len = len + (add_tz ? 5 : 0);
        char * t = NULL;

        /* todo: we assume input is already in correct timestamp format.
         * We may need to check that is true prior to processing
         */
    	if (addquote)
    	{
    		out = (char *) palloc0(new_len + 2 + 1);
    		out[0] = '\'';
    		strcpy(&out[1], in);
    		t = strchr(out, 'T');
    		if (t)
    			*t = ' ';

    		if (add_tz)
    			strcpy(&out[len], "+00:00");
    	}
    	else
    	{
    		out = (char *) palloc0(new_len + 1);
    		strcpy(out, in);
    		t = strchr(out, 'T');
    		if (t)
    			*t = ' ';

    		if (add_tz)
    			strcpy(&out[len - 1], "+00:00");
    	}
    }

    return out;
}

static char *
construct_timetr(long long input, bool addquote, int timerep, int typemod)
{
	time_t seconds = 0, remains = 0;
	char time[15 + 1] = {0};	/* hh:mm:ss.xxxxxx */
	char * out = NULL;

	switch(timerep)
	{
		case TIME_TIME:
			/* milliseconds since midnight - convert to seconds since midnight */
			seconds = (time_t)(input / 1000);
			remains = input % 1000;
			break;
		case TIME_MICROTIME:
			/* microseconds since midnight - convert to seconds since midnight */
			seconds = (time_t)(input / 1000 / 1000);
			remains = input % 1000000;
			break;
		case TIME_NANOTIME:
			/* nanoseconds since midnight - convert to seconds since midnight */
			seconds = (time_t)(input / 1000 / 1000 / 1000);
			remains = input % 1000000000;
			break;
		case TIME_UNDEF:
		default:
		{
			/* todo: intelligently figure out the right timerep based on input */
			set_shm_connector_errmsg(myConnectorId, "no time representation available to"
					"process TIMEOID value");
			elog(ERROR, "no time representation available to process TIMEOID value");
		}
	}
	if (typemod > 0)
	{
		snprintf(time, sizeof(time), "%02d:%02d:%02d.%06ld",
				(int)((seconds / (60 * 60)) % 24),
				(int)((seconds / 60) % 60),
				(int)(seconds % 60),
				remains);
	}
	else
	{
		snprintf(time, sizeof(time), "%02d:%02d:%02d",
				(int)((seconds / (60 * 60)) % 24),
				(int)((seconds / 60) % 60),
				(int)(seconds % 60));
	}

	if (addquote)
	{
		out = (char *) palloc0(strlen(time) + 2 + 1);
		snprintf(out, strlen(time) + 2 + 1, "'%s'", time);
	}
	else
	{
		out = (char *) palloc0(strlen(time) + 1);
		strcpy(out, time);
	}
	return out;
}

static char *
handle_base64_to_time(const char * in, bool addquote, int timerep, int typemod)
{
	long long input = 0;
	int tmpoutlen = pg_b64_dec_len(strlen(in));
	unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen + 1);

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
	tmpoutlen = pg_b64_decode(in, strlen(in), tmpout, tmpoutlen);
#else
	tmpoutlen = pg_b64_decode(in, strlen(in), (char *)tmpout, tmpoutlen);
#endif
	input = derive_value_from_byte(tmpout, tmpoutlen);
	return construct_timetr(input, addquote, timerep, typemod);
}

static char *
handle_numeric_to_time(const char * in, bool addquote, int timerep, int typemod)
{
	unsigned long long input = atoll(in);

	return construct_timetr(input, addquote, timerep, typemod);
}

static char *
handle_string_to_time(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to time. May fail to apply
	 * if it contains non-time characters
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
handle_base64_to_byte(const char * in, bool addquote)
{
	int tmpoutlen = pg_b64_dec_len(strlen(in));
	unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen);
	char * out = NULL;

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
	tmpoutlen = pg_b64_decode(in, strlen(in), tmpout, tmpoutlen);
#else
	tmpoutlen = pg_b64_decode(in, strlen(in), (char*)tmpout, tmpoutlen);
#endif
	if (addquote)
	{
		/* hexstring + 2 single quotes + '\x' + terminating null */
		out = (char *) palloc0((tmpoutlen * 2) + 2 + 2 + 1);
		bytearray_to_escaped_string(tmpout, tmpoutlen, out);
	}
	else
	{
		/* bytearray + terminating null */
		out = (char *) palloc0(tmpoutlen + 1);
		memcpy(out, tmpout, tmpoutlen);
	}
	pfree(tmpout);
	return out;
}

static char *
handle_string_to_byte(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to byte. May fail to apply
	 * if it contains non-byte characters
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
handle_numeric_to_byte(const char * in, bool addquote)
{
	/*
	 * no special handling to convert numeric to byte. May fail to apply
	 * if it contains non-byte numerics
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
construct_intervalstr(long long input, bool addquote, int timerep, int typemod)
{
	time_t seconds = 0, remains = 0;
	char inter[64 + 1] = {0};
	int fields;
	char * out = NULL;

	switch (timerep)
	{
		case TIME_MICRODURATION:
		{
			/* interval in microseconds - convert to seconds */
			seconds = (time_t)(input / 1000 / 1000);
			remains = input % 1000000;
			break;
		}
		default:
		{
			/* todo: intelligently figure out the right timerep based on input */
			set_shm_connector_errmsg(myConnectorId, "no interval representation available to"
					"process INTERVALOID value");
			elog(ERROR, "no interval representation available to process INTERVALOID value");
		}
	}

	/* we need to figure out interval's range and precision from colval->typemod */
	fields = INTERVAL_RANGE(typemod);
	switch (fields)
	{
		case INTERVAL_MASK(YEAR):	/* year */
			snprintf(inter, sizeof(inter), "%d years",
					(int) seconds / SECS_PER_YEAR);
			break;
		case INTERVAL_MASK(MONTH):	/* month */
			snprintf(inter, sizeof(inter), "%d months",
					(int) seconds / (SECS_PER_DAY * DAYS_PER_MONTH));
			break;
		case INTERVAL_MASK(DAY):	/* day */
			snprintf(inter, sizeof(inter), "%d days",
					(int) seconds / SECS_PER_DAY);
			break;
		case INTERVAL_MASK(HOUR):	/* hour */
			snprintf(inter, sizeof(inter), "%d days",
					(int) seconds / SECS_PER_HOUR);
			break;
		case INTERVAL_MASK(MINUTE):	/* minute */
			snprintf(inter, sizeof(inter), "%d days",
					(int) seconds / SECS_PER_MINUTE);
			break;
		case INTERVAL_MASK(SECOND):	/* second */
			snprintf(inter, sizeof(inter), "%d seconds",
					(int) seconds);
			break;
		case INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH):	/* year to month */
			snprintf(inter, sizeof(inter), "%d years %d months",
					(int) seconds / SECS_PER_YEAR,
					(int) (seconds / (SECS_PER_DAY * DAYS_PER_MONTH)) % MONTHS_PER_YEAR);
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR):		/* day to hour */
			snprintf(inter, sizeof(inter), "%d days %d hours",
					(int) seconds / SECS_PER_DAY,
					(int) (seconds / SECS_PER_HOUR) % HOURS_PER_DAY);
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):	/* day to minute */
			snprintf(inter, sizeof(inter), "%d days %02d:%02d",
					(int) seconds / SECS_PER_DAY,
					(int) (seconds / SECS_PER_HOUR) % HOURS_PER_DAY,
					(int) (seconds / SECS_PER_MINUTE) % MINS_PER_HOUR);
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):	/* day to second */
			snprintf(inter, sizeof(inter), "%d days %02d:%02d:%02d.%06d",
					(int) seconds / SECS_PER_DAY,
					(int) (seconds / SECS_PER_HOUR) % HOURS_PER_DAY,
					(int) (seconds / SECS_PER_MINUTE) % MINS_PER_HOUR,
					(int) (seconds % SECS_PER_MINUTE) % SECS_PER_MINUTE,
					(int) remains);
			break;
		case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):	/* hour to minute */
			snprintf(inter, sizeof(inter), "%02d:%02d",
					(int) seconds / SECS_PER_HOUR,
					(int) (seconds / SECS_PER_MINUTE) % MINS_PER_HOUR);
			break;
		case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):	/* hour to second */
			snprintf(inter, sizeof(inter), "%02d:%02d:%02d.%06d",
					(int) (seconds / SECS_PER_HOUR),
					(int) (seconds / SECS_PER_MINUTE) % MINS_PER_HOUR,
					(int) (seconds % SECS_PER_MINUTE) % SECS_PER_MINUTE,
					(int) remains);
			break;
		case INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):	/* minute to second */
			snprintf(inter, sizeof(inter), "%02d:%02d.%06d",
					(int) seconds / SECS_PER_MINUTE,
					(int) (seconds % SECS_PER_MINUTE) % SECS_PER_MINUTE,
					(int) remains);
			break;
		case INTERVAL_FULL_RANGE:	/* full range */
			snprintf(inter, sizeof(inter), "%d years %d months % days %02d:%02d:%02d.%06d",
					(int) seconds / SECS_PER_YEAR,
					(int) (seconds / (SECS_PER_DAY * DAYS_PER_MONTH)) % MONTHS_PER_YEAR,
					(int) (seconds / SECS_PER_DAY) % (SECS_PER_DAY * DAYS_PER_MONTH),
					(int) (seconds / SECS_PER_HOUR) % HOURS_PER_DAY,
					(int) (seconds / SECS_PER_MINUTE) % MINS_PER_HOUR,
					(int) (seconds % SECS_PER_MINUTE) % SECS_PER_MINUTE,
					(int) remains);
			break;
		default:
		{
			set_shm_connector_errmsg(myConnectorId, "invalid INTERVAL typmod");
			elog(ERROR, "invalid INTERVAL typmod: 0x%x", typemod);
			break;
		}
	}
	if (addquote)
	{
		out = escapeSingleQuote(inter, addquote);
	}
	else
	{
		out = (char *) palloc0(strlen(inter) + 1);
		memcpy(out, inter, strlen(inter));
	}
	return out;
}

static char *
handle_base64_to_interval(const char * in, bool addquote, int timerep, int typemod)
{
	int tmpoutlen = pg_b64_dec_len(strlen(in));
	unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen);
	long long input = 0;

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
	tmpoutlen = pg_b64_decode(in, strlen(in), tmpout, tmpoutlen);
#else
	tmpoutlen = pg_b64_decode(in, strlen(in), (char *)tmpout, tmpoutlen);
#endif
	input = derive_value_from_byte(tmpout, tmpoutlen);
	return construct_intervalstr(input, addquote, timerep, typemod);
}

static char *
handle_numeric_to_interval(const char * in, bool addquote, int timerep, int typemod)
{
	long long input = atoll(in);

	return construct_intervalstr(input, addquote, timerep, typemod);
}

static char *
handle_string_to_interval(const char * in, bool addquote)
{
	/*
	 * no special handling to convert string to interval. May fail to apply
	 * if it contains incorrect interval format
	 */
	return (addquote ? escapeSingleQuote(in, addquote) : pstrdup(in));
}

static char *
handle_data_by_type_category(char * in, DBZ_DML_COLUMN_VALUE * colval, ConnectorType conntype, bool addquote)
{
	char * out = NULL;
	elog(DEBUG1, "%s: col %s timerep %d dbztype %d category %c typname %s",__FUNCTION__,
			colval->name, colval->timerep, colval->dbztype, colval->typcategory, colval->typname);
	switch (colval->typcategory)
	{
		case TYPCATEGORY_BOOLEAN:
		case TYPCATEGORY_NUMERIC:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval, conntype);
					out = handle_base64_to_numeric_with_scale(colval->value, colval->scale);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_numeric_with_scale(in, colval->scale);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				case DBZTYPE_STRING:
				{
					/* no special handling for now. todo */
					out = pstrdup(in);
					break;
				}
				default:
				{
					out = pstrdup(in);
					break;
				}
			}
			break;
		}
		case TYPCATEGORY_DATETIME:
		{
			/*
			 * DATE, TIME or IMESTAMP, we cannot possibly know at this point, so we assume the
			 * type name would contain descriptive words to help us identify. It may not be 100%
			 * accurate, so it is best to define a transform function after this stage to
			 * correctly process it. todo: we can expose a new GUC to allow user to configure
			 * non-native data type and tell synchdb how to process them
			 */
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval, conntype);
					if (strstr(colval->typname, "date"))
						out = handle_base64_to_date(colval->value, addquote, colval->timerep);
					else if (strstr(colval->typname, "timestamp"))
						out = handle_base64_to_timestamp(colval->value, addquote, colval->timerep, colval->typemod);
					else
						out = handle_base64_to_time(colval->value, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					if (strstr(colval->typname, "date"))
						out = handle_base64_to_date(in, addquote, colval->timerep);
					else if (strstr(colval->typname, "timestamp"))
						out = handle_base64_to_timestamp(in, addquote, colval->timerep, colval->typemod);
					else
						out = handle_base64_to_time(in, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					if (strstr(colval->typname, "timestamp") || strstr(colval->typname, "timetz"))
					{
						out = handle_string_to_timestamp(in, addquote, conntype);
					}
					else
					{
						if (addquote)
							out = escapeSingleQuote(in, addquote);
						else
							out = pstrdup(in);
					}
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:
				{
					/* OLR represents date as nanoseconds since epoch*/
					if (strstr(colval->typname, "date"))
						out = handle_numeric_to_date(in, addquote, TIME_NANOTIMESTAMP);
					else if (strstr(colval->typname, "timestamp"))
						out = handle_numeric_to_timestamp(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					else
						out = handle_numeric_to_time(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					break;
				}
#endif
				default:
				{
					if (strstr(colval->typname, "date"))
						out = handle_numeric_to_date(in, addquote, colval->timerep);
					else if (strstr(colval->typname, "timestamp"))
						out = handle_numeric_to_timestamp(in, addquote, colval->timerep, colval->typemod);
					else
						out = handle_numeric_to_time(in, addquote, colval->timerep, colval->typemod);
					break;
				}
			}
			break;
		}
		case TYPCATEGORY_BITSTRING:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval, conntype);
					out = handle_base64_to_bit(colval->value, addquote, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_bit(in, addquote, colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				default:
				{
					if (addquote)
						out = escapeSingleQuote(in, addquote);
					else
						out = pstrdup(in);
					break;
				}
			}
			break;
		}
		case TYPCATEGORY_TIMESPAN:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval, conntype);
					out = handle_base64_to_interval(colval->value, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_interval(in, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					if (addquote)
						out = escapeSingleQuote(in, addquote);
					else
						out = pstrdup(in);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:
				{
					/* oracle number represented as interval - usd nanoseconds */
					out = handle_numeric_to_interval(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					break;
				}
#endif
				default:
				{
					out = handle_numeric_to_interval(in, addquote, colval->timerep, colval->typemod);
					break;
				}
			}
			break;
		}
		case TYPCATEGORY_USER:
		case TYPCATEGORY_ENUM:
		case TYPCATEGORY_GEOMETRIC:
		case TYPCATEGORY_STRING:
		default:
		{
			/* todo */
			elog(DEBUG1, "no special handling for category %c", colval->typcategory);
			if (addquote)
			{
				out = escapeSingleQuote(in, addquote);
			}
			else
			{
				out = pstrdup(in);
			}
			break;
		}
	}
	return out;
}

/*
 * processDataByType
 *
 * this function performs necessary data conversions to convert input data
 * as string and output a processed string based on type
 */
static char *
processDataByType(DBZ_DML_COLUMN_VALUE * colval, bool addquote, char * remoteObjectId, ConnectorType type)
{
	char * out = NULL;
	char * in = colval->value;
	char * transformExpression = NULL;

	if (!in || strlen(in) == 0)
		return NULL;

	if (!strcasecmp(in, "NULL"))
		return NULL;

	elog(DEBUG1, "%s: col %s typoid %d timerep %d dbztype %d category %c",__FUNCTION__,
			colval->name, colval->datatype, colval->timerep, colval->dbztype, colval->typcategory);
	switch(colval->datatype)
	{
		case BOOLOID:
		case INT8OID:
		case INT2OID:
		case INT4OID:
		case FLOAT8OID:
		case FLOAT4OID:
		case NUMERICOID:
		case MONEYOID:
		{
			/* special handling for MONEYOID to use scale 4 to account for cents */
			if (colval->datatype == MONEYOID)
				colval->scale = 4;

			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval, type);
					out = handle_base64_to_numeric_with_scale(colval->value, colval->scale);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_numeric_with_scale(in, colval->scale);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_numeric(in, addquote);
					break;
				}
				default:
				{
					/* numeric to numeric, just use as is */
					out = pstrdup(in);
					break;
				}
			}
			break;
		}
		case BPCHAROID:
		case TEXTOID:
		case VARCHAROID:
		case CSTRINGOID:
		case JSONBOID:
		case UUIDOID:
		{
			if (addquote)
				out = escapeSingleQuote(in, addquote);
			else
				out = pstrdup(in);
			break;
		}
		case VARBITOID:
		case BITOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval, type);
					out = handle_base64_to_bit(colval->value, addquote, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_bit(in, addquote, colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_bit(in, addquote);
					break;
				}
				default:
				{
					out = handle_numeric_to_bit(in, addquote);
					break;
				}
			}
			break;
		}
		case DATEOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval, type);
					out = handle_base64_to_date(colval->value, addquote, colval->timerep);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_date(in, addquote, colval->timerep);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_date(in, addquote);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:
				{
					out = handle_numeric_to_date(in, addquote, TIME_NANOTIMESTAMP);
					break;
				}
#endif
				default:
				{
					out = handle_numeric_to_date(in, addquote, colval->timerep);
					break;
				}
			}
			break;
		}
		case TIMESTAMPTZOID:
		case TIMESTAMPOID:
		case TIMETZOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval, type);
					out = handle_base64_to_timestamp(colval->value, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_timestamp(in, addquote, colval->timerep,  colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_timestamp(in, addquote, type);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:

				{
					/* OLR represents date as nanoseconds since epoch*/
					out = handle_numeric_to_timestamp(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					break;
				}
#endif
				default:
				{
					out = handle_numeric_to_timestamp(in, addquote, colval->timerep, colval->typemod);
					break;
				}
			}
			break;
		}
		case TIMEOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval, type);
					out = handle_base64_to_time(colval->value, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_time(in, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_time(in, addquote);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:
				{
					out = handle_numeric_to_time(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					break;
				}
#endif
				default:
				{
					out = handle_numeric_to_time(in, addquote, colval->timerep, colval->typemod);
					break;
				}
			}
			break;
		}
		case BYTEAOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval, type);
					out = handle_base64_to_byte(colval->value, addquote);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_byte(in, addquote);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_byte(in, addquote);
					break;
				}
				default:
				{
					out = handle_numeric_to_byte(in, addquote);
					break;
				}
			}
			break;
		}
		case INTERVALOID:
		{
			switch (colval->dbztype)
			{
				case DBZTYPE_STRUCT:
				{
					expand_struct_value(in, colval, type);
					out = handle_base64_to_interval(colval->value, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_BYTES:
				{
					out = handle_base64_to_interval(in, addquote, colval->timerep, colval->typemod);
					break;
				}
				case DBZTYPE_STRING:
#ifdef WITH_OLR
				case OLRTYPE_STRING:
#endif
				{
					out = handle_string_to_interval(in, addquote);
					break;
				}
#ifdef WITH_OLR
				case OLRTYPE_NUMBER:
				{
					/* oracle number represented as interval - usd nanoseconds */
					out = handle_numeric_to_interval(in, addquote, TIME_NANOTIMESTAMP, colval->typemod);
					break;
				}
#endif
				default:
				{
					out = handle_numeric_to_interval(in, addquote, colval->timerep, colval->typemod);
					break;
				}
			}
			break;
		}
		default:
		{
			/*
			 * if this column data type does not fall in the cases above, then we will try
			 * to process it based on its type category.
			 */
			out = handle_data_by_type_category(in, colval, type, addquote);
			break;
		}
	}

	/*
	 * after the data is prepared, we need to check if we need to transform the data
	 * with a user-defined expression by looking up against transformExpressionHash.
	 * Note, we have to use colval->remoteColumnName to look up because colval->name
	 * may have been transformed to something else.
	 */
	transformExpression = transform_data_expression(remoteObjectId, colval->remoteColumnName);
	if (transformExpression)
	{
		StringInfoData strinfo;
		Datum jsonb_datum;
		Jsonb *jb;
		char * wkb = NULL, * srid = NULL;
		char * transData = NULL;
		char * escapedData = NULL;

		elog(DEBUG1, "transforming remote column %s.%s's data '%s' with expression '%s'",
				remoteObjectId, colval->remoteColumnName, out, transformExpression);

		/*
		 * data could be expressed in JSON to represent a geometry with
		 * wkb and srid fields, so let's check if this is the case
		 */
		initStringInfo(&strinfo);

		/*
		 * special case for MySQL GEOMETRY type. It is expressed as JSON with "wkb" and
		 * "srid" inside. Need to process these accordingly. There may be more special
		 * cases expressed as JSON, we will add more here as they are discovered. TODO
		 */
		if (out[0] == '{' && out[strlen(out) - 1] == '}' && strstr(out, "\"wkb\""))
		{

			PG_TRY();
			{
				jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(out));
				jb = DatumGetJsonbP(jsonb_datum);
			}
			PG_CATCH();
			{
				FlushErrorState();
				elog(WARNING, "bad geometric expression out: %s", out);
				/* skip transform on out */
				return out;
			}
			PG_END_TRY();

			getPathElementString(jb, "wkb", &strinfo, true);
			if (!strcasecmp(strinfo.data, "null"))
				wkb = pstrdup("0");
			else
				wkb = pstrdup(strinfo.data);

			getPathElementString(jb, "srid", &strinfo, true);
			if (!strcasecmp(strinfo.data, "null"))
				srid = pstrdup("0");
			else
				srid = pstrdup(strinfo.data);

			elog(DEBUG1,"wkb = %s, srid = %s", wkb, srid);

			escapedData = escapeSingleQuote(out, false);
			transData = ra_transformDataExpression(escapedData, wkb, srid, transformExpression);
			if (transData)
			{
				elog(DEBUG1, "transformed remote column %s.%s's data '%s' to '%s' with expression '%s'",
						remoteObjectId, colval->remoteColumnName, out, transData, transformExpression);

				/* replace return value with transData */
				pfree(out);
				out = pstrdup(transData);
				pfree(transData);
				pfree(escapedData);
			}
			pfree(wkb);
			pfree(srid);
		}
		else
		{
			/* regular data - proceed with regular transform */
			escapedData = escapeSingleQuote(out, false);
			transData = ra_transformDataExpression(escapedData, NULL, NULL, transformExpression);
			if (transData)
			{
				elog(DEBUG1, "transformed remote column %s.%s's data '%s' to '%s' with expression '%s'",
						remoteObjectId, colval->remoteColumnName, out, transData, transformExpression);

				/* replace return value with transData */
				pfree(out);
				out = pstrdup(transData);
				pfree(transData);
				pfree(escapedData);
			}
		}
	}

	elog(DEBUG1, "data processing completed. Returning output: ");
	elog(DEBUG1, "%s", out);
	return out;
}

/*
 * list_sort_cmp
 *
 * helper function to compare 2 ListCells for sorting
 */
int
list_sort_cmp(const ListCell *a, const ListCell *b)
{
	DBZ_DML_COLUMN_VALUE * colvala = (DBZ_DML_COLUMN_VALUE *) lfirst(a);
	DBZ_DML_COLUMN_VALUE * colvalb = (DBZ_DML_COLUMN_VALUE *) lfirst(b);

	if (colvala->position < colvalb->position)
		return -1;
	if (colvala->position > colvalb->position)
		return 1;
	return 0;
}

/*
 * convert2PGDML
 *
 * this function converts  DBZ_DML to PG_DML strucutre
 */
PG_DML *
convert2PGDML(DBZ_DML * dbzdml, ConnectorType type)
{
	PG_DML * pgdml = (PG_DML*) palloc0(sizeof(PG_DML));
	ListCell * cell, * cell2;

	StringInfoData strinfo;

	initStringInfo(&strinfo);

	/* copy identification data to PG_DML */
	pgdml->op = dbzdml->op;
	pgdml->tableoid = dbzdml->tableoid;
	pgdml->natts = dbzdml->natts;

	switch(dbzdml->op)
	{
		case 'r':
		case 'c':
		{
			if (synchdb_dml_use_spi)
			{
				/* --- Convert to use SPI to handler DML --- */
				appendStringInfo(&strinfo, "INSERT INTO %s(", dbzdml->mappedObjectId);
				foreach(cell, dbzdml->columnValuesAfter)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					appendStringInfo(&strinfo, "%s,", colval->name);
				}
				strinfo.data[strinfo.len - 1] = '\0';
				strinfo.len = strinfo.len - 1;
				appendStringInfo(&strinfo, ") VALUES (");

				foreach(cell, dbzdml->columnValuesAfter)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					char * data = processDataByType(colval, true, dbzdml->remoteObjectId, type);

					if (data != NULL)
					{
						appendStringInfo(&strinfo, "%s,", data);
						pfree(data);
					}
					else
					{
						appendStringInfo(&strinfo, "%s,", "null");
					}
				}
				/* remove extra "," */
				strinfo.data[strinfo.len - 1] = '\0';
				strinfo.len = strinfo.len - 1;

				appendStringInfo(&strinfo, ");");
			}
			else
			{
				/* --- Convert to use Heap AM to handler DML --- */
				foreach(cell, dbzdml->columnValuesAfter)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					PG_DML_COLUMN_VALUE * pgcolval = palloc0(sizeof(PG_DML_COLUMN_VALUE));

					char * data = processDataByType(colval, false, dbzdml->remoteObjectId, type);

					if (data != NULL)
					{
						pgcolval->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval->value = pstrdup("NULL");

					pgcolval->datatype = colval->datatype;
					pgcolval->position = colval->position;

					pgdml->columnValuesAfter = lappend(pgdml->columnValuesAfter, pgcolval);
				}
				pgdml->columnValuesBefore = NULL;
			}
			break;
		}
		case 'd':
		{
			if (synchdb_dml_use_spi)
			{
				bool atleastone = false;

				/* --- Convert to use SPI to handler DML --- */
				appendStringInfo(&strinfo, "DELETE FROM %s WHERE ", dbzdml->mappedObjectId);
				foreach(cell, dbzdml->columnValuesBefore)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					char * data;

					if (!colval->ispk)
						continue;

					appendStringInfo(&strinfo, "%s = ", colval->name);
					data = processDataByType(colval, true, dbzdml->remoteObjectId, type);
					if (data != NULL)
					{
						appendStringInfo(&strinfo, "%s", data);
						pfree(data);
					}
					else
					{
						appendStringInfo(&strinfo, "%s", "null");
					}
					appendStringInfo(&strinfo, " AND ");
					atleastone = true;
				}

				if (atleastone)
				{
					/* remove extra " AND " */
					strinfo.data[strinfo.len - 5] = '\0';
					strinfo.len = strinfo.len - 5;
				}
				else
				{
					/*
					 * no primary key to use as WHERE clause, logs a warning and skip this operation
					 * for now
					 */
					elog(WARNING, "no primary key available to build DELETE query for table %s. Operation"
							" skipped. Set synchdb.dml_use_spi = false to support DELETE without primary key",
							dbzdml->mappedObjectId);

					pfree(strinfo.data);
					destroyPGDML(pgdml);
					return NULL;
				}
				appendStringInfo(&strinfo, ";");
			}
			else
			{
				/* --- Convert to use Heap AM to handler DML --- */
				foreach(cell, dbzdml->columnValuesBefore)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					PG_DML_COLUMN_VALUE * pgcolval = palloc0(sizeof(PG_DML_COLUMN_VALUE));

					char * data = processDataByType(colval, false, dbzdml->remoteObjectId, type);

					if (data != NULL)
					{
						pgcolval->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval->value = pstrdup("NULL");

					pgcolval->datatype = colval->datatype;
					pgcolval->position = colval->position;

					pgdml->columnValuesBefore = lappend(pgdml->columnValuesBefore, pgcolval);
				}
				pgdml->columnValuesAfter = NULL;
			}
			break;
		}
		case 'u':
		{
			if (synchdb_dml_use_spi)
			{
				bool atleastone = false;

				/* --- Convert to use SPI to handler DML --- */
				appendStringInfo(&strinfo, "UPDATE %s SET ", dbzdml->mappedObjectId);
				foreach(cell, dbzdml->columnValuesAfter)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					char * data;

					appendStringInfo(&strinfo, "%s = ", colval->name);
					data = processDataByType(colval, true, dbzdml->remoteObjectId, type);
					if (data != NULL)
					{
						appendStringInfo(&strinfo, "%s,", data);
						pfree(data);
					}
					else
					{
						appendStringInfo(&strinfo, "%s,", "null");
					}
				}
				/* remove extra "," */
				strinfo.data[strinfo.len - 1] = '\0';
				strinfo.len = strinfo.len - 1;

				appendStringInfo(&strinfo,  " WHERE ");
				foreach(cell, dbzdml->columnValuesBefore)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					char * data;

					if (!colval->ispk)
						continue;

					appendStringInfo(&strinfo, "%s = ", colval->name);
					data = processDataByType(colval, true, dbzdml->remoteObjectId, type);
					if (data != NULL)
					{
						appendStringInfo(&strinfo, "%s", data);
						pfree(data);
					}
					else
					{
						appendStringInfo(&strinfo, "%s", "null");
					}
					appendStringInfo(&strinfo, " AND ");
					atleastone = true;
				}

				if (atleastone)
				{
					/* remove extra " AND " */
					strinfo.data[strinfo.len - 5] = '\0';
					strinfo.len = strinfo.len - 5;
				}
				else
				{
					/*
					 * no primary key to use as WHERE clause, logs a warning and skip this operation
					 * for now
					 */
					elog(WARNING, "no primary key available to build UPDATE query for table %s. Operation"
							" skipped. Set synchdb.dml_use_spi = false to support UPDATE without primary key",
							dbzdml->mappedObjectId);

					pfree(strinfo.data);
					destroyPGDML(pgdml);
					return NULL;
				}
				appendStringInfo(&strinfo, ";");
			}
			else
			{
				/* --- Convert to use Heap AM to handler DML --- */
				forboth(cell, dbzdml->columnValuesAfter, cell2, dbzdml->columnValuesBefore)
				{
					DBZ_DML_COLUMN_VALUE * colval_after = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					DBZ_DML_COLUMN_VALUE * colval_before = (DBZ_DML_COLUMN_VALUE *) lfirst(cell2);
					PG_DML_COLUMN_VALUE * pgcolval_after = palloc0(sizeof(PG_DML_COLUMN_VALUE));
					PG_DML_COLUMN_VALUE * pgcolval_before = palloc0(sizeof(PG_DML_COLUMN_VALUE));

					char * data = processDataByType(colval_after, false, dbzdml->remoteObjectId, type);

					if (data != NULL)
					{
						pgcolval_after->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval_after->value = pstrdup("NULL");

					pgcolval_after->datatype = colval_after->datatype;
					pgcolval_after->position = colval_after->position;
					pgdml->columnValuesAfter = lappend(pgdml->columnValuesAfter, pgcolval_after);

					data = processDataByType(colval_before, false, dbzdml->remoteObjectId, type);
					if (data != NULL)
					{
						pgcolval_before->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval_before->value = pstrdup("NULL");

					pgcolval_before->datatype = colval_before->datatype;
					pgcolval_before->position = colval_before->position;
					pgdml->columnValuesBefore = lappend(pgdml->columnValuesBefore, pgcolval_before);
				}
			}
			break;
		}
		default:
		{
			elog(ERROR, "op %c not supported", dbzdml->op);
			destroyPGDML(pgdml);
			return NULL;
		}
	}

	pgdml->dmlquery = pstrdup(strinfo.data);

	/* free the data inside strinfo as we no longer needs it */
	pfree(strinfo.data);

	if (synchdb_dml_use_spi)
		elog(DEBUG1, "pgdml->dmlquery %s", pgdml->dmlquery);

	return pgdml;
}

/*
 * fc_get_connector_type
 *
 * this function takes connector type in string and returns a corresponding enum
 */
ConnectorType
fc_get_connector_type(const char * connector)
{
	if (!strcasecmp(connector, "mysql"))
		return TYPE_MYSQL;
	else if (!strcasecmp(connector, "oracle"))
		return TYPE_ORACLE;
	else if (!strcasecmp(connector, "sqlserver"))
		return TYPE_SQLSERVER;
	else if (!strcasecmp(connector, "olr"))
		return TYPE_OLR;
	else
		return TYPE_UNDEF;
}

/*
 * updateSynchdbAttribute
 *
 * update synchdb_attribute table based on debezium's DDL info (dbzddl) and the transformed pg
 * equivalent (pgddl).
 */
void
updateSynchdbAttribute(DBZ_DDL * dbzddl, PG_DDL * pgddl, ConnectorType conntype, const char * name)
{
	ListCell * cell, *cell2;
	StringInfoData strinfo;

	if (!pgddl || !dbzddl)
		return;

	initStringInfo(&strinfo);

	if (pgddl->type == DDL_CREATE_TABLE ||
		(pgddl->type == DDL_ALTER_TABLE && pgddl->subtype == SUBTYPE_ADD_COLUMN) ||
		(pgddl->type == DDL_ALTER_TABLE && pgddl->subtype == SUBTYPE_ALTER_COLUMN))
	{
		int j = 0;
		Oid schemaoid, tableoid;

		if (list_length(dbzddl->columns) <= 0 || list_length(pgddl->columns) <= 0)
		{
			elog(WARNING, "Invalid input column lists. Skipping attribute update");
			return;
		}

		/* convert schema and table name to lowercase letters before lookup */
		for (j = 0; j < strlen(pgddl->schema); j++)
			pgddl->schema[j] = (char) pg_tolower((unsigned char) pgddl->schema[j]);

		for (j = 0; j < strlen(pgddl->tbname); j++)
			pgddl->tbname[j] = (char) pg_tolower((unsigned char) pgddl->tbname[j]);

		schemaoid = get_namespace_oid(pgddl->schema, false);
		if (!OidIsValid(schemaoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for schema '%s'", pgddl->schema);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		tableoid = get_relname_relid(pgddl->tbname, schemaoid);
		if (!OidIsValid(tableoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for table '%s'", pgddl->tbname);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		appendStringInfo(&strinfo, "INSERT INTO %s (name, type, attrelid, attnum, "
				"ext_tbname, ext_attname, ext_atttypename) VALUES ",
				SYNCHDB_ATTRIBUTE_TABLE);

		forboth(cell, dbzddl->columns, cell2, pgddl->columns)
		{
			DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);
			PG_DDL_COLUMN * pgcol = (PG_DDL_COLUMN *) lfirst(cell2);

			if (pgcol->attname == NULL || pgcol->atttype == NULL)
				continue;

			appendStringInfo(&strinfo, "(lower('%s'),lower('%s'),%d,%d,'%s','%s','%s'),",
					name,
					connectorTypeToString(conntype),
					tableoid,
					pgcol->position,
					dbzddl->id,
					col->name,
					col->typeName);
		}
		/* remove extra "," */
		strinfo.data[strinfo.len - 1] = '\0';
		strinfo.len = strinfo.len - 1;

		appendStringInfo(&strinfo, " ON CONFLICT(name, type, attrelid, attnum) "
				"DO UPDATE SET "
				"ext_tbname = EXCLUDED.ext_tbname,"
				"ext_attname = EXCLUDED.ext_attname,"
				"ext_atttypename = EXCLUDED.ext_atttypename;");
	}
	else if (pgddl->type == DDL_DROP_TABLE)
	{
		appendStringInfo(&strinfo, "DELETE FROM %s "
				"WHERE lower(ext_tbname) = lower('%s') AND "
				"lower(name) = lower('%s') AND "
				"lower(type) = lower('%s');",
				SYNCHDB_ATTRIBUTE_TABLE,
				dbzddl->id,
				name,
				connectorTypeToString(conntype));
	}
	else if (pgddl->type == DDL_ALTER_TABLE && pgddl->subtype == SUBTYPE_DROP_COLUMN)
	{
		if (list_length(pgddl->columns) <= 0)
		{
			elog(WARNING, "cannot update attribute table. no column dropped by ALTER");
			return;
		}
		foreach(cell, pgddl->columns)
		{
			PG_DDL_COLUMN * pgcol = (PG_DDL_COLUMN *) lfirst(cell);
			appendStringInfo(&strinfo, "UPDATE %s SET "
					"ext_attname = '........synchdb.dropped.%d........',"
					"ext_atttypename = null WHERE "
					"lower(ext_attname) = lower('%s') AND "
					"lower(name) = lower('%s') AND "
					"lower(type) = lower('%s') AND "
					"lower(ext_tbname) = lower('%s');",
					SYNCHDB_ATTRIBUTE_TABLE,
					pgcol->position,
					pgcol->attname,
					name,
					connectorTypeToString(conntype),
					dbzddl->id);
		}
	}
	else
	{
		elog(DEBUG1, "unknown ddl type %d. Skipping attribute update", pgddl->type);
		return;
	}

	/*
	 * regardless of the operation, always insert extra query to clean up
	 * synchdb_attribute table in case some relations have been removed by user
	 * manually.
	 */
	appendStringInfo(&strinfo, "DELETE FROM %s WHERE attrelid NOT IN "
			"(SELECT oid FROM pg_class);", SYNCHDB_ATTRIBUTE_TABLE);

	/* execute the query using SPI */
	ra_executeCommand(strinfo.data);
}

/*
 * fc_initFormatConverter
 *
 * main entry to initialize all hash tables for all supported database types
 */
void
fc_initFormatConverter(ConnectorType connectorType)
{
	switch (connectorType)
	{
		case TYPE_MYSQL:
		{
			init_mysql();
			break;
		}
		case TYPE_ORACLE:
		case TYPE_OLR:
		{
			init_oracle();
			break;
		}
		case TYPE_SQLSERVER:
		{
			init_sqlserver();
			break;
		}
		default:
		{
			set_shm_connector_errmsg(myConnectorId, "unsupported connector type");
			elog(ERROR, "unsupported connector type");
		}
	}
}

/*
 * fc_deinitFormatConverter
 *
 * main entry to de-initialize all hash tables for all supported database types
 */
void
fc_deinitFormatConverter(ConnectorType connectorType)
{
	switch (connectorType)
	{
		case TYPE_MYSQL:
		{
			hash_destroy(mysqlDatatypeHash);
			break;
		}
		case TYPE_ORACLE:
		case TYPE_OLR:
		{
			hash_destroy(oracleDatatypeHash);
#ifdef WITH_OLR
			/* unload oracle parser for OLR is needed */
			if (connectorType == TYPE_OLR)
				unload_oracle_parser();
#endif
			break;
		}
		case TYPE_SQLSERVER:
		{
			hash_destroy(sqlserverDatatypeHash);
			break;
		}
		default:
		{
			set_shm_connector_errmsg(myConnectorId, "unsupported connector type");
			elog(ERROR, "unsupported connector type");
		}
	}
}

void
fc_initDataCache(void)
{
	/* init data cache hash */
	HASHCTL	info;

	info.keysize = sizeof(DataCacheKey);
	info.entrysize = sizeof(DataCacheEntry);
	info.hcxt = CurrentMemoryContext;

	dataCacheHash = hash_create("data cache hash",
							 256,
							 &info,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

void
fc_deinitDataCache(void)
{
	/* destroy data cache hash */
	if (dataCacheHash)
	{
		hash_destroy(dataCacheHash);
		dataCacheHash = NULL;
	}
}

void
fc_resetDataCache(void)
{
	/* deinit followed by another init */
	fc_deinitDataCache();
	fc_initDataCache();
}

bool
fc_load_objmap(const char * name, ConnectorType connectorType)
{
	ObjectMap * objs = NULL;
	int numobjs = 0;
	int ret = -1;
	int i = 0;
	bool found = false;
	ObjMapHashEntry objmapentry = {0};
	ObjMapHashEntry * objmaplookup;
	HASHCTL	info;
	HTAB * rulehash = NULL;
	DatatypeHashEntry hashentry;
	DatatypeHashEntry * entrylookup;

	TransformExpressionHashEntry expressentry;
	TransformExpressionHashEntry * expressentrylookup;

	switch (connectorType)
	{
		case TYPE_MYSQL:
			rulehash = mysqlDatatypeHash;
			break;
		case TYPE_ORACLE:
		case TYPE_OLR:
			rulehash = oracleDatatypeHash;
			break;
		case TYPE_SQLSERVER:
			rulehash = sqlserverDatatypeHash;
			break;
		default:
		{
			set_shm_connector_errmsg(myConnectorId, "unsupported connector type");
			elog(ERROR, "unsupported connector type");
		}
	}

	if (!rulehash)
	{
		set_shm_connector_errmsg(myConnectorId, "data type hash not initialized");
		elog(ERROR, "data type hash not initialized");
	}

	ret = ra_listObjmaps(name, &objs, &numobjs);
	if (ret)
	{
		elog(WARNING, "no object mapping rules found for connector '%s'", name);
		return true;
	}

	for (i = 0; i < numobjs; i++)
	{
		elog(DEBUG1, "type %s, src %s dst %s: (%s) (%s) enabled %d", objs[i].objtype, objs[i].srcobj, objs[i].dstobj,
				objs[i].curr_pg_tbname, objs[i].curr_pg_attname, objs[i].enabled);
		/* initialized objectMappingHash if not done yet */

		if (!strcasecmp(objs[i].objtype, "table") || !strcasecmp(objs[i].objtype, "column") )
		{
			if (!objectMappingHash)
			{
				info.keysize = sizeof(ObjMapHashKey);
				info.entrysize = sizeof(ObjMapHashEntry);
				info.hcxt = CurrentMemoryContext;
				objectMappingHash = hash_create("object mapping hash",
												 256,
												 &info,
												 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
			}

			memset(&objmapentry, 0, sizeof(ObjMapHashEntry));

			strlcpy(objmapentry.key.extObjType, objs[i].objtype, SYNCHDB_OBJ_TYPE_SIZE);
			strlcpy(objmapentry.key.extObjName, objs[i].srcobj, SYNCHDB_OBJ_NAME_SIZE);
			strlcpy(objmapentry.pgsqlObjName, objs[i].dstobj, SYNCHDB_OBJ_NAME_SIZE);

			if (!objs[i].enabled)
			{
				/* disable this table or column name mapping rule by removing it from the hash table */
				objmaplookup = hash_search(objectMappingHash, &(objmapentry.key),
						HASH_REMOVE, NULL);

				if (objmaplookup)
					elog(WARNING, "deleted object mapping '%s(%s)' <-> '%s'", objmaplookup->key.extObjName,
							objmapentry.key.extObjType, objmaplookup->pgsqlObjName);
			}
			else
			{
				objmaplookup = (ObjMapHashEntry *) hash_search(objectMappingHash,
						&(objmapentry.key), HASH_ENTER, &found);

				/* found or not, just update or insert it */
				memset(objmaplookup->key.extObjName, 0, SYNCHDB_OBJ_NAME_SIZE);
				strlcpy(objmaplookup->key.extObjName,
						objmapentry.key.extObjName,
						SYNCHDB_OBJ_NAME_SIZE);

				memset(objmaplookup->pgsqlObjName, 0, SYNCHDB_OBJ_NAME_SIZE);
				strlcpy(objmaplookup->pgsqlObjName,
						objmapentry.pgsqlObjName,
						SYNCHDB_OBJ_NAME_SIZE);

				elog(WARNING, "Inserted / updated object mapping '%s(%s)' <-> '%s'", objmaplookup->key.extObjName,
						objmapentry.key.extObjType, objmaplookup->pgsqlObjName);

				/*
				 * if this is a table object mapping and that the connector has already created a matching table
				 * in PostgreSQL, we need to check if the mapped table is the same as the one created. If not
				 * then the user is requesting to rename the table.
				 */
				if (!strcasecmp(objs[i].objtype, "table") && objs[i].curr_pg_tbname[0] != '\0')
				{
					/* if dstobj contains no dot(no schema info), we will default to public schema */
					if (strchr(objs[i].dstobj, '.') == NULL)
					{
						char temp[SYNCHDB_TRANSFORM_EXPRESSION_SIZE];
						strlcpy(temp, objs[i].dstobj, SYNCHDB_TRANSFORM_EXPRESSION_SIZE);

						snprintf(&objs[i].dstobj[0], SYNCHDB_TRANSFORM_EXPRESSION_SIZE, "public.%s", temp);
					}

					if (strcasecmp(objs[i].dstobj, objs[i].curr_pg_tbname))
					{
						alter_tbname(objs[i].curr_pg_tbname, objs[i].dstobj);
					}
				}

				/*
				 * if this is a column object mapping and that the connector has already created a matching table
				 * in PostgreSQL, we need to check if the mapped column is the same as the one created. If not
				 * then the user is requesting to rename the column.
				 */
				if (!strcasecmp(objs[i].objtype, "column") && objs[i].curr_pg_attname[0] != '\0' && objs[i].curr_pg_tbname[0] != '\0')
				{
					if (strcasecmp(objs[i].dstobj, objs[i].curr_pg_attname))
					{
						alter_attname(objs[i].curr_pg_tbname, objs[i].curr_pg_attname, objs[i].dstobj);
					}
				}
			}
		}
		else if (!strcasecmp(objs[i].objtype, "transform"))
		{
			if (!transformExpressionHash)
			{
				info.keysize = sizeof(TransformExpressionHashKey);
				info.entrysize = sizeof(TransformExpressionHashEntry);
				info.hcxt = CurrentMemoryContext;

				/* initialize transform expression hash common to all connector types */
				transformExpressionHash = hash_create("transform expression hash",
												 256,
												 &info,
												 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
			}
			memset(&expressentry, 0, sizeof(TransformExpressionHashEntry));

			/*
			 * srcobj should be expressed in fully qualified column name: db.schema.table.col or
			 * db.table.col. dstobj is the expression to execute on the column data before applying
			 */
			strlcpy(expressentry.key.extObjName, objs[i].srcobj, SYNCHDB_OBJ_NAME_SIZE);
			strlcpy(expressentry.pgsqlTransExpress, objs[i].dstobj, SYNCHDB_TRANSFORM_EXPRESSION_SIZE);

			if (!objs[i].enabled)
			{
				/* disable this transform rule by removing it from the hash table */
				expressentrylookup = hash_search(transformExpressionHash, &(expressentry.key),
						HASH_REMOVE, NULL);

				if (expressentrylookup)
					elog(WARNING, "deleted transform expression mapping '%s' <-> '%s'",
							expressentrylookup->key.extObjName,
							expressentrylookup->pgsqlTransExpress);
			}
			else
			{
				/* add this new transform rule */
				expressentrylookup = (TransformExpressionHashEntry *) hash_search(transformExpressionHash,
						&(expressentry.key), HASH_ENTER, &found);

				/* found or not, just update or insert it */
				memset(expressentrylookup->key.extObjName, 0, SYNCHDB_OBJ_NAME_SIZE);
				strlcpy(expressentrylookup->key.extObjName,
						expressentry.key.extObjName,
						SYNCHDB_OBJ_NAME_SIZE);

				memset(expressentrylookup->pgsqlTransExpress, 0, SYNCHDB_TRANSFORM_EXPRESSION_SIZE);
				strlcpy(expressentrylookup->pgsqlTransExpress,
						expressentry.pgsqlTransExpress,
						SYNCHDB_TRANSFORM_EXPRESSION_SIZE);

				elog(WARNING, "Inserted / updated transform expression mapping '%s' <-> '%s'",
						expressentrylookup->key.extObjName,
						expressentrylookup->pgsqlTransExpress);
			}
		}
		else if (!strcasecmp(objs[i].objtype, "datatype"))
		{
			char * srccopy = pstrdup(objs[i].srcobj);
			char * dstcopy = pstrdup(objs[i].dstobj);
			char * tmp = NULL;

			if (!objs[i].enabled)
			{
				/*
				 * ignore disabled data type rules. We will not attempt to delete data type hash
				 * tables because it could contain overwritten default entries that we do not want
				 * to remove
				 */
				elog(WARNING, "Ignored disabled data type mapping '%s' <-> '%s'", srccopy, dstcopy);
				pfree(srccopy);
				pfree(dstcopy);
				continue;
			}
			memset(&hashentry, 0, sizeof(DatatypeHashEntry));

			strlcpy(hashentry.key.extTypeName, strtok(srccopy, "|"), SYNCHDB_DATATYPE_NAME_SIZE);

			tmp = strtok(NULL, "|");
			if (tmp && !strcasecmp(tmp, "true"))
				hashentry.key.autoIncremented = true;
			else
				hashentry.key.autoIncremented = false;

			strlcpy(hashentry.pgsqlTypeName, strtok(dstcopy, "|"), SYNCHDB_DATATYPE_NAME_SIZE);
			tmp = strtok(NULL, "|");
			if (tmp)
				hashentry.pgsqlTypeLength = atoi(tmp);
			else
				hashentry.pgsqlTypeLength = -1;

			entrylookup = (DatatypeHashEntry *) hash_search(rulehash,
					&(hashentry.key), HASH_ENTER, &found);

			entrylookup->key.autoIncremented = hashentry.key.autoIncremented;
			memset(entrylookup->key.extTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strlcpy(entrylookup->key.extTypeName,
					hashentry.key.extTypeName,
					SYNCHDB_DATATYPE_NAME_SIZE);

			entrylookup->pgsqlTypeLength = hashentry.pgsqlTypeLength;
			memset(entrylookup->pgsqlTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strlcpy(entrylookup->pgsqlTypeName,
					hashentry.pgsqlTypeName,
					SYNCHDB_DATATYPE_NAME_SIZE);

			elog(WARNING, "Inserted / updated data type mapping '%s' <-> '%s' %d - curr %s", entrylookup->key.extTypeName,
					entrylookup->pgsqlTypeName, entrylookup->pgsqlTypeLength, objs[i].curr_pg_atttypename);

			if (objs[i].curr_pg_atttypename[0] != '\0' && objs[i].curr_pg_tbname[0] != '\0' &&
					objs[i].curr_pg_attname[0] != '\0')
			{
				elog(WARNING, "comparing data types consistency for %s.%s: '%s' vs '%s'",
						objs[i].curr_pg_tbname,
						objs[i].curr_pg_attname,
						objs[i].curr_pg_atttypename,
						hashentry.pgsqlTypeName);
				if (strcasecmp(objs[i].curr_pg_atttypename, hashentry.pgsqlTypeName))
				{
					/* todo complex conversion with conversion func not supported yet */
					alter_atttype(objs[i].curr_pg_tbname, objs[i].curr_pg_attname, hashentry.pgsqlTypeName,
							entrylookup->pgsqlTypeLength, NULL);
				}
			}
			pfree(srccopy);
			pfree(dstcopy);
		}
	}
	return true;
}


/*
 * find_exact_string_match
 *
 * Function to find exact match from given line
 */
bool
find_exact_string_match(const char * line, const char * wordtofind)
{
	char * p = strstr(line, wordtofind);
	if ((p == line) || (p != NULL && !isalnum((unsigned char)p[-1])))
	{
		p += strlen(wordtofind);
		if (!isalnum((unsigned char)*p))
			return true;
		else
			return false;
	}
	return false;
}

/*
 * remove_double_quotes
 *
 * Function to remove double quotes from a string
 */
void
remove_double_quotes(StringInfoData * str)
{
	char *src = str->data, *dst = str->data;
	int newlen = 0;

	while (*src)
	{
		if (*src != '"' && *src != '\\')
		{
			*dst++ = *src;
			newlen++;
		}
		src++;
	}
	*dst = '\0';
	str->len = newlen;
}

/*
 * escapeSingleQuote
 *
 * escape the single quotes in the given input and returns a palloc-ed string
 */
char *
escapeSingleQuote(const char * in, bool addquote)
{
	int i = 0, j = 0, outlen = 0;
	char * out = NULL;

	/* escape possible single quotes */
	for (i = 0; i < strlen(in); i++)
	{
		if (in[i] == '\'')
		{
			/* single quote will be escaped so +2 in size */
			outlen += 2;
		}
		else
		{
			outlen++;
		}
	}

	if (addquote)
		/* 2 more to account for open and closing quotes */
		out = (char *) palloc0(outlen + 1 + 2);
	else
		out = (char *) palloc0(outlen + 1);

	if (addquote)
		out[j++] = '\'';
	for (i = 0; i < strlen(in); i++)
	{
		if (in[i] == '\'')
		{
			out[j++] = '\'';
			out[j++] = '\'';
		}
		else
		{
			out[j++] = in[i];
		}
	}
	if (addquote)
		out[j++] = '\'';

	return out;
}

/*
 * transform_object_name
 *
 * transform the remote object name based on the object name rule file definitions
 */
char *
transform_object_name(const char * objid, const char * objtype)
{
	ObjMapHashEntry * entry = NULL;
	ObjMapHashKey key = {0};
	bool found = false;
	char * res = NULL;

	/*
	 * return NULL immediately if objectMappingHash has not been initialized. Most
	 * likely the connector does not have a rule file specified
	 */
	if (!objectMappingHash)
		return NULL;

	if (!objid || !objtype)
		return NULL;

	strncpy(key.extObjName, objid, strlen(objid));
	strncpy(key.extObjType, objtype, strlen(objtype));
	entry = (ObjMapHashEntry *) hash_search(objectMappingHash, &key, HASH_FIND, &found);
	if (found)
	{
		res = pstrdup(entry->pgsqlObjName);
	}
	return res;
}


/*
 * getPathElementString
 *
 * Function to get a string element from a JSONB path
 */
int
getPathElementString(Jsonb * jb, char * path, StringInfoData * strinfoout, bool removequotes)
{
	Datum * datum_elems = NULL;
	char * str_elems = NULL, * p = path;
	int numPaths = 0, curr = 0;
	char * pathcopy = path;
	Datum res;
	bool isnull;

	if (!strinfoout)
		return -1;

    /* Count the number of elements in the path */
	if (strchr(pathcopy, '.'))
	{
		while (*p != '\0')
		{
			if (*p == '.')
			{
				numPaths++;
			}
			p++;
		}
		numPaths++; /* Add the last one */
		pathcopy = pstrdup(path);
	}
	else
	{
		numPaths = 1;
	}

	datum_elems = palloc0(sizeof(Datum) * numPaths);

    /* Parse the path into elements */
	if (numPaths > 1)
	{
		str_elems= strtok(pathcopy, ".");
		if (str_elems)
		{
			datum_elems[curr] = CStringGetTextDatum(str_elems);
			curr++;
			while ((str_elems = strtok(NULL, ".")))
			{
				datum_elems[curr] = CStringGetTextDatum(str_elems);
				curr++;
			}
		}
	}
	else
	{
		/* only one level, just use pathcopy*/
		datum_elems[curr] = CStringGetTextDatum(pathcopy);
	}

    /* Get the element from JSONB */
    res = jsonb_get_element(jb, datum_elems, numPaths, &isnull, false);
    if (isnull)
    {
    	resetStringInfo(strinfoout);
    	appendStringInfoString(strinfoout, "NULL");
    }
    else
    {
    	Jsonb *resjb = DatumGetJsonbP(res);
    	resetStringInfo(strinfoout);
		JsonbToCString(strinfoout, &resjb->root, VARSIZE(resjb));

		/*
		 * note: buf.data includes double quotes and escape char \.
		 * We need to remove them
		 */
		if (removequotes)
			remove_double_quotes(strinfoout);

		if (resjb)
			pfree(resjb);
    }

	pfree(datum_elems);
	if (pathcopy != path)
		pfree(pathcopy);
	return 0;
}

/*
 * splitIdString
 *
 * Function to split ID string into database, schema, and table.
 *
 * This function breaks down a fully qualified id string (database.
 * schema.table, schema.table, or database.table) into individual
 * components.
 */
void
splitIdString(char * id, char ** db, char ** schema, char ** table, bool usedb)
{
	int dotCount = 0;
	char *p = NULL;

	for (p = id; *p != '\0'; p++)
	{
		if (*p == '.')
			dotCount++;
	}

	if (dotCount == 1)
	{
		if (usedb)
		{
			/* treat it as database.table */
			*db = strtok(id, ".");
			*schema = NULL;
			*table = strtok(NULL, ".");
		}
		else
		{
			/* treat it as schema.table */
			*db = NULL;
			*schema = strtok(id, ".");
			*table = strtok(NULL, ".");
		}
	}
	else if (dotCount == 2)
	{
		/* treat it as database.schema.table */
		*db = strtok(id, ".");
		*schema = strtok(NULL, ".");
		*table = strtok(NULL, ".");
	}
	else if (dotCount == 0)
	{
		/* treat it as table */
		*db = NULL;
		*schema = NULL;
		*table = id;
	}
	else
	{
		elog(WARNING, "invalid ID string format %s", id);
		*db = NULL;
		*schema = NULL;
		*table = NULL;
	}
}

/*
 * convert2PGDDL
 *
 * This functions converts DBZ_DDL to PG_DDL structure
 */
PG_DDL *
convert2PGDDL(DBZ_DDL * dbzddl, ConnectorType type)
{
	PG_DDL * pgddl = (PG_DDL*) palloc0(sizeof(PG_DDL));
	ListCell * cell;
	StringInfoData strinfo;
	char * mappedObjName = NULL;
	char * db = NULL, * schema = NULL, * table = NULL;
	PG_DDL_COLUMN * pgcol = NULL;

	initStringInfo(&strinfo);

	pgddl->type = dbzddl->type;

	if (dbzddl->type == DDL_CREATE_TABLE)
	{
		int attnum = 1;
		mappedObjName = transform_object_name(dbzddl->id, "table");
		if (mappedObjName)
		{
			/*
			 * we will used the transformed object name here, but first, we will check if
			 * transformed name is valid. It should be expressed in one of the forms below:
			 * - schema.table
			 * - table
			 *
			 * then we check if schema is spplied. If yes, we need to add the CREATE SCHEMA
			 * clause as well.
			 */
			splitIdString(mappedObjName, &db, &schema, &table, false);
			if (!table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "transformed object ID is invalid: %s",
						mappedObjName);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			if (schema && table)
			{
				/* include create schema clause */
				appendStringInfo(&strinfo, "CREATE SCHEMA IF NOT EXISTS %s; ", schema);

				/* table stays as table under the schema */
				appendStringInfo(&strinfo, "CREATE TABLE IF NOT EXISTS %s.%s (", schema, table);
				pgddl->schema = pstrdup(schema);
				pgddl->tbname = pstrdup(table);
			}
			else if (!schema && table)
			{
				/* table stays as table but no schema */
				appendStringInfo(&strinfo, "CREATE TABLE IF NOT EXISTS %s (", table);
				pgddl->schema = pstrdup("public");
				pgddl->tbname = pstrdup(table);
			}
		}
		else
		{
			/*
			 * no object name mapping found. Transform it using default methods below:
			 *
			 * database.table:
			 * 	- database becomes schema in PG
			 * 	- table name stays
			 *
			 * database.schema.table:
			 * 	- database becomes schema in PG
			 * 	- schema is ignored
			 * 	- table name stays
			 */
			char * idcopy = pstrdup(dbzddl->id);
			splitIdString(idcopy, &db, &schema, &table, true);

			/* database and table must be present. schema is optional */
			if (!db || !table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "malformed id field in dbz change event: %s",
						dbzddl->id);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			/* database mapped to schema */
			appendStringInfo(&strinfo, "CREATE SCHEMA IF NOT EXISTS \"%s\"; ", db);

			/* table stays as table, schema ignored */
			appendStringInfo(&strinfo, "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (", db, table);
			pgddl->schema = pstrdup(db);
			pgddl->tbname = pstrdup(table);

			pfree(idcopy);
		}

		foreach(cell, dbzddl->columns)
		{
			DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);
			pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));

			transformDDLColumns(dbzddl->id, col, type, false, &strinfo, pgcol);

			/* if both length and scale are specified, add them. For example DECIMAL(10,2) */
			if (col->length > 0 && col->scale > 0)
			{
				appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
			}

			/* if a only length if specified, add it. For example VARCHAR(30)*/
			if (col->length > 0 && col->scale == 0)
			{
				/* make sure it does not exceed postgresql allowed maximum */
				if (col->length > MaxAttrSize)
					col->length = MaxAttrSize;
				appendStringInfo(&strinfo, "(%d) ", col->length);
			}

			/* if there is UNSIGNED operator found in col->typeName, add CHECK constraint */
			if (strstr(col->typeName, "unsigned"))
			{
				appendStringInfo(&strinfo, "CHECK (%s >= 0) ", col->name);
			}

			/* is it optional? */
			if (!col->optional)
			{
				appendStringInfo(&strinfo, "NOT NULL ");
			}

			/* does it have defaults? */
			if (col->defaultValueExpression && strlen(col->defaultValueExpression) > 0
					&& !col->autoIncremented)
			{
				/* use DEFAULT NULL regardless of the value of col->defaultValueExpression
				 * because it may contain expressions not recognized by PostgreSQL. We could
				 * make this part more intelligent by checking if the given expression can
				 * be applied by PostgreSQL and use it only when it can. But for now, we will
				 * just put default null here. Todo
				 */
				appendStringInfo(&strinfo, "DEFAULT %s ", "NULL");
			}

			/* for create, the position(attnum) should be in the same order as they are created */
			pgcol->position = attnum++;

			appendStringInfo(&strinfo, ",");
			pgddl->columns =lappend(pgddl->columns, pgcol);
		}

		/* remove the last extra comma */
		strinfo.data[strinfo.len - 1] = '\0';
		strinfo.len = strinfo.len - 1;

		/*
		 * finally, declare primary keys if any. iterate dbzddl->primaryKeyColumnNames
		 * and build into primary key(x, y, z) clauses. todo
		 */
		populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, false, true);

		appendStringInfo(&strinfo, ");");
	}
	else if (dbzddl->type == DDL_DROP_TABLE)
	{
		DataCacheKey cachekey = {0};
		bool found = false;

		mappedObjName = transform_object_name(dbzddl->id, "table");
		if (mappedObjName)
		{
			/*
			 * we will used the transformed object name here, but first, we will check if
			 * transformed name is valid. It should be expressed in one of the forms below:
			 * - schema.table
			 * - table
			 */
			splitIdString(mappedObjName, &db, &schema, &table, false);
			if (!table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "transformed object ID is invalid: %s",
						mappedObjName);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			if (schema && table)
			{
				/* table stays as table under the schema */
				appendStringInfo(&strinfo, "DROP TABLE IF EXISTS \"%s\".\"%s\";", schema, table);
				pgddl->schema = pstrdup(schema);
				pgddl->tbname = pstrdup(table);
			}
			else if (!schema && table)
			{
				/* table stays as table but no schema */
				schema = pstrdup("public");
				appendStringInfo(&strinfo, "DROP TABLE IF EXISTS \"%s\";", table);
				pgddl->schema = pstrdup("public");
				pgddl->tbname = pstrdup(table);
			}
		}
		else
		{
			char * idcopy = pstrdup(dbzddl->id);
			splitIdString(idcopy, &db, &schema, &table, true);

			/* database and table must be present. schema is optional */
			if (!db || !table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "malformed id field in dbz change event: %s",
						dbzddl->id);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}
			/* make schema points to db */
			schema = db;
			appendStringInfo(&strinfo, "DROP TABLE IF EXISTS \"%s\".\"%s\";", schema, table);
			pgddl->schema = pstrdup(schema);
			pgddl->tbname = pstrdup(table);
		}

		/* no column information needed for DROP */
		pgddl->columns = NULL;

		/* drop data cache for schema.table if exists */
		strlcpy(cachekey.schema, schema, SYNCHDB_CONNINFO_DB_NAME_SIZE);
		strlcpy(cachekey.table, table, SYNCHDB_CONNINFO_DB_NAME_SIZE);
		hash_search(dataCacheHash, &cachekey, HASH_REMOVE, &found);

	}
	else if (dbzddl->type == DDL_ALTER_TABLE)
	{
		int i = 0, attnum = 1, newcol = 0;
		Oid schemaoid = 0;
		Oid tableoid = 0;
		Oid pkoid = 0;
		bool found = false, altered = false;
		Relation rel;
		TupleDesc tupdesc;
		DataCacheKey cachekey = {0};
		StringInfoData colNameObjId;

		initStringInfo(&colNameObjId);

		mappedObjName = transform_object_name(dbzddl->id, "table");
		if (mappedObjName)
		{
			/*
			 * we will used the transformed object name here, but first, we will check if
			 * transformed name is valid. It should be expressed in one of the forms below:
			 * - schema.table
			 * - table
			 */
			splitIdString(mappedObjName, &db, &schema, &table, false);
			if (!table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "transformed object ID is invalid: %s",
						mappedObjName);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			if (schema && table)
			{
				/* table stays as table under the schema */
				appendStringInfo(&strinfo, "ALTER TABLE \"%s\".\"%s\" ", schema, table);
				pgddl->schema = pstrdup(schema);
				pgddl->tbname = pstrdup(table);
			}
			else if (!schema && table)
			{
				/* table stays as table but no schema */
				schema = pstrdup("public");
				appendStringInfo(&strinfo, "ALTER TABLE \"%s\" ", table);
				pgddl->schema = pstrdup("public");
				pgddl->tbname = pstrdup(table);
			}
		}
		else
		{
			/* by default, remote's db is mapped to schema in pg */
			char * idcopy = pstrdup(dbzddl->id);
			splitIdString(idcopy, &db, &schema, &table, true);

			/* database and table must be present. schema is optional */
			if (!db || !table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "malformed id field in dbz change event: %s",
						dbzddl->id);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			for (i = 0; i < strlen(db); i++)
				db[i] = (char) pg_tolower((unsigned char) db[i]);

			for (i = 0; i < strlen(table); i++)
				table[i] = (char) pg_tolower((unsigned char) table[i]);

			/* make schema points to db */
			schema = db;
			appendStringInfo(&strinfo, "ALTER TABLE \"%s\".\"%s\" ", schema, table);
			pgddl->schema = pstrdup(schema);
			pgddl->tbname = pstrdup(table);
		}

		/* drop data cache for schema.table if exists */
		strlcpy(cachekey.schema, schema, SYNCHDB_CONNINFO_DB_NAME_SIZE);
		strlcpy(cachekey.table, table, SYNCHDB_CONNINFO_DB_NAME_SIZE);
		hash_search(dataCacheHash, &cachekey, HASH_REMOVE, &found);

		/*
		 * For ALTER, we must obtain the current schema in PostgreSQL and identify
		 * which column is the new column added. We will first check if table exists
		 * and then add its column to a temporary hash table that we can compare
		 * with the new column list.
		 */
		schemaoid = get_namespace_oid(schema, false);
		if (!OidIsValid(schemaoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for schema '%s'", schema);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		tableoid = get_relname_relid(table, schemaoid);
		if (!OidIsValid(tableoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for table '%s'", table);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		elog(DEBUG1, "namespace \"%s\".\"%s\" has PostgreSQL OID %d", schema, table, tableoid);

		rel = table_open(tableoid, AccessShareLock);
		tupdesc = RelationGetDescr(rel);
#if SYNCHDB_PG_MAJOR_VERSION >= 1800
		pkoid = RelationGetPrimaryKeyIndex(rel, true);
#else
		pkoid = RelationGetPrimaryKeyIndex(rel);
#endif
		table_close(rel, AccessShareLock);

		if (type != TYPE_OLR)
		{
			/*
			 * DBZ supplies more columns than what PostreSQL have. This means ALTER
			 * TABLE ADD COLUMN operation. Let's find which one is to be added.
			 */
			if (list_length(dbzddl->columns) > count_active_columns(tupdesc))
			{
				altered = false;

				/* indicate pgddl that this is alter add column */
				pgddl->subtype = SUBTYPE_ADD_COLUMN;

				foreach(cell, dbzddl->columns)
				{
					DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);
					char * mappedColumnName = NULL;

					pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));

					/* express a column name in fully qualified id */
					resetStringInfo(&colNameObjId);
					appendStringInfo(&colNameObjId, "%s.%s", dbzddl->id, col->name);
					mappedColumnName = transform_object_name(colNameObjId.data, "column");
					if (mappedColumnName)
					{
						elog(DEBUG1, "transformed column object ID '%s'to '%s'",
								colNameObjId.data, mappedColumnName);
					}
					else
						mappedColumnName = pstrdup(col->name);

					found = false;
					for (attnum = 1; attnum <= tupdesc->natts; attnum++)
					{
						Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

						/* skip special attname indicated a dropped column */
						if (strstr(NameStr(attr->attname), "pg.dropped"))
							continue;

						if (!strcasecmp(mappedColumnName, NameStr(attr->attname)))
						{
							found = true;
							break;
						}
					}
					if (!found)
					{
						elog(DEBUG1, "adding new column %s", mappedColumnName);
						altered = true;
						appendStringInfo(&strinfo, "ADD COLUMN IF NOT EXISTS ");

						transformDDLColumns(dbzddl->id, col, type, false, &strinfo, pgcol);

						/* if both length and scale are specified, add them. For example DECIMAL(10,2) */
						if (col->length > 0 && col->scale > 0)
						{
							appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
						}

						/* if a only length if specified, add it. For example VARCHAR(30)*/
						if (col->length > 0 && col->scale == 0)
						{
							/* make sure it does not exceed postgresql allowed maximum */
							if (col->length > MaxAttrSize)
								col->length = MaxAttrSize;
							appendStringInfo(&strinfo, "(%d) ", col->length);
						}

						/* if there is UNSIGNED operator found in col->typeName, add CHECK constraint */
						if (strstr(col->typeName, "unsigned"))
						{
							appendStringInfo(&strinfo, "CHECK (%s >= 0) ", pgcol->attname);
						}

						/* is it optional? */
						if (!col->optional)
						{
							appendStringInfo(&strinfo, "NOT NULL ");
						}

						/* does it have defaults? */
						if (col->defaultValueExpression && strlen(col->defaultValueExpression) > 0
								&& !col->autoIncremented)
						{
							/* use DEFAULT NULL regardless of the value of col->defaultValueExpression
							 * because it may contain expressions not recognized by PostgreSQL. We could
							 * make this part more intelligent by checking if the given expression can
							 * be applied by PostgreSQL and use it only when it can. But for now, we will
							 * just put default null here. Todo
							 */
							appendStringInfo(&strinfo, "DEFAULT %s ", "NULL");
						}
						appendStringInfo(&strinfo, ",");

						/* assign new attnum for this newly added column */
						pgcol->position = attnum + newcol;
						newcol++;
					}
					else
					{
						/* not a column to add, point data to null and act as a placeholder in the list */
						pgcol->attname = NULL;
						pgcol->atttype = NULL;
					}

					/*
					 * add to list regardless if this column is to be added or not so we keep both ddl->columns
					 * and pgddl->columns the same size
					 */
					pgddl->columns = lappend(pgddl->columns, pgcol);

					if(mappedColumnName)
						pfree(mappedColumnName);

					if (colNameObjId.data)
						pfree(colNameObjId.data);
				}

				if (altered)
				{
					/* something has been altered, continue to wrap up... */
					/* remove the last extra comma */
					strinfo.data[strinfo.len - 1] = '\0';
					strinfo.len = strinfo.len - 1;

					/*
					 * finally, declare primary keys if current table has no primary key index.
					 * Iterate dbzddl->primaryKeyColumnNames and build into primary key(x, y, z)
					 * clauses. We will not add new primary key if current table already has pk.
					 *
					 * If a primary key is added in the same clause as alter table add column, debezium
					 * may not be able to include the list of primary keys properly (observed in oracle
					 * connector). We need to ensure that it is done in 2 qureies; first to add a new
					 * column and second to add a new primary on the new column.
					 */
					if (pkoid == InvalidOid)
						populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, true, true);
				}
				else
				{
					elog(DEBUG1, "no column altered");
					destroyPGDDL(pgddl);
					return NULL;
				}
			}
			else if (list_length(dbzddl->columns) < count_active_columns(tupdesc))
			{
				/*
				 * DBZ supplies less columns than what PostreSQL have. This means ALTER
				 * TABLE DROP COLUMN operation. Let's find which one is to be dropped.
				 */
				altered = false;

				/* indicate pgddl that this is alter drop column */
				pgddl->subtype = SUBTYPE_DROP_COLUMN;

				for (attnum = 1; attnum <= tupdesc->natts; attnum++)
				{
					Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
					found = false;

					/* skip special attname indicated a dropped column */
					if (strstr(NameStr(attr->attname), "pg.dropped"))
						continue;

					foreach(cell, dbzddl->columns)
					{
						DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);
						char * mappedColumnName = NULL;

						/* express a column name in fully qualified id */
						resetStringInfo(&colNameObjId);
						appendStringInfo(&colNameObjId, "%s.%s", dbzddl->id, col->name);
						mappedColumnName = transform_object_name(colNameObjId.data, "column");
						if (mappedColumnName)
						{
							elog(DEBUG1, "transformed column object ID '%s'to '%s'",
									colNameObjId.data, mappedColumnName);
						}
						else
							mappedColumnName = pstrdup(col->name);

						if (!strcasecmp(mappedColumnName, NameStr(attr->attname)))
						{
							found = true;
							if (mappedColumnName)
								pfree(mappedColumnName);

							if (colNameObjId.data)
								pfree(colNameObjId.data);

							break;
						}
						if (mappedColumnName)
							pfree(mappedColumnName);

						if (colNameObjId.data)
							pfree(colNameObjId.data);
					}
					if (!found)
					{
						elog(DEBUG1, "dropping old column %s", NameStr(attr->attname));

						pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));
						altered = true;
						appendStringInfo(&strinfo, "DROP COLUMN IF EXISTS %s,", NameStr(attr->attname));
						pgcol->attname = pstrdup(NameStr(attr->attname));
						pgcol->position = attnum;
						pgddl->columns = lappend(pgddl->columns, pgcol);
					}
				}
				if(altered)
				{
					/* something has been altered, continue to wrap up... */
					/* remove the last extra comma */
					strinfo.data[strinfo.len - 1] = '\0';
					strinfo.len = strinfo.len - 1;
				}
				else
				{
					elog(DEBUG1, "no column altered");
					destroyPGDDL(pgddl);
					return NULL;
				}
			}
			else
			{
				/*
				 * DBZ supplies the same number of columns as what PostreSQL have. This means
				 * general ALTER TABLE operation.
				 */
				char * alterclause = NULL;

				/* indicate pgddl that this is alter column ddl*/
				pgddl->subtype = SUBTYPE_ALTER_COLUMN;

				alterclause = composeAlterColumnClauses(dbzddl->id, type, dbzddl->columns, tupdesc, rel, pgddl);
				if (alterclause)
				{
					appendStringInfo(&strinfo, "%s", alterclause);
					elog(DEBUG1, "alter clause: %s", strinfo.data);
					pfree(alterclause);
				}
				else
				{
					elog(DEBUG1, "no column altered");
					destroyPGDDL(pgddl);
					return NULL;
				}
				/*
				 * finally, declare primary keys if current table has no primary key index.
				 * Iterate dbzddl->primaryKeyColumnNames and build into primary key(x, y, z)
				 * clauses. We will not add new primary key if current table already has pk.
				 */
				if (pkoid == InvalidOid)
					populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, true, true);
			}
		}
#ifdef WITH_OLR
		else
		{
			/* pass the subtype to pgddl */
			pgddl->subtype = dbzddl->subtype;

			/*
			 * OLR connector gives the added and dropped columns directly so there is no
			 * need to figure out that information
			 */
			if (dbzddl->subtype == SUBTYPE_ADD_COLUMN)
			{
				/* ADD COLUMN */
				if (list_length(dbzddl->columns) <= 0)
				{
					elog(WARNING, "no columns to add");
					destroyPGDDL(pgddl);
					return NULL;
				}

				altered = false;
				foreach(cell, dbzddl->columns)
				{
					OLR_DDL_COLUMN * col = (OLR_DDL_COLUMN *) lfirst(cell);
					char * mappedColumnName = NULL;

					pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));

					/* express a column name in fully qualified id */
					resetStringInfo(&colNameObjId);
					appendStringInfo(&colNameObjId, "%s.%s", dbzddl->id, col->name);
					mappedColumnName = transform_object_name(colNameObjId.data, "column");
					if (mappedColumnName)
					{
						elog(DEBUG1, "transformed column object ID '%s'to '%s'",
								colNameObjId.data, mappedColumnName);
					}
					else
						mappedColumnName = pstrdup(col->name);

					elog(DEBUG1, "adding new column %s", mappedColumnName);
					altered = true;
					appendStringInfo(&strinfo, "ADD COLUMN IF NOT EXISTS ");

					transformDDLColumns(dbzddl->id, col, type, false, &strinfo, pgcol);

					/* if both length and scale are specified, add them. For example DECIMAL(10,2) */
					if (col->length > 0 && col->scale > 0)
					{
						appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
					}

					/* if a only length if specified, add it. For example VARCHAR(30)*/
					if (col->length > 0 && col->scale == 0)
					{
						/* make sure it does not exceed postgresql allowed maximum */
						if (col->length > MaxAttrSize)
							col->length = MaxAttrSize;
						appendStringInfo(&strinfo, "(%d) ", col->length);
					}

					/* if there is UNSIGNED operator found in col->typeName, add CHECK constraint */
					if (strstr(col->typeName, "unsigned"))
					{
						appendStringInfo(&strinfo, "CHECK (%s >= 0) ", pgcol->attname);
					}

					/* is it optional? */
					if (!col->optional)
					{
						appendStringInfo(&strinfo, "NOT NULL ");
					}

					/* does it have defaults? */
					if (col->defaultValueExpression && strlen(col->defaultValueExpression) > 0
							&& !col->autoIncremented)
					{
						/* use DEFAULT NULL regardless of the value of col->defaultValueExpression
						 * because it may contain expressions not recognized by PostgreSQL. We could
						 * make this part more intelligent by checking if the given expression can
						 * be applied by PostgreSQL and use it only when it can. But for now, we will
						 * just put default null here. Todo
						 */
						appendStringInfo(&strinfo, "DEFAULT %s ", "NULL");
					}
					appendStringInfo(&strinfo, ",");

					newcol++;
					pgcol->position = tupdesc->natts + newcol;
					pgddl->columns = lappend(pgddl->columns, pgcol);

					if(mappedColumnName)
						pfree(mappedColumnName);

					if (colNameObjId.data)
						pfree(colNameObjId.data);
				}

				if (altered)
				{
					/* something has been altered, continue to wrap up... */
					/* remove the last extra comma */
					strinfo.data[strinfo.len - 1] = '\0';
					strinfo.len = strinfo.len - 1;

					/*
					 * finally, declare primary keys if current table has no primary key index.
					 * Iterate dbzddl->primaryKeyColumnNames and build into primary key(x, y, z)
					 * clauses. We will not add new primary key if current table already has pk.
					 *
					 * If a primary key is added in the same clause as alter table add column, debezium
					 * may not be able to include the list of primary keys properly (observed in oracle
					 * connector). We need to ensure that it is done in 2 qureies; first to add a new
					 * column and second to add a new primary on the new column.
					 */
					if (pkoid == InvalidOid)
						populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, true, true);
				}
				else
				{
					elog(DEBUG1, "no column altered");
					destroyPGDDL(pgddl);
					return NULL;
				}
			}
			else if (dbzddl->subtype == SUBTYPE_DROP_COLUMN)
			{
				/* DROP COLUMN */
				if (list_length(dbzddl->columns) <= 0)
				{
					elog(WARNING, "no columns to drop");
					destroyPGDDL(pgddl);
					return NULL;
				}

				altered = false;
				foreach(cell, dbzddl->columns)
				{
					OLR_DDL_COLUMN * col = (OLR_DDL_COLUMN *) lfirst(cell);
					char * mappedColumnName = NULL;

					/* express a column name in fully qualified id */
					resetStringInfo(&colNameObjId);
					appendStringInfo(&colNameObjId, "%s.%s", dbzddl->id, col->name);
					mappedColumnName = transform_object_name(colNameObjId.data, "column");
					if (mappedColumnName)
					{
						elog(DEBUG1, "transformed column object ID '%s'to '%s'",
								colNameObjId.data, mappedColumnName);
					}
					else
						mappedColumnName = pstrdup(col->name);

					/*
					 * in order to update synchdb_attribute table correctly, we need to find
					 * this dropped column's attribute id from postgres
					 */
					for (attnum = 1; attnum <= tupdesc->natts; attnum++)
					{
						Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

						/* skip special attname indicated a dropped column */
						if (strstr(NameStr(attr->attname), "pg.dropped"))
							continue;

						if (!strcasecmp(mappedColumnName, NameStr(attr->attname)))
							break;
					}

					appendStringInfo(&strinfo, "DROP COLUMN IF EXISTS %s,", mappedColumnName);

					elog(DEBUG1, "dropping old column %s", mappedColumnName);

					pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));
					altered = true;

					pgcol->attname = pstrdup(mappedColumnName);
					pgcol->position = attnum;
					pgddl->columns = lappend(pgddl->columns, pgcol);

					if (mappedColumnName)
						pfree(mappedColumnName);

					if (colNameObjId.data)
						pfree(colNameObjId.data);
				}
				if(altered)
				{
					/* something has been altered, continue to wrap up... */
					/* remove the last extra comma */
					strinfo.data[strinfo.len - 1] = '\0';
					strinfo.len = strinfo.len - 1;
				}
			}
			else if (dbzddl->subtype == SUBTYPE_ALTER_COLUMN)
			{
				/* ALTER COLUMN */
				if (list_length(dbzddl->columns) <= 0)
				{
					elog(WARNING, "no columns to alter");
					destroyPGDDL(pgddl);
					return NULL;
				}

				altered = false;
				foreach(cell, dbzddl->columns)
				{
					char * mappedColumnName = NULL;
					OLR_DDL_COLUMN * col = (OLR_DDL_COLUMN *) lfirst(cell);
					pgcol = (PG_DDL_COLUMN *) palloc0(sizeof(PG_DDL_COLUMN));

					/* express a column name in fully qualified id */
					resetStringInfo(&colNameObjId);
					appendStringInfo(&colNameObjId, "%s.%s", dbzddl->id, col->name);
					mappedColumnName = transform_object_name(colNameObjId.data, "column");
					if (mappedColumnName)
					{
						elog(DEBUG1, "transformed column object ID '%s'to '%s'",
								colNameObjId.data, mappedColumnName);
					}
					else
						mappedColumnName = pstrdup(col->name);

					/*
					 * in order to update synchdb_attribute table correctly, we need to find
					 * this dropped column's attribute id from postgres
					 */
					for (attnum = 1; attnum <= tupdesc->natts; attnum++)
					{
						Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

						/* skip special attname indicated a dropped column */
						if (strstr(NameStr(attr->attname), "pg.dropped"))
							continue;

						if (!strcasecmp(mappedColumnName, NameStr(attr->attname)))
							break;
					}

					/* check data type */
					elog(DEBUG1, "altering column %s", col->name);
					appendStringInfo(&strinfo, "ALTER COLUMN %s SET DATA TYPE",
							mappedColumnName);

					transformDDLColumns(dbzddl->id, col, type, true, &strinfo, pgcol);
					if (col->length > 0 && col->scale > 0)
					{
						appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
					}

					/* if a only length if specified, add it. For example VARCHAR(30)*/
					if (col->length > 0 && col->scale == 0)
					{
						/* make sure it does not exceed postgresql allowed maximum */
						if (col->length > MaxAttrSize)
							col->length = MaxAttrSize;
						appendStringInfo(&strinfo, "(%d) ", col->length);
					}

					/*
					 * todo: for complex data type transformation, postgresql requires
					 * the user to specify a function to cast existing values to the new
					 * data type via the USING clause. This is needed for INT -> TEXT,
					 * INT -> DATE or NUMERIC -> VARCHAR. We do not support USING now as
					 * we do not know what function the user wants to use for casting the
					 * values. Perhaps we can include these cast functions in the rule file
					 * as well, but for now this is not supported and PostgreSQL may complain
					 * if we attempt to do complex data type changes.
					 */
					appendStringInfo(&strinfo, ", ");

					if (col->defaultValueExpression)
					{
						/*
						 * synchdb can receive a default expression not supported in postgresql.
						 * so for now, we always set to default null. todo
						 */
						appendStringInfo(&strinfo, "ALTER COLUMN %s SET DEFAULT %s",
								mappedColumnName, "NULL");
					}
					else
					{
						/* remove default value */
						appendStringInfo(&strinfo, "ALTER COLUMN %s DROP DEFAULT",
								mappedColumnName);
					}

					appendStringInfo(&strinfo, ", ");

					/* check if nullable or not nullable */
					if (!col->optional)
					{
						appendStringInfo(&strinfo, "ALTER COLUMN %s SET NOT NULL",
								mappedColumnName);
					}
					else
					{
						appendStringInfo(&strinfo, "ALTER COLUMN %s DROP NOT NULL",
								mappedColumnName);
					}

					appendStringInfo(&strinfo, ",");

					/*
					 * finally, declare primary keys if current table has no primary key index.
					 * Iterate dbzddl->primaryKeyColumnNames and build into primary key(x, y, z)
					 * clauses. We will not add new primary key if current table already has pk.
					 */
					if (pkoid == InvalidOid)
						populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, true, true);

					pgcol->position = attnum;
					pgddl->columns = lappend(pgddl->columns, pgcol);
				}

				/* remove extra "," */
				strinfo.data[strinfo.len - 1] = '\0';
				strinfo.len = strinfo.len - 1;
			}
			else if (dbzddl->subtype == SUBTYPE_ADD_CONSTRAINT)
			{
				if (pkoid == InvalidOid)
				{
					appendStringInfo(&strinfo, "ADD CONSTRAINT %s ", dbzddl->constraintName);
					populate_primary_keys(&strinfo, dbzddl->id, dbzddl->primaryKeyColumnNames, true, false);
				}
				else
				{
					elog(WARNING, "relation already has primary key, skip adding...");
					destroyPGDDL(pgddl);
					return NULL;
				}
			}
			else if (dbzddl->subtype == SUBTYPE_DROP_CONSTRAINT)
			{
				if (dbzddl->constraintName)
					appendStringInfo(&strinfo, "DROP CONSTRAINT %s ", dbzddl->constraintName);
				else
				{
					elog(WARNING, "constaint name to drop is NULL. Skipping...");
					destroyPGDDL(pgddl);
					return NULL;
				}
			}
			else
			{
				elog(WARNING, "unsupported ALTER TABLE sub type %d", dbzddl->subtype);
				destroyPGDDL(pgddl);
				return NULL;
			}
		}
#else
		else
		{
			set_shm_connector_errmsg(myConnectorId, "OLR connector is not enabled in this synchdb build");
			elog(ERROR, "OLR connector is not enabled in this synchdb build");
		}
#endif
	}
	else if (dbzddl->type == DDL_TRUNCATE_TABLE)
	{
		mappedObjName = transform_object_name(dbzddl->id, "table");
		if (mappedObjName)
		{
			/*
			 * we will used the transformed object name here, but first, we will check if
			 * transformed name is valid. It should be expressed in one of the forms below:
			 * - schema.table
			 * - table
			 */
			splitIdString(mappedObjName, &db, &schema, &table, false);
			if (!table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "transformed object ID is invalid: %s",
						mappedObjName);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}

			if (schema && table)
			{
				/* table stays as table under the schema */
				appendStringInfo(&strinfo, "TRUNCATE TABLE \"%s\".\"%s\";", schema, table);
				pgddl->schema = pstrdup(schema);
				pgddl->tbname = pstrdup(table);
			}
			else if (!schema && table)
			{
				/* table stays as table but no schema */
				schema = pstrdup("public");
				appendStringInfo(&strinfo, "TRUNCATE TABLE \"%s\";", table);
				pgddl->schema = pstrdup("public");
				pgddl->tbname = pstrdup(table);
			}
		}
		else
		{
			char * idcopy = pstrdup(dbzddl->id);
			splitIdString(idcopy, &db, &schema, &table, true);

			/* database and table must be present. schema is optional */
			if (!db || !table)
			{
				/* save the error */
				char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
				snprintf(msg, SYNCHDB_ERRMSG_SIZE, "malformed id field in dbz change event: %s",
						dbzddl->id);
				set_shm_connector_errmsg(myConnectorId, msg);

				/* trigger pg's error shutdown routine */
				elog(ERROR, "%s", msg);
			}
			/* make schema points to db */
			schema = db;
			appendStringInfo(&strinfo, "TRUNCATE TABLE \"%s\".\"%s\";", schema, table);
			pgddl->schema = pstrdup(schema);
			pgddl->tbname = pstrdup(table);
		}
		/* no column information needed for TRUNCATE */
		pgddl->columns = NULL;
	}
	else
	{
		elog(WARNING, "unsupported conversion for DDL type %d", dbzddl->type);
		destroyPGDDL(pgddl);
		return NULL;
	}

	pgddl->ddlquery = pstrdup(strinfo.data);

	/* free the data inside strinfo as we no longer needs it */
	pfree(strinfo.data);

	elog(DEBUG1, "pgsql: %s ", pgddl->ddlquery);
	return pgddl;
}

bool
fc_translate_datatype(ConnectorType connectorType,
		const char * ext_datatype, int ext_length, int ext_scale,
		char ** pg_datatype, int * pg_datatype_len)
{
	DatatypeHashEntry * entry;
	DatatypeHashKey key = {0};
	bool found = 0;
	HTAB * thehash = NULL;

	switch (connectorType)
	{
		case TYPE_MYSQL:
			thehash = mysqlDatatypeHash;
			break;
		case TYPE_ORACLE:
		case TYPE_OLR:
			thehash = oracleDatatypeHash;
			break;
		case TYPE_SQLSERVER:
			thehash = sqlserverDatatypeHash;
			break;
		default:
		{
			elog(WARNING, "unsupported connector type");
			return false;
		}
	}
	if (!thehash)
	{
		elog(WARNING, "data type hash not initialized.");
		return false;
	}

	key.autoIncremented = false;

	/* handle per-connector special cases */
	if ((connectorType == TYPE_MYSQL || connectorType == TYPE_SQLSERVER) &&
			ext_length == 1 && !strcasecmp(ext_datatype, "bit"))
	{
		/* special case: bit with length 1 -> include length in lookup key */
		snprintf(key.extTypeName, sizeof(key.extTypeName), "%s(%d)",
				ext_datatype, ext_length);
	}
	else if (connectorType == TYPE_ORACLE && ext_scale == 0 &&
			!strcasecmp(ext_datatype, "number"))
	{
		/* special case: NUMBER with scale 0 -> include both length and scale in lookup key */
		snprintf(key.extTypeName, sizeof(key.extTypeName), "%s(%d,%d)",
				ext_datatype, ext_length, ext_scale);
	}
	else
		snprintf(key.extTypeName, sizeof(key.extTypeName), "%s", ext_datatype);

	entry = (DatatypeHashEntry *) hash_search(thehash, &key, HASH_FIND, &found);
	if (found)
	{
		*pg_datatype = pstrdup(entry->pgsqlTypeName);
		*pg_datatype_len = entry->pgsqlTypeLength;
		return true;
	}
	return false;
}
