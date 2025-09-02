/*
 * debezium_event_handler.h
 *
 * contains routines to process change events originated from
 * debezium connectors.
 */

#ifndef SYNCHDB_SRC_INCLUDE_CONVERTER_DEBEZIUM_EVENT_HANDLER_H_
#define SYNCHDB_SRC_INCLUDE_CONVERTER_DEBEZIUM_EVENT_HANDLER_H_

/*
 * DbzType
 *
 * enum that represents how debezium could represent
 * a data value
 */
typedef enum _DbzType
{
	/* DBZ types */
	DBZTYPE_UNDEF = 0,
	DBZTYPE_FLOAT32,
	DBZTYPE_FLOAT64,
	DBZTYPE_FLOAT,
	DBZTYPE_DOUBLE,
	DBZTYPE_BYTES,
	DBZTYPE_INT8,
	DBZTYPE_INT16,
	DBZTYPE_INT32,
	DBZTYPE_INT64,
	DBZTYPE_STRUCT,
	DBZTYPE_STRING,
} DbzType;

int fc_processDBZChangeEvent(const char * event, SynchdbStatistics * myBatchStats,
		int flag, const char * name, bool isfirst, bool islast);


#endif /* SYNCHDB_SRC_INCLUDE_CONVERTER_DEBEZIUM_EVENT_HANDLER_H_ */
