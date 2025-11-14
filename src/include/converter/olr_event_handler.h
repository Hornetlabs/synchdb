/*
 * olr_event_handler.h
 *
 * contains routines to process change events originated from
 * openlog replicator.
 */

#ifndef SYNCHDB_SRC_INCLUDE_CONVERTER_OLR_EVENT_HANDLER_H_
#define SYNCHDB_SRC_INCLUDE_CONVERTER_OLR_EVENT_HANDLER_H_

#define DBZ_LOG_MINING_FLUSH_TABLE "LOG_MINING_FLUSH"

/*
 * OlrType
 *
 * enum that represents how openlog replicator could represent
 * a data value
 */
typedef enum _OlrType
{
	/* OLR types */
	OLRTYPE_UNDEF,
	OLRTYPE_NUMBER,
	OLRTYPE_STRING
} OlrType;

int fc_processOLRChangeEvent(void * event, SynchdbStatistics * myBatchStats,
		const char * name, bool * sendconfirm, bool isfirst, bool islast);

void unload_oracle_parser(void);



#endif /* SYNCHDB_SRC_INCLUDE_CONVERTER_OLR_EVENT_HANDLER_H_ */
