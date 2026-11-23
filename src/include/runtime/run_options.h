/*
 * run_options.h
 *
 * Pointer-free options captured when a connector run is requested.
 */
#ifndef SYNCHDB_RUN_OPTIONS_H
#define SYNCHDB_RUN_OPTIONS_H

#include "c.h"

#define SYNCHDB_RUN_OPTIONS_VERSION 1
#define SYNCHDB_RUN_OPTION_TEXT_SIZE 256

typedef struct SynchdbRunOptions
{
	uint32 version;
	int dbz_batch_size;
	int dbz_queue_size;
	int dbz_connect_timeout_ms;
	int dbz_query_timeout_ms;
	char dbz_skipped_operations[SYNCHDB_RUN_OPTION_TEXT_SIZE];
	int dbz_snapshot_thread_num;
	int dbz_snapshot_fetch_size;
	int dbz_snapshot_min_row_to_stream_results;
	int dbz_incremental_snapshot_chunk_size;
	char dbz_incremental_snapshot_watermarking_strategy[SYNCHDB_RUN_OPTION_TEXT_SIZE];
	int dbz_offset_flush_interval_ms;
	bool dbz_capture_only_selected_table_ddl;
	int dbz_log_level;
	int dbz_logminer_stream_mode;
	int cdc_start_delay_ms;
	int jvm_max_heap_size;
	int jvm_max_direct_buffer_size;
	bool synchdb_fdw_use_subtx;
	int synchdb_letter_casing_strategy;
} SynchdbRunOptions;

#endif /* SYNCHDB_RUN_OPTIONS_H */
