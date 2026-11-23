/*
 * Process-local runtime settings shared by the coordinator, converters and
 * writers. Engine startup options otherwise remain private to the coordinator
 * and reach workers through their accepted SynchdbRunOptions snapshot.
 */
#ifndef SYNCHDB_RUNTIME_CONFIG_H
#define SYNCHDB_RUNTIME_CONFIG_H

#include "c.h"

extern bool synchdb_dml_use_spi;
extern int synchdb_max_connector_workers;
extern int synchdb_error_strategy;
extern bool synchdb_log_event_on_error;
extern int synchdb_letter_casing_strategy;

/* The accepted run snapshot takes precedence over live configuration. */
extern bool synchdb_operation_skipped(char operation);

/* OLR currently reads these settings directly in its I/O paths. */
extern int dbz_offset_flush_interval_ms;
extern int olr_read_buffer_size;
extern int olr_connect_timeout_ms;
extern int olr_read_timeout_ms;

/* Current worker diagnostics; the event bytes remain owned by their caller. */
extern int myConnectorId;
extern const char *g_eventStr;

#endif /* SYNCHDB_RUNTIME_CONFIG_H */
