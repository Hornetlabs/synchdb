#ifndef SYNCHDB_OLR_HANDOFF_H
#define SYNCHDB_OLR_HANDOFF_H

#include "runtime/java_runtime.h"
#include "synchdb/synchdb.h"
#include "utils/jsonb.h"

extern bool olr_handoff_prepare(const ConnectionInfo *connection, SynchdbJavaRuntime *java);
extern void olr_handoff_complete(const ConnectionInfo *connection, SynchdbJavaRuntime *java);
extern void olr_handoff_reconnect(const ConnectionInfo *connection, SynchdbJavaRuntime *java);
extern bool olr_handoff_active(void);
/* Caller owns the same transaction as the applied source rows. */
extern void olr_handoff_store_position(void);
extern bool olr_handoff_apply_record(Jsonb *record);
extern uint64 olr_json_uint(Jsonb *object, const char *field);
extern char *olr_json_text(Jsonb *object, const char *field);
extern Jsonb *olr_json_payload(Jsonb *record);

#endif
