/* Durable control commands and process-owned background-worker registrations. */
#ifndef SYNCHDB_CONTROLLER_H
#define SYNCHDB_CONTROLLER_H

#include "postmaster/bgworker.h"
#include "runtime/run_registry.h"
#include "utils/jsonb.h"

extern Jsonb *synchdb_run_options_to_json(const SynchdbRunOptions *options);
extern void synchdb_run_options_from_json(Jsonb *json, SynchdbRunOptions *options);
/* Enqueue in the caller's transaction. A positive result is an operation ID. */
extern int64 synchdb_control_enqueue(const char *name, const char *operation,
                                    const SynchdbRunSpec *spec, const char *payload);
extern int32 synchdb_control_receipt(int64 operation_id);
extern void synchdb_control_check_delete(const SynchdbConnectorKey *key);
extern void synchdb_control_register_launcher(void);
/* Explicit START/RESTART only: create the database's empty manual dispatcher.
 * It cannot execute an intent until its registry incarnation is committed. */
extern void synchdb_control_ensure_manual_controller(SynchdbSharedState *registry);
extern PGDLLEXPORT void synchdb_control_supervisor_main(Datum arg);
extern PGDLLEXPORT void synchdb_control_database_main(Datum arg);

/* Core's narrow engine adapter. Called by the controller after command commit. */
extern void synchdb_control_build_spec(const char *name, const char *mode,
                                      Jsonb *options, int snapshot_engine,
                                      bool schema_sync, bool oracle_compatible,
                                      SynchdbRunSpec *spec);
extern void synchdb_control_capture_spec(const char *name, const char *mode,
                                        SynchdbRunSpec *spec);
extern BackgroundWorkerHandle *synchdb_control_start_worker(const SynchdbRunSpec *spec,
                                      Oid execution_role, uint64 controller_generation,
                                      SynchdbRunHandle *run);

#endif
