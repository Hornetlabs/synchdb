/* Atomic runtime operations. Returned records own their values, never shared pointers. */
#ifndef SYNCHDB_RUN_REGISTRY_H
#define SYNCHDB_RUN_REGISTRY_H

#include "runtime/run_spec.h"

typedef struct _SynchdbSharedState SynchdbSharedState;

/* Accepted settings for one process claim, without pending requests or reports. */
typedef struct SynchdbAcceptedRun
{
	ConnectionInfo connection;
	ConnectorType type;
	char snapshot_mode[SYNCHDB_SNAPSHOT_MODE_SIZE];
} SynchdbAcceptedRun;

/* Control captures keep exact accepted settings for restart validation. */
typedef struct SynchdbRunSnapshot
{
	pid_t pid;
	ConnectorState state;
	ConnectorStage stage;
	ConnectorType type;
	ConnectionInfo conninfo;
	char snapshotMode[SYNCHDB_SNAPSHOT_MODE_SIZE];
	uint64 attempt;
	SynchdbRunLifecycle lifecycle;
} SynchdbRunSnapshot;

typedef struct SynchdbControllerTarget
{
	SynchdbConnectorKey key;
	SynchdbRunHandle run;
	SynchdbRunSnapshot snapshot;
} SynchdbControllerTarget;

/* Reports deliberately exclude passwords, accepted settings and command payloads. */
typedef struct SynchdbRunView
{
	SynchdbConnectorKey key;
	SynchdbRunHandle run;
	pid_t pid;
	uint64 attempt;
	SynchdbRunLifecycle lifecycle;
	ConnectorState state;
	ConnectorStage stage;
	ConnectorType type;
	char name[SYNCHDB_CONNINFO_NAME_SIZE];
	char user[SYNCHDB_CONNINFO_USERNAME_SIZE];
	char errmsg[SYNCHDB_ERRMSG_SIZE];
	char dbzoffset[SYNCHDB_OFFSET_SIZE];
	bool isOraCompat;
	bool retry;
	bool start_committed;
	SynchdbStatistics stats;
	uint64 controller_generation;
	uint64 completed_command_id;
	bool command_succeeded;
	int command_sqlstate;
	int bootstrap_status;
	bool source_ready;
	int failure_sqlstate;
	int exit_code;
} SynchdbRunView;

/* A catalog operation must revalidate this fence after its directory write. */
typedef struct SynchdbCatalogFence
{
	SynchdbConnectorKey key;
	char name[SYNCHDB_CONNINFO_NAME_SIZE];
	uint64 incarnation;
	bool captured;
	SynchdbControllerTarget target;
} SynchdbCatalogFence;

typedef enum SynchdbRunGate
{
	SYNCHDB_GATE_STOP = 0,
	SYNCHDB_GATE_WAIT,
	SYNCHDB_GATE_READY
} SynchdbRunGate;

/* Called during preload, after the postmaster capacity GUC is defined. */
extern void synchdb_registry_register_shmem(int capacity);
extern SynchdbSharedState *synchdb_registry_attach(int capacity);
/* Catalog-only utility guards must work without preload; capacity errors still fail. */
extern SynchdbSharedState *synchdb_registry_try_attach(int capacity);
extern bool synchdb_registry_is_preloaded(void);
/* GUC checks must not allocate or attach shared memory during library load. */
extern bool synchdb_registry_capacity_compatible(int capacity);
extern uint64 synchdb_registry_incarnation(const SynchdbSharedState *state);
extern bool synchdb_registry_controller_present(SynchdbSharedState *state, Oid database_oid);
extern bool synchdb_registry_valid_slot(const SynchdbSharedState *state, int slot);
extern SynchdbRunHandle synchdb_registry_reserve(SynchdbSharedState *state,
                                              const SynchdbRunSpec *spec, bool retry);
extern bool synchdb_registry_claim(SynchdbSharedState *state, const SynchdbRunHandle *handle,
                                  const SynchdbConnectorKey *key, int64 minimum_revision,
                                  SynchdbWorkerOwner *owner, SynchdbAcceptedRun *accepted);
extern bool synchdb_registry_release(SynchdbSharedState *state,
                                    const SynchdbWorkerOwner *owner, int exitcode);
/* Commit/cancel wake the current owner after releasing the internal lock. */
extern bool synchdb_registry_commit_start(SynchdbSharedState *state, const SynchdbRunHandle *run);
extern bool synchdb_registry_cancel(SynchdbSharedState *state, const SynchdbRunHandle *run);
extern bool synchdb_registry_find_run(SynchdbSharedState *state, const SynchdbConnectorKey *key,
                                     SynchdbRunHandle *run);
extern bool synchdb_registry_run_active(SynchdbSharedState *state, const SynchdbRunHandle *run);
/* Unlike run_active, this also accepts the same generation's FREE display row. */
extern bool synchdb_registry_run_unchanged(SynchdbSharedState *state, const SynchdbRunHandle *run,
                                          const SynchdbConnectorKey *key);
extern bool synchdb_registry_owned(SynchdbSharedState *state, const SynchdbWorkerOwner *owner);
extern SynchdbRunGate synchdb_registry_gate(SynchdbSharedState *state, const SynchdbWorkerOwner *owner);
extern void synchdb_registry_cleanup_failed(SynchdbSharedState *state,
                                           const SynchdbWorkerOwner *owner, const char *message);

/* Optional owner filters worker reads; NULL permits a controller slot snapshot. */
extern bool synchdb_registry_read_slot(SynchdbSharedState *state, int slot,
                                      const SynchdbWorkerOwner *owner, SynchdbRunView *view);
extern bool synchdb_registry_next_view(SynchdbSharedState *state, Oid database_oid,
                                      int *cursor, SynchdbRunView *view);
extern bool synchdb_registry_any_active(SynchdbSharedState *state, Oid database_oid,
                                       char *name, Size name_size);
extern bool synchdb_registry_report_state(SynchdbSharedState *state,
                                         const SynchdbWorkerOwner *owner, ConnectorState value);
extern bool synchdb_registry_report_stage(SynchdbSharedState *state,
                                         const SynchdbWorkerOwner *owner, ConnectorStage value);
extern bool synchdb_registry_report_error(SynchdbSharedState *state,
                                         const SynchdbWorkerOwner *owner, const char *value);
extern bool synchdb_registry_report_offset(SynchdbSharedState *state,
                                          const SynchdbWorkerOwner *owner, const char *value);
extern bool synchdb_registry_report_stats(SynchdbSharedState *state,
                                         const SynchdbWorkerOwner *owner, const SynchdbStatistics *delta);
/* Source acknowledgement changes its counter without replacing target timing. */
extern bool synchdb_registry_report_batch_completion(SynchdbSharedState *state,
													const SynchdbWorkerOwner *owner);
extern bool synchdb_registry_poll_request(SynchdbSharedState *state,
                                         const SynchdbWorkerOwner *owner, SynchdbRequest *request,
                                         ConnectorState *current_state);
extern bool synchdb_registry_ack_request(SynchdbSharedState *state,
                                        const SynchdbWorkerOwner *owner, uint64 command_id);
extern bool synchdb_registry_complete_request(SynchdbSharedState *state,
                                        const SynchdbWorkerOwner *owner, uint64 command_id,
                                        bool succeeded, int sqlstate, const char *message);
extern void synchdb_registry_report_failure(SynchdbSharedState *state,
                                        const SynchdbWorkerOwner *owner, int sqlstate,
                                        const char *message);
extern void synchdb_registry_report_bootstrap(SynchdbSharedState *state,
                                        const SynchdbWorkerOwner *owner, int status);
extern void synchdb_registry_report_ready(SynchdbSharedState *state,
                                        const SynchdbWorkerOwner *owner, bool ready);
extern uint64 synchdb_registry_controller_claim(SynchdbSharedState *state, Oid database_oid);
extern void synchdb_registry_controller_release(SynchdbSharedState *state, Oid database_oid,
                                               uint64 generation);
extern bool synchdb_registry_controller_valid(SynchdbSharedState *state, Oid database_oid,
                                             uint64 generation);
extern bool synchdb_registry_assign_controller(SynchdbSharedState *state,
                                              const SynchdbRunHandle *run, uint64 generation);
/* proven_stopped requires BGWH_STOPPED from the owner's real opaque handle. */
extern bool synchdb_registry_reap(SynchdbSharedState *state, const SynchdbRunHandle *run,
                                 bool proven_stopped);
extern bool synchdb_registry_publish_run(SynchdbSharedState *state,
                                        const SynchdbWorkerOwner *owner, const SynchdbRunSpec *spec,
                                        const SynchdbRequest *consumed_request);
extern bool synchdb_registry_publish_loglevel(SynchdbSharedState *state,
                                             const SynchdbWorkerOwner *owner, int level);

/* Capture returns false when the key has no suitable slot; no catalog access. */
extern bool synchdb_registry_capture(SynchdbSharedState *state, const SynchdbConnectorKey *key,
                                    bool running, SynchdbControllerTarget *target);
extern void synchdb_registry_submit(SynchdbSharedState *state, const SynchdbControllerTarget *target,
                                    const SynchdbWorkerOwner *caller, SynchdbRequest *request);
extern void synchdb_registry_reset_stats(SynchdbSharedState *state, const SynchdbControllerTarget *target,
                                         const SynchdbWorkerOwner *caller);
extern void synchdb_registry_add_snapstats(SynchdbSharedState *state, const SynchdbControllerTarget *target,
                                          const SynchdbWorkerOwner *caller, int64 tables, int64 rows,
                                          int64 begin, int64 end);
extern void synchdb_registry_catalog_prepare(SynchdbSharedState *state, const SynchdbConnectorKey *key,
                                            const char *name, SynchdbCatalogFence *fence);
/* false means the catalog writer must roll back; forget clears the display row. */
extern bool synchdb_registry_catalog_finish(SynchdbSharedState *state, const SynchdbCatalogFence *fence,
                                           const SynchdbWorkerOwner *caller, bool forget);

#endif /* SYNCHDB_RUN_REGISTRY_H */
