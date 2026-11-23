/*
 * run_spec.h
 *
 * A complete accepted connector configuration, independent of worker globals.
 */
#ifndef SYNCHDB_RUN_SPEC_H
#define SYNCHDB_RUN_SPEC_H

#include "synchdb/synchdb.h"

typedef enum SynchdbSourceDialect
{
	SYNCHDB_SOURCE_UNDEFINED = 0,
	SYNCHDB_SOURCE_MYSQL,
	SYNCHDB_SOURCE_ORACLE,
	SYNCHDB_SOURCE_SQLSERVER,
	SYNCHDB_SOURCE_POSTGRES
} SynchdbSourceDialect;

typedef enum SynchdbCaptureBackend
{
	SYNCHDB_CAPTURE_UNDEFINED = 0,
	SYNCHDB_CAPTURE_DEBEZIUM,
	SYNCHDB_CAPTURE_OLR
} SynchdbCaptureBackend;

typedef enum SynchdbSnapshotProvider
{
	SYNCHDB_SNAPSHOT_UNDEFINED = 0,
	SYNCHDB_SNAPSHOT_DEBEZIUM,
	SYNCHDB_SNAPSHOT_FDW
} SynchdbSnapshotProvider;

typedef struct SynchdbRunSpec
{
	ConnectorType legacy_type;
	SynchdbSourceDialect source_dialect;
	SynchdbCaptureBackend capture_backend;
	SynchdbSnapshotProvider snapshot_provider;
	ConnectionInfo connection;
	char snapshot_mode[SYNCHDB_SNAPSHOT_MODE_SIZE];
} SynchdbRunSpec;

/*
 * Validate and copy inputs; no GUC, catalog, shared-memory or engine access.
 * ConnectionInfo must contain a valid stable identity, a positive persistent
 * configuration revision and a versioned options snapshot. Transient execution
 * flags are normalized in the copy, leaving the caller untouched.
 */
extern void synchdb_run_spec_init(SynchdbRunSpec *out, ConnectorType type,
								  const ConnectionInfo *connection,
								  const char *snapshot_mode);

/* Reject identity changes, revision regression and changes requiring a new worker.
 * Equal persistent revisions still allow mode or mutable runtime-option changes. */
extern void synchdb_run_spec_validate_restart(const SynchdbRunSpec *current,
											const SynchdbRunSpec *next);

#endif /* SYNCHDB_RUN_SPEC_H */
