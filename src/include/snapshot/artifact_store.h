/* Persistent engine artifact identity; independent of worker/run generations. */
#ifndef SYNCHDB_ARTIFACT_STORE_H
#define SYNCHDB_ARTIFACT_STORE_H

#include "runtime/connector_identity.h"
#include "synchdb/synchdb.h"

typedef struct SynchdbArtifactPaths
{
	pg_uuid_t generation;
	char base[MAXPGPATH];
	char offset[MAXPGPATH];
	char history[MAXPGPATH];
	char signal[MAXPGPATH];
	char cache[MAXPGPATH];
	char table_filter[MAXPGPATH];
	char snapshot_filter[MAXPGPATH];
	/* A committed prior ENGINE_OWNED row, never inferred from file presence. */
	bool resume_existing;
	/* Only LEGACY_COPY PostgreSQL generations retain old remote identities. */
	char postgres_legacy_slot[NAMEDATALEN];
	char postgres_legacy_publication[NAMEDATALEN];
} SynchdbArtifactPaths;

/* Pure, ASCII path generator. Caller must not substitute an alias or DB name. */
extern void synchdb_artifact_paths(const SynchdbConnectorKey *key,
		const pg_uuid_t *generation, SynchdbArtifactPaths *out);
/* Owns and commits a short transaction; call only from an idle worker before
 * starting an engine. A restart reuses ENGINE_OWNED material, never a seed. */
extern void synchdb_artifact_store_prepare(const ConnectionInfo *connection,
		ConnectorType type, SynchdbArtifactPaths *out);
/* Active-transaction lookup for diagnostics/offset maintenance. */
extern bool synchdb_artifact_store_lookup(const SynchdbConnectorKey *key,
		SynchdbArtifactPaths *out);
/* Idle controller only; explicit requests survive controller/server restart.
 * Each call processes at most one generation and 256 destructive filesystem
 * operations, preserving PURGING if another cycle is needed. Each deletion
 * intent is committed before touching its exact owned paths. */
extern int synchdb_artifact_store_process_purges(void);

#endif
