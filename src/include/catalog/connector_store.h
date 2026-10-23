/*
 * connector_store.h
 *
 * Typed access to the extension's connector configuration catalog.
 */
#ifndef SYNCHDB_CONNECTOR_STORE_H
#define SYNCHDB_CONNECTOR_STORE_H

#include "synchdb/synchdb.h"
#include "utils/memutils.h"

/*
 * Callers own the transaction and active snapshot.  These functions only own
 * their SPI connection; they never start, commit, or abort a transaction.
 *
 * Missing rows are ordinary results.  SQL failures and invalid stored values
 * raise ERROR.  Output strings/arrays are copied into result_context before
 * SPI_finish; no returned pointer belongs to SPI.  The fixed-layout
 * ConnectionInfo remains a compatibility representation, not a RunSpec.
 */
extern bool synchdb_connector_store_get(const char *name,
										ConnectionInfo *out, char **connector,
										MemoryContext result_context);
extern void synchdb_connector_store_list_active(char ***out, int *numout,
											MemoryContext result_context);
/* Returns whether one connector row was updated under caller privileges. */
extern bool synchdb_connector_store_set_active(const char *name, bool active);

/* Identity reads do not decode credentials or other configuration fields. */
extern bool synchdb_connector_store_get_identity(const char *name,
												 SynchdbConnectorKey *key, int64 *revision);
/* expected_revision = 0 matches this identity at any revision; > 0 adds a CAS. */
extern bool synchdb_connector_store_set_active_identity(const SynchdbConnectorKey *key,
													  int64 expected_revision, bool active);

typedef enum SynchdbConnectorOptionGroup
{
	SYNCHDB_CONNECTOR_OPTIONS_EXTRA,
	SYNCHDB_CONNECTOR_OPTIONS_FDW,
	SYNCHDB_CONNECTOR_OPTIONS_JMX,
	SYNCHDB_CONNECTOR_OPTIONS_JMX_EXPORTER,
	SYNCHDB_CONNECTOR_OPTIONS_OLR,
	SYNCHDB_CONNECTOR_OPTIONS_ISPN
} SynchdbConnectorOptionGroup;

/*
 * Writers bind all data values and preserve the legacy JSON representation.
 * Optional fixed-array text containing "null" is stored as JSON null where
 * the corresponding SQL API historically did so.  Creation starts inactive.
 * Duplicate creation raises ERROR; updates/deletion return whether one
 * connector row was affected.  Only an affected parent permits the matching
 * attribute/object-map cleanup under caller privileges.  The Store never
 * stops a worker or removes files.
 */
extern void synchdb_connector_store_create(const ConnectionInfo *config,
										  const char *connector);
extern bool synchdb_connector_store_delete(const char *name);
extern bool synchdb_connector_store_delete_identity(const char *name,
													 const SynchdbConnectorKey *key);
extern bool synchdb_connector_store_set_extra(const char *name,
											 const ExtraConnectionInfo *options);
extern bool synchdb_connector_store_set_fdw(const char *name,
										   const FdwConnectionInfo *options);
extern bool synchdb_connector_store_set_jmx(const char *name,
										   const JMXConnectionInfo *options);
/* Uses only the exporter fields; set_jmx leaves those fields untouched. */
extern bool synchdb_connector_store_set_jmx_exporter(const char *name,
													const JMXConnectionInfo *options);
extern bool synchdb_connector_store_set_olr(const char *name,
										   const OLRConnectionInfo *options);
extern bool synchdb_connector_store_set_ispn(const char *name,
											const IspnInfo *options);
/* JMX and JMX_EXPORTER clear only their own independent option fields. */
extern bool synchdb_connector_store_clear_options(const char *name,
													SynchdbConnectorOptionGroup group);

#endif /* SYNCHDB_CONNECTOR_STORE_H */
