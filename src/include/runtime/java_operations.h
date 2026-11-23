/* Operation-local JNI calls; no borrowed local references escape an operation. */
#ifndef SYNCHDB_JAVA_OPERATIONS_H
#define SYNCHDB_JAVA_OPERATIONS_H

#include "runtime/java_runtime.h"
#include "utils/jsonb.h"

struct _ConnectionInfo;

extern int synchdb_java_start(SynchdbJavaRuntime *java,
							 const struct _ConnectionInfo *connection,
							 int connector_type, const char *snapshot_mode);
/* Compatibility handoff only: these calls do not establish snapshot consistency.
 * Both require an idle worker and a bound JVM. Source preparation precedes FDW
 * reads; publication follows the committed complete bootstrap result. */
extern void synchdb_java_prepare_fdw_source(SynchdbJavaRuntime *java,
		const struct _ConnectionInfo *connection, int connector_type);
extern void synchdb_java_prepare_fdw_handoff(SynchdbJavaRuntime *java,
		const struct _ConnectionInfo *connection, int connector_type, Jsonb *material);
extern int synchdb_java_set_offset(SynchdbJavaRuntime *java, int connector_type,
								  const char *database, const char *offset, const char *file);
extern int synchdb_java_create_offset(SynchdbJavaRuntime *java, int connector_type,
									 const char *database, const char *offset, const char *file);
extern int synchdb_java_memory_dump(SynchdbJavaRuntime *java);
extern int synchdb_java_set_loglevel(SynchdbJavaRuntime *java, int level);
extern char *synchdb_java_string_operation(SynchdbJavaRuntime *java,
		const char *method_name, const char *request);

#endif /* SYNCHDB_JAVA_OPERATIONS_H */
