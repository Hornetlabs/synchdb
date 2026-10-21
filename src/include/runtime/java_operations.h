/* Operation-local JNI calls; no borrowed local references escape an operation. */
#ifndef SYNCHDB_JAVA_OPERATIONS_H
#define SYNCHDB_JAVA_OPERATIONS_H

#include "runtime/java_runtime.h"

struct _ConnectionInfo;

extern int synchdb_java_start(SynchdbJavaRuntime *java,
							 const struct _ConnectionInfo *connection,
							 int connector_type, const char *snapshot_mode);
extern int synchdb_java_set_offset(SynchdbJavaRuntime *java, int connector_type,
								  const char *database, const char *offset, const char *file);
extern int synchdb_java_create_offset(SynchdbJavaRuntime *java, int connector_type,
									 const char *database, const char *offset, const char *file);
extern int synchdb_java_memory_dump(SynchdbJavaRuntime *java);
extern int synchdb_java_set_loglevel(SynchdbJavaRuntime *java, int level);

#endif /* SYNCHDB_JAVA_OPERATIONS_H */
