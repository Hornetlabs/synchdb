/* Object-mapping catalog access; callers own the transaction and snapshot. */
#ifndef SYNCHDB_MAPPING_STORE_H
#define SYNCHDB_MAPPING_STORE_H

#include "synchdb/synchdb.h"
#include "utils/memutils.h"

typedef enum SynchdbMappingKind
{
	SYNCHDB_MAPPING_TABLE,
	SYNCHDB_MAPPING_COLUMN,
	SYNCHDB_MAPPING_DATATYPE,
	SYNCHDB_MAPPING_TRANSFORM
} SynchdbMappingKind;

extern SynchdbMappingKind synchdb_mapping_kind(const char *value);
extern bool synchdb_mapping_store_upsert(const char *connector,
		SynchdbMappingKind kind, const char *source, const char *destination);
extern bool synchdb_mapping_store_disable(const char *connector,
		SynchdbMappingKind kind, const char *source);
/*
 * Includes disabled rules for the legacy loader's hash removal paths.  The
 * result is one allocation in result_context: each srcobj points into the
 * string storage following its ObjectMap array.  One pfree(*out) releases all
 * rows and strings; callers must not free or retain individual srcobj values.
 */
extern void synchdb_mapping_store_list(const char *connector, ObjectMap **out,
		int *count, MemoryContext result_context);
/* Same owned rule rows, without looking up or binding any target relation. */
extern void synchdb_mapping_store_list_raw(const char *connector, ObjectMap **out,
		int *count, MemoryContext result_context);

#endif
