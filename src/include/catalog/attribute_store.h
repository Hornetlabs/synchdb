/* Typed persistence of source-column to target-attribute bindings. */
#ifndef SYNCHDB_ATTRIBUTE_STORE_H
#define SYNCHDB_ATTRIBUTE_STORE_H

#include "access/attnum.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"

typedef struct SynchdbAttributeBinding
{
	Oid target_relation;
	AttrNumber target_attnum;
	const char *source_table;
	const char *source_column;
	const char *source_type;
} SynchdbAttributeBinding;

/*
 * Caller owns an active transaction/snapshot and resolves/locks target
 * attributes. No source DDL, case strategy, transaction or error policy is
 * interpreted here. Errors propagate; source identifiers remain case-sensitive.
 * Connector type tokens are normalized to ASCII lower case for legacy storage.
 * Connector name/type remain NAME tokens. Source text is stored without
 * truncation and uses the shared source-ID byte limit. A source table and column
 * must also fit that limit when joined by a dot for column-specific mappings.
 */
extern void synchdb_attribute_store_upsert(const char *name, const char *type,
										  const SynchdbAttributeBinding *bindings,
										  int count);
/* Full source identifiers and physical positions; no target name rebinding. */
extern List *synchdb_attribute_store_list_source_table(const char *name,
		const char *type, const char *source_table, MemoryContext result_context);
extern List *synchdb_attribute_store_list_target_relation(const char *name,
		const char *type, Oid relation, MemoryContext result_context);
extern uint64 synchdb_attribute_store_delete_source_table(const char *name,
														 const char *type,
														 const char *source_table);
extern uint64 synchdb_attribute_store_mark_dropped(const char *name,
												 const char *type,
												 const char *source_table,
												 Oid target_relation,
												 AttrNumber target_attnum);
/* Only this connector/type's mappings to missing pg_class rows are removed. */
extern uint64 synchdb_attribute_store_prune_missing(const char *name,
												  const char *type);

#endif /* SYNCHDB_ATTRIBUTE_STORE_H */
