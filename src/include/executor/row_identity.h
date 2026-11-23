/* Live indexed identity shared by binding, conversion and both Writers. */
#ifndef SYNCHDB_ROW_IDENTITY_H
#define SYNCHDB_ROW_IDENTITY_H

#include "postgres.h"
#include "access/attnum.h"
#include "nodes/bitmapset.h"
#include "utils/relcache.h"

typedef struct SynchdbIdentityKey
{
	AttrNumber position;
	Oid input_type;
	Oid equality_operator;
	Oid collation;
} SynchdbIdentityKey;

typedef struct SynchdbRowIdentity
{
	Oid index_oid;
	bool primary_equivalent;
	/* Attribute numbers offset by FirstLowInvalidHeapAttributeNumber. */
	Bitmapset *attributes;
	int nkeys;
	/* Index order, including repeated attributes; INCLUDE columns are absent. */
	SynchdbIdentityKey *keys;
} SynchdbRowIdentity;

/* Caller holds RowExclusiveLock on the table and owns the active transaction.
 * Result belongs to CurrentMemoryContext; the index lock lasts to transaction end.
 * Until source old-field provenance is available, identity uses the PK columns. */
extern SynchdbRowIdentity *synchdb_resolve_row_identity(Relation relation);

#endif /* SYNCHDB_ROW_IDENTITY_H */
