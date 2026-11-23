/* A target-bound row. Values and names belong to its event context. */
#ifndef SYNCHDB_TARGET_CHANGE_H
#define SYNCHDB_TARGET_CHANGE_H

#include "postgres.h"
#include "nodes/pg_list.h"

typedef struct SynchdbTargetColumn
{
	bool is_null;
	char *name;
	Datum value;
	Oid datatype;
	int32 typmod;
	bool typbyval;
	int position;
} SynchdbTargetColumn;

typedef struct SynchdbTargetChange
{
	char op;
	Oid tableoid;
	Oid identity_index_oid;
	int natts;
	List *columnValuesBefore;
	List *columnValuesAfter;
} SynchdbTargetChange;

#endif /* SYNCHDB_TARGET_CHANGE_H */
