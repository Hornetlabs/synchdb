/*
 * replication_agent.c
 *
 * Implementation of replication agent functionality for SynchDB
 *
 * This file contains functions for executing DDL and DML operations
 * as part of the database replication process. It provides both
 * SPI-based execution and Heap Tuple execution for insert, update, and
 * delete operations.
 * 
 * Copyright (c) Hornetlabs Technology, Inc.
 *
 */

#include "postgres.h"
#include "fmgr.h"
#include "executor/replication_agent.h"
#include "executor/spi.h"
#include "converter/format_converter.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "access/table.h"
#include "executor/tuptable.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "utils/snapmgr.h"
#include "parser/parse_relation.h"
#include "replication/logicalrelation.h"
#include "synchdb/synchdb.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "storage/ipc.h"
#include "utils/datum.h"

/* external global variables */
extern bool synchdb_dml_use_spi;
extern uint64 SPI_processed;
extern int myConnectorId;
extern int synchdb_error_strategy;
extern bool synchdb_log_event_on_error;
extern char * g_eventStr;

/*
 * swap_tokens
 *
 * helper function to swap specific token strings with the given data
 */
static char *
swap_tokens(const char * expression, const char * data, const char * wkb, const char * srid)
{
	char		filledexpression[SYNCHDB_TRANSFORM_EXPRESSION_SIZE];
	char	   *dp;
	char	   *endp;
	const char *sp;

	/*
	 * construct the expression to run
	 */
	dp = filledexpression;
	endp = filledexpression + SYNCHDB_TRANSFORM_EXPRESSION_SIZE - 1;
	*endp = '\0';

	for (sp = expression; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'd':
					/* %d: data */
					sp++;
					strlcpy(dp, data == NULL ? "null" : data, endp - dp);
					dp += strlen(dp);
					break;
				case 'w':
					/* %w: well-known-binary for geometry, aka wkb */
					sp++;
					strlcpy(dp, wkb == NULL ? "null" : wkb, endp - dp);
					dp += strlen(dp);
					break;
				case 's':
					/* %s: srid for geometry */
					sp++;
					strlcpy(dp, srid == NULL ? "null" : srid, endp - dp);
					dp += strlen(dp);
					break;
				case '%':
					/* convert %% to a single % */
					sp++;
					if (dp < endp)
						*dp++ = *sp;
					break;
				default:
					/* otherwise treat the % as not special */
					if (dp < endp)
						*dp++ = *sp;
					break;
			}
		}
		else
		{
			if (dp < endp)
				*dp++ = *sp;
		}
	}
	*dp = '\0';

	return pstrdup(filledexpression);
}
/*
 * spi_execute_select_one
 *
 * This function performs SPI_execute SELECT and returns an array of
 * Datums that represent each column, Caller is expected to know exactly
 * how to process this array of Datums
 */
static Datum *
spi_execute_select_one(const char * query, int * numcols, MemoryContext ctx)
{
	int ret = -1, i = 0;
	int numrows = -1;
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum colval;
	Datum * rowval;
	bool isnull;
	bool skiptx = false;
	MemoryContext oldcontext;

	/*
	 * if we are already in transaction or transaction block, we can skip
	 * the transaction and snapshot acquisition code below
	 */
	if (IsTransactionOrTransactionBlock())
		skiptx = true;

	if (!skiptx)
	{
		/* Start a transaction and set up a snapshot */
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(WARNING, "synchdb_pgsql - SPI_connect failed");
		if (!skiptx)
		{
			/* Start a transaction and set up a snapshot */
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
		}
		return NULL;
	}

	/* we only want to select 1 row */
	ret = SPI_execute(query, true, 1);
	switch (ret)
	{
		case SPI_OK_SELECT:
		{
			break;
		}
		default:
		{
			SPI_finish();
			if (!skiptx)
			{
				/* Start a transaction and set up a snapshot */
				StartTransactionCommand();
				PushActiveSnapshot(GetTransactionSnapshot());
			}
			return NULL;
		}
	}
	numrows = SPI_processed;
	if (numrows == 0)
	{
		SPI_finish();
		if (!skiptx)
		{
			/* Start a transaction and set up a snapshot */
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
		}
		return NULL;
	}

	/* only one row expected */
	tuple = SPI_tuptable->vals[0];
	tupdesc = SPI_tuptable->tupdesc;
	*numcols = tupdesc->natts;

	oldcontext = MemoryContextSwitchTo(ctx);
	rowval = (Datum *) palloc0(*numcols * sizeof(Datum));

	for (i = 0; i < *numcols; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		colval = SPI_getbinval(tuple, tupdesc, i + 1, &isnull);
		if (isnull)
			rowval[i] = (Datum) 0;
		else
		{
			if (!attr->attbyval)
			{
				// Ensure detoasted, then deep copy
				Datum detoasted = PointerGetDatum(PG_DETOAST_DATUM_COPY(colval));
				rowval[i] = datumCopy(detoasted, false, attr->attlen);
			}
			else
			{
				rowval[i] = datumCopy(colval, true, attr->attlen);
			}
		}
	}
	MemoryContextSwitchTo(oldcontext);

	/* Close the connection */
	SPI_finish();

	if (!skiptx)
	{
		/* Commit the transaction */
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	return rowval;
}

/*
 * spi_execute - Execute a query using the Server Programming Interface (SPI)
 *
 * This function sets up a transaction, executes the given query using SPI,
 * and handles any errors that occur during execution.
 */
static int
spi_execute(const char * query, ConnectorType type)
{
	int ret = -1;
	bool skiptx = false;
	/*
	 * if we are already in transaction or transaction block, we can skip
	 * the transaction and snapshot acquisition code below
	 */
	if (IsTransactionOrTransactionBlock())
		skiptx = true;

	PG_TRY();
	{
		if (!skiptx)
		{
			/* Start a transaction and set up a snapshot */
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
		}
		if (SPI_connect() != SPI_OK_CONNECT)
		{
			elog(ERROR, "synchdb_pgsql - SPI_connect failed");
		}

		ret = SPI_exec(query, 0);
		switch (ret)
		{
			case SPI_OK_INSERT:
			case SPI_OK_UTILITY:
			case SPI_OK_DELETE:
			case SPI_OK_UPDATE:
			{
				break;
			}
			default:
			{
				elog(ERROR, "SPI_exec failed: %d", ret);
			}
		}

		ret = 0;
		if (SPI_finish() != SPI_OK_FINISH)
		{
			elog(ERROR, "SPI_finish failed");
		}

		if (!skiptx)
		{
			/* Commit the transaction */
			PopActiveSnapshot();
			CommitTransactionCommand();
		}
	}
	PG_CATCH();
	{
		MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
		ErrorData  *errdata = CopyErrorData();

		if (errdata)
			set_shm_connector_errmsg(myConnectorId, errdata->message);

		/* dump the JSON change event as additional detail if available */
		if (synchdb_log_event_on_error && g_eventStr != NULL)
			elog(LOG, "%s", g_eventStr);

		FreeErrorData(errdata);
		MemoryContextSwitchTo(oldctx);
		SPI_finish();
		ret = -1;
		PG_RE_THROW();
	}
	PG_END_TRY();

	return ret;
}

/*
 * synchdb_handle_insert - Custom handler for INSERT operations
 *
 * This function performs an INSERT operation without using SPI.
 * It creates a tuple from the provided column values and inserts it into the table.
 */
static int
synchdb_handle_insert(List * colval, Oid tableoid, ConnectorType type, int natts)
{
	Relation rel = NULL;
	TupleTableSlot *slot;
	EState	   *estate = NULL;
	RangeTblEntry *rte;
	List	   *perminfos = NIL;
	ResultRelInfo *resultRelInfo = NULL;
	ListCell * cell;
	int i = 0;

	/*
	 * we put in TRY and CATCH block to capture potential exceptions raised
	 * from PostgreSQL, which would cause this worker to exit. The last error
	 * messages related with the exception will be stored in synchdb's shared
	 * memory state so user will have an idea what is wrong.
	 */
	PG_TRY();
	{
		rel = table_open(tableoid, AccessShareLock);

		/* initialize estate */
		estate = CreateExecutorState();

		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RELATION;
		rte->relid = RelationGetRelid(rel);
		rte->relkind = rel->rd_rel->relkind;
		rte->rellockmode = AccessShareLock;

		addRTEPermissionInfo(&perminfos, rte);

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
		ExecInitRangeTable(estate, list_make1(rte), perminfos,
				bms_make_singleton(1));
#else
		ExecInitRangeTable(estate, list_make1(rte), perminfos);
#endif
		estate->es_output_cid = GetCurrentCommandId(true);

		/* initialize resultRelInfo */
		resultRelInfo = makeNode(ResultRelInfo);
		InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

		/* turn colval into TupleTableSlot */
		slot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel), &TTSOpsVirtual);

		ExecClearTuple(slot);

		/* initialize all values in slot to null */
		for (i = 0; i < natts; i++)
			slot->tts_isnull[i] = true;

		/* then we fill valid data to slot */
		foreach(cell, colval)
		{
			PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
			Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, colval->position - 1);
			Oid			typinput;
			Oid			typioparam;

			if (!strcasecmp(colval->value, "NULL"))
				slot->tts_isnull[colval->position - 1] = true;
			else
			{
				getTypeInputInfo(colval->datatype, &typinput, &typioparam);
				slot->tts_values[colval->position - 1] =
					OidInputFunctionCall(typinput, colval->value,
										 typioparam, attr->atttypmod);
				slot->tts_isnull[colval->position - 1] = false;
			}
		}
		ExecStoreVirtualTuple(slot);

		/* We must open indexes here. */
		ExecOpenIndices(resultRelInfo, false);

		/* Do the insert. */
		ExecSimpleRelationInsert(resultRelInfo, estate, slot);

		/* increment command ID */
		CommandCounterIncrement();

		/* Cleanup. */
		ExecCloseIndices(resultRelInfo);

		table_close(rel, AccessShareLock);

		ExecResetTupleTable(estate->es_tupleTable, false);
		FreeExecutorState(estate);
	}
	PG_CATCH();
	{
		MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
		ErrorData  *errdata = CopyErrorData();
		if (errdata)
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "%s.%s: %s | %s",
					errdata->schema_name == NULL ? "" : errdata->schema_name,
					errdata->table_name == NULL ? "" : errdata->table_name,
					errdata->message,
					errdata->detail == NULL ? "" : errdata->detail);
			set_shm_connector_errmsg(myConnectorId, msg);
			pfree(msg);
		}
		FreeErrorData(errdata);
		MemoryContextSwitchTo(oldctx);

		/* dump the JSON change event as additional detail if available */
		if (synchdb_log_event_on_error && g_eventStr != NULL)
			elog(LOG, "%s", g_eventStr);

		if (synchdb_error_strategy == STRAT_SKIP_ON_ERROR)
		{
			ExecCloseIndices(resultRelInfo);
			table_close(rel, AccessShareLock);
			ExecResetTupleTable(estate->es_tupleTable, false);
			FreeExecutorState(estate);
			FlushErrorState();
			return -1;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
	return 0;
}

/*
 * synchdb_handle_update - Custom handler for UPDATE operations
 *
 * This function performs an UPDATE operation without using SPI.
 * It locates the existing tuple, creates a new tuple with updated values,
 * and replaces the old tuple with the new one.
 */
static int
synchdb_handle_update(List * colvalbefore, List * colvalafter, Oid tableoid, ConnectorType type, int natts)
{
	Relation rel = NULL;
	TupleTableSlot * remoteslot, * localslot;
	EState	   *estate = NULL;
	RangeTblEntry *rte;
	List	   *perminfos = NIL;
	ResultRelInfo *resultRelInfo = NULL;
	ListCell * cell;
	int ret = 0, i = 0;
	EPQState	epqstate;
	bool found;
	Oid idxoid = InvalidOid;

	/*
	 * we put in TRY and CATCH block to capture potential exceptions raised
	 * from PostgreSQL, which would cause this worker to exit. The last error
	 * messages related with the exception will be stored in synchdb's shared
	 * memory state so user will have an idea what is wrong.
	 */
	PG_TRY();
	{
		rel = table_open(tableoid, AccessShareLock);

		/* initialize estate */
		estate = CreateExecutorState();

		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RELATION;
		rte->relid = RelationGetRelid(rel);
		rte->relkind = rel->rd_rel->relkind;
		rte->rellockmode = AccessShareLock;

		addRTEPermissionInfo(&perminfos, rte);

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
		ExecInitRangeTable(estate, list_make1(rte), perminfos,
				bms_make_singleton(1));
#else
		ExecInitRangeTable(estate, list_make1(rte), perminfos);
#endif
		estate->es_output_cid = GetCurrentCommandId(true);

		/* initialize resultRelInfo */
		resultRelInfo = makeNode(ResultRelInfo);
		InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

		/* turn colvalbefore into TupleTableSlot */
		remoteslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel), &TTSOpsVirtual);
		localslot = table_slot_create(rel, &estate->es_tupleTable);

		ExecClearTuple(remoteslot);

		/* initialize all values in slot to null */
		for (i = 0; i < natts; i++)
			remoteslot->tts_isnull[i] = true;

		/* then we fill valid data to slot */
		foreach(cell, colvalbefore)
		{
			PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
			Form_pg_attribute attr = TupleDescAttr(remoteslot->tts_tupleDescriptor, colval->position - 1);
			Oid			typinput;
			Oid			typioparam;

			if (!strcasecmp(colval->value, "NULL"))
				remoteslot->tts_isnull[colval->position - 1] = true;
			else
			{
				getTypeInputInfo(colval->datatype, &typinput, &typioparam);
				remoteslot->tts_values[colval->position - 1] =
					OidInputFunctionCall(typinput, colval->value,
										 typioparam, attr->atttypmod);
				remoteslot->tts_isnull[colval->position - 1] = false;
			}
		}
		ExecStoreVirtualTuple(remoteslot);
		EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1, NIL);

		/* We must open indexes here. */
		ExecOpenIndices(resultRelInfo, false);
		idxoid = GetRelationIdentityOrPK(rel);
		if (OidIsValid(idxoid))
		{
			elog(DEBUG1, "attempt to find old tuple by index");
			found = RelationFindReplTupleByIndex(rel, idxoid,
												 LockTupleExclusive,
												 remoteslot, localslot);
		}
		else
		{
			elog(DEBUG1, "attempt to find old tuple by seq scan");
			found = RelationFindReplTupleSeq(rel, LockTupleExclusive,
											 remoteslot, localslot);
		}

		/*
		 * localslot should now contain the reference to the old tuple that is yet
		 * to be updated
		 */
		if (found)
		{
			/* turn colvalafter into TupleTableSlot */
			ExecClearTuple(remoteslot);

			/* initialize all values in slot to null */
			for (i = 0; i < natts; i++)
				remoteslot->tts_isnull[i] = true;

			/* then we fill valid data to slot */
			foreach(cell, colvalafter)
			{
				PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
				Form_pg_attribute attr = TupleDescAttr(remoteslot->tts_tupleDescriptor, colval->position - 1);
				Oid			typinput;
				Oid			typioparam;

				if (!strcasecmp(colval->value, "NULL"))
					remoteslot->tts_isnull[colval->position - 1] = true;
				else
				{
					getTypeInputInfo(colval->datatype, &typinput, &typioparam);
					remoteslot->tts_values[colval->position - 1] =
						OidInputFunctionCall(typinput, colval->value,
											 typioparam, attr->atttypmod);
					remoteslot->tts_isnull[colval->position - 1] = false;
				}
			}
			ExecStoreVirtualTuple(remoteslot);
			EvalPlanQualSetSlot(&epqstate, remoteslot);
			ExecSimpleRelationUpdate(resultRelInfo, estate, &epqstate, localslot,
									 remoteslot);
		}
		else
		{
			elog(WARNING, "tuple to update not found");
		}

		/* increment command ID */
		CommandCounterIncrement();

		/* Cleanup. */
		ExecCloseIndices(resultRelInfo);
		EvalPlanQualEnd(&epqstate);
		ExecResetTupleTable(estate->es_tupleTable, false);
		FreeExecutorState(estate);
		table_close(rel, AccessShareLock);
	}
	PG_CATCH();
	{
		MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
		ErrorData  *errdata = CopyErrorData();
		if (errdata)
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "%s.%s: %s | %s",
					errdata->schema_name == NULL ? "" : errdata->schema_name,
					errdata->table_name == NULL ? "" : errdata->table_name,
					errdata->message,
					errdata->detail == NULL ? "" : errdata->detail);
			set_shm_connector_errmsg(myConnectorId, msg);
			pfree(msg);
		}
		FreeErrorData(errdata);
		MemoryContextSwitchTo(oldctx);

		/* dump the JSON change event as additional detail if available */
		if (synchdb_log_event_on_error && g_eventStr != NULL)
			elog(LOG, "%s", g_eventStr);

		if (synchdb_error_strategy == STRAT_SKIP_ON_ERROR)
		{
			ExecCloseIndices(resultRelInfo);
			EvalPlanQualEnd(&epqstate);
			ExecResetTupleTable(estate->es_tupleTable, false);
			FreeExecutorState(estate);
			table_close(rel, AccessShareLock);
			FlushErrorState();
			return -1;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ret;
}

/*
 * synchdb_handle_delete - Custom handler for DELETE operations
 *
 * This function performs a DELETE operation without using SPI.
 * It locates the existing tuple based on the provided column values and deletes it.
 */
static int
synchdb_handle_delete(List * colvalbefore, Oid tableoid, ConnectorType type, int natts)
{
	Relation rel = NULL;
	TupleTableSlot * remoteslot, * localslot;
	EState	   *estate = NULL;
	RangeTblEntry *rte;
	List	   *perminfos = NIL;
	ResultRelInfo *resultRelInfo = NULL;
	ListCell * cell;
	int ret = 0, i = 0;
	EPQState	epqstate;
	bool found;
	Oid idxoid = InvalidOid;

	/*
	 * we put in TRY and CATCH block to capture potential exceptions raised
	 * from PostgreSQL, which would cause this worker to exit. The last error
	 * messages related with the exception will be stored in synchdb's shared
	 * memory state so user will have an idea what is wrong.
	 */
	PG_TRY();
	{
		rel = table_open(tableoid, AccessShareLock);

		/* initialize estate */
		estate = CreateExecutorState();

		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RELATION;
		rte->relid = RelationGetRelid(rel);
		rte->relkind = rel->rd_rel->relkind;
		rte->rellockmode = AccessShareLock;

		addRTEPermissionInfo(&perminfos, rte);

#if SYNCHDB_PG_MAJOR_VERSION >= 1800
		ExecInitRangeTable(estate, list_make1(rte), perminfos,
				bms_make_singleton(1));
#else
		ExecInitRangeTable(estate, list_make1(rte), perminfos);
#endif
		estate->es_output_cid = GetCurrentCommandId(true);

		/* initialize resultRelInfo */
		resultRelInfo = makeNode(ResultRelInfo);
		InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

		/* turn colvalbefore into TupleTableSlot */
		remoteslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel), &TTSOpsVirtual);
		localslot = table_slot_create(rel, &estate->es_tupleTable);

		ExecClearTuple(remoteslot);

		/* initialize all values in slot to null */
		for (i = 0; i < natts; i++)
			remoteslot->tts_isnull[i] = true;

		/* then we fill valid data to slot */
		foreach(cell, colvalbefore)
		{
			PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
			Form_pg_attribute attr = TupleDescAttr(remoteslot->tts_tupleDescriptor, colval->position - 1);
			Oid			typinput;
			Oid			typioparam;

			if (!strcasecmp(colval->value, "NULL"))
				remoteslot->tts_isnull[colval->position - 1] = true;
			else
			{
				getTypeInputInfo(colval->datatype, &typinput, &typioparam);
				remoteslot->tts_values[colval->position - 1] =
					OidInputFunctionCall(typinput, colval->value,
										 typioparam, attr->atttypmod);
				remoteslot->tts_isnull[colval->position - 1] = false;
			}
		}
		ExecStoreVirtualTuple(remoteslot);
		EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1, NIL);

		/* We must open indexes here. */
		ExecOpenIndices(resultRelInfo, false);
		idxoid = GetRelationIdentityOrPK(rel);
		if (OidIsValid(idxoid))
		{
			elog(DEBUG1, "attempt to find old tuple by index");
			found = RelationFindReplTupleByIndex(rel, idxoid,
												 LockTupleExclusive,
												 remoteslot, localslot);
		}
		else
		{
			elog(DEBUG1, "attempt to find old tuple by seq scan");
			found = RelationFindReplTupleSeq(rel, LockTupleExclusive,
											 remoteslot, localslot);
		}

		/*
		 * localslot should now contain the reference to the old tuple that is yet
		 * to be updated
		 */
		if (found)
		{
			EvalPlanQualSetSlot(&epqstate, localslot);
			ExecSimpleRelationDelete(resultRelInfo, estate, &epqstate, localslot);
		}
		else
		{
			elog(WARNING, "tuple to delete not found");
		}

		/* increment command ID */
		CommandCounterIncrement();

		/* Cleanup. */
		ExecCloseIndices(resultRelInfo);
		EvalPlanQualEnd(&epqstate);
		ExecResetTupleTable(estate->es_tupleTable, false);
		FreeExecutorState(estate);
		table_close(rel, AccessShareLock);
	}
	PG_CATCH();
	{
		MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
		ErrorData  *errdata = CopyErrorData();
		if (errdata)
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "%s.%s: %s | %s",
					errdata->schema_name == NULL ? "" : errdata->schema_name,
					errdata->table_name == NULL ? "" : errdata->table_name,
					errdata->message,
					errdata->detail == NULL ? "" : errdata->detail);
			set_shm_connector_errmsg(myConnectorId, msg);
			pfree(msg);
		}
		FreeErrorData(errdata);
		MemoryContextSwitchTo(oldctx);

		/* dump the JSON change event as additional detail if available */
		if (synchdb_log_event_on_error && g_eventStr != NULL)
			elog(LOG, "%s", g_eventStr);

		if (synchdb_error_strategy == STRAT_SKIP_ON_ERROR)
		{
			ExecCloseIndices(resultRelInfo);
			EvalPlanQualEnd(&epqstate);
			ExecResetTupleTable(estate->es_tupleTable, false);
			FreeExecutorState(estate);
			table_close(rel, AccessShareLock);
			FlushErrorState();
			return -1;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ret;
}

/*
 * ra_executePGDDL - Execute a PostgreSQL DDL operation
 *
 * This function is the entry point for executing DDL operations.
 * It uses SPI to execute the DDL query.
 */
int
ra_executePGDDL(PG_DDL * pgddl, ConnectorType type)
{
	if (!pgddl || !pgddl->ddlquery)
    {
        elog(WARNING, "Invalid DDL query");
        return -1;
    }
	return spi_execute(pgddl->ddlquery, type);
}

/*
 * ra_executePGDML - Execute a PostgreSQL DML operation
 *
 * This function is the entry point for executing DML operations.
 * Depending on the operation type and configuration, it either uses SPI
 * or calls a custom handler function.
 */
int
ra_executePGDML(PG_DML * pgdml, ConnectorType type, SynchdbStatistics * myBatchStats,
		bool isInSnapshot)
{
	int ret = -1;
	if (!pgdml)
    {
        elog(WARNING, "Invalid DML operation");
        return -1;
    }

	switch (pgdml->op)
	{
		case 'r':  // Read operation
		{
			if (synchdb_dml_use_spi)
				ret = spi_execute(pgdml->dmlquery, type);
			else
				ret = synchdb_handle_insert(pgdml->columnValuesAfter, pgdml->tableoid, type, pgdml->natts);

			increment_connector_statistics(myBatchStats, STATS_ROWS, 1);
			break;
		}
		case 'c':  // Create operation
		{
			if (synchdb_dml_use_spi)
				ret = spi_execute(pgdml->dmlquery, type);
			else
				ret = synchdb_handle_insert(pgdml->columnValuesAfter, pgdml->tableoid, type, pgdml->natts);

			if (!isInSnapshot)
				increment_connector_statistics(myBatchStats, STATS_CREATE, 1);
			break;
		}
		case 'u':  // Update operation
		{
			if (synchdb_dml_use_spi)
				ret = spi_execute(pgdml->dmlquery, type);
			else
				ret = synchdb_handle_update(pgdml->columnValuesBefore,
											 pgdml->columnValuesAfter,
											 pgdml->tableoid,
											 type, pgdml->natts);
			if (!isInSnapshot)
				increment_connector_statistics(myBatchStats, STATS_UPDATE, 1);
			break;
		}
		case 'd':  // Delete operation
		{
			if (synchdb_dml_use_spi)
				ret = spi_execute(pgdml->dmlquery, type);
			else
				ret = synchdb_handle_delete(pgdml->columnValuesBefore, pgdml->tableoid, type, pgdml->natts);

			if (!isInSnapshot)
				increment_connector_statistics(myBatchStats, STATS_DELETE, 1);
			break;
		}
		default:
		{
			/* all others, use SPI to execute regardless what synchdb_dml_use_spi is */
			return spi_execute(pgdml->dmlquery, type);
		}
	}
	return ret;
}

/*
 * ra_getConninfoByName
 *
 * This function executes a SELECT query on synchdb_conninfo table with the given
 * connector name as filter and returns a ConnectionInfo structure
 */
int
ra_getConninfoByName(const char * name, ConnectionInfo * conninfo, char ** connector)
{
	int numcols = -1;
	StringInfoData strinfo;
	Datum * res;
	MemoryContext conninfoContext;

	initStringInfo(&strinfo);
	conninfoContext = AllocSetContextCreate(TopMemoryContext, "CONNINIT", ALLOCSET_DEFAULT_SIZES);

	appendStringInfo(&strinfo, "SELECT "
			"coalesce(data->>'hostname', 'null'), "
			"coalesce(data->>'port', 'null'), "
			"coalesce(data->>'user', 'null'), "
			"pgp_sym_decrypt((data->>'pwd')::bytea, '%s'), "
			"coalesce(data->>'srcdb', 'null'), "
			"coalesce(data->>'dstdb', 'null'), "
			"coalesce(data->>'table', 'null'), "
			"coalesce(data->>'snapshottable', 'null'), "
			"coalesce(data->>'connector', 'null'), "
			"isactive,"
			"coalesce(data->>'ssl_mode', 'null'), "
			"coalesce(data->>'ssl_keystore', 'null'), "
			"coalesce(pgp_sym_decrypt((data->>'ssl_keystore_pass')::bytea, '%s'), 'null'), "
			"coalesce(data->>'ssl_truststore', 'null'), "
			"coalesce(pgp_sym_decrypt((data->>'ssl_truststore_pass')::bytea, '%s'), 'null'), "
			"coalesce(data->>'jmx_listenaddr', 'null'), "
			"coalesce(data->>'jmx_port', 'null'), "
			"coalesce(data->>'jmx_rmi_address', 'null'), "
			"coalesce(data->>'jmx_rmi_port', 'null'), "
			"(data->>'jmx_auth')::boolean, "
			"coalesce(data->>'jmx_auth_passwdfile', 'null'), "
			"coalesce(data->>'jmx_auth_accessfile', 'null'), "
			"(data->>'jmx_ssl')::boolean, "
			"coalesce(data->>'jmx_ssl_keystore', 'null'), "
			"coalesce(pgp_sym_decrypt((data->>'jmx_ssl_keystore_pass')::bytea, '%s'), 'null'), "
			"coalesce(data->>'jmx_ssl_truststore', 'null'), "
			"coalesce(pgp_sym_decrypt((data->>'jmx_ssl_truststore_pass')::bytea, '%s'), 'null'), "
			"coalesce(data->>'jmx_exporter', 'null'), "
			"coalesce(data->>'jmx_exporter_port', 'null'), "
			"coalesce(data->>'jmx_exporter_conf', 'null'), "
			"coalesce(data->>'olr_host', 'null'), "
			"coalesce(data->>'olr_port', 'null'), "
			"coalesce(data->>'olr_source', 'null'), "
			"coalesce(data->>'ispn_cache_type', 'null'), "
			"coalesce(data->>'ispn_memory_type', 'null'), "
			"coalesce(data->>'ispn_memory_size', 'null'), "
			"coalesce(data->>'srcschema', 'null') "
			"FROM "
			"synchdb_conninfo WHERE name = '%s'",
			SYNCHDB_SECRET, SYNCHDB_SECRET, SYNCHDB_SECRET, SYNCHDB_SECRET, SYNCHDB_SECRET, name);

	res = spi_execute_select_one(strinfo.data, &numcols, conninfoContext);
	if (!res)
		return -1;

	strlcpy(conninfo->name, name, SYNCHDB_CONNINFO_NAME_SIZE);
	strlcpy(conninfo->hostname, TextDatumGetCString(res[0]), SYNCHDB_CONNINFO_HOSTNAME_SIZE) ;
	conninfo->port = atoi(TextDatumGetCString(res[1]));
	strlcpy(conninfo->user, TextDatumGetCString(res[2]), SYNCHDB_CONNINFO_USERNAME_SIZE);
	strlcpy(conninfo->pwd, TextDatumGetCString(res[3]), SYNCHDB_CONNINFO_PASSWORD_SIZE);
	strlcpy(conninfo->srcdb, TextDatumGetCString(res[4]), SYNCHDB_CONNINFO_DB_NAME_SIZE);
	strlcpy(conninfo->dstdb, TextDatumGetCString(res[5]), SYNCHDB_CONNINFO_DB_NAME_SIZE);
	strlcpy(conninfo->table, TextDatumGetCString(res[6]) ,SYNCHDB_CONNINFO_TABLELIST_SIZE);
	strlcpy(conninfo->snapshottable, TextDatumGetCString(res[7]) ,SYNCHDB_CONNINFO_TABLELIST_SIZE);
	*connector = pstrdup(TextDatumGetCString(res[8]));
	conninfo->active = DatumGetBool(res[9]);
	strlcpy(conninfo->extra.ssl_mode, TextDatumGetCString(res[10]), SYNCHDB_CONNINFO_NAME_SIZE);
	strlcpy(conninfo->extra.ssl_keystore, TextDatumGetCString(res[11]), SYNCHDB_CONNINFO_KEYSTORE_SIZE);
	strlcpy(conninfo->extra.ssl_keystore_pass, TextDatumGetCString(res[12]), SYNCHDB_CONNINFO_PASSWORD_SIZE);
	strlcpy(conninfo->extra.ssl_truststore, TextDatumGetCString(res[13]), SYNCHDB_CONNINFO_KEYSTORE_SIZE);
	strlcpy(conninfo->extra.ssl_truststore_pass, TextDatumGetCString(res[14]) ,SYNCHDB_CONNINFO_PASSWORD_SIZE);

	strlcpy(conninfo->jmx.jmx_listenaddr, TextDatumGetCString(res[15]), SYNCHDB_CONNINFO_HOSTNAME_SIZE);
	conninfo->jmx.jmx_port = atoi(TextDatumGetCString(res[16]));
	strlcpy(conninfo->jmx.jmx_rmiserveraddr, TextDatumGetCString(res[17]), SYNCHDB_CONNINFO_HOSTNAME_SIZE);
	conninfo->jmx.jmx_rmiport = atoi(TextDatumGetCString(res[18]));
	conninfo->jmx.jmx_auth = DatumGetBool(res[19]);
	strlcpy(conninfo->jmx.jmx_auth_passwdfile, TextDatumGetCString(res[20]), SYNCHDB_METADATA_PATH_SIZE);
	strlcpy(conninfo->jmx.jmx_auth_accessfile, TextDatumGetCString(res[21]), SYNCHDB_METADATA_PATH_SIZE);
	conninfo->jmx.jmx_ssl = DatumGetBool(res[22]);
	strlcpy(conninfo->jmx.jmx_ssl_keystore, TextDatumGetCString(res[23]), SYNCHDB_CONNINFO_KEYSTORE_SIZE);
	strlcpy(conninfo->jmx.jmx_ssl_keystore_pass, TextDatumGetCString(res[24]), SYNCHDB_CONNINFO_PASSWORD_SIZE);
	strlcpy(conninfo->jmx.jmx_ssl_truststore, TextDatumGetCString(res[25]), SYNCHDB_CONNINFO_KEYSTORE_SIZE);
	strlcpy(conninfo->jmx.jmx_ssl_truststore_pass, TextDatumGetCString(res[26]), SYNCHDB_CONNINFO_PASSWORD_SIZE);
	strlcpy(conninfo->jmx.jmx_exporter, TextDatumGetCString(res[27]), SYNCHDB_METADATA_PATH_SIZE);
	conninfo->jmx.jmx_exporter_port = atoi(TextDatumGetCString(res[28]));
	strlcpy(conninfo->jmx.jmx_exporter_conf, TextDatumGetCString(res[29]), SYNCHDB_METADATA_PATH_SIZE);

	strlcpy(conninfo->olr.olr_host, TextDatumGetCString(res[30]), SYNCHDB_CONNINFO_HOSTNAME_SIZE);
	conninfo->olr.olr_port = atoi(TextDatumGetCString(res[31]));
	strlcpy(conninfo->olr.olr_source, TextDatumGetCString(res[32]), SYNCHDB_CONNINFO_NAME_SIZE);

	strlcpy(conninfo->ispn.ispn_cache_type, TextDatumGetCString(res[33]), INFINISPAN_TYPE_SIZE);
	strlcpy(conninfo->ispn.ispn_memory_type, TextDatumGetCString(res[34]), INFINISPAN_TYPE_SIZE);
	conninfo->ispn.ispn_memory_size = atoi(TextDatumGetCString(res[35]));
	strlcpy(conninfo->srcschema, TextDatumGetCString(res[36]), SYNCHDB_CONNINFO_DB_NAME_SIZE);

	elog(LOG, "name=%s hostname=%s, port=%d, user=%s srcdb=%s srcschema=%s"
			"dstdb=%s table=%s snapshottable=%s connector=%s extras(ssl_mode=%s ssl_keystore=%s "
			"ssl_keystore_pass=%s ssl_truststore=%s ssl_truststore_pass=%s) "
			"jmx(jmx_listenaddr=%s jmx_port=%d jmx_rmiserveraddr=%s jmx_rmiport=%d "
			"jmx_auth=%s jmx_auth_passwdfile=%s jmx_auth_accessfile=%s jmx_ssl=%s "
			"jmx_ssl_keystore=%s jmx_ssl_keystore_pass=%s jmx_ssl_truststore=%s "
			"jmx_ssl_truststore_pass=%s jmx_exporter=%s jmx_exporter_port=%d "
			"jmx_exporter_conf=%s) "
			"olr(olr_host=%s olr_port=%d olr_source=%s) "
			"ispn(ispn_cache_type='%s' ispn_memory_type='%s' ispn_memory_size=%u)",
			conninfo->name, conninfo->hostname, conninfo->port,
			conninfo->user, conninfo->srcdb, conninfo->srcschema,
			conninfo->dstdb, conninfo->table, conninfo->snapshottable, *connector,
			conninfo->extra.ssl_mode, conninfo->extra.ssl_keystore, conninfo->extra.ssl_keystore_pass,
			conninfo->extra.ssl_truststore, conninfo->extra.ssl_truststore_pass,
			conninfo->jmx.jmx_listenaddr, conninfo->jmx.jmx_port, conninfo->jmx.jmx_rmiserveraddr,
			conninfo->jmx.jmx_rmiport, conninfo->jmx.jmx_auth ? "true" : "false",
			conninfo->jmx.jmx_auth_passwdfile, conninfo->jmx.jmx_auth_accessfile,
			conninfo->jmx.jmx_ssl ? "true" : "false", conninfo->jmx.jmx_ssl_keystore,
			conninfo->jmx.jmx_ssl_keystore_pass, conninfo->jmx.jmx_ssl_truststore,
			conninfo->jmx.jmx_ssl_truststore_pass, conninfo->jmx.jmx_exporter,
			conninfo->jmx.jmx_exporter_port, conninfo->jmx.jmx_exporter_conf,
			conninfo->olr.olr_host, conninfo->olr.olr_port, conninfo->olr.olr_source,
			conninfo->ispn.ispn_cache_type, conninfo->ispn.ispn_memory_type,
			conninfo->ispn.ispn_memory_size);

	MemoryContextDelete(conninfoContext);
	return 0;
}

/*
 * ra_executeCommand
 *
 * Main entry to execute a query with SPI
 */
int
ra_executeCommand(const char * query)
{
	return spi_execute(query, TYPE_UNDEF);
}

/*
 * ra_listConnInfoNames
 *
 * This function executes a query on synchdb_conninfo and returns a list of connector
 * names
 */
int
ra_listConnInfoNames(char ** out, int * numout)
{
	int ret = -1, i = 0;
	char * query = "SELECT name FROM synchdb_conninfo WHERE isactive = true";
	char * value;
	MemoryContext oldcontext;
	bool skiptx = false;

	/*
	 * if we are already in transaction or transaction block, we can skip
	 * the transaction and snapshot acquisition code below
	 */
	if (IsTransactionOrTransactionBlock())
		skiptx = true;

	if (!skiptx)
	{
		/* Start a transaction and set up a snapshot */
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(WARNING, "synchdb_pgsql - SPI_connect failed");
		goto end;
	}

	ret = SPI_execute(query, true, 0);
	switch (ret)
	{
		case SPI_OK_SELECT:
		{
			break;
		}
		default:
		{
			SPI_finish();
			goto end;
		}
	}
	*numout = SPI_processed;
	if (*numout == 0)
	{
		SPI_finish();
		goto end;
	}

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	for (i = 0; i < *numout; i++)
	{
		value = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
		out[i] = pstrdup(value);
	}
	MemoryContextSwitchTo(oldcontext);

	/* Close the connection */
	SPI_finish();
	ret = 0;
end:
	if (!skiptx)
	{
		/* Commit the transaction */
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	return ret;
}

/*
 * ra_transformDataExpression
 *
 * Main entry to perform data transformation on the given data using SPI
 */
char *
ra_transformDataExpression(char * data, char * wkb, char * srid, char * expression)
{
	char * filledExpression = NULL;
	int ret = -1, i = 0;
	char * value = NULL;
	MemoryContext oldcontext;
	StringInfoData strinfo;
	bool skiptx = false;

	/*
	 * if we are already in transaction or transaction block, we can skip
	 * the transaction and snapshot acquisition code below
	 */
	if (IsTransactionOrTransactionBlock())
		skiptx = true;

	initStringInfo(&strinfo);

	filledExpression = swap_tokens(expression, data, wkb, srid);
	appendStringInfo(&strinfo, "SELECT %s;", filledExpression);

	elog(DEBUG1,"expression to execute = '%s'", strinfo.data);

	/* run the filled expression with SPI and obtain result as string */
	if (!skiptx)
	{
		/* Start a transaction and set up a snapshot */
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(WARNING, "transform data expression - SPI_connect failed");
		goto end;
	}

	ret = SPI_execute(strinfo.data, true, 1);
	switch (ret)
	{
		case SPI_OK_SELECT:
		{
			break;
		}
		default:
		{
			SPI_finish();
			goto end;
		}
	}
	if (SPI_processed == 0)
	{
		SPI_finish();
		elog(WARNING, "data transform expression results in no value");
		goto end;
	}

	/* only 1 record at most is expected */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	value = pstrdup(SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
	MemoryContextSwitchTo(oldcontext);

	/* Close the connection */
	SPI_finish();
end:
	if (!skiptx)
	{
		/* Commit the transaction */
		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	if (filledExpression)
		pfree(filledExpression);

	if (strinfo.data)
		pfree(strinfo.data);

	return value;
}

/*
 * ra_listConnInfoNames
 *
 * This function executes a query on both synchdb_att_view and synchdb_objmap and returns a list of mapping
 * information
 */
int
ra_listObjmaps(const char * name, ObjectMap ** out, int * numout)
{
	int ret = -1, i = 0;
	StringInfoData strinfo;
	char * value = NULL;

	MemoryContext oldcontext;
	bool skiptx = false;

	initStringInfo(&strinfo);
	appendStringInfo(&strinfo, "SELECT objtype, enabled, srcobj, dstobj, "
			"(SELECT pg_tbname FROM synchdB_att_view WHERE (ext_tbname=srcobj OR (ext_tbname || '.' || "
			"ext_attname) = srcobj) AND (objtype='table' OR objtype='column' OR objtype='datatype') "
			"AND %s.name=%s.name LIMIT 1), "
			"(SELECT pg_attname FROM synchdB_att_view WHERE (ext_tbname || '.' || ext_attname)=srcobj "
			"AND (objtype='column' OR objtype='datatype') AND %s.name=%s.name),"
			"(SELECT pg_atttypename FROM synchdB_att_view WHERE (ext_tbname || '.' || ext_attname)=srcobj "
			"AND objtype='datatype' AND %s.name=%s.name) "
			"FROM %s WHERE name = '%s' ORDER BY CASE objtype WHEN 'transform' THEN 1 WHEN 'datatype' THEN 2 "
			"WHEN 'column' THEN 3 WHEN 'table' then 4 ELSE 5 END;",
			SYNCHDB_OBJECT_MAPPING_TABLE,
			SYNCHDB_ATTRIBUTE_VIEW,
			SYNCHDB_OBJECT_MAPPING_TABLE,
			SYNCHDB_ATTRIBUTE_VIEW,
			SYNCHDB_OBJECT_MAPPING_TABLE,
			SYNCHDB_ATTRIBUTE_VIEW,
			SYNCHDB_OBJECT_MAPPING_TABLE, name);
	/*
	 * if we are already in transaction or transaction block, we can skip
	 * the transaction and snapshot acquisition code below
	 */
	if (IsTransactionOrTransactionBlock())
		skiptx = true;

	if (!skiptx)
	{
		/* Start a transaction and set up a snapshot */
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(WARNING, "synchdb_pgsql - SPI_connect failed");
		goto end;
	}

	ret = SPI_execute(strinfo.data, true, 0);
	switch (ret)
	{
		case SPI_OK_SELECT:
		{
			break;
		}
		default:
		{
			SPI_finish();
			goto end;
		}
	}
	*numout = SPI_processed;
	if (*numout == 0)
	{
		SPI_finish();
		goto end;
	}

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	*out = palloc0(sizeof(ObjectMap) * (*numout));
	for (i = 0; i < *numout; i++)
	{
		value = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
		if (value)
			strlcpy((*out)[i].objtype, value, SYNCHDB_CONNINFO_NAME_SIZE);

		value = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
		if (value)
		{
			if (strcmp(value, "t") == 0)
				(*out)[i].enabled = true;
			else if (strcmp(value, "f") == 0)
				(*out)[i].enabled = false;
			else
				(*out)[i].enabled = true;
		}

		value = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3);
		if (value)
			strlcpy((*out)[i].srcobj, value, SYNCHDB_CONNINFO_NAME_SIZE);

		value = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4);
		if (value)
			strlcpy((*out)[i].dstobj, value, SYNCHDB_TRANSFORM_EXPRESSION_SIZE);

		value = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 5);
		if (value)
			strlcpy((*out)[i].curr_pg_tbname, value, SYNCHDB_CONNINFO_NAME_SIZE);

		value = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 6);
		if (value)
			strlcpy((*out)[i].curr_pg_attname, value, SYNCHDB_CONNINFO_NAME_SIZE);

		value = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 7);
		if (value)
			strlcpy((*out)[i].curr_pg_atttypename, value, SYNCHDB_CONNINFO_NAME_SIZE);
	}
	MemoryContextSwitchTo(oldcontext);

	/* Close the connection */
	SPI_finish();
	ret = 0;
end:
	if (!skiptx)
	{
		/* Commit the transaction */
		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	if(strinfo.data)
		pfree(strinfo.data);

	return ret;
}

/*
 * destroyPGDDL
 *
 * Function to destroy PG_DDL structure
 */
void
destroyPGDDL(PG_DDL * ddlinfo)
{
	if (ddlinfo)
	{
		if (ddlinfo->ddlquery)
			pfree(ddlinfo->ddlquery);

		if (ddlinfo->schema)
			pfree(ddlinfo->schema);

		if (ddlinfo->tbname)
			pfree(ddlinfo->tbname);

		list_free_deep(ddlinfo->columns);

		pfree(ddlinfo);
	}
}

/*
 * destroyPGDML
 *
 * Function to destroy PG_DML structure
 */
void
destroyPGDML(PG_DML * dmlinfo)
{
	if (dmlinfo)
	{
		if (dmlinfo->dmlquery)
			pfree(dmlinfo->dmlquery);

		if (dmlinfo->columnValuesBefore)
			list_free_deep(dmlinfo->columnValuesBefore);

		if (dmlinfo->columnValuesAfter)
			list_free_deep(dmlinfo->columnValuesAfter);
		pfree(dmlinfo);
	}
}

char *
ra_run_orafdw_initial_snapshot_spi(ConnectorType connType, ConnectionInfo * conninfo,
		int flag, const char * snapshot_tables, const char * offset, bool fdw_use_subtx,
		bool write_schema_hist, const char * snapshotMode, int letter_casing_strategy)
{
	int ret = -1;
	bool isnull = false;
	Datum d;
	bool skiptx = false;
	char dstdb[SYNCHDB_CONNINFO_DB_NAME_SIZE] = {0};
	MemoryContext oldctx;
	char * snapshot_str = NULL;

	const char *sql = (flag & CONNFLAG_SCHEMA_SYNC_MODE) ||
			!strcasecmp(snapshotMode, "no_data")?
			"SELECT synchdb_do_schema_sync("
			"  $1::name,$2::text,$3::name,$4::name,$5::name,"
			"  $6::text,$7::name,$8::bool,$9::text,$10::text,"
			"  $11::text,$12::bool,$13::bool,$14::text)" :
			"SELECT synchdb_do_initial_snapshot("
			"  $1::name,$2::text,$3::name,$4::name,$5::name,"
			"  $6::text,$7::name,$8::bool,$9::text,$10::text,"
			"  $11::text,$12::bool,$13::bool,$14::text)";
	Oid   argtypes[14] = {
		NAMEOID, TEXTOID, NAMEOID, NAMEOID, NAMEOID,
		TEXTOID, NAMEOID, BOOLOID, TEXTOID, TEXTOID,
		TEXTOID, BOOLOID, BOOLOID, TEXTOID
	};
	Datum values[14];
	char  nulls[14] = {' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '};

	/*
	 * if we are already in transaction or transaction block, we can skip
	 * the transaction and snapshot acquisition code below
	 */
	if (IsTransactionOrTransactionBlock())
		skiptx = true;

	strlcpy(dstdb, conninfo->srcdb, SYNCHDB_CONNINFO_DB_NAME_SIZE);
	fc_normalize_name(letter_casing_strategy, dstdb, strlen(dstdb));

	PG_TRY();
	{
		if (!skiptx)
		{
			/* Start a transaction and set up a snapshot */
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
		}

		/* Build Datum values for name/text/bool/numeric */
		values[0]  = DirectFunctionCall1(namein,   CStringGetDatum(conninfo->name));
		values[1]  = CStringGetTextDatum(SYNCHDB_SECRET);
		values[2]  = DirectFunctionCall1(namein,   CStringGetDatum("ora_obj"));
		values[3]  = DirectFunctionCall1(namein,   CStringGetDatum("ora_stage"));
		values[4]  = DirectFunctionCall1(namein,   CStringGetDatum(dstdb));
		values[5]  = CStringGetTextDatum(conninfo->srcdb);

		/* we srcschema is not available, we put srcdb -> in the case of MySQL */
		if (strlen(conninfo->srcschema) == 0 || !strcmp(conninfo->srcschema, "null"))
			values[6]  = DirectFunctionCall1(namein,   CStringGetDatum(conninfo->srcdb));
		else
			values[6]  = DirectFunctionCall1(namein,   CStringGetDatum(conninfo->srcschema));

		values[7]  = BoolGetDatum(true);
		values[8]  = CStringGetTextDatum("replace");

		if (offset)
			values[9] = CStringGetTextDatum(offset);
		else
			nulls[9] = 'n';

		if (snapshot_tables)
			values[10] = CStringGetTextDatum(snapshot_tables);
		else
			nulls[10] = 'n';

		values[11] = BoolGetDatum(fdw_use_subtx);
		values[12] = BoolGetDatum(write_schema_hist);

		if (letter_casing_strategy == LCS_NORMALIZE_LOWERCASE)
			values[13] = CStringGetTextDatum("lower");
		else if (letter_casing_strategy == LCS_NORMALIZE_UPPERCASE)
			values[13] = CStringGetTextDatum("upper");
		else
			values[13] = CStringGetTextDatum("asis");

		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed");

		ret = SPI_execute_with_args(sql, 14, argtypes, values, nulls, false, 1);
		if (ret != SPI_OK_SELECT || SPI_processed != 1 || SPI_tuptable == NULL)
		{
			SPI_finish();
			elog(ERROR, "snapshot call failed with ret = %d", ret);
		}

		d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull)
		{
			SPI_finish();
			elog(ERROR, "snapshot function returned NULL");
		}

		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		snapshot_str = TextDatumGetCString(d);
		MemoryContextSwitchTo(oldctx);

		SPI_finish();

		if (!skiptx)
		{
			/* Commit the transaction */
			PopActiveSnapshot();
			CommitTransactionCommand();
		}
	}
	PG_CATCH();
	{
		ErrorData  *errdata;
		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		errdata = CopyErrorData();

		if (errdata)
			set_shm_connector_errmsg(myConnectorId, errdata->message);

		FreeErrorData(errdata);
		MemoryContextSwitchTo(oldctx);
		SPI_finish();
		snapshot_str = NULL;
		PG_RE_THROW();
	}
	PG_END_TRY();

	elog(WARNING, "snapshot to resume cdc: %s", snapshot_str ? snapshot_str : "N/A");
	return snapshot_str;
}

/*
 * ra_get_fdw_snapshot_err_table_list
 *
 * This function queries synchdb_fdw_snapshot_errors tables and build a list of tables that
 * have failed in previous FDW based initial snapshot.
 */
int
ra_get_fdw_snapshot_err_table_list(const char *name, char **out, int *numout, char **offset_out)
{
	int ret = -1;
	StringInfoData strinfo;
	char *agg_tbls = NULL;
	char *offset_txt  = NULL;
	bool skiptx = false;

	if (out)
		*out = NULL;
	if (numout)
		*numout = 0;
	if (offset_out)
		*offset_out = NULL;

	initStringInfo(&strinfo);
	appendStringInfo(&strinfo,
			"SELECT string_agg(tbl, ',' ORDER BY tbl) AS failed_tables, "
			"max(err_offset) AS err_offset "
			"FROM synchdb_fdw_snapshot_errors_%s "
			"WHERE connector_name = '%s'",
			name, name);

	if (IsTransactionOrTransactionBlock())
		skiptx = true;

	if (!skiptx)
	{
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	PG_TRY();
	{
		if (SPI_connect() != SPI_OK_CONNECT)
		{
			elog(WARNING, "SPI_connect failed");
			goto cleanup_tx;
		}

		ret = SPI_execute(strinfo.data, true, 0);
		if (ret != SPI_OK_SELECT || SPI_tuptable == NULL)
		{
			SPI_finish();
			goto cleanup_tx;
		}

		if (SPI_processed == 1)
		{
			MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);

			/* col 1: failed_tables (text via string_agg) */
			agg_tbls = SPI_getvalue(SPI_tuptable->vals[0],
						SPI_tuptable->tupdesc, 1);
			if (agg_tbls != NULL && out)
			{
				*out = pstrdup(agg_tbls);
				if (numout) *numout = 1; /* you treat it as a single CSV string */
			}

			/* col 2: err_offset (text) */
			offset_txt = SPI_getvalue(SPI_tuptable->vals[0],
					   SPI_tuptable->tupdesc, 2);
			if (offset_txt != NULL && offset_out)
			{
				*offset_out = pstrdup(offset_txt);
			}
			MemoryContextSwitchTo(oldctx);
		}

		SPI_finish();
		ret = 0;
	}
	PG_CATCH();
	{
		SPI_finish();
		FlushErrorState();

		if (!skiptx)
		{
			PopActiveSnapshot();
			CommitTransactionCommand();
		}
		pfree(strinfo.data);
		return -1;
	}
	PG_END_TRY();

cleanup_tx:
	if (!skiptx)
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	pfree(strinfo.data);
	return ret;
}

int
dump_schema_history_to_file(const char * connector_name, const char *out_path)
{
	StringInfoData sql;
	FILE *fp = NULL;
	const long fetchSize = 10000;
	Portal portal;
	int ret = -1, i = 0;
	SPIPlanPtr plan;
	TupleDesc tupdesc;
	SPITupleTable * tuptable;
	bool skiptx = false;

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT line FROM schema_history_%s", connector_name);

	if (IsTransactionOrTransactionBlock())
		skiptx = true;

	if (!skiptx)
	{
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	/* Open output file (truncate) */
	fp = AllocateFile(out_path, "w");
	if (!fp)
		return -1;

	PG_TRY();
	{
		if (SPI_connect() != SPI_OK_CONNECT)
		{
			elog(WARNING, "SPI_connect failed");
			goto cleanup_tx;
		}

		plan = SPI_prepare(sql.data, 0, NULL);
		if (plan == NULL)
		{
			elog(WARNING, "SPI_prepare failed");
			SPI_finish();
			goto cleanup_tx;
		}

		portal = SPI_cursor_open(NULL, plan, NULL, NULL, true);
		if (portal == NULL)
		{
			elog(WARNING, "SPI_cursor_open failed");
			SPI_finish();
			goto cleanup_tx;
		}

		for (;;)
		{
			SPI_cursor_fetch(portal, true, fetchSize);

			if (SPI_processed == 0)
			{
				if (SPI_tuptable)
					SPI_freetuptable(SPI_tuptable);
				break;
			}

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;

			for (i = 0; i < SPI_processed; i++)
			{
				HeapTuple tup = tuptable->vals[i];
				char *line = SPI_getvalue(tup, tupdesc, 1);
				if (!line)
					continue;

				fputs(line, fp);
				fputc('\n', fp);
			}
			SPI_freetuptable(SPI_tuptable);
		}

		SPI_cursor_close(portal);
		SPI_finish();
		ret = 0;
	}
	PG_CATCH();
	{
		SPI_finish();
		FlushErrorState();

		if (FreeFile(fp) != 0)
			ereport(WARNING,
					(errmsg("error closing file: %s", out_path)));

		if (!skiptx)
		{
			PopActiveSnapshot();
			CommitTransactionCommand();
		}
		pfree(sql.data);
		return -1;
	}
	PG_END_TRY();

cleanup_tx:
	if (FreeFile(fp) != 0)
		ereport(WARNING,
				(errmsg("error closing file: %s", out_path)));

	if (!skiptx)
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	pfree(sql.data);
	return ret;
}
