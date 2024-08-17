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
#include "replication_agent.h"
#include "executor/spi.h"
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
#include "synchdb.h"

extern bool synchdb_dml_use_spi;

/*
 * spi_execute - Execute a query using the Server Programming Interface (SPI)
 *
 * This function sets up a transaction, executes the given query using SPI,
 * and handles any errors that occur during execution.
 */
static int
spi_execute(char * query, ConnectorType type)
{
	int ret = -1;

	PG_TRY();
	{
		/* Start a transaction and set up a snapshot */
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (SPI_connect() != SPI_OK_CONNECT)
		{
			elog(WARNING, "synchdb_pgsql - SPI_connect failed");
			return ret;
		}

		ret = SPI_exec(query, 0);
		switch (ret)
		{
			case SPI_OK_INSERT:
			case SPI_OK_UTILITY:
			case SPI_OK_DELETE:
			case SPI_OK_UPDATE:
			{
				elog(WARNING, "SPI OK with ret %d", ret);
				break;
			}
			default:
			{
				SPI_finish();
				elog(WARNING, "SPI_exec failed: %d", ret);
				return ret;
			}
		}

		ret = 0;
		if (SPI_finish() != SPI_OK_FINISH)
		{
			elog(WARNING, "SPI_finish failed");
		}

		/* Commit the transaction */
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		ErrorData  *errdata = CopyErrorData();

		if (errdata)
			set_shm_connector_errmsg(type, errdata->message);

		FreeErrorData(errdata);
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
synchdb_handle_insert(List * colval, Oid tableoid, ConnectorType type)
{
	Relation rel;
	TupleDesc tupdesc;
	TupleTableSlot *slot;
	EState	   *estate;
	RangeTblEntry *rte;
	List	   *perminfos = NIL;
	ResultRelInfo *resultRelInfo;
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
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		rel = table_open(tableoid, NoLock);

		/* initialize estate */
		estate = CreateExecutorState();

		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RELATION;
		rte->relid = RelationGetRelid(rel);
		rte->relkind = rel->rd_rel->relkind;
		rte->rellockmode = AccessShareLock;

		addRTEPermissionInfo(&perminfos, rte);

		ExecInitRangeTable(estate, list_make1(rte), perminfos);
		estate->es_output_cid = GetCurrentCommandId(true);

		/* initialize resultRelInfo */
		resultRelInfo = makeNode(ResultRelInfo);
		InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

		/* turn colval into TupleTableSlot */
		tupdesc = RelationGetDescr(rel);
		slot = ExecInitExtraTupleSlot(estate, tupdesc, &TTSOpsVirtual);

		ExecClearTuple(slot);

		i = 0;
		foreach(cell, colval)
		{
			PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
			Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, i);
			Oid			typinput;
			Oid			typioparam;

			if (!strcasecmp(colval->value, "NULL"))
				slot->tts_isnull[i] = true;
			else
			{
				getTypeInputInfo(colval->datatype, &typinput, &typioparam);
				slot->tts_values[i] =
					OidInputFunctionCall(typinput, colval->value,
										 typioparam, attr->atttypmod);
				slot->tts_isnull[i] = false;
			}
			i++;
		}
		ExecStoreVirtualTuple(slot);

		/* We must open indexes here. */
		ExecOpenIndices(resultRelInfo, false);

		/* Do the insert. */
		ExecSimpleRelationInsert(resultRelInfo, estate, slot);

		/* Cleanup. */
		ExecCloseIndices(resultRelInfo);

		table_close(rel, NoLock);

		ExecResetTupleTable(estate->es_tupleTable, false);
		FreeExecutorState(estate);

		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		ErrorData  *errdata = CopyErrorData();
		if (errdata)
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "table %d: %s",
					tableoid, errdata->message);
			set_shm_connector_errmsg(type, msg);
			pfree(msg);
		}

		FreeErrorData(errdata);
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
synchdb_handle_update(List * colvalbefore, List * colvalafter, Oid tableoid, ConnectorType type)
{
	Relation rel;
	TupleDesc tupdesc;
	TupleTableSlot * remoteslot, * localslot;
	EState	   *estate;
	RangeTblEntry *rte;
	List	   *perminfos = NIL;
	ResultRelInfo *resultRelInfo;
	ListCell * cell;
	int i = 0, ret = 0;
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
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		rel = table_open(tableoid, NoLock);

		/* initialize estate */
		estate = CreateExecutorState();

		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RELATION;
		rte->relid = RelationGetRelid(rel);
		rte->relkind = rel->rd_rel->relkind;
		rte->rellockmode = AccessShareLock;

		addRTEPermissionInfo(&perminfos, rte);

		ExecInitRangeTable(estate, list_make1(rte), perminfos);
		estate->es_output_cid = GetCurrentCommandId(true);

		/* initialize resultRelInfo */
		resultRelInfo = makeNode(ResultRelInfo);
		InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

		/* turn colvalbefore into TupleTableSlot */
		tupdesc = RelationGetDescr(rel);

		remoteslot = ExecInitExtraTupleSlot(estate, tupdesc, &TTSOpsVirtual);
		localslot = table_slot_create(rel, &estate->es_tupleTable);

		ExecClearTuple(remoteslot);

		i = 0;
		foreach(cell, colvalbefore)
		{
			PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
			Form_pg_attribute attr = TupleDescAttr(remoteslot->tts_tupleDescriptor, i);
			Oid			typinput;
			Oid			typioparam;

			if (!strcasecmp(colval->value, "NULL"))
				remoteslot->tts_isnull[i] = true;
			else
			{
				getTypeInputInfo(colval->datatype, &typinput, &typioparam);
				remoteslot->tts_values[i] =
					OidInputFunctionCall(typinput, colval->value,
										 typioparam, attr->atttypmod);
				remoteslot->tts_isnull[i] = false;
			}
			i++;
		}
		ExecStoreVirtualTuple(remoteslot);
		EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1, NIL);

		/* We must open indexes here. */
		ExecOpenIndices(resultRelInfo, false);

		/*
		 * check if there is a PK or relation identity index that we could use to
		 * locate the old tuple. If no identity or PK, there may potentially be
		 * other indexes created on other columns that can be used. But for now,
		 * we do not bother checking for them. Mark it as todo for later.
		 */
		idxoid = GetRelationIdentityOrPK(rel);
		if (OidIsValid(idxoid))
		{
			elog(WARNING, "attempt to find old tuple by index");
			found = RelationFindReplTupleByIndex(rel, idxoid,
												 LockTupleExclusive,
												 remoteslot, localslot);
		}
		else
		{
			elog(WARNING, "attempt to find old tuple by seq scan");
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

			i = 0;
			foreach(cell, colvalafter)
			{
				PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
				Form_pg_attribute attr = TupleDescAttr(remoteslot->tts_tupleDescriptor, i);
				Oid			typinput;
				Oid			typioparam;

				if (!strcasecmp(colval->value, "NULL"))
					remoteslot->tts_isnull[i] = true;
				else
				{
					getTypeInputInfo(colval->datatype, &typinput, &typioparam);
					remoteslot->tts_values[i] =
						OidInputFunctionCall(typinput, colval->value,
											 typioparam, attr->atttypmod);
					remoteslot->tts_isnull[i] = false;
				}
				i++;
			}
			ExecStoreVirtualTuple(remoteslot);

			EvalPlanQualSetSlot(&epqstate, remoteslot);

			ExecSimpleRelationUpdate(resultRelInfo, estate, &epqstate, localslot,
									 remoteslot);
		}
		else
		{
			elog(WARNING, "tuple to update not found");
			ret = -1;
		}

		/* Cleanup. */
		ExecCloseIndices(resultRelInfo);
		EvalPlanQualEnd(&epqstate);
		ExecResetTupleTable(estate->es_tupleTable, false);
		FreeExecutorState(estate);
		table_close(rel, NoLock);

		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		ErrorData  *errdata = CopyErrorData();
		if (errdata)
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "table %d: %s",
					tableoid, errdata->message);
			set_shm_connector_errmsg(type, msg);
			pfree(msg);
		}

		FreeErrorData(errdata);
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
synchdb_handle_delete(List * colvalbefore, Oid tableoid, ConnectorType type)
{
	Relation rel;
	TupleDesc tupdesc;
	TupleTableSlot * remoteslot, * localslot;
	EState	   *estate;
	RangeTblEntry *rte;
	List	   *perminfos = NIL;
	ResultRelInfo *resultRelInfo;
	ListCell * cell;
	int i = 0, ret = 0;
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
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		rel = table_open(tableoid, NoLock);

		/* initialize estate */
		estate = CreateExecutorState();

		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RELATION;
		rte->relid = RelationGetRelid(rel);
		rte->relkind = rel->rd_rel->relkind;
		rte->rellockmode = AccessShareLock;

		addRTEPermissionInfo(&perminfos, rte);

		ExecInitRangeTable(estate, list_make1(rte), perminfos);
		estate->es_output_cid = GetCurrentCommandId(true);

		/* initialize resultRelInfo */
		resultRelInfo = makeNode(ResultRelInfo);
		InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

		/* turn colvalbefore into TupleTableSlot */
		tupdesc = RelationGetDescr(rel);

		remoteslot = ExecInitExtraTupleSlot(estate, tupdesc, &TTSOpsVirtual);
		localslot = table_slot_create(rel, &estate->es_tupleTable);

		ExecClearTuple(remoteslot);

		i = 0;
		foreach(cell, colvalbefore)
		{
			PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
			Form_pg_attribute attr = TupleDescAttr(remoteslot->tts_tupleDescriptor, i);
			Oid			typinput;
			Oid			typioparam;

			if (!strcasecmp(colval->value, "NULL"))
				remoteslot->tts_isnull[i] = true;
			else
			{
				getTypeInputInfo(colval->datatype, &typinput, &typioparam);
				remoteslot->tts_values[i] =
					OidInputFunctionCall(typinput, colval->value,
										 typioparam, attr->atttypmod);
				remoteslot->tts_isnull[i] = false;
			}
			i++;
		}
		ExecStoreVirtualTuple(remoteslot);
		EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1, NIL);

		/* We must open indexes here. */
		ExecOpenIndices(resultRelInfo, false);

		/*
		 * check if there is a PK or relation identity index that we could use to
		 * locate the old tuple. If no identity or PK, there may potentially be
		 * other indexes created on other columns that can be used. But for now,
		 * we do not bother checking for them. Mark it as todo for later.
		 */
		idxoid = GetRelationIdentityOrPK(rel);
		if (OidIsValid(idxoid))
		{
			elog(WARNING, "attempt to find old tuple by index");
			found = RelationFindReplTupleByIndex(rel, idxoid,
												 LockTupleExclusive,
												 remoteslot, localslot);
		}
		else
		{
			elog(WARNING, "attempt to find old tuple by seq scan");
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
			ret = -1;
		}

		/* Cleanup. */
		ExecCloseIndices(resultRelInfo);
		EvalPlanQualEnd(&epqstate);
		ExecResetTupleTable(estate->es_tupleTable, false);
		FreeExecutorState(estate);
		table_close(rel, NoLock);

		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		ErrorData  *errdata = CopyErrorData();
		if (errdata)
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "table %d: %s",
					tableoid, errdata->message);
			set_shm_connector_errmsg(type, msg);
			pfree(msg);
		}

		FreeErrorData(errdata);
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
ra_executePGDML(PG_DML * pgdml, ConnectorType type)
{
	if (!pgdml)
    {
        elog(WARNING, "Invalid DML operation");
        return -1;
    }

	switch (pgdml->op)
	{
		case 'r':  // Read operation
		case 'c':  // Create operation
		{
			if (synchdb_dml_use_spi)
				return spi_execute(pgdml->dmlquery, type);
			else
				return synchdb_handle_insert(pgdml->columnValuesAfter, pgdml->tableoid, type);
		}
		case 'u':  // Update operation
		{
			if (synchdb_dml_use_spi)
				return spi_execute(pgdml->dmlquery, type);
			else
				return synchdb_handle_update(pgdml->columnValuesBefore,
											 pgdml->columnValuesAfter,
											 pgdml->tableoid,
											 type);
		}
		case 'd':  // Delete operation
		{
			if (synchdb_dml_use_spi)
				return spi_execute(pgdml->dmlquery, type);
			else
				return synchdb_handle_delete(pgdml->columnValuesBefore, pgdml->tableoid, type);
		}
		default:
		{
			/* all others, use SPI to execute regardless what synchdb_dml_use_spi is */
			return spi_execute(pgdml->dmlquery, type);
		}
	}
	return -1;
}
