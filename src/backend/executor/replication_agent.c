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
#include "miscadmin.h"
#include "executor/replication_agent.h"
#include "catalog/connector_store.h"
#include "catalog/mapping_store.h"
#include "executor/spi.h"
#include "executor/spi_writer.h"
#include "executor/tuple_writer.h"
#include "converter/transform_expression.h"
#include "converter/format_converter.h"
#include "runtime/config.h"
#include "access/xact.h"
#include "access/table.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "synchdb/synchdb.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "storage/ipc.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/relcache.h"

/*
 * Execute a utility query with its legacy transaction boundary.
 *
 * This function sets up a transaction, executes the given query using SPI,
 * and handles any errors that occur during execution.
 */
static int
spi_execute(const char *query, ConnectorType type)
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
				break;
			default:
				elog(ERROR, "SPI_exec failed: %d", ret);
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

/* Keep connector diagnostics outside the statement executor. */
static SynchdbRowApplyResult
execute_dml(const PG_DML *change)
{
	SynchdbRowApplyResult result = {0};

	PG_TRY();
	{
		if (synchdb_dml_use_spi)
		{
			if (SPI_connect() != SPI_OK_CONNECT)
				elog(ERROR, "could not connect the SPI row writer");
			result = synchdb_spi_writer_apply(change);
			if (SPI_finish() != SPI_OK_FINISH)
				elog(ERROR, "could not finish the SPI row writer");
		}
		else
			result = synchdb_tuple_writer_apply(change);
	}
	PG_CATCH();
	{
		MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
		ErrorData *error = CopyErrorData();
		char *message = palloc0(SYNCHDB_ERRMSG_SIZE);

		snprintf(message, SYNCHDB_ERRMSG_SIZE, "%s.%s: %s | %s",
				 error->schema_name == NULL ? "" : error->schema_name,
				 error->table_name == NULL ? "" : error->table_name,
				 error->message,
				 error->detail == NULL ? "" : error->detail);
		set_shm_connector_errmsg(myConnectorId, message);
		pfree(message);
		FreeErrorData(error);
		MemoryContextSwitchTo(old_context);
		if (synchdb_log_event_on_error && g_eventStr != NULL)
			elog(LOG, "%s", g_eventStr);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
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
ra_executePGDML(PG_DML *pgdml, ConnectorType type, SynchdbStatistics *myBatchStats,
		bool isInSnapshot)
{
	SynchdbRowApplyResult result;
	ConnectorStatistics counter;

	if (pgdml == NULL || !IsTransactionState())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("row apply requires a bound change and an active transaction")));
	switch (pgdml->op)
	{
		case 'r':
			counter = STATS_ROWS;
			break;
		case 'c':
			counter = STATS_CREATE;
			break;
		case 'u':
			counter = STATS_UPDATE;
			break;
		case 'd':
			counter = STATS_DELETE;
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported source row operation '%c'", pgdml->op)));
	}

	/* No source type currently proves that a conflicting create is replay.
	 * Both Writers must finish exactly one target row, or raise ERROR. */
	result = execute_dml(pgdml);
	result = synchdb_row_apply_require_one(result.affected_rows);
	if (pgdml->op == 'r' || !isInSnapshot)
		/* The exact-one check makes narrowing to the legacy counter safe. */
		increment_connector_statistics(myBatchStats, counter,
									   (int) result.affected_rows);

	/* Keep the DBZ/OLR event status independent from the target row count. */
	return 0;
}

/*
 * Compatibility entry point.  Store operations require a transaction/snapshot;
 * these legacy wrappers create them only when the caller has not done so.
 * Results always belong to the context current before any transaction/SPI work.
 */
int
ra_getConninfoByName(const char *name, ConnectionInfo *conninfo, char **connector)
{
	MemoryContext result_context = CurrentMemoryContext;
	bool own_transaction = !IsTransactionOrTransactionBlock();
	bool own_snapshot = !ActiveSnapshotSet();
	volatile bool snapshot_pushed = false;
	bool found = false;

	if (!name || !conninfo || !connector)
		elog(ERROR, "invalid connection information arguments");
	memset(conninfo, 0, sizeof(*conninfo));
	*connector = NULL;
	if (own_transaction)
		StartTransactionCommand();
	PG_TRY();
	{
		if (own_snapshot)
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			snapshot_pushed = true;
		}
		found = synchdb_connector_store_get(name, conninfo, connector, result_context);
		if (snapshot_pushed)
		{
			PopActiveSnapshot();
			snapshot_pushed = false;
		}
		if (own_transaction)
			CommitTransactionCommand();
		MemoryContextSwitchTo(result_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(result_context);
		if (*connector)
			pfree(*connector);
		memset(conninfo, 0, sizeof(*conninfo));
		*connector = NULL;
		if (own_transaction)
			AbortCurrentTransaction();
		else if (snapshot_pushed)
			PopActiveSnapshot();
		MemoryContextSwitchTo(result_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return found ? 0 : -1;
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

/* See ra_getConninfoByName for the legacy transaction ownership contract. */
int
ra_listConnInfoNames(char ***out, int *numout)
{
	MemoryContext result_context = CurrentMemoryContext;
	bool own_transaction = !IsTransactionOrTransactionBlock();
	bool own_snapshot = !ActiveSnapshotSet();
	volatile bool snapshot_pushed = false;

	if (!out || !numout)
		return -1;
	*out = NULL;
	*numout = 0;
	if (own_transaction)
		StartTransactionCommand();
	PG_TRY();
	{
		if (own_snapshot)
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			snapshot_pushed = true;
		}
		synchdb_connector_store_list_active(out, numout, result_context);
		if (snapshot_pushed)
		{
			PopActiveSnapshot();
			snapshot_pushed = false;
		}
		if (own_transaction)
			CommitTransactionCommand();
		MemoryContextSwitchTo(result_context);
	}
	PG_CATCH();
	{
		int i;

		MemoryContextSwitchTo(result_context);
		if (*out)
		{
			for (i = 0; i < *numout; i++)
				pfree((*out)[i]);
			pfree(*out);
		}
		*out = NULL;
		*numout = 0;
		if (own_transaction)
			AbortCurrentTransaction();
		else if (snapshot_pushed)
			PopActiveSnapshot();
		MemoryContextSwitchTo(result_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return 0;
}

/* Mapping results belong to the context current before any owned transaction. */
int
ra_listObjmaps(const char *name, ObjectMap **out, int *numout)
{
	MemoryContext result_context = CurrentMemoryContext;
	bool own_transaction = !IsTransactionOrTransactionBlock();
	bool own_snapshot = !ActiveSnapshotSet();
	volatile bool snapshot_pushed = false;

	if (!out || !numout)
		return -1;
	*out = NULL;
	*numout = 0;
	if (own_transaction)
		StartTransactionCommand();
	PG_TRY();
	{
		if (own_snapshot)
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			snapshot_pushed = true;
		}
		synchdb_mapping_store_list(name, out, numout, result_context);
		if (snapshot_pushed)
		{
			PopActiveSnapshot();
			snapshot_pushed = false;
		}
		if (own_transaction)
			CommitTransactionCommand();
		MemoryContextSwitchTo(result_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(result_context);
		if (*out)
			pfree(*out);
		*out = NULL;
		*numout = 0;
		if (own_transaction)
			AbortCurrentTransaction();
		else if (snapshot_pushed)
			PopActiveSnapshot();
		MemoryContextSwitchTo(result_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return 0;
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
static void
destroy_target_columns(List *columns)
{
	ListCell *cell;

	foreach(cell, columns)
	{
		PG_DML_COLUMN_VALUE *column = lfirst(cell);

		if (column->name != NULL)
			pfree(column->name);
		if (!column->is_null && !column->typbyval)
			pfree(DatumGetPointer(column->value));
		pfree(column);
	}
	list_free(columns);
}

void
destroyPGDML(PG_DML * dmlinfo)
{
	if (dmlinfo)
	{
		destroy_target_columns(dmlinfo->columnValuesBefore);
		destroy_target_columns(dmlinfo->columnValuesAfter);
		pfree(dmlinfo);
	}
}

int
dump_schema_history_to_file(const char * connector_name, const char *out_path)
{
	StringInfoData sql;
	MemoryContext callerContext = CurrentMemoryContext;
	FILE *volatile fp = NULL;
	char * table_name;
	char * temp_path;
	bool volatile temp_created = false;
	const long fetchSize = 10000;
	int volatile ret = -1;
	bool skiptx = IsTransactionOrTransactionBlock();

	initStringInfo(&sql);
	table_name = psprintf("schema_history_%s", connector_name);
	appendStringInfo(&sql, "SELECT line FROM %s", quote_identifier(table_name));
	pfree(table_name);
	temp_path = psprintf("%s.tmp.%d." INT64_FORMAT,
			out_path, MyProcPid, GetCurrentTimestamp());

	PG_TRY();
	{
		Portal portal = NULL;
		bool spi_connected = false;
		bool write_failed = false;

		if (!skiptx)
		{
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
		}

		do
		{
			SPIPlanPtr plan;

			/* Never truncate a previously usable history file on export failure. */
			fp = AllocateFile(temp_path, "wx");
			if (!fp)
			{
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not create schema history temporary file \"%s\": %m", temp_path)));
				break;
			}
			temp_created = true;

			if (SPI_connect() != SPI_OK_CONNECT)
			{
				elog(WARNING, "SPI_connect failed");
				break;
			}
			spi_connected = true;

			plan = SPI_prepare(sql.data, 0, NULL);
			if (plan == NULL)
			{
				elog(WARNING, "SPI_prepare failed");
				break;
			}
			portal = SPI_cursor_open(NULL, plan, NULL, NULL, true);
			if (portal == NULL)
			{
				elog(WARNING, "SPI_cursor_open failed");
				break;
			}

			for (;;)
			{
				SPITupleTable * tuptable;
				uint64 i;

				SPI_cursor_fetch(portal, true, fetchSize);
				if (SPI_processed == 0)
				{
					if (SPI_tuptable)
						SPI_freetuptable(SPI_tuptable);
					break;
				}

				tuptable = SPI_tuptable;
				for (i = 0; i < SPI_processed; i++)
				{
					char * line = SPI_getvalue(tuptable->vals[i], tuptable->tupdesc, 1);

					if (!line)
						continue;
					if (fputs(line, fp) == EOF || fputc('\n', fp) == EOF)
					{
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not write schema history temporary file \"%s\": %m", temp_path)));
						write_failed = true;
					}
					pfree(line);
					if (write_failed)
						break;
				}
				SPI_freetuptable(tuptable);
				if (write_failed)
					break;
			}
			if (!write_failed)
				ret = 0;
		} while (false);

		if (portal)
			SPI_cursor_close(portal);
		if (spi_connected && SPI_finish() != SPI_OK_FINISH)
			ret = -1;
		if (fp)
		{
			if (fflush(fp) != 0)
			{
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not flush schema history temporary file \"%s\": %m", temp_path)));
				ret = -1;
			}
			if (ret == 0 && pg_fsync(fileno(fp)) != 0)
			{
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not sync schema history temporary file \"%s\": %m", temp_path)));
				ret = -1;
			}
			if (FreeFile(fp) != 0)
			{
				fp = NULL;
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not close schema history temporary file \"%s\": %m", temp_path)));
				ret = -1;
			}
			fp = NULL;
		}

		if (!skiptx)
		{
			PopActiveSnapshot();
			CommitTransactionCommand();
		}

		/* An outer transaction remains the caller's responsibility. */
		if (ret == 0)
		{
			if (rename(temp_path, out_path) != 0)
			{
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not replace schema history file \"%s\": %m", out_path)));
				ret = -1;
			}
			else
				temp_created = false;
		}
	}
	PG_CATCH();
	{
		ErrorData * edata = NULL;

		MemoryContextSwitchTo(callerContext);
		if (!skiptx)
			edata = CopyErrorData();
		if (fp)
		{
			if (FreeFile(fp) != 0)
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not close schema history temporary file \"%s\": %m", temp_path)));
			fp = NULL;
		}
		if (temp_created)
		{
			if (unlink(temp_path) == 0)
				temp_created = false;
		}
		if (skiptx)
			PG_RE_THROW();

		FlushErrorState();
		AbortCurrentTransaction();
		ereport(WARNING,
				(errmsg("schema history export failed: %s", edata->message)));
		FreeErrorData(edata);
		ret = -1;
	}
	PG_END_TRY();

	if (temp_created && unlink(temp_path) != 0)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not remove schema history temporary file \"%s\": %m", temp_path)));
	pfree(temp_path);
	pfree(sql.data);
	return ret;
}
