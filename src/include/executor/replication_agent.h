/*
 * replication_agent.h
 *
 * Header file for the SynchDB replication agent
 *
 * This file defines the data structures and function prototypes
 * used by the replication agent to handle DDL and DML operations
 * in PostgreSQL format.
 *
 * Key components:
 * - Structures for representing DDL and DML operations
 * - Function prototypes for executing DDL and DML operations
 * 
 * Copyright (c) 2024 Hornetlabs Technology, Inc.
 *
 */

#ifndef SYNCHDB_REPLICATION_AGENT_H_
#define SYNCHDB_REPLICATION_AGENT_H_

#include "executor/target_change.h"
#include "synchdb/synchdb.h"

/* Data structures representing PostgreSQL data formats */
typedef struct pg_ddl
{
	char * ddlquery;	/* to be fed into SPI*/
	DdlType type;		/* CREATE, DROP or ALTER...etc */
	AlterSubType subtype;	/* subtype for ALTER TABLE */
	char * schema;		/* name of PG schema */
	char * tbname;		/* name of PG table */
	List * columns;		/* list of PG_DDL_COLUMN */
} PG_DDL;

/*
 * Structure to represent a PG column in a DDL event that is
 * sufficient to update the attribute table. It does not need
 * to contain full column information
 */
typedef struct pg_ddl_column
{
	char * attname;
	char * atttype;
	int position;
} PG_DDL_COLUMN;

/* Legacy source adapters retain the old names without owning the model. */
typedef SynchdbTargetColumn PG_DML_COLUMN_VALUE;
typedef SynchdbTargetChange PG_DML;

/* Function prototypes */
int ra_executePGDDL(PG_DDL * pgddl, ConnectorType type);
int ra_executePGDML(PG_DML * pgdml, ConnectorType type, SynchdbStatistics * myBatchStats,
		bool isInSnapshot);
int ra_getConninfoByName(const char * name, ConnectionInfo * conninfo, char ** connector);
int ra_executeCommand(const char * query);
/* Returned names and their array belong to the caller's memory context. */
int ra_listConnInfoNames(char *** out, int * numout);
/* Caller-context array; success includes an empty list. */
int ra_listObjmaps(const char * name, ObjectMap ** out, int * numout);

void destroyPGDDL(PG_DDL * ddlinfo);
void destroyPGDML(PG_DML * dmlinfo);
/* Replacing the file is not rolled back if the caller owns the transaction. */
int dump_schema_history_to_file(const char * connector_name, const char *out_path);

#endif /* SYNCHDB_REPLICATION_AGENT_H_ */
