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

#include "executor/tuptable.h"
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

typedef struct pg_dml_column_value
{
	char * value;	/* string representation of column values that
					 * is processed and ready to be used to built
					 * into TupleTableSlot.
					 */
	Oid datatype;
	int position;	/* position of this value's attribute in tupdesc */
} PG_DML_COLUMN_VALUE;

typedef struct pg_dml
{
	char * dmlquery;	/* to be fed into SPI */

	char op;
	Oid tableoid;
	int natts;					/* number of columns of this pg table */
	List * columnValuesBefore;	/* list of PG_DML_COLUMN_VALUE */
	List * columnValuesAfter;	/* list of PG_DML_COLUMN_VALUE */
} PG_DML;

/* Function prototypes */
int ra_executePGDDL(PG_DDL * pgddl, ConnectorType type);
int ra_executePGDML(PG_DML * pgdml, ConnectorType type, SynchdbStatistics * myBatchStats);
int ra_getConninfoByName(const char * name, ConnectionInfo * conninfo, char ** connector);
int ra_executeCommand(const char * query);
int ra_listConnInfoNames(char ** out, int * numout);
char * ra_transformDataExpression(char * data, char * wkb, char * srid, char * expression);
int ra_listObjmaps(const char * name, ObjectMap ** out, int * numout);

void destroyPGDDL(PG_DDL * ddlinfo);
void destroyPGDML(PG_DML * dmlinfo);

#endif /* SYNCHDB_REPLICATION_AGENT_H_ */
