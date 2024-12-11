/*
 * synchdb.h
 *
 * Header file for the SynchDB synchronization system
 *
 * This file defines the core structures and functions used by SynchDB
 * to manage database synchronization across different connector types.
 *
 * Key components:
 * - Connector types and states
 * - Shared state structures for different database connectors
 * - Function prototypes for shared memory operations
 * 
 * Copyright (c) 2024 Hornetlabs Technology, Inc.
 *
 */

#ifndef SYNCHDB_SYNCHDB_H_
#define SYNCHDB_SYNCHDB_H_

#include "storage/lwlock.h"

/* Constants */
#define SYNCHDB_CONNINFO_NAME_SIZE 64
#define SYNCHDB_CONNINFO_HOSTNAME_SIZE 256
#define SYNCHDB_CONNINFO_USERNAME_SIZE 64
#define SYNCHDB_CONNINFO_PASSWORD_SIZE 128
#define SYNCHDB_CONNINFO_TABLELIST_SIZE 256
#define SYNCHDB_CONNINFO_RULEFILENAME_SIZE 64
#define SYNCHDB_CONNINFO_DB_NAME_SIZE 64
#define SYNCHDB_MAX_ACTIVE_CONNECTORS 30

#define DEBEZIUM_SHUTDOWN_TIMEOUT_MSEC 100000

#define SYNCHDB_OFFSET_SIZE 256
#define SYNCHDB_ERRMSG_SIZE 256
#define SYNCHDB_SNAPSHOT_MODE_SIZE 32

#define SYNCHDB_DATATYPE_NAME_SIZE 64
#define SYNCHDB_OBJ_NAME_SIZE 128
#define SYNCHDB_OBJ_TYPE_SIZE 32
#define SYNCHDB_TRANSFORM_EXPRESSION_SIZE 256
#define SYNCHDB_JSON_PATH_SIZE 128
#define SYNCHDB_INVALID_BATCH_ID -1

/*
 * ex: 	pg_synchdb/[connector]_[name]_offsets.dat
 * 		pg_synchdb/mysql_mysqlconn_offsets.dat
 */
#define SYNCHDB_OFFSET_FILE_PATTERN "pg_synchdb/%s_%s_offsets.dat"
#define SYNCHDB_SECRET "930e62fb8c40086c23f543357a023c0c"

#define SYNCHDB_CONNINFO_TABLE "synchdb_conninfo"
/* Enumerations */

/**
 * ConnectorType - Enum representing different types of database connectors
 */
typedef enum _connectorType
{
	TYPE_UNDEF = 0,
	TYPE_MYSQL,
	TYPE_ORACLE,
	TYPE_SQLSERVER,
} ConnectorType;

/**
 * ConnectorState - Enum representing different states of a connector
 */
typedef enum _connectorState
{
	STATE_UNDEF = 0,
	STATE_STOPPED,		/* connector is stopped */
	STATE_INITIALIZING,	/* connector is initializing java dbz runner engine */
	STATE_PAUSED,		/* connector paused until commanded to resume */
	STATE_SYNCING,		/* connector is polling changes from dbz engine */
	STATE_PARSING,		/* got a change event, try to parse it */
	STATE_CONVERTING,	/* parsing done, try to convert it to pg */
	STATE_EXECUTING,	/* conversion done, try to execute it on pg */
	STATE_OFFSET_UPDATE,/* in this state when user requests offset update */
	STATE_RESTARTING,	/* connector is restarting with new snapshot mode */
	STATE_MEMDUMP,		/* connector is dumping jvm heap memory info */
} ConnectorState;

/**
 * ConnectorStage - Enum representing different stages of connector
 */
typedef enum _connectorStage
{
	STAGE_UNDEF = 0,
	STAGE_INITIAL_SNAPSHOT,
	STAGE_CHANGE_DATA_CAPTURE,
} ConnectorStage;

/**
 * BatchInfo - Structure containing the metadata of a batch change request
 */
typedef struct _BatchInfo
{
	 int batchId;
	 int batchSize;
} BatchInfo;

/**
 * ConnectionInfo - DBZ Connection info. These are put in shared memory so
 * connector background workers can access when they are spawned.
 */
typedef struct _ConnectionInfo
{
	char name[SYNCHDB_CONNINFO_NAME_SIZE];
    char hostname[SYNCHDB_CONNINFO_HOSTNAME_SIZE];
    unsigned int port;
    char user[SYNCHDB_CONNINFO_USERNAME_SIZE];
    char pwd[SYNCHDB_CONNINFO_PASSWORD_SIZE];
	char srcdb[SYNCHDB_CONNINFO_DB_NAME_SIZE];
	char dstdb[SYNCHDB_CONNINFO_DB_NAME_SIZE];
    char table[SYNCHDB_CONNINFO_TABLELIST_SIZE];
    bool active;
    char rulefile[SYNCHDB_CONNINFO_RULEFILENAME_SIZE];
} ConnectionInfo;

/**
 * ExtraConnectionInfo - Extra DBZ Connection info parameters read from the
 * rule file (if specified). These won't be put in shared memory so they
 * are declared as pointers.
 */
typedef struct _ExtraConnectionInfo
{
	char * ssl_mode;
	char * ssl_keystore;
	char * ssl_keystore_pass;
	char * ssl_truststore;
	char * ssl_truststore_pass;
} ExtraConnectionInfo;

/**
 * SynchdbRequest - Structure representing a request to change connector state
 */
typedef struct _SynchdbRequest
{
	ConnectorState reqstate;
	char reqdata[SYNCHDB_ERRMSG_SIZE];
	ConnectionInfo reqconninfo;
} SynchdbRequest;

/**
 *  Structure holding state information for connectors
 */
typedef struct _ActiveConnectors
{
	pid_t pid;
	ConnectorState state;
	ConnectorStage stage;
	ConnectorType type;
	SynchdbRequest req;
	char errmsg[SYNCHDB_ERRMSG_SIZE];
	char dbzoffset[SYNCHDB_OFFSET_SIZE];
	char snapshotMode[SYNCHDB_SNAPSHOT_MODE_SIZE];
	ConnectionInfo conninfo;
} ActiveConnectors;

/**
 * SynchdbSharedState - Shared state information for synchdb background worker
 */
typedef struct _SynchdbSharedState
{
	LWLock		lock;		/* mutual exclusion */
	ActiveConnectors connectors[SYNCHDB_MAX_ACTIVE_CONNECTORS];
} SynchdbSharedState;

/* Function prototypes */
const char * get_shm_connector_name(ConnectorType type);
pid_t get_shm_connector_pid(int connectorId);
void set_shm_connector_pid(int connectorId, pid_t pid);
void set_shm_connector_errmsg(int connectorId, const char * err);
const char * get_shm_connector_errmsg(int connectorId);
void set_shm_connector_state(int connectorId, ConnectorState state);
const char * get_shm_connector_state(int connectorId);
void set_shm_dbz_offset(int connectorId);
const char * get_shm_dbz_offset(int connectorId);
ConnectorState get_shm_connector_state_enum(int connectorId);
const char* connectorTypeToString(ConnectorType type);
void set_shm_connector_stage(int connectorId, ConnectorStage stage);
ConnectorStage get_shm_connector_stage_enum(int connectorId);

#endif /* SYNCHDB_SYNCHDB_H_ */
