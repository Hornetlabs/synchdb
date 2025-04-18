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
#define SYNCHDB_CONNINFO_KEYSTORE_SIZE 128

#define DEBEZIUM_SHUTDOWN_TIMEOUT_MSEC 100000

#define SYNCHDB_OFFSET_SIZE 256
#define SYNCHDB_ERRMSG_SIZE 256
#define SYNCHDB_SNAPSHOT_MODE_SIZE 32
#define SYNCHDB_METADATA_PATH_SIZE 256
#define SYNCHDB_DATATYPE_NAME_SIZE 64
#define SYNCHDB_OBJ_NAME_SIZE 128
#define SYNCHDB_OBJ_TYPE_SIZE 32
#define SYNCHDB_TRANSFORM_EXPRESSION_SIZE 256
#define SYNCHDB_JSON_PATH_SIZE 128
#define SYNCHDB_INVALID_BATCH_ID -1

#define SYNCHDB_PG_MAJOR_VERSION  PG_VERSION_NUM / 100

/*
 * ex: 	pg_synchdb/[connector]_[name]_offsets.dat
 * 		pg_synchdb/mysql_mysqlconn_offsets.dat
 */
#define SYNCHDB_OFFSET_FILE_PATTERN "pg_synchdb/%s_%s_offsets.dat"
#define SYNCHDB_SECRET "930e62fb8c40086c23f543357a023c0c"
#define SYNCHDB_CONNINFO_TABLE "synchdb_conninfo"
#define SYNCHDB_ATTRIBUTE_TABLE "synchdb_attribute"
#define SYNCHDB_OBJECT_MAPPING_TABLE "synchdb_objmap"
#define SYNCHDB_ATTRIBUTE_VIEW "synchdb_att_view"

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
	STATE_SCHEMA_SYNC_DONE, /* connect has completed schema sync as requested */
	STATE_RELOAD_OBJMAP, /* connect is reloading object mapping */
} ConnectorState;

/**
 * ConnectorStage - Enum representing different stages of connector
 */
typedef enum _connectorStage
{
	STAGE_UNDEF = 0,
	STAGE_INITIAL_SNAPSHOT,
	STAGE_CHANGE_DATA_CAPTURE,
	STAGE_SCHEMA_SYNC,
} ConnectorStage;

/**
 * ConnectorStatistics - Enum representing different statistics of a connector
 */
typedef enum _connectorStatistics
{
	STATS_UNDEF = 0,
	STATS_DDL,
	STATS_DML,
	STATS_READ,
	STATS_CREATE,
	STATS_UPDATE,
	STATS_DELETE,
	STATS_BAD_CHANGE_EVENT,
	STATS_TOTAL_CHANGE_EVENT,
	STATS_BATCH_COMPLETION,
	STATS_AVERAGE_BATCH_SIZE
} ConnectorStatistics;

/**
 * ErrorStrategies - Enum representing different strategies to handle and error
 */
typedef enum _ErrorStrategies
{
	STRAT_UNDEF = 0,
	STRAT_EXIT_ON_ERROR,
	STRAT_SKIP_ON_ERROR,
	STRAT_RETRY_ON_ERROR
} ErrorStrategies;

/**
 * ErrorStrategies - Log levels of Debezium runner
 */
typedef enum _DbzLogLevels
{
	LOG_LEVEL_UNDEF = 0,
	LOG_LEVEL_ALL,
	LOG_LEVEL_DEBUG,
	LOG_LEVEL_INFO,
	LOG_LEVEL_WARN,
	LOG_LEVEL_ERROR,
	LOG_LEVEL_FATAL,
	LOG_LEVEL_OFF,
	LOG_LEVEL_TRACE
} DbzLogLevels;

/**
 * BatchInfo - Structure containing the metadata of a batch change request
 */
typedef struct _BatchInfo
{
	 int batchId;
	 int batchSize;
} BatchInfo;

/**
 * ExtraConnectionInfo - Extra DBZ Connector parameters are put here. Should
 * all be optional
 */
typedef struct _ExtraConnectionInfo
{
	char ssl_mode[SYNCHDB_CONNINFO_NAME_SIZE];
	char ssl_keystore[SYNCHDB_CONNINFO_KEYSTORE_SIZE];
	char ssl_keystore_pass[SYNCHDB_CONNINFO_PASSWORD_SIZE];
	char ssl_truststore[SYNCHDB_CONNINFO_KEYSTORE_SIZE];
	char ssl_truststore_pass[SYNCHDB_CONNINFO_PASSWORD_SIZE];
} ExtraConnectionInfo;

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
    bool isShcemaSync;
    bool isOraCompat; /* added to support ivorysql's oracle compatible mode */
    ExtraConnectionInfo extra;
} ConnectionInfo;

/**
 * ConnectorName - Used to store as a List* of names for automatic connector
 * resume feature
 */
typedef struct _ConnectorName
{
	char name[SYNCHDB_CONNINFO_NAME_SIZE];
} ConnectorName;

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
 * SynchdbRequest - Structure representing a statistic info per connector.
 * If you add new stats values here, make sure to add the same to ConnectorStatistics
 * enum above
 *
 * todo: to be persisted in future
 */
typedef struct _SynchdbStatistics
{
	unsigned long long stats_ddl;				/* number of DDL operations performed */
	unsigned long long stats_dml;				/* number of DML operations performed */
	unsigned long long stats_read;				/* READ events generated during initial snapshot */
	unsigned long long stats_create;			/* INSERT events generated during CDC */
	unsigned long long stats_update;			/* UPDATE events generated during CDC */
	unsigned long long stats_delete;			/* DELETE events generated during CDC */
	unsigned long long stats_bad_change_event;	/* number of bad change events */
	unsigned long long stats_total_change_event;/* number of total change events */
	unsigned long long stats_batch_completion;	/* number of batches completed */
	unsigned long long stats_average_batch_size;/* calculated average batch size: */
	unsigned long long stats_first_src_ts;	/* timestamp(ms) of last batch's first event generation in source db */
	unsigned long long stats_first_dbz_ts;	/* timestamp(ms) of last batch's first event processed by dbz */
	unsigned long long stats_first_pg_ts;	/* timestamp(ms) of last batch's first event processed by postgresql */
	unsigned long long stats_last_src_ts;	/* timestamp(ms) of last batch's last event generation in source db */
	unsigned long long stats_last_dbz_ts;	/* timestamp(ms) of last batch's last event processed by dbz */
	unsigned long long stats_last_pg_ts;	/* timestamp(ms) of last batch's last event processed by postgresql */
} SynchdbStatistics;

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
	SynchdbStatistics stats;
} ActiveConnectors;

/**
 * SynchdbSharedState - Shared state information for synchdb background worker
 */
typedef struct _SynchdbSharedState
{
	LWLock		lock;		/* mutual exclusion */
	ActiveConnectors * connectors;
} SynchdbSharedState;

typedef struct _ObjectMap
{
	char objtype[SYNCHDB_CONNINFO_NAME_SIZE];
	bool enabled;
	char srcobj[SYNCHDB_CONNINFO_NAME_SIZE];
	char dstobj[SYNCHDB_TRANSFORM_EXPRESSION_SIZE];
	char curr_pg_tbname[SYNCHDB_CONNINFO_NAME_SIZE];
	char curr_pg_attname[SYNCHDB_CONNINFO_NAME_SIZE];
	char curr_pg_atttypename[SYNCHDB_CONNINFO_NAME_SIZE];
} ObjectMap;

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
const char * get_shm_connector_name_by_id(int connectorId);
ConnectorState get_shm_connector_state_enum(int connectorId);
const char* connectorTypeToString(ConnectorType type);
void set_shm_connector_stage(int connectorId, ConnectorStage stage);
ConnectorStage get_shm_connector_stage_enum(int connectorId);
void increment_connector_statistics(SynchdbStatistics * myStats, ConnectorStatistics which, int incby);

#endif /* SYNCHDB_SYNCHDB_H_ */
