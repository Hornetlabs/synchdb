---
weight: 15
---
# Source Database Setups

Before Synchdb can interact with an external, heterogeneous database and start the replication, it needs to be configured according to the procedure below.

## **Set up MySQL for SynchDB**

### **Create a User**

create a user
```sql
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
```

grant required permissions
```sql
mysql> GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```

finalize the user's permission
```sql
mysql> FLUSH PRIVILEGES;
```

### **Enable binlog**

check if binlog is enabled
```sql
// for MySQL 5.x
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM information_schema.global_variables WHERE variable_name='log_bin';

// for MySQL 8.x
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM performance_schema.global_variables WHERE variable_name='log_bin';
```

add the properties below to configuration file if binlog is `OFF`
```
server-id         			= 223344
log_bin                     = mysql-bin
binlog_format               = ROW
binlog_row_image            = FULL
binlog_expire_logs_seconds  = 864000
```

check binlog status again
```sql
// for MySQL 5.x
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM information_schema.global_variables WHERE variable_name='log_bin';

// for MySQL 8.x
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM performance_schema.global_variables WHERE variable_name='log_bin';
```

### **Enable GTIDs (Optional)**

Global transaction identifiers (GTIDs) uniquely identify transactions that occur on a server within a cluster. Though not required for SynchDB connector, using GTIDs simplifies replication and enables you to more easily confirm if primary and replica servers are consistent.

enable `gtid_mode`
```sql
mysql> gtid_mode=ON
```

enable `enforce_gtid_consistency`
```sql
mysql> enforce_gtid_consistency=ON
```

confirm changes
```
mysql> show global variables like '%GTID%';
+--------------------------+-------+
| Variable_name            | Value |
+--------------------------+-------+
| enforce_gtid_consistency | ON    |
| gtid_mode                | ON    |
+--------------------------+-------+
```

### **Configure Session Timeout**

When an initial consistent snapshot is made for large databases, your established connection could timeout while the tables are being read. You can prevent this behavior by configuring `interactive_timeout` and `wait_timeout` in your MySQL configuration file.

configure `interactive_timeout:`
```sql
mysql> interactive_timeout=<duration in seconds>
```

configure `wait_timeout`
```sql
mysql> wait_timeout=<duration in seconds>
```

### **Enable Query Log Events**

You might want to see the original SQL statement for each binlog event. Enabling the `binlog_rows_query_log_events` option in the MySQL configuration file allows you to do this. Currently SynchDB does not process or parse the original SQL statement in anyway even if they are included. These are for reference / debug only.

enable `binlog_rows_query_log_events `
```sql
mysql> binlog_rows_query_log_events=ON
```

### **Validate Binlog Row Value Options**

Verify the setting of the `binlog_row_value_options` variable in the database. To enable the connector to consume UPDATE events, this variable must be set to a value other than `PARTIAL_JSON`.

check current variable value
```sql
mysql> show global variables where variable_name = 'binlog_row_value_options';
+--------------------------+-------+
| Variable_name            | Value |
+--------------------------+-------+
| binlog_row_value_options |       |
+--------------------------+-------+
```

if the value of the variable is `PARTIAL_JSON`, run the following to unset it
```sql
mysql> set @@global.binlog_row_value_options="" ;
```


## **Set up SQLServer for SynchDB**

### **Enable CDC on the SQLServer Database**

Before you can enable CDC for a table, you must enable it for the SQL Server database. A SQLServer admin enables CDC by running a system stored procedure. System stored procedures can be run by using SQL Server Management Studio, or by using Transact-SQL.

```sql
USE MyDB
GO
EXEC sys.sp_cdc_enable_db
GO
```

### **Enable CDC on a SQLServer Table**

SQLServer admin must enable change data capture on the source tables that you want SynchDB to capture. The database must already be enabled for CDC. To enable CDC on a table, a SQLServer administrator runs the stored procedure `sys.sp_cdc_enable_table` for the table. SQL Server CDC must be enabled for every table that you want to capture.

enable 3 tables `customer`, `district`, and `history` for SynchDB to capture:

```sql
USE MyDB
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customer', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'district', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'history', @role_name = NULL, @supports_net_changes = 0;
GO
```

### **Verify User Permission for CDC Tables**

A SQLServer administrator can run a system stored procedure to query a database or table to retrieve its CDC configuration information. 

The query below returns configuration information for each table in the database that is enabled for CDC and that contains change data that the caller is authorized to access. If the result is empty, verify that the user has privileges to access both the capture instance and the CDC tables.

```sql
USE MyDB;
GO
EXEC sys.sp_cdc_help_change_data_capture
GO
```

### **When Table Schema Changed While CDC is Enabled**

If a table has already been added to the CDC capture list and being captured by SynchDB already, any schema change that has happened to this table on SQLServer needs to be re-added back to the CDC capture list to generate a proper DDL ALTER TABLE event to SynchDB. Refer to [DDL Replication](../../architecture/ddl_replication/) page for more information.

## **Set up Oracle for SynchDB**

The following examples are based on container database `FREE` and pluggable database `FREEPDB1`

### **Set a Password for Sys User**

```sql
sqlplus / as sysdba
	Alter user sys identified by oracle;
Exit
```

### **Configure logminer**

```sql
sqlplus /nolog

	CONNECT sys/oracle as sysdba;
	alter system set db_recovery_file_dest_size = 10G;
	alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
	shutdown immediate;
	startup mount;
	alter database archivelog;
	alter database open;
	archive log list;
exit
```

### **Create a logminer User**

```sql
sqlplus sys/oracle@//localhost:1521/FREE as sysdba

	ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
	ALTER PROFILE DEFAULT LIMIT FAILED_LOGIN_ATTEMPTS UNLIMITED;
	exit;

sqlplus sys/oracle@//localhost:1521/FREE as sysdba

	CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
	exit;
	
sqlplus sys/oracle@//localhost:1521/FREEPDB1 as sysdba

	CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
	exit;

sqlplus sys/oracle@//localhost:1521/FREE as sysdba

	CREATE USER c##dbzuser IDENTIFIED BY dbz DEFAULT TABLESPACE LOGMINER_TBS QUOTA UNLIMITED ON LOGMINER_TBS CONTAINER=ALL;
	
	GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
	GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
	GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
	GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
	GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;
	
	GRANT SELECT ANY DICTIONARY TO c##dbzuser CONTAINER=ALL;
	
	GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
	GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
	GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;
	
	GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
	GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;
	
	GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
	
	GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER=ALL; 
	GRANT SELECT ON V_$MYSTAT TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$STATNAME TO c##dbzuser CONTAINER=ALL; 
	
	GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO C##DBZUSER;
	GRANT SELECT ON DBA_HIST_SNAPSHOT TO C##DBZUSER;
	GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO PUBLIC;
	
	
	Exit

```

### **Enable Supplemental Log Data for Tables Designated for Capture**

This configuration needs to be run on each table designated for capture in order to correctly handle the UPDATE and DELETE operations.

```sql
ALTER TABLE customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
... etc
```

## **Set up Native Openlog Replicator Connector for SynchDB**

### **Requirements**

- **Openlog Replicator Version**: `1.3.0` ~ `1.8.5` (verified compatibility for Debezium 2.7.x)
- Oracle instance with redo logs accessible to OLR
- Additional permissions must be granted for OLR (see below)
- Openlog Replicator must be configured and running
- An existing Oracle connector in SynchDB (created using `synchdb_add_conninfo()`)
- <<<**IMPORTANT**>>> **SynchDB must be compiled and built with flag (WITH_OLR=1) to support native openlog replicator connector.**

Refer to this [external guide](https://highgo.atlassian.net/wiki/external/OTUzY2Q2OWFkNzUzNGVkM2EyZGIyMDE1YzVhMDdkNWE) for details on deploying Openlog Replicator via Docker.

### **Additional Oracle Permissions**

In addition to the settings required for Oracle connector, Openlog Replicator requires additional permissions as below:

```sql

GRANT SELECT, FLASHBACK ON SYS.CCOL$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.CDEF$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.COL$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.DEFERRED_STG$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.ECOL$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.LOB$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.LOBCOMPPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.LOBFRAG$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.OBJ$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TAB$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TABCOMPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TABPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TABSUBPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TS$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.USER$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON XDB.XDB$TTSET TO DBZUSER;
GRANT FLASHBACK ANY TABLE TO DBZUSER;
GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO DBZUSER;
GRANT SELECT ON SYS.V_$DATABASE TO DBZUSER;
GRANT SELECT ON SYS.V_$DATABASE_INCARNATION TO DBZUSER;
GRANT SELECT ON SYS.V_$LOG TO DBZUSER;
GRANT SELECT ON SYS.V_$LOGFILE TO DBZUSER;
GRANT SELECT ON SYS.V_$PARAMETER TO DBZUSER;
GRANT SELECT ON SYS.V_$STANDBY_LOG TO DBZUSER;
GRANT SELECT ON SYS.V_$TRANSPORTABLE_PLATFORM TO DBZUSER;
DECLARE
    CURSOR C1 IS SELECT TOKSUF FROM XDB.XDB$TTSET;
    CMD VARCHAR2(2000);
BEGIN
    FOR C IN C1 LOOP
        CMD := 'GRANT SELECT, FLASHBACK ON XDB.X$NM' || C.TOKSUF || ' TO DBZUSER';
        EXECUTE IMMEDIATE CMD;
        CMD := 'GRANT SELECT, FLASHBACK ON XDB.X$QN' || C.TOKSUF || ' TO DBZUSER';
        EXECUTE IMMEDIATE CMD;
        CMD := 'GRANT SELECT, FLASHBACK ON XDB.X$PT' || C.TOKSUF || ' TO DBZUSER';
        EXECUTE IMMEDIATE CMD;
    END LOOP;
END;
```

## **Openlog Replicator Configuration Example**

SynchDB's OLR support is built against the configuration example below. 

**Version 1.3.0**
```json
{
  "version": "1.3.0",
  "source": [
    {
      "alias": "SOURCE",
      "name": "ORACLE",
      "reader": {
        "type": "online",
        "user": "DBZUSER",
        "password": "dbz",
        "server": "//ora19c:1521/FREE"
      },
      "format": {
        "type": "json",
        "column": 2,
        "db": 3,
        "interval-dts": 9,
        "interval-ytm": 4,
        "message": 2,
        "rid": 1,
        "schema": 7,
        "timestamp-all": 1,
        "scn-all": 1
      },
      "memory": {
        "min-mb": 64,
        "max-mb": 1024
      },
      "filter": {
        "table": [
          {"owner": "DBZUSER", "table": ".*"}
        ]
      },
      "flags": 32
    }
  ],
  "target": [
    {
      "alias": "SYNCHDB",
      "source": "SOURCE",
      "writer": {
        "type": "network",
        "uri": "0.0.0.0:7070"
      }
    }
  ]
}

```

**Version 1.8.5**
```json
{
  "version": "1.8.5",
  "source": [
    {
      "alias": "SOURCE",
      "name": "ORACLE",
      "reader": {
        "type": "online",
        "user": "DBZUSER",
        "password": "dbz",
        "server": "//ora19c:1521/FREE"
      },
      "format": {
        "type": "json",
        "column": 2,
        "db": 3,
        "interval-dts": 9,
        "interval-ytm": 4,
        "message": 2,
        "rid": 1,
        "schema": 7,
        "timestamp-all": 1,
        "scn-type": 1
      },
      "memory": {
        "min-mb": 64,
        "max-mb": 1024,
        "swap-path": "/opt/OpenLogReplicator/olrswap"
      },
      "filter": {
        "table": [
          {"owner": "DBZUSER", "table": ".*"}
        ]
      },
      "flags": 32
    }
  ],
  "target": [
    {
      "alias": "DEBEZIUM",
      "source": "SOURCE",
      "writer": {
        "type": "network",
        "uri": "0.0.0.0:7070"
      }
    }
  ]
}

```

Please note the following:

- "source"."name": "ORACLE" -> this should match the `olr_source` value when defining OLR parameters via `synchdb_add_olr_conninfo()` (See below)
- "source"."reader"."user" -> this should match the `username` value when creating a connector via `synchdb_add_conninfo()`
- "source"."reader"."password" -> this should match the `password` value when creating a connector via `synchdb_add_conninfo()`
- "source"."reader"."server" -> this should contain the values of `hostname`, `port` and `source database` values when creating a connector via `synchdb_add_conninfo()`
- "source"."filter"."table":[] -> this filters the change events that Openlog Replicator captures. <<<**IMPORTANT**>>>: This is currently the only way to filter change events from Oracle as OLR implementations in SynchDB does not do any filtering at this moment. (The `table` and `snapshot table` values are ignored when creating a connector via `synchdb_add_conninfo()`) 
- "format":{} -> the specific paylod format ingested by Debezium based or native Openlog Replicator connector. Use these values as specified.
- "memory"."swap-path" -> this tells OLR where to write swap files in low memory scenario.
- "target".[0]."writer"."type": -> this must specify `network` as both Debezium and native Openlog Replicator connector communicate with Openlog Replicator via network
- "target".[0]."writer"."uri": -> this is the bind host and port Openlog Replicator listens on that SynchDB should be able to access via `olr_host` and `olr_port` when defining OLR parameters via `synchdb_add_olr_conninfo()`. 

## **Set up Postgres Connector for SynchDB**

PostgreSQL server needs to be configured to be used as a database source to SynchDB

### GUC Settings

```
wal_level = logical
max_wal_senders = << may need to be adjusted based on your requirement - default is 10 >>
max_replication_slots = << may need to be adjusted based on your requirement - default is 10 >>
wal_writer_delay = << recommend to set to 10ms if `synchronous_commit` has a value other than "on". Default is 200ms >>

```

### Permission Settings

Create a new user with replication privileges:

```sql
CREATE ROLE <name> REPLICATION LOGIN;

```

SynchDB streams change events for PostgreSQL source tables from publications that are created for the tables. Publications contain a filtered set of change events that are generated from one or more tables. The data in each publication is filtered based on the publication specification. The specification can be created by the PostgreSQL database administrator or by the Debezium connector. To permit the SynchDB Postgres connector to create publications and specify the data to replicate to them, the connector must operate with specific privileges in the database.

There are several options for determining how publications are created. In general, it is best to manually create publications for the tables that you want to capture, before you set up the connector. However, you can configure your environment in a way that permits SynchDB to create publications automatically, and to specify the data that is added to them.

SynchDB uses include list properties (when a connector is created with `synchdb_add_conninfo`) to specify how data is inserted in the publication. 

For SynchDB to create a PostgreSQL publication, it must run as a user that has the following privileges:

* Replication privileges in the database to add the table to a publication.
* CREATE privileges on the database to add publications.
* SELECT privileges on the tables to copy the initial table data. Table owners automatically have SELECT permission for the table.

To add tables to a publication, the user must be an owner of the table. But because the source table already exists, you need a mechanism to share ownership with the original owner. To enable shared ownership, you create a PostgreSQL replication group, and then add the existing table owner and the replication user to the group.

Procedure:

* Create a replication group.

```
CREATE ROLE <replication_group>;
```

* Add the original owner of the table to the group.

```
GRANT REPLICATION_GROUP TO <original_owner>;
```

* Add the Debezium replication user to the group.

```
GRANT REPLICATION_GROUP TO <replication_user>;
```

* Transfer ownership of the table to <replication_group>.

```
ALTER TABLE <table_name> OWNER TO REPLICATION_GROUP;
```

### pg_hba Settings

To enable Debezium to replicate PostgreSQL data, you must configure the database to permit replication with the host that runs the PostgreSQL connector. To specify the clients that are permitted to replicate with the database, add entries to the PostgreSQL host-based authentication file, `pg_hba.conf`. For more information about the pg_hba.conf file, see the [PostgreSQL documentation](https://www.postgresql.org/docs/current/static/datatype-net-types.html).

```
local   replication     <youruser>                          trust   
host    replication     <youruser>  127.0.0.1/32            trust   
host    replication     <youruser>  ::1/128                 trust 
```

### Install DDL Trigger and Custom LSN View
To allow logical replication of user table DDLs to SynchDB, you must install a DDL trigger at source PostgreSQL database. Also, you need to create a custom view that returns the current LSN at the source database so that SycnhDB can retrieve it via FDW during snapshot process. 

SynchDB source repo contains a SQL script to set up and a script to tear down. Make sure to run them at the source PostgreSQL database:

Install DDL trigger function and current LSN view via a psql session:

```
psql -U <user> -d <database> < postgres-connector-src-ddl-setup.sql

``` 

Tear down DDL trigger function via a psql session:

```
psql -U <user> -d <database> < postgres-connector-src-ddl-teardown.sql

``` 

If you do not need DDL replication, then you do not need to install this DDL trigger function at the source. The current LSN view, however, is needed if you would like to take a current snapshot of the source database (current table + data). You can add it to the source database like this without loading the SQL script above. Please ensure this VIEW is created under `public` schema.

```sql
CREATE VIEW synchdb_wal_lsn AS SELECT pg_current_wal_lsn()::pg_lsn AS wal_lsn;
```

