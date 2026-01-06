---
weight: 15
---
# 源数据库设置

在 Synchdb 可以与外部异构数据库交互并启动复制之前，需要根据下列流程进行配置

## **为 SynchDB 设置 MySQL**

### **创建用户**

创建用户
```sql
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
```

授予所需权限
```sql
mysql> GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```

确定用户的权限
```sql
mysql> FLUSH PRIVILEGES;
```

### **启用 binlog**

检查 binlog 是否启用
```sql
// for MySQL 5.x
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM information_schema.global_variables WHERE variable_name='log_bin';

// for MySQL 8.x
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM performance_schema.global_variables WHERE variable_name='log_bin';
```

如果 binlog 处于“OFF”状态，则将以下属性添加到配置文件中
```
server-id         			= 223344
log_bin                     = mysql-bin
binlog_format               = ROW
binlog_row_image            = FULL
binlog_expire_logs_seconds  = 864000
```

再次检查binlog状态
```sql
// for MySQL 5.x
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM information_schema.global_variables WHERE variable_name='log_bin';

// for MySQL 8.x
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM performance_schema.global_variables WHERE variable_name='log_bin';
```

### **启用 GTID（可选）**

全局事务标识符 (GTID) 唯一地标识集群内服务器上发生的事务。虽然 SynchDB 连接器不强制需要，但使用 GTID 可以简化复制，并使您能够更轻松地确认主服务器和副本服务器是否一致。

启用 `gtid_mode`
```sql
mysql> gtid_mode=ON
```

启用 `enforce_gtid_consistency`
```sql
mysql> enforce_gtid_consistency=ON
```

确认更改
```
mysql> show global variables like '%GTID%';
+--------------------------+-------+
| Variable_name            | Value |
+--------------------------+-------+
| enforce_gtid_consistency | ON    |
| gtid_mode                | ON    |
+--------------------------+-------+
```

### **配置会话超时**

当为大型数据库制处理 initial snapshot时，您建立的连接可能会在读取表时超时。您可以通过在 MySQL 配置文件中配置`interactive_timeout`和`wait_timeout`来防止此行为。

配置 `interactive_timeout:`
```sql
mysql> interactive_timeout=<duration in seconds>
```

配置 `wait_timeout`
```sql
mysql> wait_timeout=<duration in seconds>
```

### **启用查询日志事件**

您可能希望查看每个二进制日志事件的原始 SQL 语句。在 MySQL 配置文件中启用`binlog_rows_query_log_events`选项允许您执行此操作。目前 SynchDB 不会以任何方式处理或解析原始 SQL 语句，即使它们包含在 binlog 内。这些仅供参考/调试

启用 `binlog_rows_query_log_events `
```sql
mysql> binlog_rows_query_log_events=ON
```

### **验证 Binlog 行值选项**

验证数据库中 `binlog_row_value_options` 变量的设置。要使连接器能够使用 UPDATE 事件，必须将此变量设置为除 `PARTIAL_JSON` 以外的值。

check current variable value
```sql
mysql> show global variables where variable_name = 'binlog_row_value_options';
+--------------------------+-------+
| Variable_name            | Value |
+--------------------------+-------+
| binlog_row_value_options |       |
+--------------------------+-------+
```

如果变量的值为“PARTIAL_JSON”，请运行以下命令取消设置它
```sql
mysql> set @@global.binlog_row_value_options="" ;
```


## **为 SynchDB 设置 SQLServer**

### **在 SQLServer 数据库上启用 CDC**

在为表启用 CDC 之前，必须先为 SQL Server 数据库启用它。SQLServer 管理员通过运行系统存储过程来启用 CDC。可以使用 SQL Server Management Studio 或 Transact-SQL 来运行系统存储过程。

```sql
USE MyDB
GO
EXEC sys.sp_cdc_enable_db
GO
```

### **在 SQLServer 表上启用 CDC**

SQLServer 管理员必须在您希望 SynchDB 捕获的源表上启用变更数据捕获。数据库必须已启用 CDC。要在表上启用 CDC，SQLServer 管理员需要为表运行存储过程“sys.sp_cdc_enable_table”。必须为要捕获的每个表启用 SQL Server CDC。

为 SynchDB 启用 3 个表`customer`、`district` 和 `history` 以进行捕获：

```sql
USE MyDB
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customer', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'district', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'history', @role_name = NULL, @supports_net_changes = 0;
GO
```

### **验证用户对 CDC 表的权限**

SQLServer 管理员可以运行系统存储过程来查询数据库或表以检索其 CDC 配置信息。

以下查询返回数据库中启用 CDC 的每个表的配置信息，其中包含调用者有权访问的更改数据。如果结果为空，请验证用户是否有权访问捕获实例和 CDC 表。

```sql
USE MyDB;
GO
EXEC sys.sp_cdc_help_change_data_capture
GO
```

### **当启用 CDC 时表架构发生更改**

如果某个表已添加到 CDC 捕获列表并已被 SynchDB 捕获，则需要将 SQLServer 上此表发生的任何架构更改重新添加回 CDC 捕获列表，以向 SynchDB 生成正确的 DDL ALTER TABLE 事件。有关更多信息，请参阅 [DDL 复制](architecture/ddl_replication/) 页面。

## **为 SynchDB 设置 Oracle**

以下示例基于容器数据库 `FREE` 和可插拔数据库 `FREEPDB1`:

### **为 Sys 用户设置密码**

```sql
sqlplus / as sysdba
	Alter user sys identified by oracle;
Exit
```

### **配置日志挖掘器**

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

### **创建 logminer 用户**

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

### **为指定捕获表启用补充日志数据**

需要在每个被捕获的表运行此配置，以便正确处理 UPDATE 和 DELETE 操作。

```sql
ALTER TABLE customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
... etc
```

## **Openlog Replicator 支持的其他 Oracle 设置**

Openlog Replicator 需要额外权限才能流式传输 Oracle 更改：

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

## **設定 SynchDB 的 Postgres 連接器**

需要配置 PostgreSQL 伺服器，使其用作 SynchDB 的資料庫來源。

### GUC 設定

```

wal_level = logical
max_wal_senders = << 可能需要根據您的需求進行調整 - 預設值為 10 >>
max_replication_slots = << 可能需要根據您的需求進行調整 - 預設值為 10 >>
wal_writer_delay = << 如果 `synchronous_commit` 的值不是 “on”，建議設定為 10ms。預設值為 200ms >>

```

### 權限設定

建立一個具有複製權限的新使用者：

```sql

CREATE ROLE <name> REPLICATION LOGIN;

```

SynchDB 從為 PostgreSQL 來源表建立的發布中取得來源表的變更事件流。發布包含從一個或多個表產生的經過篩選的變更事件集。每個發布中的數據都根據發布規範進行過濾。規格可以由 PostgreSQL 資料庫管理員或 Debezium 連接器建立。為了允許 SynchDB Postgres 連接器建立發布並指定要複製到其中的數據，連接器必須以資料庫中的特定權限運行。

有幾種方法可以確定如何建立發布。通常，最好在設定連接器之前，為要捕獲的表手動建立發布。但是，您可以設定環境，使 SynchDB 自動建立發布並指定要新增到其中的資料。

SynchDB 使用包含清單屬性（當使用 `synchdb_add_conninfo` 建立連接器時）來指定如何將資料插入到發布中。

要使 SynchDB 建立 PostgreSQL 發布，它必須以具有以下權限的使用者身分執行：

* 資料庫中的複製權限，用於將表格新增至發布。
* 資料庫的 CREATE 權限，用於新增發布。
* 需要對錶擁有 SELECT 權限才能複製初始表資料。表所有者自動擁有對錶的 SELECT 權限。

若要將表格新增至發布中，使用者必須是該表格的擁有者。但由於來源表已存在，因此需要一種機制與原始所有者共享所有權。若要啟用共享所有權，您可以建立 PostgreSQL 複製群組，然後將現有表格擁有者和複製使用者新增至該群組。

步驟：

* 建立複製組。

```

CREATE ROLE <replication_group>;

```

* 將表格的原始擁有者新增至該群組。

```

GRANT REPLICATION_GROUP TO <original_owner>;

```

* 將 Debezium 複製使用者新增至該群組。

```

GRANT REPLICATION_GROUP TO <replication_user>;

```

* 將表格的所有權轉移給 <replication_group>。

```

ALTER TABLE <table_name> OWNER TO REPLICATION_GROUP;

```

### pg_hba 設定

要使 Debezium 能夠複製 PostgreSQL 數據，您必須配置資料庫以允許與執行 PostgreSQL 連接器的主機進行複製。若要指定允許與資料庫複製的用戶端，請在 PostgreSQL 基於主機的驗證檔案 `pg_hba.conf` 新增條目。有關 pg_hba.conf 文件的更多信息，請參閱 [PostgreSQL 文件](https://www.postgresql.org/docs/current/static/datatype-net-types.html)。

```
local replication <youruser> trust
host replication <youruser> 127.0.0.1/32 trust
host replication <youruser> ::1/128 trust
```

### 安裝 DDL 觸發器和自訂 LSN 視圖

若要將使用者表 DDL 邏輯複製到 SynchDB，您必須在來源 PostgreSQL 資料庫中安裝 DDL 觸發器。此外，您還需要建立自訂視圖，該視圖會傳回來源資料庫中的目前 LSN，以便 SynchDB 可以在快照過程中透過 FDW 檢索它。

SynchDB 原始碼庫包含一個用於設定的 SQL 腳本和一個用於清理的腳本。請確保在來源 PostgreSQL 資料庫中運行它們：

通过 psql 會話安裝 DDL 觸發器函數：

```

psql -U <user> -d <database> < postgres-connector-src-ddl-setup.sql

```

通过 psql 會話清理 DDL 觸發器函數：

```

psql -U <user> -d <database> < postgres-connector-src-ddl-teardown.sql

```

如果您不需要 DDL 複製，則無需在來源資料庫安裝此 DDL 觸發器函數。但是，如果您想要對來源資料庫（目前表格 + 資料）進行目前快照，則需要目前 LSN 視圖。您無需載入上述 SQL 腳本，即可將其新增至來源資料庫。請確保此視圖是在 `public` 模式下建立的。

```sql

CREATE VIEW synchdb_wal_lsn AS SELECT pg_current_wal_lsn()::pg_lsn AS wal_lsn;

```