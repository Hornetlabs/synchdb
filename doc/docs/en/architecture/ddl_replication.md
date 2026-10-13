---
weight: 80
---
# DDL Replication

## **Overview**

SynchDB provides comprehensive support for Data Definition Language (DDL) operations, allowing real-time schema synchronization across different database systems.

## **Supported DDL Commands**

SynchDB supports the following general DDL operations for Debezium based connectors (MySQL, SQL Server, Oracle):

✅ CREATE [table]  
✅ ALTER [table] ADD COLUMN  
✅ ALTER [table] DROP COLUMN  
✅ ALTER [table] ALTER COLUMN  
✅ DROP [table]

For Openlog Replicator based connector these Oracle DDL syntax is supported: 

✅ CREATE [table]
✅ DROP [table]
✅ ALTER [table] MODIFY
✅ ALTER [table] ADD COLUMN
✅ ALTER [table] DROP COLUMN
✅ ALTER [table] ADD CONSTRAINT
✅ ALTER [table] DROP CONSTRAINT

✅ TRUNCATE [table] (capture-dependent; disabled by default)

## **Detailed Command Support**


### TRUNCATE TABLE

TRUNCATE is disabled by default. Enable it using the registered GUC (the historical spelling is intentional), reload configuration, then restart the relevant Debezium connector so its capture configuration changes:

```sql
ALTER SYSTEM SET synchdb.dbz_skipped_oeprations = 'none';
SELECT pg_reload_conf();
SELECT synchdb_restart_connector('myconnector', 'no_data');
```

An empty GUC value also enables all operations; SynchDB sends it to Debezium as `none`, because Debezium 3.5 rejects an empty `skipped.operations`. Other operation lists remain supported. Native OLR applies this GUC when processing incoming DDL.

The Debezium `payload.op = "t"` path reads `source.connector`, `source.db`, `source.table`, and the source schema for schema-based connectors. It does not require a message key, `before`, `after`, column definitions, or primary keys. Boolean and string snapshot metadata are supported; snapshot metadata and timestamps are optional for truncate data events. Explicit `tableChanges[].type = "TRUNCATE"` / `"TRUNCATE TABLE"` and single-table TRUNCATE DDL text also use the DDL execution path. Multi-table schema-change records and SQL batches are rejected rather than partially applied.

| Source | Capture format and requirements |
|---|---|
| MySQL, Debezium 3.5.2 | Binlog TRUNCATE becomes an `op="t"` data event. Table/database filters and `skipped.operations` must permit capture. |
| PostgreSQL | `pgoutput` emits `op="t"` for each affected table. The publication must include the table and publish `truncate`; normal DDL is not automatically emitted by logical decoding. |
| Oracle, Debezium 3.5.2 | The connector documents `op="t"` with `db`, `schema`, and `table`; the shipped LogMiner implementation dispatches TRUNCATE events. Capture still depends on the adapter, retained redo/archive logs and table filters. Older versions/adapters must be checked separately. |
| Native OpenLogReplicator | JSON contains `payload[0].op="ddl"`, `sql`, `schema.owner`, `schema.table`, top-level `db`, `scn`, `c_scn`, and `c_idx`. Enable DDL output and these metadata fields; the repository example uses `flags=32`, `db=3`, `schema=7`, and `message=2`. Owner/table filtering still applies. |
| SQL Server CDC | CDC does not provide TRUNCATE events; SQL Server disallows truncating a CDC-enabled table. SynchDB cannot reconstruct an event that the source does not emit. |

Table mappings use the original source object ID. Without a mapping, the source database becomes the target PostgreSQL schema, following existing SynchDB DDL/DML policy; the source schema is part of the mapping key. The configured letter-casing strategy applies to unmapped names. Explicit target mappings preserve their supplied case; a table-only destination uses `public`. Quote mapping components containing dots, spaces, or double quotes, for example `"target.schema"."table"" name"`.

The executor uses PostgreSQL identifier quoting and a fully qualified `TRUNCATE TABLE schema.table`. It adds neither `CASCADE` nor `RESTART IDENTITY`. Table definitions, indexes, relation OIDs, sequences and `synchdb_attribute` mappings survive; empty-table and repeated events succeed. Foreign-key restrictions, missing targets and permissions can fail execution. A multi-table source truncate can therefore fail on target foreign keys when applied as individual events.

Each successful truncate increments `synchdb_cdcstats.truncates` and its DDL count. Filtered events increment neither success counts nor `bad_events`. Execution errors propagate through the existing SPI transaction policy: the failed batch rolls back and is not acknowledged. Native OLR also restores its previous committed SCN before shutdown can persist it. Failed-batch success counters are not published. Subsequent DML uses the existing table and attribute mappings; UPDATE/DELETE without a primary key require the default native DML path (`synchdb.dml_use_spi=false`).

Both entry points log connector name and the raw event at `LOG` before JSON parsing; native OLR uses the message length.

Capture references: [Debezium MySQL](https://debezium.io/documentation/reference/3.5/connectors/mysql.html#mysql-truncate-events), [PostgreSQL](https://debezium.io/documentation/reference/3.5/connectors/postgresql.html#postgresql-truncate-events), [Oracle](https://debezium.io/documentation/reference/3.5/connectors/oracle.html#oracle-truncate-events), [PostgreSQL publication options](https://www.postgresql.org/docs/18/sql-createpublication.html), and [SQL Server TRUNCATE restrictions](https://learn.microsoft.com/en-us/sql/t-sql/statements/truncate-table-transact-sql).

### **CREATE TABLE**

SynchDB captures these properties during CREATE TABLE events:

| Property | Description |
|----------|-------------|
| Table Name | Fully Qualified Name (FQN) format |
| Column Names | Individual column identifiers |
| Data Types | Column data type specifications |
| Data Length | Length/precision specifications (if applicable) |
| Unsigned Flag | Unsigned constraints for numeric types |
| Nullability | NULL/NOT NULL constraints |
| Default Values | Default value expressions |
| Primary Keys | Primary key column definitions |

> **Note**: Additional CREATE TABLE properties are not currently supported

### **DROP TABLE**

Captured properties:
- Table name (in FQN format) to be dropped

### **ALTER TABLE ADD COLUMN**

Captures the following properties:

| Property | Description |
|----------|-------------|
| Column Names | Names of newly added columns |
| Data Types | Data types for new columns |
| Data Length | Length specifications (if applicable) |
| Unsigned Flag | Unsigned constraints |
| Nullability | NULL/NOT NULL specifications |
| Default Values | Default value expressions |
| Primary Keys | Updated primary key definitions |

Other properties that can be specified during ALTER TABLE ADD COLUMN  are not supported at the moment.

### **ALTER TABLE DROP COLUMN**

Captures:
- List of column names to be dropped

### **ALTER TABLE ALTER COLUMN**

Supported modifications:

| Modification | Description |
|--------------|-------------|
| Data Type | Change column data type |
| Type Length | Modify type length/precision |
| Default Value | Alter/drop default values |
| NOT NULL | Modify/drop NOT NULL constraint |

Other properties that can be specified during ALTER TABLE ALTER COLUMN  are not supported at the moment.

Please note that SynchDB only supports basic data type change on an existing column. For example, `INT` → `BIGINT` or `VARCHAR` → `TEXT`. Complex data type changes such as  `TEXT` → `INT` or `INT` → `TIMESTAMP` are not currently supported. This is because PostgreSQL requires the user to additionally supply a type casting function to perform the type casting as the result of complex data type change. SynchDB currently has limited knowledge what type casting functions to use for specific type conversion. In the future, We may allow user to supply his or her own casting functions to use for specific type conversions via the rule file, but for now, it is not supported.

## **Database-Specific Behavior**

### **MySQL and Oracle DDL Change Events**

Since MySQL logs both DDL and DML operations in the binlog and Oracles logs the same to logminer, SynchDB is able to replicate both DDLs and DMLs as they happen. No special actions are needed on MySQL or Oracle side to enable DDLs replication.

### **SQLServer DDL Change Events **

SQLServer does not natively supports DDL replication in streaming mode. The table schema is constructed by SynchDB during initial snapshot construction phase when the connector is started for the very first time. After this phase, SynchDB will try to detect any schema changes but they need to be explicitly added to SQL server's CDC table list.

#### **Trigger CREATE TABLE event on SQLServer**

To create a new table on SQL Server and added to its CDC table list:
```sql
CREATE TABLE dbo.altertest (
	a INT,
	b TEXT
	);
GO

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
	@source_name = 'altertest',
	@role_name = NULL,
	@supports_net_changes = 0,
	@capture_instance = 'dbo_altertest_1'
GO
```

The command adds the table `dbo.altertest` to the CDC table list and would cause SynchDB to receive a CREATE TABLE DDL change event.

#### **Trigger ALTER TABLE Events**

If an existing table is altered (add, drop or alter column), it needs to be explicitly updated to SQLServer's CDC table list, so that SynchDB will be able to receive the ALTER TABLE events.

For example:

Alter a table in SQLServer:
```sql
ALTER TABLE altertest ADD c NVARCHAR(MAX),
	d INT DEFAULT 0 NOT NULL,
	e NVARCHAR(255) NOT NULL,
	f INT DEFAULT 5 NOT NULL,
	CONSTRAINT PK_altertest PRIMARY KEY (
	d,
	f
	);
GO
```

Disable the old capture instance:
```sql
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo',
	@source_name = 'altertest',
	@capture_instance = 'dbo_altertest_1';
GO
```

Enable as a new capture intance:
```sql
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
	@source_name = 'altertest',
	@role_name = NULL,
	@supports_net_changes = 0,
	@capture_instance = 'dbo_altertest_2';
GO
```

Add new record:
```sql
INSERT INTO altertest VALUES(1, 's', 'c', 1, 'v', 5);
GO
```

The above example should allow SynchDB to receive an ALTER TABLE ADD COLUMN and an INSERT events. There is no need to restart both SQL Server or SynchDB to capture such events. The same apply to ALTER TABLE DROP COLUMN and ALTER TABLE ALTER COLUMN events as well.



