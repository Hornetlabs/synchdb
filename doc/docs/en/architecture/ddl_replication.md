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

## **Detailed Command Support**

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



