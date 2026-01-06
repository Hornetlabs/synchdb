# SQL Server -> PostgreSQL

## **Prepare SQL Server Database for SynchDB**

Before SynchDB can be used to replicate from SQL Server, SQL Server needs to be configured according to the procedure outlined [here](../../getting-started/remote_database_setups/)

Please ensure the desired tables have already been enabled as CDC table in SQL Server. The following commands can be run on SQL Server client to enable CDC for `dbo.customer`, `dbo.district`, and `dbo.history`. You will continue to add new tables as needed.

```sql
USE testDB
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customer', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'district', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'history', @role_name = NULL, @supports_net_changes = 0;
GO
```


## **Create a SQL Server Connector**

Create a connector that targets all the tables under `testDB` database and `dbo` schema in SQL Server.
```sql
SELECT 
  synchdb_add_conninfo(
    'sqlserverconn', '127.0.0.1', 1433, 
    'sa', 'Password!', 'testDB', 'dbo', 
    'null', 'null', 'sqlserver');
```
## **Initial Snapshot**
"Initial snapshot" (or table snapshot) in SynchDB means to copy table schema plus initial data for all designated tables. This is similar to the term "table sync" in PostgreSQL logical replication. When a connector is started using the default `initial` mode, it will automatically perform the initial snapshot before going to Change Data Capture (CDC) stage. This can be partially omitted with mode `no_data`. See [here](../../user-guide/start_stop_connector/) for all snapshot options.

Once the initial snapshot is completed, the connector will not do it again upon subsequent restarts and will just resume with CDC since the last incomplete offset. This behavior is controled by the metadata files managed by Debezium engine. See [here](../../architecture/metadata_files/) for more about metadata files.
**
## **Different Connector Launch Modes**

### **Initial Snapshot + CDC**

Start the connector using `initial` mode will perform the initial snapshot of all designated tables (all in this case). After this is completed, the change data capture (CDC) process will begin to stream for new changes.

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'initial');

or 

SELECT synchdb_start_engine_bgw('sqlserverconn');
```

The stage of this connector should be in `initial snapshot` the first time it runs:
```sql
postgres=# select * from synchdb_state_view where name='sqlserverconn';
     name      | connector_type |  pid   |      stage       |  state  |   err    |       last_dbz_offset
---------------+----------------+--------+------------------+---------+----------+-----------------------------
 sqlserverconn | sqlserver      | 526003 | initial snapshot | polling | no error | offset file not flushed yet
(1 row)


```

A new schema called `testdb` will be created and all tables streamed by the connector will be replicated under that schema.
```sql
postgres=# set search_path=testdb;
SET
postgres=# \d
                  List of relations
 Schema |          Name           |   Type   | Owner
--------+-------------------------+----------+--------
 testdb | customers               | table    | ubuntu
 testdb | customers_id_seq        | sequence | ubuntu
 testdb | orders                  | table    | ubuntu
 testdb | orders_order_number_seq | sequence | ubuntu
 testdb | products                | table    | ubuntu
 testdb | products_id_seq         | sequence | ubuntu
 testdb | products_on_hand        | table    | ubuntu

```

After the initial snapshot is completed, and at least one subsequent changes is received and processed, the connector stage shall change from `initial snapshot` to `Change Data Capture`.
```sql
postgres=# select * from synchdb_state_view where name='sqlserverconn';
     name      | connector_type |  pid   |        stage        |  state  |   err    |
             last_dbz_offset
---------------+----------------+--------+---------------------+---------+----------+-----------------------------
----------------------------------------------------------------------
 sqlserverconn | sqlserver      | 526290 | change data capture | polling | no error | {"event_serial_no":1,"commit
_lsn":"0000002b:000004d8:0004","change_lsn":"0000002b:000004d8:0003"}

```

This means that the connector is now streaming for new changes of the designated tables. Restarting the connector in `initial` mode will proceed replication since the last successful point and initial snapshot will not be re-run.

### **Initial Snapshot Only and no CDC**

Start the connector using `initial_only` mode will perform the initial snapshot of all designated tables (all in this case) only and will not perform CDC after.

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'initial_only');

```

The connector would still appear to be `polling` from the connector but no change will be captured because Debzium internally has stopped the CDC. You have the option to shut it down. Restarting the connector in `initial_only` mode will not rebuild the tables as they have already been built.


### **Capture Table Schema Only + CDC**

Start the connector using `no_data` mode will perform the schema capture only, build the corresponding tables in PostgreSQL and it does not replicate existing table data (skip initial snapshot). After the schema capture is completed, the connector goes into CDC mode and will start capture subsequent changes to the tables.

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'no_data');

```

Restarting the connector in `no_data` mode will not rebuild the schema again, and it will resume CDC since the last successful point.

### **Always do Initial Snapahot + CDC**

Start the connector using `always` mode will always capture the schemas of capture tables, always redo the initial snapshot and then go to CDC. This is similar to a reset button because everything will be rebuilt using this mode. Use it with caution especially when you have large number of tables being captured, which could take a long time to finish. After the rebuild, CDC resumes as normal.

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'always');

```

After the initial snapshot, CDC will begin. Restarting a connector in `always` mode will repeat the same process described above.

## **Possible Snapshot Modes for SQL Server Connector**

* initial (default)
* initial_only
* no_data
* always
* schemasync

## **Preview Source and Destination Table Relationships with schemasync mode**

Before attempting to do an initial snapshot of current table and data, which may be huge, it is possible to "preview" all the tables and data type mappings between source and destination tables before the actual data migration. This gives you an opportunity to modify a data type mapping, or an object name before actual migration happens. This can be done with the special "schemasync" initial snapshot mode. Refer to [object mapping workflow](../../tutorial/object_mapping_workflow/) for a detailed example.

## **Selective Table Sync**

### **Select Desired Tables and Start it for the First Time**

Table selection is done during connector creation phase via `synchdb_add_conninfo()` where we specify a list of tables (expressed in FQN, separated by a comma) to replicate from.

For example, the following command creates a connector that only replicates change from `dbo.orders` tables from remote SQL Server database.
```sql
SELECT synchdb_add_conninfo(
    'sqlserverconn', 
    '127.0.0.1', 
    1433, 
    'sa', 
    'Password!', 
    'testDB', 
    'dbo', 
    'dbo.orders,dbo.products',
    'null', 
    'sqlserver'
);
```

Starting this connector for the very first time will trigger an initial snapshot being performed and selected 2 tables' schema and data will be replicated.

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn');
```

### **Verify the Connector State and Tables**

Examine the connector state and the new tables:
```sql
postgres=# Select name, state, err from synchdb_state_view;
     name      |  state  |   err
---------------+---------+----------
 sqlserverconn | polling | no error

postgres=# \dt testdb.*
           List of tables
 Schema |   Name   | Type  | Owner
--------+----------+-------+--------
 testdb | orders   | table | ubuntu
 testdb | products | table | ubuntu

```

By default, source database name is mapped to a schema in destination, so `dbo.orders` becomes `testdb.orders` in postgreSQL. Once the tables have done the initial snapshot, the connector will start CDC to stream subsequent changes for these tables.

### **Add More Tables to Replicate During Run Time.**

If we would like to add more tables to replicate from, we will need to notify the Debezium engine about the updated table section and perform the initial snapshot again. Here's how it is done:

1. Update the `synchdb_conninfo` table to include additional tables.
2. In this example, we add the `dbo.customers` table to the sync list:
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{table}', '"dbo.orders,dbo.products,dbo.customers"') 
WHERE name = 'sqlserverconn';
```
3. Restart the connector with the snapshot mode set to `always` to perform another initial snapshot:
```sql
DROP table testdb.orders, testdb.products;
SELECT synchdb_restart_connector('sqlserverconn', 'always');
```
This forces Debezium to re-snapshot all the tables again, including the old tables `dbo.orders` and `dbo.products` and the new before going to CDC streaming. This means, to add the third table, the existing tables have to be dropped (to prevent duplicate table and primary key errors) and do the entire initial snapshot again. This is quite redundant and Debezium suggests using incremental snasphot to add the addition tables without re-snapshotting. We will update this procedure once we add the incremental snapshot support to SynchDB.

### **Verify the Updated Tables**

Now, we can examine our tables again:
```sql
postgres=# \dt "testDB".*
           List of tables
 Schema |   Name    | Type  | Owner
--------+-----------+-------+--------
 testDB | customers | table | ubuntu
 testDB | orders    | table | ubuntu
 testDB | products  | table | ubuntu

```