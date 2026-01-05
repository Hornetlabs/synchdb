# SQL Server -> PostgreSQL

## Prepare SQL Server Database for SynchDB

Before SynchDB can be used to replicate from SQL Server, SQL Server needs to be configured according to the procedure outlined [here](../../getting-started/remote_database_setups/)

Please ensure the desired tables have already been enabled as CDC table in SQL Server. The following commands can be run on SQL Server client to enable CDC for `dbo.customer`, `dbo.district`, and `dbo.history`. You will continue to add new tables as needed.

```sql
USE MyDB
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customer', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'district', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'history', @role_name = NULL, @supports_net_changes = 0;
GO
```

## Create a SQL Server Connector

Create a connector that targets all the tables under `testDB` database and `dbo` schema in SQL Server.
```sql
SELECT 
  synchdb_add_conninfo(
    'sqlserverconn', '127.0.0.1', 1433, 
    'sa', 'Password!', 'testDB', 'dbo', 
    'null', 'null', 'sqlserver');
```

## Initial Snapshot + CDC

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

## Initial Snapshot Only and no CDC

Start the connector using `initial_only` mode will perform the initial snapshot of all designated tables (all in this case) only and will not perform CDC after.

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'initial_only');

```

The connector would still appear to be `polling` from the connector but no change will be captured because Debzium internally has stopped the CDC. You have the option to shut it down. Restarting the connector in `initial_only` mode will not rebuild the tables as they have already been built.


## Capture Table Schema Only + CDC

Start the connector using `no_data` mode will perform the schema capture only, build the corresponding tables in PostgreSQL and it does not replicate existing table data (skip initial snapshot). After the schema capture is completed, the connector goes into CDC mode and will start capture subsequent changes to the tables.

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'no_data');

```

Restarting the connector in `no_data` mode will not rebuild the schema again, and it will resume CDC since the last successful point.

## CDC only

Start the connector using `never` will skip schema capture and initial snapshot entirely and will go to CDC mode to capture subsequent changes. Please note that the connector expects all the capture tables have been created in PostgreSQL prior to starting in `never` mode. If the tables do not exist, the connector will encounter an error when it tries to apply a CDC change to a non-existent table.

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'never');

```

Restarting the connector in `never` mode will resume CDC since the last successful point.

## Always do Initial Snapahot + CDC

Start the connector using `always` mode will always capture the schemas of capture tables, always redo the initial snapshot and then go to CDC. This is similar to a reset button because everything will be rebuilt using this mode. Use it with caution especially when you have large number of tables being captured, which could take a long time to finish. After the rebuild, CDC resumes as normal.

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'always');

```

However, it is possible to select partial tables to redo the initial snapshot by using the `snapshottable` option of the connector. Tables matching the criteria in `snapshottable` will redo the inital snapshot, if not, their initial snapshot will be skipped. If `snapshottable` is null or empty, by default, all the tables specified in `table` option of the connector will redo the initial snapshot under `always` mode.

This example makes the connector only redo the initial snapshot of `inventory.customers` table. All other tables will have their snapshot skipped.
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{snapshottable}', '"testDB.dbo.customers"') 
WHERE name = 'sqlserverconn';
```

After the initial snapshot, CDC will begin. Restarting a connector in `always` mode will repeat the same process described above.