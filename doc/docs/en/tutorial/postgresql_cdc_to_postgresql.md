# Postgres Connector

## **Prepare PostgreSQL Database for SynchDB**

Before SynchDB can be used to replicate from PotgreSQL, PostgreSQL server needs to be configured according to the procedure outlined [here](../../getting-started/remote_database_setups/)

## **Create a PostgreSQL Connector**

Create a connector that targets all the tables under database `postgres` and schema `public`.
```sql
SELECT 
  synchdb_add_conninfo(
    'pgconn', '127.0.0.1', 5432, 
    'myuser', 'mypass', 'postgres', 'public', 
    'null', 'null', 'postgres');
```

## **Initial Snapshot**
"Initial snapshot" (or table snapshot) in SynchDB means to copy table schema plus initial data for all designated tables. This is similar to the term "table sync" in PostgreSQL logical replication. When a connector is started using the default `initial` mode, it will automatically perform the initial snapshot before going to Change Data Capture (CDC) stage. This can be partially omitted with mode `no_data`. See [here](../../user-guide/start_stop_connector/) for all snapshot options.

Once the initial snapshot is completed, the connector will not do it again upon subsequent restarts and will just resume with CDC since the last incomplete offset. This behavior is controled by the metadata files managed by Debezium engine. See [here](../../architecture/metadata_files/) for more about metadata files.

PostgreSQL connector's initial snapshot is a little different. Debezium engine does not build the initial table schema like other connectors do. This is because PostgreSQL does not explicitly emits DDL WAL events. PostgreSQL's native logical replication also behaves the same. The user must pre-create the table schema at the destination before launching logical replication. So, when launching the Debezium based PostgreSQL connector for the first time, it assumes you have already created the designated table schemas and their initial data, and would enter CDC streaming mode immediately, without actually doing initial snapshot.

This can be remedied by FDW based initial snapshot, which uses `postgres_fdw` to build the initial table schema and data, before transitioning to CDC streaming via Debezium. To use it, you must set `synchdb.olr_snapshot_engine` to `fdw` before launching the connector. 


## **Different Connector Launch Modes**

### **Initial Snapshot + CDC**

**with synchdb.olr_snapshot_engine = 'debezium':**

You are expected to create the initial table schema and data at the destination database. Debezium will not create them.

**with synchdb.olr_snapshot_engine = 'fdw':**

Start the connector using `initial` mode will perform the initial snapshot of all designated tables (via postgres_fdw). After this is completed, the change data capture (CDC) process will begin to stream for new changes (via Debezium).

```sql
SELECT synchdb_start_engine_bgw('pgconn', 'initial');

or 

SELECT synchdb_start_engine_bgw('pgconn');
```

The stage of this connector should be in `initial snapshot` the first time it runs:
```sql
postgres=# select * from synchdb_state_view where name='oracleconn';
  name  | connector_type |  pid   |      stage       |  state  |   err    |       last_dbz_offset
--------+----------------+--------+------------------+---------+----------+-----------------------------
 pgconn | postgres       | 528746 | initial snapshot | polling | no error | offset file not flushed yet

```

A new schema called `postgres` will be created and all tables streamed by the connector will be replicated under that schema.
```sql
postgres=# set search_path=postgres;
SET
postgres=# \d
              List of relations
  Schema  |        Name        | Type  | Owner
----------+--------------------+-------+--------
 postgres | orders             | table | ubuntu

```

After the initial snapshot is completed, and at least one subsequent changes is received and processed, the connector stage shall change from `initial snapshot` to `Change Data Capture`.
```sql
postgres=# select * from synchdb_state_view where name='pgconn';
  name  | connector_type |   pid   |        stage        |  state  |   err    |
       last_dbz_offset
--------+----------------+---------+---------------------+---------+----------+-----------------------------------
-----------------------------------------------------------------
 pgconn | postgres       | 1604388 | change data capture | polling | no error | {"lsn_proc":37396384,"messageType"
:"INSERT","lsn":37396384,"txId":1015,"ts_usec":1767740340957961}

```

This means that the connector is now streaming for new changes of the designated tables. Restarting the connector in `initial` mode will proceed replication since the last successful point and initial snapshot will not be re-run.

### **Initial Snapshot Only and no CDC**

**with synchdb.olr_snapshot_engine = 'debezium':**

You are expected to create the initial table schema and data at the destination database. Debezium will not create them.

**with synchdb.olr_snapshot_engine = 'fdw':**

Start the connector using `initial_only` mode will perform the initial snapshot of all designated tables (all in this case) only and will not perform CDC after.

```sql
SELECT synchdb_start_engine_bgw('pgconn', 'initial_only');

```

The connector would still appear to be `polling` from the connector but no change will be captured because Debzium internally has stopped the CDC. You have the option to shut it down. Restarting the connector in `initial_only` mode will not rebuild the tables as they have already been built.

### **Capture Table Schema Only + CDC**

**with synchdb.olr_snapshot_engine = 'debezium':**

You are expected to create the initial table schema and data at the destination database. Debezium will not create them.

**with synchdb.olr_snapshot_engine = 'fdw':**

Start the connector using `no_data` mode will perform the schema capture only, build the corresponding tables in PostgreSQL and it does not replicate existing table data (skip initial snapshot). After the schema capture is completed, the connector goes into CDC mode and will start capture subsequent changes to the tables.

```sql
SELECT synchdb_start_engine_bgw('pgconn', 'no_data');

```

Restarting the connector in `no_data` mode will not rebuild the schema again, and it will resume CDC since the last successful point.

### **Always do Initial Snapahot + CDC**

**with synchdb.olr_snapshot_engine = 'debezium':**

You are expected to create the initial table schema and data at the destination database. Debezium will not create them.

**with synchdb.olr_snapshot_engine = 'fdw':**

Start the connector using `always` mode will always capture the schemas of capture tables, always redo the initial snapshot and then go to CDC. This is similar to a reset button because everything will be rebuilt using this mode. Use it with caution especially when you have large number of tables being captured, which could take a long time to finish. After the rebuild, CDC resumes as normal.

```sql
SELECT synchdb_start_engine_bgw('pgconn', 'always');

```

However, it is possible to select partial tables to redo the initial snapshot by using the `snapshottable` option of the connector. Tables matching the criteria in `snapshottable` will redo the inital snapshot, if not, their initial snapshot will be skipped. If `snapshottable` is null or empty, by default, all the tables specified in `table` option of the connector will redo the initial snapshot under `always` mode.

This example makes the connector only redo the initial snapshot of `inventory.customers` table. All other tables will have their snapshot skipped.
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{snapshottable}', '"public.customers"') 
WHERE name = 'pgconn';
```

After the initial snapshot, CDC will begin. Restarting a connector in `always` mode will repeat the same process described above.

## **Possible Snapshot Modes for Postgres Connector**

* initial (default)
* initial_only
* no_data
* always
* schemasync

## **Preview Source and Destination Table Relationships with schemasync mode**

Before attempting to do an initial snapshot of current table and data, which may be huge, it is possible to "preview" all the tables and data type mappings between source and destination tables before the actual data migration. This gives you an opportunity to modify a data type mapping, or an object name before actual migration happens. This can be done with the special "schemasync" initial snapshot mode.

Please note that you must set `synchdb.olr_snapshot_engine` to 'fdw' in order to use `schemasync` mode to preview the tables.

### **Create a Connector and Start it in `schemasync` Mode**

`schemasync` is a special mode that makes the connector connects to remote database and attempt to sync only the schema of designated tables. After this is done, the connector is put to `paused` state and user is able to review all the tables and data types created using the default rules and make change if needed.

```sql
SELECT synchdb_add_conninfo(
    'pgconn', 
    '127.0.0.1', 
    5433, 
    'pguser', 
    'pgpass', 
    'postgres', 
    'public', 
    'null', 
    'null', 
    'postgres'
);

SELECT synchdb_start_engine_bgw('pgconn', 'schemasync');
```

### **Ensure the connector is put to paused state**

```sql
SELECT name, connector_type, pid, stage, state FROM synchdb_state_view WHERE name = 'pgconn';;
  name  | connector_type |   pid   |        stage        | state
--------+----------------+---------+---------------------+--------
 pgconn | postgres       | 1643157 | change data capture | paused

```

### **Review the Tables Created by Default Mapping Rules**

```sql
SELECT * FROM synchdb_att_view WHERE name = 'pgconn';
  name  |   type   | attnum |       ext_tbname        |    pg_tbname     | ext_attname | pg_attname |  ext_atttypename  |  pg_atttypename   | transform
--------+----------+--------+-------------------------+------------------+-------------+------------+-------------------+-------------------+-----------
 pgconn | postgres |      1 | postgres.public.mytble  | postgres.mytble  | a           | a          | numeric           | numeric           |
 pgconn | postgres |      2 | postgres.public.mytble  | postgres.mytble  | b           | b          | numeric           | numeric           |
 pgconn | postgres |      3 | postgres.public.mytble  | postgres.mytble  | c           | c          | numeric           | numeric           |
 pgconn | postgres |      1 | postgres.public.testing | postgres.testing | a           | a          | integer           | integer           |
 pgconn | postgres |      2 | postgres.public.testing | postgres.testing | b           | b          | text              | text              |
 pgconn | postgres |      3 | postgres.public.testing | postgres.testing | c           | c          | character varying | character varying |
 pgconn | postgres |      4 | postgres.public.testing | postgres.testing | d           | d          | bigint            | bigint            |
 pgconn | postgres |      1 | postgres.public.xyz     | postgres.xyz     | bbb         | bbb        | character varying | character varying |
 pgconn | postgres |      2 | postgres.public.xyz     | postgres.xyz     | ccc         | ccc        | bytea             | bytea             |
 pgconn | postgres |      3 | postgres.public.xyz     | postgres.xyz     | ddd         | ddd        | numeric           | numeric           |
 pgconn | postgres |      4 | postgres.public.xyz     | postgres.xyz     | eee         | eee        | numeric           | numeric           |
 pgconn | postgres |      5 | postgres.public.xyz     | postgres.xyz     | fff         | fff        | numeric           | numeric           |
 pgconn | postgres |      6 | postgres.public.xyz     | postgres.xyz     | ggg         | ggg        | bigint            | bigint            |
 pgconn | postgres |      7 | postgres.public.xyz     | postgres.xyz     | aaa         | aaa        | integer           | integer           |


```

### **Define Custom Mapping Rules (If Needed)**

User can use `synchdb_add_objmap` function to create custom mapping rules. It can be used to map table name, column name, data types and defines a data transform expression rule

```sql
SELECT synchdb_add_objmap('pgconn','table','postgres.public.mytble','postgres.thetable');
SELECT synchdb_add_objmap('pgconn','column','postgres.public.testing.c','ccc');
SELECT synchdb_add_objmap('pgconn','datatype','postgres.public.xyz.ggg','int|0');
SELECT synchdb_add_objmap('pgconn','transform','postgres.public.xyz.bbb','''>>>>>'' || ''%d'' || ''<<<<<''');
```

The above means:

* source table 'postgres.public.mytble' will be mapped to 'postgres.thetable' in destination
* source column 'postgres.public.testing.c'will be mapped to 'ccc' in destination
* source data type for column 'postgres.public.xyz.ggg' will be mapped to 'int'
* source column data 'postgres.public.xyz.bbb' will be transformed accoring to the expression where %d is the data placeholder

### **Review All Object Mapping Rules Created So Far**

```sql
SELECT * FROM synchdb_objmap WHERE name = 'pgconn';
  name  |  objtype  | enabled |          srcobj           |           dstobj
--------+-----------+---------+---------------------------+----------------------------
 pgconn | table     | t       | postgres.public.mytble    | postgres.thetable
 pgconn | column    | t       | postgres.public.testing.c | ccc
 pgconn | datatype  | t       | postgres.public.xyz.ggg   | int|0
 pgconn | transform | t       | postgres.public.xyz.bbb   | '>>>>>' || '%d' || '<<<<<'


```

### **Reload the Object Mapping Rules**

Once all custom rules have been defined, we need to signal the connector to load them. This will cause the connector to read and apply the object mapping rules. If it sees a discrepancy between current PostgreSQL values and the object mapping values, it will attempt to correct the mapping.

```sql
SELECT synchdb_reload_objmap('pgconn');

```

### **Review `synchdb_att_view` Again for Changes**

```sql
SELECT * from synchdb_att_view WHERE name = 'pgconn';;
  name  |   type   | attnum |       ext_tbname        |     pg_tbname     | ext_attname | pg_attname |  ext_atttypename  |  pg_atttypename   |         transform
--------+----------+--------+-------------------------+-------------------+-------------+------------+-------------------+-------------------+----------------------------
 pgconn | postgres |      1 | postgres.public.mytble  | postgres.thetable | a           | a          | numeric           | numeric           |
 pgconn | postgres |      2 | postgres.public.mytble  | postgres.thetable | b           | b          | numeric           | numeric           |
 pgconn | postgres |      3 | postgres.public.mytble  | postgres.thetable | c           | c          | numeric           | numeric           |
 pgconn | postgres |      1 | postgres.public.testing | postgres.testing  | a           | a          | integer           | integer           |
 pgconn | postgres |      2 | postgres.public.testing | postgres.testing  | b           | b          | text              | text              |
 pgconn | postgres |      3 | postgres.public.testing | postgres.testing  | c           | ccc        | character varying | character varying |
 pgconn | postgres |      4 | postgres.public.testing | postgres.testing  | d           | d          | bigint            | bigint            |
 pgconn | postgres |      1 | postgres.public.xyz     | postgres.xyz      | bbb         | bbb        | character varying | character varying | '>>>>>' || '%d' || '<<<<<'
 pgconn | postgres |      2 | postgres.public.xyz     | postgres.xyz      | ccc         | ccc        | bytea             | bytea             |
 pgconn | postgres |      3 | postgres.public.xyz     | postgres.xyz      | ddd         | ddd        | numeric           | numeric           |
 pgconn | postgres |      4 | postgres.public.xyz     | postgres.xyz      | eee         | eee        | numeric           | numeric           |
 pgconn | postgres |      5 | postgres.public.xyz     | postgres.xyz      | fff         | fff        | numeric           | numeric           |
 pgconn | postgres |      6 | postgres.public.xyz     | postgres.xyz      | ggg         | ggg        | bigint            | integer           |
 pgconn | postgres |      7 | postgres.public.xyz     | postgres.xyz      | aaa         | aaa        | integer           | integer           |

```

### **Resume the Connector or Redo the Entire Snapshot**

Once the object mappings have been confirmed correct, we can resume the connector. Please note that, resume will proceed to streaming only the new table changes. The existing data of the tables will not be copied.

```sql
SELECT synchdb_resume_engine('pgconn');
```

To capture the table's existing data, we can also redo the entire snapshot with the new object mapping rules.

```sql
SELECT synchdb_stop_engine_bgw('pgconn');
SELECT synchdb_start_engine_bgw('pgconn', 'always');
```

## **Selective Table Sync**

### **Select Desired Tables and Start it for the First Time**

Table selection is done during connector creation phase via `synchdb_add_conninfo()` where we specify a list of tables (expressed in FQN, separated by a comma) to replicate from.

For example, the following command creates a connector that only replicates change from `public.orders` tables from remote PostgreSQL database.
```sql
SELECT synchdb_add_conninfo(
    'pgconn', 
    '127.0.0.1', 
    5433, 
    'pguser', 
    'pgpass', 
    'postgres', 
    'public', 
    'public.orders', 
    'null', 
    'postgres'
);
```

Starting this connector for the very first time will trigger an initial snapshot being performed and selected tables' schema and data will be replicated.

```sql
SELECT synchdb_start_engine_bgw('mysqlconn');
```

### **Verify the Connector State and Tables**

Examine the connector state and the new tables:
```sql
postgres=# Select name, state, err from synchdb_state_view;
  name  |  state  |   err
--------+---------+----------
 pgconn | polling | no error


postgres=# \dt postgres.*
           List of tables
  Schema  |  Name  | Type  | Owner
----------+--------+-------+--------
 postgres | orders | table | pguser

```

Once the snapshot is complete, the connector will continue capturing subsequent changes to the table.

### **Add More Tables to Replicate During Run Time.**

The `mysqlconn` from previous section has already completed the initial snapshot and obtained the table schemas of the selected table. If we would like to add more tables to replicate from, we will need to notify the Debezium engine about the updated table section and perform the initial snapshot again. Here's how it is done:

1. Update the `synchdb_conninfo` table to include additional tables.
2. In this example, we add the `inventory.customers` table to the sync list:
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{table}', '"public.orders,public.customers"') 
WHERE name = 'pgconn';
```
3. Configure the snapshot table parameter to include only the new table `inventory.customers` to that SynchDB does not try to rebuild the 2 tables that have already finished the snapshot.
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{snapshottable}', '"public.customers"') 
WHERE name = 'pgconn';
``` 
4. Restart the connector with the snapshot mode set to `always` to perform another initial snapshot:
```sql
SELECT synchdb_stop_engine_bgw('pgconn');
SELECT synchdb_start_engine_bgw('pgconn', 'always');
```
**<<IMPORTANT>>** Please note that we do not use `synchdb_restart_connector` to restart the connector here because this function is mostly designed for restarting Debezium engine with a different snapshot mode. Since Postgres connector uses FDW instead of Debezium to build the initial tables, we have to explicity do a `stop engine`, followed by `start engine` calls to trigger FDW routines to run again. 

### **Verify the Updated Tables**

Now, we can examine our tables again:
```sql
postgres=# \dt inventory.*
             List of tables
  Schema   |   Name    | Type  | Owner
-----------+-----------+-------+--------
 inventory | customers | table | ubuntu
 inventory | orders    | table | ubuntu
 inventory | products  | table | ubuntu

```