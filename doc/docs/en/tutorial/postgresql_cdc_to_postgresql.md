# Postgres -> Postgres

## Prepare PostgreSQL Database for SynchDB

Before SynchDB can be used to replicate from PotgreSQK, PostgreSQL server needs to be configured according to the procedure outlined [here](../../getting-started/remote_database_setups/)

## Create a PostgreSQL Connector

Create a connector that targets all the tables under database `postgres` and schema `public`.
```sql
SELECT 
  synchdb_add_conninfo(
    'pgconn', '127.0.0.1', 5432, 
    'myuser', 'mypass', 'postgres', 'public', 
    'null', 'null', 'postgres');
```

## Initial Snapshot
"Initial snapshot" (or table snapshot) in SynchDB means to copy table schema plus initial data for all designated tables. This is similar to the term "table sync" in PostgreSQL logical replication. When a connector is started using the default `initial` mode, it will automatically perform the initial snapshot before going to Change Data Capture (CDC) stage. This can be omitted entirely with mode `never` or partially omitted with mode `no_data`. See [here](../../user-guide/start_stop_connector/) for all snapshot options.

Once the initial snapshot is completed, the connector will not do it again upon subsequent restarts and will just resume with CDC since the last incomplete offset. This behavior is controled by the metadata files managed by Debezium engine. See [here](../../architecture/metadata_files/) for more about metadata files.

## Different Connector Launch Modes

### Initial Snapshot + CDC

Start the connector using `initial` mode will perform the initial snapshot of all designated tables (all in this case). After this is completed, the change data capture (CDC) process will begin to stream for new changes.

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
postgres=# select * from synchdb_state_view where name='oracleconn';
  name  | connector_type |  pid   |        stage        |  state  |   err    |
    last_dbz_offset
--------+----------------+--------+---------------------+---------+----------+-------------------------------
-------------------------------------------------------
 pgconn | postgres       | 528746 | change data capture | polling | no error | {"lsn_proc":37051152,"messageType":"INSERT","lsn_commit":36989040,"lsn":3705
1152,"txId":1007,"ts_usec":1767208115470483}

```

This means that the connector is now streaming for new changes of the designated tables. Restarting the connector in `initial` mode will proceed replication since the last successful point and initial snapshot will not be re-run.

### Initial Snapshot Only and no CDC

Start the connector using `initial_only` mode will perform the initial snapshot of all designated tables (all in this case) only and will not perform CDC after.

```sql
SELECT synchdb_start_engine_bgw('pgconn', 'initial_only');

```

The connector would still appear to be `polling` from the connector but no change will be captured because Debzium internally has stopped the CDC. You have the option to shut it down. Restarting the connector in `initial_only` mode will not rebuild the tables as they have already been built.

### Capture Table Schema Only + CDC

Start the connector using `no_data` mode will perform the schema capture only, build the corresponding tables in PostgreSQL and it does not replicate existing table data (skip initial snapshot). After the schema capture is completed, the connector goes into CDC mode and will start capture subsequent changes to the tables.

```sql
SELECT synchdb_start_engine_bgw('pgconn', 'no_data');

```

Restarting the connector in `no_data` mode will not rebuild the schema again, and it will resume CDC since the last successful point.

### Always do Initial Snapahot + CDC

Start the connector using `always` mode will always capture the schemas of capture tables, always redo the initial snapshot and then go to CDC. This is similar to a reset button because everything will be rebuilt using this mode. Use it with caution especially when you have large number of tables being captured, which could take a long time to finish. After the rebuild, CDC resumes as normal.

```sql
SELECT synchdb_start_engine_bgw('pgconn', 'always');

```

However, it is possible to select partial tables to redo the initial snapshot by using the `snapshottable` option of the connector. Tables matching the criteria in `snapshottable` will redo the inital snapshot, if not, their initial snapshot will be skipped. If `snapshottable` is null or empty, by default, all the tables specified in `table` option of the connector will redo the initial snapshot under `always` mode.

This example makes the connector only redo the initial snapshot of `inventory.customers` table. All other tables will have their snapshot skipped.
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{snapshottable}', '"free.customers"') 
WHERE name = 'pgconn';
```

After the initial snapshot, CDC will begin. Restarting a connector in `always` mode will repeat the same process described above.

## Possible Snapshot Modes for Postgres Connector

* initial (default)
* initial_only
* no_data
* always
* schemasync

## Preview Source and Destination Table Relationships with schemasync mode

Before attempting to do an initial snapshot of current table and data, which may be huge, it is possible to "preview" all the tables and data type mappings between source and destination tables before the actual data migration. This gives you an opportunity to modify a data type mapping, or an object name before actual migration happens. This can be done with the special "schemasync" initial snapshot mode. Refer to [object mapping workflow](../../tutorial/object_mapping_workflow/) for a detailed example.