# MySQL -> PostgreSQL

## Prepare MySQL Database for SynchDB

Before SynchDB can be used to replicate from MySQL, MySQL needs to be configured according to the procedure outlined [here](../../getting-started/remote_database_setups/)

## Create a MySQL Connector

Create a connector that targets all the tables under `inventory` database in MySQL.
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', '127.0.0.1', 3306, 'mysqluser', 
    'mysqlpwd', 'inventory', 'null', 
    'null', 'null', 'mysql');
```

## Initial Snapshot + CDC

Start the connector using `initial` mode will perform the initial snapshot of all designated tables (all in this case). After this is completed, the change data capture (CDC) process will begin to stream for new changes.

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'initial');

or 

SELECT synchdb_start_engine_bgw('mysqlconn');
```

The stage of this connector should be in `initial snapshot` the first time it runs:
```sql
postgres=# select * from synchdb_state_view;
    name    | connector_type |  pid   |      stage       |  state  |   err    |                      last_dbz_offs
et
------------+----------------+--------+------------------+---------+----------+-----------------------------------
-------------------------
 mysqlconn  | mysql          | 522195 | initial snapshot | polling | no error | {"ts_sec":1750375008,"file":"mysql
-bin.000003","pos":1500}
(1 row)

```

A new schema called `inventory` will be created and all tables streamed by the connector will be replicated under that schema.
```sql
postgres=# set search_path=inventory;
SET
postgres=# \d
                    List of relations
  Schema   |          Name           |   Type   | Owner
-----------+-------------------------+----------+--------
 inventory | addresses               | table    | ubuntu
 inventory | addresses_id_seq        | sequence | ubuntu
 inventory | customers               | table    | ubuntu
 inventory | customers_id_seq        | sequence | ubuntu
 inventory | geom                    | table    | ubuntu
 inventory | geom_id_seq             | sequence | ubuntu
 inventory | orders                  | table    | ubuntu
 inventory | orders_order_number_seq | sequence | ubuntu
 inventory | products                | table    | ubuntu
 inventory | products_id_seq         | sequence | ubuntu
 inventory | products_on_hand        | table    | ubuntu

```

After the initial snapshot is completed, and at least one subsequent changes is received and processed, the connector stage shall change from `initial snapshot` to `Change Data Capture`.
```sql
postgres=# select * from synchdb_state_view;
    name    | connector_type |  pid   |        stage        |  state  |   err    |                      last_dbz_o
ffset
------------+----------------+--------+---------------------+---------+----------+--------------------------------
----------------------------
 mysqlconn  | mysql          | 522195 | change data capture | polling | no error | {"ts_sec":1750375008,"file":"my
sql-bin.000003","pos":1500}

```

This means that the connector is now streaming for new changes of the designated tables. Restarting the connector in `initial` mode will proceed replication since the last successful point and initial snapshot will not be re-run.

## Initial Snapshot Only and no CDC

Start the connector using `initial_only` mode will perform the initial snapshot of all designated tables (all in this case) only and will not perform CDC after.

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'initial_only');

```

The connector would still appear to be `polling` from the connector but no change will be captured because Debzium internally has stopped the CDC. You have the option to shut it down. Restarting the connector in `initial_only` mode will not rebuild the tables as they have already been built.

```sql
postgres=# select * from synchdb_state_view;
    name    | connector_type |  pid   |      stage       |  state  |   err    |       last_dbz_offset
------------+----------------+--------+------------------+---------+----------+-----------------------------
 mysqlconn  | mysql          | 522330 | initial snapshot | polling | no error | offset file not flushed yet

```

## Capture Table Schema Only + CDC

Start the connector using `no_data` mode will perform the schema capture only, build the corresponding tables in PostgreSQL and it does not replicate existing table data (skip initial snapshot). After the schema capture is completed, the connector goes into CDC mode and will start capture subsequent changes to the tables.

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'no_data');

```

Restarting the connector in `no_data` mode will not rebuild the schema again, and it will resume CDC since the last successful point.

## CDC only

Start the connector using `never` will skip schema capture and initial snapshot entirely and will go to CDC mode to capture subsequent changes. Please note that the connector expects all the capture tables have been created in PostgreSQL prior to starting in `never` mode. If the tables do not exist, the connector will encounter an error when it tries to apply a CDC change to a non-existent table.

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'never');

```

Restarting the connector in `never` mode will resume CDC since the last successful point.

## Always do Initial Snapahot + CDC

Start the connector using `always` mode will always capture the schemas of capture tables, always redo the initial snapshot and then go to CDC. This is similar to a reset button because everything will be rebuilt using this mode. Use it with caution especially when you have large number of tables being captured, which could take a long time to finish. After the rebuild, CDC resumes as normal.

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'always');

```

However, it is possible to select partial tables to redo the initial snapshot by using the `snapshottable` option of the connector. Tables matching the criteria in `snapshottable` will redo the inital snapshot, if not, their initial snapshot will be skipped. If `snapshottable` is null or empty, by default, all the tables specified in `table` option of the connector will redo the initial snapshot under `always` mode.

This example makes the connector only redo the initial snapshot of `inventory.customers` table. All other tables will have their snapshot skipped.
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{snapshottable}', '"inventory.customers"') 
WHERE name = 'mysqlconn';
```

After the initial snapshot, CDC will begin. Restarting a connector in `always` mode will repeat the same process described above.