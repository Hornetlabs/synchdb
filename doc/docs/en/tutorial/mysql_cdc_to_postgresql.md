# MySQL -> PostgreSQL

## **Prepare MySQL Database for SynchDB**

Before SynchDB can be used to replicate from MySQL, MySQL needs to be configured according to the procedure outlined [here](../../getting-started/remote_database_setups/)

## **Create a MySQL Connector**

Create a connector that targets all the tables under `inventory` database in MySQL.
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', '127.0.0.1', 3306, 'mysqluser', 
    'mysqlpwd', 'inventory', 'null', 
    'null', 'null', 'mysql');
```
## **Initial Snapshot**
"Initial snapshot" (or table snapshot) in SynchDB means to copy table schema plus initial data for all designated tables. This is similar to the term "table sync" in PostgreSQL logical replication. When a connector is started using the default `initial` mode, it will automatically perform the initial snapshot before going to Change Data Capture (CDC) stage. This can be omitted entirely with mode `never` or partially omitted with mode `no_data`. See [here](../../user-guide/start_stop_connector/) for all snapshot options.

Once the initial snapshot is completed, the connector will not do it again upon subsequent restarts and will just resume with CDC since the last incomplete offset. This behavior is controled by the metadata files managed by Debezium engine. See [here](../../architecture/metadata_files/) for more about metadata files.

## **Different Connector Launch Modes**

### **Initial Snapshot + CDC**

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

### **Initial Snapshot Only and no CDC**

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

### **Capture Table Schema Only + CDC**

Start the connector using `no_data` mode will perform the schema capture only, build the corresponding tables in PostgreSQL and it does not replicate existing table data (skip initial snapshot). After the schema capture is completed, the connector goes into CDC mode and will start capture subsequent changes to the tables.

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'no_data');

```

Restarting the connector in `no_data` mode will not rebuild the schema again, and it will resume CDC since the last successful point.

### **CDC only**

Start the connector using `never` will skip schema capture and initial snapshot entirely and will go to CDC mode to capture subsequent changes. Please note that the connector expects all the capture tables have been created in PostgreSQL prior to starting in `never` mode. If the tables do not exist, the connector will encounter an error when it tries to apply a CDC change to a non-existent table.

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'never');

```

Restarting the connector in `never` mode will resume CDC since the last successful point.

### **Always do Initial Snapahot + CDC**

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

## **Possible Snapshot Modes for MySQL Connector**

* initial (default)
* initial_only
* no_data
* never
* always
* schemasync

## **Preview Source and Destination Table Relationships with schemasync mode**

Before attempting to do an initial snapshot of current table and data, which may be huge, it is possible to "preview" all the tables and data type mappings between source and destination tables before the actual data migration. This gives you an opportunity to modify a data type mapping, or an object name before actual migration happens. This can be done with the special "schemasync" initial snapshot mode. Refer to [object mapping workflow](../../tutorial/object_mapping_workflow/) for a detailed example.

## **Selective Table Sync**

### **Select Desired Tables and Start it for the First Time**

Table selection is done during connector creation phase via `synchdb_add_conninfo()` where we specify a list of tables (expressed in FQN, separated by a comma) to replicate from.

For example, the following command creates a connector that only replicates change from `inventory.orders` and `inventory.products` tables from remote MySQL database.
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', 
    '127.0.0.1', 
    3306, 
    'mysqluser', 
    'mysqlpwd', 
    'inventory', 
    'null', 
    'inventory.orders,inventory.products', 
    'null', 
    'mysql'
);
```

Starting this connector for the very first time will trigger an initial snapshot being performed and selected 2 tables' schema and data will be replicated.

```sql
SELECT synchdb_start_engine_bgw('mysqlconn');
```

### **Verify the Connector State and Tables**

Examine the connector state and the new tables:
```sql
postgres=# Select name, state, err from synchdb_state_view;
     name      |  state  |   err
---------------+---------+----------
 mysqlconn     | polling | no error
(1 row)

postgres=# \dt inventory.*
            List of tables
  Schema   |   Name   | Type  | Owner
-----------+----------+-------+--------
 inventory | orders   | table | ubuntu
 inventory | products | table | ubuntu

```

Once the snapshot is complete, the `mysqlconn` connector will continue capturing subsequent changes to the `inventory.orders` and `inventory.products` tables.

### **Add More Tables to Replicate During Run Time.**

The `mysqlconn` from previous section has already completed the initial snapshot and obtained the table schemas of the selected table. If we would like to add more tables to replicate from, we will need to notify the Debezium engine about the updated table section and perform the initial snapshot again. Here's how it is done:

1. Update the `synchdb_conninfo` table to include additional tables.
2. In this example, we add the `inventory.customers` table to the sync list:
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{table}', '"inventory.orders,inventory.products,inventory.customers"') 
WHERE name = 'mysqlconn';
```
3. Configure the snapshot table parameter to include only the new table `inventory.customers` to that SynchDB does not try to rebuild the 2 tables that have already finished the snapshot.
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{snapshottable}', '"inventory.customers"') 
WHERE name = 'mysqlconn';
``` 
4. Restart the connector with the snapshot mode set to `always` to perform another initial snapshot:
```sql
SELECT synchdb_restart_connector('mysqlconn', 'always');
```
This forces Debezium to re-snapshot only the new table `inventory.customers` while leaving the old tables `inventory.orders` and `inventory.products` untouched. The CDC for all tables will resume once snapshot is complete. 


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
