# Native Openlog Replicator Connector

## **Prepare MySQL Database for SynchDB**

Before SynchDB can be used to replicate from Native Openlog Replicator (OLR) Connector, Both OLR and Oracle database itself need to be configured according to the procedure outlined [here](../../getting-started/remote_database_setups/)

## **Behavior Notes for Native based Openlog Replicator Connector**

* SynchDB manages connections to Openlog Replicator and streams changes without using Debezium.
* Requires OLR configuration or connector will error on startup.
* Relies on Debezium's oracle connector to complete initial snapshot and shuts down when done, subsequent CDC is done natively within SynchDB against Openlog Replicator.
* Relies on IvorySQL's oracle parser to handle DDL events. This must be compiled and installed prior to using native openlog replicator connector.
* Visit [here](https://github.com/bersler/OpenLogReplicator) for more information about Openlog Replicator.

## **Create a Native Openlog Replicator Connector**

Create a connector that targets all the tables under `FREE` database and schema `DBZUSER` via native Openlog Replicator Connector.
```sql
SELECT synchdb_add_conninfo(
    'olrconn', '127.0.0.1', 1521, 'DBZUSER', 
    'dbz', 'FREE', 'DBZUSER', 
    'null', 'null', 'olr');

SELECT synchdb_add_olr_conninfo(
	'olrconn',
	'127.0.0.1',
	7070,
	'ORACLE');

```

## **Initial Snapshot**
"Initial snapshot" (or table snapshot) in SynchDB means to copy table schema plus initial data for all designated tables. This is similar to the term "table sync" in PostgreSQL logical replication. When a connector is started using the default `initial` mode, it will automatically perform the initial snapshot before going to Change Data Capture (CDC) stage. This can be partially omitted with mode `no_data`. See [here](../../user-guide/start_stop_connector/) for all snapshot options.

Once the initial snapshot is completed, the connector will not do it again upon subsequent restarts and will just resume with CDC since the last incomplete offset. This behavior is controled by the metadata files managed by Debezium engine. See [here](../../architecture/metadata_files/) for more about metadata files.

## **Different Connector Launch Modes**

### **Initial Snapshot + CDC**

Start the connector using `initial` mode will perform the initial snapshot of all designated tables (all in this case). After this is completed, the change data capture (CDC) process will begin to stream for new changes.

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'initial');

or 

SELECT synchdb_start_engine_bgw('olrconn');
```

The stage of this connector should be in `initial snapshot` or `change data capture` if it has processed at least one change event in CDC

```sql
postgres=# select * from synchdb_state_view;
  name   | connector_type |   pid   |       stage      |  state  |   err    | last_dbz_offset
---------+----------------+---------+------------------+---------+----------+-----------------
 olrconn | olr            | 1702522 | initial snapshot | polling | no error | no offset



```

A new schema called `free` will be created and all tables streamed by the connector will be replicated under that schema.
```sql
postgres=# \dt free.*
           List of tables
 Schema |   Name    | Type  | Owner
--------+-----------+-------+--------
 free   | customers | table | ubuntu
 free   | orders    | table | ubuntu

```

After the initial snapshot is completed, and at least one subsequent changes is received and processed, the connector stage shall change from `initial snapshot` to `Change Data Capture`.
```sql
postgres=# select * from synchdb_state_view;
  name   | connector_type |   pid   |        stage        |  state  |   err    |               last_dbz_offset
---------+----------------+---------+---------------------+---------+----------+---------------------------------------------
 olrconn | olr            | 1702522 | change data capture | polling | no error | {"scn":5031082, "c_scn":5031085, "c_idx":3}

```

This means that the connector is now streaming for new changes of the designated tables. Restarting the connector in `initial` mode will proceed replication since the last successful point and initial snapshot will not be re-run.

### **Initial Snapshot Only and no CDC**

Start the connector using `initial_only` mode will perform the initial snapshot of all designated tables (all in this case) only and will not perform CDC after.

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'initial_only');

```

### **Capture Table Schema Only + CDC**

Start the connector using `no_data` mode will perform the schema capture only, build the corresponding tables in PostgreSQL and it does not replicate existing table data (skip initial snapshot). After the schema capture is completed, the connector goes into CDC mode and will start capture subsequent changes to the tables.

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'no_data');

```

Restarting the connector in `no_data` mode will not rebuild the schema again, and it will resume CDC since the last successful point.

### **CDC only**

Start the connector using `never` will skip schema capture and initial snapshot entirely and will go to CDC mode to capture subsequent changes. Please note that the connector expects all the capture tables have been created in PostgreSQL prior to starting in `never` mode. If the tables do not exist, the connector will encounter an error when it tries to apply a CDC change to a non-existent table.

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'never');

```

Restarting the connector in `never` mode will resume CDC since the last successful point.

### **Always do Initial Snapahot + CDC**

Start the connector using `always` mode will always capture the schemas of capture tables, always redo the initial snapshot and then go to CDC. This is similar to a reset button because everything will be rebuilt using this mode. Use it with caution especially when you have large number of tables being captured, which could take a long time to finish. After the rebuild, CDC resumes as normal.

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'always');

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

Before attempting to do an initial snapshot of current table and data, which may be huge, it is possible to "preview" all the tables and data type mappings between source and destination tables before the actual data migration. This gives you an opportunity to modify a data type mapping, or an object name before actual migration happens. This can be done with the special "schemasync" initial snapshot mode.

Please note that you must set `synchdb.olr_snapshot_engine` to 'fdw' in order to use `schemasync` mode to preview the tables.

### **Create a Connector and Start it in `schemasync` Mode**

`schemasync` is a special mode that makes the connector connects to remote database and attempt to sync only the schema of designated tables. After this is done, the connector is put to `paused` state and user is able to review all the tables and data types created using the default rules and make change if needed.

```sql
SELECT synchdb_add_conninfo(
    'oracleconn', 
    '127.0.0.1', 
    1521, 
    'DBZUSER', 
    'dbz', 
    'FREE', 
    'DBZUSER', 
    'null', 
    'null', 
    'oracle'
);

SELECT synchdb_add_olr_conninfo(
	'olrconn',
	'127.0.0.1',
	7070,
	'ORACLE');

SELECT synchdb_start_engine_bgw('olrconn', 'schemasync');
```

### **Ensure the Connector is Put to Paused State**

```sql
SELECT name, connector_type, pid, stage, state FROM synchdb_state_view WHERE name = 'olrconn';
  name   | connector_type |   pid   |        stage        | state
---------+----------------+---------+---------------------+--------
 olrconn | olr            | 1703430 | change data capture | paused

```

### **Review the Tables Created by Default Mapping Rules**

```sql
SELECT * FROM synchdb_att_view WHERE name = 'olrconn';
  name   |  type  | attnum |       ext_tbname       |   pg_tbname    | ext_attname  |  pg_attname  | ext_atttypename |       pg_atttypename        | transform
---------+--------+--------+------------------------+----------------+--------------+--------------+-----------------+-----------------------------+-----------
 olrconn | oracle |      1 | FREE.DBZUSER.CUSTOMERS | free.customers | ID           | id           | number          | numeric                     |
 olrconn | oracle |      2 | FREE.DBZUSER.CUSTOMERS | free.customers | NAME         | name         | varchar2        | character varying           |
 olrconn | oracle |      1 | FREE.DBZUSER.ORDERS    | free.orders    | ORDER_NUMBER | order_number | number          | numeric                     |
 olrconn | oracle |      2 | FREE.DBZUSER.ORDERS    | free.orders    | ORDER_DATE   | order_date   | date            | timestamp without time zone |
 olrconn | oracle |      3 | FREE.DBZUSER.ORDERS    | free.orders    | PURCHASER    | purchaser    | number          | numeric                     |
 olrconn | oracle |      4 | FREE.DBZUSER.ORDERS    | free.orders    | QUANTITY     | quantity     | number          | numeric                     |
 olrconn | oracle |      5 | FREE.DBZUSER.ORDERS    | free.orders    | PRODUCT_ID   | product_id   | number          | numeric                     |

```

### **Define Custom Mapping Rules (If Needed)**

User can use `synchdb_add_objmap` function to create custom mapping rules. It can be used to map table name, column name, data types and defines a data transform expression rule

```sql
SELECT synchdb_add_objmap('olrconn','table','FREE.DBZUSER.ORDERS','free.myorders');
SELECT synchdb_add_objmap('olrconn','column','FREE.DBZUSER.ORDERS.PURCHASER','who');
SELECT synchdb_add_objmap('olrconn','datatype','FREE.DBZUSER.ORDERS.QUANTITY','bigint|0');
SELECT synchdb_add_objmap('olrconn','transform','FREE.DBZUSER.CUSTOMERS.NAME','''>>>>>'' || ''%d'' || ''<<<<<''');
```
The above means:

* source table 'FREE.DBZUSER.ORDERS' will be mapped to 'free.myorders' in destination
* source column 'FREE.DBZUSER.ORDERS.PURCHASE' will be mapped to 'who' in destination
* source data type for column 'FREE.DBZUSER.ORDERS.QUANTITY' will be mapped to 'bigint'
* source column data 'FREE.DBZUSER.CUSTOMERS.NAME' will be transformed accoring to the expression where %d is the data placeholder

### **Review All Object Mapping Rules Created So Far**

```sql
SELECT * FROM synchdb_objmap WHERE name = 'olrconn';
  name   |  objtype  | enabled |            srcobj             |           dstobj
---------+-----------+---------+-------------------------------+----------------------------
 olrconn | table     | t       | FREE.DBZUSER.ORDERS           | free.myorders
 olrconn | column    | t       | FREE.DBZUSER.ORDERS.PURCHASER | who
 olrconn | datatype  | t       | FREE.DBZUSER.ORDERS.QUANTITY  | bigint|0
 olrconn | transform | t       | FREE.DBZUSER.CUSTOMERS.NAME   | '>>>>>' || '%d' || '<<<<<'

```

### **Reload the Object Mapping Rules**

Once all custom rules have been defined, we need to signal the connector to load them. This will cause the connector to read and apply the object mapping rules. If it sees a discrepancy between current PostgreSQL values and the object mapping values, it will attempt to correct the mapping.

```sql
SELECT synchdb_reload_objmap('olrconn');

```

### **Review `synchdb_att_view` Again for Changes**

```sql
SELECT * from synchdb_att_view WHERE name = 'olrconn';
  name   | type | attnum |       ext_tbname       |   pg_tbname    | ext_attname  |  pg_attname  | ext_atttypename |       pg_atttypename        |         transform
---------+------+--------+------------------------+----------------+--------------+--------------+-----------------+-----------------------------+----------------------------
 olrconn | olr  |      1 | FREE.DBZUSER.CUSTOMERS | free.customers | ID           | id           | number          | numeric                     |
 olrconn | olr  |      2 | FREE.DBZUSER.CUSTOMERS | free.customers | NAME         | name         | varchar2        | character varying           | '>>>>>' || '%d' || '<<<<<'
 olrconn | olr  |      1 | FREE.DBZUSER.ORDERS    | free.myorders  | ORDER_NUMBER | order_number | number          | numeric                     |
 olrconn | olr  |      2 | FREE.DBZUSER.ORDERS    | free.myorders  | ORDER_DATE   | order_date   | date            | timestamp without time zone |
 olrconn | olr  |      3 | FREE.DBZUSER.ORDERS    | free.myorders  | PURCHASER    | who          | number          | numeric                     |
 olrconn | olr  |      4 | FREE.DBZUSER.ORDERS    | free.myorders  | QUANTITY     | quantity     | number          | bigint                      |
 olrconn | olr  |      5 | FREE.DBZUSER.ORDERS    | free.myorders  | PRODUCT_ID   | product_id   | number          | numeric                     |

```

### **Resume the Connector or Redo the Entire Snapshot**

Once the object mappings have been confirmed correct, we can resume the connector. Please note that, resume will proceed to streaming only the new table changes. The existing data of the tables will not be copied.

```sql
SELECT synchdb_resume_engine('olrconn');
```

To capture the table's existing data, we can also redo the entire snapshot with the new object mapping rules.

```sql
SELECT synchdb_stop_engine_bgw('olrconn');
SELECT synchdb_start_engine_bgw('olrconn', 'always');
```

## **Selective Table Sync**

### **Select Desired Tables and Start it for the First Time**

Table selection is done during connector creation phase via `synchdb_add_conninfo()` where we specify a list of tables (expressed in FQN, separated by a comma) to replicate from.

For example, the following command creates a connector that only replicates change from `FREE.ORDERS` tables from Oracle.

```sql
SELECT synchdb_add_conninfo(
    'olrconn', 
    '127.0.0.1', 
    1521, 
    'DBZUSER', 
    'dbz', 
    'FREE', 
    'DBZUSER', 
    'DBZUSER.ORDERS', 
    'null', 
    'oracle'
);

SELECT synchdb_add_olr_conninfo(
	'olrconn',
	'127.0.0.1',
	7070,
	'ORACLE');
```

Starting this connector for the very first time will trigger an initial snapshot being performed and selected tables' schema and data will be replicated.

```sql
SELECT synchdb_start_engine_bgw('olrconn');
```

### **Verify the Connector State and Tables**

Examine the connector state and the new tables:

```sql
postgres=# Select name, state, err from synchdb_state_view;
   name  |  state  |   err
---------+---------+----------
 olrconn | polling | no error

postgres=# \dt free.*
          List of tables
 Schema |  Name  | Type  | Owner
--------+--------+-------+--------
 free   | orders | table | ubuntu

```

By default, source database name is mapped to a schema in destination with letter casing strategy = lowercase, so `FREE.ORDERS` becomes `free.orders` in postgreSQL. Once the tables have done the initial snapshot, the connector will start CDC to stream subsequent changes for these tables.

### **Add More Tables to Replicate During Run Time.**

The `olrconn` from previous section has already completed the initial snapshot and obtained the table schemas of the selected table. If we would like to add more tables to replicate from, we will need to notify the Debezium engine about the updated table section and perform the initial snapshot again. Here's how it is done:

1. Update the `synchdb_conninfo` table to include additional tables.
2. In this example, we add the `DBZUSER.CUSTOMERS` table to the sync list:

```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{table}', '"DBZUSER.ORDERS,DBZUSER.CUSTOMERS"') 
WHERE name = 'olrconn';
```

3. Restart the connector with the snapshot mode set to `always` to perform another initial snapshot:

```sql
DROP table free.orders;
SELECT synchdb_restart_connector('olrconn', 'always');
```

This forces Debezium to re-snapshot all the tables again, including the existing tables `free.orders` and the new `free.customers` before going to CDC streaming. This means, to add a new table, the existing tables have to be dropped (to prevent duplicate table and primary key errors) and do the entire initial snapshot again. This is quite redundant and Debezium suggests using incremental snasphot to add the addition tables without re-snapshotting. We will update this procedure once we add the incremental snapshot support to SynchDB.

### **Filter Openlog Replicator Tables (IMPORTANT)**

For initial snapshot (via Debezium or FDW), we are able to select desired tables to replicate, but once the connector enters CDC stage via Openlog Replicator, the selected table filter does not automatically apply to Openlog Replicator. The user is required to configure Openlog Replicator service to output change events only for selected tables. If there is a discrepancy between SynchDB and Openlog Replicator's table filter, it is possible for SynchDB to receive a change event that it has not created a table for, causing an error. Therefore, it is a good practice to configure both SynchDB and Openlog Replicator to the same table filters.

## Custom Start Offset Values

A start offset value represents a point to start replication from in the similar way as PostgreSQL's resume LSN. When Debezium runner engine starts, it will start the replication from this offset value. Setting this offset value to a earlier value will cause Debezium runner engine to start replication from earlier records, possibly replicating duplicate data records. We should be extra cautious when setting start offset values on Debezium.

### **Record Settable Offset Values**

During operation, new offsets will be generated and flushed to disk by Debezium runner engine. The last flushed offset can be retrieved from `synchdb_state_view()` utility command:

```sql
postgres=# select name, last_dbz_offset from synchdb_state_view;
  name   |               last_dbz_offset
---------+---------------------------------------------
 olrconn | {"scn":5044305, "c_scn":5044300, "c_idx":3}

```

We should save this values regularly, so in case we run into a problem, we know the offset location in the past that can be set to resume the replication operation.

### **Pause the Connector**

A connector must be in a `paused` state before a new offset value can be set.

Use `synchdb_pause_engine()` SQL function to pause a runnng connector. This will halt the Debezium runner engine from replicating from the heterogeneous database. When paused, it is possible to alter the Debezium connector's offset value to replicate from a specific point in the past using `synchdb_set_offset()` SQL routine. It takes `conninfo_name` as its argument which can be found from the output of `synchdb_get_state()` view.

For example:

```sql
SELECT synchdb_pause_engine('olrconn');
```

### **Set the new Offset**

Use `synchdb_set_offset()` SQL function to change a connector worker's starting offset. This can only be done when the connector is put into `paused` state. The function takes 2 parameters, `conninfo_name` and `a valid offset string`, both of which can be found from the output of `synchdb_get_state()` view.

For example:

```sql
SELECT 
  synchdb_set_offset(
    'olrconn', '{"scn":5044305, "c_scn":5044300, "c_idx":3}'
  );
```

### **Resume the Connector**

Use `synchdb_resume_engine()` SQL function to resume Debezium operation from a paused state. This function takes `connector name` as its only parameter, which can be found from the output of `synchdb_get_state()` view. The resumed Debezium runner engine will start the replication from the newly set offset value.

For example:

```sql
SELECT synchdb_resume_engine('olrconn');
```