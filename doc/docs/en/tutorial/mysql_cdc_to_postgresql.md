# MySQL Connector

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

Before attempting to do an initial snapshot of current table and data, which may be huge, it is possible to "preview" all the tables and data type mappings between source and destination tables before the actual data migration. This gives you an opportunity to modify a data type mapping, or an object name before actual migration happens. This can be done with the special "schemasync" initial snapshot mode.

Please note that you must set `synchdb.olr_snapshot_engine` to 'fdw' in order to use `schemasync` mode to preview the tables.

### **Create a Connector and Start it in `schemasync` Mode**

`schemasync` is a special mode that makes the connector connects to remote database and attempt to sync only the schema of designated tables. After this is done, the connector is put to `paused` state and user is able to review all the tables and data types created using the default rules and make change if needed.

```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', 
    '127.0.0.1', 
    3306, 
    'mysqluser', 
    'mysqlpwd', 
    'inventory', 
    'null', 
    'null', 
    'null', 
    'mysql'
);

SELECT synchdb_start_engine_bgw('mysqlconn', 'schemasync');
```

### **Ensure the connector is put to paused state**

```sql
SELECT name, connector_type, pid, stage, state FROM synchdb_state_view WHERE name = 'mysqlconn';
   name    | connector_type |   pid   |        stage        | state
-----------+----------------+---------+---------------------+--------
 mysqlconn | mysql          | 1644218 | change data capture | paused

```

### **Review the Tables Created by Default Mapping Rules**

```sql
SELECT * FROM synchdb_att_view WHERE name = 'mysqlconn';
   name    |   type   | attnum |         ext_tbname         |         pg_tbname          | ext_attname  |  pg_attname  |  ext_atttypename  |  pg_atttypename   |         transform

-----------+----------+--------+----------------------------+----------------------------+--------------+--------------+-------------------+-------------------+----------------------
------
 mysqlconn | mysql    |      1 | inventory.addresses        | inventory.addresses        | id           | id           | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.addresses        | inventory.addresses        | customer_id  | customer_id  | int               | integer           |
 mysqlconn | mysql    |      3 | inventory.addresses        | inventory.addresses        | street       | street       | varchar           | character varying |
 mysqlconn | mysql    |      4 | inventory.addresses        | inventory.addresses        | city         | city         | varchar           | character varying |
 mysqlconn | mysql    |      5 | inventory.addresses        | inventory.addresses        | state        | state        | varchar           | character varying |
 mysqlconn | mysql    |      6 | inventory.addresses        | inventory.addresses        | zip          | zip          | varchar           | character varying |
 mysqlconn | mysql    |      7 | inventory.addresses        | inventory.addresses        | type         | type         | enum              | text              |
 mysqlconn | mysql    |      1 | inventory.customers        | inventory.customers        | id           | id           | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.customers        | inventory.customers        | first_name   | first_name   | varchar           | character varying |
 mysqlconn | mysql    |      3 | inventory.customers        | inventory.customers        | last_name    | last_name    | varchar           | character varying |
 mysqlconn | mysql    |      4 | inventory.customers        | inventory.customers        | email        | email        | varchar           | character varying |
 mysqlconn | mysql    |      1 | inventory.geom             | inventory.geom             | id           | id           | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.geom             | inventory.geom             | g            | g            | geometry          | text              |
 mysqlconn | mysql    |      3 | inventory.geom             | inventory.geom             | h            | h            | geometry          | text              |
 mysqlconn | mysql    |      1 | inventory.orders           | inventory.orders           | order_number | order_number | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.orders           | inventory.orders           | order_date   | order_date   | date              | date              |
 mysqlconn | mysql    |      3 | inventory.orders           | inventory.orders           | purchaser    | purchaser    | int               | integer           |
 mysqlconn | mysql    |      4 | inventory.orders           | inventory.orders           | quantity     | quantity     | int               | integer           |
 mysqlconn | mysql    |      5 | inventory.orders           | inventory.orders           | product_id   | product_id   | int               | integer           |
 mysqlconn | mysql    |      1 | inventory.products         | inventory.products         | id           | id           | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.products         | inventory.products         | name         | name         | varchar           | character varying |
 mysqlconn | mysql    |      3 | inventory.products         | inventory.products         | description  | description  | varchar           | character varying |
 mysqlconn | mysql    |      4 | inventory.products         | inventory.products         | weight       | weight       | float             | real              |
 mysqlconn | mysql    |      1 | inventory.products_on_hand | inventory.products_on_hand | product_id   | product_id   | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.products_on_hand | inventory.products_on_hand | quantity     | quantity     | int               | integer           |

```

### **Define Custom Mapping (If Needed)**

```sql
SELECT synchdb_add_objmap('mysqlconn','table','inventory.products','inventory.myproducts');
SELECT synchdb_add_objmap('mysqlconn','column','inventory.customers.email','contact');
SELECT synchdb_add_objmap('mysqlconn','datatype','inventory.orders.quantity','bigint|0');
SELECT synchdb_add_objmap('mysqlconn','transform','inventory.products.name','''>>>>>'' || ''%d'' || ''<<<<<''');
```
The above means:

* source table 'inventory.products' will be mapped to 'inventory.myproducts' in destination
* source column 'inventory.customers.email' will be mapped to 'contact' in destination
* source data type for column 'inventory.orders.quantity' will be mapped to 'bigint'
* source column data 'inventory.products.name' will be transformed accoring to the expression where %d is the data placeholder

### **Review All Object Mapping Rules Created So Far**

```sql
SELECT * FROM synchdb_objmap WHERE name = 'mysqlconn';
   name    |  objtype  | enabled |          srcobj           |           dstobj
-----------+-----------+---------+---------------------------+----------------------------
 mysqlconn | table     | t       | inventory.products        | inventory.myproducts
 mysqlconn | column    | t       | inventory.customers.email | contact
 mysqlconn | datatype  | t       | inventory.orders.quantity | bigint|0
 mysqlconn | transform | t       | inventory.products.name   | '>>>>>' || '%d' || '<<<<<'

```

### **Reload the Object Mapping Rules**

Once all custom rules have been defined, we need to signal the connector to load them. This will cause the connector to read and apply the object mapping rules. If it sees a discrepancy between current PostgreSQL values and the object mapping values, it will attempt to correct the mapping.

```sql
SELECT synchdb_reload_objmap('mysqlconn');

```

### **Review `synchdb_att_view` Again for Changes**

```sql
   name    | type  | attnum |         ext_tbname         |         pg_tbname          | ext_attname  |  pg_attname  | ext_atttypename |  pg_atttypename   |         transform

-----------+-------+--------+----------------------------+----------------------------+--------------+--------------+-----------------+-------------------+---------------------------
-
 mysqlconn | mysql |      1 | inventory.addresses        | inventory.addresses        | id           | id           | int             | integer           |
 mysqlconn | mysql |      2 | inventory.addresses        | inventory.addresses        | customer_id  | customer_id  | int             | integer           |
 mysqlconn | mysql |      3 | inventory.addresses        | inventory.addresses        | street       | street       | varchar         | character varying |
 mysqlconn | mysql |      4 | inventory.addresses        | inventory.addresses        | city         | city         | varchar         | character varying |
 mysqlconn | mysql |      5 | inventory.addresses        | inventory.addresses        | state        | state        | varchar         | character varying |
 mysqlconn | mysql |      6 | inventory.addresses        | inventory.addresses        | zip          | zip          | varchar         | character varying |
 mysqlconn | mysql |      7 | inventory.addresses        | inventory.addresses        | type         | type         | enum            | text              |
 mysqlconn | mysql |      1 | inventory.customers        | inventory.customers        | id           | id           | int             | integer           |
 mysqlconn | mysql |      2 | inventory.customers        | inventory.customers        | first_name   | first_name   | varchar         | character varying |
 mysqlconn | mysql |      3 | inventory.customers        | inventory.customers        | last_name    | last_name    | varchar         | character varying |
 mysqlconn | mysql |      4 | inventory.customers        | inventory.customers        | email        | contact      | varchar         | character varying |
 mysqlconn | mysql |      1 | inventory.geom             | inventory.geom             | id           | id           | int             | integer           |
 mysqlconn | mysql |      2 | inventory.geom             | inventory.geom             | g            | g            | geometry        | text              |
 mysqlconn | mysql |      3 | inventory.geom             | inventory.geom             | h            | h            | geometry        | text              |
 mysqlconn | mysql |      1 | inventory.orders           | inventory.orders           | order_number | order_number | int             | integer           |
 mysqlconn | mysql |      2 | inventory.orders           | inventory.orders           | order_date   | order_date   | date            | date              |
 mysqlconn | mysql |      3 | inventory.orders           | inventory.orders           | purchaser    | purchaser    | int             | integer           |
 mysqlconn | mysql |      4 | inventory.orders           | inventory.orders           | quantity     | quantity     | int             | bigint            |
 mysqlconn | mysql |      5 | inventory.orders           | inventory.orders           | product_id   | product_id   | int             | integer           |
 mysqlconn | mysql |      1 | inventory.products         | inventory.myproducts       | id           | id           | int             | integer           |
 mysqlconn | mysql |      2 | inventory.products         | inventory.myproducts       | name         | name         | varchar         | character varying | '>>>>>' || '%d' || '<<<<<'
 mysqlconn | mysql |      3 | inventory.products         | inventory.myproducts       | description  | description  | varchar         | character varying |
 mysqlconn | mysql |      4 | inventory.products         | inventory.myproducts       | weight       | weight       | float           | real              |
 mysqlconn | mysql |      1 | inventory.products_on_hand | inventory.products_on_hand | product_id   | product_id   | int             | integer           |
 mysqlconn | mysql |      2 | inventory.products_on_hand | inventory.products_on_hand | quantity     | quantity     | int             | integer           |


```

### **Resume the Connector or Redo the Entire Snapshot**

Once the object mappings have been confirmed correct, we can resume the connector. Please note that, resume will proceed to streaming only the new table changes. The existing data of the tables will not be copied.

```sql
SELECT synchdb_resume_engine('mysqlconn');
```

To capture the table's existing data, we can also redo the entire snapshot with the new object mapping rules.

```sql
SELECT synchdb_stop_engine_bgw('mysqlconn');
SELECT synchdb_start_engine_bgw('mysqlconn', 'always');
```

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

## Secured Connection

### **Configure Secured Connection**

to secure the connection to remote database, we need to configure additional SSL related parameters to a connector that has been created by `synchdb_add_conninfo`. The SSL certificates and private keys must be packaged as Java keystore file with a passphrase. These information is then passed to SynchDB via synchdb_add_extra_conninfo().

### **synchdb_add_extra_conninfo**

**Purpose**: Configures extra connector parameters to an existing connector created by `synchdb_add_conninfo`

| Parameter | Description | Required | Example | Notes |
|:-:|:-|:-:|:-|:-|
| `name` | Unique identifier for this connector | ✓ | `'mysqlconn'` | Must be unique across all connectors |
| `ssl_mode` | SSL mode | ☐ | `'verify_ca'` | can be one of: <br><ul><li> "disabled" - no SSL is used. </li><li> "preferred" - SSL is used if server supports it. </li><li> "required" - SSL must be used to establish a connection. </li><li> "verify_ca" - connector establishes TLS with the server and will also verify server's TLS certificate against configured truststore. </li><li> "verify_identity" - same behavior as verify_ca but it also checks the server certificate's common name to match the hostname of the system. |
| `ssl_keystore` | keystore path | ☐ | `/path/to/keystore` | path to the keystore file |
| `ssl_keystore_pass` | keystore password | ☐ | `'mykeystorepass'` | password to access the keystore file |
| `ssl_truststore` | trust store path | ☐ | `'/path/to/truststore'` | path to the truststore file |
| `ssl_truststore_pass` | trust store password | ☐ | `'mytruststorepass'` | password to access the truststore file |


```sql
SELECT synchdb_add_extra_conninfo('mysqlconn', 'verify_ca', '/path/to/keystore', 'mykeystorepass', '/path/to/truststore', 'mytruststorepass');
```

### **synchdb_del_extra_conninfo**

**Purpose**: Deletes extra connector paramters created by `synchdb_add_extra_conninfo`
```sql
SELECT synchdb_del_extra_conninfo('mysqlconn');
```

## Custom Start Offset Values

A start offset value represents a point to start replication from in the similar way as PostgreSQL's resume LSN. When Debezium runner engine starts, it will start the replication from this offset value. Setting this offset value to a earlier value will cause Debezium runner engine to start replication from earlier records, possibly replicating duplicate data records. We should be extra cautious when setting start offset values on Debezium.

### **Record Settable Offset Values**

During operation, new offsets will be generated nd flushed to disk by Debezium runner engine. The last flushed offset can be retrieved from `synchdb_state_view()` utility command:

```sql
postgres=# select name, last_dbz_offset from synchdb_state_view;
     name      |                                           last_dbz_offset
---------------+------------------------------------------------------------------------------------------------------
 mysqlconn     | {"ts_sec":1741301103,"file":"mysql-bin.000009","pos":574318212,"row":1,"server_id":223344,"event":2}

```

Depending on the connector type, this offset value differs. From the example above, the `mysql` connector's last flushed offset is `{"ts_sec":1741301103,"file":"mysql-bin.000009","pos":574318212,"row":1,"server_id":223344,"event":2}` and `sqlserver`'s last flushed offset is `{"commit_lsn":"0000006a:00006608:0003","snapshot":true,"snapshot_completed":false}`. 

We should save this values regularly, so in case we run into a problem, we know the offset location in the past that can be set to resume the replication operation.


### **Pause the Connector**

A connector must be in a `paused` state before a new offset value can be set.

Use `synchdb_pause_engine()` SQL function to pause a runnng connector. This will halt the Debezium runner engine from replicating from the heterogeneous database. When paused, it is possible to alter the Debezium connector's offset value to replicate from a specific point in the past using `synchdb_set_offset()` SQL routine. It takes `conninfo_name` as its argument which can be found from the output of `synchdb_get_state()` view.

For example:

```sql
SELECT synchdb_pause_engine('mysqlconn');
```

### **Set the new Offset**

Use `synchdb_set_offset()` SQL function to change a connector worker's starting offset. This can only be done when the connector is put into `paused` state. The function takes 2 parameters, `conninfo_name` and `a valid offset string`, both of which can be found from the output of `synchdb_get_state()` view.

For example:

```sql
SELECT 
  synchdb_set_offset(
    'mysqlconn', '{"ts_sec":1741301103,"file":"mysql-bin.000009","pos":574318212,"row":1,"server_id":223344,"event":2}'
  );
```

### **Resume the Connector**

Use `synchdb_resume_engine()` SQL function to resume Debezium operation from a paused state. This function takes `connector name` as its only parameter, which can be found from the output of `synchdb_get_state()` view. The resumed Debezium runner engine will start the replication from the newly set offset value.

For example:

```sql
SELECT synchdb_resume_engine('mysqlconn');
```
