# Oracle Connector

## **Prepare Oracle Database for SynchDB**

Before SynchDB can be used to replicate from Oracle, Oracle needs to be configured according to the procedure outlined [here](../../getting-started/remote_database_setups/)

Please ensure that supplemental log data is enabled for all columns for each desired table to be replicated by SynchDB. This is needed for SynchDB to correctly handle UPDATE and DELETE oeprations.

For example, the following enables supplemental log data for all columns for `customer` and `products` table. Please add more tables as needed.

```sql
ALTER TABLE customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
... etc
```
## **Initial Snapshot**
"Initial snapshot" (or table snapshot) in SynchDB means to copy table schema plus initial data for all designated tables. This is similar to the term "table sync" in PostgreSQL logical replication. When a connector is started using the default `initial` mode, it will automatically perform the initial snapshot before going to Change Data Capture (CDC) stage. This can be partially omitted with mode `no_data`. See [here](../../user-guide/start_stop_connector/) for all snapshot options.

Once the initial snapshot is completed, the connector will not do it again upon subsequent restarts and will just resume with CDC since the last incomplete offset. This behavior is controled by the metadata files managed by Debezium engine. See [here](../../architecture/metadata_files/) for more about metadata files.

## **Different Connector Launch Modes**

### **Create a Oracle Connector**

Create a connector that targets all the tables under `FREE` database and `DBZUSER` schema in Oracle.
```sql
SELECT 
  synchdb_add_conninfo(
    'oracleconn', '127.0.0.1', 1521, 
    'DBZUSER', 'dbz', 'FREE', 'DBZUSER', 
    'null', 'null', 'oracle');
```

### **Initial Snapshot + CDC**

Start the connector using `initial` mode will perform the initial snapshot of all designated tables (all in this case). After this is completed, the change data capture (CDC) process will begin to stream for new changes.

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'initial');

or 

SELECT synchdb_start_engine_bgw('oracleconn');
```

The stage of this connector should be in `initial snapshot` the first time it runs:
```sql
postgres=# select * from synchdb_state_view where name='oracleconn';
    name    | connector_type |  pid   |      stage       |  state  |   err    |       last_dbz_offset
------------+----------------+--------+------------------+---------+----------+-----------------------------
 oracleconn | oracle         | 528146 | initial snapshot | polling | no error | offset file not flushed yet

```

A new schema called `inventory` will be created and all tables streamed by the connector will be replicated under that schema.
```sql
postgres=# set search_path=free;
SET
postgres=# \d
              List of relations
 Schema |        Name        | Type  | Owner
--------+--------------------+-------+--------
 free   | orders             | table | ubuntu

```

After the initial snapshot is completed, and at least one subsequent changes is received and processed, the connector stage shall change from `initial snapshot` to `Change Data Capture`.
```sql
postgres=# select * from synchdb_state_view where name='oracleconn';
    name    | connector_type |  pid   |        stage        |  state  |   err    |
    last_dbz_offset
------------+----------------+--------+---------------------+---------+----------+-------------------------------
-------------------------------------------------------
 oracleconn | oracle         | 528414 | change data capture | polling | no error | {"commit_scn":"3118146:1:02001
f00c0020000","snapshot_scn":"3081987","scn":"3118125"}

```

This means that the connector is now streaming for new changes of the designated tables. Restarting the connector in `initial` mode will proceed replication since the last successful point and initial snapshot will not be re-run.

### **Initial Snapshot Only and no CDC**

Start the connector using `initial_only` mode will perform the initial snapshot of all designated tables (all in this case) only and will not perform CDC after.

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'initial_only');

```

The connector would still appear to be `polling` from the connector but no change will be captured because Debzium internally has stopped the CDC. You have the option to shut it down. Restarting the connector in `initial_only` mode will not rebuild the tables as they have already been built.

### **Capture Table Schema Only + CDC**

Start the connector using `no_data` mode will perform the schema capture only, build the corresponding tables in PostgreSQL and it does not replicate existing table data (skip initial snapshot). After the schema capture is completed, the connector goes into CDC mode and will start capture subsequent changes to the tables.

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'no_data');

```

Restarting the connector in `no_data` mode will not rebuild the schema again, and it will resume CDC since the last successful point.

### **Always do Initial Snapahot + CDC**

Start the connector using `always` mode will always capture the schemas of capture tables, always redo the initial snapshot and then go to CDC. This is similar to a reset button because everything will be rebuilt using this mode. Use it with caution especially when you have large number of tables being captured, which could take a long time to finish. After the rebuild, CDC resumes as normal.

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'always');

```

After the initial snapshot, CDC will begin. Restarting a connector in `always` mode will repeat the same process described above.

## **Possible Snapshot Modes for Oracle Connector**

* initial (default)
* initial_only
* no_data
* always
* schemasync

## **Preview Source and Destination Table Relationships with schemasync mode**

Before attempting to do an initial snapshot of current table and data, which may be huge, it is possible to "preview" all the tables and data type mappings between source and destination tables before the actual data migration. This gives you an opportunity to modify a data type mapping, or an object name before actual migration happens. This can be done with the special "schemasync" initial snapshot mode.

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

SELECT synchdb_start_engine_bgw('oracleconn', 'schemasync');
```

### **Ensure the Connector is Put to Paused State**

```sql
SELECT name, connector_type, pid, stage, state FROM synchdb_state_view WHERE name = 'oracleconn';
    name    | connector_type |   pid   |        stage        | state
------------+----------------+---------+---------------------+--------
 oracleconn | oracle         | 1648935 | change data capture | paused


```

### **Review the Tables Created by Default Mapping Rules**

```sql
SELECT * FROM synchdb_att_view WHERE name = 'oracleconn';
    name    |  type  | attnum |       ext_tbname       |   pg_tbname    | ext_attname  |  pg_attname  | ext_atttypename |       pg_atttypename        | transform
------------+--------+--------+------------------------+----------------+--------------+--------------+-----------------+-----------------------------+-----------
 oracleconn | oracle |      1 | FREE.DBZUSER.CUSTOMERS | free.customers | ID           | id           | number          | numeric                     |
 oracleconn | oracle |      2 | FREE.DBZUSER.CUSTOMERS | free.customers | NAME         | name         | varchar2        | character varying           |
 oracleconn | oracle |      1 | FREE.DBZUSER.ORDERS    | free.orders    | ORDER_NUMBER | order_number | number          | numeric                     |
 oracleconn | oracle |      2 | FREE.DBZUSER.ORDERS    | free.orders    | ORDER_DATE   | order_date   | date            | timestamp without time zone |
 oracleconn | oracle |      3 | FREE.DBZUSER.ORDERS    | free.orders    | PURCHASER    | purchaser    | number          | numeric                     |
 oracleconn | oracle |      4 | FREE.DBZUSER.ORDERS    | free.orders    | QUANTITY     | quantity     | number          | numeric                     |
 oracleconn | oracle |      5 | FREE.DBZUSER.ORDERS    | free.orders    | PRODUCT_ID   | product_id   | number          | numeric                     |

```

### **Define Custom Mapping Rules (If Needed)**

User can use `synchdb_add_objmap` function to create custom mapping rules. It can be used to map table name, column name, data types and defines a data transform expression rule

```sql
SELECT synchdb_add_objmap('oracleconn','table','FREE.DBZUSER.ORDERS','free.myorders');
SELECT synchdb_add_objmap('oracleconn','column','FREE.DBZUSER.ORDERS.PURCHASER','who');
SELECT synchdb_add_objmap('oracleconn','datatype','FREE.DBZUSER.ORDERS.QUANTITY','bigint|0');
SELECT synchdb_add_objmap('oracleconn','transform','FREE.DBZUSER.CUSTOMERS.NAME','''>>>>>'' || ''%d'' || ''<<<<<''');
```
The above means:

* source table 'FREE.DBZUSER.ORDERS' will be mapped to 'free.myorders' in destination
* source column 'FREE.DBZUSER.ORDERS.PURCHASE' will be mapped to 'who' in destination
* source data type for column 'FREE.DBZUSER.ORDERS.QUANTITY' will be mapped to 'bigint'
* source column data 'FREE.DBZUSER.CUSTOMERS.NAME' will be transformed accoring to the expression where %d is the data placeholder

### **Review All Object Mapping Rules Created So Far**

```sql
SELECT * FROM synchdb_objmap WHERE name = 'oracleconn';
    name    |  objtype  | enabled |            srcobj             |           dstobj
------------+-----------+---------+-------------------------------+----------------------------
 oracleconn | table     | t       | FREE.DBZUSER.ORDERS           | free.myorders
 oracleconn | column    | t       | FREE.DBZUSER.ORDERS.PURCHASER | who
 oracleconn | datatype  | t       | FREE.DBZUSER.ORDERS.QUANTITY  | bigint|0
 oracleconn | transform | t       | FREE.DBZUSER.CUSTOMERS.NAME   | '>>>>>' || '%d' || '<<<<<'

```

### **Reload the Object Mapping Rules**

Once all custom rules have been defined, we need to signal the connector to load them. This will cause the connector to read and apply the object mapping rules. If it sees a discrepancy between current PostgreSQL values and the object mapping values, it will attempt to correct the mapping.

```sql
SELECT synchdb_reload_objmap('oracleconn');

```

### **Review `synchdb_att_view` Again for Changes**

```sql
SELECT * from synchdb_att_view WHERE name = 'oracleconn';
    name    |  type  | attnum |       ext_tbname       |   pg_tbname    | ext_attname  |  pg_attname  | ext_atttypename |       pg_atttypename        |         transform
------------+--------+--------+------------------------+----------------+--------------+--------------+-----------------+-----------------------------+----------------------------
 oracleconn | oracle |      1 | FREE.DBZUSER.CUSTOMERS | free.customers | ID           | id           | number          | numeric                     |
 oracleconn | oracle |      2 | FREE.DBZUSER.CUSTOMERS | free.customers | NAME         | name         | varchar2        | character varying           | '>>>>>' || '%d' || '<<<<<'
 oracleconn | oracle |      1 | FREE.DBZUSER.ORDERS    | free.myorders  | ORDER_NUMBER | order_number | number          | numeric                     |
 oracleconn | oracle |      2 | FREE.DBZUSER.ORDERS    | free.myorders  | ORDER_DATE   | order_date   | date            | timestamp without time zone |
 oracleconn | oracle |      3 | FREE.DBZUSER.ORDERS    | free.myorders  | PURCHASER    | who          | number          | numeric                     |
 oracleconn | oracle |      4 | FREE.DBZUSER.ORDERS    | free.myorders  | QUANTITY     | quantity     | number          | bigint                      |
 oracleconn | oracle |      5 | FREE.DBZUSER.ORDERS    | free.myorders  | PRODUCT_ID   | product_id   | number          | numeric                     |

```

### **Resume the Connector or Redo the Entire Snapshot**

Once the object mappings have been confirmed correct, we can resume the connector. Please note that, resume will proceed to streaming only the new table changes. The existing data of the tables will not be copied.

```sql
SELECT synchdb_resume_engine('oracleconn');
```

To capture the table's existing data, we can also redo the entire snapshot with the new object mapping rules.

```sql
SELECT synchdb_stop_engine_bgw('oracleconn');
SELECT synchdb_start_engine_bgw('oracleconn', 'always');
```

## **Selective Table Sync**

### **Select Desired Tables and Start it for the First Time**

Table selection is done during connector creation phase via `synchdb_add_conninfo()` where we specify a list of tables (expressed in FQN, separated by a comma) to replicate from.

For example, the following command creates a connector that only replicates change from `FREE.ORDERS` tables from Oracle.

```sql
SELECT synchdb_add_conninfo(
    'oracleconn', 
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
```

Starting this connector for the very first time will trigger an initial snapshot being performed and selected tables' schema and data will be replicated.

```sql
SELECT synchdb_start_engine_bgw('oracleconn');
```

### **Verify the Connector State and Tables**

Examine the connector state and the new tables:

```sql
postgres=# Select name, state, err from synchdb_state_view;
    name    |  state  |   err
------------+---------+----------
 oracleconn | polling | no error

postgres=# \dt free.*
          List of tables
 Schema |  Name  | Type  | Owner
--------+--------+-------+--------
 free   | orders | table | ubuntu

```
By default, source database name is mapped to a schema in destination with letter casing strategy = lowercase, so `FREE.ORDERS` becomes `free.orders` in postgreSQL. Once the tables have done the initial snapshot, the connector will start CDC to stream subsequent changes for these tables.

### **Add More Tables to Replicate During Run Time.**

The `oracleconn` from previous section has already completed the initial snapshot and obtained the table schemas of the selected table. If we would like to add more tables to replicate from, we will need to notify the Debezium engine about the updated table section and perform the initial snapshot again. Here's how it is done:

1. Update the `synchdb_conninfo` table to include additional tables.
2. In this example, we add the `DBZUSER.CUSTOMERS` table to the sync list:

```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{table}', '"DBZUSER.ORDERS,DBZUSER.CUSTOMERS"') 
WHERE name = 'oracleconn';
```

3. Restart the connector with the snapshot mode set to `always` to perform another initial snapshot:

```sql
DROP table free.orders;
SELECT synchdb_restart_connector('oracleconn', 'always');
```

This forces Debezium to re-snapshot all the tables again, including the existing tables `free.orders` and the new `free.customers` before going to CDC streaming. This means, to add a new table, the existing tables have to be dropped (to prevent duplicate table and primary key errors) and do the entire initial snapshot again. This is quite redundant and Debezium suggests using incremental snasphot to add the addition tables without re-snapshotting. We will update this procedure once we add the incremental snapshot support to SynchDB.

### **Verify the Updated Tables**

Now, we can examine our tables again:

```sql
postgres=# \dt free.*
          List of tables
 Schema      |  Name  | Type  | Owner
-------------+--------+-------+--------
 free        | orders | table | ubuntu
 customers   | orders | table | ubuntu

```

## **Configure Infinispan for Oracle Connector**

By default, Debezium's Oracle connector uses the JVM heap to cache incoming change events before passing them to SynchDB for processing. The maximum heap size is controlled via the `synchdb.jvm_max_heap_size` GUC. When the JVM heap runs out of memory (especially under large transactions or schemas with many columns), the connector may fail with OutOfMemoryError, making heap sizing a critical tuning challenge.

As an alternative, Debezium can be configured to use Infinispan as its caching layer. Infinispan supports memory storage on both the JVM heap and off-heap (direct memory), offering more flexibility. Additionally, it supports passivation, allowing excess data to be spilled to disk when memory limits are reached. This makes it far more resilient under heavy workloads and ensures large transactions or schema changes can be handled gracefully without running out of memory.

### **`synchdb_add_infinispan()`**

**Signature:**

```sql
synchdb_add_infinispan(
    connectorName NAME,     -- Name of the connector
    memoryType NAME,        -- memory type, can be "heap" or "off heap"
    memorySize INT          -- size of memory in MB to reserve as cache
)
```

Registers an Infinispan-backed caching configuration for a given connector. This allows the connector to use Infinispan for buffering change events, supporting both heap-based and off-heap memory allocation with spill-to-disk (passivation).

**Note:**

- If called on a connector that is currently running, the setting will take effect on next restart.
- If an Infinispan cache already exists for the connector, it will be replaced.
- memoryType='off_heap' utilizes native (direct) memory and is not limited by JVM heap, but should be sized carefully.
- The cache will automatically support passivation to disk when memory fills.

**Example:**
``` sql
SELECT synchdb_add_infinispan('oracleconn', 'off_heap', 2048);



```

### **`synchdb_del_infinispan()`**

**Signature:**

```sql
synchdb_add_infinispan(
    connectorName NAME         -- Name of the connector
)
```

Removes the Infinispan cache configuration and associated on-disk metadata for a given connector. This operation will delete:

- All cache files
- Any passivated (spilled-to-disk) state
- The associated Infinispan configuration

**Note:**

- This function can only be executed when the connector is stopped.
- Attempting to run it while the connector is active will result in an error.
- Use this command to clean up after permanently disabling or reconfiguring a connector’s cache backend.

**Example:**
``` sql
SELECT synchdb_del_infinispan('oracleconn');

```

### **`Passivation`**

Passivation simply means infinispan will eject and write data on disk if cache memory is full. As long as there is space in memory to hold change events, disk write will not occur. The data is written to `$PGDATA/pg_synchdb/ispn_[connector name]_[destination database name]`


### **Example Oracle SQLs to test Infinispan with a Large Transaction**

**Create a test table in Oracle:**
```sql
CREATE TABLE big_tx_test (
    id       NUMBER PRIMARY KEY,
    payload  VARCHAR2(4000),
    created  DATE DEFAULT SYSDATE
);

```

**Create a relatively large transaction:**
```sql
BEGIN
  FOR i IN 1..100000 LOOP
    INSERT INTO big_tx_test (id, payload)
    VALUES (i, RPAD('x', 4000, 'x'));
  END LOOP;
  COMMIT;
END;
/

```

**Behaviors Under Large Transaction**

no infinispan setting + low JVM heap (synchdb.jvm_max_heap_size=128)

* --> OutOfMemory error will occur

low JVM heap (128) + high infinispan off heap (2048)

* --> large transaction handled successfully

low JVM heap (128) + low infinispan off heap (128)

* --> passivation will occur
* --> `ispn_[connector name]_[destination database name]` size will grow during processing and reduce once large transaction finished processing
* --> large transaction handled successfully but at slower speed.

low JVM heap (128) + high infinispan heap (2048)

* --> OutOfMemory error will occur
* --> do not configure infinispan to use more heap memory than JVM max heap.

low JVM heap (128) + low infinispan heap (64)

* --> passivation will occur
* --> `ispn_[connector name]_[destination database name]` size will grow during processing and reduce once large transaction finished processing
* --> not recommended as half of the JVM heap can potentially be used by infinispan and may not have enough left for other Debezium operations

low JVM heap (128) + same infinispan heap (128)

* --> not recommended as all of the JVM heap can potentially be used by infinispan and eventually causing OutOfMemory error

high JVM heap (2048) + low infinispan heap (128)

* --> passivation will occur
* --> `ispn_[connector name]_[destination database name]` size will grow during processing and reduce once large transaction finished processing
* --> large transaction handled successfully
* --> not efficient - as only a small portion of JVM heap is used as cache + unnessary passivation.

## Custom Start Offset Values

A start offset value represents a point to start replication from in the similar way as PostgreSQL's resume LSN. When Debezium runner engine starts, it will start the replication from this offset value. Setting this offset value to a earlier value will cause Debezium runner engine to start replication from earlier records, possibly replicating duplicate data records. We should be extra cautious when setting start offset values on Debezium.

### **Record Settable Offset Values**

During operation, new offsets will be generated and flushed to disk by Debezium runner engine. The last flushed offset can be retrieved from `synchdb_state_view()` utility command:

```sql
postgres=# select name, last_dbz_offset from synchdb_state_view;
     name      |                                           last_dbz_offset
---------------+------------------------------------------------------------------------------------------------------
 oracleconn    | {"commit_scn":"2311579","snapshot_scn":"2311578","scn":"2311578"}

```

We should save this values regularly, so in case we run into a problem, we know the offset location in the past that can be set to resume the replication operation.

### **Pause the Connector**

A connector must be in a `paused` state before a new offset value can be set.

Use `synchdb_pause_engine()` SQL function to pause a runnng connector. This will halt the Debezium runner engine from replicating from the heterogeneous database. When paused, it is possible to alter the Debezium connector's offset value to replicate from a specific point in the past using `synchdb_set_offset()` SQL routine. It takes `conninfo_name` as its argument which can be found from the output of `synchdb_get_state()` view.

For example:

```sql
SELECT synchdb_pause_engine('oracleconn');
```

### **Set the new Offset**

Use `synchdb_set_offset()` SQL function to change a connector worker's starting offset. This can only be done when the connector is put into `paused` state. The function takes 2 parameters, `conninfo_name` and `a valid offset string`, both of which can be found from the output of `synchdb_get_state()` view.

For example:

```sql
SELECT 
  synchdb_set_offset(
    'oracleconn', '{"commit_scn":"2311579","snapshot_scn":"2311578","scn":"2311578"}'
  );
```

### **Resume the Connector**

Use `synchdb_resume_engine()` SQL function to resume Debezium operation from a paused state. This function takes `connector name` as its only parameter, which can be found from the output of `synchdb_get_state()` view. The resumed Debezium runner engine will start the replication from the newly set offset value.

For example:

```sql
SELECT synchdb_resume_engine('oracleconn');
```

## Configure Debezium Oracle Connector to Use Openlog Replicator (non-native)

Though not recommended, it is possible to configure a Debezium based Oracle Connector to stream from Openlog Replicator instead of logminer. It is different from [Native Openlog Replicator](../../tutorial/native_olr_cdc_to_postgresql), which is built natively within SynchDB without the use of Debezium and is the recommended way to use OLR.

To create a **Debezium-based** OLR connector (use `type` = 'oracle'):

```sql
SELECT synchdb_add_conninfo('olrconn',
                            'ora19c',
                            1521,
                            'DBZUSER',
                            'dbz',
                            'FREE',
                            'postgres',
                            'null',
                            'null',
                            'oracle');

```

Then, attach Openlog Replicator information with `synchdb_add_olr_conninfo` with signature:

```sql
synchdb_add_olr_conninfo(
    conn_name TEXT,     -- Name of the connector
    olr_host TEXT,      -- Hostname or IP of the OLR instance
    olr_port INT,       -- Port number exposed by OLR (typically 7070)
    olr_source TEXT     -- Oracle source name as configured in OLR
)
```

**Example:**

```sql
SELECT synchdb_add_olr_conninfo('olrconn', '127.0.0.1', 7070, 'ORACLE');

```

To removes the OLR configuration from a specific connector and revert back to Logminer, use `synchdb_del_olr_conninfo` with signature:

```sql
synchdb_del_olr_conninfo(conn_name TEXT)

```

**Example:**

```sql
SELECT synchdb_del_olr_conninfo('olrconn');

```

**Behavior Notes for Debezium based Openlog Replicator Connector**

* When both LogMiner and OLR configurations exist, SynchDB defaults to using Openlog Replicator for change capture.
* If OLR configurations are absent, SynchDB uses logmining strategy to stream changes.
* Restarting the connector is required after modifying its OLR configuration.