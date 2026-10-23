# Native Openlog Replicator Connector

## Snapshot-to-CDC handoff requirements

The native handoff is tested with OLR 1.8.5 and requires the Debezium snapshot provider, one redo thread, and the complete configured schema. Configure JSON output with `format.message=2`, `format.schema=7`, and `format.scn-type=3`. Enable checkpoint output with flag `4096`; use `flags=4128` to retain DDL output as well.

The connector maintains `SYNCHDB_OLR_FENCE` in the source schema and requires table-creation privileges plus access to database identity, active transactions, and redo metadata. This internal table is excluded from replication. Its committed token must arrive on the actual OLR connection. CDC starts only after the finite snapshot producer has successfully completed and all target batches have committed. The native read-view SCN and its pending transaction IDs distinguish snapshotted rows from transactions that commit later. Target changes and the OLR cursor commit in one PostgreSQL transaction before source acknowledgement.

`synchdb_olr_positions` binds handoff state to the connector UUID and artifact generation. Retain continuous redo from the selected replay sequence, and avoid captured-table DDL throughout the snapshot. Partial snapshots, missing logs, changed database incarnation, or an OLR start beyond the verified boundary require explicit recovery. Legacy alias-based SCN files and completion markers cannot authorize CDC. Rebaselining with `always` requires a clean target and an explicitly reset artifact generation. FDW snapshots and schema previews do not establish a CDC handoff.

## **Prepare Oracle Database for SynchDB**

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

Once the initial snapshot is completed, the connector will not do it again upon subsequent restarts and will just resume with CDC since the last incomplete offset. Recovery uses the committed `synchdb_olr_positions` row together with the engine-owned artifact generation; Debezium metadata alone cannot authorize CDC. See [here](../../architecture/metadata_files/) for more about metadata files.

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

### **Always do Initial Snapshot + CDC**

`always` performs a full snapshot for a new artifact generation. Before deliberately rebuilding an existing baseline, stop the connector, prepare a clean target, and call `synchdb_artifact_reset(connector_uuid)`. An existing generation rejects `always` until explicitly reset; use `initial` to resume its durable CDC cursor.

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'always');

```

After the initial snapshot and verified handoff, CDC begins. A further rebuild requires the same explicit target cleanup and generation reset.

## **Possible Snapshot Modes for Openlog Replicator Connector**

* initial (default)
* initial_only
* no_data
* always
* schemasync

## **Preview Source and Destination Table Relationships with schemasync mode**

Before attempting to do an initial snapshot of current table and data, which may be huge, it is possible to "preview" all the tables and data type mappings between source and destination tables before the actual data migration. This gives you an opportunity to modify a data type mapping, or an object name before actual migration happens. This can be done with the special "schemasync" initial snapshot mode.

`schemasync` can use the native snapshot provider, or the FDW provider for an offline schema preview. The FDW provider does not establish a CDC boundary.

### **Create a Connector and Start it in `schemasync` Mode**

`schemasync` is a special mode that makes the connector connects to remote database and attempt to sync only the schema of designated tables. After this is done, the connector is put to `paused` state and user is able to review all the tables and data types created using the default rules and make change if needed.

```sql
SELECT synchdb_add_conninfo(
    'olrconn',
    '127.0.0.1', 
    1521, 
    'DBZUSER', 
    'dbz', 
    'FREE', 
    'DBZUSER', 
    'null', 
    'null', 
    'olr'
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

A native `schemasync` run can resume CDC after its verified handoff; it does not copy existing table rows. An FDW schema preview cannot resume into OLR CDC. To populate existing rows, stop the preview, select the Debezium snapshot provider, prepare a clean target, reset the connector's artifact generation, and start a fresh `initial` run with the chosen mapping rules.

## Capture scope

The verified native OLR handoff currently requires the complete configured schema. Leave connector table and snapshot-table filters unset, and configure OLR to capture that schema, including the internal `SYNCHDB_OLR_FENCE` table. The worker consumes the fence as source evidence and excludes it from target replication. Native `initial_only`, FDW `initial_only`, and FDW `schemasync` runs can select tables; these snapshot-only runs do not establish a CDC handoff.

Source verification buffers unacknowledged changes on the actual OLR connection. Set `synchdb.olr_read_buffer_size` large enough for that replay backlog and the largest source transaction; exceeding the limit stops the connector without acknowledging the uncommitted batch.

## Durable replay position

Inspect `snapshot_scn`, `scn`, `c_scn`, and `c_idx` in `synchdb_olr_positions`. The `(c_scn, c_idx)` pair is the actual OLR replay coordinate, not an interchangeable Oracle snapshot SCN. The cursor and target changes commit atomically, so stopping and starting the same connector resumes the committed position.

Manual OLR offset changes through `synchdb_set_offset()` are rejected. Restore the matching target and handoff state together, or prepare a clean target and explicitly reset the artifact generation for a new baseline. Do not edit the cursor table or copy legacy SCN files to bypass source verification.
