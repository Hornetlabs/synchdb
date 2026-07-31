# Connector Running State

## Check Connector Running State
Use `synchdb_state_view()` to examine all connectors' running states.

See below for an example output:
``` SQL
postgres=# select * from synchdb_state_view;
     name      | connector_type |  pid   |        stage        |  state  |   err    |                                           last_dbz_offset
---------------+----------------+--------+---------------------+---------+----------+------------------------------------------------------------------------------------------------------
 sqlserverconn | sqlserver      | 579820 | change data capture | polling | no error | {"commit_lsn":"0000006a:00006608:0003","snapshot":true,"snapshot_completed":false}
 mysqlconn     | mysql          | 579845 | change data capture | polling | no error | {"ts_sec":1741301103,"file":"mysql-bin.000009","pos":574318212,"row":1,"server_id":223344,"event":2}
 oracleconn    | oracle         | 580053 | change data capture | polling | no error | offset file not flushed yet
(3 rows)

```

Column Details:

| fields          | description |
|-|-|
| name   | the associated connector info name created by `synchdb_add_conninfo()`|
| connector_type       | the type of connector (mysql, oracle, sqlserver...etc)|
| pid             | the PID of the connector worker process|
| stage           | the stage of the connector. See below.|
| state           | the state of the connector. See below.|
| err             | the last error message encountered by the worker which would have caused it to exit. This error could originated from PostgreSQL while processing a change, or originated from Debezium running engine while accessing data from heterogeneous database. |
| last_dbz_offset | the last Debezium offset captured by synchdb. Note that this may not reflect the current and real-time offset value of the connector engine. Rather, this is shown as a checkpoint that we could restart from this offset point if needed.|

**Possible States**:

- 🔴 `stopped` - Inactive
- 🟡 `initializing` - Starting up
- 🟠 `paused` - Temporarily halted
- 🟢 `syncing` - Actively polling
- 🔵 `parsing` - Processing events
- 🟣 `converting` - Transforming data
- ⚪ `executing` - Applying changes
- 🟤 `updating offset` - Updating checkpoint
- 🟨 `restarting` - Reinitializing
- ⚪ `dumping memory` - JVM is preparing to dump memory info in log file
- ⚫ `unknown` - Indeterminate state

**Possible Stages**:

- `initial snapshot` - connector is performing initial snapshot (building table schema and optionally the initial data)
- `change data capture` - connector is streaming subsequent table changes (CDC)
- `schema sync` - connector is copying table schema only (no data)