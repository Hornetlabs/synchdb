# Start / Stop a Connector

## **Control a Connector**

SynchDB provides several utility function to control the behavior and life cycle of a created connector.

## **Start a Connector with Default Snapshot Mode**

synchdb_start_engine_bgw() can be used to start a connector with default snapshot mode called `initial`.

```sql
SELECT synchdb_start_engine_bgw('mysqlconn');
```

## **Start a Connector with Custom Snapshot Mode**

Using the same function synchdb_start_engine_bgw(), it is possible to include snapshot mode to start the connector with, otherwise the `initial` mode will be used by default.

```sql
-- capture table schema and proceed to stream new changes
SELECT synchdb_start_engine_bgw('mysqlconn', 'no_data');

-- always re-build the table schema, existing data and proceed to stream new changes
SELECT synchdb_start_engine_bgw('mysqlconn', 'always');
```

## **Supported Snapshot Modes**

| Mode | Description | Use Case |
|:-:|-|-|
| `always` | Full snapshot on every start | Complete data verification |
| `initial` | First-time snapshot only | Normal operations |
| `initial_only` | One-time snapshot, then stop | Data migration |
| `no_data` | Structure only, no data | Schema synchronization |
| `never` | Skip snapshot, stream only | Real-time updates |
| `schemasync` | Structure only, no data, no CDC | normal operations |


**Refer to the [tutorial](../../tutorial/selective_table_sync/) on when to use what mode**

## **Pause and Resume a Connector**

Pause a connector with `synchdb_pause_engine`, which temporarily halts a running connector
```sql
SELECT synchdb_pause_engine('mysqlconn');
```

Resume a paused connector with `synchdb_resume_engine`.
```sql
SELECT synchdb_resume_engine('mysqlconn');
```

## **Stop or Restart a Running Connector**

Stops a running connector with `synchdb_stop_engine_bgw`
```sql
SELECT synchdb_stop_engine_bgw('mysqlconn');
```

Restarts a running connector with `synchdb_restart_connector` with a different snapshot mode.
```sql
-- Restart with specific snapshot mode
SELECT synchdb_restart_connector('mysqlconn', 'initial');

-- Start with specific snapshot mode
SELECT synchdb_start_engine_bgw('mysqlconn', 'always');
```