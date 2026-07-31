# Connector Statistics

## **Check Connector Running Statistics**

SynchDB keeps a record of statistics per connector. These statistics are separate from Debezium's or JVM's JMX based statistics though they may have similar or overlapping parameters.

There are 3 categories of statistics:

* General Statistics
* Snapshot Statistics
* CDC Statistics

Please note that these statistics are not persistent and will be lost/reset upon PostgreSQL restarts.

## **General Statistics**

Obtained via "synchdb_genstats" view:
```sql
select * from synchdb_genstats;

  name   | bad_events | total_events | batches_done | average_batch_size | first_src_ts  |  first_pg_ts  |  last_src_ts  |  last_pg_ts
---------+------------+--------------+--------------+--------------------+---------------+---------------+---------------+---------------
 olrconn |        191 |          453 |           14 |                 32 | 1761170446000 | 1761170450120 | 1761170448000 | 1761170450120
(1 row)
```

Column Details:

| fields | description |
|-|-|
| name | the associated connector info name created by `synchdb_add_conninfo()`|
| bad_events | number of bad events ignored (such as empty events, unsupported DDL events..etc) |
| total_events | total number of events processed (including bad_events) |
| batches_done | number of batches completed |
| average_batch_size | average batch size (total_events / batches_done) |
| first_src_ts | the timestamp in milliseconds when the last batch's first event is produced at the external database |
| first_pg_ts | the timestamp in milliseconds when the last batch's first event is applied to PostgreSQL |
| last_src_ts | the timestamp in milliseconds when the last batch's last event is produced at the external database |
| last_pg_ts | the timestamp in milliseconds when the last batch's last event is applied to PostgreSQL |

## **Snapshot Statistics**

Obtained via "synchdb_snapstats" view:
```sql
select * from synchdb_snapstats;

  name   | tables |  rows  | snapshot_begin_ts | snapshot_end_ts
---------+--------+--------+-------------------+-----------------
 olrconn |      2 | 100032 |     1761160017191 |   1761160033250
(1 row)

```

Column Details:

| fields | description |
|-|-|
| name | the associated connector info name created by `synchdb_add_conninfo()`|
| tables | number of table schemas migrated during snapshot process |
| rows | number of rows migrated during snapshot process |
| snapshot_begin_ts | the timestamp in milliseconds when the snapshot process begins |
| snapshot_end_ts | the timestamp in milliseconds when the snapshot process ends |


## **CDC Statistics**

Obtained via "synchdb_cdcstats" view:
```sql
select * from synchdb_cdcstats;

  name   | ddls | dmls | creates | updates | deletes | txs | truncates
---------+------+------+---------+---------+---------+-----+-----------
 olrconn |  161 |  124 |      10 |      47 |      67 | 562 |         0
(1 row)


```

Column Details:

| fields | description |
|-|-|
| name | the associated connector info name created by `synchdb_add_conninfo()`|
| ddls | number of DDLs operations completed |
| dmls | number of DMLs operations completed |
| creates | number of CREATES events completed during CDC stage |
| updates | number of UPDATES events completed during CDC stage |
| deletes | number of DELETES events completed during CDC stage |
| txs | number of transaction events processed such as begin and commit |
| truncates | number of truncate events processed |


## **synchdb_reset_stats**

**Purpose**: Resets all statistic information of given connector name

```sql
SELECT synchdb_reset_stats('olrconn');
```
