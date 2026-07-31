# 連接器統計訊息

## **檢查連接器運行統計信息**

SynchDB 會記錄每個連接器的統計信息。這些統計信息與 Debezium 或 JVM 基於 JMX 的統計不同，儘管它們可能具有相似或重疊的參數。

統計信息分為三類：

* 常規統計信息
* 快照統計訊息
* CDC 統計信息

請注意，這些統計信息不是持久化的，在 PostgreSQL 重新啟動後會遺失/重置。

## **常規統計信息**

透過 synchdb_genstats 視圖取得：
```sql
select * from synchdb_genstats;

  name   | bad_events | total_events | batches_done | average_batch_size | first_src_ts  |  first_pg_ts  |  last_src_ts  |  last_pg_ts
---------+------------+--------------+--------------+--------------------+---------------+---------------+---------------+---------------
 olrconn |        191 |          453 |           14 |                 32 | 1761170446000 | 1761170450120 | 1761170448000 | 1761170450120
(1 row)
```

列詳情：

| 字段 | 描述 |
|-|-|
| 名稱 | 由 `synchdb_add_conninfo()` 建立的關聯連接器資訊名稱 |
| bad_events | 忽略的錯誤事件數量（例如空事件、不支援的 DDL 事件等）|
| total_events |已處理的事件總數（包括 bad_events）|
| batches_done | 已完成的批次數 |
| average_batch_size | 平均批次大小 (total_events / batches_done) |
| first_src_ts | 最後一個批次的第一個事件在外部資料庫產生的時間戳記（以毫秒為單位）|
| first_pg_ts | 最後一個批次的第一個事件應用到 PostgreSQL 的時間戳（以毫秒為單位）|
| last_src_ts | 最後一個批次的最後一個事件在外部資料庫產生的時間戳記（以毫秒為單位）|
| last_pg_ts | 最後一個批次的最後一個事件應用到 PostgreSQL 的時間戳（以毫秒為單位）|

## **快照統計信息**

透過 synchdb_snapstats 視圖取得：
```sql
select * from synchdb_snapstats;

  name   | tables |  rows  | snapshot_begin_ts | snapshot_end_ts
---------+--------+--------+-------------------+-----------------
 olrconn |      2 | 100032 |     1761160017191 |   1761160033250
(1 row)

```

列詳情：

| 字段 | 描述 |
|-|-|
| 名稱 | 由 `synchdb_add_conninfo()` 建立的關聯連接器資訊名稱 |
| 表 | 快照過程中遷移的表格模式數量 |
| 行 | 快照過程中遷移的行數 |
| 快照開始時間 | 快照開始時間的時間戳記（以毫秒為單位）|
| 快照結束時間 | 快照結束時間的時間戳記（以毫秒為單位）|

## **CDC 統計信息**

透過 synchdb_cdcstats 視圖取得：
```sql
select * from synchdb_cdcstats;

  name   | ddls | dmls | creates | updates | deletes | txs | truncates
---------+------+------+---------+---------+---------+-----+-----------
 olrconn |  161 |  124 |      10 |      47 |      67 | 562 |         0
(1 row)


```

列詳情：

| 字段 | 描述 |
|-|-|
| name | 由 `synchdb_add_conninfo()` 建立的關聯連接器資訊名稱 |
| ddls | 已完成的 DDL 運算元 |
| dmls | 已完成的 DML 運算元 |
| creates | CDC 階段完成的 CREATES 事件數 |
| updates | CDC 階段完成的 UPDATES 事件數 |
| deletes | CDC 階段完成的 DELETES 事件數 |
| txs | 處理的事務事件數，例如 begin 和 commit |
| truncates | 處理的 truncate 事件數 |

## **synchdb_reset_stats**

**用途**：重置指定連接器名稱的所有統計信息

```sql
SELECT synchdb_reset_stats('olrconn');
```