# Postgres -> Postgres

## 為 SynchDB 準備 PostgreSQL 資料庫

在使用 SynchDB 從 PostgreSQK 複製資料之前，需要按照[此處](../../getting-started/remote_database_setups/)中概述的步驟設定 PostgreSQL 伺服器。

## 建立 PostgreSQL 連接器

建立一個連接器，指向資料庫 `postgres` 和 schema `public` 下的所有資料表。

```sql
SELECT
synchdb_add_conninfo(
  'pgconn', '127.0.0.1', 5432,
  'myuser', 'mypass', 'postgres', 'public',
  'null', 'null', 'postgres');

```

## 初始快照 + CDC

使用 `initial` 模式啟動連接器將對所有指定表（本例中為所有表）執行初始快照。完成後，變更資料擷取 (CDC) 流程將開始串流新變更。

```sql

SELECT synchdb_start_engine_bgw('pgconn', 'initial');

或

SELECT synchdb_start_engine_bgw('pgconn');

```

此連接器首次運作時應處於 `initial snapshot` 階段：

```sql
postgres=# select * from synchdb_state_view where name='oracleconn';
    name    | connector_type |  pid   |      stage       |  state  |   err    |       last_dbz_offset
------------+----------------+--------+------------------+---------+----------+-----------------------------
 oracleconn | oracle         | 528146 | initial snapshot | polling | no error | offset file not flushed yet

```

將建立一個名為 `postgres` 的新模式，連接器串流傳輸的所有表都會複製到該模式下。

```sql
postgres=# set search_path=postgres;
SET
postgres=# \d
              List of relations
  Schema  |        Name        | Type  | Owner
----------+--------------------+-------+--------
 postgres | orders             | table | ubuntu

```

初始快照完成後，並且至少接收並處理了一個後續更改，連接器階段將從 `初始快照` 更改為 `變更資料擷取`。

```sql
postgres=# select * from synchdb_state_view where name='oracleconn';
  name  | connector_type |  pid   |        stage        |  state  |   err    |
    last_dbz_offset
--------+----------------+--------+---------------------+---------+----------+-------------------------------
-------------------------------------------------------
 pgconn | postgres       | 528746 | change data capture | polling | no error | {"lsn_proc":37051152,"messageType":"INSERT","lsn_commit":36989040,"lsn":3705
1152,"txId":1007,"ts_usec":1767208115470483}

```

這意味著連接器現在正在串流指定表的新變更。以「初始」模式重新啟動連接器將從上次成功點開始複製，並且不會重新執行初始快照。

## 僅初始快照，不執行 CDC

使用 `initial_only` 模式啟動連接器，將僅對所有指定資料表（本例中為所有資料表）執行初始快照，之後不會執行 CDC。

```sql

SELECT synchdb_start_engine_bgw('pgconn', 'initial_only');

```

連接器看起來仍然在輪詢，但不會捕獲任何更改，因為 Debzium 內部已停止 CDC。您可以選擇關閉 CDC。以 `initial_only` 模式重新啟動連接器不會重建表，因為它們已經建置完成。

## 僅捕獲表架構 + CDC

使用 `no_data` 模式啟動連接器，將僅執行架構捕獲，並在 PostgreSQL 中建立相應的表，並且不會複製現有表資料（跳過初始快照）。架構擷取完成後，連接器將進入 CDC 模式，並開始擷取表的後續變更。

```sql

SELECT synchdb_start_engine_bgw('pgconn', 'no_data');

```

以 `no_data` 模式重新啟動連接器不會重新建構模式，而是從上次成功捕獲的位置恢復 CDC。

## 僅 CDC

使用 `never` 模式啟動連接器將完全跳過模式擷取和初始快照，直接進入 CDC 模式擷取後續變更。請注意，連接器要求所有擷取表在以 `never` 模式啟動之前已在 PostgreSQL 中建立。如果表不存在，連接器在嘗試將 CDC 變更套用到不存在的表時會遇到錯誤。

```sql

SELECT synchdb_start_engine_bgw('pgconn', 'never');

```

以 `never` 模式重啟連接器將從上次成功捕獲的位置恢復 CDC。

## 始終執行初始快照 + CDC

使用 `always` 模式啟動連接器將始終擷取擷取表的模式，始終重新執行初始快照，然後執行 CDC。這類似於重置按鈕，因為在此模式下所有內容都將重建。請謹慎使用此模式，尤其是在捕獲大量表格時，因為捕獲過程可能需要很長時間才能完成。重建完成後，CDC 將照常恢復。

```sql

SELECT synchdb_start_engine_bgw('pgconn', 'always');

```

但是，可以使用連接器的 `snapshottable` 選項選擇部分錶來重新執行初始快照。符合 `snapshottable` 中條件的表將重新執行初始快照，否則將跳過其初始快照。如果 `snapshottable` 為空，則預設情況下，連接器的 `table` 選項中指定的所有表都會在 `always` 模式下重新執行初始快照。

此範例僅使連接器重新建立 `inventory.customers` 表的初始快照。所有其他表的快照都將被跳過。

```sql
UPDATE synchdb_conninfo
SET data = jsonb_set(data, '{snapshottable}', '"free.customers"')
WHERE name = 'pgconn';

```

初始快照完成後，CDC 將開始運行。以 `always` 模式重新啟動連接器將重複上述相同的過程。