# Oracle -> PostgreSQL

## **为 SynchDB 准备 Oracle 数据库**

在使用 SynchDB 从 Oracle 复制之前，需要按照[此处](../../getting-started/remote_database_setups/) 概述的步骤配置 Oracle。

请确保 SynchDB 需要复制的每个表的所有列都启用了补充日志数据。这是 SynchDB 正确处理更新和删除操作所必需的。

例如，以下命令为“customer”和“products”表的所有列启用了补充日志数据。请根据需要添加更多表。

```sql
ALTER TABLE customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
... etc
```

## **创建 Oracle 连接器**

创建一个连接器，指向 Oracle 中 `FREE` 数据库和 `DBZUSER` schema 下的所有表。
```sql
SELECT
synchdb_add_conninfo(
'oracleconn','127.0.0.1',1521,
'DBZUSER','dbz','FREE','DBZUSER',
'null','null','oracle');
```

## **初始快照**

SynchDB 中的「初始快照」（或表快照）是指複製所有指定表的表結構和初始資料。這類似於 PostgreSQL 邏輯複製中的「表同步」。當使用預設的 `initial` 模式啟動連接器時，它會在進入變更資料擷取 (CDC) 階段之前自動執行初始快照。可以使用 `no_data` 模式部分省略此步驟。有關所有快照選項，請參閱[此處](../../user-guide/start_stop_connector/)。

初始快照完成後，連接器在後續重新啟動時不會再次執行此操作，而是直接從上次未完成的偏移量處恢復 CDC。此行為由 Debezium 引擎管理的元資料檔案控制。有關元資料檔案的更多信息，請參閱[此處](../../architecture/metadata_files/)。

## **不同的連接器啟動模式**

### **初始快照 + CDC**

使用 `initial` 模式启动连接器将对所有指定表（在本例中为全部）执行初始快照。完成后，变更数据捕获 (CDC) 进程将开始流式传输新的变更。

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'initial');

或

SELECT synchdb_start_engine_bgw('oracleconn');
```

此连接器首次运行时，其阶段应处于 `initial snapper` 状态：
```sql
postgres=# select * from synchdb_state_view where name='oracleconn';
    name    | connector_type |  pid   |      stage       |  state  |   err    |       last_dbz_offset
------------+----------------+--------+------------------+---------+----------+-----------------------------
 oracleconn | oracle         | 528146 | initial snapshot | polling | no error | offset file not flushed yet
(1 row)

```

将创建一个名为“inventory”的新模式，并且连接器流式传输的所有表都将在该模式下复制。
```sql
postgres=# set search_path=free;
SET
postgres=# \d
              List of relations
 Schema |        Name        | Type  | Owner
--------+--------------------+-------+--------
 free   | orders             | table | ubuntu

```
初始快照完成后，如果至少接收并处理了一个后续更改，则连接器阶段应从“初始快照”更改为“更改数据捕获”。
```sql
postgres=# select * from synchdb_state_view where name='oracleconn';
    name    | connector_type |  pid   |        stage        |  state  |   err    |
    last_dbz_offset
------------+----------------+--------+---------------------+---------+----------+-------------------------------
-------------------------------------------------------
 oracleconn | oracle         | 528414 | change data capture | polling | no error | {"commit_scn":"3118146:1:02001
f00c0020000","snapshot_scn":"3081987","scn":"3118125"}

```
这意味着连接器现在正在流式传输指定表的新更改。以“initial”模式重启连接器将从上次成功点开始继续复制，并且不会重新运行初始快照。

### **仅初始快照，无CDC**

使用“initial_only”模式启动连接器将仅对所有指定表（在本例中为所有表）执行初始快照，之后将不再执行CDC。

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'initial_only');

```

连接器仍然会显示正在“轮询”，但由于Debzium内部已停止CDC，因此不会捕获任何更改。您可以选择关闭它。以“initial_only”模式重启连接器不会重建表，因为它们已经构建好了。

### **仅捕获表模式 + CDC**

使用 `no_data` 模式启动连接器将仅执行模式捕获，在 PostgreSQL 中构建相应的表，并且不会复制现有表数据（跳过初始快照）。模式捕获完成后，连接器将进入 CDC 模式，并开始捕获对表的后续更改。

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'no_data');

```

在 `no_data` 模式下重新启动连接器将不会再次重建模式，并且它将从上次成功点恢复 CDC。

### **始终执行初始快照 + CDC**

使用 `always` 模式启动连接器将始终捕获捕获表的模式，始终重做初始快照，然后转到 CDC。这类似于重置按钮，因为使用此模式将重建所有内容。请谨慎使用此模式，尤其是在捕获大量表时，这可能需要很长时间才能完成。重建后，CDC 将恢复正常。

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'always');

```

初始快照完成后，持续数据捕获 (CDC) 将开始。在 `always` 模式下重新启动连接器将重复上述过程。

## **Oracle 連接器的可用快照模式**

* initial (default)
* initial_only
* no_data
* always
* schemasync

## **使用 schemasync 模式預覽來源表和目標表關係**

在嘗試對當前表和資料（可能非常龐大）進行初始快照之前，可以在實際資料遷移之前「預覽」來源表和目標表之間的所有表和資料類型對應。這樣，您就有機會在實際遷移之前修改資料類型對應或物件名稱。這可以透過特殊的「schemasync」初始快照模式來實現。有關詳細範例，請參閱[对象映射工作流程](../../tutorial/object_mapping_workflow/)。

## **選擇性表同步**

### **選擇所需表並首次啟動同步**

表格選擇在連接器建立階段透過 `synchdb_add_conninfo()` 函數完成，該函數用於指定要從中複製的表列表（以完全限定名稱 (FQN) 表示，並以逗號分隔）。

例如，以下命令建立一個連接器，該連接器僅從遠端 MySQL 資料庫複製 `inventory.orders` 和 `inventory.products` 表中的變更。
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

首次啟動此連接器時，將觸發執行初始快照，並複製選定的表的架構和資料。

```sql
SELECT synchdb_start_engine_bgw('oracleconn');
```

### **驗證連接器狀態和表格**

檢查連接器狀態和新表格：
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

預設情況下，來源資料庫名稱會對應到目標資料庫的模式，並且字母大小寫策略為小寫，因此 `FREE.ORDERS` 在 PostgreSQL 中會變成 `free.orders`。表完成初始快照後，連接器將啟動 CDC 以串流傳輸這些表的後續變更。

### **運行時加入更多要複製的表**

上一節中的 `oracleconn` 已完成初始快照並取得了所選表格的表格模式。如果我們想要新增更多要複製的表，則需要通知 Debezium 引擎更新後的表格部分，並再次執行初始快照。具體操作如下：

1. 更新 `synchdb_conninfo` 表以包含其他表。

2. 在本例中，我們將 `DBZUSER.CUSTOMERS` 表加入同步清單：
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{table}', '"DBZUSER.ORDERS,DBZUSER.CUSTOMERS"') 
WHERE name = 'oracleconn';
```
3. 將快照模式設為“始終”，然後重新啟動連接器，以執行另一次初始快照：
```sql
DROP table free.orders;
SELECT synchdb_restart_connector('oracleconn', 'always');
```
這迫使 Debezium 在進行 CDC 串流之前，重新建立所有表格的快照，包括現有的 `free.orders` 表和新的 `free.customers` 表。這意味著，要新增表，必須刪除現有表（以防止重複表和主鍵錯誤），並重新建立整個初始快照。這相當冗餘，Debezium 建議使用增量快照來新增資料表，而無需重新建立快照。一旦我們將增量快照支援新增至 SynchDB，我們將更新此流程。

### **驗證更新後的表格**

現在，我們可以再次檢查我們的表：
```sql
postgres=# \dt free.*
          List of tables
 Schema      |  Name  | Type  | Owner
-------------+--------+-------+--------
 free        | orders | table | ubuntu
 customers   | orders | table | ubuntu

```