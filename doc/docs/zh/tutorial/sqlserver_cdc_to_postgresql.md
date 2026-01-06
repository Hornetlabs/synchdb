# SQL Server -> PostgreSQL

## **为 SynchDB 准备 SQL Server 数据库**

在使用 SynchDB 从 SQL Server 进行复制之前，需要按照[此处](../../getting-started/remote_database_setups/) 概述的步骤配置 SQL Server。

请确保所需的表已在 SQL Server 中启用为 CDC 表。您可以在 SQL Server 客户端上运行以下命令，为“dbo.customer”、“dbo.district”和“dbo.history”启用 CDC。您将根据需要继续添加新表。

```sql
USE testDB
GO
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customer', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'district', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'history', @role_name = NULL, @supports_net_changes = 0;
GO
```

## **初始快照**

SynchDB 中的「初始快照」（或表快照）是指複製所有指定表的表結構和初始資料。這類似於 PostgreSQL 邏輯複製中的「表同步」。當使用預設的 `initial` 模式啟動連接器時，它會在進入變更資料擷取 (CDC) 階段之前自動執行初始快照。可以使用 `no_data` 模式部分省略此步驟。有關所有快照選項，請參閱[此處](../../user-guide/start_stop_connector/)。

初始快照完成後，連接器在後續重新啟動時不會再次執行此操作，而是直接從上次未完成的偏移量處恢復 CDC。此行為由 Debezium 引擎管理的元資料檔案控制。有關元資料檔案的更多信息，請參閱[此處](../../architecture/metadata_files/)。

## **不同的連接器啟動模式**

### **创建 SQL Server 连接器**

创建一个连接器，该连接器指向 SQL Server 中 `testDB` 数据库下的所有表。
```sql
SELECT
synchdb_add_conninfo(
'sqlserverconn', '127.0.0.1', 1433,
'sa', 'Password!', 'testDB', 'postgres',
'null', 'null', 'sqlserver');
```

### **初始快照 + 变更数据捕获 (CDC)**

使用 `initial` 模式启动连接器将对所有指定的表（在本例中为所有表）执行初始快照。完成后，变更数据捕获 (CDC) 过程将开始流式传输新的变更。

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'initial');

或

SELECT synchdb_start_engine_bgw('sqlserverconn');
```

此连接器首次运行时，其阶段应处于“初始快照”状态：
```sql
postgres=# select * from synchdb_state_view where name='sqlserverconn';
     name      | connector_type |  pid   |      stage       |  state  |   err    |       last_dbz_offset
---------------+----------------+--------+------------------+---------+----------+-----------------------------
 sqlserverconn | sqlserver      | 526003 | initial snapshot | polling | no error | offset file not flushed yet
(1 row)


```

将创建一个名为“testdb”的新模式，并且连接器流式传输的所有表都将在该模式下复制。
```sql
postgres=# set search_path=public,testdb;
SET
postgres=# \d
                  List of relations
 Schema |          Name           |   Type   | Owner
--------+-------------------------+----------+--------
 public | synchdb_att_view        | view     | ubuntu
 public | synchdb_attribute       | table    | ubuntu
 public | synchdb_conninfo        | table    | ubuntu
 public | synchdb_objmap          | table    | ubuntu
 public | synchdb_state_view      | view     | ubuntu
 public | synchdb_stats_view      | view     | ubuntu
 testdb | customers               | table    | ubuntu
 testdb | customers_id_seq        | sequence | ubuntu
 testdb | orders                  | table    | ubuntu
 testdb | orders_order_number_seq | sequence | ubuntu
 testdb | products                | table    | ubuntu
 testdb | products_id_seq         | sequence | ubuntu
 testdb | products_on_hand        | table    | ubuntu
(13 rows)

```

初始快照完成后，如果至少接收并处理了一个后续更改，则连接器阶段应从“初始快照”更改为“更改数据捕获”。
```sql
postgres=# select * from synchdb_state_view where name='sqlserverconn';
     name      | connector_type |  pid   |        stage        |  state  |   err    |
             last_dbz_offset
---------------+----------------+--------+---------------------+---------+----------+-----------------------------
----------------------------------------------------------------------
 sqlserverconn | sqlserver      | 526290 | change data capture | polling | no error | {"event_serial_no":1,"commit
_lsn":"0000002b:000004d8:0004","change_lsn":"0000002b:000004d8:0003"}
(1 row

```
这意味着连接器现在正在流式传输指定表的新更改。以“initial”模式重启连接器将从上次成功点开始继续复制，并且不会重新运行初始快照。

### **仅初始快照，无CDC**

使用“initial_only”模式启动连接器将仅对所有指定表（在本例中为所有表）执行初始快照，之后将不再执行CDC。

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'initial_only');

```

连接器仍然会显示正在“轮询”，但由于Debzium内部已停止CDC，因此不会捕获任何更改。您可以选择关闭它。以“initial_only”模式重启连接器不会重建表，因为它们已经构建好了。

### **仅捕获表模式 + CDC**

使用 `no_data` 模式启动连接器将仅执行模式捕获，在 PostgreSQL 中构建相应的表，并且不会复制现有表数据（跳过初始快照）。模式捕获完成后，连接器将进入 CDC 模式，并开始捕获对表的后续更改。

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'no_data');

```

在 `no_data` 模式下重新启动连接器将不会再次重建模式，并且它将从上次成功点恢复 CDC。

### **始终执行初始快照 + CDC**

使用 `always` 模式启动连接器将始终捕获捕获表的模式，始终重做初始快照，然后转到 CDC。这类似于重置按钮，因为使用此模式将重建所有内容。请谨慎使用此模式，尤其是在捕获大量表时，这可能需要很长时间才能完成。重建后，CDC 将恢复正常。

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn', 'always');

```

但是，可以使用连接器的 `snapshottable` 选项选择部分表来重做初始快照。符合 `snapshottable` 中条件的表将重做初始快照，否则将跳过其初始快照。如果 `snapshottable` 为 null 或为空，默认情况下，连接器的 `table` 选项中指定的所有表将在 `always` 模式下重做初始快照。

此示例使连接器仅重做 `inventory.customers` 表的初始快照。所有其他表的快照将被跳过。
```sql
UPDATE synchdb_conninfo
SET data = jsonb_set(data, '{snapshottable}', '"inventory.customers"')
WHERE name = 'sqlserverconn';
```

初始快照完成后，CDC 将开始。在 `always` 模式下重新启动连接器将重复上述过程。

## **SQL Server 連接器的可用快照模式**

* initial (default)
* initial_only
* no_data
* always
* schemasync

## **使用 schemasync 模式預覽來源表和目標表關係**

在嘗試對當前表和資料（可能非常龐大）進行初始快照之前，可以在實際資料遷移之前「預覽」來源表和目標表之間的所有表和資料類型對應。這樣，您就有機會在實際遷移之前修改資料類型對應或物件名稱。這可以透過特殊的「schemasync」初始快照模式來實現。有關詳細範例，請參閱[对象映射工作流程](../../tutorial/object_mapping_workflow/)。

## **選擇性表同步**

### **選擇所需表並首次啟動**

表格選擇在連接器建立階段透過 `synchdb_add_conninfo()` 函數完成，該函數用於指定要從中複製的表列表（以完全限定名稱 (FQN) 表示，並以逗號分隔）。

例如，以下命令建立一個連接器，該連接器僅複製遠端 SQL Server 資料庫中 `dbo.orders` 資料表的變更。
```sql
SELECT synchdb_add_conninfo(
    'sqlserverconn', 
    '127.0.0.1', 
    1433, 
    'sa', 
    'Password!', 
    'testDB', 
    'dbo', 
    'dbo.orders,dbo.products',
    'null', 
    'sqlserver'
);
```

首次啟動此連接器時，將觸發執行初始快照，並複製選定的 2 個表的架構和資料。

```sql
SELECT synchdb_start_engine_bgw('sqlserverconn');
```

### **驗證連接器狀態和表格**

檢查連接器狀態和新表格：
```sql
postgres=# Select name, state, err from synchdb_state_view;
     name      |  state  |   err
---------------+---------+----------
 sqlserverconn | polling | no error

postgres=# \dt testdb.*
           List of tables
 Schema |   Name   | Type  | Owner
--------+----------+-------+--------
 testdb | orders   | table | ubuntu
 testdb | products | table | ubuntu

```

預設情況下，來源資料庫名稱會對應到目標資料庫的模式，因此 `dbo.orders` 在 PostgreSQL 中會變成 `testdb.orders`。表完成初始快照後，連接器將啟動 CDC 以串流傳輸這些表的後續變更。

### **運行時加入更多要複製的表**

如果我們想要新增更多要複製的表，需要通知 Debezium 引擎更新了表格部分，並重​​新執行初始快照。具體操作如下：

1. 更新 `synchdb_conninfo` 表，使其包含其他表格。

2. 在本例中，我們將 `dbo.customers` 表加入同步清單：
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{table}', '"dbo.orders,dbo.products,dbo.customers"') 
WHERE name = 'sqlserverconn';
```
3. 將快照模式設為“始終”，然後重新啟動連接器，以執行另一次初始快照：
```sql
DROP table testdb.orders, testdb.products;
SELECT synchdb_restart_connector('sqlserverconn', 'always');
```

這迫使 Debezium 重新建立所有表的快照，包括舊表 `dbo.orders` 和 `dbo.products`，以及在進行 CDC 串流傳輸之前建立的新表。這意味著，要新增第三個表，必須刪除現有表（以防止重複表和主鍵錯誤），並重新建立整個初始快照。這相當冗餘，Debezium 建議使用增量快照來新增資料表，而無需重新建立快照。一旦我們將增量快照支援新增至 SynchDB，我們將更新此過程。

### **驗證更新後的表格**

現在，我們可以再次檢查我們的表：
```sql
postgres=# \dt "testDB".*
           List of tables
 Schema |   Name    | Type  | Owner
--------+-----------+-------+--------
 testDB | customers | table | ubuntu
 testDB | orders    | table | ubuntu
 testDB | products  | table | ubuntu

```