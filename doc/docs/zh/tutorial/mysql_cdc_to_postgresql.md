# MySQL -> PostgreSQL

## **为 SynchDB 准备 MySQL 数据库**

在使用 SynchDB 从 MySQL 复制之前，需要按照[此处](../../getting-started/remote_database_setups/) 概述的步骤配置 MySQL

## **创建 MySQL 连接器**

创建一个连接器，该连接器指向 MySQL 中 `inventory` 数据库下的所有表。
```sql
SELECT synchdb_add_conninfo(
'mysqlconn', '127.0.0.1', 3306, 'mysqluser',
'mysqlpwd', 'inventory', 'postgres',
'null', 'null', 'mysql');
```
## **初始快照**

SynchDB 中的「初始快照」（或表快照）是指複製所有指定表的表結構和初始資料。這類似於 PostgreSQL 邏輯複製中的「表同步」。當使用預設的 `initial` 模式啟動連接器時，它會在進入變更資料擷取 (CDC) 階段之前自動執行初始快照。可以使用 `never` 模式完全省略此步驟，或使用 `no_data` 模式部分省略此步驟。有關所有快照選項，請參閱[此處](../../user-guide/start_stop_connector/)。

初始快照完成後，連接器在後續重新啟動時不會再次執行此操作，而是直接從上次未完成的偏移量處恢復 CDC。此行為由 Debezium 引擎管理的元資料檔案控制。有關元資料檔案的更多信息，請參閱[此處](../../architecture/metadata_files/)。

## **不同的連接器啟動模式**

### **初始快照 + CDC**

使用 `initial` 模式启动连接器将对所有指定表（在本例中为所有表）执行初始快照。完成后，变更数据捕获 (CDC) 进程将开始流式传输新的变更。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'initial');

或

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

初始快照完成后，如果至少有一条后续更改被接收并处理，连接器阶段将从“初始快照”切换为“变更数据捕获”。
```sql
postgres=# select * from synchdb_state_view;
    name    | connector_type |  pid   |        stage        |  state  |   err    |                      last_dbz_o
ffset
------------+----------------+--------+---------------------+---------+----------+--------------------------------
----------------------------
 mysqlconn  | mysql          | 522195 | change data capture | polling | no error | {"ts_sec":1750375008,"file":"my
sql-bin.000003","pos":1500}

```

这意味着连接器现在正在流式传输指定表的新更改。以“初始”模式重新启动连接器将从上次成功点开始继续复制，并且不会重新运行初始快照。

### **仅初始快照，无 CDC**

使用 `initial_only` 模式启动连接器将仅对所有指定表（在本例中为全部）执行初始快照，之后将不再执行 CDC。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'initial_only');

```

连接器似乎仍在“轮询”其他连接器，但不会捕获任何更改，因为 Debzium 内部已停止 CDC。您可以选择关闭它。在 `initial_only` 模式下重新启动连接器不会重建表，因为它们已经构建好了。

```sql
postgres=# select * from synchdb_state_view;
    name    | connector_type |  pid   |      stage       |  state  |   err    |       last_dbz_offset
------------+----------------+--------+------------------+---------+----------+-----------------------------
 mysqlconn  | mysql          | 522330 | initial snapshot | polling | no error | offset file not flushed yet

```

### **仅捕获表模式 + CDC**

使用 `no_data` 模式启动连接器将仅执行模式捕获，在 PostgreSQL 中构建相应的表，并且不会复制现有表数据（跳过初始快照）。模式捕获完成后，连接器将进入 CDC 模式，并开始捕获对表的后续更改。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'no_data');

```

在 `no_data` 模式下重新启动连接器将不会再次重建模式，并且它将从上次成功点恢复 CDC。

### **仅 CDC**

使用 `never` 模式启动连接器将完全跳过模式捕获和初始快照，并进入 CDC 模式以捕获后续更改。请注意，连接器要求在以 `never` 模式启动之前，所有捕获表都已在 PostgreSQL 中创建。如果表不存在，连接器在尝试将 CDC 更改应用于不存在的表时将遇到错误。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'never');

```

以“never”模式重启连接器将从上次成功点开始恢复 CDC。

### **始终执行初始快照 + CDC**

使用 `always` 模式启动连接器将始终捕获捕获表的模式，始终重做初始快照，然后转到 CDC。这类似于重置按钮，因为使用此模式将重建所有内容。请谨慎使用，尤其是在捕获大量表时，这可能需要很长时间才能完成。重建后，CDC 将恢复正常。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'always');

```

但是，可以使用连接器的 `snapshottable` 选项选择部分表来重做初始快照。符合 `snapshottable` 中条件的表将重做初始快照，否则将跳过其初始快照。如果 `snapshottable` 为 null 或为空，默认情况下，连接器 `table` 选项中指定的所有表都将在 `always` 模式下重做初始快照。

此示例使连接器仅重做 `inventory.customers` 表的初始快照。所有其他表的快照将被跳过。
```sql
UPDATE synchdb_conninfo
SET data = jsonb_set(data, '{snapshottable}', '"inventory.customers"')
WHERE name = 'mysqlconn';
```

初始快照完成后，CDC 将开始。在 `always` 模式下重新启动连接器将重复上述过程。

### **MySQL 連接器的可用快照模式**

* initial (default)
* initial_only
* no_data
* never
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

首次啟動此連接器時，將觸發執行初始快照，並複製選定的 2 個表的架構和資料。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn');
```

### **驗證連接器狀態和表格**

檢查連接器狀態和新表格：

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

快照完成後，`mysqlconn` 連接器將繼續擷取 `inventory.orders` 和 `inventory.products` 表的後續變更。

### **運行時加入更多要複製的表**

上一節中的 `mysqlconn` 已完成初始快照並取得了所選表格的表格結構。如果我們想要新增更多要複製的表，則需要通知 Debezium 引擎更新了表結構，並再次執行初始快照。具體操作如下：

1. 更新 `synchdb_conninfo` 表以包含其他表。

2. 在本例中，我們將 `inventory.customers` 表加入同步清單：
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{table}', '"inventory.orders,inventory.products,inventory.customers"') 
WHERE name = 'mysqlconn';
```
3. 配置快照表參數，使其只包含新表 `inventory.customers`，這樣 SynchDB 就不會嘗試重建已經完成快照的 2 個表。
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{snapshottable}', '"inventory.customers"') 
WHERE name = 'mysqlconn';
``` 
4. 將快照模式設為“始終”，然後重新啟動連接器，以執行另一次初始快照：
```sql
SELECT synchdb_restart_connector('mysqlconn', 'always');
```
這樣一來，Debezium 就只能對新表 `inventory.customers` 進行快照，而舊表 `inventory.orders` 和 `inventory.products` 則保持不變。快照完成後，所有表格的 CDC 操作將會恢復。

### **驗證更新後的表格**

現在，我們可以再次檢查表格：
```sql
postgres=# \dt inventory.*
             List of tables
  Schema   |   Name    | Type  | Owner
-----------+-----------+-------+--------
 inventory | customers | table | ubuntu
 inventory | orders    | table | ubuntu
 inventory | products  | table | ubuntu

```