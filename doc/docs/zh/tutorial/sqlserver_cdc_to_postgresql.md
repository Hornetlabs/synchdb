# SQL Server 连接器

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

在嘗試對當前表和資料（可能非常龐大）進行初始快照之前，可以在實際資料遷移之前「預覽」來源表和目標表之間的所有表和資料類型對應。這樣，您有機會在實際遷移之前修改資料類型對應或物件名稱。這可以透過特殊的「schemasync」初始快照模式來實現。

請注意，您必須將 `synchdb.olr_snapshot_engine` 設定為 'fdw' 才能使用 `schemasync` 模式預覽表。

### **建立連接器並以 `schemasync` 模式啟動它**

`schemasync` 是一種特殊模式，它使連接器連接到遠端資料庫並嘗試僅同步指定表的模式。完成後，連接器將處於「暫停」狀態，使用者可以查看使用預設規則建立的所有資料表和資料類型，並根據需要進行變更。

```sql
SELECT synchdb_add_conninfo(
    'sqlserverconn', 
    '127.0.0.1', 
    1433, 
    'sa', 
    'Password!', 
    'testDB', 
    'dbo', 
    'null',
    'null', 
    'sqlserver'
);

SELECT synchdb_start_engine_bgw('sqlserverconn', 'schemasync');
```

### **確保連接器處於暫停狀態**

```sql
SELECT name, connector_type, pid, stage, state FROM synchdb_state_view WHERE name = 'sqlserverconn';
     name      | connector_type |   pid   |        stage        | state
---------------+----------------+---------+---------------------+--------
 sqlserverconn | sqlserver      | 1647884 | change data capture | paused

```

### **查看預設映射規則所建立的表**

```sql
SELECT * FROM synchdb_att_view WHERE name = 'sqlserverconn';
     name      |   type    | attnum |         ext_tbname          |        pg_tbname        | ext_attname  |  pg_attname  | ext_atttypename |  pg_atttypename   | transform
---------------+-----------+--------+-----------------------------+-------------------------+--------------+--------------+-----------------+-------------------+-----------
 sqlserverconn | sqlserver |      1 | testDB.dbo.customers        | testdb.customers        | id           | id           | int identity    | integer           |
 sqlserverconn | sqlserver |      2 | testDB.dbo.customers        | testdb.customers        | first_name   | first_name   | varchar         | character varying |
 sqlserverconn | sqlserver |      3 | testDB.dbo.customers        | testdb.customers        | last_name    | last_name    | varchar         | character varying |
 sqlserverconn | sqlserver |      4 | testDB.dbo.customers        | testdb.customers        | email        | email        | varchar         | character varying |
 sqlserverconn | sqlserver |      1 | testDB.dbo.orders           | testdb.orders           | order_number | order_number | int identity    | integer           |
 sqlserverconn | sqlserver |      2 | testDB.dbo.orders           | testdb.orders           | order_date   | order_date   | date            | date              |
 sqlserverconn | sqlserver |      3 | testDB.dbo.orders           | testdb.orders           | purchaser    | purchaser    | int             | integer           |
 sqlserverconn | sqlserver |      4 | testDB.dbo.orders           | testdb.orders           | quantity     | quantity     | int             | integer           |
 sqlserverconn | sqlserver |      5 | testDB.dbo.orders           | testdb.orders           | product_id   | product_id   | int             | integer           |
 sqlserverconn | sqlserver |      1 | testDB.dbo.products         | testdb.products         | id           | id           | int identity    | integer           |
 sqlserverconn | sqlserver |      2 | testDB.dbo.products         | testdb.products         | name         | name         | varchar         | character varying |
 sqlserverconn | sqlserver |      3 | testDB.dbo.products         | testdb.products         | description  | description  | varchar         | character varying |
 sqlserverconn | sqlserver |      4 | testDB.dbo.products         | testdb.products         | weight       | weight       | float           | real              |
 sqlserverconn | sqlserver |      1 | testDB.dbo.products_on_hand | testdb.products_on_hand | product_id   | product_id   | int             | integer           |
 sqlserverconn | sqlserver |      2 | testDB.dbo.products_on_hand | testdb.products_on_hand | quantity     | quantity     | int             | integer           |

```

### **定義自訂映射規則（如有需要）**

使用者可以使用 `synchdb_add_objmap` 函數建立自訂映射規則。此函數可用於對應表名、列名、資料類型，並定義資料轉換表達式規則。

```sql
SELECT synchdb_add_objmap('sqlserverconn','table','testDB.dbo.products','testdb.myproducts');
SELECT synchdb_add_objmap('sqlserverconn','column','testDB.dbo.customers.email','contact');
SELECT synchdb_add_objmap('sqlserverconn','datatype','testDB.dbo.products_on_hand.quantity','bigint|0');
SELECT synchdb_add_objmap('sqlserverconn','transform','testDB.dbo.products.name','''>>>>>'' || ''%d'' || ''<<<<<''');
```

以上內容意味著：

* 來源表“testDB.dbo.product”將會對應到目標表“testdb.myproducts”
* 來源列“testDB.dbo.customers.email”將會對應到目標表“contact”
* 來源列“testDB.dbo.products_on_hand.quantity”的資料型別將會對應到“bigint”
* 來源列「testDB.dbo.products.name」的資料將根據表達式進行轉換，其中 %d 為資料佔位符

### **回顧所有已建立的物件映射規則**

```sql
SELECT * FROM synchdb_objmap WHERE name = 'sqlserverconn';
     name      |  objtype  | enabled |                srcobj                |           dstobj
---------------+-----------+---------+--------------------------------------+----------------------------
 sqlserverconn | table     | t       | testDB.dbo.products                  | testdb.myproducts
 sqlserverconn | column    | t       | testDB.dbo.customers.email           | contact
 sqlserverconn | datatype  | t       | testDB.dbo.products_on_hand.quantity | bigint|0
 sqlserverconn | transform | t       | testDB.dbo.product.name              | '>>>>>' || '%d' || '<<<<<'

```

### **重新載入物件映射規則**

定義完所有自訂規則後，我們需要通知連接器載入這些規則。這將使連接器讀取並應用物件映射規則。如果連接器發現目前 PostgreSQL 值與物件對應值之間存在差異，它將嘗試修正映射。

```sql
SELECT synchdb_reload_objmap('sqlserverconn');

```

### **再次檢查 `synchdb_att_view` 是否有更改**

```sql
SELECT * from synchdb_att_view WHERE name = 'sqlserverconn';
     name      |   type    | attnum |         ext_tbname          |        pg_tbname        | ext_attname  |  pg_attname  | ext_atttypename |  pg_atttypename   |         transform

---------------+-----------+--------+-----------------------------+-------------------------+--------------+--------------+-----------------+-------------------+---------------------
-------
 sqlserverconn | sqlserver |      1 | testDB.dbo.customers        | testdb.customers        | id           | id           | int identity    | integer           |
 sqlserverconn | sqlserver |      2 | testDB.dbo.customers        | testdb.customers        | first_name   | first_name   | varchar         | character varying |
 sqlserverconn | sqlserver |      3 | testDB.dbo.customers        | testdb.customers        | last_name    | last_name    | varchar         | character varying |
 sqlserverconn | sqlserver |      4 | testDB.dbo.customers        | testdb.customers        | email        | contact      | varchar         | character varying |
 sqlserverconn | sqlserver |      1 | testDB.dbo.orders           | testdb.orders           | order_number | order_number | int identity    | integer           |
 sqlserverconn | sqlserver |      2 | testDB.dbo.orders           | testdb.orders           | order_date   | order_date   | date            | date              |
 sqlserverconn | sqlserver |      3 | testDB.dbo.orders           | testdb.orders           | purchaser    | purchaser    | int             | integer           |
 sqlserverconn | sqlserver |      4 | testDB.dbo.orders           | testdb.orders           | quantity     | quantity     | int             | integer           |
 sqlserverconn | sqlserver |      5 | testDB.dbo.orders           | testdb.orders           | product_id   | product_id   | int             | integer           |
 sqlserverconn | sqlserver |      1 | testDB.dbo.products         | testdb.products         | id           | id           | int identity    | integer           |
 sqlserverconn | sqlserver |      2 | testDB.dbo.products         | testdb.products         | name         | name         | varchar         | character varying | '>>>>>' || '%d' || '
<<<<<'
 sqlserverconn | sqlserver |      3 | testDB.dbo.products         | testdb.products         | description  | description  | varchar         | character varying |
 sqlserverconn | sqlserver |      4 | testDB.dbo.products         | testdb.products         | weight       | weight       | float           | real              |
 sqlserverconn | sqlserver |      1 | testDB.dbo.products_on_hand | testdb.products_on_hand | product_id   | product_id   | int             | integer           |
 sqlserverconn | sqlserver |      2 | testDB.dbo.products_on_hand | testdb.products_on_hand | quantity     | quantity     | int             | bigint            |


```

### **恢復連接器或重新建立整個快照**

確認物件映射正確後，我們可以恢復連接器。請注意，復原操作只會傳輸新的表更改，不會複製表中的現有資料。

```sql
SELECT synchdb_resume_engine('sqlserverconn');

```

要擷取表中的現有數據，我們也可以使用新的物件映射規則重新建立整個快照。

```sql
SELECT synchdb_stop_engine_bgw('sqlserverconn');
SELECT synchdb_start_engine_bgw('sqlserverconn', 'always');

```

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

## 安全连接

### **配置安全连接**

为了确保与远程数据库的连接安全，我们需要为 `synchdb_add_conninfo` 创建的连接器配置额外的 SSL 相关参数。SSL 证书和私钥必须打包为 Java 密钥库文件，并附带密码。这些信息随后会通过 synchdb_add_extra_conninfo() 传递给 SynchDB。

### **synchdb_add_extra_conninfo**

**用途**：为 `synchdb_add_conninfo` 创建的现有连接器配置额外的连接器参数

| 参数 | 描述 | 必需 | 示例 | 备注 |
|:-:|:-|:-:|:-|:-|
| `name` | 此连接器的唯一标识符 | ✓ | `'mysqlconn'` | 必须在所有连接器中唯一 |
| `ssl_mode` | SSL 模式 | ☐ | `'verify_ca'` |可以是以下之一：<br><ul><li>“disabled”- 不使用 SSL。</li><li>“preferred”- 如果服务器支持，则使用 SSL。</li><li>“required”- 必须使用 SSL 建立连接。</li><li>“verify_ca”- 连接器与服务器建立 TLS，并将根据配置的信任库验证服务器的 TLS 证书。</li><li>“verify_identity”- 与 verify_ca 行为相同，但它还会检查服务器证书的通用名称以匹配系统的主机名。|
| `ssl_keystore` | 密钥库路径 | ☐ | `/path/to/keystore` | 密钥库文件的路径 |
| `ssl_keystore_pass` | 密钥库密码 | ☐ | `'mykeystorepass'` | 访问密钥库文件的密码 |
| `ssl_truststore` | 信任库路径 | ☐ | `'/path/to/truststore'` | 信任库文件路径 |
| `ssl_truststore_pass` | 信任库密码 | ☐ | `'mytruststorepass'` | 访问信任库文件的密码 |

```sql
SELECT synchdb_add_extra_conninfo('sqlserverconn', 'verify_ca', '/path/to/keystore', 'mykeystorepass', '/path/to/truststore', 'mytruststorepass');
```

### **synchdb_del_extra_conninfo**

**用途**：删除由 `synchdb_add_extra_conninfo` 创建的额外连接器参数
```sql
SELECT synchdb_del_extra_conninfo('sqlserverconn');
```

## 自定义起始偏移量值

起始偏移量值代表开始复制的点，类似于 PostgreSQL 的恢复 LSN。当 Debezium 运行引擎启动时，它将从这个偏移量值开始复制。将此偏移量值设置为较早的值将导致 Debezium 运行引擎从较早的记录开始复制，可能会复制重复的数据记录。在设置 Debezium 的起始偏移量值时，我们应该格外谨慎。

### **记录可设置的偏移量值**

在操作过程中，Debezium 运行引擎将生成新的偏移量并将其刷新到磁盘。最后刷新的偏移量可以通过 `synchdb_state_view()` 实用命令检索：

```
postgres=# select name, last_dbz_offset from synchdb_state_view;
     name      |                                           last_dbz_offset
---------------+------------------------------------------------------------------------------------------------------
 sqlserverconn | {"commit_lsn":"0000006a:00006608:0003","snapshot":true,"snapshot_completed":false}

```

我们应该定期保存这些值，这样如果遇到问题，我们就知道过去可以设置的偏移量位置，以恢复复制操作。

### **暂停连接器**

在设置新的偏移量值之前，连接器必须处于 `paused`（暂停）状态。

使用 `synchdb_pause_engine()` SQL 函数暂停正在运行的连接器。这将停止 Debezium 运行引擎从异构数据库复制。当暂停时，可以使用 `synchdb_set_offset()` SQL 例程更改 Debezium 连接器的偏移量值，以从过去的特定点开始复制。它以 `conninfo_name` 作为参数，可以从 `synchdb_get_state()` 视图的输出中找到。

例如：

```sql
SELECT synchdb_pause_engine('sqlserverconn');
```

### **设置新的偏移量**

使用 `synchdb_set_offset()` SQL 函数更改连接器工作进程的起始偏移量。只有当连接器处于 `paused` 状态时才能执行此操作。该函数接受两个参数，`conninfo_name` 和 `有效的偏移量字符串`，这两个参数都可以从 `synchdb_get_state()` 视图的输出中找到。

例如：

```sql
SELECT synchdb_set_offset('sqlserverconn', '{"commit_lsn":"0000006a:00006608:0003","snapshot":true,"snapshot_completed":false}');
```

### **恢复连接器**

使用 `synchdb_resume_engine()` SQL 函数从暂停状态恢复 Debezium 操作。此函数以 `连接器名称` 作为其唯一参数，可以从 `synchdb_get_state()` 视图的输出中找到。恢复的 Debezium 运行引擎将从新设置的偏移量值开始复制。

例如：

```sql
SELECT synchdb_resume_engine('sqlserverconn');
```