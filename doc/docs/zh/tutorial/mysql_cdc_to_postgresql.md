# MySQL 连接器

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

在嘗試對當前表和資料（可能非常龐大）進行初始快照之前，可以在實際資料遷移之前「預覽」來源表和目標表之間的所有表和資料類型對應。這樣，您有機會在實際遷移之前修改資料類型對應或物件名稱。這可以透過特殊的「schemasync」初始快照模式來實現。

請注意，您必須將 `synchdb.olr_snapshot_engine` 設定為 'fdw' 才能使用 `schemasync` 模式預覽表。

### **建立連接器並以 `schemasync` 模式啟動它**

`schemasync` 是一種特殊模式，它使連接器連接到遠端資料庫並嘗試僅同步指定表的模式。完成後，連接器將處於「暫停」狀態，使用者可以查看使用預設規則建立的所有資料表和資料類型，並根據需要進行變更。

```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', 
    '127.0.0.1', 
    3306, 
    'mysqluser', 
    'mysqlpwd', 
    'inventory', 
    'null', 
    'null', 
    'null', 
    'mysql'
);

SELECT synchdb_start_engine_bgw('mysqlconn', 'schemasync');
```
### **確保連接器處於暫停狀態**

```sql
SELECT name, connector_type, pid, stage, state FROM synchdb_state_view WHERE name = 'mysqlconn';
   name    | connector_type |   pid   |        stage        | state
-----------+----------------+---------+---------------------+--------
 mysqlconn | mysql          | 1644218 | change data capture | paused

```

### **查看預設映射規則所建立的表**

```sql
SELECT * FROM synchdb_att_view WHERE name = 'mysqlconn';
   name    |   type   | attnum |         ext_tbname         |         pg_tbname          | ext_attname  |  pg_attname  |  ext_atttypename  |  pg_atttypename   |         transform

-----------+----------+--------+----------------------------+----------------------------+--------------+--------------+-------------------+-------------------+----------------------
------
 mysqlconn | mysql    |      1 | inventory.addresses        | inventory.addresses        | id           | id           | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.addresses        | inventory.addresses        | customer_id  | customer_id  | int               | integer           |
 mysqlconn | mysql    |      3 | inventory.addresses        | inventory.addresses        | street       | street       | varchar           | character varying |
 mysqlconn | mysql    |      4 | inventory.addresses        | inventory.addresses        | city         | city         | varchar           | character varying |
 mysqlconn | mysql    |      5 | inventory.addresses        | inventory.addresses        | state        | state        | varchar           | character varying |
 mysqlconn | mysql    |      6 | inventory.addresses        | inventory.addresses        | zip          | zip          | varchar           | character varying |
 mysqlconn | mysql    |      7 | inventory.addresses        | inventory.addresses        | type         | type         | enum              | text              |
 mysqlconn | mysql    |      1 | inventory.customers        | inventory.customers        | id           | id           | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.customers        | inventory.customers        | first_name   | first_name   | varchar           | character varying |
 mysqlconn | mysql    |      3 | inventory.customers        | inventory.customers        | last_name    | last_name    | varchar           | character varying |
 mysqlconn | mysql    |      4 | inventory.customers        | inventory.customers        | email        | email        | varchar           | character varying |
 mysqlconn | mysql    |      1 | inventory.geom             | inventory.geom             | id           | id           | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.geom             | inventory.geom             | g            | g            | geometry          | text              |
 mysqlconn | mysql    |      3 | inventory.geom             | inventory.geom             | h            | h            | geometry          | text              |
 mysqlconn | mysql    |      1 | inventory.orders           | inventory.orders           | order_number | order_number | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.orders           | inventory.orders           | order_date   | order_date   | date              | date              |
 mysqlconn | mysql    |      3 | inventory.orders           | inventory.orders           | purchaser    | purchaser    | int               | integer           |
 mysqlconn | mysql    |      4 | inventory.orders           | inventory.orders           | quantity     | quantity     | int               | integer           |
 mysqlconn | mysql    |      5 | inventory.orders           | inventory.orders           | product_id   | product_id   | int               | integer           |
 mysqlconn | mysql    |      1 | inventory.products         | inventory.products         | id           | id           | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.products         | inventory.products         | name         | name         | varchar           | character varying |
 mysqlconn | mysql    |      3 | inventory.products         | inventory.products         | description  | description  | varchar           | character varying |
 mysqlconn | mysql    |      4 | inventory.products         | inventory.products         | weight       | weight       | float             | real              |
 mysqlconn | mysql    |      1 | inventory.products_on_hand | inventory.products_on_hand | product_id   | product_id   | int               | integer           |
 mysqlconn | mysql    |      2 | inventory.products_on_hand | inventory.products_on_hand | quantity     | quantity     | int               | integer           |

```
### **定義自訂映射(如有需要)**

```sql
SELECT synchdb_add_objmap('mysqlconn','table','inventory.products','inventory.myproducts');
SELECT synchdb_add_objmap('mysqlconn','column','inventory.customers.email','contact');
SELECT synchdb_add_objmap('mysqlconn','datatype','inventory.orders.quantity','bigint|0');
SELECT synchdb_add_objmap('mysqlconn','transform','inventory.products.name','''>>>>>'' || ''%d'' || ''<<<<<''');
```

以上內容意味著：

* 來源表“inventory.products”將會對應到目標表“inventory.myproducts”
* 來源列“inventory.customers.email”將會對應到目標表“contact”
* 來源列“inventory.orders.quantity”的資料類型將會對應到“bigint”
* 來源列「inventory.products.name」的資料將根據表達式進行轉換，其中 %d 為資料佔位符

### **回顧所有已建立的物件映射規則**

```sql
SELECT * FROM synchdb_objmap WHERE name = 'mysqlconn';
   name    |  objtype  | enabled |          srcobj           |           dstobj
-----------+-----------+---------+---------------------------+----------------------------
 mysqlconn | table     | t       | inventory.products        | inventory.myproducts
 mysqlconn | column    | t       | inventory.customers.email | contact
 mysqlconn | datatype  | t       | inventory.orders.quantity | bigint|0
 mysqlconn | transform | t       | inventory.products.name   | '>>>>>' || '%d' || '<<<<<'

```

### **重新載入物件映射規則**

所有自訂規則定義完畢後，我們需要通知連接器載入這些規則。這將使連接器讀取並應用物件映射規則。如果連接器發現目前 PostgreSQL 值與物件對應值之間存在差異，它將嘗試修正映射。

```sql
SELECT synchdb_reload_objmap('mysqlconn');

```

### **再次檢查 `synchdb_att_view` 是否有更改**

```sql
   name    | type  | attnum |         ext_tbname         |         pg_tbname          | ext_attname  |  pg_attname  | ext_atttypename |  pg_atttypename   |         transform

-----------+-------+--------+----------------------------+----------------------------+--------------+--------------+-----------------+-------------------+---------------------------
-
 mysqlconn | mysql |      1 | inventory.addresses        | inventory.addresses        | id           | id           | int             | integer           |
 mysqlconn | mysql |      2 | inventory.addresses        | inventory.addresses        | customer_id  | customer_id  | int             | integer           |
 mysqlconn | mysql |      3 | inventory.addresses        | inventory.addresses        | street       | street       | varchar         | character varying |
 mysqlconn | mysql |      4 | inventory.addresses        | inventory.addresses        | city         | city         | varchar         | character varying |
 mysqlconn | mysql |      5 | inventory.addresses        | inventory.addresses        | state        | state        | varchar         | character varying |
 mysqlconn | mysql |      6 | inventory.addresses        | inventory.addresses        | zip          | zip          | varchar         | character varying |
 mysqlconn | mysql |      7 | inventory.addresses        | inventory.addresses        | type         | type         | enum            | text              |
 mysqlconn | mysql |      1 | inventory.customers        | inventory.customers        | id           | id           | int             | integer           |
 mysqlconn | mysql |      2 | inventory.customers        | inventory.customers        | first_name   | first_name   | varchar         | character varying |
 mysqlconn | mysql |      3 | inventory.customers        | inventory.customers        | last_name    | last_name    | varchar         | character varying |
 mysqlconn | mysql |      4 | inventory.customers        | inventory.customers        | email        | contact      | varchar         | character varying |
 mysqlconn | mysql |      1 | inventory.geom             | inventory.geom             | id           | id           | int             | integer           |
 mysqlconn | mysql |      2 | inventory.geom             | inventory.geom             | g            | g            | geometry        | text              |
 mysqlconn | mysql |      3 | inventory.geom             | inventory.geom             | h            | h            | geometry        | text              |
 mysqlconn | mysql |      1 | inventory.orders           | inventory.orders           | order_number | order_number | int             | integer           |
 mysqlconn | mysql |      2 | inventory.orders           | inventory.orders           | order_date   | order_date   | date            | date              |
 mysqlconn | mysql |      3 | inventory.orders           | inventory.orders           | purchaser    | purchaser    | int             | integer           |
 mysqlconn | mysql |      4 | inventory.orders           | inventory.orders           | quantity     | quantity     | int             | bigint            |
 mysqlconn | mysql |      5 | inventory.orders           | inventory.orders           | product_id   | product_id   | int             | integer           |
 mysqlconn | mysql |      1 | inventory.products         | inventory.myproducts       | id           | id           | int             | integer           |
 mysqlconn | mysql |      2 | inventory.products         | inventory.myproducts       | name         | name         | varchar         | character varying | '>>>>>' || '%d' || '<<<<<'
 mysqlconn | mysql |      3 | inventory.products         | inventory.myproducts       | description  | description  | varchar         | character varying |
 mysqlconn | mysql |      4 | inventory.products         | inventory.myproducts       | weight       | weight       | float           | real              |
 mysqlconn | mysql |      1 | inventory.products_on_hand | inventory.products_on_hand | product_id   | product_id   | int             | integer           |
 mysqlconn | mysql |      2 | inventory.products_on_hand | inventory.products_on_hand | quantity     | quantity     | int             | integer           |


```

### **恢復連接器或重新建立整個快照**

確認物件映射正確後，我們可以恢復連接器。請注意，復原操作只會傳輸新的表更改，不會複製表中的現有資料。

```sql
SELECT synchdb_resume_engine('mysqlconn');

```

要擷取表中的現有數據，我們也可以使用新的物件映射規則重新建立整個快照。

```sql
SELECT synchdb_stop_engine_bgw('mysqlconn');
SELECT synchdb_start_engine_bgw('mysqlconn', 'always');

```

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
SELECT synchdb_add_extra_conninfo('mysqlconn', 'verify_ca', '/path/to/keystore', 'mykeystorepass', '/path/to/truststore', 'mytruststorepass');
```

### **synchdb_del_extra_conninfo**

**用途**：删除由 `synchdb_add_extra_conninfo` 创建的额外连接器参数
```sql
SELECT synchdb_del_extra_conninfo('mysqlconn');
```

## 自定义起始偏移量值

起始偏移量值代表开始复制的点，类似于 PostgreSQL 的恢复 LSN。当 Debezium 运行引擎启动时，它将从这个偏移量值开始复制。将此偏移量值设置为较早的值将导致 Debezium 运行引擎从较早的记录开始复制，可能会复制重复的数据记录。在设置 Debezium 的起始偏移量值时，我们应该格外谨慎。

### **记录可设置的偏移量值**

在操作过程中，Debezium 运行引擎将生成新的偏移量并将其刷新到磁盘。最后刷新的偏移量可以通过 `synchdb_state_view()` 实用命令检索：

```
postgres=# select name, last_dbz_offset from synchdb_state_view;
     name      |                                           last_dbz_offset
---------------+------------------------------------------------------------------------------------------------------
 mysqlconn     | {"ts_sec":1741301103,"file":"mysql-bin.000009","pos":574318212,"row":1,"server_id":223344,"event":2}

```

我们应该定期保存这些值，这样如果遇到问题，我们就知道过去可以设置的偏移量位置，以恢复复制操作。

### **暂停连接器**

在设置新的偏移量值之前，连接器必须处于 `paused`（暂停）状态。

使用 `synchdb_pause_engine()` SQL 函数暂停正在运行的连接器。这将停止 Debezium 运行引擎从异构数据库复制。当暂停时，可以使用 `synchdb_set_offset()` SQL 例程更改 Debezium 连接器的偏移量值，以从过去的特定点开始复制。它以 `conninfo_name` 作为参数，可以从 `synchdb_get_state()` 视图的输出中找到。

例如：

```sql
SELECT synchdb_pause_engine('mysqlconn');
```

### **设置新的偏移量**

使用 `synchdb_set_offset()` SQL 函数更改连接器工作进程的起始偏移量。只有当连接器处于 `paused` 状态时才能执行此操作。该函数接受两个参数，`conninfo_name` 和 `有效的偏移量字符串`，这两个参数都可以从 `synchdb_get_state()` 视图的输出中找到。

例如：

```sql
SELECT synchdb_set_offset('mysqlconn', '{"ts_sec":1741301103,"file":"mysql-bin.000009","pos":574318212,"row":1,"server_id":223344,"event":2}');
```

### **恢复连接器**

使用 `synchdb_resume_engine()` SQL 函数从暂停状态恢复 Debezium 操作。此函数以 `连接器名称` 作为其唯一参数，可以从 `synchdb_get_state()` 视图的输出中找到。恢复的 Debezium 运行引擎将从新设置的偏移量值开始复制。

例如：

```sql
SELECT synchdb_resume_engine('mysqlconn');
```