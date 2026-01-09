# Oracle 连接器

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

在嘗試對當前表和資料（可能非常龐大）進行初始快照之前，可以在實際資料遷移之前「預覽」來源表和目標表之間的所有表和資料類型對應。這樣，您有機會在實際遷移之前修改資料類型對應或物件名稱。這可以透過特殊的「schemasync」初始快照模式來實現。

### **建立連接器並以 `schemasync` 模式啟動它**

`schemasync` 是一種特殊模式，它使連接器連接到遠端資料庫並嘗試僅同步指定表的模式。完成後，連接器將處於「暫停」狀態，使用者可以查看使用預設規則建立的所有資料表和資料類型，並根據需要進行變更。

```sql
SELECT synchdb_add_conninfo(
    'oracleconn', 
    '127.0.0.1', 
    1521, 
    'DBZUSER', 
    'dbz', 
    'FREE', 
    'DBZUSER', 
    'null', 
    'null', 
    'oracle'
);

SELECT synchdb_start_engine_bgw('oracleconn', 'schemasync');
```
### **確保連接器處於暫停狀態**

```sql
SELECT name, connector_type, pid, stage, state FROM synchdb_state_view WHERE name = 'oracleconn';
    name    | connector_type |   pid   |        stage        | state
------------+----------------+---------+---------------------+--------
 oracleconn | oracle         | 1648935 | change data capture | paused


```
### **查看預設映射規則所建立的表**

```sql
SELECT * FROM synchdb_att_view WHERE name = 'oracleconn';
    name    |  type  | attnum |       ext_tbname       |   pg_tbname    | ext_attname  |  pg_attname  | ext_atttypename |       pg_atttypename        | transform
------------+--------+--------+------------------------+----------------+--------------+--------------+-----------------+-----------------------------+-----------
 oracleconn | oracle |      1 | FREE.DBZUSER.CUSTOMERS | free.customers | ID           | id           | number          | numeric                     |
 oracleconn | oracle |      2 | FREE.DBZUSER.CUSTOMERS | free.customers | NAME         | name         | varchar2        | character varying           |
 oracleconn | oracle |      1 | FREE.DBZUSER.ORDERS    | free.orders    | ORDER_NUMBER | order_number | number          | numeric                     |
 oracleconn | oracle |      2 | FREE.DBZUSER.ORDERS    | free.orders    | ORDER_DATE   | order_date   | date            | timestamp without time zone |
 oracleconn | oracle |      3 | FREE.DBZUSER.ORDERS    | free.orders    | PURCHASER    | purchaser    | number          | numeric                     |
 oracleconn | oracle |      4 | FREE.DBZUSER.ORDERS    | free.orders    | QUANTITY     | quantity     | number          | numeric                     |
 oracleconn | oracle |      5 | FREE.DBZUSER.ORDERS    | free.orders    | PRODUCT_ID   | product_id   | number          | numeric                     |

```
### **定義自訂映射規則（如有需要）**

使用者可以使用 `synchdb_add_objmap` 函數建立自訂映射規則。此函數可用於對應表名、列名、資料類型，並定義資料轉換表達式規則。

```sql
SELECT synchdb_add_objmap('oracleconn','table','FREE.DBZUSER.ORDERS','free.myorders');
SELECT synchdb_add_objmap('oracleconn','column','FREE.DBZUSER.ORDERS.PURCHASER','who');
SELECT synchdb_add_objmap('oracleconn','datatype','FREE.DBZUSER.ORDERS.QUANTITY','bigint|0');
SELECT synchdb_add_objmap('oracleconn','transform','FREE.DBZUSER.CUSTOMERS.NAME','''>>>>>'' || ''%d'' || ''<<<<<''');
```

以上內容意味著：

* 來源表“FREE.DBZUSER.ORDERS”將對應到目標表“free.myorders”
* 來源列“FREE.DBZUSER.ORDERS.PURCHASE”將對應到目標表“who”
* 來源列“FREE.DBZUSER.ORDERS.QUANTITY”的資料類型將會對應到“bigint”
* 來源列「FREE.DBZUSER.CUSTOMERS.NAME」的資料將根據表達式進行轉換，其中 %d 為資料佔位符

### **回顧所有已建立的物件映射規則**

```sql
SELECT * FROM synchdb_objmap WHERE name = 'oracleconn';
    name    |  objtype  | enabled |            srcobj             |           dstobj
------------+-----------+---------+-------------------------------+----------------------------
 oracleconn | table     | t       | FREE.DBZUSER.ORDERS           | free.myorders
 oracleconn | column    | t       | FREE.DBZUSER.ORDERS.PURCHASER | who
 oracleconn | datatype  | t       | FREE.DBZUSER.ORDERS.QUANTITY  | bigint|0
 oracleconn | transform | t       | FREE.DBZUSER.CUSTOMERS.NAME   | '>>>>>' || '%d' || '<<<<<'

```

### **重新載入物件映射規則**

定義完所有自訂規則後，我們需要通知連接器載入這些規則。這將使連接器讀取並應用物件映射規則。如果連接器發現目前 PostgreSQL 值與物件對應值之間存在差異，它將嘗試修正映射。

```sql
SELECT synchdb_reload_objmap('oracleconn');

```

### **再次檢查 `synchdb_att_view` 是否有更改**

```sql
SELECT * from synchdb_att_view WHERE name = 'oracleconn';
    name    |  type  | attnum |       ext_tbname       |   pg_tbname    | ext_attname  |  pg_attname  | ext_atttypename |       pg_atttypename        |         transform
------------+--------+--------+------------------------+----------------+--------------+--------------+-----------------+-----------------------------+----------------------------
 oracleconn | oracle |      1 | FREE.DBZUSER.CUSTOMERS | free.customers | ID           | id           | number          | numeric                     |
 oracleconn | oracle |      2 | FREE.DBZUSER.CUSTOMERS | free.customers | NAME         | name         | varchar2        | character varying           | '>>>>>' || '%d' || '<<<<<'
 oracleconn | oracle |      1 | FREE.DBZUSER.ORDERS    | free.myorders  | ORDER_NUMBER | order_number | number          | numeric                     |
 oracleconn | oracle |      2 | FREE.DBZUSER.ORDERS    | free.myorders  | ORDER_DATE   | order_date   | date            | timestamp without time zone |
 oracleconn | oracle |      3 | FREE.DBZUSER.ORDERS    | free.myorders  | PURCHASER    | who          | number          | numeric                     |
 oracleconn | oracle |      4 | FREE.DBZUSER.ORDERS    | free.myorders  | QUANTITY     | quantity     | number          | bigint                      |
 oracleconn | oracle |      5 | FREE.DBZUSER.ORDERS    | free.myorders  | PRODUCT_ID   | product_id   | number          | numeric                     |

```

### **恢復連接器或重新建立整個快照**

確認物件映射正確後，我們可以恢復連接器。請注意，復原操作只會傳輸新的表更改，不會複製表中的現有資料。

```sql
SELECT synchdb_resume_engine('oracleconn');

```

要擷取表中的現有數據，我們也可以使用新的物件映射規則重新建立整個快照。

```sql
SELECT synchdb_stop_engine_bgw('oracleconn');
SELECT synchdb_start_engine_bgw('oracleconn', 'always');

```

## **選擇性表同步**

### **選擇所需表並首次啟動同步**

表格選擇在連接器建立階段透過 `synchdb_add_conninfo()` 函數完成，該函數用於指定要從中複製的表列表（以完全限定名稱 (FQN) 表示，並以逗號分隔）。

例如，以下命令建立一個連接器，該連接器僅從遠端 Oracle 資料庫複製 `FREE.ORDERS` 表中的變更。
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

## **配置 Infinispan功能**

默认情况下，Debezium 的 Oracle 连接器使用 JVM 堆缓存传入的变更事件，然后再将它们传递给 SynchDB 进行处理。最大堆大小通过 `synchdb.jvm_max_heap_size` GUC 控制。当 JVM 堆内存不足时（尤其是在处理大型事务或包含大量列的模式时），连接器可能会出现 OutOfMemoryError 错误，这使得堆大小调整成为一项关键的调优挑战。

作为替代方案，Debezium 可以配置使用 Infinispan 作为其缓存层。Infinispan 支持 JVM 堆和堆外（直接内存）的内存存储，从而提供更大的灵活性。此外，它还支持钝化功能，允许在达到内存限制时将多余的数据溢出到磁盘。这使得它在高负载下更具弹性，并确保能够优雅地处理大型事务或模式变更，而不会耗尽内存。

### **`synchdb_add_infinispan()`**

**签名：**

```sql
synchdb_add_infinispan(
    connectorName NAME,         -- 连接器名称
    memoryType NAME,            -- 内存类型，可以是“堆”或“非堆”
    memorySize INT              -- 预留为缓存的内存大小（以 MB 为单位）
)
```

为给定的连接器注册一个由 Infinispan 支持的缓存配置。这允许连接器使用 Infinispan 缓冲更改事件，并支持基于堆和堆外内存分配以及溢出到磁盘（钝化）。

**注意：**

- 如果在当前正在运行的连接器上调用，则该设置将在下次重启时生效。
- 如果连接器已存在 Infinispan 缓存，它将被替换。
- memoryType='off_heap' 使用本机（直接）内存，不受 JVM 堆限制，但应谨慎调整大小。
- 当内存填满时，缓存将自动支持钝化到磁盘。

**示例：**
``` sql
SELECT synchdb_add_infinispan('oracleconn', 'off_heap', 2048);

```

### **`synchdb_del_infinispan()`**

**签名：**

```sql
synchdb_add_infinispan(
    connectorName NAME          -- 连接器名称
)
```

删除指定连接器的 Infinispan 缓存配置及其相关的磁盘元数据。此操作将删除：

- 所有缓存文件
- 所有钝化（溢出到磁盘）状态
- 相关的 Infinispan 配置

**注意：**

- 此函数只能在连接器停止时执行。
- 在连接器处于活动状态时尝试运行此函数将导致错误。
- 永久禁用或重新配置连接器的缓存后端后，使用此命令进行清理。

**Example:**
``` sql
SELECT synchdb_del_infinispan('oracleconn');

```

### **`钝化`**

钝化简单来说就是，如果缓存已满，infinispan 会弹出并将数据写入磁盘。只要内存中有足够的空间来保存更改事件，就不会发生磁盘写入。数据将写入 `$PGDATA/pg_synchdb/ispn_[连接器名称]_[目标数据库名称]`

### **用于测试 Infinispan 大型事务的 Oracle SQL 示例**

**在 Oracle 中创建测试表：**

```sql
CREATE TABLE big_tx_test (
    id       NUMBER PRIMARY KEY,
    payload  VARCHAR2(4000),
    created  DATE DEFAULT SYSDATE
);

```

**创建一个相对较大的交易：**
```sql
BEGIN
  FOR i IN 1..100000 LOOP
    INSERT INTO big_tx_test (id, payload)
    VALUES (i, RPAD('x', 4000, 'x'));
  END LOOP;
  COMMIT;
END;
/

```

**大事务下的行为**

未设置 infinispan + 低 JVM heap size (128)

* --> 将发生 OutOfMemory 错误

低 JVM heap size (128) + 高 infinispan heap size (2048)

* --> 大型事务成功处理

低 JVM heap size (128) + 低 infinispan heap size (128)

* --> 将发生钝化
* --> `ispn_[连接器名称]_[目标数据库名称]` 大小在处理过程中会增加，大型事务处理完成后会减小
* --> 大型事务已成功处理，但速度较慢。

低 JVM heap size (128) + 高 infinispan heap size (2048)

* --> 将发生 OutOfMemory 错误
* --> 请勿将 infinispan 配置 heap size 超过 JVM 最大 heap 内存

较低的 JVM heap size (128) + 较低的 infinispan heap size (64)

* --> 将发生钝化
* --> `ispn_[连接器名称]_[目标数据库名称]` 的大小将在处理过程中增加，并在大型事务处理完成后减小。
* --> 不建议使用，因为 infinispan 可能会占用一半的 JVM heap，而剩余空间可能不足以用于其他 Debezium 操作。

低 JVM heap size (128) + 相同的 infinispan heap size (128)

* --> 不建议使用，因为 infinispan 可能会占用所有 JVM 堆空间，最终导致 OutOfMemory 错误。

高 JVM heap size (2048) + 低 infinispan heap size (128)

* --> 将发生钝化。
* --> `ispn_[连接器名称]_[目标数据库名称]` 的大小在处理过程中会增加，并在大型事务处理完成后减小。
* --> 大型事务处理成功。
* --> 效率低下 - 因为只有一小部分 JVM heap 空间用作缓存 + 不必要的钝化。


## 自定义起始偏移量值

起始偏移量值代表开始复制的点，类似于 PostgreSQL 的恢复 LSN。当 Debezium 运行引擎启动时，它将从这个偏移量值开始复制。将此偏移量值设置为较早的值将导致 Debezium 运行引擎从较早的记录开始复制，可能会复制重复的数据记录。在设置 Debezium 的起始偏移量值时，我们应该格外谨慎。

### **记录可设置的偏移量值**

在操作过程中，Debezium 运行引擎将生成新的偏移量并将其刷新到磁盘。最后刷新的偏移量可以通过 `synchdb_state_view()` 实用命令检索：

```
postgres=# select name, last_dbz_offset from synchdb_state_view;
     name      |                                           last_dbz_offset
---------------+------------------------------------------------------------------------------------------------------
 oracleconn    | {"commit_scn":"2311579","snapshot_scn":"2311578","scn":"2311578"}

```

我们应该定期保存这些值，这样如果遇到问题，我们就知道过去可以设置的偏移量位置，以恢复复制操作。

### **暂停连接器**

在设置新的偏移量值之前，连接器必须处于 `paused`（暂停）状态。

使用 `synchdb_pause_engine()` SQL 函数暂停正在运行的连接器。这将停止 Debezium 运行引擎从异构数据库复制。当暂停时，可以使用 `synchdb_set_offset()` SQL 例程更改 Debezium 连接器的偏移量值，以从过去的特定点开始复制。它以 `conninfo_name` 作为参数，可以从 `synchdb_get_state()` 视图的输出中找到。

例如：

```sql
SELECT synchdb_pause_engine('oracleconn');
```

### **设置新的偏移量**

使用 `synchdb_set_offset()` SQL 函数更改连接器工作进程的起始偏移量。只有当连接器处于 `paused` 状态时才能执行此操作。该函数接受两个参数，`conninfo_name` 和 `有效的偏移量字符串`，这两个参数都可以从 `synchdb_get_state()` 视图的输出中找到。

例如：

```sql
SELECT synchdb_set_offset('oracleconn', '{"commit_scn":"2311579","snapshot_scn":"2311578","scn":"2311578"}');
```

### **恢复连接器**

使用 `synchdb_resume_engine()` SQL 函数从暂停状态恢复 Debezium 操作。此函数以 `连接器名称` 作为其唯一参数，可以从 `synchdb_get_state()` 视图的输出中找到。恢复的 Debezium 运行引擎将从新设置的偏移量值开始复制。

例如：

```sql
SELECT synchdb_resume_engine('oracleconn');
```

## 設定 Debezium Oracle 連接器以使用 Openlog Replicator（非原生）

雖然不建議，但可以配置基於 Debezium 的 Oracle 連接器，使其從 Openlog Replicator 而非 Logminer 串流傳輸資料。這與 [原生 Openlog Replicator](../../tutorial/native_olr_cdc_to_postgresql) 不同，後者是 SynchDB 內部原生建構的，無需使用 Debezium，也是使用 OLR 的建議方式。

要建立**基於 Debezium**的 OLR 連接器（使用 `type` = 'oracle'）：

```sql
SELECT synchdb_add_conninfo('olrconn',
                            'ora19c',
                            1521,
                            'DBZUSER',
                            'dbz',
                            'FREE',
                            'postgres',
                            'null',
                            'null',
                            'oracle');

```

然後，使用帶有簽署的 `synchdb_add_olr_conninfo` 函數附加 Openlog Replicator 資訊：

```sql
synchdb_add_olr_conninfo(
    conn_name TEXT,     -- Name of the connector
    olr_host TEXT,      -- Hostname or IP of the OLR instance
    olr_port INT,       -- Port number exposed by OLR (typically 7070)
    olr_source TEXT     -- Oracle source name as configured in OLR
)
```

**範例：**

```sql
SELECT synchdb_add_olr_conninfo('olrconn', '127.0.0.1', 7070, 'ORACLE');

```

若要從特定連接器移除 OLR 設定並還原至 Logminer，請使用具有下列簽章的 `synchdb_del_olr_conninfo` 函數：

```sql
synchdb_del_olr_conninfo(conn_name TEXT)

```

**範例：**

```sql

SELECT synchdb_del_olr_conninfo('olrconn');

```

**基於 Debezium 的 Openlog Replicator 連接器的行為說明**

* 當 LogMiner 和 OLR 配置同時存在時，SynchDB 預設使用 Openlog Replicator 進行變更擷取。
* 如果 OLR 設定缺失，SynchDB 將使用日誌原則來傳輸變更。
* 修改 OLR 配置後，需要重新啟動連接器。