# Postgres 连接器

## **為 SynchDB 準備 PostgreSQL 資料庫**

在使用 SynchDB 從 PostgreSQL 複製資料之前，需要按照[此處](../../getting-started/remote_database_setups/)中概述的步驟設定 PostgreSQL 伺服器。

## **建立 PostgreSQL 連接器**

建立一個連接器，該連接器指向資料庫 `postgres` 和模式 `public` 下的所有資料表。

```sql
SELECT 
  synchdb_add_conninfo(
    'pgconn', '127.0.0.1', 5432, 
    'myuser', 'mypass', 'postgres', 'public', 
    'null', 'null', 'postgres');
```

## **初始快照**

SynchDB 中的「初始快照」（或表快照）是指複製所有指定資料表的表結構及其初始資料。這類似於 PostgreSQL 邏輯複製中的「表同步」。當使用預設的 `initial` 模式啟動連接器時，它會在進入變更資料擷取 (CDC) 階段之前自動執行初始快照。可以使用 `no_data` 模式部分省略此步驟。有關所有快照選項，請參閱[此處](../../user-guide/start_stop_connector/)。

初始快照完成後，連接器在後續重新啟動時不會再次執行初始快照，而是直接從上次未完成的偏移量處恢復 CDC。此行為由 Debezium 引擎管理的元資料檔案控制。有關元資料檔案的更多信息，請參閱[此處](../../architecture/metadata_files/)。

PostgreSQL 連接器的初始快照略有不同。與其他連接器不同，Debezium 引擎不會建構初始表結構。這是因為 PostgreSQL 不會明確地發出 DDL WAL 事件。 PostgreSQL 的原生邏輯複製也存在著同樣的問題。使用者必須在啟動邏輯複製之前預先在目標資料庫建立表格模式。因此，首次啟動基於 Debezium 的 PostgreSQL 連接器時，它會假定您已經建立了指定的表模式及其初始數據，並立即進入 CDC 流模式，而無需實際執行初始快照。

可以透過基於 FDW 的初始快照來解決此問題。該快照使用 `postgres_fdw` 建立初始表模式和數據，然後再透過 Debezium 切換到 CDC 流模式。若要使用此功能，您必須在啟動連接器之前將 `synchdb.olr_snapshot_engine` 設定為 `fdw`。

## **不同的連接器啟動模式**

### **初始快照 + CDC**

**使用 `synchdb.olr_snapshot_engine = 'debezium'`：**

您需要在目標資料庫建立初始表模式和資料。 Debezium 不會創建它們。

**使用 synchdb.olr_snapshot_engine = 'fdw' 時：**

以 `initial` 模式啟動連接器將對所有指定的表執行初始快照（透過 postgres_fdw）。完成後，變更資料擷取 (CDC) 流程將開始串流新變更（透過 Debezium）。

```sql
SELECT synchdb_start_engine_bgw('pgconn', 'initial');

or 

SELECT synchdb_start_engine_bgw('pgconn');
```

此連接器首次運作時，其狀態應為「初始快照」：

```sql
postgres=# select * from synchdb_state_view where name='oracleconn';
  name  | connector_type |  pid   |      stage       |  state  |   err    |       last_dbz_offset
--------+----------------+--------+------------------+---------+----------+-----------------------------
 pgconn | postgres       | 528746 | initial snapshot | polling | no error | offset file not flushed yet

```

將建立一個名為「postgres」的新模式，連接器傳輸的所有表都將在該模式下進行複製。

```sql
postgres=# set search_path=postgres;
SET
postgres=# \d
              List of relations
  Schema  |        Name        | Type  | Owner
----------+--------------------+-------+--------
 postgres | orders             | table | ubuntu

```

初始快照完成後，並且至少收到並處理了一個後續更改，連接器階段應從“初始快照”更改為“變更資料擷取”。

```sql
postgres=# select * from synchdb_state_view where name='pgconn';
  name  | connector_type |   pid   |        stage        |  state  |   err    |
       last_dbz_offset
--------+----------------+---------+---------------------+---------+----------+-----------------------------------
-----------------------------------------------------------------
 pgconn | postgres       | 1604388 | change data capture | polling | no error | {"lsn_proc":37396384,"messageType"
:"INSERT","lsn":37396384,"txId":1015,"ts_usec":1767740340957961}

```

這意味著連接器現在正在串流指定表的新變更。以 `initial` 模式重新啟動連接器將從上次成功複製點開始繼續複製，並且不會重新執行初始快照。

### **僅初始快照，不執行 CDC**

**使用 synchdb.olr_snapshot_engine = 'debezium' 時：**

您需要在目標資料庫中建立初始表架構和資料。 Debezium 不會創建它們。

**使用 synchdb.olr_snapshot_engine = 'fdw' 時：**

使用 `initial_only` 模式啟動連接器將僅對所有指定資料表（在本例中為所有資料表）執行初始快照，且之後不會執行 CDC。

```sql
SELECT synchdb_start_engine_bgw('pgconn', 'initial_only');

```

連接器看起來仍然在輪詢，但由於 Debzium 內部已停止 CDC，因此不會捕獲任何更改。您可以選擇將其關閉。以 `initial_only` 模式重新啟動連接器不會重建表，因為它們已經建立完成。

### **僅捕獲表架構 + CDC**

**使用 synchdb.olr_snapshot_engine = 'debezium' 時：**

您需要在目標資料庫中建立初始表架構和資料。 Debezium 不會創建它們。

**使用 synchdb.olr_snapshot_engine = 'fdw' 時：**

使用 `no_data` 模式啟動連接器將僅執行架構捕獲，並在 PostgreSQL 中建立相應的表，而不會複製現有表資料（跳過初始快照）。架構擷取完成後，連接器將進入 CDC 模式，並開始擷取後續表變更。

```sql
SELECT synchdb_start_engine_bgw('pgconn', 'no_data');

```

以 `no_data` 模式重新啟動連接器不會重新重建模式，而是從上次成功點恢復 CDC。

### **始終執行初始快照 + CDC**

**使用 synchdb.olr_snapshot_engine = 'debezium' 時：**

您需要在目標資料庫中建立初始表模式和資料。 Debezium 不會創建它們。

**使用 synchdb.olr_snapshot_engine = 'fdw' 時：**

使用 `always` 模式啟動連接器將始終擷取擷取表的模式，始終重新執行初始快照，然後進行 CDC。這類似於重置按鈕，因為使用此模式將重建所有內容。請謹慎使用此模式，尤其是在捕獲大量表時，這可能需要很長時間才能完成。重建完成後，CDC 將照常恢復。

```sql
SELECT synchdb_start_engine_bgw('pgconn', 'always');

```

但是，可以使用連接器的 `snapshottable` 選項來選擇部分錶重新建立初始快照。符合 `snapshottable` 中條件的表將重新建立初始快照；否則，將跳過其初始快照。如果 `snapshottable` 為空，則預設情況下，連接器的 `table` 選項中指定的所有表都會以 `always` 模式重新建立初始快照。

此範例僅使連接器重新建立 `inventory.customers` 表的初始快照。所有其他表的快照都將被跳過。

```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{snapshottable}', '"public.customers"') 
WHERE name = 'pgconn';
```

初始快照完成後，CDC 將開始運行。以 `always` 模式重新啟動連接器將重複上述過程。

## **Postgres 連接器的可用快照模式**

* initial（預設）
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
    'pgconn', 
    '127.0.0.1', 
    5433, 
    'pguser', 
    'pgpass', 
    'postgres', 
    'public', 
    'null', 
    'null', 
    'postgres'
);

SELECT synchdb_start_engine_bgw('pgconn', 'schemasync');
```

### **確保連接器處於暫停狀態**

```sql
SELECT name, connector_type, pid, stage, state FROM synchdb_state_view WHERE name = 'pgconn';;
  name  | connector_type |   pid   |        stage        | state
--------+----------------+---------+---------------------+--------
 pgconn | postgres       | 1643157 | change data capture | paused

```

### **查看預設映射規則所建立的表**

```sql
SELECT * FROM synchdb_att_view WHERE name = 'pgconn';
  name  |   type   | attnum |       ext_tbname        |    pg_tbname     | ext_attname | pg_attname |  ext_atttypename  |  pg_atttypename   | transform
--------+----------+--------+-------------------------+------------------+-------------+------------+-------------------+-------------------+-----------
 pgconn | postgres |      1 | postgres.public.mytble  | postgres.mytble  | a           | a          | numeric           | numeric           |
 pgconn | postgres |      2 | postgres.public.mytble  | postgres.mytble  | b           | b          | numeric           | numeric           |
 pgconn | postgres |      3 | postgres.public.mytble  | postgres.mytble  | c           | c          | numeric           | numeric           |
 pgconn | postgres |      1 | postgres.public.testing | postgres.testing | a           | a          | integer           | integer           |
 pgconn | postgres |      2 | postgres.public.testing | postgres.testing | b           | b          | text              | text              |
 pgconn | postgres |      3 | postgres.public.testing | postgres.testing | c           | c          | character varying | character varying |
 pgconn | postgres |      4 | postgres.public.testing | postgres.testing | d           | d          | bigint            | bigint            |
 pgconn | postgres |      1 | postgres.public.xyz     | postgres.xyz     | bbb         | bbb        | character varying | character varying |
 pgconn | postgres |      2 | postgres.public.xyz     | postgres.xyz     | ccc         | ccc        | bytea             | bytea             |
 pgconn | postgres |      3 | postgres.public.xyz     | postgres.xyz     | ddd         | ddd        | numeric           | numeric           |
 pgconn | postgres |      4 | postgres.public.xyz     | postgres.xyz     | eee         | eee        | numeric           | numeric           |
 pgconn | postgres |      5 | postgres.public.xyz     | postgres.xyz     | fff         | fff        | numeric           | numeric           |
 pgconn | postgres |      6 | postgres.public.xyz     | postgres.xyz     | ggg         | ggg        | bigint            | bigint            |
 pgconn | postgres |      7 | postgres.public.xyz     | postgres.xyz     | aaa         | aaa        | integer           | integer           |


```

### **定義自訂映射規則（如有需要）**

使用者可以使用 `synchdb_add_objmap` 函數建立自訂映射規則。此函數可用於對應表名、列名、資料類型，並定義資料轉換表達式規則。

```sql
SELECT synchdb_add_objmap('pgconn','table','postgres.public.mytble','postgres.thetable');
SELECT synchdb_add_objmap('pgconn','column','postgres.public.testing.c','ccc');
SELECT synchdb_add_objmap('pgconn','datatype','postgres.public.xyz.ggg','int|0');
SELECT synchdb_add_objmap('pgconn','transform','postgres.public.xyz.bbb','''>>>>>'' || ''%d'' || ''<<<<<''');
```

以上內容意味著：

* 來源表 'postgres.public.mytble' 將會對應到目標表 'postgres.thetable'
* 來源列 'postgres.public.testing.c' 將會對應到目標列 'ccc'
* 來源列 'postgres.public.xyz.ggg' 的資料型別將會對應到 'int'
* 來源列資料 'postgres.public.xyz.bbb' 將根據表達式進行轉換，其中 %d 為資料佔位符

### **回顧所有已建立的物件映射規則**

```sql
SELECT * FROM synchdb_objmap WHERE name = 'pgconn';
  name  |  objtype  | enabled |          srcobj           |           dstobj
--------+-----------+---------+---------------------------+----------------------------
 pgconn | table     | t       | postgres.public.mytble    | postgres.thetable
 pgconn | column    | t       | postgres.public.testing.c | ccc
 pgconn | datatype  | t       | postgres.public.xyz.ggg   | int|0
 pgconn | transform | t       | postgres.public.xyz.bbb   | '>>>>>' || '%d' || '<<<<<'


```

### **重新載入物件映射規則**

定義完所有自訂規則後，我們需要通知連接器載入這些規則。這將使連接器讀取並應用物件映射規則。如果連接器發現目前 PostgreSQL 值與物件對應值之間存在差異，它將嘗試修正映射。

```sql
SELECT synchdb_reload_objmap('pgconn');

```

### **再次檢查 `synchdb_att_view` 是否有更改**

```sql
SELECT * from synchdb_att_view WHERE name = 'pgconn';;
  name  |   type   | attnum |       ext_tbname        |     pg_tbname     | ext_attname | pg_attname |  ext_atttypename  |  pg_atttypename   |         transform
--------+----------+--------+-------------------------+-------------------+-------------+------------+-------------------+-------------------+----------------------------
 pgconn | postgres |      1 | postgres.public.mytble  | postgres.thetable | a           | a          | numeric           | numeric           |
 pgconn | postgres |      2 | postgres.public.mytble  | postgres.thetable | b           | b          | numeric           | numeric           |
 pgconn | postgres |      3 | postgres.public.mytble  | postgres.thetable | c           | c          | numeric           | numeric           |
 pgconn | postgres |      1 | postgres.public.testing | postgres.testing  | a           | a          | integer           | integer           |
 pgconn | postgres |      2 | postgres.public.testing | postgres.testing  | b           | b          | text              | text              |
 pgconn | postgres |      3 | postgres.public.testing | postgres.testing  | c           | ccc        | character varying | character varying |
 pgconn | postgres |      4 | postgres.public.testing | postgres.testing  | d           | d          | bigint            | bigint            |
 pgconn | postgres |      1 | postgres.public.xyz     | postgres.xyz      | bbb         | bbb        | character varying | character varying | '>>>>>' || '%d' || '<<<<<'
 pgconn | postgres |      2 | postgres.public.xyz     | postgres.xyz      | ccc         | ccc        | bytea             | bytea             |
 pgconn | postgres |      3 | postgres.public.xyz     | postgres.xyz      | ddd         | ddd        | numeric           | numeric           |
 pgconn | postgres |      4 | postgres.public.xyz     | postgres.xyz      | eee         | eee        | numeric           | numeric           |
 pgconn | postgres |      5 | postgres.public.xyz     | postgres.xyz      | fff         | fff        | numeric           | numeric           |
 pgconn | postgres |      6 | postgres.public.xyz     | postgres.xyz      | ggg         | ggg        | bigint            | integer           |
 pgconn | postgres |      7 | postgres.public.xyz     | postgres.xyz      | aaa         | aaa        | integer           | integer           |

```

### **恢復連接器或重新建立整個快照**

確認物件映射正確後，我們可以恢復連接器。請注意，復原操作只會傳輸新的表更改，不會複製表中的現有資料。

```sql
SELECT synchdb_resume_engine('pgconn');

```

要擷取表中的現有數據，我們也可以使用新的物件映射規則重新建立整個快照。

```sql
SELECT synchdb_stop_engine_bgw('pgconn');
SELECT synchdb_start_engine_bgw('pgconn', 'always');

```

## **選擇性表同步**

### **選擇所需表並首次啟動**

表格選擇在連接器建立階段透過 `synchdb_add_conninfo()` 函數完成，該函數用於指定要從中複製的表列表（以完全限定名稱 (FQN) 表示，並以逗號分隔）。

例如，以下命令建立一個連接器，該連接器僅複製遠端 PostgreSQL 資料庫中 `public.orders` 表的變更。
```sql
SELECT synchdb_add_conninfo(
    'pgconn', 
    '127.0.0.1', 
    5433, 
    'pguser', 
    'pgpass', 
    'postgres', 
    'public', 
    'public.orders', 
    'null', 
    'postgres'
);
```

首次啟動此連接器時，將觸發執行初始快照，並複製所選表的架構和資料。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn');
```

### **驗證連接器狀態和表格**

檢查連接器狀態和新表格：
```sql
postgres=# Select name, state, err from synchdb_state_view;
  name  |  state  |   err
--------+---------+----------
 pgconn | polling | no error


postgres=# \dt postgres.*
           List of tables
  Schema  |  Name  | Type  | Owner
----------+--------+-------+--------
 postgres | orders | table | pguser

```

快照完成後，連接器將繼續擷取表格的後續變更。

### **運行時加入更多要複製的表**

上一節中的 `mysqlconn` 已完成初始快照並取得了所選表格的表格結構。如果我們想要新增更多要複製的表，則需要通知 Debezium 引擎更新了表結構，並再次執行初始快照。具體操作如下：

1. 更新 `synchdb_conninfo` 表以包含其他表。
2. 在本例中，我們將 `inventory.customers` 表加入同步清單：
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{table}', '"public.orders,public.customers"') 
WHERE name = 'pgconn';
```
3. 配置快照表參數，使其只包含新表 `inventory.customers`，這樣 SynchDB 就不會嘗試重建已經完成快照的 2 個表。
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{snapshottable}', '"public.customers"') 
WHERE name = 'pgconn';
``` 
4. 將快照模式設為“始終”，然後重新啟動連接器，以執行另一次初始快照：
```sql
SELECT synchdb_stop_engine_bgw('pgconn');
SELECT synchdb_start_engine_bgw('pgconn', 'always');
```

**<<重要提示>>** 請注意，此處我們不使用 `synchdb_restart_connector` 來重啟連接器，因為該函數主要用於以不同的快照模式重新啟動 Debezium 引擎。由於 Postgres 連接器使用 FDW 而非 Debezium 來建立初始表，因此我們必須明確地執行 `stop engine`，然後再呼叫 `start engine` 來觸發 FDW 例程再次運行。

### **驗證更新後的表格**

現在，我們可以再次檢查我們的表：
```sql
postgres=# \dt inventory.*
             List of tables
  Schema   |   Name    | Type  | Owner
-----------+-----------+-------+--------
 inventory | customers | table | ubuntu
 inventory | orders    | table | ubuntu
 inventory | products  | table | ubuntu

```
