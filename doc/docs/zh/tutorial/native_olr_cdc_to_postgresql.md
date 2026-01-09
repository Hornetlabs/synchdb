#原生 Openlog Replicator 連接器

## **準備用於 SynchDB 的 MySQL 資料庫**

在使用 SynchDB 透過原生 Openlog Replicator (OLR) 連接器進行複製之前，OLR 和 Oracle 資料庫本身都需要按照[此處](../../getting-started/remote_database_setups/)中概述的步驟進行設定。

## **原生的 Openlog Replicator 连接器的行为说明**

* 目前为 BETA 版本。
* SynchDB 管理与 Openlog Replicator 的连接，并在不使用 Debezium 的情况下流式传输变更。
* 需要 OLR 配置，否则连接器启动时会出错。
* 依赖 Debezium 的 Oracle 连接器完成初始快照，并在完成后关闭，后续的 CDC 由 SynchDB 内部针对 Openlog Replicator 原生完成。
* 依赖 IvorySQL 的 Oracle 解析器来处理 DDL 事件。在使用原生 openlog replicator 连接器之前，必须先编译并安装它。
* 访问[此处](https://github.com/bersler/OpenLogReplicator) 了解有关 Openlog Replicator 的更多信息。

## **建立原生 Openlog Replicator 連接器**

建立一個連接器，該連接器透過原生 Openlog Replicator 連接器指向 `FREE` 資料庫和 `DBZUSER` 模式下的所有表。

```sql
SELECT synchdb_add_conninfo(
    'olrconn', '127.0.0.1', 1521, 'DBZUSER', 
    'dbz', 'FREE', 'DBZUSER', 
    'null', 'null', 'olr');

SELECT synchdb_add_olr_conninfo(
	'olrconn',
	'127.0.0.1',
	7070,
	'ORACLE');

```
## **初始快照**

SynchDB 中的「初始快照」（或表快照）是指複製所有指定表的表結構以及初始資料。這類似於 PostgreSQL 邏輯複製中的「表同步」。當使用預設的 `initial` 模式啟動連接器時，它會在進入變更資料擷取 (CDC) 階段之前自動執行初始快照。可以使用 `no_data` 模式部分省略此步驟。有關所有快照選項，請參閱[此處](../../user-guide/start_stop_connector/)。

初始快照完成後，連接器在後續重新啟動時不會再次執行初始快照，而是直接從上次未完成的偏移量處恢復 CDC。此行為由 Debezium 引擎管理的元資料檔案控制。有關元資料檔案的更多信息，請參閱[此處](../../architecture/metadata_files/)。

## **不同的連接器啟動模式**

### **初始快照 + CDC**

使用 `initial` 模式啟動連接器將對所有指定表（本例中為所有表）執行初始快照。完成後，變更資料擷取 (CDC) 流程將開始收集新的變更資料。

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'initial');

or 

SELECT synchdb_start_engine_bgw('olrconn');
```

如果此連接器已處理 CDC 中的至少一個變更事件，則其階段應為「初始快照」或「變更資料擷取」。

```sql
postgres=# select * from synchdb_state_view;
  name   | connector_type |   pid   |       stage      |  state  |   err    | last_dbz_offset
---------+----------------+---------+------------------+---------+----------+-----------------
 olrconn | olr            | 1702522 | initial snapshot | polling | no error | no offset



```

將建立一個名為「free」的新模式，連接器傳輸的所有表都將在該模式下進行複製。

```sql
postgres=# \dt free.*
           List of tables
 Schema |   Name    | Type  | Owner
--------+-----------+-------+--------
 free   | customers | table | ubuntu
 free   | orders    | table | ubuntu

```

初始快照完成後，並且至少收到並處理了一個後續更改，連接器階段應從“初始快照”更改為“變更資料擷取”。

```sql
postgres=# select * from synchdb_state_view;
  name   | connector_type |   pid   |        stage        |  state  |   err    |               last_dbz_offset
---------+----------------+---------+---------------------+---------+----------+---------------------------------------------
 olrconn | olr            | 1702522 | change data capture | polling | no error | {"scn":5031082, "c_scn":5031085, "c_idx":3}

```

這意味著連接器現在正在檢測指定表的新變更。以「初始」模式重新啟動連接器將從上次成功複製點開始繼續複製，不會重新執行初始快照。

### **僅初始快照，不執行 CDC**

使用 `initial_only` 模式啟動連接器，將僅對所有指定資料表（本例中為所有資料表）執行初始快照，之後不會執行 CDC。

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'initial_only');

```

### **僅捕獲表架構 + CDC**

使用 `no_data` 模式啟動連接器將僅執行架構捕獲，並在 PostgreSQL 中建立相應的表，但不會複製現有表資料（跳過初始快照）。架構擷取完成後，連接器將進入 CDC 模式，並開始擷取表的後續變更。

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'no_data');

```

以 `no_data` 模式重新啟動連接器不會再次重建架構，而是從上次成功捕獲的位置繼續 CDC 擷取。

### **僅 CDC**

使用 `never` 模式啟動連接器將完全跳過架構擷取和初始快照，直接進入 CDC 模式擷取後續變更。請注意，連接器要求所有擷取表在以 `never` 模式啟動之前已在 PostgreSQL 中建立。如果表不存在，連接器在嘗試將 CDC 變更套用到不存在的表時會遇到錯誤。

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'never');

```

以 `never` 模式重新啟動連接器將從上次成功點恢復 CDC。

### **始終執行初始快照 + CDC**

使用 `always` 模式啟動連接器將始終擷取擷取表的模式，始終重新執行初始快照，然後再進行 CDC。這類似於重置按鈕，因為使用此模式將重建所有內容。請謹慎使用此模式，尤其是在捕獲大量表時，這可能需要很長時間才能完成。重建完成後，CDC 將照常恢復。

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'always');

```

初始快照完成後，CDC 將開始。在 `always` 模式下重新啟動連接器將重複上述過程。

## **MySQL 連接器的可用快照模式**

* initial（預設）
* initial_only
* no_data
* never
* always
* schemasync

## **使用 schemasync 模式預覽來源表和目標表關係**

在嘗試對當前表和資料（可能非常龐大）進行初始快照之前，可以在當前資料遷移之前「預覽」來源表和目標表之間的所有表和資料類型對應。這樣，您有機會在實際遷移之前修改資料類型對應或物件名稱。這可以透過特殊的「schemasync」初始快照模式來實現。

請注意，您必須將 `synchdb.olr_snapshot_engine` 設定為 'fdw' 才能使用 `schemasync` 模式預覽表。

### **建立連接器並以 `schemasync` 模式啟動它**

`schemasync` 是一種特殊模式，它使連接器連接到遠端資料庫，並嘗試僅同步指定表的模式。完成後，連接器將處於「暫停」狀態，使用者可以查看使用預設規則建立的所有資料表和資料類型，並根據需要進行變更。

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

SELECT synchdb_add_olr_conninfo(
	'olrconn',
	'127.0.0.1',
	7070,
	'ORACLE');

SELECT synchdb_start_engine_bgw('olrconn', 'schemasync');
```

### **確保連接器處於暫停狀態**

```sql
SELECT name, connector_type, pid, stage, state FROM synchdb_state_view WHERE name = 'olrconn';
  name   | connector_type |   pid   |        stage        | state
---------+----------------+---------+---------------------+--------
 olrconn | olr            | 1703430 | change data capture | paused

```

### **查看預設映射規則所建立的表**

```sql
SELECT * FROM synchdb_att_view WHERE name = 'olrconn';
  name   |  type  | attnum |       ext_tbname       |   pg_tbname    | ext_attname  |  pg_attname  | ext_atttypename |       pg_atttypename        | transform
---------+--------+--------+------------------------+----------------+--------------+--------------+-----------------+-----------------------------+-----------
 olrconn | oracle |      1 | FREE.DBZUSER.CUSTOMERS | free.customers | ID           | id           | number          | numeric                     |
 olrconn | oracle |      2 | FREE.DBZUSER.CUSTOMERS | free.customers | NAME         | name         | varchar2        | character varying           |
 olrconn | oracle |      1 | FREE.DBZUSER.ORDERS    | free.orders    | ORDER_NUMBER | order_number | number          | numeric                     |
 olrconn | oracle |      2 | FREE.DBZUSER.ORDERS    | free.orders    | ORDER_DATE   | order_date   | date            | timestamp without time zone |
 olrconn | oracle |      3 | FREE.DBZUSER.ORDERS    | free.orders    | PURCHASER    | purchaser    | number          | numeric                     |
 olrconn | oracle |      4 | FREE.DBZUSER.ORDERS    | free.orders    | QUANTITY     | quantity     | number          | numeric                     |
 olrconn | oracle |      5 | FREE.DBZUSER.ORDERS    | free.orders    | PRODUCT_ID   | product_id   | number          | numeric                     |

```

### **定義自訂映射規則（如有需要）**

使用者可以使用 `synchdb_add_objmap` 函數建立自訂映射規則。此函數可用於對應表名、列名、資料類型，並定義資料轉換表達式規則。

```sql
SELECT synchdb_add_objmap('olrconn','table','FREE.DBZUSER.ORDERS','free.myorders');
SELECT synchdb_add_objmap('olrconn','column','FREE.DBZUSER.ORDERS.PURCHASER','who');
SELECT synchdb_add_objmap('olrconn','datatype','FREE.DBZUSER.ORDERS.QUANTITY','bigint|0');
SELECT synchdb_add_objmap('olrconn','transform','FREE.DBZUSER.CUSTOMERS.NAME','''>>>>>'' || ''%d'' || ''<<<<<''');
```

The above means:

* source table 'FREE.DBZUSER.ORDERS' will be mapped to 'free.myorders' in destination
* source column 'FREE.DBZUSER.ORDERS.PURCHASE' will be mapped to 'who' in destination
* source data type for column 'FREE.DBZUSER.ORDERS.QUANTITY' will be mapped to 'bigint'
* source column data 'FREE.DBZUSER.CUSTOMERS.NAME' will be transformed accoring to the expression where %d is the data placeholder

### **回顧所有已建立的物件映射規則**

```sql
SELECT * FROM synchdb_objmap WHERE name = 'olrconn';
  name   |  objtype  | enabled |            srcobj             |           dstobj
---------+-----------+---------+-------------------------------+----------------------------
 olrconn | table     | t       | FREE.DBZUSER.ORDERS           | free.myorders
 olrconn | column    | t       | FREE.DBZUSER.ORDERS.PURCHASER | who
 olrconn | datatype  | t       | FREE.DBZUSER.ORDERS.QUANTITY  | bigint|0
 olrconn | transform | t       | FREE.DBZUSER.CUSTOMERS.NAME   | '>>>>>' || '%d' || '<<<<<'

```

### **重新載入物件映射規則**

定義完所有自訂規則後，我們需要通知連接器載入這些規則。這將使連接器讀取並應用物件映射規則。如果連接器發現目前 PostgreSQL 值與物件對應值之間存在差異，它將嘗試修正映射。

```sql
SELECT synchdb_reload_objmap('olrconn');

```

### **再次檢查 `synchdb_att_view` 是否有更改**

```sql
SELECT * from synchdb_att_view WHERE name = 'olrconn';
  name   | type | attnum |       ext_tbname       |   pg_tbname    | ext_attname  |  pg_attname  | ext_atttypename |       pg_atttypename        |         transform
---------+------+--------+------------------------+----------------+--------------+--------------+-----------------+-----------------------------+----------------------------
 olrconn | olr  |      1 | FREE.DBZUSER.CUSTOMERS | free.customers | ID           | id           | number          | numeric                     |
 olrconn | olr  |      2 | FREE.DBZUSER.CUSTOMERS | free.customers | NAME         | name         | varchar2        | character varying           | '>>>>>' || '%d' || '<<<<<'
 olrconn | olr  |      1 | FREE.DBZUSER.ORDERS    | free.myorders  | ORDER_NUMBER | order_number | number          | numeric                     |
 olrconn | olr  |      2 | FREE.DBZUSER.ORDERS    | free.myorders  | ORDER_DATE   | order_date   | date            | timestamp without time zone |
 olrconn | olr  |      3 | FREE.DBZUSER.ORDERS    | free.myorders  | PURCHASER    | who          | number          | numeric                     |
 olrconn | olr  |      4 | FREE.DBZUSER.ORDERS    | free.myorders  | QUANTITY     | quantity     | number          | bigint                      |
 olrconn | olr  |      5 | FREE.DBZUSER.ORDERS    | free.myorders  | PRODUCT_ID   | product_id   | number          | numeric                     |

```

### **恢復連接器或重新執行整個快照**

確認物件映射正確後，即可對連接器進行匯總。請注意，復原操作只會傳輸新的表更改，不會複製表中的現有資料。

```sql
SELECT synchdb_resume_engine('olrconn');
```

為了捕獲表中的現有數據，我們還可以使用新的物件映射規則重新建立整個快照。

```sql
SELECT synchdb_stop_engine_bgw('olrconn');
SELECT synchdb_start_engine_bgw('olrconn', 'always');
```

## **選擇性表同步**

### **選擇所需表並首次啟動同步**

表格選擇在連接器建立階段透過 `synchdb_add_conninfo()` 函數完成，該函數用於指定要從中複製的表列表（以完全限定名稱 (FQN) 表示，並以逗號分隔）。

例如，以下命令建立一個連接器，該連接器僅從遠端 Oracle 資料庫複製 `FREE.ORDERS` 表中的變更。
```sql
SELECT synchdb_add_conninfo(
    'olrconn', 
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
SELECT synchdb_start_engine_bgw('olrconn');
```

### **驗證連接器狀態和表格**

檢查連接器狀態和新表格：
```sql
postgres=# Select name, state, err from synchdb_state_view;
   name  |  state  |   err
---------+---------+----------
 olrconn | polling | no error

postgres=# \dt free.*
          List of tables
 Schema |  Name  | Type  | Owner
--------+--------+-------+--------
 free   | orders | table | ubuntu

```

預設情況下，來源資料庫名稱會對應到目標資料庫的模式，並且字母大小寫策略為小寫，因此 `FREE.ORDERS` 在 PostgreSQL 中會變成 `free.orders`。表完成初始快照後，連接器將啟動 CDC 以串流傳輸這些表的後續變更。

### **運行時加入更多要複製的表**

上一節中的 `olrconn` 已完成初始快照並取得了所選表格的表格模式。如果我們想要新增更多要複製的表，則需要通知 Debezium 引擎更新後的表格部分，並再次執行初始快照。具體操作如下：

1. 更新 `synchdb_conninfo` 表以包含其他表。

2. 在本例中，我們將 `DBZUSER.CUSTOMERS` 表加入同步清單：

```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{table}', '"DBZUSER.ORDERS,DBZUSER.CUSTOMERS"') 
WHERE name = 'olrconn';
```

3. 將快照模式設為“始終”，然後重新啟動連接器，以執行另一次初始快照：

```sql
DROP table free.orders;
SELECT synchdb_restart_connector('olrconn', 'always');
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

### **篩選 Openlog Replicator 表（重要）**

對於初始快照（透過 Debezium 或 FDW），我們可以選擇要複製的表。但是，一旦連接器通過 Openlog Replicator 進入 CDC 階段，所選的表格篩選器不會自動應用於 Openlog Replicator。使用者需要配置 Openlog Replicator 服務，使其僅輸出所選表的變更事件。如果 SynchDB 和 Openlog Replicator 的表格篩選器不一致，SynchDB 可能會收到它尚未建立對應表的變更事件，從而導致錯誤。 因此，將 SynchDB 和 Openlog Replicator 配置為使用相同的表格過濾器是一種很好的做法。