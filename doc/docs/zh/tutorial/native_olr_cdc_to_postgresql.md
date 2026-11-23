#原生 Openlog Replicator 連接器

## 快照到 CDC 的交接约束

原生 OLR 交接以 OLR 1.8.5 验证，要求 Debezium 快照引擎、单 redo 线程和完整的配置 schema。OLR 的 JSON 输出必须包含事务边界、完整列元数据和每条消息的提交 SCN：`format.message=2`、`format.schema=7`、`format.scn-type=3`，并在 `flags` 中启用 `4096`（输出检查点）；保留现有 DDL 标志时可设为 `4128`。

连接器在源 schema 中维护 `SYNCHDB_OLR_FENCE`，需要建表和读取数据库身份、活动事务及 redo 目录的权限。该内部表不复制到目标。源探针必须通过实际 OLR 连接到达，快照生产者全部提交并成功完成后才进入 CDC。原生快照的读取 SCN 和未完成事务 ID 用于区分快照已覆盖的数据与之后提交的跨快照事务。目标数据和 OLR 恢复位置在同一 PostgreSQL 事务中提交，随后确认源端消息。

交接状态保存在 `synchdb_olr_positions`，按连接器 UUID 和 artifact generation 隔离。保留日志必须连续覆盖最初选择的 redo 序列；快照期间不得修改捕获表的结构。快照中断、日志缺失、数据库 incarnation 改变或 OLR 起点晚于已验证边界时，连接器停止并要求显式恢复，不自动重置进度。旧的别名 SCN 文件和快照完成标记不能用于恢复。`always` 重新建基线需要先清理目标并显式重置 artifact generation。FDW 仍仅支持快照或 schema 预览，不能交接至 CDC。

## **準備用於 SynchDB 的 Oracle 資料庫**

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

初始快照完成後，連接器在後續重新啟動時不會再次執行初始快照，而是直接從上次未完成的偏移量處恢復 CDC。恢復使用已提交的 `synchdb_olr_positions` 記錄及引擎擁有的 artifact generation；僅憑 Debezium 元資料不能進入 CDC。有關元資料檔案的更多信息，請參閱[此處](../../architecture/metadata_files/)。

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


### **始終執行初始快照 + CDC**

`always` 對新的 artifact generation 執行完整快照。重新建立既有基線前，須停止連接器、準備乾淨的目標，並呼叫 `synchdb_artifact_reset(connector_uuid)`。既有 generation 會拒絕 `always`，須明確重置；若要從持久化游標恢復 CDC，請使用 `initial`。

```sql
SELECT synchdb_start_engine_bgw('olrconn', 'always');

```

初始快照與交接驗證完成後開始 CDC。再次建立基線仍須明確清理目標並重置 generation。

## **Oracle 連接器的可用快照模式**

* initial（預設）
* initial_only
* no_data
* always
* schemasync

## **使用 schemasync 模式預覽來源表和目標表關係**

在嘗試對當前表和資料（可能非常龐大）進行初始快照之前，可以在當前資料遷移之前「預覽」來源表和目標表之間的所有表和資料類型對應。這樣，您有機會在實際遷移之前修改資料類型對應或物件名稱。這可以透過特殊的「schemasync」初始快照模式來實現。

`schemasync` 可以使用原生快照引擎，也可以使用 FDW 進行離線 schema 預覽。FDW 不會建立 CDC 交接邊界。

### **建立連接器並以 `schemasync` 模式啟動它**

`schemasync` 是一種特殊模式，它使連接器連接到遠端資料庫，並嘗試僅同步指定表的模式。完成後，連接器將處於「暫停」狀態，使用者可以查看使用預設規則建立的所有資料表和資料類型，並根據需要進行變更。

```sql
SELECT synchdb_add_conninfo(
    'olrconn',
    '127.0.0.1', 
    1521, 
    'DBZUSER', 
    'dbz', 
    'FREE', 
    'DBZUSER', 
    'null', 
    'null', 
    'olr'
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

### 恢復連接器或重新建立快照

原生 `schemasync` 完成交接後可恢復 CDC，但不會複製表中已有的資料。FDW schema 預覽不能直接恢復為 OLR CDC。若要複製已有資料，須停止預覽、選用 Debezium 快照引擎、準備乾淨的目標並重置連接器的 artifact generation，再以選定的映射規則啟動新的 `initial` 快照。

## 捕獲範圍

目前已驗證的原生 OLR 交接要求捕獲完整的設定 schema。連接器的表及 snapshot-table 篩選器須留空，OLR 也須捕獲該 schema，包括內部表 `SYNCHDB_OLR_FENCE`。工作程序將此表作為來源證據，不會複製到目標。原生 `initial_only`、FDW `initial_only` 和 FDW `schemasync` 仍可選擇表；這些只執行快照的操作不會建立 CDC 交接邊界。

來源驗證會在實際 OLR 連線上暫存尚未確認的變更。`synchdb.olr_read_buffer_size` 須能容納此重播積壓和最大的來源事務；超過上限時連接器停止，不會確認尚未提交的批次。

## 持久化重播位置

可在 `synchdb_olr_positions` 查看 `snapshot_scn`、`scn`、`c_scn`、`c_idx`。`(c_scn, c_idx)` 是真正的 OLR 重播座標，不能以 Oracle 快照 SCN 代替。游標與目標資料在同一事務中提交，因此同一連接器停止後再啟動會從已提交的位置恢復。

`synchdb_set_offset()` 不允許手動修改 OLR 位移。恢復時須一起還原相符的目標資料與交接狀態，或準備乾淨的目標並明確重置 artifact generation，建立新基線。不要直接修改游標表或複製舊 SCN 檔案以跳過來源驗證。
