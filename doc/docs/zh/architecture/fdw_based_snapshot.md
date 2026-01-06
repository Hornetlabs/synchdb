# 基於 FDW 的快照

## **概述**

所有連接器都支援基於 Debezium 的初始快照（Postgres 連接器除外），該快照會將遠端表模式遷移到 PostgreSQL，並可選擇是否包含初始資料（取決於所使用的快照模式）。雖然這種方法在大多數情況下都能正常運作，但當需要遷移大量資料表且 JNI 呼叫引入額外開銷時，可能會出現效能問題。只要我們能夠保證資料一致性並確定一個允許 CDC 恢復的“截止點”，就可以使用外部資料包裝器 (FDW) 實現相同的初始快照。

以下連接器支援基於 FDW 的快照：

* MySQL 連接器
* Postgres 連接器
* Oracle 和 Openlog Replicator 連接器

## **SynchDB 如何保證資料一致性並取得截止點**

### **MySQL 連接器**

* 以 repeatable read 隔離等級啟動事务。
* 暫時對所有目標表上锁。
* 讀取 `binlog_log_file`、`binlog_pos` 和 `server_id`。這些參數將作為快照的「截止點」。
* 釋放表鎖。
* 在同一個 repeatable read 事务中，使用正確的類型轉換遷移所有目標表的模式和資料。
* 完成後，CDC 可以從截止點恢復，處理快照期間發生的資料變更。

警告：**需要 BACKUP_ADMIN 權限才能取得「截止點」參數**

### **Postgres 連接器**

* 以 repeatable read 隔離等級啟動事务。
* 暫時對所有目標表上锁。
* 讀取目前 LSN，它作為快照的「截止」點。
* 釋放表鎖。
* 在同一個 repeatable read 事务中，使用正確的類型轉換遷移所有目標表的架構和資料。
* 完成後，CDC 可以從截止點恢復，處理快照期間發生的資料變更。

### **Oracle 和 Openlog Replicator 連接器**

* 在快照開始之前，讀取目前 SCN 值，它作為快照的「截止點」。
* 在外部表架構遷移期間，每個目標外部表都會關聯一個額外的屬性 “AS OF SCN xxx”，這將導致所有外部讀取操作都使用 Oracle 的 FLASHBACK 查詢。
* FLASHBACK 查詢傳回指定 SCN 下的表結果，因此可以自動保證一致性。無需額外上锁。
* 使用 FLASHBACK 查詢遷移所有目標表的架構和數據，並進行正確的類型轉換。
* 完成後，CDC 可以從截止點恢復運行，並處理快照期間發生的資料變更。

警告：**取得「截止點」參數需要 FLASHBACK 權限**

## **基於 FDW 的快照如何運作**

基於 FDW 的快照大約包含 10 個步驟：

### **1. 準備**

此步驟檢查 `oracle_fdw` 是否已安裝且可用，並根據 `synchdb_add_conninfo` 建立的連接器資訊建立 `server` 和 `user` 對應。

### **2. 建立 Oracle 物件視圖**

此步驟在 PostgreSQL 中，在單獨的模式（例如 `ora_obj`）中建立多個外部表。這些外部表（查詢時）透過 oracle_fdw 連接到 Oracle，並取得 Oracle 上大多數（如果不是全部）可用的物件。這些對象包括：

* 表
* 列
* 鍵
* 索引
* 函數
* 序列
* 視圖
* 觸發器
* 等等

SynchDB 無需考慮每個物件即可完成初始快照；它僅考慮 "表"、 "列" 和 "鍵" 物件來建立 PostgreSQL 中的表。

### **3. 建立外部表以取得目前截止点**


將建立外部表，以便從不同的來源資料庫讀取截止值：

**MySQL 連接器**

* 從 performance_schema.log_status 表讀取 `binlog_file` 和 `binlog_pos` 表
* 從 performance_schema.global_variables 表讀取 `server_id` 表

**Postgres 連接器**

* 從名為 public.synchdb_wal_lsn 的自訂檢視讀取目前 `LSN` 值。此視圖必須預先建立。

**Oracle 和 Openlog Replicator 連接器**

* 從 current_scn 表中讀取目前 `SCN` 值

### **4.建立所需外部表格清單**

此步驟的目標是建立一個新的暫存模式（例如 ora_stage），並基於下列條件為快照建立所需的外部表：

* 步驟 2 中建立的 Oracle 物件視圖
* SynchDB 在 `synchdb_conninfo` 中的 `data->'snapshottable'` 參數 -> 這是 SynchDB 執行快照所需的資料表的篩選器。如果設定為 `null`，則所有表都將被視為快照。
* 額外的資料類型映射，如 `synchdb_objmap` 中所述
* 步驟 3 中獲得的截止 SCN（僅限 Oracle 和 Openlog Replicator 連接器）

在此步驟結束時，暫存模式將包含外部表，其資料類型將根據 SynchDB 的資料類型對應規則進行對應。對於 Oracle 和 Openlog Replicator 連接器，外部表將具有 `AS OF SCN xxx` 屬性，導致每次外部讀取僅傳回指定 SCN 之前的資料。對於其他連接器類型，外部讀取將傳回可重複事務開始時的所有資料。

### **5.物化模式**

此步驟的目標是僅物化步驟 4 中建立的表模式（將外部表轉換為真正的 PostgreSQL 表），並將其放入新的目標模式（例如 dst_stage）。因此，在此步驟結束時，目標模式將包含與暫存模式中結構相同的真實表。

### **6. 遷移主鍵**

基於步驟 2 中建立的 Oracle 物件視圖，對步驟 5 中建立的物化表執行 `ALTER TABLE ADD PRIMARY KEY` 命令，以根據需要新增主鍵。

### **7. 應用列名映射**

基於 `synchdb_objmap` 中所述的列名映射，對步驟 5 中建立的物化表執行 `ALTER TABLE RENAME COLUMN` 命令，以根據需要更改列名。

### **8.使用轉換遷移資料**

此步驟的目標是將表格資料​​從暫存區遷移到目標模式，並執行 `synchdb_objmap` 中描述的任何值轉換，大致如下：

* 遠端查詢
* 数据轉換
* 本地插入

### **9. 應用表名映射**

資料遷移完成後，SynchDB 將套用 `synchdb_objmap` 中所描述的表名映射。之所以最後執行此操作，是因為表名映射可能會將表移動到我們不希望在物化過程中發生的其他模式。

### **10. 完成初始快照**

此時初始快照即視為完成。此步驟將執行下列清理操作：

* 刪除步驟 1 中建立的伺服器和使用者映射
* 刪除步驟 2 中建立的 Oracle 物件視圖
* 刪除步驟 4 中建立的暫存模式

步驟 5 中建立的目標模式中的表格是初始快照的最終結果。