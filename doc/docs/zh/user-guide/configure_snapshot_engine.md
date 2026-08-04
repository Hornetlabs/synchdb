# 設定快照引擎

## **初始快照與變更資料擷取 (CDC)**

初始快照是指將表格結構和初始資料從遠端資料庫遷移到 SynchDB 的過程。對於大多數連接器類型，此程序僅在首次啟動連接器時透過嵌入式 Debezium 運行器執行一次，因為只有 Debezium 運行器才能執行初始快照。初始快照完成後，變更資料擷取將開始向 SynchDB 即時傳輸變更。 「快照模式」可以進一步控制初始快照和 CDC 的行為。有關更多信息，請參閱[此處](../../user-guide/start_stop_connector/)。

SynchDB 支援兩種快照引擎，每種引擎支援多種連接器類型：

**基於 Debezium 的快照：**

* MySQL
* SQL Server
* Oracle
* Openlog Replicator

**基於 FDW 的快照：**

<**注意**> IvorySQL 5.x 版本無法編譯 mysql_fdw

* MySQL（使用 mysql_fdw）
* Oracle（使用 oracle_fdw）
* Openlog Replicator（使用 oracle_fdw）
* Postgres（使用 postgres_fdw）

## **基於 Debezium 的初始快照**

這是預設的快照引擎（synchdb.olr_snapshot_engine='debezium'），它透過 JNI 依賴 Debezium 引擎來啟動快照過程。在大多數情況下，它都能正常工作，但如果快照中包含的表數量很多（數千個或更多），則可能會出現效能問題。 Debezium 首先會進行分析，存取每個表格一次以了解其結構並估算要快照的行數。此過程完全由單一工作流程完成，在此期間，引擎可能看似“掛起”，不會發出任何變更事件。對於大量表，此過程本身可能需要數小時才能完成。完成後，它將開始向 SynchDB 發送變更事件。

在呼叫 `synchdb_start_engine_bgw()` 時自動觸發，快照模式要求執行快照，完成後不會再執行。

## **基於 FDW 的初始快照**

* 在 `postgresql.conf` 中透過將「synchdb.olr_snapshot_engine」設為「fdw」來啟用。
* 在呼叫 `synchdb_start_engine_bgw()` 時自動觸發，快照模式要求執行快照，完成後不會再執行。
* 必須有對應的 FDW 可用。

## **Oracle 和 OLR 連接器的 FDW 快照**

可以使用 oracle_fdw 為 Oracle 和 OLR 連接器執行初始快照，速度比 Debezium 的同類工具快得多。 SynchDB 整合了 [oracle_fdw](https://github.com/laurenz/oracle_fdw) 2.8.0 版本，其底層驅動程式為 OCI v23.9.0。這只是測試版本，更舊或更新的版本可能也適用。

### **安裝 OCI**

建置和運行 oracle_fdw 需要 OCI。本範例使用的是 23.9.0 版本：

```bash
# Get the pre-build packages:
wget https://download.oracle.com/otn_software/linux/instantclient/2390000/instantclient-basic-linux.x64-23.9.0.25.07.zip
wget https://download.oracle.com/otn_software/linux/instantclient/2390000/instantclient-sdk-linux.x64-23.9.0.25.07.zip

# unzip them: Select 'Y' to overwrite metadata files
unzip instantclient-basic-linux.x64-23.9.0.25.07.zip
unzip instantclient-sdk-linux.x64-23.9.0.25.07.zip
```

更新環境變量，以便系統知道在哪裡可以找到 OCI 頭檔和庫。

```bash
export OCI_HOME=/home/$USER/instantclient_23_9
export OCI_LIB_DIR=$OCI_HOME
export OCI_INC_DIR=$OCI_HOME/sdk/include
export LD_LIBRARY_PATH=$OCI_HOME:${LD_LIBRARY_PATH}
export PATH=$OCI_HOME:$PATH

```

您可以將上述命令新增至 `~/.bashrc` 檔案的末尾，以便在每次登入系統時自動設定 PATH 環境變數。

您也可以將 `$OCI_HOME` 新增至 `/etc/ld.so.conf.d/x86_64-linux-gnu.conf` 檔案（以 Ubuntu 為例），作為尋找共享庫的新路徑。

您應該執行 `ldconfig` 命令，以便連結器知道在哪裡可以找到共用程式庫：

### **建置並安裝 oracle_fdw 2.8.0**

```bash
git clone https://github.com/laurenz/oracle_fdw.git --branch ORACLE_FDW_2_8_0

```

請確保 oracle_fdw 的 Makefile 可以找到 OCI 頭檔和函式庫，方法是調整 Makefile 中的這兩行：

```bash
FIND_INCLUDE := $(wildcard /usr/include/oracle/*/client64 /usr/include/oracle/*/client /home/$USER/instantclient_23_9/sdk/include)
FIND_LIBDIRS := $(wildcard /usr/lib/oracle/*/client64/lib /usr/lib/oracle/*/client/lib /home/$USER/instantclient_23_9)

```

建置並安裝oracle_fdw：

```bash
make PG_CONFIG=/usr/local/pgsql/bin/pg_config
sudo make install PG_CONFIG=/usr/local/pgsql/bin/pg_config

```

oracle_fdw 已準備就緒。正常啟動連接器，並將 synchdb.olr_snapshot_engine 設定為 'fdw'。如果需要快照，SynchDB 將透過 FDW 完成。您無需在使用基於 FDW 的初始快照之前執行 `CREATE EXTENSION oracle_fdw`，也無需執行 `CREATE SERVER` 或 `CREATE USER MAPPING`。 SynchDB 在執行快照時會自動處理所有這些操作。

<**注意**> 若目標為 Oracle 容器資料庫（CDB/PDB），在 `synchdb_add_conninfo()` 建立連接器時，將來源資料庫（srcdb）指定為 `CDB/PDB` 格式（例如 `FREE/FREEPDB1`）即可，SynchDB 會自動連線到對應的 PDB 服務。

## **MySQL 連接器的 FDW 快照**

可以使用 mysql_fdw 為 MySQL 連接器執行初始快照，其速度遠遠超過 Debezium 的同類工具。 SynchDB 整合了 [mysql_fdw](https://github.com/EnterpriseDB/mysql_fdw) 2.9.3 版本，並以 libmysqlclient v8 作為其底層驅動程式。這只是經過測試的版本，更舊或更新的版本可能也適用。

### **安裝 MySQL 用戶端開發套件**

您可以從[此處](https://packages.debian.org/sid/libmysqlclient-dev)下載並安裝 MySQL 用戶端開發套件。如果您使用的是 Ubuntu 系統，則可以使用套件管理器進行安裝：

```bash
sudo apt update
sudo apt install libmysqlclient-dev

```

### **建置並安裝 mysql_fdw 2.9.3**

```bash
git clone https://github.com/EnterpriseDB/mysql_fdw.git --branch REL-2_9_3

```

建置並安裝 mysql_fdw：

```bash
make PG_CONFIG=/usr/local/pgsql/bin/pg_config
sudo make install PG_CONFIG=/usr/local/pgsql/bin/pg_config

```

mysql_fdw 已準備就緒。正常啟動連接器，並將 synchdb.olr_snapshot_engine 設定為 'fdw'。如果需要快照，SynchDB 將透過 FDW 完成。您無需在使用基於 FDW 的初始快照之前執行 `CREATE EXTENSION mysql_fdw`，也無需執行 `CREATE SERVER` 或 `CREATE USER MAPPING`。 SynchDB 會在執行快照時處理所有這些操作。

## **Postgres 連接器的 FDW 快照**

可以使用 postgres_fdw 為 Postgres 連接器執行初始快照，這比 Debezium 的同類方法速度快得多。 SynchDB 與基於 PostgreSQL 18 的 [postgres_fdw](https://github.com/postgres/postgres/tree/master/contrib/postgres_fdw) 整合。這只是經過測試的版本，更舊或更新的版本可能也適用。

### **建置與安裝 postgres_fdw**

```bash
git clone https://github.com/postgres/postgres.git --branch REL_18_0
cd postgres
./configure
make 
sudo make install

cd contrib/postgres_fdw
make 
sudo make install

```

postgres_fdw 已準備就緒。正常啟動連接器，並將 synchdb.olr_snapshot_engine 設定為 'fdw'。如果需要快照，SynchDB 將透過 FDW 完成。您無需在使用基於 FDW 的初始快照之前執行 `CREATE EXTENSION postgres_fdw`，也無需執行 `CREATE SERVER` 或 `CREATE USER MAPPING`。 SynchDB 在執行快照時會自動處理所有這些操作。

## **FDW 快照的 TLS 安全連接**

基於 FDW 的快照在連線遠端資料庫時，可以依連接器類型設定 TLS（MySQL、Postgres）或 Oracle Wallet（Oracle、OLR）以確保連線安全。這與 [`synchdb_add_extra_conninfo()`](../../tutorial/mysql_cdc_to_postgresql/#synchdb_add_extra_conninfo) 為 Debezium JDBC 連線設定的 Java Keystore/Truststore 是相互獨立的兩套機制。

### **synchdb_add_fdw_conninfo**

用途：為指定連接器設定 FDW 連線所需的 TLS 憑證檔案路徑（客戶端憑證、私鑰、CA/根憑證）以及可選的加密套件清單。

```sql
SELECT synchdb_add_fdw_conninfo(name, ssl_cert, ssl_key, ssl_rootcert, ssl_cipher);
```

| 參數 | 說明 |
|-|-|
| `ssl_cert` | 客戶端憑證檔案路徑（PEM 格式）。不需要時可留空字串 `''` |
| `ssl_key` | 客戶端私鑰檔案路徑（PEM 格式）。不需要時可留空字串 `''` |
| `ssl_rootcert` | CA / 根憑證檔案路徑（PEM 格式）。**對 Oracle 和 OLR 連接器而言，此參數改為 Oracle Wallet 所在的目錄路徑**。不需要時可留空字串 `''` |
| `ssl_cipher` | 加密套件清單（僅 mysql_fdw 支援）。不需要時可留空字串 `''` |

各連接器類型的實際行為：

* **MySQL（mysql_fdw）**：`ssl_cert`、`ssl_key`、`ssl_rootcert`（對應 mysql_fdw 的 `ssl_ca`）、`ssl_cipher` 會直接套用到 `CREATE SERVER ... OPTIONS (...)` 中。
* **Postgres（postgres_fdw）**：`ssl_cert`、`ssl_key`、`ssl_rootcert` 分別對應 postgres_fdw 的 `sslcert`、`sslkey`、`sslrootcert` 選項；`sslmode` 則透過 [`synchdb_add_extra_conninfo()`](../../tutorial/mysql_cdc_to_postgresql/#synchdb_add_extra_conninfo) 設定（keystore/truststore 相關參數在此情況下可留空）。
* **Oracle 和 OLR（oracle_fdw）**：僅使用 `ssl_rootcert`，其值為 Oracle Wallet 所在的目錄路徑；SynchDB 會自動將連線字串切換為 Easy Connect Plus 格式（`tcps://host:port/service?wallet_location=...&ssl_server_dn_match=yes`）。`ssl_cert`／`ssl_key` 對 oracle_fdw 無效——若需要用戶端憑證，須事先透過 `orapki` 或 `openssl pkcs12` 將其匯入到指定的 Wallet 目錄中。

範例（MySQL）：
```sql
SELECT synchdb_add_fdw_conninfo('mysqlconn', '/path/to/client-cert.pem', '/path/to/client-key.pem', '/path/to/ca.pem', '');
```

範例（Oracle / OLR，使用 Oracle Wallet）：
```sql
SELECT synchdb_add_fdw_conninfo('oracleconn', '', '', '/path/to/wallet_dir', '');
```

### **synchdb_del_fdw_conninfo**

用途：刪除由 `synchdb_add_fdw_conninfo` 設定的所有 TLS 憑證路徑。

```sql
SELECT synchdb_del_fdw_conninfo('mysqlconn');
```