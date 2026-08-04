# 变更日志
本项目的所有重要变更都将记录在此文件中。

本文格式基于 [Keep a Changelog](http://keepachangelog.com/)，
且本项目遵循 [语义化版本](http://semver.org/)。

## **[SynchDB 1.4](https://github.com/Hornetlabs/synchdb/releases/tag/v1.4) - 2026-08-04**

SynchDB 1.4 将 Oracle 容器数据库（CDB/PDB）支持扩展到了 Debezium、oracle_fdw 与 OLR 三条执行路径；为 MySQL 和 PostgreSQL 连接器新增了 TLS 安全连接支持（Oracle/OLR 连接器则使用 Oracle Wallet）；并支持在连接器运行期间动态调整 Debezium 运行器的日志级别，无需重启。此版本还包含一轮稳定性修复（JNI 引用泄漏、format converter 中的二次释放问题、IvorySQL 下的符号冲突），并将 CI 构建与测试矩阵扩展到以 IvorySQL 作为宿主平台。

### **新增**

#### [Oracle 容器数据库（CDB/PDB）支持](../user-guide/create_a_connector.md)

* 基于 Debezium 的 Oracle 连接器现在可以接受 `CDB/PDB` 格式的数据库名，并自动映射为 Debezium 的 `database.dbname` 与 `database.pdb.name` 属性。
* 基于 [oracle_fdw](https://github.com/laurenz/oracle_fdw) 的 FDW 快照路径现已支持 Oracle 容器数据库架构。
* 新增了针对 Oracle 23ai（Free PDB1）的 CDB/PDB 专项测试覆盖。

#### [TLS / 安全连接](../user-guide/configure_snapshot_engine.md)

* 通过 extra conninfo 参数为 MySQL 和 PostgreSQL 连接器新增了 TLS 连接支持。
* Oracle 与 OLR 连接器改为使用 Oracle Wallet 实现安全连接，而非标准 TLS 参数。

#### [运行时动态调整 Debezium 日志级别](../getting-started/configuration.md)

* 现在可以在连接器运行期间调整 Debezium 运行器的日志级别，无需重启连接器。（[#106](https://github.com/Hornetlabs/synchdb/issues/106)）

### **变更**

* Snapshot 模式 `never` 现已限定为仅 MySQL 连接器可用。此前依赖该模式的其他连接器应改用 `no_data`；`schemasync` 现已在底层统一实现为 `no_data` 语义。
* Debezium 版本从 2.6.2.Final 大版本升级至 3.5.2.Final（期间曾尝试 3.6.0.Final，但发现该版本存在已知缺陷，因此改用 3.5.2.Final），同时一并升级了 Kafka Connect（3.6.2 → 4.1.2）、Jackson 与 Log4j2 等依赖。
* 数据类型转换器现已正确捕获并传输 `tsvector` 类型的值（此前会被同步为 NULL）。
* CI 构建与测试矩阵已扩展，除 PostgreSQL 外还覆盖以 IvorySQL 作为宿主平台的场景。

### **已修复**

* 修复了 Auto Launcher 此前由于硬编码连接、只能为默认的 `postgres` 数据库启动连接器的问题；现已正确枚举所有可连接的非模板数据库，并为每个数据库派生独立的启动进程，不再受限于 SynchDB 实际安装在哪个数据库。（[#71](https://github.com/Hornetlabs/synchdb/issues/71)）
* 修复了 Debezium 桥接层中的 JNI 局部引用（local reference）泄漏问题，该问题在持续高负载下可能导致崩溃。
* 修复了 format converter 中 `StringInfoData` 的二次释放（double-free）问题。（[#252](https://github.com/Hornetlabs/synchdb/issues/252)）
* 修复了在 IvorySQL 宿主环境下，OLR 连接器加载解析器库时，SynchDB 自带的 Oracle 原始解析器与 IvorySQL 内建 Oracle 解析器之间的符号冲突问题；解析器库现改为以 `RTLD_LOCAL` 方式加载。
* `synchdb_del_conninfo()` 现在会正确清理共享内存中残留的连接器状态数据。
* 修复了 replication agent 与 OLR client 中一处潜在的内存处理问题。

### **已知問題和其他信息**

* mysql_fdw 目前无法在 IvorySQL 5.x 下编译或使用，因此该组合下基于 FDW 的 MySQL 快照路径暂不可用，待上游兼容性问题解决后再支持。
* 基于 FDW 的 SQL Server 快照仍不支持。
* 由于 FDW 快照引擎会在复制槽（replication slot）建立之前先写入 offset 文件，这与 Debezium 3.x 新增的 `validateLogPosition()` 检查冲突。目前通过将 `offset.mismatch.strategy` 设为 `trust_slot` 绕过此检查，恢复到 Debezium 3.x 之前（如 2.6）的启动行为，这是临时方案，后续会有更完整的修复。详见 [Issue #256](https://github.com/Hornetlabs/synchdb/issues/256)。

## **[SynchDB 1.3](https://github.com/Hornetlabs/synchdb/releases/tag/v1.3) - 2025-11-25**

SynchDB 1.3 憑藉全新的基於 FDW 的快照引擎，顯著提升了效能，初始快照速度遠超 Debezium。這項改進使得 OpenLog Replicator (OLR) 連接器成為一個完全原生的快照 + CDC 管線（無需使用 Debezium），從而顯著降低了大型 Oracle 資料集的延遲和開銷。平台支援也得到了擴展。 SynchDB 現在可在 PostgreSQL 18 和 IvorySQL 5 上運行，並針對整個系統進行了一系列效能最佳化和 I/O 改進。

### **新增**

#### [基於 FDW 的快照引擎](architecture/fdw_based_snapshot/)

* 基於 FDW 的快照引擎比 Debezium 的初始快照流程速度更快。
* 新增 GUC 參數“synchdb.snapshot_engine”，用於選擇快照引擎，可以是“debezium”或“fdw”。
* 新增 GUC 參數“synchdb.cdc_start_delay_ms”，用於在初始快照完成後、CDC 啟動前新增毫秒延遲。
* 新增 GUC 參數“synchdb.fdw_migrate_with_subtx”，用於在基於 FDW 的快照流程中是否使用子交易遷移每個資料表。
* 僅適用於透過 [oracle_fdw](https://github.com/laurenz/oracle_fdw) 2.8.0 連接的 Openlog Replicator 和 Oracle 連接器類型。其他連接器類型可能會在未來的版本中得到支援。
* 支援重試機制：如果某些表快照失敗，錯誤訊息將保存到 synchdb_fdw_snapshot_errors_xxx 表中，可以透過恢復連接器進行重試。成功快照的表將保留。
* 如果使用「schemasync」或「nodata」快照模式，FDW 引擎將僅同步表模式。
* 透過 FDW 引擎建立的表格和資料受「synchdb_objmap」中定義的名稱和表達式轉換規則的約束。
* 新增函數 synchdb_translate_datatype()，該函數會根據所選連接器類型傳回 SynchDB 的資料類型轉換結果。

#### [改進的統計視圖](monitoring/stats_view/)

* 改進了統計視圖，將不同的統計資料分組到適當的類別。
* 已移除 synchdb_stats_view() 函數。
* 新增了 synchdb_genstats 視圖，用於記錄已處理事件批次的統計資料。
* 新增了 synchdb_snapstats 視圖，用於記錄初始快照過程的統計資料。
* 新增了 synchdb_cdcstats 視圖，用於記錄 CDC 流程的統計資料。

#### PostgreSQL 18 和 IvorySQL 5 相容性

* SynchDB 與 PostgreSQL 18 和 IvorySQL 5 相容。
* 在 IvorySQL 5 下以「oracle」相容模式運行基於 FDW 的快照時，由於 PL/pgSQL 函數是用 PostgreSQL 標準編寫的，因此在快照期間，該模式將切換回「pg」模式。
* SynchDB 將在 SynchDB 工作進程中將“ivorysql.identifier_case_switch”設定設為“normal”，以防止字母大小寫被錯誤地反轉。
* 將 liboracle_parser.so 及其內部的 oracle_raw_parser() 符號重新命名為 libsynchdb_oracle_parser.so 和 synchdb_oracle_raw_parser()，以防止符號名稱與 IvorySQL 衝突。

### **变更**

* Openlog Replicator 連接器：增強了 Oracle 解析器，以支援更多約束運算子：enable、disable、novalidate 和 validate。
* Openlog Replicator 連接器：增強了 Oracle 解析器，以支援帶有括號和不帶括號的 MODIFY 子句。
* Openlog Replicator 連接器：增強了 Oracle 解析器，以支援 DEFAULT ON NULL 子句。
* Openlog Replicator 連接器：透過移除一個預掃描循環來提高處理效能。
* Openlog Replicator 連接器：最佳化了以 PostgreSQL 文字類型處理非空終止事件的操作，從而減少了一次複製操作。 (PG17+)
* Openlog Replicator 連接器：預設讀取緩衝區大小變更為 128MB
* Openlog Replicator 連接器：新增 GUC 參數“synchdb.olr_read_timeout_ms”，用於設定讀取逾時時間。
* Openlog Replicator 連接器：新增 GUC 參數“synchdb.olr_connect_timeout_ms”，用於設定連線逾時時間。
* Openlog Replicator 連接器：將忽略 Debezium 建立的「log_mining_flush」表。
* 更新了所有連接器類型的預設資料類型映射
* 更健壯的 JSON 處理：不再因為 JSON 元素查找失敗而崩潰

### 已修復

* Openlog Replicator 連接器：修正了在 PostgreSQL 16 下執行時間 Oracle 解析器解析失敗的問題。
* Openlog Replicator 連接器：修正了將包含系統擁有者的意外 DDL 語句錯誤地傳遞給 SynchDB 的問題。
* Openlog Replicator 連接器：調整了 SCN 和 C_SCN 值，使其恢復 CDC 時使用偏移檔案中儲存的值，而不是加 1，從而避免在復原時跳過先前失敗的變更事件。
* SynchDB 現在允許來源表或模式名稱包含空格。
* 修正了對負十進制值處理不正確的問題。
* 修正了對大十進制溢出值處理不正確的問題。
* 修正了 Oracle 預設 NUMBER 資料類型對應中錯誤地省略長度和小數位數的問題。
* 修正了 MySQL 連接器中帶有自增屬性的缺失預設資料類型映射

### 已知問題和其他信息

* SynchDB 在將傳入的表名、模式名和列名套用到 PostgreSQL 之前，總是會將它們規範化為小寫字母。如果來源表支援名稱相同但大小寫不同的對象，則會出現問題。例如，'mytable' 和 'MYTABLE' 在 MySQL 和 Oracle 中被視為不同的表名。問題連結[此處](https://github.com/Hornetlabs/synchdb/issues/194)
* 目前尚不支援基於 FDW 的 MySQL 和 SQL Server 快照，我們可能會在未來的版本中新增支援。更多資訊[此處](https://github.com/Hornetlabs/synchdb/issues/190)

## **[SynchDB 1.2](https://github.com/Hornetlabs/synchdb/releases/tag/v1.2) - 2025-09-04**

SynchDB 1.2 引入了原生 Openlog Replicator 连接器（测试版）、增强的 JMX 和 Grafana 监控功能，以及用于快速部署和测试的全新 ezdeploy.sh 工具。此外，它还增加了快照表选择功能、性能改进和关键修复，同时解决了连接器隔离和稳定性问题。

### **新增**

#### [原生 Openlog Replicator 连接器](user-guide/configure_olr/) - 测试版
* 新增 `synchdb_add_olr_conninfo` 和 `synchdb_del_olr_conninfo`，用于启用或禁用基于 Openlog Replicator 的流式传输。
* 添加了新的连接器类型“olr”，它是一个原生（非 Debezium）Openlog 复制器客户端，可从外部 Openlog Replicator 服务流式传输 Oracle 数据库更改。
* 支持的 DML：插入、更新/删除
* 支持的 DDL：创建表、删除表、修改表、添加/删除列、添加/删除约束、截断
* 基于 libprotobuf-c 与 Openlog Replicator 和 IvorySQL 的 Oracle 解析器通信，以处理传入的 DDL 查询事件。
* 支持的快照模式：initial、initial_only、no_data、always
* 与其他基于 Debezium 的连接器一样，支持批处理、模式历史记录和偏移量管理。
* 除原生连接器外，还支持基于 Debezium 的 Openlog Replicator 连接器。

#### [监控](monitoring/jmx_monitor/)

* 新增 `synchdb_add_jmx_conninfo` 和 `synchdb_del_jmx_conninfo`，用于启用或禁用基于 JMX 的监控。
* 新增 `synchdb_add_jmx_exporter_conninfo` 和 `synchdb_del_jmx_exporter_conninfo`，用于启用或禁用 Prometheus 和 Grafana 的监控支持。
* 新增基于 Grafana 的仪表板模板，用于支持基于 Debezium 的连接器（MySQL、SQL Server 和 Oracle）。

#### [ezdeploy.sh](getting-started/quick_start/)
* 新增 `ezdeploy.sh` 工具，可快速部署预构建的 SynchDB 和所选的源数据库类型，以便快速进行连接器测试。
* 支持部署：MySQL、SQL Server、Oracle23ai、Oracle19c、Openlog Replicator 1.3.0
* 支持预加载仪表板的 Prometheus 和 Grafana 部署。
* 支持预编译的 SynchDB v1.2，以便快速部署和测试。

### **变更**

* 在 `synchdb_add_conninfo` 中添加了一个名为 `snapshot table` 的新参数，允许用户在以 `always` 快照模式启动时选择要重做初始快照的表。
* 更新了 pytest 框架，以支持基于 hammerdb 的 Oracle TPC 测试。
* 通过使用直接缓冲区代替频繁的 JNI 调用，增强了 Debezium 引擎和 SynchDB 之间事件轮询的性能。
* 当连接器通过 `synchdb_resume_engine` 恢复时，它将始终以 `initial` 快照模式恢复，而不是首次启动时的模式。
* `synchdb_state_view` 和 `synchdb_stats_view` 现在仅显示当前 SynchDB 扩展创建的连接器信息。其他 SynchDB 扩展在不同数据库中创建的连接器将不显示。

### **已修复**

* 修复了 `spi_execute_select_one()` 函数崩溃的问题，该函数在 SPI 执行成功后会返回一个已被 SPI 内存上下文销毁的引用。
* 解决了使用 cassert 构建 PostgreSQL 时 SynchDB 的编译问题。
* 修复了多个 SynchDB 扩展（创建于不同数据库中）创建的同名连接器会相互覆盖共享内存数据的问题。

### **已知问题及其他信息**

* 原生 Openlog Replicator 连接器目前会流式传输指定数据库下的所有表。表过滤功能需要在 Openlog Replicator 中配置，而不是在 SynchDB 中。
* 基于 Prometheus 和 Grafana 的监控需要 JMX Exporter，可以从[此处](https://github.com/prometheus/jmx_exporter) 下载

## **[SynchDB 1.1](https://github.com/Hornetlabs/synchdb/releases/tag/v1.1) - 2025-04-17**

SynchDB 1.1 版本引入了 Oracle 连接器支持，增强了数据类型转换能力，并显著改进了核心数据处理引擎。本次更新通过智能缓存和优化的 JSON 解析，提升了性能表现，同时扩展了与 PostgreSQL 16、17 和 IvorySQL 4.4 的兼容性。

### **添加**

#### **连接器支持**

* 新增 Oracle 连接器支持
* 新增 Oracle 可变大小/精度 NUMBER 数据类型处理支持
* 支持 PostgreSQL 16、17 和 IvorySQL 4.4

#### **对象映射与数据转换**

* 新增 `synchdb_objmap` 表存储对象映射条目
* 新增 `synchdb_add_objamp()` 函数添加对象映射，支持表名、列名、数据类型和转换表达式
* 新增 `synchdb_reload_objmap()` 函数强制连接器重新加载对象映射
* 新增 `synchdb_del_objmap()` 函数禁用并删除对象映射条目
* 归一化所有数据类型映射条目，使用小写字母

#### **元数据与监控**

* 新增 `synchdb_attribute` 表存储远程表属性信息
* 新增 `synchdb_att_view` 视图显示数据类型、表名和列名映射的并排比较
* 在 `synchdb_get_stats` 中增加源时间戳、DBZ 时间戳和 PG 时间戳信息

#### **连接管理**

* 新增 `synchdb_add_extra_conninfo()` 函数配置额外的 SSL 连接参数
* 新增 `synchdb_del_extra_conninfo()` 函数删除由 `synchdb_add_extra_conninfo()` 创建的所有额外参数
* 新增 `synchdb_del_conninfo()` 函数删除现有连接器信息

#### **配置与控制**

* 新增 `synchdb.error_handling_strategy` GUC 参数控制错误处理策略（跳过、退出或重试）
* 新增 `synchdb.dbz_log_level` GUC 参数控制 Debezium 运行引擎的日志级别
* 新增 `schemasync` 快照模式

#### **性能优化**

* 优化 DML 解析器，使用缓存哈希表而非每次更改事件都访问目录并重建哈希
* DML 解析器现使用更高效的 JSONB API 调用提升性能
* 优化 JNI 调用，使用缓存的 JNI 句柄而非每次更改事件都重新创建
* 仅标记批次中的第一个和最后一个更改事件，而非所有事件
* 重新设计数据处理引擎，采用更模块化的设计，以处理更复杂的数据类型映射

### **变更**

* 处理 ALTER TABLE 更改事件时，仅在表本身没有主键时添加主键
* 移除在 `synchdb_add_conninfo()` 中配置连接器连接到安装了 synchdb 以外的目标 PostgreSQL 数据库的功能
* 在每个 DDL 更改事件处理结束时，更新新的 `synchdb_attribute` 表
* 连接器将根据配置的对象映射在启动或重新加载时更正表名、列名和数据类型
* 规则文件已被 `synchdb_add_objmap()` 工具替代
* 所有基于 ID 的 SQL 函数列定义，数据类型从 TEXT 更改为 NAME
* 从 `synchdb_add_conninfo()` 中移除 rulefile 参数
* 非原生数据类型现在基于类别处理，而非全部作为文本处理

### **修复**

* 修复 SPI 更新或删除可能因仅在 WHERE 子句中包含主键字段而导致更新或删除失败的问题
* 修复 ALTER TABLE 在尝试添加重复主键时出错的问题
* 修复 synchdb 偶尔从 JSON 更改事件中错误查找列架构值的问题
* 修复"跳过错误"模式下因错误堆栈溢出而导致崩溃的问题
* 修复 ALTER TABLE ADD 和 DROP 列操作中未意识到列名可能在 PostgreSQL 中映射到不同值的问题

## **[SynchDB 1.0](https://github.com/Hornetlabs/synchdb/releases/tag/v1.0) - 2024-12-24**

此版本侧重于 v1.0 Beta1 版本之后的错误修复和性能增强，使其在中高数据负载下更易于使用。更多与 Debezium 调优相关的参数也已作为 PostgreSQL GUC 公开，允许用户使用不同的参数进行测试。

### **添加**

* 在 DML 解析阶段添加了数据缓存，以防止频繁访问 PostgreSQL catalog 以获取表的元组描述符结构。
* 添加 `synchdb_start_engine_bgw(name, mode)` 的变体，它接受第二个参数来指示启动连接器时使用的自定义快照模式。
* 添加多个可以调整 Debezium Runner 性能的 GUC。完整列表请参阅[此处](getting-started/configuration)。
* 添加一个调试 SQL 函数 `synchdb_log_jvm_meminfo(name)`，该函数使指定的连接器在 PostgreSQL 日志文件中输出当前 JVM 堆内存使用情况摘要。
* 添加一个新视图 `synchdb_stats_view`，用于打印所有连接器的统计信息。
* 增加了新的SQL函数 `synchdb_reset_stats(name)` 来清除指定连接器的统计信息。
* 添加一个测试数据创建脚本，用于快速生成 MySQL 数据库类型的测试表和数据。

### **变更**

* synchdb_state_view()：添加了一个名为 `stage` 的新字段，用于标识 connector 的当前阶段（值可以是 `Initial Snapshot` 或 `Change Data Capture`）。
* synchdb_state_view()：仅显示有效连接器的状态。
* 删除了在发生错误时向 Debezium Runner 发送 “部分批处理完成” 通知，因为批处理现在由一个 PostgreSQL 事务处理，且不允许部分完成。
* 现在可以在规则文件中指定 connector 所需的 SSL 相关参数。
* 现在可以通过 GUC 配置分配给运行 Debezium Runner 的 JVM 的最大堆内存。
* 现在可以配置连接器后台进程的最大数量，而不是硬编码的 30。

### **修复**

* 在 Debezium runner 内添加节流控制，修复了 Debezium Runner 在数据量大时内存快速积累问题。
* 解决了 SynchDB 和 Debezium runner 组件中的大部分内存泄漏问题。
* 更正了 SynchDB 中 memory context 的使用，以便在每次更改事件处理结束时正确释放内存。
* 通过在单个 PostgreSQL 事务中处理一批改动，显著提高了 SynchDB 的处理速度。
* 将 SQLServer 的 `char` 类型的默认数据类型大小映射从 0 更正为 -1。
* 解决了使用 SPI 进行 DML 处理期间内存使用率过高的问题。

## [SynchDB 1.0 Beta1](https://github.com/Hornetlabs/synchdb/releases/tag/v1.0_beta1) - 2024-10-23

第一版 SynchDB 软件发布，为从异构数据库到 PostgreSQL 的无缝复制奠定了坚实的基础

### **新增**

* 从异构数据库进行逻辑复制：（MySQL 和 SQLServer）。
* [DDL 复制](architecture/ddl_replication/) (CREATE TABLE、DROP TABLE、ALTER TABLE ADD COLUMN、ALTER TABLE DROP COLUMN、ALTER TABLE ALTER COLUMN)
* DML 复制（INSERT、UPDATE、DELETE）。
* 最多 30 个并发连接器工作器。
* PostgreSQL 启动时 [自动连接器启动器](user-guide/connector_auto_launcher/)。
* 全局连接器状态和最后错误消息视图。
* [选择性数据库和表复制](tutorial/selective_table_sync/)。
* 批量更改事件。
* 连接器以不同的快照模式重新启动。
* [偏移管理接口](user-guide/set_offset) 用于选择自定义复制恢复点。
* 支持的异构数据库的默认数据类型和对象名称转换规则。
* [JSON 规则文件](user-guide/object_mapping_rules) 用于定义自定义：（数据类型、列名、表名和数据表达式转换规则）。
* 2 种数据应用模式（SPI、HeapAM API）
* 新增实用函数用于执行连接器操作：（启动、停止、暂停、恢复）。

### **变更**

### **修复**