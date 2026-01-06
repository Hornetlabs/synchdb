---
weight: 40
---
# 配置说明

SynchDB 在 postgresql.conf 中支持以下 GUC 变量。这些是适用于 SynchDB 管理的所有Debezium连接器的通用参数：

| GUC 变量 | 类型 | 默认值 | 描述 |
|-|-|-|-|
| synchdb.naptime | integer | 100 | 从 Debezium runner 引擎轮询数据的时间间隔（毫秒） |
| synchdb.dml_use_spi | boolean | false | 是否使用 SPI 处理 DML 操作 |
| synchdb.synchdb_auto_launcher | boolean | true | 是否自动启动活跃的 SynchDB 连接器工作进程。此选项仅在 SynchDB 被包含在 `shared_preload_library` GUC 选项中时生效 |
| synchdb.dbz_batch_size | integer | 2048 | Debezium 嵌入式引擎生成的 SynchDB 可处理的最大变更事件数。此批次变更由 SynchDB 在单个事务中处理 |
| synchdb.dbz_queue_size | integer | 8192 | Debezium 嵌入式引擎的变更事件队列的最大大小（以变更事件数衡量）。应将其设置为 `synchdb.dbz_batch_size` 的至少两倍 |
| synchdb.dbz_connect_timeout_ms | integer | 30000 | Debezium 嵌入式引擎与远程数据库建立初始连接的超时值（以毫秒为单位） |
| synchdb.dbz_query_timeout_ms | integer | 600000 | Debezium 嵌入式引擎在远程数据库上执行查询的超时值（以毫秒为单位） |
| synchdb.dbz_skipped_oeprations | string | “t” | Debezium 在处理更改事件时应跳过的操作的逗号分隔列表。 “c” 表示插入，“u” 表示更新，“d” 表示删除，“t” 表示截断 |
| synchdb.jvm_max_heap_size | integer | 1024 | 启动连接器时分配给 Java 虚拟机 (JVM) 的最大heap内存大小（以 MB 为单位）。 |
| synchdb.dbz_snapshot_thread_num | integer | 2 | Debezium 嵌入式连接器在初始快照期间应产生的线程数。请注意，根据 Debezium，多线程快照是一项“孵化功能” |
| synchdb.dbz_snapshot_fetch_size | integer | 0 | Debezium 嵌入式连接器在初始快照期间应一次获取的行数。将其设置为 0 以让引擎自动选择 |
| synchdb.<br>dbz_snapshot_min_row_<br>to_stream_results | integer | 0 | 在初始快照期间，Debezium 嵌入式引擎切换到流模式之前远程表应包含的最小行数。将其设置为 0 以始终切换到流模式 |
| synchdb.<br>dbz_incremental_<br>snapshot_chunk_size | integer | 2048 | 在增量快照期间，Debezium 嵌入式引擎为 SynchDB 生成的更改事件的最大数量 |
| synchdb.<br>dbz_incremental_<br>snapshot_watermarking_strategy | 字符串 | “insert-insert | Debezium 嵌入式引擎用于解决增量快照期间潜在冲突的水印策略。可能的值是“insert-insert”和“insert-delete”|
| synchdb.dbz_offset_flush_interval_ms | integer | 60000 | Debezium 嵌入式引擎将偏移数据刷新到磁盘的间隔（以毫秒为单位）|
| synchdb.<br>dbz_capture_only_selected_table_ddl | boolean | true | Debezium 嵌入式引擎是否应在初始快照期间捕获所有表（false）或选定表（true）的模式 |
| synchdb.max_connector_workers | integer | 30 | 最大的连接器后台进程数量 |
| synchdb.error_handling_strategy | enum | "exit" | 配置连接器工作器的错误处理策略。可能的值有“exit”表示出错时退出，“skip”表示出错时继续，“retry”表示出错时重试 |
| synchdb.dbz_log_level | enum | "warn" | Debezium Runner 的日志级别设置。可能的值有“debug”，“info”，“warn”，“error”，“all”，“fatal”，“off”，“trace” |
| synchdb.log_change_on_error | boolean | true | 连接器是否应在发生错误时记录原始 JSON 更改事件 |
| synchdb.jvm_max_direct_buffer_size | integer | 1024 | 分配用于保存 JSON 更改事件的最大直接缓冲区大小（以 MB 为单位）|
| synchdb.dbz_logminer_stream_mode | enum | "uncommitted" | 基於 Debezium 的 Oracle 連接器的流模式。預設值為uncommitted，這表示透過 Debezium 從 Oracle 串流傳輸的所有變更均未提交。這表明 Debezium 必須執行一些工作來確保事務和所有相關變更的完整性。設定為 "committed" 會將這項工作轉移到 Oralce 端 |
| synchdb.olr_connect_timeout_ms | integer | 5000 |（僅影響 OLR 連接器）連接到 OpenLog 複製器服務時的連線逾時時間（以毫秒為單位）|
| synchdb.olr_read_timeout_m | integer | 5000 |（僅影響 OLR 連接器）從套接字讀取時的讀取逾時時間（以毫秒為單位）|
| synchdb.olr_snapshot_engine | enum | "debezium" | （僅影響 OLR 連接器）指定底層引擎完成初始快照程序。可以是“debezium”或“fdw”。如果選擇“fdw”，則需要確保在 |
| synchdb.cdc_start_delay_ms | integer | 0 | 初始快照完成後、CDC 流開始前等待的延遲時間。 |
| synchdb.fdw_migrate_with_subtx | boolean | true | 在 FDW 的快照期間，是否使用子交易遷移表的選項 |
| synchdb.letter_casing_strategy | enum | "lowercase" | synchdb 應如何處理來自不同数据库源的物件字母大小寫差異。可選值為："lower"（規範化為小寫）、"upper"（規範化為大寫）或 "asis"（保留接收到的任何字母大小寫）。

## **技术说明**

- GUC（Grand Unified Configuration）是 PostgreSQL 中的全局配置参数
- 这些值需要在 `postgresql.conf` 文件中设置
- 修改配置后需要重启服务器才能生效
- `shared_preload_library` 是一个关键的系统配置，它决定了启动时加载哪些库，synchdb 必须放在这里才能启用 [连接器自动启动器](../../user-guide/connector_auto_launcher/)

## **配置示例**

```conf
# postgresql.conf 配置示例
synchdb.naptime = 1000                                                  # 将等待时间增加到1秒
synchdb.dml_use_spi = true                                              # 启用 SPI 处理 DML 操作
synchdb.synchdb_auto_launcher = true                                    # 启用自动连接器启动synchdb.dbz_batch_size=4096 # 每个批次最多可以有 4096 个更改事件
synchdb.dbz_queue_size=8192                                             # Debezium 将使用 8192 个更改事件队列大小
synchdb.jvm_max_heap_size=2048                                          # 分配给连接器的 2GB 堆内存
synchdb.dbz_snapshot_fetch_size=0                                       # 让 Debezium 确定在初始快照期间要获取的最佳行数
synchdb.dbz_min_row_to_stream_results=0                                 # 始终在初始快照期间流式传输结果
synchdb.dbz_snapshot_thread_num=1                                       # Debezium 初始快照期间的单个线程
synchdb.dbz_incremental_snapshot_chunk_size=4096                        # 增量快照以 4096 个批次生成更改事件max
synchdb.dbz_incremental_snapshot_watermarking_strategy='insert_insert'  # 使用 insert_insert 水印策略
synchdb.dbz_offset_flush_interval_ms=60000                              # 如果需要，每分钟将偏移数据刷新到磁盘
synchdb.dbz_capture_only_selected_table_ddl=false                       # Debezium 将仅捕获选定表的模式，而不是所有表
synchdb.max_connector_workers=false                                     # 最多允许 10 个连接器
synchdb.error_handling_strategy='retry'                                 # 连接器应在发生错误时重试
synchdb.dbz_log_leve='error'                                            # Debezium Runner 应仅记录错误消息
synchdb.log_change_on_error=true                                        # 发生错误时记录 JSON 更改事件
synchdb.cdc_start_delay_ms=30000                                        # 快照完成後等待 30 秒，CDC 開始前等待
synchdb.olr_snapshot_engine="fdw"                                       # 使用基於 FDW 的快照引擎完成快照過程
synchdb.letter_casing_strategy="asis"                                   # 保留原始檔案中所有物件字母的大小寫。
```

## **使用建议**

1. **synchdb.naptime**
    - 较低的值：更新频率更高，但系统负载更大
    - 较高的值：系统负载较低，但更新频率降低
    - 根据数据延迟要求进行调整

2. **synchdb.dml_use_spi**
    - 需要特定 SPI 集成时启用
    - 标准 DML 操作保持 `false`

3. **synchdb.synchdb_auto_launcher**
    - 建议保持 `true` 以实现connecot自动启用管理
    - 仅在需要手动控制连接器时改为 `false`

4. **synchdb.dbz_batch_size**
    - 值越低：JVM 内存使用量越低，处理变更事件的速度越慢
    - 值越高：JVM 内存使用量越高，处理变更事件的速度越快
    - 根据资源需求进行调整

5. **synchdb.dbz_queue_size**
    - 值越低：用于保存变更事件的 Debezium 队列越小
    - 值越高：用于保存变更事件的 Debezium 队列越大
    - 需要设置为 `synchdb.dbz_batch_size` 的至少两倍

6. **synchdb.jvm_max_heap_size**
    - 值越低：分配给 JVM 的堆内存越小
    - 值越高：分配给 JVM 的堆内存越大
    - 根据系统资源和工作负载需求进行调整
    - 处理大量表时需求会增加

7. **synchdb.dbz_snapshot_fetch_size**
    - 值越低：快照期间从表中获取的行越少
    - 值越高：获取的行越多快照期间要从表中获取的行数
    - 建议将其保留为 0，以便让 Debezium 找出最佳值

8. **synchdb.dbz_min_row_to_stream_results**
    - 较低的值：JVM 内存需求较少，更改事件处理较慢
    - 较高的值：JVM 内存需求较多，更改事件处理较快
    - 建议将其保留为 0，以便让 Debezium 始终使用流模式以减少内存使用量

9. **synchdb.dbz_snapshot_thread_num**
    - 较低的值：将数据导出到 SynchDB 进行处理的速度较慢
    - 较高的值：将数据导出到 SyncDB 进行处理的速度较快
    - 建议将其设置为相同的 CPU 核心数

10. **synchdb.dbz_incremental_snapshot_chunk_size**
    - 较低的值：增量快照期间，在较低的 JVM 内存使用量下，更改事件处理较慢
    - 较高的值：增量快照期间，在较高的 JVM 内存使用量下，更改事件处理较快
    - 建议将其设置为与 `synchdb.dbz_batch_size` 相同，并根据资源需求进行调整

11. **synchdb.dbz_offset_flush_interval_ms**
    - 较低的值：更频繁地更新偏移文件，更多的 IO，故障恢复后重新处理的旧批次较少
    - 较高的值：更不频繁地更新偏移文件，更少的 IO，故障恢复后重新处理的旧批次较多
    - 建议将其设置为 60000，这是 Debezium 的建议

12. **synchdb.max_connector_workers**
    - 较低的值：一次可以运行的连接器工作器越少，共享内存需求越少
    - 较高的值：一次可以运行的连接器工作器越多，共享内存需求越多

## **性能考虑**

- 根据系统负载和延迟要求调整 `synchdb.naptime`
- 调高 `synchdb.dbz_batch_size` 和 `synchdb.dbz_queue_size` 以增加处理吞吐量
- 根据工作负载调整 `synchdb.jvm_max_heap_size`
- 表数量较少（10k 或更少）+ 每个表的数据量适中或较大：512MB ~ 1024MB 就足够了
- 表数量较多（100k 或更多）+ 每个表的数据量适中：考虑增加到 2048MB 或以上
- 将 `synchdb.dbz_snapshot_fetch_size` 设置为 0，让 Debezium 选择最佳提取值
- 将 `synchdb.dbz_snapshot_thread_num` 设置为与 CPU 核心数匹配
- 将 `synchdb.dbz_min_row_to_stream_results` 设置为 0，以始终使用流模式来减少内存使用量

## **常见使用场景**

### **高吞吐量系统**
```conf
synchdb.naptime = 10            # 更快的轮询以实现实时更新
synchdb.dml_use_spi = false     # 标准 DML 以获得更好的性能
synchdb.dbz_batch_size = 16384
synchdb.dbz_queue_size = 32768
synchdb.jvm_max_heap_size = 2048
synchdb.dbz_snapshot_thread_num = 4
synchdb.dbz_snapshot_fetch_size = 0
synchdb.dbz_min_row_to_stream_results = 0
```

### **资源受限系统**
```conf
synchdb.naptime = 1000          # 降低轮询频率
synchdb.dml_use_spi = false     # 最小化额外开销
synchdb.dbz_batch_size = 1024
synchdb.dbz_queue_size = 2048
synchdb.jvm_max_heap_size = 512
synchdb.dbz_snapshot_thread_num = 1
synchdb.dbz_snapshot_fetch_size = 0
synchdb.dbz_min_row_to_stream_results = 0
```

### **开发/测试环境**
```conf
synchdb.naptime = 500           # 默认轮询间隔
synchdb.dml_use_spi = true      # 启用高级功能进行测试
synchdb.dbz_batch_size = 2048
synchdb.dbz_queue_size = 4096
synchdb.jvm_max_heap_size = 1024
synchdb.dbz_snapshot_thread_num = 2
synchdb.dbz_snapshot_fetch_size = 0
synchdb.dbz_min_row_to_stream_results = 0
```

## **故障排除**

1. **CPU 使用率高**
    - 增加 `synchdb.naptime` 值
    - 检查 DML 操作模式
    - 减少 `synchdb.dbz_batch_size` 和 `synchdb.dbz_queue_size`
    - 增加 `synchdb.dbz_snapshot_thread_num`

2. **数据延迟问题**
    - 减少 `synchdb.naptime` 值
    - 增加 `synchdb.dbz_batch_size` 和 `synchdb.dbz_queue_size`
    - 检查网络连接
    - 增加 `shared_buffers`
    - 将工作负载拆分到多个连接器，而不仅仅是一个
    - 使用 `no_data` 模式启动连接器以仅获取模式并开始 CDC，而不是 `initial` 模式（在 CDC 开始之前捕获模式和初始数据）。

3. **启动问题**
    - 验证 `shared_preload_library` 配置
    - 检查 `synchdb_get_state()` 的错误消息
    - 检查连接器工作状态

4. **内存不足问题**
    - 增加 `synchdb.jvm_max_heap_size`
    - 增加 `shared_buffers`

## **最佳实践**

1. **初始设置**
    - 从默认值开始
    - 监控系统性能
    - 根据需求逐步调整

2. **生产环境**
    - 记录所有配置更改
    - 在测试环境中先测试更改
    - 保持正常工作配置的备份

3. **监控**
    - 跟踪系统资源使用情况
    - 监控数据同步延迟
    - 记录配置更改

## **补充说明**

1. **性能优化**
    - 在高负载情况下适当增加 `naptime`
    - 需要实时性时可以降低 `naptime`，但要注意系统负载
    - 建议根据实际业务场景进行压力测试

2. **安全考虑**
    - 定期检查配置文件权限
    - 记录配置更改日志
    - 保持配置文件的备份

3. **维护建议**
    - 定期检查系统日志
    - 监控连接器状态
    - 制定配置变更流程