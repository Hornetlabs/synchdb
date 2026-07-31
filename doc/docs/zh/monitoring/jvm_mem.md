# JVM 内存使用情况

## **转储 JVM 内存使用情况**

我们可以使用实用函数 `synchdb_log_jvm_meminfo` 将当前 JVM 的堆内存和非堆内存使用情况转储到日志文件中。这纯粹是提供信息，旨在让用户了解不同工作负载的内存使用情况，这对于为连接器的 JVM 配置合适的最大堆内存值可能很重要。
```sql
SELECT synchdb_log_jvm_meminfo('mysqlconn');
```

检查 PostgreSQL 日志文件：
```
2024-12-09 14:34:21.910 PST [25491] LOG:  Requesting memdump for mysqlconn connector
2024-12-09 14:34:21 WARN  DebeziumRunner:297 - Heap Memory:
2024-12-09 14:34:21 WARN  DebeziumRunner:298 -   Used: 19272600 bytes
2024-12-09 14:34:21 WARN  DebeziumRunner:299 -   Committed: 67108864 bytes
2024-12-09 14:34:21 WARN  DebeziumRunner:300 -   Max: 2147483648 bytes
2024-12-09 14:34:21 WARN  DebeziumRunner:302 - Non-Heap Memory:
2024-12-09 14:34:21 WARN  DebeziumRunner:303 -   Used: 42198864 bytes
2024-12-09 14:34:21 WARN  DebeziumRunner:304 -   Committed: 45023232 bytes
2024-12-09 14:34:21 WARN  DebeziumRunner:305 -   Max: -1 bytes

```