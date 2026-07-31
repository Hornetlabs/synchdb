# JVM Memory Usage

## **Dump JVM Memory Usage**

We can use an utility function `synchdb_log_jvm_meminfo` to dump current JVM's current heap and non-heap memory usage in the logfile. This is purely informational and is intended to give user an idea the memory usage of different workload, which may be important to configure a suitable value for max heap memory to be allocated to JVM of a connector.
```sql
SELECT synchdb_log_jvm_meminfo('mysqlconn');
```

Check the PostgreSQL log file:
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