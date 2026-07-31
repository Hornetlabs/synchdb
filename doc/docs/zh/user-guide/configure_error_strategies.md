# 配置错误处理策略

## **配置错误处理策略**

在初始快照或 CDC 期间，可能会发生诸如主键冲突、数据无效等错误，SynchDB 需要知道如何处理这些错误。对于 Synchdb 处理过程中或应用到 PostgreSQL 过程中发生的错误，可以通过配置参数“synchdb.error_handling_strategy”使用多种策略来处理它们。

请注意，以下提到的错误策略仅在错误源自或检测到 C 端变更事件处理过程中时才会执行。如果错误源自 Java 端的 Debezium Runner，例如连接问题或与远程数据库的连接问题，则上述策略将不会执行。在这种情况下，Debezium Runner 的错误消息将传播到 C 端的 SynchDB，并显示在 `synchdb_state_view()` 中，连接器将退出。

### **exit (默认)**

这是默认的错误策略，当发生错误时，连接器工作器会退出。当前正在处理的导致错误的批次将不会被标记为已完成，并且不会提交同一批次中已成功完成的更改事件。用户应检查“synchdb_state_view()”或日志文件中返回的错误消息以解决错误。当连接器工作器重新启动时，连接器将自动重试之前失败的同一批。

### **retry**

此策略为连接器工作器添加了 5 秒的“restart_time”，这会导致 PostgreSQL 的 bgworker 引擎在工作器退出时每 5 秒自动启动一次工作器。这意味着当发生错误时，连接器工作器仍将退出，但与上面的 `exit` 策略不同，它将由 bgworker 引擎自动重新启动，后者将在失败的同一批次上重试。它将继续退出并重新启动，直到错误得到解决。

### **skip**

顾名思义，当处于 `skip` 错误策略中时，连接器工作器遇到的任何错误都不会导致工作器退出，但是错误消息仍将写入日志或 `synchdb_state_view()`，但连接器本身将忽略错误并继续处理下一个更改事件甚至下一个批次。

## **查看连接器的最后一个错误消息**

如上所述，源自 Java 端的 Debezium Runner 或 C 端的 SynchDB 的错误将会传播并显示在 `synchdb_state_view()` 中。例如：

```sql
select name, pid, err from synchdb_state_view;

   name    | pid |                                                                                                          err
-----------+-----+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 mysqlconn |  -1 | Connector configuration is not valid. Unable to connect: Communications link failure  The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.
(1 row)

```

如果在处理 JSON 事件时发生错误，并且 GUC 参数 `synchdb.log_change_on_error` 设置为 true，SynchDB 还会在 PostgreSQL 日志文件中输出导致错误的 JSON 变化事件，以便进行故障排除。