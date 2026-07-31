---
weight: 70
---
# 自动启动器

## **启用 SynchDB 自动启动器**
当对特定 `connector name` 执行 `synchdb_start_engine_bgw()` 时，连接工作进程将具备自动启动资格。同样，当执行 `synchdb_stop_engine_bgw()` 时，将取消此资格。

启用自动连接器启动器需要：

* 在 postgresql.conf 中将 `synchdb` 添加到 `shared_preload_libraries` GUC 选项
* 在 postgresql.conf 中将新的 GUC 选项 `synchdb.synchdb_auto_launcher` 设置为 true
* 重启 PostgreSQL 服务器使更改生效

示例：
```
shared_preload_libraries = 'synchdb'
synchdb.synchdb_auto_launcher = true
```

启动时，SynchDB 扩展会在早期被预加载。当 `synchdb.synchdb_auto_launcher` 设置为 true 时，SynchDB 会启动一个 `synchdb_auto_launcher` 后台工作进程（background worker），该进程会进一步为每一个可连接的非模板数据库派生一个 `synchdb_db_launcher` 工作进程。每个 `synchdb_db_launcher` 会从 `synchdb_conninfo` 表（如果存在）中检索所有标记为 `active`（即 `isactive` 标志设置为 `true`）的连接信息（conninfo），然后以与调用 `synchdb_start_engine_bgw()` 相同的方式，将它们分别作为独立的后台工作进程自动启动。待所有 `synchdb_db_launcher` 完成各自的工作后，`synchdb_auto_launcher` 工作进程才会退出。
