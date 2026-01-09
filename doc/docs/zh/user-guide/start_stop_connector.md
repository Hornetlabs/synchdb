# 启动/停止连接器

## **控制连接器**

SynchDB 提供了几个实用函数来控制已创建连接器的行为和生命周期。

## **以默认快照模式启动连接器**

synchdb_start_engine_bgw() 可用于以名为“initial”的默认快照模式启动连接器。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn');
```

## **以自定义快照模式启动连接器**

使用相同的函数 synchdb_start_engine_bgw()，可以包含快照模式来启动连接器，否则将默认使用“initial”模式。

```sql
-- 捕获表结构并继续流式传输新的更改
SELECT synchdb_start_engine_bgw('mysqlconn', 'no_data');

-- 始终重建表结构和现有数据，并继续流式传输新的更改
SELECT synchdb_start_engine_bgw('mysqlconn', 'always');
```

## **支持的快照模式**

| 模式 | 描述 | 用例 |
|:-:|-|-|
| `always` | 每次启动时都进行完整快照 | 完成数据验证 |
| `initial` | 仅限首次快照 | 正常操作 |
| `initial_only` | 一次性快照，然后停止 | 数据迁移 |
| `no_data` | 仅结构，无数据 | 结构同步 |
| `never` | 跳过快照，仅流式传输 | 实时更新 |
| `schemasync` | 仅结构，无数据，无 CDC | 正常操作 |

**请参阅[教程](../../tutorial/selective_table_sync/)，了解何时使用何种模式**

## **暂停和恢复连接器**

使用 `synchdb_pause_engine` 暂停连接器，这会暂时停止正在运行的连接器。
```sql
SELECT synchdb_pause_engine('mysqlconn');
```

使用 `synchdb_resume_engine` 恢复已暂停的连接器。
```sql
SELECT synchdb_resume_engine('mysqlconn');
```

## **停止或重启正在运行的连接器**

使用 `synchdb_stop_engine_bgw` 停止正在运行的连接器。
```sql
SELECT synchdb_stop_engine_bgw('mysqlconn');
```

使用 `synchdb_restart_connector` 以不同的快照模式重启正在运行的连接器。
```sql
-- 使用特定快照模式重启
SELECT synchdb_restart_connector('mysqlconn', 'initial');

-- 使用特定快照模式启动
SELECT synchdb_start_engine_bgw('mysqlconn', 'always');
```