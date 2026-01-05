# Oracle -> PostgreSQL

## 为 SynchDB 准备 Oracle 数据库

在使用 SynchDB 从 Oracle 复制之前，需要按照[此处](../../getting-started/remote_database_setups/) 概述的步骤配置 Oracle。

请确保 SynchDB 需要复制的每个表的所有列都启用了补充日志数据。这是 SynchDB 正确处理更新和删除操作所必需的。

例如，以下命令为“customer”和“products”表的所有列启用了补充日志数据。请根据需要添加更多表。

```sql
ALTER TABLE customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
... etc
```

## 创建 Oracle 连接器

创建一个连接器，指向 Oracle 中 `FREE` 数据库和 `DBZUSER` schema 下的所有表。
```sql
SELECT
synchdb_add_conninfo(
'oracleconn','127.0.0.1',1521,
'DBZUSER','dbz','FREE','DBZUSER',
'null','null','oracle');
```

## 初始快照 + CDC

使用 `initial` 模式启动连接器将对所有指定表（在本例中为全部）执行初始快照。完成后，变更数据捕获 (CDC) 进程将开始流式传输新的变更。

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'initial');

或

SELECT synchdb_start_engine_bgw('oracleconn');
```

此连接器首次运行时，其阶段应处于 `initial snapper` 状态：
```sql
postgres=# select * from synchdb_state_view where name='oracleconn';
    name    | connector_type |  pid   |      stage       |  state  |   err    |       last_dbz_offset
------------+----------------+--------+------------------+---------+----------+-----------------------------
 oracleconn | oracle         | 528146 | initial snapshot | polling | no error | offset file not flushed yet
(1 row)

```

将创建一个名为“inventory”的新模式，并且连接器流式传输的所有表都将在该模式下复制。
```sql
postgres=# set search_path=free;
SET
postgres=# \d
              List of relations
 Schema |        Name        | Type  | Owner
--------+--------------------+-------+--------
 free   | orders             | table | ubuntu

```
初始快照完成后，如果至少接收并处理了一个后续更改，则连接器阶段应从“初始快照”更改为“更改数据捕获”。
```sql
postgres=# select * from synchdb_state_view where name='oracleconn';
    name    | connector_type |  pid   |        stage        |  state  |   err    |
    last_dbz_offset
------------+----------------+--------+---------------------+---------+----------+-------------------------------
-------------------------------------------------------
 oracleconn | oracle         | 528414 | change data capture | polling | no error | {"commit_scn":"3118146:1:02001
f00c0020000","snapshot_scn":"3081987","scn":"3118125"}

```
这意味着连接器现在正在流式传输指定表的新更改。以“initial”模式重启连接器将从上次成功点开始继续复制，并且不会重新运行初始快照。

## 仅初始快照，无CDC

使用“initial_only”模式启动连接器将仅对所有指定表（在本例中为所有表）执行初始快照，之后将不再执行CDC。

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'initial_only');

```

连接器仍然会显示正在“轮询”，但由于Debzium内部已停止CDC，因此不会捕获任何更改。您可以选择关闭它。以“initial_only”模式重启连接器不会重建表，因为它们已经构建好了。

## 仅捕获表模式 + CDC

使用 `no_data` 模式启动连接器将仅执行模式捕获，在 PostgreSQL 中构建相应的表，并且不会复制现有表数据（跳过初始快照）。模式捕获完成后，连接器将进入 CDC 模式，并开始捕获对表的后续更改。

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'no_data');

```

在 `no_data` 模式下重新启动连接器将不会再次重建模式，并且它将从上次成功点恢复 CDC。

## 仅 CDC

使用 `never` 模式启动连接器将完全跳过模式捕获和初始快照，并进入 CDC 模式以捕获后续更改。请注意，连接器要求在以 `never` 模式启动之前，所有捕获表都已在 PostgreSQL 中创建。如果表不存在，连接器在尝试将 CDC 更改应用于不存在的表时将遇到错误。

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'never');

```

以 `never` 模式重启连接器将从上次成功点开始恢复 CDC。

## 始终执行初始快照 + CDC

使用 `always` 模式启动连接器将始终捕获捕获表的模式，始终重做初始快照，然后转到 CDC。这类似于重置按钮，因为使用此模式将重建所有内容。请谨慎使用此模式，尤其是在捕获大量表时，这可能需要很长时间才能完成。重建后，CDC 将恢复正常。

```sql
SELECT synchdb_start_engine_bgw('oracleconn', 'always');

```

但是，可以使用连接器的 `snapshottable` 选项选择部分表来重做初始快照。符合 `snapshottable` 中条件的表将重做初始快照，否则将跳过其初始快照。如果 `snapshottable` 为 null 或为空，默认情况下，连接器的 `table` 选项中指定的所有表将在 `always` 模式下重做初始快照。

此示例使连接器仅重做 `inventory.customers` 表的初始快照。所有其他表的快照将被跳过。
```sql
UPDATE synchdb_conninfo
SET data = jsonb_set(data, '{snapshottable}', '"free.customers"')
WHERE name = 'oracleconn';
```

初始快照完成后，持续数据捕获 (CDC) 将开始。在 `always` 模式下重新启动连接器将重复上述过程。