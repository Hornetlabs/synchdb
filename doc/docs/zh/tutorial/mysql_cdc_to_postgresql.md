# MySQL -> PostgreSQL

## 为 SynchDB 准备 MySQL 数据库

在使用 SynchDB 从 MySQL 复制之前，需要按照[此处](../../getting-started/remote_database_setups/) 概述的步骤配置 MySQL

## 创建 MySQL 连接器

创建一个连接器，该连接器指向 MySQL 中 `inventory` 数据库下的所有表。
```sql
SELECT synchdb_add_conninfo(
'mysqlconn', '127.0.0.1', 3306, 'mysqluser',
'mysqlpwd', 'inventory', 'postgres',
'null', 'null', 'mysql');
```

## 初始快照 + CDC

使用 `initial` 模式启动连接器将对所有指定表（在本例中为所有表）执行初始快照。完成后，变更数据捕获 (CDC) 进程将开始流式传输新的变更。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'initial');

或

SELECT synchdb_start_engine_bgw('mysqlconn');
```

The stage of this connector should be in `initial snapshot` the first time it runs:
```sql
postgres=# select * from synchdb_state_view;
    name    | connector_type |  pid   |      stage       |  state  |   err    |                      last_dbz_offs
et
------------+----------------+--------+------------------+---------+----------+-----------------------------------
-------------------------
 mysqlconn  | mysql          | 522195 | initial snapshot | polling | no error | {"ts_sec":1750375008,"file":"mysql
-bin.000003","pos":1500}

```

A new schema called `inventory` will be created and all tables streamed by the connector will be replicated under that schema.
```sql
postgres=# set search_path=inventory;
SET
postgres=# \d
                    List of relations
  Schema   |          Name           |   Type   | Owner
-----------+-------------------------+----------+--------
 inventory | addresses               | table    | ubuntu
 inventory | addresses_id_seq        | sequence | ubuntu
 inventory | customers               | table    | ubuntu
 inventory | customers_id_seq        | sequence | ubuntu
 inventory | geom                    | table    | ubuntu
 inventory | geom_id_seq             | sequence | ubuntu
 inventory | orders                  | table    | ubuntu
 inventory | orders_order_number_seq | sequence | ubuntu
 inventory | products                | table    | ubuntu
 inventory | products_id_seq         | sequence | ubuntu
 inventory | products_on_hand        | table    | ubuntu

```

初始快照完成后，如果至少有一条后续更改被接收并处理，连接器阶段将从“初始快照”切换为“变更数据捕获”。
```sql
postgres=# select * from synchdb_state_view;
    name    | connector_type |  pid   |        stage        |  state  |   err    |                      last_dbz_o
ffset
------------+----------------+--------+---------------------+---------+----------+--------------------------------
----------------------------
 mysqlconn  | mysql          | 522195 | change data capture | polling | no error | {"ts_sec":1750375008,"file":"my
sql-bin.000003","pos":1500}

```

这意味着连接器现在正在流式传输指定表的新更改。以“初始”模式重新启动连接器将从上次成功点开始继续复制，并且不会重新运行初始快照。

## 仅初始快照，无 CDC

使用 `initial_only` 模式启动连接器将仅对所有指定表（在本例中为全部）执行初始快照，之后将不再执行 CDC。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'initial_only');

```

连接器似乎仍在“轮询”其他连接器，但不会捕获任何更改，因为 Debzium 内部已停止 CDC。您可以选择关闭它。在 `initial_only` 模式下重新启动连接器不会重建表，因为它们已经构建好了。

```sql
postgres=# select * from synchdb_state_view;
    name    | connector_type |  pid   |      stage       |  state  |   err    |       last_dbz_offset
------------+----------------+--------+------------------+---------+----------+-----------------------------
 mysqlconn  | mysql          | 522330 | initial snapshot | polling | no error | offset file not flushed yet

```

## 仅捕获表模式 + CDC

使用 `no_data` 模式启动连接器将仅执行模式捕获，在 PostgreSQL 中构建相应的表，并且不会复制现有表数据（跳过初始快照）。模式捕获完成后，连接器将进入 CDC 模式，并开始捕获对表的后续更改。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'no_data');

```

在 `no_data` 模式下重新启动连接器将不会再次重建模式，并且它将从上次成功点恢复 CDC。

## 仅 CDC

使用 `never` 模式启动连接器将完全跳过模式捕获和初始快照，并进入 CDC 模式以捕获后续更改。请注意，连接器要求在以 `never` 模式启动之前，所有捕获表都已在 PostgreSQL 中创建。如果表不存在，连接器在尝试将 CDC 更改应用于不存在的表时将遇到错误。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'never');

```

以“never”模式重启连接器将从上次成功点开始恢复 CDC。

## 始终执行初始快照 + CDC

使用 `always` 模式启动连接器将始终捕获捕获表的模式，始终重做初始快照，然后转到 CDC。这类似于重置按钮，因为使用此模式将重建所有内容。请谨慎使用，尤其是在捕获大量表时，这可能需要很长时间才能完成。重建后，CDC 将恢复正常。

```sql
SELECT synchdb_start_engine_bgw('mysqlconn', 'always');

```

但是，可以使用连接器的 `snapshottable` 选项选择部分表来重做初始快照。符合 `snapshottable` 中条件的表将重做初始快照，否则将跳过其初始快照。如果 `snapshottable` 为 null 或为空，默认情况下，连接器 `table` 选项中指定的所有表都将在 `always` 模式下重做初始快照。

此示例使连接器仅重做 `inventory.customers` 表的初始快照。所有其他表的快照将被跳过。
```sql
UPDATE synchdb_conninfo
SET data = jsonb_set(data, '{snapshottable}', '"inventory.customers"')
WHERE name = 'mysqlconn';
```

初始快照完成后，CDC 将开始。在 `always` 模式下重新启动连接器将重复上述过程。