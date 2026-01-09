# 创建连接器

## **创建连接器**

连接器代表与特定源数据库的连接，复制一组表并应用于 PostgreSQL。如果您有多个需要复制的源数据库，则需要多个连接器（每个连接器一个）。也可以创建多个连接到同一源数据库但复制不同或相同表集的连接器。

可以使用实用 SQL 函数 `synchdb_add_conninfo()` 创建连接器。

synchdb_add_conninfo 接受以下参数：

| argumet | description |
|-------------------- |-|
| name | 表示此连接器信息的唯一标识符 |
| hostname | 异构数据库的 IP 地址或主机名。|
| port | 连接到异构数据库的端口号。|
| username | 用于与异构数据库进行身份验证的用户名。|
| password | 用于验证用户名的密码 |
| source database | 这是我们要从中复制更改的异构数据库中的源数据库的名称。|
| source schema | 這是來源資料庫中來源模式的名稱，我們要從中複製變更。 |
| table |（可选）- 以 `[database].[table]` 或 `[database].[schema].[table]` 的形式表示，该参数必须存在于异构数据库中，因此引擎将仅复制指定的表。如果留空，则复制所有表。或者，可以使用 `file:` 前缀指定表列表文件 |
| snapshot table |（可选）- 以 `[database].[table]` 或 `[database].[schema].[table]` 的形式表示，该参数必须存在于上述 `table` 设置中，因此引擎仅在快照模式设置为 `always` 时才会重建这些表的快照。如果留空或为 null，则当快照模式设置为 `always` 时，将重建上述 `table` 设置中指定的所有表。或者，可以使用 `file:` 前缀指定快照表列表文件 |
| connector | 要使用的连接器类型（如下）。|

<<**注意**>> `來源資料庫`、`來源模式`、`使用者名稱`、`密碼`、`表`和`快照表`區分大小寫，您必須按照來源資料庫中的名稱準確指定它們，因此請記住這些名稱的字母大小寫。

## **连接器类型**

SynchDb 支持以下连接器类型：

* mysql             -> MySQL 数据库
* sqlserver         -> Microsoft SQL Server 数据库
* oracle            -> Oracle 数据库
* olr               -> 原生 Openlog Replicator
* postgres          -> PostgreSQL 数据库

## **检查已创建的连接器**

已创建的连接器显示在 `synchdb_conninfo` 表中。我们可以查看其内容并根据需要进行修改。请注意，用户凭证的密码由 pgcrypto 使用只有 synchdb 知道的密钥加密。因此，请勿直接修改密码字段，因为如果被篡改，可能会被错误解密。以下是示例输出：

```sql
postgres=# \x
Expanded display is on.

postgres=# select * from synchdb_conninfo;
-[ RECORD 1 ]-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name     | sqlserverconn
isactive | t
data     | {"pwd": "\\xc30d0407030245ca4a983b6304c079d23a0191c6dabc1683e4f66fc538db65b9ab2788257762438961f8201e6bcefafa60460fbf441e55d844e7f27b31745f04e7251c0123a159540676c4", "port": 1433, "user": "sa", "srcschema": "dbo", "srcdb": "testDB", "table": null, "snapshottable": null, "hostname": "192.168.1.86", "connector": "sqlserver"}
-[ RECORD 2 ]-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name     | mysqlconn
isactive | t
data     | {"pwd": "\\xc30d04070302986aff858065e96b62d23901b418a1f0bfdf874ea9143ec096cd648a1588090ee840de58fb6ba5a04c6430d8fe7f7d466b70a930597d48b8d31e736e77032cb34c86354e", "port": 3306, "user": "mysqluser", "srcschema": null, "srcdb": "inventory", "table": null, "snapshottable": null, "hostname": "192.168.1.86", "connector": "mysql"}
-[ RECORD 3 ]-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name     | oracleconn
isactive | t
data     | {"pwd": "\\xc30d04070302e3baf1293d0d553066d234014f6fc52e6eea425884b1f65f1955bf504b85062dfe538ca2e22bfd6db9916662406fc45a3a530b7bf43ce4cfaa2b049a1c9af8", "port": 1528, "user": "DBZUSER", "srcschema": "DBZUSER", "srcdb": "FREE", "table": null, "snapshottable": null, "hostname": "192.168.1.86", "connector": "oracle"}


```

<<<**重要**>>> 本机 Openlog Replicator 连接器当前不支持通过“table”和“snapshot table”参数指定白名单表，因此以下部分不适用于本机 Openlog Replicator 连接器。

## **使用表列表文件指定表**

如果要复制大量表，可以使用表列表文件来指定表。该列表必须采用 JSON 格式，如下所示：

```
{
    "table_list":
    [
        "myschema.mytable1",
        "myschema.mytable2",
        ...
        ...
    ],
    "snapshot_table_list":
    [
        "myschema.mytable1",
        "myschema.mytable2",
        ...
        ...
    ]
}
```

SynchDB 通过名称查找以下关键 JSON 数组：
* `table_list` 是一个 JSON 数组，包含以字符串形式表示的待复制表。当 `table` 参数以前缀 `file:` 开头，后跟文件路径时，此参数为必填项。
* `snapshot_table_list` 也是一个 JSON 数组，包含要执行快照的表。当 `snapshot table` 参数以前缀 `file:` 开头，后跟文件路径时，此参数为必填项。

文件路径可以是相对于 PostgreSQL 数据目录的相对路径，也可以是绝对路径。

## **何时指定快照表列表？**

通常，我们可以将 `snapshot table list` 参数留空或保留为 `null`，后者默认与 `table` 参数的值相同。这意味着 SynchDB 将在需要时对 `table` 参数中指定的所有表执行初始快照（复制架构并复制初始数据）。在某些情况下，我们可能只希望对“表”的子集执行初始快照。如果是这种情况，我们可以设置不同的“快照表列表”，指示 SynchDB 仅重建指定的表快照。

## **示例：为每个支持的源数据库创建一个连接器以复制所有表**

1. 建立一個名為 `mysqlconn` 的 MySQL 連接器，用於複製 MySQL 中 `inventory` 下的所有表。來源模式可以設定為 'null'，因為 MySQL 不支援空值。
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', '127.0.0.1', 3306, 'mysqluser', 
    'mysqlpwd', 'inventory', 'null', 
    'null', 'null', 'mysql');
```

2. 建立一個名為 `sqlserver conn` 的 SQL Server 連接器，以複製 `testDB` 資料庫和 `dbo` 架構下的所有資料表。
```sql
SELECT 
  synchdb_add_conninfo(
    'sqlserverconn', '127.0.0.1', 1433, 
    'sa', 'Password!', 'testDB', 'dbo', 
    'null', 'null', 'sqlserver');
```

3. 建立一個名為 `oracleconn` 的 Oracle 連接器，用於複製 `FREE` 資料庫和 `DBZUSER` 模式下的所有表：
```sql
SELECT 
  synchdb_add_conninfo(
    'oracleconn', '127.0.0.1', 1521, 
    'DBZUSER', 'dbz', 'FREE', 'DBZUSER', 
    'null', 'null', 'oracle');
```

## **示例：创建连接器以复制指定的表**

请注意，表必须使用完全限定名称（例如“[database].[table]”或“[database].[schema].[table]”）指定，并且必须存在于源数据库中。

创建一个名为“mysqlconn”的 MySQL 连接器，将 MySQL 中“inventory”下的“orders”和“customers”表复制到 PostgreSQL 中的目标数据库“postgres”：
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', '127.0.0.1', 3306, 'mysqluser', 
    'mysqlpwd', 'inventory', 'null', 
    'inventory.orders,inventory.customers', 'null', 'mysql');

```

## **示例：创建连接器以使用文件复制指定的表**

创建一个名为“mysqlconn”的 MySQL 连接器，将 MySQL 中“inventory”下表文件中指定的表复制到 PostgreSQL 中的目标数据库“postgres”：
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', '127.0.0.1', 3306, 'mysqluser', 
    'mysqlpwd', 'inventory', 'null', 
    'file:/path/to/mytablefile.json', 'file:/path/to/mytablefile.json', 'mysql');

```

其中 `/path/to/mytablefile.json` 可以是：
```json
{
    "table_list":
    [
        "inventory.orders",
        "inventory.customers"
    ],
    "snapshot_table_list":
    [
        "inventory.orders",
        "inventory.customers"
    ]
}
```