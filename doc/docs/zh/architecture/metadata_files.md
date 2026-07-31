# 元数据文件

在操作过程中，Debezium 运行引擎在 $PGDATA/pg_synchdb 下生成元数据文件。目前生成并持久化了两种类型的元数据文件：
* 偏移量文件：包含在启动时恢复复制操作的偏移量
* 架构历史文件：包含为复制构建所有表的架构信息。这在初始数据快照同步期间创建，并可在操作期间更新。

这些元数据文件名由以下部分组成：

* (连接器类型)_(连接器名称)_(目标数据库)_offsets.dat

* (连接器类型)_(连接器名称)_(目标数据库)_schemahistory.dat

```
ls $PGDATA/pg_synchdb
mysql_mysqlconn_postgres_offsets.dat        sqlserver_sqlserverconn_postgres_offsets.dat
mysql_mysqlconn_postgres_schemahistory.dat  sqlserver_sqlserverconn_postgres_schemahistory.dat
```

这些二进制文件的内容可以使用 hexdump 命令查看：
```
hexdump -C $PGDATA/pg_synchdb/mysql_mysqlconn_postgres_offsets.dat
hexdump -C $PGDATA/pg_synchdb/mysql_mysqlconn_postgres_schemahistory.dat
```

## **重置连接器**

连接器可以重置（重新复制和重新同步）所有指定表，只需：

* 停止连接器
* 删除偏移量和架构历史文件
* 启动连接器