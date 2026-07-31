---
weight:  20
---
# 快速测试环境

此处提到的步骤旨在快速启动外部数据库，用于 Synchdb 的验证和功能演示。启动示例异构数据库所需的文件和脚本可以在 SynchDB 代码库中找到（此处）(https://github.com/Hornetlabs/synchdb/testenv/)。

## **准备一个样本 MySQL 数据库**

我们可以使用 docker compose 启动一个样本 MySQL 数据库进行测试。用户凭证在 `synchdb-mysql-test.yaml` 文件中描述。

```
docker compose -f synchdb-mysql-test.yaml up -d
```

以 `root` 身份登录 MySQL，并授予 `mysqluser` 用户执行实时 CDC 的权限
```sql
mysql -h 127.0.0.1 -u root -p

GRANT replication client on *.* to mysqluser;
GRANT replication slave  on *.* to mysqluser;
GRANT RELOAD ON *.* TO 'mysqluser'@'%';
FLUSH PRIVILEGES;
```

退出 mysql 客户端工具：
```
\q
```

## **准备一个样本 SQL Server 数据库**

我们可以使用 docker compose 启动一个样本 SQL Server 数据库进行测试。用户凭证在 `synchdb-sqlserver-test.yaml` 文件中描述。
```
docker compose -f synchdb-sqlserver-test.yaml up -d
```
如果需要启用 SSL 证书的 SQL Server，请使用 synchdb-sqlserver-withssl-test.yaml 文件。

你可能没有安装 SQL Server 客户端工具，你可以登录到 SQL Server 容器来访问其客户端工具。

找出 SQL Server 的容器 ID：
```
id=$(docker ps | grep sqlserver | awk '{print $1}')
```

将数据库架构复制到 SQL Server 容器中：
```
docker cp inventory.sql $id:/
```

登录到 SQL Server 容器：
```
docker exec -it $id bash
```

根据架构构建数据库：
```
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -i /inventory.sql
```

运行一些简单的查询（如果你使用的是启用 SSL 的 SQL Server，请添加 -N -C）：
```
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -Q "insert into orders(order_date, purchaser, quantity, product_id) values( '2024-01-01', 1003, 2, 107)"

/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -Q "select * from orders"
```

## **准备一个示例 Oracle 数据库**

我们可以使用 Oracle 提供的免费 Oracle 数据库 docker 镜像来测试和评估 SynchDB。它附带一个免费的容器数据库 `FREE` 和一个可插入数据库 `FREEPDB1`
```
docker run -d -p 1521:1521 container-registry.oracle.com/database/free:latest
```

找出容器 ID 并登录：
```
id=$(docker ps | grep oracle | awk '{print $1}')
docker exec -it $id bash
```

按照[此处](../../user-guide/remote_database_setups/) 中描述的步骤设置 logminer 和 logminer 用户

## **使用 CI 脚本准备测试数据库**

示例 MySQL、SQL Server 和 Oracle 数据库也可以使用位于源存储库下 `ci/` 文件夹中的脚本进行准备。该脚本需要 docker 和 docker-compose 才能运行，并且基本上遵循与上述相同的步骤。

准备 MySQL 数据库进行测试：
```
DBTYPE=mysql ci/setup-remotebs.sh

```

准备 SQL Server 数据库进行测试：
```
DBTYPE=sqlserver ci/setup-remotebs.sh

```

准备 Oracle 数据库进行测试：
```
DBTYPE=oracle ci/setup-remotebs.sh

```

准备 Oracle19c 数据库进行测试：
```
DBTYPE=ora19c ci/setup-remotebs.sh

```

使用 Openlog Replicator 准备 Oracle19c 数据库进行测试：
```
DBTYPE=olr OLRVER=1.3.0 ci/setup-remotebs.sh

```