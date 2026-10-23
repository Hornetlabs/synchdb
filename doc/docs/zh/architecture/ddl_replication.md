---
weight: 80
---
# DDL 复制

## **概述**

SynchDB 为数据定义语言（DDL）操作提供全面支持，实现不同数据库系统之间的实时架构同步。

## **支持的 DDL 命令**

SynchDB 支持以下 DDL 操作：

✅ CREATE [table]  
✅ ALTER [table] ADD COLUMN  
✅ ALTER [table] DROP COLUMN  
✅ ALTER [table] ALTER COLUMN  
✅ DROP [table]  

对于基于 Openlog Replicator 的连接器，支持以下 Oracle DDL 语法：

✅ CREATE [table]
✅ DROP [table]
✅ ALTER [table] MODIFY
✅ ALTER [table] ADD COLUMN
✅ ALTER [table] DROP COLUMN
✅ ALTER [table] ADD CONSTRAINT
✅ ALTER [table] DROP CONSTRAINT

✅ TRUNCATE [table]（依赖源端捕获，默认跳过）

## **详细命令支持**


### TRUNCATE TABLE

默认跳过 TRUNCATE。启用时使用实际注册的 GUC（保留历史拼写），重新加载配置，再重启 Debezium 连接器，使源端捕获配置生效：

```sql
ALTER SYSTEM SET synchdb.dbz_skipped_oeprations = 'none';
SELECT pg_reload_conf();
SELECT synchdb_restart_connector('myconnector', 'no_data');
```

也可以设置空字符串；SynchDB 将其转换为 Debezium 的 `none`，因为 Debezium 3.5 不接受空的 `skipped.operations`。其他跳过操作列表继续有效。原生 OLR 在处理收到的 DDL 时检查此配置。

Debezium 的 `payload.op="t"` 根据 `source.connector`、`source.db`、`source.table` 和有模式连接器的 `source.schema` 定位源表，不需要消息键、`before`、`after`、列信息或主键。snapshot 支持布尔值和字符串；TRUNCATE 数据事件的 snapshot 和时间戳可省略。明确的 `tableChanges[].type="TRUNCATE"` / `"TRUNCATE TABLE"` 或单表 TRUNCATE DDL 文本也复用 DDL 转换和执行路径。多表 schema-change 记录和 SQL 批次会被拒绝，避免只执行其中一部分。

| 源连接器 | 实际格式与捕获限制 |
|---|---|
| MySQL / Debezium 3.5.2 | binlog 中的 TRUNCATE 转为 `op="t"`；数据库、表过滤和 skipped.operations 必须允许捕获。 |
| PostgreSQL | pgoutput 为每张受影响的表生成 `op="t"`；publication 必须包含源表并发布 truncate。普通 DDL 不会自动由逻辑解码输出。 |
| Oracle / Debezium 3.5.2 | 官方格式为包含 db/schema/table 的 `op="t"`；仓库依赖的 LogMiner 实现包含 TRUNCATE 分发。仍取决于适配器、redo/archive 日志保留及表过滤；旧版本和其他适配器需要分别确认。 |
| 原生 OpenLogReplicator | `payload[0].op="ddl"`，包含 sql、schema.owner、schema.table，以及顶层 db/scn/c_scn/c_idx。需要开启 DDL 输出和必要元数据；仓库示例使用 flags=32、db=3、schema=7、message=2。现有 owner/表过滤继续有效。 |
| SQL Server CDC | 不产生 TRUNCATE 事件，而且已启用 CDC 的表不能执行 TRUNCATE；SynchDB 无法补造源端没有输出的事件。 |

表名映射按原始源对象 ID 查找。无映射时沿用既有规则：源数据库映射为 PostgreSQL 模式，源模式参与映射键；大小写策略应用于未映射名称。显式目标映射保留指定大小写，仅指定表名时固定使用 public。包含点、空格、双引号的映射组件应使用 SQL 引号，例如 `"target.schema"."table"" name"`。

目标执行使用 PostgreSQL 标识符引用函数生成完整限定的 `TRUNCATE TABLE schema.table`，不会添加 CASCADE 或 RESTART IDENTITY。表定义、索引、关系 OID、序列和 synchdb_attribute 映射保持有效；空表及重复清空也成功。缺少目标表、权限不足或外键限制会导致执行失败；源端多表清空拆成逐表事件后，也可能因目标外键而失败。

成功后 truncates 和 DDL 计数各增加 1。配置过滤不增加成功计数或 bad_events。执行失败沿用现有 SPI 错误策略：事务批次回滚，不能确认；原生 OLR 恢复上一次已提交的 SCN，避免退出时写出失败批次的检查点。失败批次的成功统计不会发布。后续 DML 继续使用现有结构和字段映射；无主键 UPDATE/DELETE 需要默认原生 DML 路径（synchdb.dml_use_spi=false）。

Debezium 和原生 OLR 均在解析 JSON 前以 LOG 记录连接器名及原始事件；OLR 按消息长度打印。

格式依据：[Debezium MySQL](https://debezium.io/documentation/reference/3.5/connectors/mysql.html#mysql-truncate-events)、[PostgreSQL](https://debezium.io/documentation/reference/3.5/connectors/postgresql.html#postgresql-truncate-events)、[Oracle](https://debezium.io/documentation/reference/3.5/connectors/oracle.html#oracle-truncate-events)、[PostgreSQL publication](https://www.postgresql.org/docs/18/sql-createpublication.html) 和 [SQL Server TRUNCATE 限制](https://learn.microsoft.com/en-us/sql/t-sql/statements/truncate-table-transact-sql)。

### **CREATE TABLE**

SynchDB 在 CREATE TABLE 事件期间捕获以下属性：

| 属性 | 描述 |
|----------|-------------|
| 表名 | 完全限定名称（FQN）格式 |
| 列名 | 单独的列标识符 |
| 数据类型 | 列数据类型规范 |
| 数据长度 | 长度/精度规范（如适用） |
| 无符号标志 | 数值类型的无符号约束 |
| 可空性 | NULL/NOT NULL 约束 |
| 默认值 | 默认值表达式 |
| 主键 | 主键列定义 |

> **注意**：当前不支持其他 CREATE TABLE 属性

### **DROP TABLE**

捕获的属性：
- 要删除的表名（FQN 格式）

### **ALTER TABLE ADD COLUMN**

捕获以下属性：

| 属性 | 描述 |
|----------|-------------|
| 列名 | 新添加列的名称 |
| 数据类型 | 新列的数据类型 |
| 数据长度 | 长度规范（如适用） |
| 无符号标志 | 无符号约束 |
| 可空性 | NULL/NOT NULL 规范 |
| 默认值 | 默认值表达式 |
| 主键 | 更新的主键定义 |

目前不支持在 ALTER TABLE ADD COLUMN 期间可以指定的其他属性。

### **ALTER TABLE DROP COLUMN**

捕获：
- 要删除的列名列表

### **ALTER TABLE ALTER COLUMN**

支持的修改：

| 修改 | 描述 |
|--------------|-------------|
| 数据类型 | 更改列数据类型 |
| 类型长度 | 修改类型长度/精度 |
| 默认值 | 更改/删除默认值 |
| NOT NULL | 修改/删除 NOT NULL 约束 |

目前不支持在 ALTER TABLE ALTER COLUMN 期间可以指定的其他属性。

请注意，SynchDB 仅支持对现有列进行基本的数据类型更改。例如，`INT` → `BIGINT` 或 `VARCHAR` → `TEXT`。目前不支持复杂的数据类型更改，如 `TEXT` → `INT` 或 `INT` → `TIMESTAMP`。这是因为 PostgreSQL 要求用户额外提供类型转换函数来执行复杂数据类型更改的类型转换。SynchDB 目前不知道对特定类型转换使用什么类型转换函数。将来，我们可能允许用户通过规则文件提供自己的转换函数用于特定类型转换，但目前尚不支持。

## **数据库特定行为**

### **MySQL DDL 更改事件**

由于 MySQL 在 binlog 和 Oracle 在 logminer 中同时记录 DDL 和 DML 操作，SynchDB 能够在它们发生时复制 DDL 和 DML。在 MySQL 和 Oracle 端不需要特殊操作来启用 DDL 复制。

### **SQLServer DDL 更改事件**

SQLServer 在流模式下不原生支持 DDL 复制。表架构是在连接器首次启动时的初始快照构建阶段由 SynchDB 构建的。在此阶段之后，SynchDB 将尝试检测任何架构更改，但需要明确将其添加到 SQL Server 的 CDC 表列表中。

#### **在 SQLServer 上触发 CREATE TABLE 事件**

要在 SQL Server 上创建新表并添加到其 CDC 表列表中：
```sql
CREATE TABLE dbo.altertest (
	a INT,
	b TEXT
	);
GO

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
	@source_name = 'altertest',
	@role_name = NULL,
	@supports_net_changes = 0,
	@capture_instance = 'dbo_altertest_1'
GO
```

该命令将表 `dbo.altertest` 添加到 CDC 表列表中，并将导致 SynchDB 接收 CREATE TABLE DDL 更改事件。

#### **触发 ALTER TABLE 事件**

如果更改了现有表（添加、删除或更改列），需要明确更新到 SQLServer 的 CDC 表列表中，以便 SynchDB 能够接收 ALTER TABLE 事件。

例如：

在 SQLServer 中更改表：
```sql
ALTER TABLE altertest ADD c NVARCHAR(MAX),
	d INT DEFAULT 0 NOT NULL,
	e NVARCHAR(255) NOT NULL,
	f INT DEFAULT 5 NOT NULL,
	CONSTRAINT PK_altertest PRIMARY KEY (
	d,
	f
	);
GO
```

禁用旧的捕获实例：
```sql
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo',
	@source_name = 'altertest',
	@capture_instance = 'dbo_altertest_1';
GO
```

启用为新的捕获实例：
```sql
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
	@source_name = 'altertest',
	@role_name = NULL,
	@supports_net_changes = 0,
	@capture_instance = 'dbo_altertest_2';
GO
```

添加新记录：
```sql
INSERT INTO altertest VALUES(1, 's', 'c', 1, 'v', 5);
GO
```

上述示例应允许 SynchDB 接收 ALTER TABLE ADD COLUMN 和 INSERT 事件。无需重启 SQL Server 或 SynchDB 即可捕获此类事件。这同样适用于 ALTER TABLE DROP COLUMN 和 ALTER TABLE ALTER COLUMN 事件。