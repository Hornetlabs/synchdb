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

## **详细命令支持**

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