# 属性视图

## **查看连接器管理的属性**

连接器完成初始表快照后，源表、列和数据类型将根据[对象映射规则](../../user-guide/object_mapping_rules/)在 PostgreSQL 端进行转换和创建。SynchDB 提供了一个视图，可并排显示连接器的数据类型、名称映射以及源表和目标表之间的转换规则关系。

此视图仅供参考，旨在向用户显示连接器当前正在跟踪的表的列表以及每个表/列使用的映射/转换规则。

```sql
SELECT * FROM synchdb_att_view();
```

**返回字段**：

| 字段 | 描述 | 类型 |
|-|-|-|
| `name` | 连接器标识符 | 文本 |
| `attnum` | 属性编号 | 整数 |
| `ext_tbname` | 远程显示的表名 | 文本 |
| `pg_tbname` | PostgreSQL 中的映射表名 | 文本 |
| `ext_attname` | 远程显示的列名 | 文本 |
| `pg_attname` | PostgreSQL 中映射的列名 | 文本 |
| `ext_atttypename` | 远程显示的数据类型 | 文本 |
| `pg_atttypename` | PostgreSQL 中映射的数据类型 | 文本 |
| `transform` | 转换表达式 | 文本 |

**示例输出**

```sql
SELECT * from synchdb_att_view;
   name    | type  | attnum |         ext_tbname         |         pg_tbname          | ext_attname | pg_attname  | ext_atttypename | pg_atttypename |         transform
-----------+-------+--------+----------------------------+----------------------------+-------------+-------------+-----------------+----------------+----------------------------
 mysqlconn | mysql |      1 | inventory.addresses        | inventory.addresses        | id          | id          | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.addresses        | inventory.addresses        | customer_id | customer_id | INT             | int4           |
 mysqlconn | mysql |      3 | inventory.addresses        | inventory.addresses        | street      | street      | VARCHAR         | varchar        |
 mysqlconn | mysql |      4 | inventory.addresses        | inventory.addresses        | city        | city        | VARCHAR         | varchar        |
 mysqlconn | mysql |      5 | inventory.addresses        | inventory.addresses        | state       | state       | VARCHAR         | varchar        |
 mysqlconn | mysql |      6 | inventory.addresses        | inventory.addresses        | zip         | zip         | VARCHAR         | varchar        |
 mysqlconn | mysql |      7 | inventory.addresses        | inventory.addresses        | type        | type        | ENUM            | text           |
 mysqlconn | mysql |      1 | inventory.customers        | schema1.people             | id          | id          | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.customers        | schema1.people             | first_name  | first_name  | VARCHAR         | varchar        |
 mysqlconn | mysql |      3 | inventory.customers        | schema1.people             | last_name   | family_name | VARCHAR         | varchar        |
 mysqlconn | mysql |      4 | inventory.customers        | schema1.people             | email       | contact     | VARCHAR         | varchar        |
 mysqlconn | mysql |      1 | inventory.geom             | inventory.geom             | id          | id          | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.geom             | inventory.geom             | g           | g           | GEOMETRY        | geometry       |
 mysqlconn | mysql |      3 | inventory.geom             | inventory.geom             | h           | h           | GEOMETRY        | text           |
 mysqlconn | mysql |      1 | inventory.products         | public.stuff               | id          | id          | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.products         | public.stuff               | name        | name        | VARCHAR         | varchar        | '>>>>>' || '%d' || '<<<<<'
 mysqlconn | mysql |      3 | inventory.products         | public.stuff               | description | description | VARCHAR         | varchar        |
 mysqlconn | mysql |      4 | inventory.products         | public.stuff               | weight      | weight      | FLOAT           | float4         |
 mysqlconn | mysql |      1 | inventory.products_on_hand | inventory.products_on_hand | product_id  | product_id  | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.products_on_hand | inventory.products_on_hand | quantity    | quantity    | INT             | int8           |
```