# 对象映射工作流程

SynchDB 具有默认名称和数据类型映射规则来处理传入的更改事件。在大多数情况下，默认规则都可以正常工作。但是，如果您有特定的转换要求，或者默认规则不适合您，则可以将自己的对象映射规则配置到特定连接器。请按照以下工作流程查看和调整任何特定的对象映射规则。

## **创建连接器并以 `schemasync` 模式启动它**

`schemasync` 是一种特殊模式，它使连接器连接到远程数据库并尝试仅同步指定表的架构。完成此操作后，连接器将处于 `暂停` 状态，用户可以查看使用默认规则创建的所有表和数据类型，并在需要时进行更改。

```sql
SELECT synchdb_add_conninfo(
    'mysqlconn',
    '127.0.0.1',
    3306,
    'mysqluser',
    'mysqlpwd',
    'inventory',
    'null',
    'null',
    'mysql');

SELECT synchdb_start_engine_bgw('mysqlconn', 'schemasync');
```

## **确保连接器处于暂停状态**

```sql
SELECT name, connector_type, pid, stage, state FROM synchdb_state_view;
     name      | connector_type |  pid   |    stage    |  state
---------------+----------------+--------+-------------+---------
 mysqlconn     | mysql          | 579845 | schema sync | polling

```

## **查看默认创建的表的映射规则**

```sql
postgres=# select * from synchdb_att_view;
   name    | type  | attnum |         ext_tbname         |         pg_tbname          | ext_attname | pg_attname  | ext_atttypename | pg_atttypename | transform
-----------+-------+--------+----------------------------+----------------------------+-------------+-------------+-----------------+----------------+-----------
 mysqlconn | mysql |      1 | inventory.addresses        | inventory.addresses        | id          | id          | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.addresses        | inventory.addresses        | customer_id | customer_id | INT             | int4           |
 mysqlconn | mysql |      3 | inventory.addresses        | inventory.addresses        | street      | street      | VARCHAR         | varchar        |
 mysqlconn | mysql |      4 | inventory.addresses        | inventory.addresses        | city        | city        | VARCHAR         | varchar        |
 mysqlconn | mysql |      5 | inventory.addresses        | inventory.addresses        | state       | state       | VARCHAR         | varchar        |
 mysqlconn | mysql |      6 | inventory.addresses        | inventory.addresses        | zip         | zip         | VARCHAR         | varchar        |
 mysqlconn | mysql |      7 | inventory.addresses        | inventory.addresses        | type        | type        | ENUM            | text           |
 mysqlconn | mysql |      1 | inventory.customers        | inventory.customers        | id          | id          | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.customers        | inventory.customers        | first_name  | first_name  | VARCHAR         | varchar        |
 mysqlconn | mysql |      3 | inventory.customers        | inventory.customers        | last_name   | last_name   | VARCHAR         | varchar        |
 mysqlconn | mysql |      4 | inventory.customers        | inventory.customers        | email       | email       | VARCHAR         | varchar        |
 mysqlconn | mysql |      1 | inventory.geom             | inventory.geom             | id          | id          | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.geom             | inventory.geom             | g           | g           | GEOMETRY        | text           |
 mysqlconn | mysql |      3 | inventory.geom             | inventory.geom             | h           | h           | GEOMETRY        | text           |
 mysqlconn | mysql |      1 | inventory.products         | inventory.products         | id          | id          | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.products         | inventory.products         | name        | name        | VARCHAR         | varchar        |
 mysqlconn | mysql |      3 | inventory.products         | inventory.products         | description | description | VARCHAR         | varchar        |
 mysqlconn | mysql |      4 | inventory.products         | inventory.products         | weight      | weight      | FLOAT           | float4         |
 mysqlconn | mysql |      1 | inventory.products_on_hand | inventory.products_on_hand | product_id  | product_id  | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.products_on_hand | inventory.products_on_hand | quantity    | quantity    | INT             | int4           |
(20 rows)

```

## **定义自定义映射规则**

用户可以使用`synchdb_add_objmap`函数创建自定义映射规则。它可用于映射表名、列名、数据类型并定义数据转换表达式规则

```sql
SELECT synchdb_add_objmap('mysqlconn','table','inventory.products','stuff');
SELECT synchdb_add_objmap('mysqlconn','table','inventory.customers','schema1.people');
SELECT synchdb_add_objmap('mysqlconn','column','inventory.customers.last_name','family_name');
SELECT synchdb_add_objmap('mysqlconn','column','inventory.customers.email','contact');
SELECT synchdb_add_objmap('mysqlconn','datatype','inventory.geom.g','geometry|0');
SELECT synchdb_add_objmap('mysqlconn','datatype','inventory.orders.quantity','bigint|0');
SELECT synchdb_add_objmap('mysqlconn','transform','inventory.products.name','''>>>>>'' || ''%d'' || ''<<<<<''');
```

## **审查迄今为止创建的所有对象映射规则**

```sql
postgres=# select * from synchdb_objmap;
   name    |  objtype  | enabled |            srcobj             |           dstobj
-----------+-----------+---------+-------------------------------+----------------------------
 mysqlconn | table     | t       | inventory.products            | stuff
 mysqlconn | column    | t       | inventory.customers.last_name | family_name
 mysqlconn | column    | t       | inventory.customers.email     | contact
 mysqlconn | table     | t       | inventory.customers           | schema1.people
 mysqlconn | transform | t       | inventory.products.name       | '>>>>>' || '%d' || '<<<<<'
 mysqlconn | datatype  | t       | inventory.geom.g              | geometry|0
 mysqlconn | datatype  | t       | inventory.orders.quantity     | bigint|0
(7 rows)

```

## **重新加载对象映射规则**

一旦定义了所有自定义规则，我们就需要向连接器发出信号来加载它们。这将导致连接器读取并应用对象映射规则。如果它发现当前 PostgreSQL 值与对象映射值之间存在差异，它将尝试更正映射。

```sql
SELECT synchdb_reload_objmap('mysqlconn');

```

## **再次检查 `synchdb_att_view` 是否有变化**

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
 mysqlconn | mysql |      2 | inventory.geom             | inventory.geom             | g           | g           | GEOMETRY        | geometry           |
 mysqlconn | mysql |      3 | inventory.geom             | inventory.geom             | h           | h           | GEOMETRY        | text           |
 mysqlconn | mysql |      1 | inventory.products         | public.stuff               | id          | id          | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.products         | public.stuff               | name        | name        | VARCHAR         | varchar        | '>>>>>' || '%d' || '<<<<<'
 mysqlconn | mysql |      3 | inventory.products         | public.stuff               | description | description | VARCHAR         | varchar        |
 mysqlconn | mysql |      4 | inventory.products         | public.stuff               | weight      | weight      | FLOAT           | float4         |
 mysqlconn | mysql |      1 | inventory.products_on_hand | inventory.products_on_hand | product_id  | product_id  | INT             | int4           |
 mysqlconn | mysql |      2 | inventory.products_on_hand | inventory.products_on_hand | quantity    | quantity    | INT             | int8           |
```

## **恢复连接器或重做整个快照**

一旦确认对象映射正确，我们就可以恢复连接器。请注意，恢复只会继续流式传输新的表更改。不会复制表的现有数据。
```sql
SELECT synchdb_resume_engine('mysqlconn');
```

要捕获表的现有数据，我们还可以使用新的对象映射规则重做整个快照：
```sql
SELECT synchdb_restart_connector('mysqlconn', 'always');
```