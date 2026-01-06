# Object Mapping Workflow

## **Create a Connector and Start it in `schemasync` Mode**

`schemasync` is a special mode that makes the connector connects to remote database and attempt to sync only the schema of designated tables. After this is done, the connector is put to `paused` state and user is able to review all the tables and data types created using the default rules and make change if needed.

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
## **Ensure the connector is put to paused state**
```sql
SELECT name, connector_type, pid, stage, state FROM synchdb_state_view;
     name      | connector_type |  pid   |    stage    |  state
---------------+----------------+--------+-------------+---------
 mysqlconn     | mysql          | 579845 | schema sync | polling

```

## **Review the tables created by default mapping rules**

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

## **Define custom mapping rules**
User can use `synchdb_add_objmap` function to create custom mapping rules. It can be used to map table name, column name, data types and defines a data transform expression rule

```sql
SELECT synchdb_add_objmap('mysqlconn','table','inventory.products','stuff');
SELECT synchdb_add_objmap('mysqlconn','table','inventory.customers','schema1.people');
SELECT synchdb_add_objmap('mysqlconn','column','inventory.customers.last_name','family_name');
SELECT synchdb_add_objmap('mysqlconn','column','inventory.customers.email','contact');
SELECT synchdb_add_objmap('mysqlconn','datatype','inventory.geom.g','geometry|0');
SELECT synchdb_add_objmap('mysqlconn','datatype','inventory.orders.quantity','bigint|0');
SELECT synchdb_add_objmap('mysqlconn','transform','inventory.products.name','''>>>>>'' || ''%d'' || ''<<<<<''');
```

## **Review all object mapping rules created so far**
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

## **Reload the object mapping rules**
Once all custom rules have been defined, we need to signal the connector to load them. This will cause the connector to read and apply the object mapping rules. If it sees a discrepancy between current PostgreSQL values and the object mapping values, it will attempt to correct the mapping.
```sql
SELECT synchdb_reload_objmap('mysqlconn');

```

## **Review `synchdb_att_view` again for changes**
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

## **Resume the connector or redo the entire snapshot**
Once the object mappings have been confirmed correct, we can resume the connector. Please note that, resume will proceed to streaming only the new table changes. The existing data of the tables will not be copied.
```sql
SELECT synchdb_resume_engine('mysqlconn');
```

To capture the table's existing data, we can also redo the entire snapshot with the new object mapping rules:
```sql
SELECT synchdb_restart_connector('mysqlconn', 'always');
```
