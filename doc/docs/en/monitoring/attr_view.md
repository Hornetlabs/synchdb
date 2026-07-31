# Attribute View

## **Review Attributes Managed by a Connector**

As a connector completes initial table snapshot, the source tables, columns and data types would have been translated, transformed and created on PostgreSQL side according to the [object mapping rules](../../user-guide/object_mapping_rules/). SynchDB provides a view that displays a side-by-side view of a connector's data type, name mapping and transform rule relationships between source and destination tables.

This view is informational and it intends to show the user the list of tables a connector currently is tracking and the mapping/transform rules it uses per table/column.

```sql
SELECT * FROM synchdb_att_view();
```

**Return Fields**:

| Field | Description | Type |
|-|-|-|
| `name` | Connector identifier | Text |
| `attnum` | Attribute number | Integer |
| `ext_tbname` | table name as appeared remotely | Text |
| `pg_tbname` | mapped table name in PostgreSQL | Text |
| `ext_attname` | column name as appeared remotely | Text |
| `pg_attname` | mapped column name in PostgreSQL | Text |
| `ext_atttypename` | data type as appeared remotely | Text |
| `pg_atttypename` | mapped data type in PostgreSQL | Text |
| `transform` | transform expression | Text |

**Example Output**

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