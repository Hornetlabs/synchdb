# Configure Object Mapping and Transform Rules

SynchDB has a default name and data type mapping rules to handle incoming change events. In most cases, the defaults work fine. However, if you have specific transform requirements or when the defaults do not work for you, you can configure your own object mapping rules to a particular connector. 

## **synchdb_add_objmap**

This utility function can be used to configure table name, column name, data types as well as transform rules. it takes 4 parameters:

| Parameter | Description | Required | Example | Notes |
|:-:|:-|:-:|:-|:-|
| `name` | Unique identifier for this connector | ✓ | `'mysqlconn'` | Must be unique across all connectors |
| `object type` | type of object mapping | ✓ | `'table'` | can be `table` to map a table name, `column` to map a column name, `datatype` to map a data type, or `transform` to run a data transform expression |
| `source object` | source object represented as fully-qualified name | ✓ | `inventory.customers` | the object name as represented in the remote database |
| `destination object` | destination object name | ✓ | `'schema1.people'` | The destination object name in PostgreSQL side. Can be a fully-qualified table name, a column name, a data type or transform expression |

## **Configure Table Name Mappings**

* `source object` represents the table in fully-qualified name in remote database
* `destination object` represents the table name in PostgreSQL. It can be just a name (default to public schema) or in schema.name format. 

This example maps `inventory.customers` table in the source database to `schema1.people` in PostgreSQL.
```sql
SELECT synchdb_add_objmap('mysqlconn','table','inventory.customers','schema1.people');
```

## **Configure Column Name Mappings**

* `source object` represents the column in fully-qualified name in remote database
* `destination object` represents the column name in PostgreSQL. No need to format it as fully-qualified column name.

This example maps `inventory.customers.email` column in the source table to `contact` in PostgreSQL.
```sql
SELECT synchdb_add_objmap('mysqlconn','column','inventory.customers.email','contact');
```

## **Configure Data Type Mappings**

* `source object` can be expressed as one of:
    * a fully-qualified column (inventory.geom.g). This means the data type mapping applies to this particular column only.
    * a general data type string (int). Use a pipe (|) to add if it is a autoincrement data type (int|true for autoincremented int) or (int|false for a non-autoincremented int). This means data type mapping applies to all data type with the matching condition.

* `destination object` should be expressed as a general data type string that exists in PostgreSQL. Use a pipe (|) to overwrite the size (text|0 to overwrite the size to 0 because text is variable size) or (varchar|-1 to use whatever size that comes with the change event)

This example maps all non-autoincrement `point` data type to `text` data type in PostgreSQL.
```sql
SELECT synchdb_add_objmap('mysqlconn','datatype','point|false','text|0');
```

This example maps the table `inventory.geom`'s column `g`'s data type to `geometry` in PostgreSQL.
```sql
SELECT synchdb_add_objmap('mysqlconn','datatype','inventory.geom.g','geometry|0');
```

## **Configure Transform Rules**

* `source object` represents the column to be transformed
* `destination object` represents an expression to be run on the column data before it is applied to PostgreSQL. Use %d as a placeholder for input column data. In case of geometry type, use %w for WKB and %s for SRID. 

This example will prepend '>>>>>' and append '<<<<<' to whatever value SynchDB receives for column 'inventory.products.name'
```sql
SELECT synchdb_add_objmap('mysqlconn','transform','inventory.products.name','''>>>>>'' || ''%d'' || ''<<<<<''');
```

This example will always add 500 to whatever value SynchDB receives for column 'inventory.orders.quantity':
```sql
SELECT synchdb_add_objmap('mysqlconn','transform','inventory.orders.quantity','%d + 500');
```

## **Apply the Rules**

If the connector is not running, the rules are automatically applied at the next startup via `synchdb_start_engine_bgw`.

If the connector is already running, the rules are not automatically applied, we have to tell the connector to reload the object mapping rules and apply using utility function `synchdb_reload_objmap`.

```sql
SELECT synchdb_reload_objmap('mysqlconn');
```

