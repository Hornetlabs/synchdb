# Create a Connector

## **Create a Connector**

A connector represents a connection to a particular source database, replicate one set of tables and apply to PostgreSQL. If you have multiple source databases that need replication, multiple connectors are required (one for each). It is also possible to create multiple connectors that connect to the same source database but replicate different or same sets of tables.

Creating a connector can be done with utility SQL function `synchdb_add_conninfo()`.

synchdb_add_conninfo takes these arguments:

|        argument        | description |
|-------------------- |-|
| name                  | a unique identifier that represents this connector info |
| hostname              | the IP address or hostname of the heterogeneous database. |
| port                  | the port number to connect to the heterogeneous database. |
| username              | user name to use to authenticate with heterogeneous database.|
| password              | password to authenticate the username |
| source database       | this is the name of source database that we want to replicate changes from.|
| source schema  | this is the name of source schema under source database that we want to replicate changes from  |
| table                 | (optional) - expressed in the form of `[database].[table]` or `[schema].[table]` that must exists in source database / schema so the engine will only replicate the specified tables. If left empty, all tables are replicated. Alternatively, a table list file can be specified with `file:` prefix  |
| snapshot table        | (optional) - expressed in the form of `[database].[table]` or `[schema].[table]` that must exists in the `table` setting above, so the engine will only rebuild the snapshot of these tables if snapshot mode is set to `always`. If left empty or null, all tables specified in `table` setting above will be rebuilt when snapshot mode is set to `always`. Alternatively, a snapshot table list file can be specified with `file:` prefix|
| connector             | the connector type (See below) |

<<**NOTE**>> `source database`, `source schema`, `username`, `password`, `table` and `snapshot table` are case sensitive, and you must specify the names exactly as appeared in your source database, so keep in mind the letter casing of these names.  

## **Connector Types**

SynchDb supports these connector types:

* mysql         -> MySQL database
* sqlserver     -> Microsoft SQL Server database
* oracle        -> Oracle database
* olr           -> Native Openlog Replicator
* postgres      -> PostgreSQL database

## **Check Created Connectors**

Created connectors are shown in the table `synchdb_conninfo`. We are free to view its content and make modification as required. Please note that the password of a user credential is encrypted by pgcrypto using a key only known to synchdb. So please do not modify the password field directly as it will be decrypted incorrectly if tempered. See below for an example output:

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

## **Use a Table List File to Specify Tables**

If there is a large number of tables to replicate, it is possible to use a table list file to specifiy the tables. The list must be formatted as JSON like below:

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
SynchDB looks for these key JSON arrays by names:
* `table_list` is a JSON array, containing the tables to replicate expressed as string. This is required when the `table` parameter starts with prefix `file:` followed by path to the file
* `snapshot_table_list` is also a JSON array, containing the tables to perform snapshot on. This is required when `snapshot table` parameter starts with prefix `file:` followed by path to the file.

the file path can be relative to where PostgreSQL data directory or an absolute path.

## **When to Specify Snapshot Table List?**

We can normally leave `snapshot table list` parameter to either empty or as `null`, which would default to the same value as the `table` parameter. This means that SynchDB will perform an initial snapshot (replicate the schema and copy initial data) on all the tables specified in `table` parameter when needed. In some cases, we may only want a subset of `table` to perform the initial snapshot, if that is the case, we would set a different `snapshot table list` to indicate to SynchDB to only rebuild the table snapshot specified.


## **Example: Create a Connector for each Supported Source Database to Replicate All Tables**

1. Create a MySQL connector called `mysqlconn` to replicate all tables under `inventory` in MySQL. Source schema can be put as 'null' because MySQL does not support it.
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', '127.0.0.1', 3306, 'mysqluser', 
    'mysqlpwd', 'inventory', 'null', 
    'null', 'null', 'mysql');
```

2. Create a SQLServer connector called `sqlserverconn` to replicate all tables under `testDB` database and `dbo` schema.
```sql
SELECT 
  synchdb_add_conninfo(
    'sqlserverconn', '127.0.0.1', 1433, 
    'sa', 'Password!', 'testDB', 'dbo', 
    'null', 'null', 'sqlserver');
```

3. Create a Oracle connector called `oracleconn` to replicate all tables under `FREE` database and `DBZUSER` schema:
```sql
SELECT 
  synchdb_add_conninfo(
    'oracleconn', '127.0.0.1', 1521, 
    'DBZUSER', 'dbz', 'FREE', 'DBZUSER', 
    'null', 'null', 'oracle');
```

## **Example: Create a Connector to Replicate Specified Tables**

Note that the tables must be specified in fully-qualified names such as `[database].[table]` or `[schema].[table]` and exist in the source database.

Create a MySQL connector called `mysqlconn` to replicate `orders` and `customers` tables under `inventory` in MySQL to destination database `postgres` in PostgreSQL:
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', '127.0.0.1', 3306, 'mysqluser', 
    'mysqlpwd', 'inventory', 'null', 
    'inventory.orders,inventory.customers', 'null', 'mysql');

```

## **Example: Create a Connector to Replicate Specified Tables Using a File**

Create a MySQL connector called `mysqlconn` to replicate the tables specified in the table file under `inventory` in MySQL to destination database `postgres` in PostgreSQL:
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', '127.0.0.1', 3306, 'mysqluser', 
    'mysqlpwd', 'inventory', 'null', 
    'file:/path/to/mytablefile.json', 'file:/path/to/mytablefile.json', 'mysql');

```

where `/path/to/mytablefile.json` could be:

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