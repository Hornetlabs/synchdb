# Metadata Files

During Operation, Debezium Runner engine produces metadata files under $PGDATA/pg_synchdb. Currently 2 types of metadata files are generated and persisted:
* offset file: contains the offset to resume the replication operation at start up
* schema history file: contains the schema information to build all the tables for a replication. This is created during the initial data snapshot sync and can be updated during operation.

These metadata filenames consist of:

* (connector type)_(connector name)_(destination database)_offsets.dat

* (connector type)_(connector name)_(destination_database)_schemahistory.dat

```
ls $PGDATA/pg_synchdb
mysql_mysqlconn_postgres_offsets.dat        sqlserver_sqlserverconn_postgres_offsets.dat
mysql_mysqlconn_postgres_schemahistory.dat  sqlserver_sqlserverconn_postgres_schemahistory.dat
```

These binary files' contents can be viewed with hexdump command:
```
hexdump -C $PGDATA/pg_synchdb/mysql_mysqlconn_postgres_offsets.dat
hexdump -C $PGDATA/pg_synchdb/mysql_mysqlconn_postgres_schemahistory.dat
```

## **Reset Connector**

A connector can be reset (re-copy and re-synchronize) specified tables, simply by：

* stop the connector 
* Remove both the offsets and schemahistory file
* start the connector