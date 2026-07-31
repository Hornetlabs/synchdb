# Archivos de Metadatos
Durante la operación, el motor Debezium Runner genera archivos de metadatos bajo $PGDATA/pg_synchdb. Actualmente se generan y persisten 2 tipos de archivos de metadatos:
* archivo de offset: contiene el offset para reanudar la operación de replicación al inicio
* archivo de historial de esquema: contiene la información del esquema para construir todas las tablas para una replicación. Esto se crea durante la sincronización inicial de datos instantáneos y puede actualizarse durante la operación.

Estos nombres de archivos de metadatos consisten en:
* (tipo de conector)_(nombre del conector)_offsets.dat
* (tipo de conector)_(nombre del conector)_schemahistory.dat

```
ls $PGDATA/pg_synchdb
mysql_mysqlconn_offsets.dat        sqlserver_sqlserverconn_offsets.dat
mysql_mysqlconn_schemahistory.dat  sqlserver_sqlserverconn_schemahistory.dat
```

El contenido de estos archivos binarios puede visualizarse con el comando hexdump:
```
hexdump -C $PGDATA/pg_synchdb/mysql_mysqlconn_offsets.dat
hexdump -C $PGDATA/pg_synchdb/mysql_mysqlconn_schemahistory.dat
```