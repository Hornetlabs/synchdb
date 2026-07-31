---
weight: 100
---
# Valores de desplazamiento inicial personalizados

Un valor de offset de inicio representa un punto desde el cual comenzar la replicación, de manera similar al LSN de reanudación de PostgreSQL. Cuando el motor Debezium runner inicia, comenzará la replicación desde este valor de offset. Establecer este valor de offset a un valor anterior hará que el motor Debezium runner comience la replicación desde registros anteriores, posiblemente replicando registros de datos duplicados. Debemos ser extremadamente cautelosos al establecer valores de offset de inicio en Debezium.

## Registrar Valores de Offset Configurables
Durante la operación, se generarán nuevos offsets y se guardarán en disco por el motor Debezium runner. El último offset guardado puede recuperarse desde el comando de utilidad `synchdb_state_view()`:

```
postgres=# select * from synchdb_state_view;
 id | connector | conninfo_name  |  pid   |  state  |   err    |                                          last_dbz_offset
----+-----------+----------------+--------+---------+----------+---------------------------------------------------------------------------------------------------
  0 | mysql     | mysqlconn      | 461696 | syncing | no error | {"ts_sec":1725644339,"file":"mysql-bin.000004","pos":138466,"row":1,"server_id":223344,"event":2}
  1 | sqlserver | sqlserverconn  | 461739 | syncing | no error | {"event_serial_no":1,"commit_lsn":"00000100:00000c00:0003","change_lsn":"00000100:00000c00:0002"}
  3 | null      |                |     -1 | stopped | no error | no offset
  [... el resto de las filas ...]
```

Dependiendo del tipo de conector, este valor de offset difiere. Del ejemplo anterior, el último offset guardado del conector `mysql` es `{"ts_sec":1725644339,"file":"mysql-bin.000004","pos":138466,"row":1,"server_id":223344,"event":2}` y el último offset guardado de `sqlserver` es `{"event_serial_no":1,"commit_lsn":"00000100:00000c00:0003","change_lsn":"00000100:00000c00:0002"}`. 

Debemos guardar estos valores regularmente, para que en caso de que tengamos un problema, conozcamos la ubicación del offset en el pasado que se puede establecer para reanudar la operación de replicación.

## Pausar el Conector
Un conector debe estar en estado `paused` antes de que se pueda establecer un nuevo valor de offset.

Use la función SQL `synchdb_pause_engine()` para pausar un conector en ejecución. Esto detendrá el motor Debezium runner de replicar desde la base de datos heterogénea. Cuando está pausado, es posible alterar el valor de offset del conector Debezium para replicar desde un punto específico en el pasado usando la rutina SQL `synchdb_set_offset()`. Toma `conninfo_name` como su argumento, que se puede encontrar en la salida de la vista `synchdb_get_state()`.

Por ejemplo:
```
SELECT synchdb_pause_engine('mysqlconn');
```

## Establecer el nuevo Offset
Use la función SQL `synchdb_set_offset()` para cambiar el offset de inicio de un trabajador del conector. Esto solo se puede hacer cuando el conector está en estado `paused`. La función toma 2 parámetros, `conninfo_name` y `una cadena de offset válida`, ambos se pueden encontrar en la salida de la vista `synchdb_get_state()`.

Por ejemplo:
```
SELECT synchdb_set_offset('mysqlconn', '{"ts_sec":1725644339,"file":"mysql-bin.000004","pos":138466,"row":1,"server_id":223344,"event":2}');
```

## Reanudar el Conector

Use la función SQL `synchdb_resume_engine()` para reanudar la operación de Debezium desde un estado pausado. Esta función toma `connector name` como su único parámetro, que se puede encontrar en la salida de la vista `synchdb_get_state()`. El motor Debezium runner reanudado comenzará la replicación desde el valor de offset recién establecido.

Por ejemplo:
```
SELECT synchdb_resume_engine('mysqlconn');
```