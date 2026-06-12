---
weight: 10
---
# Lanzador Automático de Conectores

## Habilitar el Lanzador Automático de SynchDB
Un trabajador de conexión se vuelve elegible para el lanzamiento automático cuando se emite `synchdb_start_engine_bgw()` en un `connector name` específico. Del mismo modo, se vuelve inelegible cuando se emite `synchdb_stop_engine_bgw()`.

El lanzador automático de conectores se puede habilitar mediante:

* Agregar `synchdb` a la opción GUC `shared_preload_libraries` en postgresql.conf
* Establecer la nueva opción GUC `synchdb.synchdb_auto_launcher` en true en postgresql.conf
* Reiniciar el servidor PostgreSQL para que los cambios surtan efecto

Por ejemplo:
```conf
shared_preload_libraries = 'synchdb'
synchdb.synchdb_auto_launcher = true
```

Durante el arranque, la extensión SynchDB se precarga en una etapa temprana. Con `synchdb.synchdb_auto_launcher` configurado en true, SynchDB lanza un proceso de trabajo en segundo plano (background worker) llamado `synchdb_auto_launcher`, el cual a su vez genera un proceso `synchdb_db_launcher` por cada base de datos conectable y no plantilla. Cada `synchdb_db_launcher` recupera todos los conninfos de la tabla `synchdb_conninfo` (si existe) que estén marcados como `active` (es decir, con el indicador `isactive` establecido en `true`), y luego los inicia automáticamente como procesos de trabajo en segundo plano independientes, de la misma manera que cuando se llama a `synchdb_start_engine_bgw()`. El proceso `synchdb_auto_launcher` finaliza una vez que todos los `synchdb_db_launcher` han terminado su trabajo.
