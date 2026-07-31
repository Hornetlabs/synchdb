---
weight: 90
---
# Sincronización Selectiva de Tablas

Es posible seleccionar solo tablas específicas de una base de datos heterogénea remota para enfocarse en la replicación. Esto podría evitar que se gasten recursos en replicar tablas no deseadas. 

## Seleccione las tablas deseadas e inícielo por primera vez
La selección de tablas se realiza durante la fase de creación del conector a través de `synchdb_add_conninfo()`, donde especificamos una lista de tablas (expresadas en FQN, separadas por una coma) para replicar desde.

Por ejemplo, el siguiente comando crea un conector que solo replica los cambios de las tablas `inventory.orders` e `inventory.products` de la base de datos remota MySQL:
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn', 
    '192.168.1.86', 
    3306, 
    'mysqluser', 
    'mysqlpwd', 
    'inventory', 
    'postgres', 
    'inventory.orders,inventory.products', 
    'mysql', 
    'myrule.json'
);
```

Iniciar este conector por primera vez activará una instantánea inicial y se replicarán el esquema y los datos de las 2 tablas seleccionadas.

```sql
SELECT synchdb_start_engine_bgw('mysqlconn');
```

### Verifique el estado del conector y las tablas
Examine el estado del conector y las nuevas tablas:
```sql
postgres=# SELECT * FROM synchdb_state_view WHERE conninfo_name='mysqlconn';
 id | connector | conninfo_name |  pid   |  state  |   err    |       last_dbz_offset
----+-----------+---------------+--------+---------+----------+-----------------------------
  0 | mysql     | mysqlconn     | 807536 | syncing | no error | offset file not flushed yet
(1 row)

postgres=# SET search_path TO inventory;
SET
postgres=# \d
                 List of relations
  Schema   |        Name        |   Type   | Owner
-----------+--------------------+----------+--------
 inventory | products           | table    | ubuntu
 inventory | products_id_seq    | sequence | ubuntu
 inventory | orders             | table    | ubuntu
 inventory | orders_ididid_seq  | sequence | ubuntu
 public    | synchdb_conninfo   | table    | ubuntu
 public    | synchdb_state_view | view     | ubuntu
(6 rows)

postgres=#
```

Una vez que se complete la instantánea, el conector `mysqlconn` continuará capturando cambios posteriores en las tablas `inventory.orders` e `inventory.products`..

## Agregue más tablas para replicar durante el tiempo de ejecución
El `mysqlconn` de la sección anterior ya ha completado la instantánea inicial y obtenido los esquemas de tabla de la tabla seleccionada. Si desea agregar más tablas para replicar desde, deberá notificar al motor Debezium sobre la sección de tabla actualizada y realizar la instantánea inicial nuevamente. Así es como se hace:

1. Actualice la tabla `synchdb_conninfo` para incluir tablas adicionales.
2. En este ejemplo, agregamos la tabla inventory.customers a la lista de sincronización:
```sql
UPDATE synchdb_conninfo 
SET data = jsonb_set(data, '{table}', '"inventory.orders,inventory.products,inventory.customers"') 
WHERE name = 'mysqlconn';
```
3. Reinicie el conector con el modo de instantánea establecido en always para realizar otra instantánea inicial:
```sql
SELECT synchdb_restart_connector('mysqlconn', 'always');
```
Esto obliga a Debezium a volver a realizar una instantánea de todas las tablas especificadas, incluso si dos de ellas ya tienen los datos.

Tenga en cuenta que si el tipo de base de datos heterogénea no admite la replicación de DDL (como SQLServer), es posible que obtenga un error de conflicto de datos cuando se reconstruye la instantánea en las 2 tablas previamente seleccionadas para la replicación. Si este es el caso, es posible que deba eliminarlas o truncarlas antes de reiniciar el conector con el modo de instantánea = 'always'.

### Verifique las tablas actualizadas
Ahora, podemos examinar nuestras tablas nuevamente:
```sql
postgres=# SET search_path TO inventory;
SET
postgres=# \d
                 List of relations
  Schema   |        Name        |   Type   | Owner
-----------+--------------------+----------+--------
 inventory | products           | table    | ubuntu
 inventory | products_id_seq    | sequence | ubuntu
 inventory | orders             | table    | ubuntu
 inventory | orders_ididid_seq  | sequence | ubuntu
 inventory | customers          | table    | ubuntu
 inventory | customers_id_seq   | sequence | ubuntu
 public    | synchdb_conninfo   | table    | ubuntu
 public    | synchdb_state_view | view     | ubuntu
(8 rows)

postgres=#

```

## Modos de instantánea

SynchDB ofrece diferentes modos de instantánea, según sus necesidades de replicación:

|  **setting** |**description**|
|:-:|-|
| always       | El conector realiza una instantánea cada vez que se inicia. La instantánea incluye la estructura y los datos de las tablas capturadas. Después de que se completa la instantánea, el conector comienza a transmitir registros de eventos para cambios posteriores en la base de datos.|
| initial (default)      | El conector realiza una instantánea de la base de datos si aún no se ha realizado. Después de que se completa la instantánea, el conector comienza a transmitir registros de eventos para cambios posteriores en la base de datos.|
| initial_only | El conector realiza una instantánea de la base de datos. Después de que se completa la instantánea, el conector se detiene y no transmite registros de eventos para cambios posteriores en la base de datos.|
| no_data      | El conector captura la estructura de todas las tablas relevantes, pero no los datos que contienen.|
| never        | Cuando se inicia el conector, en lugar de realizar una instantánea, comienza inmediatamente a transmitir registros de eventos para cambios posteriores en la base de datos. (solo MySQL)|
| recovery     | Configure esta opción para restaurar un historial de esquema de base de datos que está perdido o dañado. Después de un reinicio, el conector ejecuta una instantánea que reconstruye el tema de las tablas de origen|
| when_needed  | Después de que se inicia el conector, realiza una instantánea solo si detecta una de las siguientes circunstancias:<br><ul><li>No puede detectar ningún desplazamiento del tema</li><li>Una compensación registrada previamente especifica una posición de registro que no está disponible en el servidor</li></ul> |
