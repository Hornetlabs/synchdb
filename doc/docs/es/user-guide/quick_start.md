---
weight: 30
---
# Guía de Inicio Rápido

Es muy sencillo comenzar a usar SynchDB para realizar replicación de datos desde bases de datos heterogéneas a PostgreSQL, siempre que tenga la información de conexión correcta para sus bases de datos heterogéneas.

## Instalar la Extensión SynchDB
La extensión SynchDB requiere pgcrypto para cifrar ciertos datos sensibles de credenciales. Asegúrese de que esté instalado antes de instalar SynchDB. Alternativamente, puede incluir la cláusula `CASCADE` en `CREATE EXTENSION` para instalar automáticamente las dependencias:

```sql
CREATE EXTENSION synchdb CASCADE;
```

## Crear una Información de Conexión
Esto se puede hacer con la función SQL de utilidad `synchdb_add_conninfo()`.

synchdb_add_conninfo toma estos argumentos:

|        argumento        | descripción |
|-------------------- |-|
| name                  | un identificador único que representa esta información del conector |
| hostname              | la dirección IP o nombre de host de la base de datos heterogénea |
| port                  | el número de puerto para conectarse a la base de datos heterogénea |
| username              | nombre de usuario para autenticarse con la base de datos heterogénea |
| password              | contraseña para autenticar el nombre de usuario |
| source database       | este es el nombre de la base de datos fuente en la base de datos heterogénea de la que queremos replicar los cambios |
| destination database  | este es el nombre de la base de datos de destino en PostgreSQL para aplicar los cambios. Debe ser una base de datos válida que exista en PostgreSQL |
| table                 | (opcional) - expresado en la forma de `[database].[table]` o `[database].[schema].[table]` que debe existir en la base de datos heterogénea para que el motor solo replique las tablas especificadas. Si se deja vacío, se replican todas las tablas |
| connector             | el tipo de conector a utilizar (MySQL, Oracle, SQLServer... etc) |
| rule file             | un archivo de reglas con formato JSON colocado bajo $PGDATA que este conector aplicará a sus reglas de traducción de tipo de datos predeterminadas. Consulte [aquí](https://docs.synchdb.com/user-guide/transform_rule_file/) para más información |

Ejemplos:

1. Crear un conector MySQL llamado `mysqlconn` para replicar desde la base de datos fuente `inventory` en MySQL a la base de datos de destino `postgres` en PostgreSQL, usando el archivo de reglas `myrule.json`:
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn',
    '127.0.0.1',
    3306,
    'mysqluser',
    'mysqlpwd',
    'inventory',
    'postgres',
    '',
    'mysql',
    'myrule.json');
```

2. Crear un conector MySQL llamado `mysqlconn2` para replicar desde la base de datos fuente `inventory` a la base de datos de destino `mysqldb2` en PostgreSQL usando la regla de traducción predeterminada:
```sql
SELECT synchdb_add_conninfo(
    'mysqlconn2', '127.0.0.1', 3306, 'mysqluser', 
    'mysqlpwd', 'inventory', 'mysqldb2', 
    '', 'mysql', ''
  );
```

3. Crear un conector SQLServer llamado 'sqlserverconn' para replicar desde la base de datos fuente 'testDB' a la base de datos de destino 'sqlserverdb' en PostgreSQL usando la regla de traducción predeterminada:
```sql
SELECT 
  synchdb_add_conninfo(
    'sqlserverconn', '127.0.0.1', 1433, 
    'sa', 'Password!', 'testDB', 'sqlserverdb', 
    '', 'sqlserver', ''
  );
```

4. Crear un conector MySQL llamado `mysqlconn3` para replicar desde las tablas `orders` y `customers` de la base de datos fuente `inventory` a la base de datos de destino `mysqldb3` en PostgreSQL usando el archivo de reglas `myrule2.json`:
```sql
SELECT 
  synchdb_add_conninfo(
    'mysqlconn3', '127.0.0.1', 3306, 'mysqluser', 
    'mysqlpwd', 'inventory', 'mysqldb3', 
    'inventory.orders,inventory.customers', 
    'mysql', 'myrule2.json'
  );
```

## Puntos a Tener en Cuenta
* Es posible crear múltiples conectores conectándose al mismo tipo de conector (ej, MySQL, SQLServer..etc). SynchDB generará conexiones separadas para obtener datos de cambios.
* El certificado X509 definido por el usuario y la clave privada para la conexión TLS a la base de datos remota serán compatibles en un futuro cercano. Mientras tanto, asegúrese de que la configuración TLS esté configurada como opcional.

## Verificar la Información de Conexión Creada
Toda la información de conexión se crea en la tabla `synchdb_conninfo`. Podemos ver su contenido y hacer modificaciones según sea necesario. Tenga en cuenta que la contraseña de las credenciales de usuario está cifrada por pgcrypto usando una clave que solo conoce synchdb. Así que por favor no modifique el campo de contraseña o puede descifrarse incorrectamente si se manipula. Vea a continuación un ejemplo de salida:

```sql
postgres=# \x
Expanded display is on.

postgres=# select * from synchdb_conninfo;
-[ RECORD 1 ]-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name | mysqlconn
data | {"pwd": "\\xc30d040703024828cc4d982e47b07bd23901d03e40da5995d2a631fb89d49f748b87247aee94070f71ecacc4990c3e71cad9f68d57c440de42e35bcc78fd145feab03452e454284289db", "port": 3306, "user": "mysqluser", "dstdb": "postgres", "srcdb": "inventory", "table": "null", "hostname": "192.168.1.86", "connector": "mysql"i, "myrule.json"}
-[ RECORD 2 ]-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name | sqlserverconn
data | {"pwd": "\\xc30d0407030231678e1bb0f8d3156ad23a010ca3a4b0ad35ed148f8181224885464cdcfcec42de9834878e2311b343cd184fde65e0051f75d6a12d5c91d0a0403549fe00e4219215eafe1b", "port": 1433, "user": "sa", "dstdb": "sqlserverdb", "srcdb": "testDB", "table": "null", "hostname": "192.168.1.86", "connector": "sqlserver", "null"}
```

## Iniciar un Conector
Use la función `synchdb_start_engine_bgw()` para iniciar un trabajador conector. Toma un argumento que es el nombre de conexión creado anteriormente. Este comando generará un nuevo trabajador en segundo plano para conectarse a la base de datos heterogénea con las configuraciones especificadas.

Por ejemplo, lo siguiente generará 2 trabajadores en segundo plano en PostgreSQL, uno replicando desde una base de datos MySQL, el otro desde SQL Server:

```sql
select synchdb_start_engine_bgw('mysqlconn');
select synchdb_start_engine_bgw('sqlserverconn');
```

## Verificar el Estado de Ejecución del Conector
Use la vista `synchdb_state_view()` para examinar todos los conectores en ejecución y sus estados. Actualmente, synchdb puede soportar hasta 30 trabajadores en ejecución.

Vea a continuación un ejemplo de salida:
```sql
postgres=# select * from synchdb_state_view;
 id | connector | conninfo_name  |  pid   |  state  |   err    |                                          last_dbz_offset
----+-----------+----------------+--------+---------+----------+---------------------------------------------------------------------------------------------------
  0 | mysql     | mysqlconn      | 461696 | syncing | no error | {"ts_sec":1725644339,"file":"mysql-bin.000004","pos":138466,"row":1,"server_id":223344,"event":2}
  1 | sqlserver | sqlserverconn  | 461739 | syncing | no error | {"event_serial_no":1,"commit_lsn":"00000100:00000c00:0003","change_lsn":"00000100:00000c00:0002"}
  2 | null      |                |     -1 | stopped | no error | no offset
  3 | null      |                |     -1 | stopped | no error | no offset
  4 | null      |                |     -1 | stopped | no error | no offset
  5 | null      |                |     -1 | stopped | no error | no offset
  ...
  ...
```

Detalles de las Columnas:

| campos          | descripción |
|-|-|
| id              | identificador único de un slot de conector |
| connector       | el tipo de conector (mysql, oracle, sqlserver...etc) |
| conninfo_name   | el nombre de información del conector asociado creado por `synchdb_add_conninfo()` |
| pid             | el PID del proceso trabajador del conector |
| state           | el estado del conector. Los estados posibles son: <br><br><ul><li>stopped - el conector no está ejecutándose</li><li>initializing - el conector está inicializando</li><li>paused - el conector está pausado</li><li>syncing - el conector está sondeando regularmente eventos de cambio</li><li>parsing - el conector está analizando un evento de cambio recibido</li><li>converting - el conector está convirtiendo un evento de cambio a representación PostgreSQL</li><li>executing - el conector está aplicando el evento de cambio convertido a PostgreSQL</li><li>updating offset - el conector está escribiendo un nuevo valor de offset a la gestión de offset de Debezium</li><li>restarting - el conector está reiniciando</li><li>unknown</li></ul> |
| err             | el último mensaje de error encontrado por el trabajador que habría causado su salida. Este error podría originarse desde PostgreSQL mientras procesa un cambio, o originarse desde el motor de ejecución Debezium mientras accede a datos desde la base de datos heterogénea |
| last_dbz_offset | el último offset de Debezium capturado por synchdb. Tenga en cuenta que esto puede no reflejar el valor de offset actual y en tiempo real del motor del conector. Más bien, esto se muestra como un punto de control desde el que podríamos reiniciar si es necesario |

## Detener un Conector
Use la función SQL `synchdb_stop_engine_bgw()` para detener un trabajador conector en ejecución o pausado. Esta función toma `conninfo_name` como su único parámetro, que se puede encontrar en la salida de la vista `synchdb_get_state()`.

Por ejemplo:
```sql
select synchdb_stop_engine_bgw('mysqlconn');
```

La función `synchdb_stop_engine_bgw()` también marca una información de conexión como `inactive`, lo que evita que este trabajador se relance automáticamente en los reinicios del servidor.