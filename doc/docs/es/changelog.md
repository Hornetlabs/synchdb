# Registro de Cambios
Todos los cambios notables de este proyecto serán documentados en este archivo.

El formato está basado en [Keep a Changelog](http://keepachangelog.com/)
y este proyecto se adhiere a [Versionado Semántico](http://semver.org/).

## **[SynchDB 1.4](https://github.com/Hornetlabs/synchdb/releases/tag/v1.4) - 2026-08-04**

SynchDB 1.4 extiende el soporte de Bases de Datos en Contenedor de Oracle (CDB/PDB) a las rutas de ejecución de Debezium, oracle_fdw y OLR; añade conexiones seguras mediante TLS para los conectores de MySQL y PostgreSQL (con soporte de Oracle Wallet para Oracle/OLR); y permite ajustar el nivel de registro (log level) del runner de Debezium en tiempo de ejecución, sin necesidad de reiniciar. Esta versión también incluye una ronda de correcciones de estabilidad (fugas de referencias JNI, una doble liberación de memoria en el conversor de formato, un conflicto de símbolos bajo IvorySQL) y amplía la cobertura de CI para compilar y probar contra IvorySQL como plataforma anfitriona.

### **Añadido**

#### [Soporte de Bases de Datos en Contenedor de Oracle (CDB/PDB)](../user-guide/create_a_connector/)

* El conector de Oracle basado en Debezium ahora acepta un nombre de base de datos con formato `CDB/PDB` y lo asigna automáticamente a las propiedades `database.dbname` y `database.pdb.name` de Debezium.
* La ruta de instantánea basada en FDW mediante [oracle_fdw](https://github.com/laurenz/oracle_fdw) ahora soporta la arquitectura de Bases de Datos en Contenedor de Oracle.
* Se añadió cobertura de pruebas dedicada para CDB/PDB contra Oracle 23ai (Free PDB1).

#### [TLS / Conexiones Seguras](../user-guide/configure_snapshot_engine/)

* Se añadió soporte de conexión TLS para los conectores de MySQL y PostgreSQL mediante parámetros adicionales de conninfo.
* Los conectores de Oracle y OLR ahora usan Oracle Wallet para conexiones seguras en lugar de parámetros TLS estándar.

#### [Nivel de Registro de Debezium Ajustable en Tiempo de Ejecución](../getting-started/configuration/)

* El nivel de registro del runner de Debezium ahora puede cambiarse mientras un conector está en ejecución, sin necesidad de reiniciarlo. ([#106](https://github.com/Hornetlabs/synchdb/issues/106))

### **Cambios**

* El modo de instantánea `never` ahora está restringido únicamente al conector de MySQL. Otros conectores que anteriormente aceptaban `never` deben usar `no_data` en su lugar; `schemasync` ahora implementa esto de manera uniforme internamente.
* Debezium se actualizó de la versión 2.6.2.Final a la 3.5.2.Final (un salto de versión mayor; primero se intentó con 3.6.0.Final, pero se revirtió tras descubrir errores conocidos en esa versión), junto con Kafka Connect (3.6.2 → 4.1.2), Jackson y Log4j2.
* El traductor de tipos de datos ahora captura y transfiere correctamente los valores `tsvector` (anteriormente se sincronizaban como NULL).
* La matriz de compilación y pruebas de CI se amplió para cubrir IvorySQL como plataforma anfitriona, además de PostgreSQL.

### **Corregido**

* Se corrigió Auto Launcher, que anteriormente solo iniciaba conectores para la base de datos `postgres` predeterminada debido a una conexión codificada de forma fija. Ahora enumera correctamente todas las bases de datos conectables y no plantilla, y genera un lanzador por cada base de datos, de modo que los conectores se inician automáticamente sin importar en qué base de datos se haya instalado SynchDB. ([#71](https://github.com/Hornetlabs/synchdb/issues/71))
* Se corrigieron fugas de referencias locales JNI en el puente de Debezium que podían provocar fallos bajo carga sostenida.
* Se corrigió una doble liberación (double-free) de `StringInfoData` en el conversor de formato. ([#252](https://github.com/Hornetlabs/synchdb/issues/252))
* Se corrigió un conflicto de símbolos entre el analizador Oracle nativo incluido en SynchDB y el analizador Oracle integrado propio de IvorySQL, al cargar la biblioteca del analizador del conector OLR bajo un host IvorySQL; la biblioteca del analizador ahora se carga con `RTLD_LOCAL`.
* `synchdb_del_conninfo()` ahora limpia correctamente los datos de estado residuales del conector en la memoria compartida.
* Se corrigió un posible problema de manejo de memoria en el agente de replicación y en el cliente OLR.

### **Problemas Conocidos e Información Adicional**

* mysql_fdw actualmente no se puede compilar ni usar bajo IvorySQL 5.x. La ruta de instantánea basada en FDW para MySQL no está disponible en esa combinación específica hasta que se resuelva la compatibilidad con el proyecto de origen.
* La instantánea basada en FDW para SQL Server todavía no es compatible.
* Debido a que el motor de instantáneas FDW escribe un archivo de offset antes de que exista el slot de replicación, esto entra en conflicto con la verificación `validateLogPosition()` introducida en Debezium 3.x. Actualmente esto se soluciona estableciendo `offset.mismatch.strategy` en `trust_slot`, restaurando el comportamiento de inicio previo a la versión 3.x (por ejemplo, 2.6) como medida temporal hasta contar con una solución más completa. Consulte el [Issue #256](https://github.com/Hornetlabs/synchdb/issues/256).

## [SynchDB 1.0 Beta1] - 2024-10-23

La primera versión de SynchDB que establece una base sólida para la replicación perfecta desde bases de datos heterogéneas a PostgreSQL.

### Añadido
* Replicación lógica desde bases de datos heterogéneas: (MySQL y SQLServer)
* [Replicación DDL](../user-guide/ddl_replication) (CREATE TABLE, DROP TABLE, ALTER TABLE ADD COLUMN, ALTER TABLE DROP COLUMN, ALTER TABLE ALTER COLUMN)
* Replicación DML (INSERT, UPDATE, DELETE)
* Máximo 30 trabajadores de conectores concurrentes
* [Lanzador automático de conectores](../user-guide/connector_auto_launcher) al inicio de PostgreSQL
* Vistas de estado global del conector y últimos mensajes de error
* [Replicación selectiva de bases de datos y tablas](../user-guide/selective_table_sync)
* Eventos de cambios en lotes
* Reinicios de conectores en diferentes modos de instantánea
* [Interfaces de gestión de offset](../user-guide/set_offset) para seleccionar punto de reanudación de replicación personalizado
* Reglas de transformación predeterminadas de tipos de datos y nombres de objetos para bases de datos heterogéneas soportadas
* [Archivo de reglas JSON](../user-guide/transform_rule_file) para definir personalizaciones: (tipo de datos, nombre de columna, nombre de tabla y reglas de transformación de expresiones de datos)
* 2 modos de aplicación de datos (SPI, HeapAM API)
* Varias [funciones de utilidad](../user-guide/utility_functions) para realizar operaciones de conector: (iniciar, detener, pausar, reanudar)

### Cambios
No aplica

### Correcciones
No aplica