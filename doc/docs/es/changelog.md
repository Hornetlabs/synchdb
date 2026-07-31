# Registro de Cambios
Todos los cambios notables de este proyecto serán documentados en este archivo.

El formato está basado en [Keep a Changelog](http://keepachangelog.com/)
y este proyecto se adhiere a [Versionado Semántico](http://semver.org/).

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