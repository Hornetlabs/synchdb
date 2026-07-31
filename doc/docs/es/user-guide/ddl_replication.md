---
weight: 80
---
# Replicación DDL

## Descripción General
SynchDB proporciona soporte integral para operaciones de Lenguaje de Definición de Datos (DDL), permitiendo la sincronización de esquemas en tiempo real entre diferentes sistemas de bases de datos.

## Comandos DDL Soportados
SynchDB soporta las siguientes operaciones DDL:

✅ CREATE [table]  
✅ ALTER [table] ADD COLUMN  
✅ ALTER [table] DROP COLUMN  
✅ ALTER [table] ALTER COLUMN  
✅ DROP [table]  

## Soporte Detallado de Comandos
### CREATE TABLE
SynchDB captura estas propiedades durante los eventos CREATE TABLE:

| Propiedad | Descripción |
|-----------|-------------|
| Nombre de Tabla | Formato de Nombre Completamente Calificado (FQN) |
| Nombres de Columnas | Identificadores individuales de columnas |
| Tipos de Datos | Especificaciones de tipos de datos de columnas |
| Longitud de Datos | Especificaciones de longitud/precisión (si aplica) |
| Marcador Unsigned | Restricciones unsigned para tipos numéricos |
| Nulabilidad | Restricciones NULL/NOT NULL |
| Valores Predeterminados | Expresiones de valores predeterminados |
| Claves Primarias | Definiciones de columnas de clave primaria |

> **Nota**: Propiedades adicionales de CREATE TABLE no están soportadas actualmente

### DROP TABLE
Propiedades capturadas:
- Nombre de la tabla (en formato FQN) a eliminar

### ALTER TABLE ADD COLUMN
Captura las siguientes propiedades:

| Propiedad | Descripción |
|-----------|-------------|
| Nombres de Columnas | Nombres de las columnas nuevas |
| Tipos de Datos | Tipos de datos para nuevas columnas |
| Longitud de Datos | Especificaciones de longitud (si aplica) |
| Marcador Unsigned | Restricciones unsigned |
| Nulabilidad | Especificaciones NULL/NOT NULL |
| Valores Predeterminados | Expresiones de valores predeterminados |
| Claves Primarias | Definiciones actualizadas de clave primaria |

Otras propiedades que pueden especificarse durante ALTER TABLE ADD COLUMN no están soportadas en este momento.

### ALTER TABLE DROP COLUMN
Captura:
- Lista de nombres de columnas a eliminar

### ALTER TABLE ALTER COLUMN
Modificaciones soportadas:

| Modificación | Descripción |
|--------------|-------------|
| Tipo de Dato | Cambiar tipo de dato de la columna |
| Longitud de Tipo | Modificar longitud/precisión del tipo |
| Valor Predeterminado | Alterar/eliminar valores predeterminados |
| NOT NULL | Modificar/eliminar restricción NOT NULL |

Otras propiedades que pueden especificarse durante ALTER TABLE ALTER COLUMN no están soportadas en este momento.

Por favor, tenga en cuenta que SynchDB solo soporta cambios básicos de tipo de datos en una columna existente. Por ejemplo, `INT` → `BIGINT` o `VARCHAR` → `TEXT`. Cambios complejos de tipo de datos como `TEXT` → `INT` o `INT` → `TIMESTAMP` no están soportados actualmente. Esto se debe a que PostgreSQL requiere que el usuario proporcione adicionalmente una función de conversión de tipo para realizar la conversión como resultado del cambio complejo de tipo de datos. SynchDB actualmente no tiene conocimiento de qué funciones de conversión usar para conversiones específicas de tipo. En el futuro, podríamos permitir que el usuario proporcione sus propias funciones de conversión para usar en conversiones específicas de tipo a través del archivo de reglas, pero por ahora, no está soportado.

## Comportamiento Específico por Base de Datos
### Eventos DDL en MySQL
Dado que MySQL registra tanto operaciones DDL como DML en el binlog, SynchDB puede replicar tanto DDLs como DMLs según ocurren. No se necesitan acciones especiales en el lado de MySQL para habilitar la replicación de DDLs.

### Eventos DDL en SQLServer
SQLServer no soporta nativamente la replicación DDL en modo streaming. El esquema de tabla es construido por SynchDB durante la fase de construcción del snapshot inicial cuando el conector se inicia por primera vez. Después de esta fase, SynchDB intentará detectar cualquier cambio de esquema, pero necesitan ser agregados explícitamente a la lista de tablas CDC de SQL Server.

#### Activar evento CREATE TABLE en SQLServer
Para crear una nueva tabla en SQL Server y agregarla a su lista de tablas CDC:
```sql
CREATE TABLE dbo.altertest (
	a INT,
	b TEXT
	);
GO

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
	@source_name = 'altertest',
	@role_name = NULL,
	@supports_net_changes = 0,
	@capture_instance = 'dbo_altertest_1'
GO
```

El comando agrega la tabla `dbo.altertest` a la lista de tablas CDC y causará que SynchDB reciba un evento DDL CREATE TABLE.

#### Activar Eventos ALTER TABLE
Si se altera una tabla existente (agregar, eliminar o modificar columna), necesita ser actualizada explícitamente en la lista de tablas CDC de SQLServer, para que SynchDB pueda recibir los eventos ALTER TABLE.

Por ejemplo:

Alterar una tabla en SQLServer:
```sql
ALTER TABLE altertest ADD c NVARCHAR(MAX),
	d INT DEFAULT 0 NOT NULL,
	e NVARCHAR(255) NOT NULL,
	f INT DEFAULT 5 NOT NULL,
	CONSTRAINT PK_altertest PRIMARY KEY (
	d,
	f
	);
GO
```

Deshabilitar la instancia de captura anterior:
```sql
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo',
	@source_name = 'altertest',
	@capture_instance = 'dbo_altertest_1';
GO
```

Habilitar como una nueva instancia de captura:
```sql
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
	@source_name = 'altertest',
	@role_name = NULL,
	@supports_net_changes = 0,
	@capture_instance = 'dbo_altertest_2';
GO
```

Agregar nuevo registro:
```sql
INSERT INTO altertest VALUES(1, 's', 'c', 1, 'v', 5);
GO
```

El ejemplo anterior debería permitir que SynchDB reciba un evento ALTER TABLE ADD COLUMN y un evento INSERT. No es necesario reiniciar SQL Server ni SynchDB para capturar estos eventos. Lo mismo aplica para los eventos ALTER TABLE DROP COLUMN y ALTER TABLE ALTER COLUMN también.