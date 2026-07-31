---
weight: 110
---
# Referencia de Funciones
## Gestión de Conectores

### synchdb_add_conninfo

**Propósito**: Crea una nueva configuración de conector

**Parámetros**:

| Parámetro | Descripción | Requerido | Ejemplo | Notas |
|:-:|:-|:-:|:-|:-|
| `name` | Identificador único para este conector | ✓ | `'mysqlconn'` | Debe ser único entre todos los conectores |
| `hostname` | IP/nombre de host de la base de datos heterogénea | ✓ | `'127.0.0.1'` | Soporta IPv4, IPv6 y nombres de host |
| `port` | Número de puerto para la conexión | ✓ | `3306` | Por defecto: MySQL(3306), SQLServer(1433) |
| `username` | Nombre de usuario para autenticación | ✓ | `'mysqluser'` | Requiere permisos apropiados |
| `password` | Contraseña de autenticación | ✓ | `'mysqlpwd'` | Almacenada de forma segura |
| `source database` | Nombre de la base de datos origen | ✓ | `'inventory'` | Debe existir en el sistema origen |
| `destination database` | Base de datos PostgreSQL destino | ✓ | `'postgres'` | Debe existir en PostgreSQL |
| `table` | Patrón de especificación de tabla | ☐ | `'[db].[table]'` | Vacío = replicar todas las tablas |
| `connector` | Tipo de conector (`mysql`/`sqlserver`) | ✓ | `'mysql'` | Ver conectores soportados arriba |
| `rule file` | Reglas de traducción de tipos de datos | ☐ | `'myrule.json'` | Debe estar en el directorio $PGDATA |

**Ejemplos de Uso**:
```sql
-- Ejemplo MySQL
SELECT synchdb_add_conninfo(
    'mysqlconn',    -- Nombre del conector
    '127.0.0.1',    -- Host
    3306,           -- Puerto
    'mysqluser',    -- Usuario
    'mysqlpwd',     -- Contraseña
    'inventory',    -- BD origen
    'postgres',     -- BD destino
    '',             -- Tablas (vacío para todas)
    'mysql',        -- Tipo de conector
    'myrule.json'   -- Archivo de reglas
);

-- Ejemplo SQL Server
SELECT synchdb_add_conninfo(
    'sqlserverconn',
    '127.0.0.1',
    1433,
    'sa',
    'MyPassword123',
    'testDB',
    'postgres',
    'dbo.orders',   -- Tabla específica
    'sqlserver',
    'mssql_rules.json'
);
```

### Funciones Básicas de Control

#### synchdb_start_engine_bgw
**Propósito**: Inicia un conector
```sql
SELECT synchdb_start_engine_bgw('mysqlconn');
```

#### synchdb_pause_engine
**Propósito**: Detiene temporalmente un conector en ejecución
```sql
SELECT synchdb_pause_engine_bgw('mysqlconn');
```

#### synchdb_resume_engine
**Propósito**: Reanuda un conector pausado
```sql
SELECT synchdb_resume_engine('mysqlconn');
```

#### synchdb_stop_engine_bgw
**Propósito**: Termina un conector
```sql
SELECT synchdb_stop_engine('mysqlconn');
```

## Gestión de Estado

### synchdb_state_view
**Propósito**: Monitorea los estados y el estado de los conectores

```sql
SELECT * FROM synchdb_state_view();
```

**Campos de Retorno**:

| Campo | Descripción | Tipo |
|-|-|-|
| `id` | Identificador de slot del conector | Integer |
| `connector` | Tipo de conector (`mysql` o `sqlserver`) | Text |
| `conninfo_name` | Nombre del conector asociado | Text |
| `pid` | ID del proceso trabajador | Integer |
| `state` | Estado actual del conector | Text |
| `err` | Último mensaje de error | Text |
| `last_dbz_offset` | Último offset de Debezium registrado | JSON |

**Estados Posibles**:

- 🔴 `stopped` - Inactivo
- 🟡 `initializing` - Iniciando
- 🟠 `paused` - Pausado temporalmente
- 🟢 `syncing` - Sondeando activamente
- 🔵 `parsing` - Procesando eventos
- 🟣 `converting` - Transformando datos
- ⚪ `executing` - Aplicando cambios
- 🟤 `updating offset` - Actualizando punto de control
- 🟨 `restarting` - Reiniciando
- ⚫ `unknown` - Estado indeterminado

### synchdb_set_offset
**Propósito**: Configura una posición de inicio personalizada

**Ejemplo para MySQL**:
```sql
SELECT synchdb_set_offset(
    'mysqlconn', 
    '{"ts_sec":1725644339,"file":"mysql-bin.000004","pos":138466,"row":1,"server_id":223344,"event":2}'
);
```

**Ejemplo para SQL Server**:
```sql
SELECT synchdb_set_offset(
    'sqlserverconn',
    '{"event_serial_no":1,"commit_lsn":"00000100:00000c00:0003","change_lsn":"00000100:00000c00:0002"}'
);
```

## Gestión de Instantáneas

### synchdb_restart_connector
**Propósito**: Reinicializa un conector con un modo de instantánea específico

**Modos de Instantánea**:

| Modo | Descripción | Caso de Uso |
|:-:|-|-|
| `always` | Realiza una instantánea completa en cada inicio | Verificación completa de datos |
| `initial` | Solo instantánea inicial | Operaciones normales |
| `initial_only` | Una única instantánea, luego se detiene | Migración de datos |
| `no_data` | Solo estructura, sin datos | Sincronización de esquema |
| `never` | Omite instantánea, solo transmite (solo MySQL) | Actualizaciones en tiempo real |
| `when_needed` | Instantánea condicional | Recuperación automática |

**Ejemplo**:
```sql
-- Reiniciar con modo de instantánea específico
SELECT synchdb_restart_connector('mysqlconn', 'initial');
```

---
📝 **Notas Adicionales**:

- Siempre validar la configuración del conector antes de iniciar
- Monitorear los recursos del sistema durante operaciones de instantánea
- Respaldar la base de datos PostgreSQL de destino antes de operaciones importantes
- Probar la conectividad desde el servidor PostgreSQL a la base de datos origen
- Asegurar que la base de datos origen tenga los permisos requeridos configurados
- Se recomienda monitoreo regular de los registros de errores
