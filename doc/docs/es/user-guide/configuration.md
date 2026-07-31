---
weight: 40
---
# Configuración

SynchDB admite las siguientes variables GUC en postgresql.conf:

| Variable GUC | Tipo | Valor Predeterminado | Descripción |
|-|-|-|-|
| synchdb.naptime | integer | 500 | El retraso en milisegundos entre cada sondeo de datos del motor Debezium runner |
| synchdb.dml_use_spi | boolean | false | Opción para usar SPI en el manejo de operaciones DML |
| synchdb.synchdb_auto_launcher | boolean | true | Opción para lanzar automáticamente los trabajadores del conector SynchDB activos. Esta opción solo funciona cuando SynchDB está incluido en la opción GUC `shared_preload_library` |

## Notas Técnicas

- Las variables GUC (Grand Unified Configuration) son parámetros de configuración global en PostgreSQL
- Los valores se configuran en el archivo `postgresql.conf`
- Los cambios requieren un reinicio del servidor para tomar efecto
- `shared_preload_library` es una configuración crítica del sistema que determina qué bibliotecas se cargan al inicio

## Ejemplos de Configuración

```conf
# Ejemplo de configuración en postgresql.conf
synchdb.naptime = 1000               # Aumentar el tiempo de espera a 1 segundo
synchdb.dml_use_spi = true           # Habilitar el uso de SPI para operaciones DML
synchdb.synchdb_auto_launcher = true # Habilitar el inicio automático del conector
```

## Recomendaciones de Uso

1.  **synchdb.naptime**
    - Valores más bajos: Mayor frecuencia de actualización pero más carga del sistema
    - Valores más altos: Menor carga del sistema pero actualizaciones menos frecuentes
    - Ajustar según los requisitos de latencia de datos
3.  **synchdb.dml_use_spi**
    - Habilitar si se necesita integración específica con SPI
    - Mantener en `false` para operaciones DML estándar
4.  **synchdb.synchdb_auto_launcher**
    - Recomendado mantener en `true` para gestión automática
    - Cambiar a `false` solo si se requiere control manual de los conectores

## Consideraciones de Rendimiento
- Ajustar `naptime` según la carga del sistema y requisitos de latencia
- Monitorear el rendimiento del sistema al modificar estas configuraciones
- Considerar el impacto en recursos del sistema al habilitar funciones adicionales