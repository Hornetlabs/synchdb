# Bienvenido a SynchDB

SynchDB es una extensión de PostgreSQL para sincronizar datos de diferentes fuentes de bases de datos.

## Introducción

SynchDB es una extensión de PostgreSQL diseñada para replicar datos de una o más bases de datos heterogéneas (como MySQL, MS SQLServer, Oracle, etc.) directamente a PostgreSQL de una manera rápida y confiable. PostgreSQL sirve como destino de múltiples fuentes de bases de datos heterogéneas. No se requiere ningún middleware ni software de terceros para orquestar la sincronización de datos entre bases de datos heterogéneas y PostgreSQL. La extensión SynchDB en sí es capaz de manejar todas las necesidades de sincronización de datos.

Proporciona dos modos de trabajo clave que se pueden invocar utilizando las funciones SQL integradas:
* Modo de sincronización (para la sincronización de datos inicial)
* Modo de seguimiento (para replicar cambios incrementales después de la sincronización inicial)

El **modo de sincronización** copia tablas de bases de datos heterogéneas a PostgreSQL, incluido su esquema, índices, activadores, otras propiedades de tabla, así como los datos actuales que contiene.
**El modo de seguimiento** se suscribe a las tablas de una base de datos heterogénea para obtener cambios incrementales y aplicarlos a las mismas tablas en PostgreSQL, de forma similar a la replicación lógica de PostgreSQL

## Características

- Sincronización de datos eficiente
- Compatibilidad con múltiples fuentes de bases de datos
- Integración sencilla con bases de datos PostgreSQL existentes

## Primeros pasos

Consulte nuestra [Guía de instalación](user-guide/installation) para comenzar a utilizar SynchDB.
