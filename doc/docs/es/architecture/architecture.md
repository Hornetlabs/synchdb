# Arquitectura
## Diagrama de Arquitectura
La extensión SynchDB consta de seis componentes principales:
* Motor Debezium Runner (Java)
* Iniciador SynchDB
* Trabajador SynchDB
* Conversor de Formato
* Agente de Replicación
* Agente de Sincronización de Tablas (Por determinar)

Consulte el diagrama de arquitectura para una representación visual de los componentes y sus interacciones.
![img](https://www.highgo.ca/wp-content/uploads/2024/07/synchdb.drawio.png)

### Motor Debezium Runner (Java)
* Una aplicación Java que utiliza la biblioteca integrada de Debezium.
* Soporta diversas implementaciones de conectores para replicar datos de cambios desde varios tipos de bases de datos como MySQL, Oracle, SQL Server, etc.
* Es invocado por el `Trabajador SynchDB` para inicializar la biblioteca integrada de Debezium y recibir datos de cambios.
* Envía los datos de cambios al `Trabajador SynchDB` en formato JSON generalizado para su posterior procesamiento.

### Iniciador SynchDB
* Responsable de crear y destruir trabajadores SynchDB utilizando las APIs de trabajadores en segundo plano de PostgreSQL.
* Configura el tipo de conector, IPs de bases de datos de destino, puertos, etc. de cada trabajador.

### Trabajador SynchDB
* Instancia un `Motor Debezium Runner` para replicar cambios desde un tipo específico de conector.
* Se comunica con Debezium Runner a través de JNI para recibir datos de cambios en formatos JSON.
* Transfiere los datos de cambios JSON al módulo `Conversor de Formato` para su posterior procesamiento.

### Conversor de Formato
* Analiza los datos de cambios JSON utilizando las APIs Jsonb de PostgreSQL
* Transforma los detalles de cambios DDL en consultas SQL compatibles con PostgreSQL siguiendo reglas de traducción definidas por el usuario.
* Transforma los detalles de cambios DML en representación de datos compatible con PostgreSQL procesándolos según los tipos de datos de las columnas. Produce HeapTupleData sin procesar que puede alimentarse directamente al Método de Acceso Heap dentro de PostgreSQL para ejecuciones más rápidas.

### Agente de Replicación
* Procesa las salidas del **`Conversor de Formato`**.
* El **`Conversor de Formato`** producirá salidas en formato HeapTupleData, luego el **`Agente de Replicación`** invocará las rutinas del método de acceso heap de PostgreSQL para manejarlas.
* Para consultas DDL, el **`Agente de Replicación`** invocará el SPI de PostgreSQL para manejarlas.

### Agente de Sincronización de Tablas
* Los detalles de diseño e implementación aún no están disponibles. Por determinar
* Destinado a proporcionar una alternativa más eficiente para realizar la sincronización inicial de tablas.

## Interfaz Nativa de Java (JNI)
La Interfaz Nativa de Java (JNI) es un marco que permite a las aplicaciones Java interactuar con código nativo escrito en lenguajes como C o C++. Permite que los programas Java llamen y sean llamados por aplicaciones y bibliotecas nativas, proporcionando un puente entre la independencia de plataforma de Java y las ventajas de rendimiento del código nativo. JNI se utiliza comúnmente para integrar características específicas de la plataforma, optimizar secciones críticas de rendimiento de una aplicación, o acceder a bibliotecas heredadas que no están disponibles en Java. Requiere una gestión cuidadosa de los recursos, ya que implica cambiar entre la Máquina Virtual de Java (JVM) y entornos nativos, lo que puede introducir complejidad.

SynchDB requiere JNI para intercambiar recursos entre el motor Debezium runner y la extensión PostgreSQL de SynchDB. JNI está disponible con la Instalación de Java (ej. openjdk).