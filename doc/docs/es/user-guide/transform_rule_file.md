---
weight: 60
---
# Archivo de Reglas de Transformación

El archivo de reglas de transformación es un archivo de configuración adicional escrito en formato JSON que describe varias reglas de transformación que un conector SynchDB debe seguir al recibir datos de tabla desde una base de datos heterogénea remota. Este archivo debe colocarse en el directorio `$PGDATA` y se selecciona cuando se crea un conector utilizando la función SQL `synchdb_add_conninfo()`. Es posible no utilizar un archivo de reglas de transformación personalizado al crear un nuevo conector; en este caso, se aplicarán las reglas de transformación predeterminadas.

## Sample rule file
```json
{
  "transform_datatype_rules": [
    {
      "translate_from": "GEOMETRY",
      "translate_from_autoinc": false,
      "translate_to": "TEXT",
      "translate_to_size": -1
    },
    {
      "translate_from": "POINT",
      "translate_from_autoinc": false,
      "translate_to": "TEXT",
      "translate_to_size": -1
    },
    {
      "translate_from": "inventory.geom.g.GEOMETRY",
      "translate_from_autoinc": false,
      "translate_to": "GEOMETRY",
      "translate_to_size": 0
    },
    {
      "translate_from": "inventory.orders.quantity.INT",
      "translate_from_autoinc": false,
      "translate_to": "BIGINT",
      "translate_to_size": 0
    }
  ],
  "transform_objectname_rules": [
    {
      "object_type": "table",
      "source_object": "inventory.orders",
      "destination_object": "schema1.orders"
    },
    {
      "object_type": "table",
      "source_object": "inventory.products",
      "destination_object": "products"
    },
    {
      "object_type": "column",
      "source_object": "inventory.orders.order_number",
      "destination_object": "ididid"
    },
    {
      "object_type": "column",
      "source_object": "inventory.orders.purchaser",
      "destination_object": "the_dude"
    },
    {
      "object_type": "column",
      "source_object": "inventory.orders.quantity",
      "destination_object": "the_numba"
    },
    {
      "object_type": "column",
      "source_object": "testDB.dbo.customers.first_name",
      "destination_object": "the_awesome_first_name"
    }
  ],
  "transform_expression_rules": [
    {
      "transform_from": "inventory.orders.quantity",
      "transform_expression": "case when %d < 500 then 0 else %d end"
    },
    {
      "transform_from": "inventory.geom.g",
      "transform_expression": "ST_SetSRID(ST_GeomFromWKB(decode('%w', 'base64')),%s)"
    },
    {
      "transform_from": "inventory.products.name",
      "transform_expression": "'>>>>>' || '%d' || '<<<<<'"
    },
    {
      "transform_from": "inventory.products.description",
      "transform_expression": "'>>>>>' || '%d' || '<<<<<'"
    }
  ]
}
```
## Transform Data Type Rules

Las reglas de transformación de tipos de datos influyen en cómo SynchDB mapea un tipo de dato desde una base de datos heterogénea de origen a un tipo de dato equivalente en PostgreSQL. Estas reglas pueden escribirse para aplicarse a todas las tablas de origen o solo a tablas seleccionadas. Si no hay una regla de tipo de dato disponible para un tipo particular, SynchDB utilizará las [reglas de mapeo de tipos de datos predeterminadas](https://docs.synchdb.com/user-guide/default_datatype_mapping/).

Custom data type transform rules can be defined with a JSON array with name `"transform_datatype_rules"` and each element in the array must contain the following objects:

|                        	| Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             	| Example                                                                	|
|------------------------	|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|------------------------------------------------------------------------	|
| **translate_from**         	| the data type name or Fully Qualified Name (FQN) from heterogeneous database to transform from. The FQN consists of: [database].[schema (if present)].[table name].[column name].[data type]: <br><ul><li>If FQN is specified, then the transformation will only be applied to a specific table's specific column.</li><li> If FQN is not used, the transformation will be applied globally if data type name matches. </li></ul>                                                                                                  	| GEOMETRY<br>POINT<br>inventory.geom.g.GEOMETRY<br>inventory.orders.quantity.INT 	|
| **translate_from_autoinc** 	| indicate if the data type specified in `"translate_from"` is marked as auto increment.                                                                                                                                                                                                                                                                                                                                                                                                                  	| false<br>true                                                             	|
| **translate_to**           	| the PostgreSQL data type to translate to. See below for list of supported PostgreSQL data types. <br>It is possible to specify data types not natively supported by PostgreSQL, for example GEOMETRY, please ensure that the data type has already been installed before starting a connector using it.                                                                                                                                                                                                     	| TEXT<br>VARCHAR<br>BIGINT<br>GEOMETRY                                           	|
| **translate_to_size**      	| indicate if we should transform the size of the data type specifed in `"translate_to"`: <br><ul><li> 0: Remove any length specifier because the PostgreSQL data type transformed does not need length specifier. For example: TEXT </li><li> -1: Use whatever length specifier from the source heterogeneous database (if available) and map it directly to the translated datatype. For example: VARCHAR(32).</li><li> others: put any other values to force the size of translated data type. Use it with caution. </li></ul> 	| 0<br>-1<br>45<br>7000                                                           	|

### Tipos de Datos PostgreSQL Soportados
 
SynchDB es compatible con los siguientes tipos de datos nativos de PostgreSQL que pueden transformarse durante la creación de la tabla. Sin embargo, es posible transformar un tipo de dato externo a un tipo de dato PostgreSQL que no esté en la lista siguiente. Por ejemplo, el tipo de dato `GEOMETRY` añadido por PostGIS. En este caso, la tabla se crea con el tipo de dato `GEOMETRY`, pero los datos que SynchDB recibe se formatearán e insertarán como `TEXT`. Depende de usted decidir si estos datos necesitan ser procesados por una expresión o función SQL (ver reglas de expresión de transformación más abajo) antes de que puedan aplicarse a PostgreSQL.

* BOOLEAN (BOOLOID)
* BIGINT (INT8OID)
* SMALLINT (INT2OID)
* INT (INT4OID)
* INTEGER (INT4OID)
* DOUBLE PRECISION (FLOAT8OID)
* REAL (FLOAT4OID)
* MONEY (MONEYOID)
* NUMERIC (NUMERICOID)
* CHAR (BPCHAROID)
* CHARACTER (BPCHAROID)
* TEXT (TEXTOID)
* VARCHAR (VARCHAROID)
* CHARACTER VARYING (VARCHAROID)
* TIMESTAMPTZ (TIMESTAMPTZOID)
* JSONB (JSONBOID)
* UUID (UUIDOID)
* VARBIT (VARBITOID)
* BIT VARYING (VARBITOID)
* BIT (BITOID)
* DATE (DATEOID)
* TIMESTAMP (TIMESTAMPOID)
* TIME (TIMEOID)
* BYTEA (BYTEAOID)

## Reglas de Transformación de Nombres de Objetos

Las reglas de transformación de nombres de objetos determinan cómo SynchDB mapea un nombre de tabla o columna desde una base de datos heterogénea a nombres de tabla o columna de PostgreSQL.

Las reglas de transformación de tipos de datos se pueden definir con un array JSON con el nombre `"transform_objectname_rules"` y cada elemento en el array debe contener los siguientes objetos:

|                    	| description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  	| example                                        	|
|--------------------	|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|------------------------------------------------	|
| **object type**        	| el tipo de objeto de este elemento de transformación. Puede ser: <br><ul><li> `table` para indicar que la transformación se realizará en nombres de tablas. </li><li>`column` para indicar que la transformación se realizará en nombres de columnas. </li></ul>                                                                                                                                                                                  	| table<br>column                                   	|
| **source_object**      	| El Nombre Completamente Calificado (FQN) del objeto fuente desde la base de datos heterogénea. <br><ul><li> Si `object_type` es `table`, el FQN consiste en [database].[schema].[table] o [database].[table]. </li><li> Si `object_type` es `column`, el FQN consiste en [database].[schema].[table].[column] o [database].[table].[column]. </li></ul><br> Tenga en cuenta que algunas bases de datos heterogéneas (como SQLServer) son sensibles a mayúsculas y minúsculas con los nombres, así que asegúrese de que el FQN esté construido correctamente con las mayúsculas y minúsculas correctas. 	| false<br>true                                     	|
| **destination_object** 	| El nombre a utilizar en PostgreSQL para representar el `source_object` de la base de datos heterogénea: <br><ul><li>Si `object_type` es `table`, el `destination_object` puede contener un nombre de esquema opcional seguido de un nombre de tabla separado por un punto. Si no se indica esquema, se utilizará el esquema `public` por defecto.</li><li> Si `object_type` es `column`, el `destination_object` solo puede contener un nombre (sin esquema).                                                                                               	| myschema1.mytable1<br>mytable2<br>the_dude<br>the_numba 	|

Si un nombre de tabla o columna no tiene reglas de transformación de nombres de objetos coincidentes, la transformación predeterminada se aplicará automáticamente como se indica a continuación:

| Remote object FQN           	| Regla de transformación predeterminada de nombres de objetos en PostgreSQL                                                          	|
|-----------------------------	|--------------------------------------------------------------------------------------------------------------------------------	|
| [database].[table]          	| <ul><li> [database] → [schema]. </li><li> [table] → [table]. </li></ul>                                	|
| [database].[schema].[table] 	| <ul><li> [database] → [schema]. </li><li> [schema] es ignorado. </li><li> [table] → [table]. </li></ul> 	|


## Reglas de Expresión de Transformación

Las reglas de expresión de transformación indican a SynchDB si necesita realizar "expresiones" adicionales en los datos recibidos antes de aplicarlos a PostgreSQL. Esta característica permite a SynchDB cambiar la representación de los datos sin lógica adicional de aplicación en el lado de PostgreSQL.

Las reglas de tipos de datos de transformación se pueden definir con un array JSON con el nombre `"transform_expression_rules"` y cada elemento en el array debe contener los siguientes objetos:

|                      	| Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    	| Example                                                                                                                          	|
|----------------------	|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|----------------------------------------------------------------------------------------------------------------------------------	|
| **transform_from**      	| el Nombre Completamente Calificado (FQN) de una columna remota que puede tener uno de estos formatos:<br><ul><li> [database].[schema].[table].[column] </li><li> [database].[table].[column] </li></ul>                                                                                                                                                                                                                                                                                                      	| inventory.orders.quantity<br>testDB.dbo.products.description                                                                     	|
| **transform_expression** 	| la expresión a ejecutar en los datos recibidos. Puede usar estos tokens de marcador de posición para construir una expresión: <br><ul><li> %d: se reemplazará con los datos recibidos </li><li> %w: representación binaria conocida. Estará presente si los datos representan datos de geometría o geografía </li><li> %s: SRID. Estará presente si los datos representan datos de geometría o geografía </li></ul> la expresión puede escribirse en cualquier sintaxis SQL estándar soportada por PostgreSQL. 	| 1. ```case when %d < 500 then 0 else %d end``` <br> establece un valor a 0 si es menor que 500, de lo contrario mantiene su valor original <br><br> 2.```ST_SetSRID(ST_GeomFromWKB(decode('%w', 'base64')),%s)``` <br>*Convierte datos de geometría WKB codificados en base64 a un objeto de geometría PostGIS con un sistema de referencia espacial especificado (SRID)* <br><br>3. ```'>>>>>' \|\| '%d' \|\| '<<<<<'``` <br>agrega marcadores visuales alrededor de un valor|