# Manejo de Cambios por Lotes

## Descripción General
SynchDB obtiene periódicamente un lote de solicitudes de cambios del motor Debezium runner en un período de `synchdb.naptime` milisegundos (por defecto 500). Este lote de solicitudes de cambios es luego procesado por SynchDB. Si todas las solicitudes de cambios dentro del lote han sido procesadas exitosamente (analizadas, transformadas y aplicadas a PostgreSQL), SynchDB notificará al motor Debezium runner que este lote ha sido completado. Esto señala a Debezium runner que debe confirmar el offset hasta el último registro de cambio completado con éxito. Con este mecanismo implementado, SynchDB puede rastrear cada registro de cambio e instruir a Debezium runner para que no obtenga un cambio antiguo que ya haya sido procesado, o no envíe un registro de cambio duplicado.

## Manejo de Lotes en Caso de Éxito
Si todas las solicitudes de cambios dentro de un lote han sido procesadas exitosamente, SynchDB simplemente envía un mensaje al motor Debezium runner para marcar el lote como procesado y completado, lo que causa que los offsets sean confirmados y finalmente escritos en disco.

![img](https://www.highgo.ca/wp-content/uploads/2024/10/synchdb-Page-4.drawio.png)

## Manejo de Lotes en Caso de Éxito Parcial
En el caso de que una de las solicitudes de cambios haya fallado en procesarse debido a errores internos de PostgreSQL, como violación de clave duplicada, SynchDB aún notificará al motor Debezium runner sobre un lote parcialmente completado. Este mensaje de notificación contiene un indicador sobre el último registro procesado exitosamente. Debezium entonces marcará todos los registros completados como procesados, mientras que dejará los registros no procesados y fallidos como están. Con esto, cuando SynchDB y Debezium runner reinicien, reanudarán desde el último registro de cambio fallido y solo continuarán cuando la falla haya sido resuelta.

![img](https://www.highgo.ca/wp-content/uploads/2024/10/synchdb-Page-5.drawio.png)