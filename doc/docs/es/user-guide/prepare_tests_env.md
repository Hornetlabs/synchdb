---
weight: 20
---
# Entorno de prueba rápida

Los procedimientos mencionados aquí están destinados a iniciar rápidamente bases de datos heterogéneas para verificación rápida y demostración de funcionalidades. Los archivos y scripts necesarios para iniciar bases de datos heterogéneas de ejemplo se pueden encontrar en el repositorio de SynchDB [aquí](https://github.com/Hornetlabs/synchdb/testenv/)

## Preparar una Base de Datos MySQL de Ejemplo
Podemos iniciar una base de datos MySQL de ejemplo para pruebas usando docker compose. Las credenciales de usuario están descritas en el archivo `synchdb-mysql-test.yaml`
```sh
docker compose -f synchdb-mysql-test.yaml up -d
```

Inicie sesión en MySQL como `root` y otorgue permisos al usuario `mysqluser` para realizar CDC en tiempo real
```sql
mysql -h 127.0.0.1 -u root -p

GRANT replication client on *.* to mysqluser;
GRANT replication slave  on *.* to mysqluser;
GRANT RELOAD ON *.* TO 'mysqluser'@'%';
FLUSH PRIVILEGES;
```

Salga de la herramienta cliente de mysql:
```
\q
```

## Preparar una Base de Datos SQL Server de Ejemplo
Podemos iniciar una base de datos SQL Server de ejemplo para pruebas usando docker compose. Las credenciales de usuario están descritas en el archivo `synchdb-sqlserver-test.yaml`
```
docker compose -f synchdb-sqlserver-test.yaml up -d
```
use el archivo synchdb-sqlserver-withssl-test.yaml para SQL Server con certificado SSL habilitado.

Es posible que no tenga instalada la herramienta cliente de SQL Server, puede iniciar sesión en el contenedor de SQL Server para acceder a su herramienta cliente.

Encuentre el ID del contenedor para SQL Server:
```
id=$(docker ps | grep sqlserver | awk '{print $1}')
```

Copie el esquema de la base de datos en el contenedor de SQL Server:
```
docker cp inventory.sql $id:/
```

Inicie sesión en el contenedor de SQL Server:
```
docker exec -it $id bash
```

Construya la base de datos según el esquema:
```
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -i /inventory.sql
```

Ejecute algunas consultas simples (agregue -N -C si está usando SQL Server con SSL habilitado):
```
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -Q "insert into orders(order_date, purchaser, quantity, product_id) values( '2024-01-01', 1003, 2, 107)"

/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -Q "select * from orders"
```