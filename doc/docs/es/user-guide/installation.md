---
weight: 10
---
# Instalación

## Requisitos
Se requiere el siguiente software para construir y ejecutar SynchDB. Las versiones listadas son las versiones probadas durante el desarrollo. Las versiones anteriores pueden seguir funcionando.
* Java Development Kit 22. Descargar [aquí](https://www.oracle.com/ca-en/java/technologies/downloads/)
* Apache Maven 3.9.8. Descargar [aquí](https://maven.apache.org/download.cgi)
* PostgreSQL 16.3 Source. Clonar Git [aquí](https://github.com/postgres/postgres). Consulte esta [wiki](https://wiki.postgresql.org/wiki/Compile_and_Install_from_source_code) para los requisitos de compilación de PostgreSQL
* Docker compose 2.28.1 (para pruebas). Consultar [aquí](https://docs.docker.com/compose/install/linux/)
* Sistema operativo basado en Unix como Ubuntu 22.04 o MacOS

## Preparar Fuente
Clone el código fuente de PostgreSQL y cambie a la etiqueta de la versión 16.3
```sh linenums="1"
git clone https://github.com/postgres/postgres.git
cd postgres
git checkout REL_16_3
```

Clone el código fuente de SynchDB desde dentro de la carpeta de extensiones
Nota: La rama *(synchdb-devel)[https://github.com/Hornetlabs/synchdb/tree/synchdb-devel]* se usa para desarrollo hasta ahora.
```sh linenums="1"
cd contrib/
git clone https://github.com/Hornetlabs/synchdb.git
```

## Preparar Herramientas
### Instalar Maven
Si está trabajando en Ubuntu 22.04.4 LTS, instale Maven como se muestra a continuación:
```sh
sudo apt install maven
```

Si está usando MacOS, puede usar el comando brew para instalar maven (consulte (aquí)[https://brew.sh/] para saber cómo instalar Homebrew) sin ninguna otra configuración:
```sh
brew install maven
```

### Instalar Java SDK (OpenJDK)
Si está trabajando en Ubuntu 22.04.4 LTS, instale OpenJDK como se muestra a continuación:
```sh
sudo apt install openjdk-21-jdk
```

Si está trabajando en MacOS, instale JDK con el comando brew:
```sh
brew install openjdk@22
```

## Compilar e Instalar PostgreSQL
Siga la documentación oficial de PostgreSQL [aquí](https://www.postgresql.org/docs/current/install-make.html) para compilar e instalar PostgreSQL desde el código fuente. Generalmente, el procedimiento consiste en:

```sh linenums="1"
cd /home/$USER/postgres
./configure
make
sudo make install
```

También debe compilar e instalar las extensiones predeterminadas:
```sh linenums="1"
cd /home/$USER/postgres/contrib
make
sudo make install
```

## Compilar e Instalar el Motor Debezium Runner
Con Java y Maven configurados, estamos listos para compilar el Motor Debezium Runner. Esto instala el archivo jar de Debezium Runner Engine en la carpeta lib de PostgreSQL.

```sh linenums="1"
cd /home/$USER/postgres/contrib/synchdb
make build_dbz
sudo make install_dbz
```

## Compilar e Instalar la Extensión PostgreSQL de SynchDB
Con la `lib` y el `include` de Java instalados en su sistema, SynchDB puede compilarse mediante:

```sh linenums="1"
cd /home/$USER/postgres/contrib/synchdb
make
sudo make install
```

## Configurar su Enlazador (Ubuntu)
Por último, también necesitamos indicarle al enlazador de su sistema dónde se encuentra la biblioteca Java recién agregada. El siguiente procedimiento está basado en Ubuntu 22.04.

```sh linenums="1"
# Configurar dinámicamente las rutas de JDK
JAVA_PATH=$(which java)
JDK_HOME_PATH=$(readlink -f ${JAVA_PATH} | sed 's:/bin/java::')
JDK_LIB_PATH=${JDK_HOME_PATH}/lib

echo $JDK_LIB_PATH
echo $JDK_LIB_PATH/server

sudo echo "$JDK_LIB_PATH" ｜ sudo tee -a /etc/ld.so.conf.d/x86_64-linux-gnu.conf
sudo echo "$JDK_LIB_PATH/server" | sudo tee -a /etc/ld.so.conf.d/x86_64-linux-gnu.conf
```
Nota: para mac con chips M1/M2, necesita agregar las dos líneas en /etc/ld.so.conf.d/aarch64-linux-gnu.conf
```sh linenums="1"
sudo echo "$JDK_LIB_PATH"       ｜ sudo tee -a /etc/ld.so.conf.d/aarch64-linux-gnu.conf
sudo echo "$JDK_LIB_PATH/server" | sudo tee -a /etc/ld.so.conf.d/aarch64-linux-gnu.conf
```

Ejecute ldconfig para recargar:
```sh
sudo ldconfig
```

## Verificar Instalación

Asegúrese de que la extensión synchdb.so pueda enlazarse con la biblioteca Java libjvm en su sistema:
```sh
ldd synchdb.so
        linux-vdso.so.1 (0x00007ffeae35a000)
        libjvm.so => /usr/lib/jdk-22.0.1/lib/server/libjvm.so (0x00007fc1276c1000)
        libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007fc127498000)
        libdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007fc127493000)
        libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007fc12748e000)
        librt.so.1 => /lib/x86_64-linux-gnu/librt.so.1 (0x00007fc127489000)
        libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007fc1273a0000)
        /lib64/ld-linux-x86-64.so.2 (0x00007fc128b81000)
```