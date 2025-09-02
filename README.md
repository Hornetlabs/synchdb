## Introduction

SynchDB is a PostgreSQL extension designed for fast and reliable data replication from multiple heterogeneous databases into PostgreSQL. It eliminates the need for middleware or third-party software, enabling direct synchronization without additional orchestration.

SynchDB is a dual-language module, combining Java (for utilizing Debezium Embedded connectors) and C (for interacting with PostgreSQL core) components, and requires the Java Virtual Machine (JVM) and Java Native Interface (JNI) to operate together.

### Supported PostgreSQL Versions
* PostgreSQL: 16, 17
* IvorySQL: 4

### Supported Source Databases
* MySQL: 8.0.x, 8.2
* SQL Server: 2017, 2019, 2022
* Oracle: 12c, 19c, 21c, 23ai
* Openlog Replicator: 1.3.0

Visit SynchDB documentation site [here](https://docs.synchdb.com/) for more design details.

## Architecture
SynchDB extension consists of these major components:
* Debezium Runner (Java) - Responsible for connecting to source databases and get change events.
* SynchDB Worker - Responsible for polling change events from Debezium Runner via JNI.
* Event Processor - Reponsible for processing raw events into internal structures.
* Data Converter - Responsible for transforming data values.
* Replication Agent - Responsible for applying changes to PostgreSQL.

![img](https://www.highgo.ca/wp-content/uploads/2025/08/synchdb-arch-2.jpg)

## Build Requirement
The following software is required to build and run SynchDB. The versions listed are the versions tested during development. Older versions may still work.
* Java Development Kit 17 or later. Download [here](https://www.oracle.com/ca-en/java/technologies/downloads/)
* Apache Maven 3.6.3 or later. Download [here](https://maven.apache.org/download.cgi)
* PostgreSQL source or build enviornment. Git clone [here](https://github.com/postgres/postgres). Refer to this [wiki](https://wiki.postgresql.org/wiki/Compile_and_Install_from_source_code) to build PostgreSQL from source or this [page](https://www.postgresql.org/download/linux/) to install PostgreSQL via packages
* Docker compose 2.28.1 (for testing). Refer to [here](https://docs.docker.com/compose/install/linux/)
* Unix based operating system like Ubuntu 22.04 or MacOS

**Additional Requirement for Openlog Replicator Connector Support (if enabled in build)**

* libprotobuf-c v1.5.2. Refer to [here](https://github.com/protobuf-c/protobuf-c.git) to build from source.

## Build Procedure
### Prepare Source (Using 16.3 as example)

Clone the PostgreSQL source and switch to 16.3 release tag
``` BASH
git clone https://github.com/postgres/postgres.git --branch REL_16_3
cd postgres
```

Clone the SynchDB source from within the extension folder
``` BASH
cd contrib/
git clone https://github.com/Hornetlabs/synchdb.git
```

### Prepare Tools & Dependencies
#### --> Maven
``` BASH
## on Ubuntu
sudo apt install maven

## on MacOS
brew install maven
```

#### --> Java (OpenJDK)
If you are working on Ubuntu 22.04.4 LTS, install the OpenJDK  as below:
``` BASH
## on Ubuntu
sudo apt install openjdk-21-jdk

## on MacOS
brew install openjdk@22
```

#### --> libprotobuf-c (optional)
This library is needed if SynchDB is to be built with openlog replicator support.

``` BASH
cd /home/$USER/
git clone https://github.com/protobuf-c/protobuf-c.git
cd /home/$USER/protobuf-c
./configure
make
sudo make install
```

### Build and Install PostgreSQL from Source
This can be done by following the standard build and install procedure as described [here](https://www.postgresql.org/docs/current/install-make.html). Generally, it consists of:

``` BASH
cd /home/$USER/postgres
./configure
make
sudo make install
```

You should build and install the default extensions as well:
``` BASH
cd /home/$USER/postgres/contrib
make
sudo make install
```

### Build SynchDB Main Components

#### --> Build Debezium Runner Engine
The commands below build and install the Debezium Runner Engine jar file to your PostgreSQL's lib folder.

``` BASH
cd /home/$USER/postgres/contrib/synchdb
make build_dbz
sudo make install_dbz
```

#### --> Build Oracle Parser (optional)
This Oracle parser (a shared library) is a modified and isoalted version of IvorySQL's Oracle parser required by openlog replicator to process incoming Oracle DDL statements. The commands below install Oracle Parser to your PostgreSQL's lib folder.

Required if SynchDB is built with openlog replicator support.

```BASH
cd /home/$USER/postgres/contrib/synchdb
make clean_oracle_parser
make oracle_parser
sudo make install_oracle_parser
```

#### --> Build SynchDB
The commands below build and install SynchDB extension to your PostgreSQL's lib and share folder.

``` BASH
cd /home/$USER/postgres/contrib/synchdb
make
sudo make install
```

SynchDB can be built with additional Openlog Replicator Connector support

```BASH
cd /home/$USER/postgres/contrib/synchdb
make WITH_OLR=1 clean
make WITH_OLR=1
sudo make WITH_OLR=1 install
```

### Configure your Linker to find Java (Ubuntu)
Lastly, we also need to tell your system's linker where the newly added Java library (libjvm.so) is located in your system.

``` BASH
# Dynamically set JDK paths
JAVA_PATH=$(which java)
JDK_HOME_PATH=$(readlink -f ${JAVA_PATH} | sed 's:/bin/java::')
JDK_LIB_PATH=${JDK_HOME_PATH}/lib

echo $JDK_LIB_PATH
echo $JDK_LIB_PATH/server

sudo echo "$JDK_LIB_PATH" ｜ sudo tee -a /etc/ld.so.conf.d/x86_64-linux-gnu.conf
sudo echo "$JDK_LIB_PATH/server" | sudo tee -a /etc/ld.so.conf.d/x86_64-linux-gnu.conf
```
Note, for mac with M1/M2 chips, the linker file is located in /etc/ld.so.conf.d/aarch64-linux-gnu.conf

Run ldconfig to reload:
``` BASH
sudo ldconfig
```

Ensure synchdo.so extension can link to libjvm Java library on your system:
``` BASH
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

If SynchDB is built with openlog replicator support, ensure it can link to libprotobuf-c library on your system:
``` BASH
ldd synchdb.so
        linux-vdso.so.1 (0x00007ffde6ba5000)
        libjvm.so => /home/ubuntu/java/jdk-22.0.1/lib/server/libjvm.so (0x00007f3c8e191000)
        libprotobuf-c.so.1 => /usr/local/lib/libprotobuf-c.so.1 (0x00007f3c8e186000)
        libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f3c8df5d000)
        libdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007f3c8df58000)
        libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007f3c8df53000)
        librt.so.1 => /lib/x86_64-linux-gnu/librt.so.1 (0x00007f3c8df4c000)
        libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007f3c8de65000)
        /lib64/ld-linux-x86-64.so.2 (0x00007f3c8f69e000)
```

## Run SynchDB Test
Refer to `testenv/README.md` or visit our documentation page [here](https://docs.synchdb.com/user-guide/prepare_tests_env/) to prepare a sample MySQL, SQL Server or Oracle database instances for testing.

SynchDB extension requires pgcrypto to encrypt certain sensitive credential data. Please make sure it is installed prior to installing SynchDB. Alternatively, you can include `CASCADE` clause in `CREATE EXTENSION` to automatically install dependencies:

``` SQL
CREATE EXTENSION synchdb CASCADE;
```

### Create a Connector
A connector represents the details to connecto to a remote heterogeneous database and describes what tables to replicate from. It can be created with `synchdb_add_conninfo()` function.

Create a MySQL connector and replicate `inventory.orders` and `inventory.customers` tables under `invnetory` database:
``` SQL
SELECT synchdb_add_conninfo('mysqlconn','127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', 'inventory.orders,inventory.customers', 'null', 'mysql');
```

Create a SQL Server connector and replicate from all tables under `testDB` database.
```SQL
SELECT synchdb_add_conninfo('sqlserverconn','127.0.0.1', 1433, 'sa', 'Password!', 'testDB', 'postgres', 'null', 'null', 'sqlserver');
```

Create a Oracle connector and replicate from all tables under `mydb` database.
```SQL
SELECT synchdb_add_conninfo('oracleconn','127.0.0.1', 1521, 'c##dbzuser', 'dbz', 'mydb', 'postgres', 'null', 'null', 'oracle');
```

Create a Openlog Replicator Connector (if supported) and replicate from all tables under `mydb` database. Note: We need to specify connection parameters to Oracle via `synchdb_add_conninfo()` and also the connection parameters to Openlog Replicator via `synchdb_add_olr_conninfo()`.
```SQL
SELECT synchdb_add_conninfo('olrconn','127.0.0.1', 1521, 'DBZUSER', 'dbz', 'mydb', 'postgres', 'null', 'null', 'olr');
SELECT synchdb_add_olr_conninfo('olrconn', '127.0.0.1', 7070, 'ORACLE');
```

### Review all Connectors Created
All created connectors are stored in the `synchdb_conninfo` table. Please note that user passwords are encrypted by SynchDB on creation so please do not modify the password field directly. 

``` SQL
postgres=# \x
Expanded display is on.

postgres=# select * from synchdb_conninfo;
-[ RECORD 1 ]-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name     | oracleconn
isactive | f
data     | {"pwd": "\\xc30d04070302e3baf1293d0d553066d234014f6fc52e6eea425884b1f65f1955bf504b85062dfe538ca2e22bfd6db9916662406fc45a3a530b7bf43ce4cfaa2b049a1c9af8", "port": 1521, "user": "c##dbzuser", "dstdb": "postgres", "srcdb": "mydb", "table": null, "snapshottable": null, "hostname": "127.0.0.1", "connector": "oracle"}
-[ RECORD 2 ]-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name     | sqlserverconn
isactive | t
data     | {"pwd": "\\xc30d0407030245ca4a983b6304c079d23a0191c6dabc1683e4f66fc538db65b9ab2788257762438961f8201e6bcefafa60460fbf441e55d844e7f27b31745f04e7251c0123a159540676c4", "port": 1433, "user": "sa", "dstdb": "postgres", "srcdb": "testDB", "table": null, "snapshottable": null, "hostname": "127.0.0.1", "connector": "sqlserver"}
-[ RECORD 3 ]-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name     | mysqlconn
isactive | t
data     | {"pwd": "\\xc30d04070302986aff858065e96b62d23901b418a1f0bfdf874ea9143ec096cd648a1588090ee840de58fb6ba5a04c6430d8fe7f7d466b70a930597d48b8d31e736e77032cb34c86354e", "port": 3306, "user": "mysqluser", "dstdb": "postgres", "srcdb": "inventory", "table": "inventory.orders,inventory.customers", "snapshottable": null, "hostname": "127.0.0.1", "connector": "mysql"}
-[ RECORD 4 ]-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name     | olrconn
isactive | t
data     | {"pwd": "\\xc30d04070302ab2b11bb2c9ece2f7dd2340138f4f541e38fd92119799a4e444b2dcbce9d702efe4f7e7337e7fb11383a577c0f0d3abe0c63022c9d79a6573fcbdbd15e12ba", "port": 1521, "user": "DBZUSER", "dstdb": "postgres", "srcdb": "FREE", "table": null, "hostname": "10.55.13.17", "olr_host": "10.55.13.17", "olr_port": 7070, "connector": "olr", "olr_source": "ORACLE", "snapshottable": null}

```

### Start a Connector
A connector can be started by `synchdb_start_engine_bgw()` function. It takes one connector name argument which must have been created by `synchdb_add_conninfo()` prior to starting. The commands below starts a connector in default snapshot mode, which replicates designated table schemas, their initial data and proceed to stream live changes (CDC).

``` SQL
select synchdb_start_engine_bgw('mysqlconn');
select synchdb_start_engine_bgw('oracleconn');
select synchdb_start_engine_bgw('sqlserverconn');
select synchdb_start_engine_bgw('olrconn');
```

### View Connector Running States
Use `synchdb_state_view()` to examine all connectors' running states.

``` SQL
postgres=# select * from synchdb_state_view;
     name      | connector_type     |  pid   |        stage        |  state  |   err    |                                           last_dbz_offset
---------------+--------------------+--------+---------------------+---------+----------+------------------------------------------------------------------------------------------------------
 sqlserverconn | sqlserver          | 579820 | change data capture | polling | no error | {"commit_lsn":"0000006a:00006608:0003","snapshot":true,"snapshot_completed":false}
 mysqlconn     | mysql              | 579845 | change data capture | polling | no error | {"ts_sec":1741301103,"file":"mysql-bin.000009","pos":574318212,"row":1,"server_id":223344,"event":2}
 oracleconn    | oracle             | 580053 | change data capture | polling | no error | offset file not flushed yet
 olrconn       | openlog_replicator | 121673 | change data capture | polling | no error | {"scn":8367394, "c_scn":8367394}
(4 rows)

```

### Stop a Connector
Use `synchdb_stop_engine_bgw()` SQL function to stop a connector.It takes one connector name argument which must have been created by `synchdb_add_conninfo()` function.

``` SQL
select synchdb_stop_engine_bgw('mysqlconn');
select synchdb_stop_engine_bgw('sqlserverconn');
select synchdb_stop_engine_bgw('oracleconn');
select synchdb_stop_engine_bgw('olrconn');
```
