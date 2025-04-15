[![build_and_test](https://github.com/Hornetlabs/synchdb/actions/workflows/synchdb-ci.yml/badge.svg)](https://github.com/Hornetlabs/synchdb/actions/workflows/synchdb-ci.yml)

## Introduction

SynchDB is a PostgreSQL extension designed for fast and reliable data replication from multiple heterogeneous databases (including MySQL, SQL Server, and Oracle) into PostgreSQL. It eliminates the need for middleware or third-party software, enabling direct synchronization without additional orchestration.

SynchDB is a dual-language module, combining Java (for utilizing Debezium Embedded connectors) and C (for interacting with PostgreSQL core) components, and requires the Java Virtual Machine (JVM) and Java Native Interface (JNI) to operate together.

Visit SynchDB documentation site [here](https://docs.synchdb.com/) for more design details.

## Architecture
SynchDB extension consists of six major components:
* Debezium Runner Engine (Java)
* SynchDB Launcher
* SynchDB Worker
* Format Converter
* Replication Agent
* Table Synch Agent (TBD)

![img](https://www.highgo.ca/wp-content/uploads/2024/07/synchdb.drawio.png)

### Debezium Runner Engine (Java)
* A java application that drives Debezium Embedded engine and utilizes all of the database connectors that it supports.
* Invoked by `SynchDB Worker` to initialize Debezium embedded library and receive change data.
* Send the change data to `SynchDB Worker` in generalized JSON format for further processing.

### SynchDB Launcher
* Responsible for creating and destroying SynchDB workers using PostgreSQL's background worker APIs.
* Configure each worker's connector type, destination database IPs, ports, etc.

### SynchDB Worker
* Instantiats a `Debezium Runner Engine` inside a Java Virtual Machine (JVM) to replicate changes from a specific connector type.
* Communicate with Debezium Runner via JNI to receive change data in JSON formats.
* Transfer the JSON change data to `Format Converter` module for further processing.

### Format Converter
* Parse the JSON change data using PostgreSQL Jsonb APIs
* Transform DDL queries to PostgreSQL compatible queries and translate heterogeneous column data type to PostgreSQL compatible types.
* Transform DML queries to PostgreSQL compatible queries and handle data processing based on column data types
* Produces raw HeapTupleData which can be fed directly to Heap Access Method within PostgreSQL for faster executions.

### Replication Agent
* Processes the outputs from **`Format Converter`**.
* **`Format Converter`** will produce HeapTupleData format outputs, then **`Replication Agent`** will invoke PostgreSQL's heap access method routines to handle them.

### Table Sync Agent
* Design details and implementation are not available yet. TBD
* Intended to provide a more efficient alternative to perform initial table synchronization.

## Build Requirement
The following software is required to build and run SynchDB. The versions listed are the versions tested during development. Older versions may still work.
* Java Development Kit 17 or later. Download [here](https://www.oracle.com/ca-en/java/technologies/downloads/)
* Apache Maven 3.6.3 or later. Download [here](https://maven.apache.org/download.cgi)
* PostgreSQL 16.3 Source. Git clone [here](https://github.com/postgres/postgres). Refer to this [wiki](https://wiki.postgresql.org/wiki/Compile_and_Install_from_source_code) for PostgreSQL build requirements
* Docker compose 2.28.1 (for testing). Refer to [here](https://docs.docker.com/compose/install/linux/)
* Unix based operating system like Ubuntu 22.04 or MacOS

## Build Procedure
### Prepare Source

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

### Prepare Tools
#### Maven
If you are working on Ubuntu 22.04.4 LTS, install the Maven as below:
``` BASH
sudo apt install maven
```

if you are using MacOS, you can use the brew command to install maven (refer (here)[https://brew.sh/] for how to install Homebrew) without any other settings:
``` BASH
brew install maven
```

#### Install Java (OpenJDK)
If you are working on Ubuntu 22.04.4 LTS, install the OpenJDK  as below:
``` BASH
sudo apt install openjdk-21-jdk
```

If you are working on MacOS, please install the JDK with brew command:
``` BASH
brew install openjdk@22
```

### Build and Install PostgreSQL
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

### Build Debezium Runner Engine
The commands below build and install the Debezium Runner Engine jar file to your PostgreSQL's lib folder.

``` BASH
cd /home/$USER/postgres/contrib/synchdb
make build_dbz
sudo make install_dbz
```

### Build SynchDB PostgreSQL Extension
The commands below build and install SynchDB extension to your PostgreSQL's lib and share folder.

``` BASH
cd /home/$USER/postgres/contrib/synchdb
make
sudo make install
```

### Configure your Linker (Ubuntu)
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
Note, for mac with M1/M2 chips, you need to the two lines into /etc/ld.so.conf.d/aarch64-linux-gnu.conf

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
SELECT synchdb_add_conninfo('mysqlconn','127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', 'inventory.orders,inventory.customers', 'mysql');
```

Create a SQL Server connector and replicate from all tables under `testDB` database.
```SQL
SELECT synchdb_add_conninfo('sqlserverconn','127.0.0.1', 1433, 'sa', 'Password!', 'testDB', 'postgres', '', 'sqlserver');
```

Create a Oracle connector and replicate from all tables under `mydb` database.
```SQL
SELECT synchdb_add_conninfo('oracleconn','127.0.0.1', 1521, 'c##dbzuser', 'dbz', 'mydb', 'postgres', '', 'oracle');
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
data     | {"pwd": "\\xc30d04070302e3baf1293d0d553066d234014f6fc52e6eea425884b1f65f1955bf504b85062dfe538ca2e22bfd6db9916662406fc45a3a530b7bf43ce4cfaa2b049a1c9af8", "port": 1521, "user": "c##dbzuser", "dstdb": "postgres", "srcdb": "mydb", "table": "null", "hostname": "127.0.0.1", "connector": "oracle"}
-[ RECORD 2 ]-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name     | sqlserverconn
isactive | t
data     | {"pwd": "\\xc30d0407030245ca4a983b6304c079d23a0191c6dabc1683e4f66fc538db65b9ab2788257762438961f8201e6bcefafa60460fbf441e55d844e7f27b31745f04e7251c0123a159540676c4", "port": 1433, "user": "sa", "dstdb": "postgres", "srcdb": "testDB", "table": "null", "hostname": "127.0.0.1", "connector": "sqlserver"}
-[ RECORD 3 ]-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name     | mysqlconn
isactive | t
data     | {"pwd": "\\xc30d04070302986aff858065e96b62d23901b418a1f0bfdf874ea9143ec096cd648a1588090ee840de58fb6ba5a04c6430d8fe7f7d466b70a930597d48b8d31e736e77032cb34c86354e", "port": 3306, "user": "mysqluser", "dstdb": "postgres", "srcdb": "inventory", "table": "inventory.orders,inventory.customers", "hostname": "127.0.0.1", "connector": "mysql"}

```

### Start a Connector
A connector can be started by `synchdb_start_engine_bgw()` function. It takes one connector name argument which must have been created by `synchdb_add_conninfo()` prior to starting. The commands below starts a connector in default snapshot mode, which replicates designated table schemas, their initial data and proceed to stream live changes (CDC).

``` SQL
select synchdb_start_engine_bgw('mysqlconn');
select synchdb_start_engine_bgw('oracleconn');
select synchdb_start_engine_bgw('sqlserverconn');
```

### View Connector Running States
Use `synchdb_state_view()` to examine all connectors' running states.

``` SQL
postgres=# select * from synchdb_state_view;
     name      | connector_type |  pid   |        stage        |  state  |   err    |                                           last_dbz_offset
---------------+----------------+--------+---------------------+---------+----------+------------------------------------------------------------------------------------------------------
 sqlserverconn | sqlserver      | 579820 | change data capture | polling | no error | {"commit_lsn":"0000006a:00006608:0003","snapshot":true,"snapshot_completed":false}
 mysqlconn     | mysql          | 579845 | change data capture | polling | no error | {"ts_sec":1741301103,"file":"mysql-bin.000009","pos":574318212,"row":1,"server_id":223344,"event":2}
 oracleconn    | oracle         | 580053 | change data capture | polling | no error | offset file not flushed yet
(3 rows)

```

### Stop a Connector
Use `synchdb_stop_engine_bgw()` SQL function to stop a connector.It takes one connector name argument which must have been created by `synchdb_add_conninfo()` function.

``` SQL
select synchdb_stop_engine_bgw('mysqlconn');
select synchdb_stop_engine_bgw('sqlserverconn');
select synchdb_stop_engine_bgw('oracleconn');
```
