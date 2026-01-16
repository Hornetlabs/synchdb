---
weight: 10
---
# Installation

## **Install from Packages**

visit our release page [here](https://github.com/Hornetlabs/synchdb/releases) to download a synchdb packages on supported platforms.

### **.deb Packages**

1. Install PostgreSQL from official apt repository (version 16 as example):
```sh linenums="1"
sudo apt install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo apt install postgresql-16
```

2. Install Java Runtime Environment:
```sh linenums="1"
sudo apt install openjdk-17-jre-headless
```

3. Update shared library path:
```sh linenums="1"
JAVA_PATH=$(which java)
JRE_HOME_PATH=$(readlink -f ${JAVA_PATH} | sed 's:/bin/java::')
JRE_LIB_PATH=${JRE_HOME_PATH}/lib
echo "$JRE_LIB_PATH" | sudo tee /etc/ld.so.conf.d/java.conf
sudo ldconfig
```

4. Install SynchDB:
```sh linenums="1"
dpkg -i synchdb-1.1-1.ub22.pg16_x86_64.deb
```
5. SynchDB should be ready to go. Refer to [quick start](../../user-guide/quick_start/) page to get started

### **.rpm Packages**

1. Install PostgreSQL from official rpm repository (version 16 as example):
Refer to official PostgreSQL rpm download instructions for other PostgreSQL versions [here](https://www.postgresql.org/download/linux/redhat/)
```sh linenums="1"
sudo dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm
sudo dnf -qy module disable postgresql
sudo dnf install -y postgresql16-server postgresql16-contrib
```
2. Install Java Runtime Environment:
```sh linenums="1"
sudo dnf install -y java-17-openjdk
```

3. Update shared library path:
```sh linenums="1"
JAVA_PATH=$(which java)
JRE_HOME_PATH=$(readlink -f ${JAVA_PATH} | sed 's:/bin/java::')
JRE_LIB_PATH=${JRE_HOME_PATH}/lib
echo "$JRE_LIB_PATH" | sudo tee /etc/ld.so.conf.d/java.conf
sudo ldconfig
```

4. Install SynchDB:
```sh linenums="1"
sudo dnf install -y synchdb-1.1-1.el9.pg16.x86_64.rpm
```
5. SynchDB should be ready to go. Refer to [quick start](../../user-guide/quick_start/) page to get started

## **Install from Pre-compiled Binaries**
visit our release page [here](https://github.com/Hornetlabs/synchdb/releases) to download a pre-compiled binaries on supported platforms. We currently support pre-compiled binaries on Debian-based Linux systems such as Ubuntu. Other platforms will be supported in the near future. SynchDB pre-compiled binaries requires existing PostgreSQL to be installed first. The version of PostgreSQL it requires is described in the package name. For example, `synchdb-1.1-1.ub22.pg16_x86_64.tar.gz` is tar.gz package built on Ubuntu 22.04 against PostgreSQL 16. 

### **Pre-compiled Binaries**
1. Extract the tar.gz package that contains the pre-compiled binaries:
```sh linenums="1"
tar xzvf synchdb-1.1-1.ub22.pg16_x86_64.tar.gz -C /tmp
```

2. Find out the lib and share directories of your current PostgreSQL installation
```sh linenums="1"
LIBDIR=$(pg_config | grep -w LIBDIR | awk -F ' = ' '/LIBDIR/ {print $2}')
SHAREDIR=$(pg_config | grep -w SHAREDIR | awk -F ' = ' '/SHAREDIR/ {print $2}')
```

3. Copy pre-compiled binaries to the respective directories:
```sh linenums="1"
cp /tmp/synchdb-1.1-1.ub22.pg16_x86_64/usr/lib/postgresql/16/lib/* $LIBDIR
cp /tmp/synchdb-1.1-1.ub22.pg16_x86_64.tar.gz/usr/share/postgresql/16/extension/* $SHAREDIR
```

4. Install Java Runtime Environment:
```sh linenums="1"
sudo apt install openjdk-17-jre-headless
```

5. Update shared library path:
```sh linenums="1"
JAVA_PATH=$(which java)
JRE_HOME_PATH=$(readlink -f ${JAVA_PATH} | sed 's:/bin/java::')
JRE_LIB_PATH=${JRE_HOME_PATH}/lib
echo "$JRE_LIB_PATH" | sudo tee /etc/ld.so.conf.d/java.conf
sudo ldconfig
```

6. SynchDB should be ready to go. Refer to [quick start](../../user-guide/quick_start/) page to get started

## **Install from Source**

This option requires you to build both PostgreSQL and SynchDB from source code.

### **Build Requirements**

The following software is required to build and run SynchDB. The versions listed are the versions tested during development. Older versions may still work.

* Java Development Kit 17 or later. Download [here](https://www.oracle.com/ca-en/java/technologies/downloads/)
* Apache Maven 3.6.3 or later. Download [here](https://maven.apache.org/download.cgi)
* PostgreSQL Source. Git clone [here](https://github.com/postgres/postgres). Refer to this [wiki](https://wiki.postgresql.org/wiki/Compile_and_Install_from_source_code) for PostgreSQL build requirements
* Docker compose 2.28.1 (for testing). Refer to [here](https://docs.docker.com/compose/install/linux/)

**Additional Requirement for Openlog Replicator Connector Support (if enabled in build)**

* libprotobuf-c v1.5.2. Refer to [here](https://github.com/protobuf-c/protobuf-c.git) to build from source.

**The following is required if you would like to use FDW based snapshot**

* OCI v23.9.0. Refer to here for more information
* oracle_fdw v2.8.0. Refer to here to build from source


### **Default SynchDB Build - Support MySQL, SQLServer and Oracle Connectors**

If you already have PostgreSQL installed, you can build and install Default SynchDB with PGXS. Please note that your PostgreSQL installation must have pgcrypto extension as required by SynchDB.

```sh
USE_PGXS=1 make PG_CONFIG=$(which pg_config)
USE_PGXS=1 make build_dbz PG_CONFIG=$(which pg_config)

sudo USE_PGXS=1 make PG_CONFIG=$(which pg_config) install
sudo USE_PGXS=1 make install_dbz PG_CONFIG=$(which pg_config)
```

### **Build SynchDB with Openlog Replicator Connector Support**

To build Synchdb with Openlog Replicator Connector support, an additional Synchdb Oracle Parser component must be built as well. This component is based on IvorySQL's Oracle Parser, modified to suit SynchDB and it requires PostgreSQL backend source codes to build successfully. Here's the procedure:

#### **Prepare Source (Using 16.3 as example)**

Clone the PostgreSQL source and switch to 16.3 release tag
```sh linenums="1"
git clone https://github.com/postgres/postgres.git
cd postgres
git checkout REL_16_3
```

Clone the SynchDB source from within the extension folder
Note: Branch *(synchdb-devel)[https://github.com/Hornetlabs/synchdb/tree/synchdb-devel]* is used for development so far.
```sh linenums="1"
cd contrib/
git clone https://github.com/Hornetlabs/synchdb.git
```

#### **Build and Install PostgreSQL**

This can be done by following the standard build and install procedure as described [here](https://www.postgresql.org/docs/current/install-make.html) 

**Warning**: SynchDB depends on pgcrypto to encrypt and decrypt sensitive access information. Please ensure PostgreSQL is built with SSL support.

```sh linenums="1"
cd /home/$USER/postgres
./configure --with-ssl=openssl --enable-cassert
make
sudo make install
```

Build the required pgcrypto extension
```sh linenums="1"
cd /home/$USER/postgres/contrib/pgcrypto
make
sudo make install
```

#### Build SynchDB with Additional Openlog Replicator Connector Support

```sh linenums="1"
# build and install debezium runner
cd /home/$USER/postgres/contrib/synchdb
make build_dbz
sudo make install_dbz

# build and install oracle parser
make oracle_parser
sudo make install_oracle_parser

# build and install synchdb
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

Ensure synchdb.so extension can link to libjvm Java library on your system:
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