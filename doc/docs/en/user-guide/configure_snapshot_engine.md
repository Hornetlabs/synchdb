# Configure Snapshot Engine

## **Initial Snapshot vs Change Data Capture (CDC)**

Initial snapshot refers to the process of migrating both the table schema and the initial data from remote database to SynchDB. For most connector types, this is done only once on first connector start via embedded Debezium runner as the only engine that can perform initial snapshot. After the initial snapshot completes, the Change Data Capture will begin to stream live changes to SynchDB. The "snapshot modes" can further control the behavior of initial snapshot and CDC. Refer to [here](../../user-guide/start_stop_connector/) for more information.

SynchDB supports 2 snapshot engines, each supports various connector types:

**Debezium based snapshot：**

* MySQL
* SQL Server
* Oracle
* Openlog Replicator

**FDW based snapshot：**

<**NOTE**> mysql_fdw cannot be compiled under IvorySQL 5.x.

* MySQL (uses mysql_fdw)
* Oracle (uses oracle_fdw)
* Openlog Replicator (uses oracle_fdw)
* Postgres (uses postgres_fdw)

## **Debezium Based Initial Snapshot**

This is the default snapshot engine (synchdb.olr_snapshot_engine='debezium') that relies of Debezium engine via JNI to initiate the snapshot process. In most cases, it works fine, but may suffer from performance issues if the number of tables included in snapshot is high (thousands or more). Debezium will first go through an analaysis stage that visits each table once to learn its structure and estimates number of rows to snapshot. This is done entirely by a single worker and during this time, the engine may appear to be "hanging" and will not emit any change events. This process itself could take hours to complete in large number of tables. Once done, it will then start emitting change events to SynchDB.

Triggered automatically upon `synchdb_start_engine_bgw()` with a snapshot mode that requires doing a snapshot and will not repeat again if completed.

## **FDW Based Initial Snapshot**

* Enabled in `postgresql.conf` by setting "synchdb.olr_snapshot_engine" to "fdw"
* Triggered automatically upon `synchdb_start_engine_bgw()` with a snapshot mode that requires doing a snapshot and will not repeat again if completed.
* Corresponding FDWs must be available.

## **FDW Snapshot for Oracle and OLR Connectors**

It is possible to use oracle_fdw to perform initial snapshot for Oracle and OLR connectors which is significantly faster than Debezium counterpart. SynchDB integrates with [oracle_fdw](https://github.com/laurenz/oracle_fdw) version 2.8.0 with OCI v23.9.0 as its underlying driver. This is just the version tested, older or newer versions may still work.

### **Install OCI**

OCI is required to build and run oracle_fdw. For this example, I am using version 23.9.0:

```bash
# Get the pre-build packages:
wget https://download.oracle.com/otn_software/linux/instantclient/2390000/instantclient-basic-linux.x64-23.9.0.25.07.zip
wget https://download.oracle.com/otn_software/linux/instantclient/2390000/instantclient-sdk-linux.x64-23.9.0.25.07.zip

# unzip them: Select 'Y' to overwrite metadata files
unzip instantclient-basic-linux.x64-23.9.0.25.07.zip
unzip instantclient-sdk-linux.x64-23.9.0.25.07.zip
```

Update environment variables so the system knows where to find OCI headers and libraries.

```bash
export OCI_HOME=/home/$USER/instantclient_23_9
export OCI_LIB_DIR=$OCI_HOME
export OCI_INC_DIR=$OCI_HOME/sdk/include
export LD_LIBRARY_PATH=$OCI_HOME:${LD_LIBRARY_PATH}
export PATH=$OCI_HOME:$PATH

```

You could add the above commands to the end of `~/.bashrc` to automatically set the PATHs every time you login to your system.

You can also add $OCI_HOME to `/etc/ld.so.conf.d/x86_64-linux-gnu.conf` (ubuntu example), as a new path to find shared libraries.

You should do a ldconfig so your linker knows where to find shared libraries:

### **Build and Install oracle_fdw 2.8.0**

```bash
git clone https://github.com/laurenz/oracle_fdw.git --branch ORACLE_FDW_2_8_0

```

Ensure oracle_fdw's Makefile can find OCI includes and libraries by adjusting these 2 lines in Makefile:

```bash
FIND_INCLUDE := $(wildcard /usr/include/oracle/*/client64 /usr/include/oracle/*/client /home/$USER/instantclient_23_9/sdk/include)
FIND_LIBDIRS := $(wildcard /usr/lib/oracle/*/client64/lib /usr/lib/oracle/*/client/lib /home/$USER/instantclient_23_9)

```

Build and install oracle_fdw:

```bash
make PG_CONFIG=/usr/local/pgsql/bin/pg_config
sudo make install PG_CONFIG=/usr/local/pgsql/bin/pg_config

```

Oralce_fdw is ready to go. Start a connector normally with synchdb.olr_snapshot_engine set to 'fdw'. If a snapshot is required, SynchDB will complete it via FDW. You do not have to run `CREATE EXTENSION oracle_fdw` prior to using FDW based initial snapshot, nor do you have to `CREATE SERVER` or `CREATE USER MAPPING`. SynchDB takes care of all of these when it performs the snapshot..

<**NOTE**> If the target is an Oracle Container Database (CDB/PDB), specify the source database (srcdb) in the `CDB/PDB` format (e.g. `FREE/FREEPDB1`) when creating the connector with `synchdb_add_conninfo()`, and SynchDB will automatically connect to the corresponding PDB service.


## **FDW Snapshot for MySQL Connectors**

It is possible to use mysql_fdw to perform initial snapshot for MySQL connectors which is significantly faster than Debezium counterpart. SynchDB integrates with [mysql_fdw](https://github.com/EnterpriseDB/mysql_fdw) version 2.9.3 with libmysqlclient v8 as its underlying driver. This is just the version tested, older or newer versions may still work.

### **Install MySQL Client Development Package**

You may download and install MySQL client development package [here](https://packages.debian.org/sid/libmysqlclient-dev). If you are on Ubuntu, it can be installed using package manager:

```bash
sudo apt update
sudo apt install libmysqlclient-dev

```

### **Build and Install mysql_fdw 2.9.3**

```bash
git clone https://github.com/EnterpriseDB/mysql_fdw.git --branch REL-2_9_3

```

Build and install mysql_fdw:

```bash
make PG_CONFIG=/usr/local/pgsql/bin/pg_config
sudo make install PG_CONFIG=/usr/local/pgsql/bin/pg_config

```

mysql_fdw is ready to go. Start a connector normally with synchdb.olr_snapshot_engine set to 'fdw'. If a snapshot is required, SynchDB will complete it via FDW. You do not have to run `CREATE EXTENSION mysql_fdw` prior to using FDW based initial snapshot, nor do you have to `CREATE SERVER` or `CREATE USER MAPPING`. SynchDB takes care of all of these when it performs the snapshot..

## **FDW Snapshot for Postgres Connectors**

It is possible to use postgres_fdw to perform initial snapshot for Postgres connectors which is significantly faster than Debezium counterpart. SynchDB integrates with [postgres_fdw](https://github.com/postgres/postgres/tree/master/contrib/postgres_fdw) based on postgresql 18. This is just the version tested, older or newer versions may still work.

### **Build and Install postgres_fdw**

```bash
git clone https://github.com/postgres/postgres.git --branch REL_18_0
cd postgres
./configure
make 
sudo make install

cd contrib/postgres_fdw
make 
sudo make install

```

postgres is ready to go. Start a connector normally with synchdb.olr_snapshot_engine set to 'fdw'. If a snapshot is required, SynchDB will complete it via FDW. You do not have to run `CREATE EXTENSION mysql_fdw` prior to using FDW based initial snapshot, nor do you have to `CREATE SERVER` or `CREATE USER MAPPING`. SynchDB takes care of all of these when it performs the snapshot..

## **TLS Secure Connections for FDW Snapshot**

FDW-based snapshots can be secured with TLS (MySQL, Postgres) or an Oracle Wallet (Oracle, OLR) depending on the connector type. This is a separate mechanism from [`synchdb_add_extra_conninfo()`](../../tutorial/mysql_cdc_to_postgresql/#synchdb_add_extra_conninfo), which configures the Java Keystore/Truststore used for the Debezium JDBC connection.

### **synchdb_add_fdw_conninfo**

Purpose: configure the TLS certificate file paths (client certificate, private key, CA/root certificate) and an optional cipher list used by the FDW connection for the specified connector.

```sql
SELECT synchdb_add_fdw_conninfo(name, ssl_cert, ssl_key, ssl_rootcert, ssl_cipher);
```

| argument | description |
|-|-|
| `ssl_cert` | Client certificate file path (PEM format). Leave as an empty string `''` if not needed |
| `ssl_key` | Client private key file path (PEM format). Leave as an empty string `''` if not needed |
| `ssl_rootcert` | CA / root certificate file path (PEM format). **For Oracle and OLR connectors, this is instead the directory path of the Oracle Wallet**. Leave as an empty string `''` if not needed |
| `ssl_cipher` | Cipher suite list (mysql_fdw only). Leave as an empty string `''` if not needed |

Actual behavior per connector type:

* **MySQL (mysql_fdw)**: `ssl_cert`, `ssl_key`, `ssl_rootcert` (mapped to mysql_fdw's `ssl_ca`), and `ssl_cipher` are applied directly to `CREATE SERVER ... OPTIONS (...)`.
* **Postgres (postgres_fdw)**: `ssl_cert`, `ssl_key`, and `ssl_rootcert` map to postgres_fdw's `sslcert`, `sslkey`, and `sslrootcert` options respectively; `sslmode` is instead configured via [`synchdb_add_extra_conninfo()`](../../tutorial/mysql_cdc_to_postgresql/#synchdb_add_extra_conninfo) (the keystore/truststore arguments can be left empty in this case).
* **Oracle and OLR (oracle_fdw)**: only `ssl_rootcert` is used, and its value is the directory path of the Oracle Wallet. SynchDB automatically switches the connection string to Easy Connect Plus format (`tcps://host:port/service?wallet_location=...&ssl_server_dn_match=yes`). `ssl_cert`/`ssl_key` have no effect for oracle_fdw — if a client certificate is required, it must be imported into the specified Wallet directory beforehand using `orapki` or `openssl pkcs12`.

Example (MySQL):
```sql
SELECT synchdb_add_fdw_conninfo('mysqlconn', '/path/to/client-cert.pem', '/path/to/client-key.pem', '/path/to/ca.pem', '');
```

Example (Oracle / OLR, using an Oracle Wallet):
```sql
SELECT synchdb_add_fdw_conninfo('oracleconn', '', '', '/path/to/wallet_dir', '');
```

### **synchdb_del_fdw_conninfo**

Purpose: remove all TLS certificate paths configured by `synchdb_add_fdw_conninfo`.

```sql
SELECT synchdb_del_fdw_conninfo('mysqlconn');
```