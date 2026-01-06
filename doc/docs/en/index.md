# About

<p align="center">
  <img src="images/synchdblogo.jpg" alt="synchdb" width="400">
</p>

## **About SynchDB**

SynchDB is a PostgreSQL extension that enables fast and reliable replication from heterogeneous databases — such as MySQL, SQL Server, and Oracle — directly into PostgreSQL. Unlike traditional data pipelines, SynchDB handles the entire synchronization and data conversion process natively within PostgreSQL, without requiring any middleware or external orchestration tools. It mainly manages the following end-to-end tasks:

* Establishes and maintains connections to external databases
* Captures change events from source systems
* Transforms these events into PostgreSQL-compatible formats
* Applies them to PostgreSQL

At its core, SynchDB integrates the Debezium Embedded Engine, a powerful Java-based change data capture (CDC) library that supports multiple database connectors. SynchDB bridges PostgreSQL's C-based runtime and Debezium's Java environment using the Java Native Interface (JNI), enabling seamless cooperation between both worlds.

This architecture allows PostgreSQL to leverage the rich ecosystem of Debezium connectors while keeping the extension lightweight, flexible, and easy to deploy.

🔗 Learn more about Debezium [here](https://debezium.io/documentation/reference/stable/index.html).

## **Notable Features**

- Efficient data synchronization
- Support replication from MySQL, SQL Server and Oracle databases
- Flexible data transformation rules, including table names, column names, data types and custom data transform expression
- Easy integration with existing PostgreSQL databases
- Initial snapshot and Change Data Capture (CDC) modes
- Support DDL and DML logical replication
- Global connector state, error and statistic provisioning

## **Supported PostgreSQL Versions**

- PostgreSQL: 16, 17, 18
- IvorySQL: 4, 5

## **Supported Source Databasess**

- MySQL: 8.0.x, 8.2
- SQL Server: 2017, 2019, 2022
- Oracle: 12c, 19c, 21c, 23ai
- Openlog Replicator: 1.3.0 ~ 1.8.5

## **Required Third Party Libraries and extensions**

- Java Runtime Environment（JRE) 17 or above
- pgcrypto extension - for encrypt and decrypt credentials

## **Optional Third Party Libraries and extensions**

- libprotobuf-c v1.5.2 (tested version) (optional for Openlog Replicator support)
- oracle_fdw extension v2.8.0 (tested version) (needed if you choose to use fdw based initial snapshot for OLR or Oracle connector)
- mysql_fdw extensions v2.9.3 (tested version) (needed if you choose to use fdw based initial snapshot for MySQL connector)
- postgres_fdw extension - (needed if you need initial snapshot for PostgreSQL connector)

## **Version History**

- [SynchDB v1.3](https://github.com/Hornetlabs/synchdb/releases/tag/v1.3)
- [SynchDB v1.2](https://github.com/Hornetlabs/synchdb/releases/tag/v1.2)
- [SynchDB v1.1](https://github.com/Hornetlabs/synchdb/releases/tag/v1.1)
- [SynchDB v1.0](https://github.com/Hornetlabs/synchdb/releases/tag/v1.0)
- [SynchDB v1.0 Beta1](https://github.com/Hornetlabs/synchdb/releases/tag/v1.0_beta1)

## **Getting Started**

The quickest way to get started with SynchDB is to use the pre-compiled Docker images together with the companion images for source database systems (MySQL, SQL Server, Oracle, etc.), all driven by the `ezdeploy.sh` tool in the source repository. This interactive tool can spin up SynchDB, one or more source databases, and optional Prometheus + Grafana for monitoring, perfect for a fast end-to-end smoke test. For step-by-step instructions, refer to the [Quick Start Guide](getting-started/quick_start/).

Other useful links:

* [SynchDB Architecture Decisions](architecture/architecture/)
* [Installation Guide](getting-started/installation/)
* [Source Database Setups](getting-started/remote_database_setups/)
