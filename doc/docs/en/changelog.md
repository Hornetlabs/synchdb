# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## **[SynchDB 1.3](https://github.com/Hornetlabs/synchdb/releases/tag/v1.3) - 2025-11-25**

SynchDB 1.3 delivers a major performance enhancement with the new FDW-based snapshot engine, offering significantly faster initial snapshot performance over Debezium. This advancement makes OpenLog Replicator (OLR) connector a fully native snapshot + CDC pipeline (no Debezium is used) with significantly reduced latency and overhead for large Oracle datasets. Platform support has been broadened as well. SynchDB now runs on PostgreSQL 18 and IvorySQL 5, accompanied by a series of performance optimizations and I/O improvements across the system.

### Added

#### [FDW-based Snapshot Engine](../architecture/fdw_based_snapshot/)

* FDW-based snapshot provides a much faster alternative to Debezium's initial snapshot process
* Added a new GUC parameter "synchdb.snapshot_engine" to select snapshot engine, can be "debezium" or "fdw"
* Added a new GUC parameter "synchdb.cdc_start_delay_ms" to add a millisecond delay that needs to be waited after initial snapshot is completed and before CDC is started.
* Added a new GUC parameter "synchdb.fdw_migrate_with_subtx" to use sub-transactions to migrate each table or not during FDW-based snapshot process.
* Applicable only to Openlog Replicator and Oracle connector types via [oracle_fdw](https://github.com/laurenz/oracle_fdw) 2.8.0. Other connector types may be supported in future releases.
* Supported a retry mechanism: If some tables fail to snapshot, the error messages will be saved to synchdb_fdw_snapshot_errors_xxx table and can be retried by resuming the connector. Tables that succeeded will remain.
* If 'schemasync' or 'nodata' snapshot mode is used, FDW-engine will synchronize table schemas only.
* Tables and data created via FDW-engine are subject to name and expression transformation rules as defined in "synchdb_objmap" 
* Added a new function synchdb_translate_datatype() that returns SynchDB's data type translation results based on selected connector type.


#### [Revamped Statistics Views](../monitoring/stats_view/)

* Revamped statistics view to group different statistics to proper categories.
* synchdb_stats_view() has been removed
* Added a new view called synchdb_genstats that records statistics about batches of events processed
* Added a new view called synchdb_snapstats that records statistics about initial snapshot process
* Added a new view called synchdb_cdcstats that records statistics about CDC process

#### PostgreSQL 18 and IvorySQL 5 Compatibility

* SynchDB is compatible with PostgreSQL 18 and IvorySQL 5.
* When running FDW-based snapshot under IvorySQL 5 in 'oracle' compatible mode, this mode will be switched back to 'pg' mode for the duration of the snapshot because the PL/pgSQL functions are written in PostgreSQL standard.
* Synchdb will set "ivorysql.identifier_case_switch" settings to 'normal' in SynchDB workers to prevent letter casing being reversed undesirably.
* Renamed liboracle_parser.so and oracle_raw_parser() symbol inside to libsynchdb_oracle_parser.so and synchdb_oracle_raw_parser() to prevent symbol name conflicts with IvorySQL.

### Changed

* Openlog Replicator Connector: Enhanced Oracle parser to support more contraint operators: enable, disable, novalidate, validate
* Openlog Replicator Connector: Enhanced Oracle parser to support MODIFY clauses with and without parenthesis
* Openlog Replicator Connector: Enhanced Oracle parser to support DEFAULT ON NULL clauses.
* Openlog Replicator Connector: Improve processing performance by removing one pre-scan loop.
* Openlog Replicator Connector: Optimized non-null terminated event processing as PostgreSQL text type to save one copy operation. (PG17+)
* Openlog Replicator Connector: default read buffer size changed to 128MB
* Openlog Replicator Connector: added a new GUC parameter "synchdb.olr_read_timeout_ms" to configure read timeout.
* Openlog Replicator Connector: added a new GUC parameter "synchdb.olr_connect_timeout_ms" to configure connect timeout.
* Openlog Replicator Connector: will ignore "log_mining_flush" table created by Debezium.
* Updated default data type mappings for all connector types
* More robust JSON processing: no longer crashes when a JSON element fails to look up 

### Fixed

* Openlog Replicator Connector: fixed an issue where Oracle parser fails to parse when running under PostgreSQL 16
* Openlog Replicator Connector: fixed an issue where unexpected DDL statement with system owner get passed to SynchDB by mistake
* Openlog Replicator Connector: Adjusted the SCN and C_SCN values to resume CDC as stored in the offset file rather than adding 1 to them, which could potentially skip a previously failed change event upon resume.
* Synchdb now allows source table or schema names to contain spaces.
* Fixed the incorrect processing of negative decimal values.
* Fixed the incorrect processing of large decimal overflow values. 
* Fixed an issue in Oracle's default data type mappings for NUMBER that omits the length and scale by mistake.
* Fixed the missing default data type mappings with auto_increment attributes for mysql connector

### Known Issues and Additional Info

* SynchDB always normalize incoming table, schema and column names to lowercase letters before applying them to PostgreSQL, this is a problem if source table supports objects with the same name but in different cases. For example, 'mytable' and 'MYTABLE' are considered different in MySQL and Oracle. Link to the issue [here](https://github.com/Hornetlabs/synchdb/issues/194)
* FDW-based snapshot for MySQL and SQL server are not supported yet and we may add support for them in future releases. More info [here](https://github.com/Hornetlabs/synchdb/issues/190)

## **[SynchDB 1.2](https://github.com/Hornetlabs/synchdb/releases/tag/v1.2) - 2025-09-04**

SynchDB 1.2 introduces introduces a native Openlog Replicator connector (BETA), enhanced monitoring with JMX and Grafana, and a new ezdeploy.sh tool for quick deployments and testing. It also adds snapshot table selection, performance improvements, and key fixes, while addressing connector isolation and stability issues.

### **Added**

#### [Native Openlog Replicator Connector](../user-guide/configure_olr/) - BETA
* Added `synchdb_add_olr_conninfo` and `synchdb_del_olr_conninfo` to enable or disable openlog replicator based streaming.
* Added a new connector type `olr`, which is a native (no Debezium) openlog replicator client that stream Oracle database changes from an external Openlog Replicator service. 
* Supported DMLs: insert, update delete
* Supported DDLs: CREATE TABLE, DROP TABLE, ALTER TABLE MODIFY, ALTER TABLE ADD/DROP COLUMN, ALTER TABLE ADD/DROP CONSTRAINT, TRUNCATE
* Based on libprotobuf-c to communiate with Openlog Replicator and IvorySQL's Oracle parser to process incoming DDL query events,
* Supported snapshot modes: initial, initial_only, no_data, always, never
* Supported batching, schema history, and offset management just like other Debezium based connectors.
* Supported Debezium-based Openlog Replicator connector in addition to native.

#### [Monitoring](../monitoring/jmx_monitor/)

* Added `synchdb_add_jmx_conninfo` and `synchdb_del_jmx_conninfo` to enable or disable JMX based monitoring.
* Added `synchdb_add_jmx_exporter_conninfo` and `synchdb_del_jmx_exporter_conninfo` to enable and disable monitoring support with Prometheus and Grafana.
* Added Grafana based dashboard templates for supported Debezium based connectors (MySQL, SQL Server and Oracle).

#### [ezdeploy.sh](../getting-started/quick_start/)
* Added `ezdeploy.sh` tool that can quickly deploy a pre-build SynchDB and selected source database types to do quick connector testing.
* supported deployment: MySQL, SQL Server, Oracle23ai, Oracle19c, Openlog Replicator 1.3.0
* supported Prometheus and Grafana deployment with preloaded dashboards.
* Supported pre-compiled SynchDB v1.2 for quick deployment + tests


### **Changed**

* Added a new argument called `snapshot table` in `synchdb_add_conninfo` to allow users to select which tables to redo initial snapshot when started in `always` snapshot mode.
* Updated pytest framework to support hammerdb based TPC tests for Oracle.
* Enhanced the performance of event polling between Debezium engine and SynchDB by using direct buffer instead of frequent JNI calls. 
* When a connector is resumed via `synchdb_resume_engine`, it will always resume in `initial` snapshot mode rather than the mode it was first started with.
* `synchdb_state_view` and `synchdb_stats_view` will now only display connector information created by the current SynchDB extension. Connectors created by other SynchDB extensions in different databases will not be shown.


### **Fixed**

* Fixed a crash in `spi_execute_select_one()` where it would return a reference that has been destroyed by SPI memory context at the end of a successful SPI execution.
* Resolved compilation issues with SynchDB when PostgreSQL is built with cassert.
* Fixed an issue where multiple connectors created with the same name by multiple SynchDB extensions (created in different databases) overwrite each other's shared memory data.

### **Known Issues and Additional Info**

* Native Openlog Replicator Connector currently stream all tables under specified database. Table Filtering is to be configured in Openlog Replicator rather than SynchDB.
* Prometheus and Grafana based monitoring require JMX Exporter which can be downloaded [here](https://github.com/prometheus/jmx_exporter)



## **[SynchDB 1.1](https://github.com/Hornetlabs/synchdb/releases/tag/v1.1) - 2025-04-17**

SynchDB 1.1 introduces Oracle connector support, enhanced data type transformation capabilities, and significantly improved core data processing engine. This update enhances performance through intelligent caching and optimized JSON parsing, while extending compatibility with PostgreSQL 16, 17, and IvorySQL 4.4.

### **Added**

#### **Connector Support**

* added Oracle connector support.
* added support for PostgreSQL 16, 17 and IvorySQL 4.4
* added support for Oracle's variable size/scale data type `NUMBER` processing. 

#### **Object Mapping & Data Transformation**

* Added new `synchdb_objmap` table to store object mapping entries
* Added new `synchdb_add_objamp()` function to add object mappings for table names, column names, data types, and transform expressions
* Added new `synchdb_reload_objmap()` function to force connectors to reload object mappings
* Added new `synchdb_del_objmap()` function to disable and remove object mapping entries
* Normalized all data type mapping entries to use lowercase letters

#### **Metadata & Monitoring**

* Added new `synchdb_attribute` table that stores remote table attribute information
* Added new `synchdb_att_view` view showing side-by-side comparison of data type, table and column name mappings
* Added source timestamp, DBZ timestamp and PG timestamp information in `synchdb_get_stats`

#### **Connection Management**

* Added `synchdb_add_extra_conninfo()` function to configure extra SSL connection parameters
* Added `synchdb_del_extra_conninfo()` function to remove all extra parameters created by `synchdb_add_extra_conninfo()`
* Added `synchdb_del_conninfo()` function to remove existing connector information

#### **Configuration & Control**

* Added new GUC `synchdb.error_handling_strategy` to control error handling strategy (skip, exit, or retry)
* Added new GUC `synchdb.dbz_log_level` to control the log level of Debezium runner engine
* Added new `schemasync` snapshot mode

#### **Performance Optimizations**

* Optimized DML parser to use cached hash table rather than accessing the catalog and rebuilding the hash at every change event
* DML parser now uses more efficient JSONB API calls for increased performance
* Optimized JNI calls to use cached JNI handle rather than recreating at every change event
* Enhanced batch completion marking by only marking the first and last change events within a batch
* Revamped data processing engine with more modular design to handle complex data type mappings

### **Changed**

* When processing ALTER TABLE change events, SynchDB now only adds a primary key when the table itself has no primary key
* Removed the ability to configure connectors to connect to PostgreSQL databases other than where SynchDB is installed
* SynchDB now updates the new `synchdb_attribute` table at the end of every DDL change event processing
* Connectors now correct table names, column names, and data types when configured object mappings differ from current values
* Rule files have been replaced by `synchdb_add_objmap()` utilities
* All ID-based SQL function column definitions have changed data types from TEXT to NAME
* Removed the rulefile parameter from `synchdb_add_conninfo()`
* Non-native data types are now processed based on their category rather than all being treated as text

### **Fixed**

* Fixed an issue where SPI update or delete could fail when only including primary key fields in WHERE clause
* Fixed an issue with ALTER TABLE errors when attempting to add duplicate primary keys
* Fixed an issue where SynchDB occasionally incorrectly looks up column schema values from JSON change events
* Fixed an issue in 'skip on error' mode causing panics due to error stack overflow
* Fixed an issue in ALTER TABLE ADD and DROP column operations not recognizing column name mappings in PostgreSQL

## **[SynchDB 1.0](https://github.com/Hornetlabs/synchdb/releases/tag/v1.0) - 2024-12-24**
 
This release focuses on bug fixes and performance enhancements following the v1.0 Beta1 release that generally makes it more usable under moderate to high data loads. A lot more Debezium tuning related parameters have also been exposed as PostgreSQL GUCs, allowing user to test with different parameters.
 
### **Added**

* added a data cache in DML parsing stage to prevent frequent access to PostgreSQL's catalog to obtain a table's tuple descriptor structure.
* added a variant of `synchdb_start_engine_bgw(name, mode)` that takes a second argument to indicate a custom snapshot mode to start the connector with.
* added several new GUCs that can be adjusted to tune the performance of Debezium Runner. Refer to [here](../getting-started/configuration/) for complete list.
* added a debug SQL function `synchdb_log_jvm_meminfo(name)` that causes specified connector to output current JVM heap memory usage summary in PostgreSQL log file.
* added a new VIEW `synchdb_stats_view` that prints statistic information for all connectors.
* added a new SQL function `synchdb_reset_stats(name)` to clear statistic information of specified connector.
* added a mess creation script to quickly generate test tables and data on MySQL database type.

### **Changed**

* synchdb_state_view(): added a new field called `stage` that indicates the current stage of a connector (value can either be `Initial Snapshot` or `Change Data Capture`).
* synchdb_state_view(): will only show state of valid connectors.
* removed sending "partial batch completion" notification to Debezium Runner in case of error, because a batch is now handled by one PostgreSQL transaction, and partial completion is not allowed.
* SSL related parameters per connector can now be specified in the rule file.
* the maximum heap memory to allocate to JVM that runs the Debezium Runner can now be configured via GUC.
* max number of connector background worker is now configurable instead of hardcoded 30.

### **Fixed**

* fixed rapid memory buildups in Debezium runner in JVM by adding a throttle control in the receiving of change events.
* resolved majority of memory leak in both SynchDB and Debezium runner components.
* corrected the use of memory context in SynchDB such that heap memory can be correctly freed at the end of each change event processing.
* significantly increased the processing speed of SynchDB by processing a batch within a single PostgreSQL transaction rather than multiple.
* corrected SQLServer's default data type size mapping for `char` type from 0 to -1.
* resolved high memory usage during DML processing using SPI.

## **[SynchDB 1.0 Beta1](https://github.com/Hornetlabs/synchdb/releases/tag/v1.0_beta1) - 2024-10-23**
 
The first SynchDB beta software release that lays a robust foundation for seamless replication from heterogeneous databases to PostgreSQL.
 
### **Added**

* logical replication from heterogeneous databases: (MySQL and SQLServer).
* DDL replication (CREATE TABLE, DROP TABLE, ALTER TABLE ADD COLUMN, ALTER TABLE DROP COLUMN, ALTER TABLE ALTER COLUMN).
* DML replication (INSERT, UPDATE, DELETE).
* max 30 concurrent connector workers.
* [automatic connector launcher](../user-guide/connector_auto_launcher) at PostgreSQL startup.
* global connector state and last error message views.
* [selective databases and tables replication](../tutorial/selective_table_sync).
* change events in batches.
* connector restarts in different snapshot modes.
* [offset management interfaces](../user-guide/set_offset) to select custom replication resume point.
* default data type and object name transform rules for supported heterogeneous databases.
* [JSON rule file](../user-guide/object_mapping_rules) to define custom: (data type, column name, table name and data expression transform rules).
* 2 data apply modes (SPI, HeapAM API).
* several utility functions to perform connector operations: (start, stop, pause, resume).
 
### **Changed**

### **Fixed**
