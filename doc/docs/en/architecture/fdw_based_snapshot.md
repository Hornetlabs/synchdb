# FDW Based Snapshot

## **Overview**

All the connectors support Debezium based initial snapshot (except for Postgres connector), which migrates remote table schemas to PostgreSQL with or without the initial data (depending on the snapshot mode used). This works mostly, but may suffer from performance issues when there is a large number of tables to migrate and extra overhead is introduced via JNI calls. It is possible to achieve the same initial snapshot using a Foreign Data Wrapper (FDW) as long as we have a way to gurantee consistency and identify a "cut-off" point to allow CDC to resume.

FDW based snapshot is supported for:

* MySQL Connector
* Postgres Connector
* Oracle and Openlog Replicator Connectors

## **How Synchdb Guarentees Consistency and Obtains Cut-Off Point**

### **MySQL Connector**

* Begin a transaction in repeatable read isolation level.
* Temporarily put a table read lock on all desired tables.
* Read `binlog_log_file`, `binlog_pos` and `server_id` from global configuration. These are served as "cut-off" point for the snapshot.
* Release the table locks.
* In the same repeatable read transaction, migrate all desired tables schema and data with proper type translations
* Once done, the CDC can resume from the cut-off point, which will handle the data changes that happened during the snapshot.

WARNING: **BACKUP_ADMIN permission is required to obtain the "cut-point" parameters.**

### **Postgres Connector**

* Begin a transaction in repeatable read isolation level.
* Temporarily put a table read lock on all desired tables.
* Read current LSN, which serves as a "cut-off" point for the snapshot.
* Release the table locks.
* In the same repeatable read transaction, migrate all desired tables schema and data with proper type translations
* Once done, the CDC can resume from the cut-off point, which will handle the data changes that happened during the snapshot.

### **Oracle and Openlog Replicator Connectors**

* Before snapshot begins, read the current SCN value, which serves as a "cut-off" point for the snapshot
* During foreign table schema migration, extra attribute "AS OF SCN xxx" will be associated with each desired foreign table,causing all the foreign reads to use Oracle's FLASHBACK query
* FLASHBACK query returns the table results as of the SCN specified so consistency is automatically guaranteed. No extra locking needed.
* Migrate all desired tables schema and data with proper type translations with FLASHBACK query.
* Once done, the CDC can resume from the cut-off point, which will handle the data changes that happened during the snapshot.

WARNING: **FLASHBACK permission is required to obtain the "cut-point" parameters.**

## **How does FDW Based Snapshot Work**

FDW based snapshot consists of about 10 steps:

### **1. Preparation**

This steps checks if `oracle_fdw` is installed and available, creates a `server` and `user mapping` based on the connector information created by `synchdb_add_conninfo` 

### **2. Create Oracle Object Views** 

This steps create numerous foreign tables in a separate schema (ex. `ora_obj`) in PostgreSQL. These foreign tables (when queried) connects to Oracle via oracle_fdw and obtains most (if not all) of the objects available on Oracle. Objects such as:

* tables
* columns
* keys
* indexes
* functions
* sequences
* views
* triggers
* ...etc

SynchDB does not need to account for every single object to complete an initial snapshot; It only accounts for `tables`, `columns` and `keys` objects to construct the tables in PostgreSQL. 

### **3. Create a Foreign Table to Fetch Cut-Off value**

Foreign tables will be created to read the cut-off value from difference source databases:

**MySQL Connector**

* Read `binglog_file` and `binlog_pos` from performance_schema.log_status
* Read `server_id` from performance_schema.global_variables

**Postgres Connector**

* Read current `LSN` from a custom view called public.synchdb_wal_lsn. This must be pre-created as a requirement

**Oracle and Openlog Replicator Connector**

* Read current `SCN` from current_scn table

### **4. Create a List of Desired Foreign Tables**

The goal for this step is to create a new staging schema (ex. ora_stage), and create desired foreign tables for the snapshot based on:

* object views created from step 2
* SynchDB's `data->'snapshottable'` parameter in `synchdb_conninfo` -> this is a filter of tables that SynchDB needs to do a snapshot. All tables will be considered in snapshot if set to `null`.
* Extra data type mapping as described in `synchdb_objmap` 
* the cut-off SCN obtained from step 3 (Oracle and Openlog Replicator Connectors only)

At the end of this step, the staging schema will contain foreign tables with their data types mapped according to SycnhDB's data type mapping rules. For Oracle and Openlog Replicator Connectors, the foreign tables will have `AS OF SCN xxx` attribute, causing each foreign read to return data only up to specified SCN. For other connector types, the foreign reads return all data at the moment when repeatable transaction starts

### **5. Materialize the Schema**

The goal for this step is to materialize (turn foreign tables into real PostgreSQL tables) only the table schemas created from step 4 and put them in a new destination schema (ex. dst_stage). So, at the end of the step, the destination schema will contain real tables with the same structure as those in staging schema.

### **6. Migrate Primary Key**

Based on oracle object views created from step 2, do `ALTER TABLE ADD PRIMARY KEY` commands on the materialized tables created from step 5 to add primary keys as needed. 

### **7. Apply Column Name Mappings**

Based on column name mappings as described in `synchdb_objmap` , do `ALTER TABLE RENAME COLUMN` commands on the materialized tables created from step 5 to change column names as needed. 

### **8. Migrate Data With Transforms**

The goal for This step is to migrate the table data from staging to destination schemas and perform any value transforms as described in `synchdb_objmap`, basically doing something like:

* remote query
* apply data transform
* local insert

### **9. Apply Table Name Mappings**

Once the data is migrated, SynchDB will apply table name mapping as described in `synchdb_objmap`. The reason it is done last is that table name mapping could potentially move the table to some other schema in which we do not wish to happen during materialization. 

### **10. Finalize Initial Snapshot**

The initial snapshot is considered done at this point. This step will do a cleanup as follows:

* drop server and user mapping created from step 1
* drop Oracle object view created from step 2
* drop staging schema created from step 4

The tables that reside in the destination schema created from step 5 is the final result of initial snapshot.



