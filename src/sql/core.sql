--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION synchdb" to load this file. \quit
 
CREATE OR REPLACE FUNCTION synchdb_start_engine_bgw(name) RETURNS int
AS 'MODULE_PATHNAME', 'synchdb_start_engine_bgw'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_start_engine_bgw(name, name) RETURNS int
AS 'MODULE_PATHNAME', 'synchdb_start_engine_bgw_snapshot_mode'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_stop_engine_bgw(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_get_state() RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE VIEW synchdb_state_view AS SELECT * FROM synchdb_get_state() AS (name text, connector_type text, pid int, stage text, state text, err text, last_dbz_offset text);

CREATE OR REPLACE FUNCTION synchdb_pause_engine(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_resume_engine(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_set_offset(name, text) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_conninfo(name, text, int, text, text, text, text, text, text, text) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_restart_connector(name, name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_log_jvm_meminfo(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_set_dbz_loglevel(name, text) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_get_stats() RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_reset_stats(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE VIEW synchdb_genstats AS
SELECT
  name,
  bad_events,
  total_events,
  batches_done,
  average_batch_size,
  first_src_ts,
  first_pg_ts,
  last_src_ts,
  last_pg_ts
FROM synchdb_get_stats() AS (
  name               text,
  ddls               bigint,
  dmls               bigint,
  creates            bigint,
  updates            bigint,
  deletes            bigint,
  txs                bigint,
  truncates          bigint,
  bad_events         bigint,
  total_events       bigint,
  batches_done       bigint,
  average_batch_size bigint,
  first_src_ts       bigint,
  first_pg_ts        bigint,
  last_src_ts        bigint,
  last_pg_ts         bigint,
  tables             bigint,
  rows               bigint,
  snapshot_begin_ts  bigint,
  snapshot_end_ts    bigint
);

CREATE OR REPLACE VIEW synchdb_snapstats AS
SELECT
  name,
  tables,
  rows,
  snapshot_begin_ts,
  snapshot_end_ts
FROM synchdb_get_stats() AS (
  name               text,
  ddls               bigint,
  dmls               bigint,
  creates            bigint,
  updates            bigint,
  deletes            bigint,
  txs                bigint,
  truncates          bigint,
  bad_events         bigint,
  total_events       bigint,
  batches_done       bigint,
  average_batch_size bigint,
  first_src_ts       bigint,
  first_pg_ts        bigint,
  last_src_ts        bigint,
  last_pg_ts         bigint,
  tables             bigint,
  rows               bigint,
  snapshot_begin_ts  bigint,
  snapshot_end_ts    bigint
);

CREATE OR REPLACE VIEW synchdb_cdcstats AS
SELECT
  name,
  ddls,
  dmls,
  creates,
  updates,
  deletes,
  txs,
  truncates
FROM synchdb_get_stats() AS (
  name               text,
  ddls               bigint,
  dmls               bigint,
  creates            bigint,
  updates            bigint,
  deletes            bigint,
  txs                bigint,
  truncates          bigint,
  bad_events         bigint,
  total_events       bigint,
  batches_done       bigint,
  average_batch_size bigint,
  first_src_ts       bigint,
  first_pg_ts        bigint,
  last_src_ts        bigint,
  last_pg_ts         bigint,
  tables             bigint,
  rows               bigint,
  snapshot_begin_ts  bigint,
  snapshot_end_ts    bigint
);

CREATE TABLE IF NOT EXISTS synchdb_conninfo(name TEXT PRIMARY KEY, isactive BOOL, data JSONB);

CREATE TABLE IF NOT EXISTS synchdb_attribute (
    name name,
    type name,
    attrelid oid,
    attnum smallint,
    ext_tbname text COLLATE pg_catalog."C",
    ext_attname text COLLATE pg_catalog."C",
    ext_atttypename text COLLATE pg_catalog."C",
    PRIMARY KEY (name, type, attrelid, attnum)
);

CREATE OR REPLACE FUNCTION synchdb_add_objmap(name, name, name, text) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_objmap(name, name, text, text) RETURNS int
AS 'MODULE_PATHNAME', 'synchdb_add_objmap_text'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_reload_objmap(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE TABLE IF NOT EXISTS synchdb_objmap (
    name name,
    objtype name,
    enabled bool,
    srcobj text COLLATE pg_catalog."C",
    dstobj text,
    PRIMARY KEY (name, objtype, srcobj)
);

CREATE OR REPLACE FUNCTION synchdb_add_extra_conninfo(name, name, text, text, text, text) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_extra_conninfo(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_fdw_conninfo(name, text, text, text, text) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_fdw_conninfo(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_conninfo(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_objmap(name, name, name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_objmap(name, name, text) RETURNS int
AS 'MODULE_PATHNAME', 'synchdb_del_objmap_text'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_jmx_conninfo(name, text, int, text, int, bool, text, text, bool, text, text, text, text) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_jmx_conninfo(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_jmx_exporter_conninfo(name, text, int, text) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_jmx_exporter_conninfo(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_olr_conninfo(name, text, int, text) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_olr_conninfo(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_infinispan(name, name, int) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_infinispan(name) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_translate_datatype(name, name, bigint, bigint, bigint) RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION synchdb_set_snapstats(name, bigint, bigint, bigint, bigint) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;
