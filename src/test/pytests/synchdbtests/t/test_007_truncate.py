"""Live source -> Debezium/native OLR -> PostgreSQL TRUNCATE regression tests."""
import time

import pytest

from common import (
    create_synchdb_connector, drop_repslot_and_pub, getConnectorName, getDbname, getSchema,
    run_pg_query, run_pg_query_one, run_remote_query, stop_and_delete_synchdb_connector,
    update_guc_conf,
)


def wait_until(cursor, query, expected, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = run_pg_query_one(cursor, query)
        if last == expected:
            return last
        status = run_pg_query_one(cursor, "SELECT state,err FROM synchdb_state_view WHERE name LIKE '%_truncate%' LIMIT 1")
        if (status and status[0] in ("stopped", "paused") and
                time.monotonic() >= deadline - timeout + 5):
            pytest.fail(f"Connector stopped while waiting for {query}: {status}")
        time.sleep(1)
    pytest.fail(f"Timed out: {query}; expected={expected}; last={last}")


def setup_truncate(cursor, vendor, name, pk=True, skipped="", spi=False):
    if vendor == "sqlserver":
        pytest.skip("SQL Server CDC does not capture TRUNCATE")
    dbname = getDbname(vendor).lower()
    table = "truncate_table_test"
    update_guc_conf(cursor, "synchdb.dbz_skipped_oeprations", f"'{skipped}'", True)
    update_guc_conf(cursor, "synchdb.dml_use_spi", "true" if spi else "false", True)
    update_guc_conf(cursor, "synchdb.naptime", "10", True)
    # PostgreSQL snapshotting uses target tables prepared by the caller.
    datatype = "NUMBER(10)" if vendor in ("oracle", "oracle23ai", "olr") else "INT"
    run_remote_query(vendor, f"CREATE TABLE {table} (id {datatype} {'PRIMARY KEY' if pk else ''}, val VARCHAR(30))")
    if vendor == "postgres":
        run_remote_query(vendor, f"ALTER TABLE {table} REPLICA IDENTITY FULL")
        run_pg_query(cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}; CREATE TABLE {dbname}.{table} (id integer {'PRIMARY KEY' if pk else ''}, val varchar(30))")
    elif vendor in ("oracle", "oracle23ai", "olr"):
        run_remote_query(vendor, f"ALTER TABLE {table} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
    run_remote_query(vendor, f"INSERT INTO {table} VALUES (1,'old')")
    create_synchdb_connector(cursor, vendor, name)
    source_schema = getSchema(vendor) or getDbname(vendor)
    source_table = f"{source_schema}.{table.upper() if vendor in ('oracle', 'oracle23ai', 'olr') else table}"
    run_pg_query(cursor, f"UPDATE synchdb_conninfo SET data=jsonb_set(data,'{{table}}',to_jsonb('{source_table}'::text)) WHERE name='{name}'")
    run_pg_query_one(cursor, f"SELECT synchdb_start_engine_bgw('{name}', 'initial')")
    wait_until(cursor, f"SELECT to_regclass('{dbname}.{table}') IS NOT NULL", (True,))
    wait_until(cursor, f"SELECT count(*) FROM {dbname}.{table}", (1,))
    wait_until(cursor, f"SELECT stage FROM synchdb_state_view WHERE name='{name}'", ("change data capture",))
    if vendor == "postgres":
        run_remote_query(vendor, f"ALTER TABLE {table} REPLICA IDENTITY FULL")
        # Drain identity DDL messages before the TRUNCATE statistics baseline.
        run_remote_query(vendor, f"INSERT INTO {table} VALUES (0,'ready')")
        wait_until(cursor, f"SELECT count(*) FROM {dbname}.{table} WHERE id=0", (1,))
        wait_until(cursor, f"SELECT creates>=1 FROM synchdb_cdcstats WHERE name='{name}'", (True,))
    return dbname, table


def cleanup(cursor, vendor, name, schema, table):
    stop_and_delete_synchdb_connector(cursor, name)
    run_remote_query(vendor, f"DROP TABLE {table}")
    run_pg_query(cursor, f"DROP TABLE IF EXISTS {schema}.{table} CASCADE")
    drop_repslot_and_pub(vendor, name, "postgres")
    update_guc_conf(cursor, "synchdb.dbz_skipped_oeprations", "'t'", True)
    update_guc_conf(cursor, "synchdb.dml_use_spi", "false", True)


@pytest.mark.parametrize("pk,spi", [(False, False), (False, True), (True, False), (True, True)],
                         ids=["no-pk-native-dml", "no-pk-spi-dml", "pk-native-dml", "pk-spi-dml"])
def test_TruncateTable(pg_cursor, dbvendor, pk, spi):
    name = getConnectorName(dbvendor) + "_truncate"
    schema, table = setup_truncate(pg_cursor, dbvendor, name, pk=pk, spi=spi)
    try:
        oid = run_pg_query_one(pg_cursor, f"SELECT '{schema}.{table}'::regclass::oid")[0]
        attrs = run_pg_query(pg_cursor, f"SELECT * FROM synchdb_attribute WHERE name='{name}' ORDER BY attnum")
        initial = run_pg_query_one(pg_cursor, f"SELECT ddls,truncates FROM synchdb_cdcstats WHERE name='{name}'")
        for n in (1, 2):
            run_remote_query(dbvendor, f"TRUNCATE TABLE {table}")
            wait_until(pg_cursor, f"SELECT count(*) FROM {schema}.{table}", (0,))
            wait_until(pg_cursor, f"SELECT ddls,truncates FROM synchdb_cdcstats WHERE name='{name}'",
                       (initial[0] + n, initial[1] + n))
        assert run_pg_query_one(pg_cursor, f"SELECT '{schema}.{table}'::regclass::oid")[0] == oid
        assert run_pg_query(pg_cursor, f"SELECT * FROM synchdb_attribute WHERE name='{name}' ORDER BY attnum") == attrs
        run_remote_query(dbvendor, f"INSERT INTO {table} VALUES (2,'new')")
        wait_until(pg_cursor, f"SELECT id,val FROM {schema}.{table}", (2, "new"))
        if pk or not spi:
            run_remote_query(dbvendor, f"UPDATE {table} SET val='updated' WHERE id=2")
            wait_until(pg_cursor, f"SELECT id,val FROM {schema}.{table}", (2, "updated"))
            run_remote_query(dbvendor, f"DELETE FROM {table} WHERE id=2")
            wait_until(pg_cursor, f"SELECT count(*) FROM {schema}.{table}", (0,))
        assert run_pg_query_one(pg_cursor, f"SELECT bad_events FROM synchdb_genstats WHERE name='{name}'")[0] == 0
    finally:
        cleanup(pg_cursor, dbvendor, name, schema, table)


def test_TruncateSkipped(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_truncateskip"
    schema, table = setup_truncate(pg_cursor, dbvendor, name, skipped="t")
    try:
        before = run_pg_query_one(pg_cursor, f"SELECT c.ddls,c.truncates,g.bad_events FROM synchdb_cdcstats c JOIN synchdb_genstats g USING(name) WHERE name='{name}'")
        count = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {schema}.{table}")[0]
        run_remote_query(dbvendor, f"TRUNCATE TABLE {table}")
        # Subsequent INSERT proves the stream has crossed the filtered operation.
        run_remote_query(dbvendor, f"INSERT INTO {table} VALUES (2,'new')")
        wait_until(pg_cursor, f"SELECT count(*) FROM {schema}.{table}", (count + 1,))
        assert run_pg_query_one(pg_cursor, f"SELECT c.ddls,c.truncates,g.bad_events FROM synchdb_cdcstats c JOIN synchdb_genstats g USING(name) WHERE name='{name}'") == before
    finally:
        cleanup(pg_cursor, dbvendor, name, schema, table)


@pytest.mark.parametrize("failure", ["missing_target", "foreign_key"])
def test_TruncateFailureReplay(pg_cursor, dbvendor, failure):
    name = getConnectorName(dbvendor) + "_truncatefail"
    schema, table = setup_truncate(pg_cursor, dbvendor, name)
    try:
        before = run_pg_query_one(pg_cursor, f"SELECT truncates FROM synchdb_cdcstats WHERE name='{name}'")[0]
        if failure == "missing_target":
            run_pg_query(pg_cursor, f"DROP TABLE {schema}.{table}")
        else:
            run_pg_query(pg_cursor, f"CREATE TABLE {schema}.truncate_child (parent int REFERENCES {schema}.{table}(id))")
        run_remote_query(dbvendor, f"TRUNCATE TABLE {table}")
        wait_until(pg_cursor, f"SELECT err <> 'no error' FROM synchdb_state_view WHERE name='{name}'", (True,))
        assert run_pg_query_one(pg_cursor, f"SELECT truncates FROM synchdb_cdcstats WHERE name='{name}'")[0] == before
        if failure == "missing_target":
            run_pg_query(pg_cursor, f"CREATE TABLE {schema}.{table} (id int PRIMARY KEY, val varchar(30))")
        else:
            assert run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {schema}.{table}")[0] > 0
            run_pg_query(pg_cursor, f"DROP TABLE {schema}.truncate_child")
        run_remote_query(dbvendor, f"INSERT INTO {table} VALUES (2,'replayed')")
        wait_until(pg_cursor, f"SELECT pid<=0 FROM synchdb_state_view WHERE name='{name}'", (True,))
        run_pg_query_one(pg_cursor, f"SELECT synchdb_start_engine_bgw('{name}', 'no_data')")
        wait_until(pg_cursor, f"SELECT id,val FROM {schema}.{table} WHERE id=2", (2, "replayed"))
        # Acknowledging the failed event would leave the old rows in the FK case.
        assert run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {schema}.{table}") == (1,)
        wait_until(pg_cursor, f"SELECT truncates>{before} FROM synchdb_cdcstats WHERE name='{name}'", (True,))
    finally:
        run_pg_query(pg_cursor, f"DROP TABLE IF EXISTS {schema}.truncate_child")
        cleanup(pg_cursor, dbvendor, name, schema, table)
