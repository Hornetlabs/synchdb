"""TRUNCATE capture, mapped targets, subsequent rows and explicit filtering."""
import time

import pytest
from psycopg2.errors import UndefinedTable

import common


def _wait_rows(cursor, name, target, expected, timeout=300):
    deadline = time.monotonic() + timeout
    while True:
        try:
            cursor.execute(f"SELECT id, payload FROM {target} ORDER BY id")
            if cursor.fetchall() == expected:
                return
        except UndefinedTable:
            pass
        cursor.execute("SELECT actual_state,last_error FROM synchdb_control_view WHERE name=%s", (name,))
        state = cursor.fetchone()
        assert state and state[0] not in ("FAILED", "RETRY_WAIT", "STOPPED"), state
        assert time.monotonic() < deadline, f"TRUNCATE target did not converge: {state}"
        time.sleep(0.2)


def _wait_stats(cursor, name, expected_truncates, timeout=300):
    """Target rows commit before the worker publishes its shared statistics."""
    expected = (expected_truncates, 0)
    deadline = time.monotonic() + timeout
    while True:
        cursor.execute("""
            SELECT c.truncates, g.bad_events
            FROM synchdb_cdcstats c JOIN synchdb_genstats g USING (name)
            WHERE c.name=%s
        """, (name,))
        actual = cursor.fetchone()
        cursor.execute("SELECT actual_state,last_error FROM synchdb_control_view WHERE name=%s", (name,))
        state = cursor.fetchone()
        detail = (f"TRUNCATE statistics for {name}: expected={expected}, "
                  f"actual={actual}, worker={state}")
        assert state and state[0] not in ("FAILED", "RETRY_WAIT", "STOPPED"), detail
        if actual is not None:
            assert actual[1] == 0, detail
            assert 0 <= actual[0] <= expected_truncates, detail
            if actual == expected:
                return
        assert time.monotonic() < deadline, f"TRUNCATE statistics did not converge: {detail}"
        time.sleep(0.2)


@pytest.mark.parametrize("use_spi", [False, True], ids=["tuple", "spi"])
@pytest.mark.parametrize("skip_truncate", [False, True], ids=["apply", "skip"])
def test_TruncateMappedTable(pg_cursor, dbvendor, use_spi, skip_truncate):
    if dbvendor == "sqlserver":
        pytest.skip("SQL Server prohibits TRUNCATE on CDC-enabled tables")
    name = "truncate_test"
    source_table = "synchdb_truncate_test"
    target = '"truncate target"."table ""data"'
    pg_cursor.execute("SHOW synchdb.dml_use_spi")
    previous_spi = pg_cursor.fetchone()[0]
    pg_cursor.execute("SHOW synchdb.dbz_skipped_operations")
    previous_skip = pg_cursor.fetchone()[0]
    source_created = False
    try:
        common.update_guc_conf(pg_cursor, "synchdb.dml_use_spi", "on" if use_spi else "off", True)
        common.update_guc_conf(pg_cursor, "synchdb.dbz_skipped_operations",
                               "'t'" if skip_truncate else "'none'", True)
        common.run_remote_query(dbvendor,
            f"CREATE TABLE {source_table} (id INTEGER PRIMARY KEY, payload VARCHAR(32))")
        source_created = True
        common.run_remote_statements(dbvendor, [
            f"INSERT INTO {source_table} VALUES (1,'before')", "COMMIT"])

        database, schema = common.getDbname(dbvendor), common.getSchema(dbvendor)
        source_name = source_table if dbvendor in ("mysql", "postgres") else source_table.upper()
        source_id = f"{database}.{source_name}" if dbvendor == "mysql" else f"{database}.{schema}.{source_name}"
        selection = f"{schema}.{source_table}" if dbvendor == "postgres" else None
        if dbvendor == "postgres":
            pg_cursor.execute('CREATE SCHEMA "truncate target"')
            pg_cursor.execute(f"CREATE TABLE {target} (id INTEGER PRIMARY KEY, payload VARCHAR(32))")
        common.create_synchdb_connector(pg_cursor, dbvendor, name, srctable=selection)
        pg_cursor.execute("SELECT synchdb_add_objmap(%s,'table',%s::text,%s::text)",
                          (name, source_id, target))
        pg_cursor.execute("SELECT synchdb_start_engine_bgw(%s,'initial')", (name,))
        common.wait_for_operation(pg_cursor, pg_cursor.fetchone()[0])
        if dbvendor in ("oracle", "oracle23ai", "olr"):
            common.wait_for_native_snapshot(pg_cursor, name)
        before = [(1, "before")]
        _wait_rows(pg_cursor, name, target, before)

        if dbvendor == "postgres":
            # A rolled-back source TRUNCATE must not clear the target. The
            # subsequent committed row proves the source stream has caught up.
            common.run_remote_statements(dbvendor, [
                "BEGIN", f"TRUNCATE TABLE {source_table}", "ROLLBACK",
                f"INSERT INTO {source_table} VALUES (11,'after rollback')"])
            before.append((11, "after rollback"))
            _wait_rows(pg_cursor, name, target, before)

        common.run_remote_query(dbvendor, f"TRUNCATE TABLE {source_table}")
        common.run_remote_statements(dbvendor, [
            f"INSERT INTO {source_table} VALUES (2,'after')", "COMMIT"])
        expected = sorted(before + [(2, "after")]) if skip_truncate else [(2, "after")]
        _wait_rows(pg_cursor, name, target, expected)
        _wait_stats(pg_cursor, name, 0 if skip_truncate else 1)
    finally:
        pg_cursor.execute("SELECT 1 FROM synchdb_conninfo WHERE name=%s", (name,))
        if pg_cursor.fetchone():
            common.stop_and_delete_synchdb_connector(pg_cursor, name)
            common.drop_repslot_and_pub(dbvendor, name, "postgres")
        if source_created:
            common.run_remote_query(dbvendor, f"DROP TABLE {source_table}")
        pg_cursor.execute('DROP SCHEMA IF EXISTS "truncate target" CASCADE')
        if dbvendor != "postgres":
            common.drop_default_pg_schema(pg_cursor, dbvendor)
        common.update_guc_conf(pg_cursor, "synchdb.dml_use_spi", previous_spi, True)
        common.update_guc_conf(pg_cursor, "synchdb.dbz_skipped_operations",
                               "'" + previous_skip.replace("'", "''") + "'", True)
        if dbvendor == "olr":
            # Oracle Flashback cannot cross the table DDL metadata interval.
            time.sleep(15)


@pytest.mark.parametrize("use_spi", [False, True], ids=["tuple", "spi"])
def test_TruncateFailureAndResume(pg_cursor, dbvendor, use_spi):
    if dbvendor != "postgres":
        pytest.skip("PostgreSQL source replay after a rejected target TRUNCATE")
    name = "truncate_recovery"
    source_table = "synchdb_truncate_recovery"
    target = '"truncate recovery".items'
    pg_cursor.execute("SHOW synchdb.dml_use_spi")
    previous_spi = pg_cursor.fetchone()[0]
    pg_cursor.execute("SHOW synchdb.dbz_skipped_operations")
    previous_skip = pg_cursor.fetchone()[0]
    source_created = False
    try:
        common.update_guc_conf(pg_cursor, "synchdb.dml_use_spi", "on" if use_spi else "off", True)
        common.update_guc_conf(pg_cursor, "synchdb.dbz_skipped_operations", "'none'", True)
        common.run_remote_statements(dbvendor, [
            f"CREATE TABLE {source_table} (id INTEGER PRIMARY KEY, payload VARCHAR(32))",
            f"INSERT INTO {source_table} VALUES (1,'before')"])
        source_created = True
        pg_cursor.execute('CREATE SCHEMA "truncate recovery"')
        pg_cursor.execute(f"CREATE TABLE {target} (id INTEGER PRIMARY KEY, payload VARCHAR(32))")
        source_id = f"{common.getDbname(dbvendor)}.{common.getSchema(dbvendor)}.{source_table}"
        common.create_synchdb_connector(pg_cursor, dbvendor, name,
            srctable=f"{common.getSchema(dbvendor)}.{source_table}")
        pg_cursor.execute("SELECT synchdb_add_objmap(%s,'table',%s::text,%s::text)", (name, source_id, target))
        pg_cursor.execute("SELECT synchdb_start_engine_bgw(%s,'initial')", (name,))
        common.wait_for_operation(pg_cursor, pg_cursor.fetchone()[0])
        _wait_rows(pg_cursor, name, target, [(1, "before")])
        pg_cursor.execute('''CREATE FUNCTION "truncate recovery".reject_truncate()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'reject truncate test'; END $$''')
        pg_cursor.execute(f'''CREATE TRIGGER reject_truncate BEFORE TRUNCATE ON {target}
            EXECUTE FUNCTION "truncate recovery".reject_truncate()''')
        common.run_remote_statements(dbvendor, [f"TRUNCATE TABLE {source_table}",
            f"INSERT INTO {source_table} VALUES (2,'after')"])
        deadline = time.monotonic() + 300
        while True:
            pg_cursor.execute("SELECT actual_state,last_sqlstate,last_error FROM synchdb_control_view WHERE name=%s", (name,))
            state = pg_cursor.fetchone()
            if state[0] == "FAILED":
                break
            assert time.monotonic() < deadline, state
            time.sleep(0.2)
        assert state[1] == "P0001" and "reject truncate test" in state[2], state
        pg_cursor.execute(f"SELECT id,payload FROM {target}")
        assert pg_cursor.fetchall() == [(1, "before")]
        pg_cursor.execute(f"DROP TRIGGER reject_truncate ON {target}")
        pg_cursor.execute("SELECT synchdb_start_engine_bgw(%s)", (name,))
        common.wait_for_operation(pg_cursor, pg_cursor.fetchone()[0])
        # Replaying the unacknowledged truncate removes the old row. Merely
        # continuing after it would leave both rows and fail this assertion.
        _wait_rows(pg_cursor, name, target, [(2, "after")])
        _wait_stats(pg_cursor, name, 1)
    finally:
        pg_cursor.execute("SELECT 1 FROM synchdb_conninfo WHERE name=%s", (name,))
        if pg_cursor.fetchone():
            common.stop_and_delete_synchdb_connector(pg_cursor, name)
            common.drop_repslot_and_pub(dbvendor, name, "postgres")
        if source_created:
            common.run_remote_query(dbvendor, f"DROP TABLE {source_table}")
        pg_cursor.execute('DROP SCHEMA IF EXISTS "truncate recovery" CASCADE')
        common.update_guc_conf(pg_cursor, "synchdb.dml_use_spi", previous_spi, True)
        common.update_guc_conf(pg_cursor, "synchdb.dbz_skipped_operations",
                               "'" + previous_skip.replace("'", "''") + "'", True)
