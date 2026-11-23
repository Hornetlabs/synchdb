"""Native OLR snapshot/CDC boundaries, atomic apply and durable recovery."""
import selectors
import subprocess
import time

import pytest

import common
from t.test_001_initialsnapshot import wait_for_pg_row


def _start(cursor, name, mode="initial"):
    common.create_and_start_synchdb_connector(cursor, "olr", name, mode)


def _cleanup(cursor, name, keys):
    common.run_remote_query("olr", f"DELETE FROM orders WHERE order_number IN ({keys})")
    common.stop_and_delete_synchdb_connector(cursor, name)
    common.drop_default_pg_schema(cursor, "olr")


def test_OLRTransactionCrossesSnapshot(pg_cursor, dbvendor):
    if dbvendor != "olr":
        pytest.skip("native OLR source transaction boundary")
    name = "olr_cross_snapshot"
    host = common.resolve_host("ora19c")
    session = subprocess.Popen(
        ["docker", "exec", "-i", "ora19c", "sqlplus", "-S",
         f"{common.ORA19C_USER}/{common.ORA19C_PASS}@//{host}:1521/FREE"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )
    try:
        session.stdin.write("WHENEVER SQLERROR EXIT SQL.SQLCODE\nSET FEEDBACK OFF\n"
                            "INSERT INTO orders VALUES (10881,SYSDATE,1002,8881,102);\n"
                            "PROMPT SYNCHDB_TRANSACTION_OPEN\n")
        session.stdin.flush()
        with selectors.DefaultSelector() as selector:
            selector.register(session.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + 30
            while True:
                assert selector.select(max(0, deadline - time.monotonic())), "Oracle transaction did not open"
                line = session.stdout.readline()
                assert line, f"Oracle transaction session exited: {session.poll()}"
                if "SYNCHDB_TRANSACTION_OPEN" in line:
                    break
        _start(pg_cursor, name)
        pg_cursor.execute("SELECT count(*) FROM free.orders WHERE order_number=10881")
        assert pg_cursor.fetchone() == (0,), "uncommitted source row entered the snapshot"
        session.stdin.write("COMMIT;\nEXIT\n")
        session.stdin.flush()
        output = session.communicate(timeout=30)[0]
        assert session.returncode == 0, output
        assert wait_for_pg_row(pg_cursor,
            "SELECT quantity FROM free.orders WHERE order_number=10881", timeout=300) == (8881,)
        assert wait_for_pg_row(pg_cursor,
            f"SELECT txs FROM synchdb_cdcstats WHERE name='{name}' AND txs > 0",
            timeout=30)[0] > 0
    finally:
        if session.poll() is None:
            session.stdin.write("ROLLBACK;\nEXIT\n")
            session.stdin.flush()
            session.communicate(timeout=30)
        _cleanup(pg_cursor, name, "10881")


def test_OLRApplyFailureRollsBackWholeTransaction(pg_cursor, dbvendor):
    if dbvendor != "olr":
        pytest.skip("native OLR transactional cursor")
    name = "olr_atomic_recovery"
    try:
        _start(pg_cursor, name)
        pg_cursor.execute("ALTER TABLE free.orders ADD CONSTRAINT olr_reject_second CHECK (quantity <> 8883)")
        common.run_remote_query("olr", """
            INSERT ALL
              INTO orders VALUES (10882,SYSDATE,1002,8882,102)
              INTO orders VALUES (10883,SYSDATE,1002,8883,102)
            SELECT 1 FROM dual
        """)
        deadline = time.monotonic() + 300
        while True:
            pg_cursor.execute("SELECT actual_state,last_error FROM synchdb_control_view WHERE name=%s", (name,))
            state = pg_cursor.fetchone()
            if state[0] == "FAILED":
                break
            assert time.monotonic() < deadline, state
            time.sleep(1)
        pg_cursor.execute("SELECT count(*) FROM free.orders WHERE order_number IN (10882,10883)")
        assert pg_cursor.fetchone() == (0,), "a source transaction was partially committed"
        pg_cursor.execute("ALTER TABLE free.orders DROP CONSTRAINT olr_reject_second")
        pg_cursor.execute("SELECT synchdb_start_engine_bgw(%s)", (name,))
        common.wait_for_operation(pg_cursor, pg_cursor.fetchone()[0])
        assert wait_for_pg_row(pg_cursor,
            "SELECT count(*) FROM free.orders WHERE order_number IN (10882,10883) HAVING count(*)=2",
            timeout=300) == (2,)
        pg_cursor.execute("SELECT order_number,quantity FROM free.orders WHERE order_number IN (10882,10883) ORDER BY 1")
        assert pg_cursor.fetchall() == [(10882,8882),(10883,8883)]
    finally:
        _cleanup(pg_cursor, name, "10882,10883")


def test_OLRNativeSnapshotOnlySelection(pg_cursor, dbvendor):
    if dbvendor != "olr":
        pytest.skip("native OLR snapshot-only selection")
    name = "olr_snapshot_only_selected"
    try:
        common.create_and_start_synchdb_connector(pg_cursor, "olr", name,
            "initial_only", srctable=f"{common.getSchema('olr')}.ORDERS",
            snapshot_table=f"{common.ORA19C_DB}.{common.getSchema('olr')}.ORDERS")
        common.wait_for_snapshot_only(pg_cursor, name, timeout=300)
        pg_cursor.execute("SELECT count(*) FROM free.orders")
        assert pg_cursor.fetchone()[0] == int(common.run_remote_query("olr", "SELECT count(*) FROM orders")[0][0])
        pg_cursor.execute("""
            SELECT count(*) FROM synchdb_olr_positions p
            JOIN synchdb_conninfo c USING (connector_id) WHERE c.name=%s
        """, (name,))
        assert pg_cursor.fetchone() == (0,), "snapshot-only run authorized an OLR handoff"
    finally:
        common.stop_and_delete_synchdb_connector(pg_cursor, name)
        common.drop_default_pg_schema(pg_cursor, "olr")
