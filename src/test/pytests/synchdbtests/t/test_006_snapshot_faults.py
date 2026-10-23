import time
import common
import pytest

from common import (
    run_pg_query, run_pg_query_one, run_remote_query,
    getConnectorName, getDbname, getSchema,
    create_synchdb_connector,
    create_and_start_synchdb_connector, stop_and_delete_synchdb_connector,
    drop_default_pg_schema, drop_repslot_and_pub, update_guc_conf
)

def wait_for_snapshot_complete(cursor, name, timeout=120, interval=2):
    """Poll until the connector leaves the initial-snapshot stage.

    Returns the final (stage, state, err).  'change data capture' + 'polling'
    means success; 'paused' with a non-'no error' err means partial failure.
    """

    time.sleep(5) # sleep to wait for connector starting
    
    deadline = time.time() + timeout
    last = (None, None, None)
    while time.time() < deadline:
        row = run_pg_query_one(
            cursor,
            f"SELECT stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
        last = (None, None, None) if row is None else (row[0], row[1], row[2])
        print(row)
        stage, state, err = last
        if stage not in ("initial snapshot", "schema sync"):
            return last
        if state in ("paused", "stopped"):
            return last
        time.sleep(interval)
    print(f"wait_for_snapshot_complete time out: from {deadline - timeout} to {time.time()}")
    return last


def test_FailThenRestartFDW(pg_cursor, dbvendor, fdw_engine, target):
    if dbvendor == "mysql" and target.key == "ivorysql5":
        pytest.skip("TODO: IvorySQL 5.4 not support mysql_fdw yet")

    BIG_VALUE = 9223372036854775807

    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)
    source_prefix = getDbname(dbvendor) if schema is None else f"{getDbname(dbvendor)}.{schema}"
    suffix = "BAD_TABLE_1.ORDER_ID" if dbvendor in ("oracle", "oracle23ai", "olr") else "bad_table_1.order_id"
    source_column = f"{source_prefix}.{suffix}"
    selection = ','.join(f"{schema or dbname}.bad_table_{i}" for i in range(3))
    name = getConnectorName(dbvendor) + "_fdwfailretry"
            
    if dbvendor == "mysql":
        query_pattern = """
        CREATE TABLE bad_table_{} (
        id INT NOT NULL,
        order_id BIGINT,
        PRIMARY KEY(id)
        );
        """
    elif dbvendor == "postgres":
        query_pattern = """
        CREATE TABLE bad_table_{} (
        id INT NOT NULL,
        order_id BIGINT,
        PRIMARY KEY(id)
        );
        """
    elif dbvendor == "sqlserver":
        query_pattern = """
        CREATE TABLE bad_table_{} (
        id INT NOT NULL,
        order_id BIGINT,
        PRIMARY KEY(id)
        );
        """
    else:
        query_pattern = """
        CREATE TABLE bad_table_{} (
        id NUMBER(10) NOT NULL,
        order_id NUMBER(19),
        PRIMARY KEY(id)
        );
        """

    # Create three tables with bigint values
    for i in range(3):
        run_remote_query(dbvendor, query_pattern.format(str(i)))
    if dbvendor in ("oracle", "oracle23ai", "olr"):
        # Let every new table become Flashback-readable before committing its
        # fixture data, just as in the FDW bootstrap data-copy cases.
        time.sleep(15)
    for i in range(3):
        run_remote_query(dbvendor, "INSERT INTO bad_table_{} values ({}, {})".format(str(i), i, BIG_VALUE))

    create_synchdb_connector(pg_cursor, dbvendor, name, srctable=selection)

    # A failed table rolls back independently; the read session then ends.
    run_pg_query(pg_cursor, f"SELECT synchdb_add_objmap('{name}','datatype','{source_column}','smallint');")
    pg_cursor.execute("SELECT synchdb_start_engine_bgw(%s, 'initial_only')", (name,))
    receipt = common.wait_for_operation(pg_cursor, pg_cursor.fetchone()[0], expected_status="FAILED")
    assert receipt["sqlstate"] == "38000", receipt
    result = common.get_bootstrap_result(pg_cursor, name)
    assert result["status"] == "PARTIAL", result
    assert result["retry_disposition"] == "RESTART_BOOTSTRAP"
    failed = [t for t in result["tables"] if t["status"] == "FAILED"]
    assert len(failed) == 1
    assert failed[0]["source"]["table"].lower() == "bad_table_1"
    assert failed[0]["sqlstate"] == "22003", failed
    assert result["cdc_eligible"] is False
    assert result["resources_disposition"] == "CLEANED"
    assert run_pg_query_one(pg_cursor, f"SELECT to_regclass('{dbname}.bad_table_1')") == (None,)
    assert run_pg_query_one(pg_cursor, f"SELECT order_id FROM {dbname}.bad_table_0") == (BIG_VALUE,)

    # RESUME cannot recreate the expired source read view or enter CDC.
    pg_cursor.execute("SELECT synchdb_resume_engine(%s)", (name,))
    receipt = common.wait_for_operation(pg_cursor, pg_cursor.fetchone()[0], expected_status="FAILED")
    assert receipt["sqlstate"] == "0A000", receipt
    pg_cursor.execute("SELECT synchdb_stop_engine_bgw(%s)", (name,))
    common.wait_for_operation(pg_cursor, pg_cursor.fetchone()[0])
    run_pg_query(pg_cursor, f"SELECT synchdb_del_objmap('{name}','datatype','{source_column}');")
    # Discard this test's partial target before a fresh complete bootstrap.
    drop_default_pg_schema(pg_cursor, dbvendor)
    pg_cursor.execute("SELECT synchdb_start_engine_bgw(%s, 'initial_only')", (name,))
    common.wait_for_operation(pg_cursor, pg_cursor.fetchone()[0])
    retried = common.get_bootstrap_result(pg_cursor, name)
    assert retried["status"] == "COMPLETE", retried
    assert retried["generation"] != result["generation"]
    for i in range(3):
        assert run_pg_query_one(pg_cursor, f"SELECT order_id FROM {dbname}.bad_table_{i}") == (BIG_VALUE,)

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    for i in range(3):
        run_remote_query(dbvendor, f"DROP TABLE bad_table_{i}")


@pytest.mark.skip("TODO: implement after upgrade Debezium")
def test_FailThenRetryDebezium(pg_cursor, dbvendor):
    if dbvendor == "postgres":
        pytest.skip("TODO: postgres cannot be tested yet")
        '''
        # TODO: with postgres, table need to be created mannually
        # changing datatype mappings may not proper
        if dbvendor == "postgres":
            # postgres in debezium snapshot needs to create tables manually
            run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
            run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")
            
            for i in range(3):
                # Note: order_id is smallint here
                run_pg_query_one(pg_cursor, """CREATE TABLE {}.bad_table_{} (
                id INT NOT NULL,
                order_id smallint,
                PRIMARY KEY(id)
                );
                """.format(dbname, str(i)))
        '''

    BIG_VALUE = 9223372036854775807

    dbname = getDbname(dbvendor).lower()
    name = getConnectorName(dbvendor) + "_debeziumfailretry"

    if dbvendor == "mysql":
        query_pattern = """
        CREATE TABLE bad_table_{} (
        id INT NOT NULL,
        order_id BIGINT,
        PRIMARY KEY(id)
        );
        """
    elif dbvendor == "postgres":
        query_pattern = """
        CREATE TABLE bad_table_{} (
        id INT NOT NULL,
        order_id BIGINT,
        PRIMARY KEY(id)
        );
        """
    elif dbvendor == "sqlserver":
        query_pattern = """
        CREATE TABLE bad_table_{} (
        id INT NOT NULL,
        order_id BIGINT,
        PRIMARY KEY(id)
        );
        """
    else:
        query_pattern = """
        CREATE TABLE bad_table_{} (
        id NUMBER(10) NOT NULL,
        order_id NUMBER(19),
        PRIMARY KEY(id)
        );
        """

    # Create three tables with bigint values
    for i in range(3):
        run_remote_query(dbvendor, query_pattern.format(str(i)))
        run_remote_query(dbvendor, "INSERT INTO bad_table_{} values ({}, {})".format(str(i), i, BIG_VALUE))

    create_synchdb_connector(pg_cursor, dbvendor, name)

    # 1. create wrong datatype mapping so that the snapshot will fail
    run_pg_query(pg_cursor, f"SELECT synchdb_add_objmap('{name}','datatype','{dbname}.bad_table_1.order_id','smallint');")

    # 2. start connector
    run_pg_query_one(pg_cursor, f"SELECT synchdb_start_engine_bgw('{name}')")
    stage, state, err = wait_for_snapshot_complete(pg_cursor, name, timeout=200)

    if state != "paused":
        print(f"Unexpected: stage: {stage}, state: {state}, err: {err}")
    assert state == "stopped"
    assert "is out of range" in err

    # 3. remove objmap then resume connector
    run_pg_query(pg_cursor, f"SELECT synchdb_del_objmap('{name}','datatype','{dbname}.bad_table_1.order_id');")
    run_pg_query_one(pg_cursor, f"SELECT synchdb_start_engine_bgw('{name}')")

    stage, state, err = wait_for_snapshot_complete(pg_cursor, name, timeout=100)
    assert state == "polling"

    ret = run_pg_query_one(pg_cursor, f"SELECT * from {dbname}.bad_table_1;")
    assert ret[1] == BIG_VALUE

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    for i in range(3):
        run_remote_query(dbvendor, f"DROP TABLE bad_table_{i}")
