import common
from common import wait_for_operation
import time
from datetime import datetime
from common import run_pg_query, run_pg_query_one, run_remote_query, create_synchdb_connector, getConnectorName, getDbname, verify_default_type_mappings, stop_and_delete_synchdb_connector, drop_default_pg_schema, create_and_start_synchdb_connector, update_guc_conf, getSchema, drop_repslot_and_pub, restart_remote_db

import pytest
from psycopg2 import sql
from psycopg2.errors import FeatureNotSupported


def wait_for_connector_cdc(cursor, name, timeout=180, interval=2):
    """Wait until a connector is polling in the CDC stage."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = run_pg_query_one(
            cursor,
            f"SELECT stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
        if last is not None:
            stage, state, err = last
            if stage == "change data capture" and state == "polling":
                return last
            if state in ("paused", "stopped") and err != "no error":
                pytest.fail(f"connector failed before CDC: stage={stage}, state={state}, err={err}")
        time.sleep(interval)
    pytest.fail(f"connector did not reach CDC within {timeout}s; last state: {last}")


def wait_for_pg_row(cursor, query, timeout=60, interval=1):
    """Poll a PostgreSQL query until it returns one row."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = run_pg_query_one(cursor, query)
        if row is not None:
            return row
        time.sleep(interval)
    pytest.fail(f"row did not arrive within {timeout}s: {query}")


def test_ConnectorCreate(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    result = create_synchdb_connector(pg_cursor, dbvendor, name)
    assert result[0] == 0

    result = run_pg_query_one(pg_cursor, f"SELECT name, isactive, data->>'connector' FROM synchdb_conninfo WHERE name = '{name}'")
    assert result[0] == name
    assert result[1] == False
    if dbvendor in ('oracle', 'oracle23ai'):
        assert result[2] == 'oracle'
    else:
        assert result[2] == dbvendor

    result = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_extra_conninfo('{name}', 'verify_ca', '/path/ks', 'kspass', '/path/ts/', 'tspass')")
    assert result[0] == 0

    result = run_pg_query_one(pg_cursor, f"SELECT data->>'ssl_mode', data->>'ssl_keystore', data->>'ssl_keystore_pass', data->>'ssl_truststore', data->>'ssl_truststore_pass' FROM synchdb_conninfo WHERE name = '{name}'")
    assert result[0] == "verify_ca"
    assert result[1] == "/path/ks"
    assert result[3] == "/path/ts/"

    stop_and_delete_synchdb_connector(pg_cursor, name)


def test_CreateExtraConninfo(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()
    result = create_synchdb_connector(pg_cursor, dbvendor, name)
    assert result[0] == 0

    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_extra_conninfo('{name}', 'verify_ca', 'keystore', 'pass', 'truststore', 'pass')")
    assert row[0] == 0

    row = run_pg_query_one(pg_cursor, f"SELECT data->'ssl_mode', data->'ssl_keystore', data->'ssl_truststore' FROM synchdb_conninfo WHERE name = '{name}'")
    assert row[0] == "verify_ca"
    assert row[1] == "keystore"
    assert row[2] == "truststore"

    stop_and_delete_synchdb_connector(pg_cursor, name)


def test_RemoveExtraConninfo(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()
    result = create_synchdb_connector(pg_cursor, dbvendor, name)
    assert result[0] == 0

    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_extra_conninfo('{name}', 'verify_ca', 'keystore', 'pass', 'truststore', 'pass')")
    assert row[0] == 0

    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_del_extra_conninfo('{name}')")
    assert row[0] == 0

    row = run_pg_query_one(pg_cursor, f"SELECT data->'ssl_mode', data->'ssl_keystore', data->'ssl_truststore' FROM synchdb_conninfo WHERE name = '{name}'")
    assert row[0] == None
    assert row[1] == None
    assert row[2] == None

    stop_and_delete_synchdb_connector(pg_cursor, name)


def test_ConnectorStart(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # Native PostgreSQL capture owns the snapshot-to-CDC boundary. Its
        # snapshot supplies rows, so provision the target table explicitly.
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int, product_id int)")

    result = create_synchdb_connector(pg_cursor, dbvendor, name)
    assert result[0] == 0

    if dbvendor == "sqlserver":
        # The default initial mode must fail before an artifact is acquired.
        with pytest.raises(FeatureNotSupported, match="capture-drain fence"):
            run_pg_query_one(pg_cursor, f"SELECT synchdb_start_engine_bgw('{name}')")
        pg_cursor.execute("""
            SELECT count(*) FROM synchdb_artifacts a
            JOIN synchdb_conninfo c USING (connector_id) WHERE c.name = %s
        """, (name,))
        assert pg_cursor.fetchone()[0] == 0
        row = run_pg_query_one(pg_cursor, f"SELECT synchdb_start_engine_bgw('{name}', 'no_data')")
    else:
        row = run_pg_query_one(pg_cursor, f"SELECT synchdb_start_engine_bgw('{name}')")
    wait_for_operation(pg_cursor, row[0])

    # oracle takes longer to start initial snapshot
    if dbvendor in ("oracle", "oracle23ai"):
        common.wait_for_native_snapshot(pg_cursor, name)
    elif dbvendor == "olr":
        time.sleep(20)
    else:
        time.sleep(10)

    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    if dbvendor in ('oracle', 'oracle23ai'):
        assert row[1] == 'oracle'
    else:
        assert row[1] == dbvendor
    assert row[2] > -1
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
    assert row[4] == "polling"
    assert row[5] == "no error"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")


def test_InitialSnapshotDBZ(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_dbzsnap"
    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)
    
    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    mode = "initial_only" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    if dbvendor in ("oracle", "oracle23ai", "olr"):
        time.sleep(30)
    else:
        time.sleep(10)

    if dbvendor == "sqlserver":
        common.wait_for_snapshot_only(pg_cursor, name)

    # check table counts
    pgtblcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM information_schema.tables where table_schema='{dbname}' and table_type = 'BASE TABLE'")
    if dbvendor == "mysql":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()")
    elif dbvendor == "sqlserver":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_CATALOG=DB_NAME() AND TABLE_SCHEMA=schema_name() AND TABLE_NAME NOT LIKE 'systranschemas%'")
    elif dbvendor == "postgres":
        exttblcount = run_remote_query(dbvendor, f"SELECT count(*) FROM information_schema.tables where table_schema='{schema}' and table_type = 'BASE TABLE'")
    else:
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%' AND table_name <> 'SYNCHDB_OLR_FENCE'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])
    
    # check row counts or orders table
    pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders")
    extrowcount = run_remote_query(dbvendor, f"SELECT count(*) FROM orders")
    assert int(pgrowcount[0]) == int(extrowcount[0][0])

    if dbvendor != "postgres":
        # check table name mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            id = row[0].split(".")
            if len(id) == 3:
                assert id[0].lower() + "." + id[2].lower() == row[1]
            else:
                assert row[0].lower() == row[1]
        # check attname mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert row[0].lower() == row[1]

        # check data type mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM {dbname}.orders WHERE order_number = 10003")
    extrow = run_remote_query(dbvendor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM orders WHERE order_number = 10003")
    assert int(pgrow[0]) == int(extrow[0][0])
    if dbvendor in ("oracle", "oracle23ai", "olr"):
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%d-%b-%y')
    elif dbvendor == "postgres":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d %H:%M:%S')
    else:
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d').date()
    assert int(pgrow[2]) == int(extrow[0][2])
    assert int(pgrow[3]) == int(extrow[0][3])
    assert int(pgrow[4]) == int(extrow[0][4])

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")


@pytest.mark.parametrize("casing", ["lowercase", "uppercase", "asis"])
@pytest.mark.parametrize("mode", ["initial_only", "schemasync"])
def test_FDWBootstrap(pg_cursor, dbvendor, fdw_engine, mode, casing):
    name = getConnectorName(dbvendor) + "_fdw_bootstrap"
    dbname = getDbname(dbvendor)
    source_table = "fdw_bootstrap_test"
    table = source_table.upper() if dbvendor in ("oracle", "oracle23ai", "olr") else source_table
    columns = ["id", "note"]
    source_scope = getSchema(dbvendor) or dbname
    selection = f"{source_scope}.{source_table}"
    if casing == "uppercase":
        dbname, table = dbname.upper(), table.upper()
        columns = [column.upper() for column in columns]
    elif casing == "lowercase":
        dbname, table = dbname.lower(), table.lower()
    elif dbvendor in ("oracle", "oracle23ai", "olr"):
        columns = [column.upper() for column in columns]
    update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", f"'{casing}'", True)
    # A dedicated selected table makes snapshot coverage independent of source
    # fixture sequences, whose state FDW bootstrap deliberately cannot copy.
    run_remote_query(dbvendor, f"CREATE TABLE {source_table} (id INTEGER PRIMARY KEY, note VARCHAR(20))")
    try:
        if dbvendor in ("oracle", "oracle23ai", "olr") and mode == "initial_only":
            # Oracle requires at least 15 seconds after CREATE TABLE before
            # committing fixture data used by Flashback Query. A fresh table
            # can otherwise reject even a current AS OF SCN with ORA-01466.
            # https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/flashback.html
            time.sleep(15)
        run_remote_query(dbvendor, f"INSERT INTO {source_table} VALUES (1, 'snapshot row')")
        create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode, srctable=selection)
        result = common.get_bootstrap_result(pg_cursor, name)
        assert result["status"] == "COMPLETE", result
        assert result["cdc_eligible"] is False
        assert result["cdc_resume_material"] is None
        assert result["resources_disposition"] == "CLEANED"
        assert len(result["tables"]) == 1, result
        copied = result["tables"][0]
        assert copied["target_name"] == table
        assert copied["target_schema"] == dbname
        assert copied["status"] == "COMPLETE"
        pg_cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position
        """, (dbname, table))
        assert [row[0] for row in pg_cursor.fetchall()] == columns
        # Compare actual snapshot rows; schema-only bootstrap must stay empty.
        pg_cursor.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(
            sql.Identifier(dbname), sql.Identifier(table)))
        expected = 1 if mode == "initial_only" else 0
        assert pg_cursor.fetchone()[0] == expected
        assert copied["rows"] == expected
        if mode == "initial_only":
            pg_cursor.execute(sql.SQL("SELECT {} FROM {}.{} WHERE {} = 1").format(
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.Identifier(dbname), sql.Identifier(table), sql.Identifier(columns[0])))
            row = pg_cursor.fetchone()
            assert row == (1, "snapshot row")
        else:
            # A metadata bootstrap is paused. RESUME must not invent a CDC
            # boundary; its asynchronous receipt reports the rejection.
            pg_cursor.execute("SELECT synchdb_resume_engine(%s)", (name,))
            receipt = wait_for_operation(pg_cursor, pg_cursor.fetchone()[0], expected_status="FAILED")
            assert receipt["sqlstate"] == "0A000", receipt
            pg_cursor.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(
                sql.Identifier(dbname), sql.Identifier(table)))
            assert pg_cursor.fetchone()[0] == 0
        stop_and_delete_synchdb_connector(pg_cursor, name)
        drop_default_pg_schema(pg_cursor, dbvendor)
    finally:
        run_remote_query(dbvendor, f"DROP TABLE {source_table}")
        update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", "'lowercase'", True)


@pytest.mark.parametrize("mode", ["initial", "always", "no_data", "never"])
def test_FDWRejectsUnverifiedHandoff(pg_cursor, dbvendor, fdw_config, mode):
    name = getConnectorName(dbvendor) + "_fdw_reject"
    create_synchdb_connector(pg_cursor, dbvendor, name)
    with pytest.raises(FeatureNotSupported, match="verified CDC handoff"):
        pg_cursor.execute("SELECT synchdb_start_engine_bgw(%s, %s)", (name, mode))
    pg_cursor.execute("""
        SELECT count(*) FROM synchdb_artifacts a
        JOIN synchdb_conninfo c USING (connector_id) WHERE c.name = %s
    """, (name,))
    assert pg_cursor.fetchone()[0] == 0
    stop_and_delete_synchdb_connector(pg_cursor, name)



def test_InitialSnapshotDBZ_uppercase(pg_cursor, dbvendor):
    
    if dbvendor == "oracle23ai":
        restart_remote_db(dbvendor)

    name = getConnectorName(dbvendor) + "_dbzsnap_upper"
    dbname = getDbname(dbvendor).upper()
    schema = getSchema(dbvendor)

    update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", "'uppercase'", True)

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS \"{dbname}\"")
        run_pg_query_one(pg_cursor, f"CREATE TABLE \"{dbname}\".\"ORDERS\" (\"ORDER_NUMBER\" int primary key, \"ORDER_DATE\" timestamp without time zone, \"PURCHASER\" int, \"QUANTITY\" int , \"PRODUCT_ID\" int)")

    mode = "initial_only" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    if dbvendor in ("oracle", "oracle23ai", "olr"):
        time.sleep(30)
    else:
        time.sleep(10)

    if dbvendor == "sqlserver":
        common.wait_for_snapshot_only(pg_cursor, name)

    # check table counts
    pgtblcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM information_schema.tables where table_schema='{dbname}' and table_type = 'BASE TABLE'")
    if dbvendor == "mysql":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()")
    elif dbvendor == "sqlserver":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_CATALOG=DB_NAME() AND TABLE_SCHEMA=schema_name() AND TABLE_NAME NOT LIKE 'systranschemas%'")
    elif dbvendor == "postgres":
        exttblcount = run_remote_query(dbvendor, f"SELECT count(*) FROM information_schema.tables where table_schema='{schema}' and table_type = 'BASE TABLE'")
    else:
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%' AND table_name <> 'SYNCHDB_OLR_FENCE'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    # check row counts or orders table
    pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM \"{dbname}\".\"ORDERS\"")
    extrowcount = run_remote_query(dbvendor, f"SELECT count(*) FROM orders")
    assert int(pgrowcount[0]) == int(extrowcount[0][0])

    if dbvendor != "postgres":
        # check table name mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            id = row[0].split(".")
            if len(id) == 3:
                assert id[0].upper() + "." + id[2].upper() == row[1]
            else:
                assert row[0].upper() == row[1]

        # check attname mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert row[0].upper() == row[1]

        # check data type mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT \"ORDER_NUMBER\", \"ORDER_DATE\", \"PURCHASER\", \"QUANTITY\", \"PRODUCT_ID\" FROM \"{dbname}\".\"ORDERS\" WHERE \"ORDER_NUMBER\" = 10003")
    extrow = run_remote_query(dbvendor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM orders WHERE order_number = 10003")
    assert int(pgrow[0]) == int(extrow[0][0])
    if dbvendor in ("oracle", "oracle23ai", "olr"):
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%d-%b-%y')
    elif dbvendor == "postgres":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d %H:%M:%S')
    else:
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d').date()
    assert int(pgrow[2]) == int(extrow[0][2])
    assert int(pgrow[3]) == int(extrow[0][3])
    assert int(pgrow[4]) == int(extrow[0][4])

    if dbvendor == "sqlserver":
        # Discard the snapshot and start an independent CDC-only generation.
        # This test does not assert a snapshot-to-CDC handoff.
        stop_and_delete_synchdb_connector(pg_cursor, name)
        drop_default_pg_schema(pg_cursor, dbvendor)
        name += "_cdc"
        create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
        wait_for_connector_cdc(pg_cursor, name)
        wait_for_pg_row(pg_cursor, f"SELECT 1 FROM pg_tables WHERE schemaname = '{dbname}' AND tablename = 'ORDERS'")
        assert run_pg_query_one(pg_cursor, f'SELECT count(*) FROM "{dbname}"."ORDERS"') == (0,)

    # test cdc now
    if dbvendor == "mysql":
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, '2025-12-12', 1002, 10000, 102);
        """
    elif dbvendor == "sqlserver":
        query = """
            INSERT INTO orders(order_date, purchaser, quantity, product_id) VALUES
            ('2025-12-12', 1002, 10000, 102)
        """
    elif dbvendor == "postgres":
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, '2025-12-12', 1002, 10000, 102);
        """
    else:
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, TO_DATE('2025-12-12', 'YYYY-MM-DD'),
            1002, 10000, 102);
        """

    run_remote_query(dbvendor, query)
    if dbvendor in ("oracle", "olr"):
        time.sleep(50)
    elif dbvendor == "oracle23ai":
        time.sleep(80)
    else:
        time.sleep(10)

    pgrow = run_pg_query_one(pg_cursor, f"SELECT \"ORDER_NUMBER\", \"ORDER_DATE\", \"PURCHASER\", \"QUANTITY\", \"PRODUCT_ID\" FROM \"{dbname}\".\"ORDERS\" WHERE \"ORDER_NUMBER\" >= 10005")
    assert pgrow != None
    assert int(pgrow[3]) == 10000

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, f"DELETE FROM orders WHERE order_number > 10004")
    update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", "'lowercase'", True)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    time.sleep(10)




def test_InitialSnapshotDBZ_asis(pg_cursor, dbvendor):
    
    if dbvendor == "oracle23ai":
        restart_remote_db(dbvendor)
    
    name = getConnectorName(dbvendor) + "_dbzsnap_asis"
    dbname = getDbname(dbvendor)
    schema = getSchema(dbvendor)

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", "'asis'", True)

    mode = "initial_only" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    if dbvendor in ("oracle", "oracle23ai", "olr"):
        time.sleep(30)
    else:
        time.sleep(10)

    if dbvendor == "sqlserver":
        common.wait_for_snapshot_only(pg_cursor, name)

    # check table counts
    pgtblcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM information_schema.tables where table_schema='{dbname}' and table_type = 'BASE TABLE'")
    if dbvendor == "mysql":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()")
    elif dbvendor == "sqlserver":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_CATALOG=DB_NAME() AND TABLE_SCHEMA=schema_name() AND TABLE_NAME NOT LIKE 'systranschemas%'")
    elif dbvendor == "postgres":
        exttblcount = run_remote_query(dbvendor, f"SELECT count(*) FROM information_schema.tables where table_schema='{schema}' and table_type = 'BASE TABLE'")
    else:
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%' AND table_name <> 'SYNCHDB_OLR_FENCE'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    # check row counts or orders table
    if dbvendor in ("oracle", "oracle23ai", "olr"):
        pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM \"{dbname}\".\"ORDERS\"")
    else:
        pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM \"{dbname}\".\"orders\"")
    extrowcount = run_remote_query(dbvendor, f"SELECT count(*) FROM orders")
    assert int(pgrowcount[0]) == int(extrowcount[0][0])

    if dbvendor != "postgres":
        # check table name mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            id = row[0].split(".")
            if len(id) == 3:
                assert id[0] + "." + id[2] == row[1]
            else:
                assert row[0] == row[1]

        # check attname mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert row[0] == row[1]

        # check data type mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    if dbvendor in ("oracle", "oracle23ai", "olr"):
        pgrow = run_pg_query_one(pg_cursor, f"SELECT \"ORDER_NUMBER\", \"ORDER_DATE\", \"PURCHASER\", \"QUANTITY\", \"PRODUCT_ID\" FROM \"{dbname}\".\"ORDERS\" WHERE \"ORDER_NUMBER\" = 10003")
    else:
        pgrow = run_pg_query_one(pg_cursor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM \"{dbname}\".orders WHERE order_number = 10003")
    extrow = run_remote_query(dbvendor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM orders WHERE order_number = 10003")
    assert int(pgrow[0]) == int(extrow[0][0])
    if dbvendor in ("oracle", "oracle23ai", "olr"):
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%d-%b-%y')
    elif dbvendor == "postgres":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d %H:%M:%S')
    else:
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d').date()
    assert int(pgrow[2]) == int(extrow[0][2])
    assert int(pgrow[3]) == int(extrow[0][3])
    assert int(pgrow[4]) == int(extrow[0][4])

    if dbvendor == "sqlserver":
        # Discard the snapshot and start an independent CDC-only generation.
        # This test does not assert a snapshot-to-CDC handoff.
        stop_and_delete_synchdb_connector(pg_cursor, name)
        drop_default_pg_schema(pg_cursor, dbvendor)
        name += "_cdc"
        create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
        wait_for_connector_cdc(pg_cursor, name)
        wait_for_pg_row(pg_cursor, f"SELECT 1 FROM pg_tables WHERE schemaname = '{dbname}' AND tablename = 'orders'")
        assert run_pg_query_one(pg_cursor, f'SELECT count(*) FROM "{dbname}".orders') == (0,)

    # test cdc now
    if dbvendor == "mysql":
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, '2025-12-12', 1002, 10000, 102)
        """
    elif dbvendor == "sqlserver":
        query = """
            INSERT INTO orders(order_date, purchaser, quantity, product_id) VALUES
            ('2025-12-12', 1002, 10000, 102)
        """
    elif dbvendor == "postgres":
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, '2025-12-12', 1002, 10000, 102);
        """
    else:
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, TO_DATE('2025-12-12', 'YYYY-MM-DD'),
            1002, 10000, 102);
        """

    run_remote_query(dbvendor, query)
    if dbvendor in ("oracle", "olr"):
        time.sleep(50)
    elif dbvendor == "oracle23ai":
        time.sleep(100)
    else:
        time.sleep(10)

    if dbvendor in ("oracle", "oracle23ai", "olr"):
        pgrow = run_pg_query_one(pg_cursor, f"SELECT \"ORDER_NUMBER\", \"ORDER_DATE\", \"PURCHASER\", \"QUANTITY\", \"PRODUCT_ID\" FROM \"{dbname}\".\"ORDERS\" WHERE \"ORDER_NUMBER\" >= 10005")
    else:
        pgrow = run_pg_query_one(pg_cursor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM \"{dbname}\".orders WHERE order_number >= 10005")
    assert pgrow != None
    assert int(pgrow[3]) == 10000

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, f"DELETE FROM orders WHERE order_number > 10004")
    update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", "'lowercase'", True)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    time.sleep(10)



def test_ConnectorStartSchemaSyncModeDBZ(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_dbz_schemasync"
    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)

    if dbvendor == "postgres":
        create_synchdb_connector(pg_cursor, dbvendor, name)
        with pytest.raises(FeatureNotSupported, match="automatic schema sync"):
            pg_cursor.execute("SELECT synchdb_start_engine_bgw(%s, 'schemasync')", (name,))
        stop_and_delete_synchdb_connector(pg_cursor, name)
        return

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "schemasync")
    assert result > 0

    if dbvendor in ("oracle", "oracle23ai", "olr"):
        time.sleep(30)
    else:
        time.sleep(10)

    # check table counts
    pgtblcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM information_schema.tables where table_schema='{dbname}' and table_type = 'BASE TABLE'")
    if dbvendor == "mysql":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()")
    elif dbvendor == "sqlserver":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_CATALOG=DB_NAME() AND TABLE_SCHEMA=schema_name() AND TABLE_NAME NOT LIKE 'systranschemas%'")
    elif dbvendor == "postgres":
        exttblcount = run_remote_query(dbvendor, f"SELECT count(*) FROM information_schema.tables where table_schema='{schema}' and table_type = 'BASE TABLE'")
    else:
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%' AND table_name <> 'SYNCHDB_OLR_FENCE'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    if dbvendor != "postgres":
        # check table name mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            id = row[0].split(".")
            if len(id) == 3:
                assert id[0].lower() + "." + id[2].lower() == row[1]
            else:
                assert row[0].lower() == row[1]

        # check attname mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:   
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert row[0].lower() == row[1]

        # check data type mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders;")
    assert int(pgrow[0]) == 0

    # check state = paused
    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    if dbvendor == "oracle23ai":
        assert row[1] == "oracle"
    else:
        assert row[1] == dbvendor
    assert int(row[2]) > 0
    assert row[3] == "schema sync" or row[3] == "change data capture"
    assert row[4] == "paused"
    assert row[5] == "no error"

    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_resume_engine('{name}')")
    wait_for_operation(pg_cursor, row[0])
    if dbvendor in ("oracle", "oracle23ai"):
        # RESUME launches a new producer. Wait for a committed batch from
        # that producer before writing the row intended for CDC-only capture.
        common.wait_for_native_snapshot(pg_cursor, name, mode="no_data")

    # test a bit of cdc
    if dbvendor == "mysql":
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, '2025-12-12',
            1002, 10000, 102);
        """
    elif dbvendor == "sqlserver":
        query = """
            INSERT INTO orders(order_date, purchaser, quantity,
            product_id) VALUES ('12-DEC-2025',
            1002, 10000, 102);
        """
    elif dbvendor == "postgres":
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, '2025-12-12', 1002, 10000, 102);
        """
    else:
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, TO_DATE('2025-12-12', 'YYYY-MM-DD'),
            1002, 10000, 102);
        """

    run_remote_query(dbvendor, query)
    if dbvendor in ("oracle", "oracle23ai"):
        assert wait_for_pg_row(pg_cursor,
            f"SELECT quantity FROM {dbname}.orders WHERE order_number = 10005",
            timeout=1200) == (10000,)
        assert run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders") == (1,)
    else:
        time.sleep(50 if dbvendor == "olr" else 10)
        pgrow = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders;")
        assert int(pgrow[0]) in (1, 4, 5)

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, f"DELETE FROM orders WHERE order_number > 10004")
    drop_repslot_and_pub(dbvendor, name, "postgres")
    time.sleep(10)



def test_ConnectorStartAlwaysModeDBZ(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_dbz_always"
    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)

    if dbvendor == "sqlserver":
        create_synchdb_connector(pg_cursor, dbvendor, name)
        with pytest.raises(FeatureNotSupported, match="capture-drain fence"):
            pg_cursor.execute("SELECT synchdb_start_engine_bgw(%s, 'always')", (name,))
        pg_cursor.execute("""
            SELECT count(*) FROM synchdb_artifacts a
            JOIN synchdb_conninfo c USING (connector_id) WHERE c.name = %s
        """, (name,))
        assert pg_cursor.fetchone()[0] == 0
        stop_and_delete_synchdb_connector(pg_cursor, name)
        return

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "always")
    assert result > 0

    if dbvendor in ("oracle", "oracle23ai", "olr"):
        time.sleep(30)
    else:
        time.sleep(10)

    # check table counts
    pgtblcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM information_schema.tables where table_schema='{dbname}' and table_type = 'BASE TABLE'")
    if dbvendor == "mysql":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()")
    elif dbvendor == "sqlserver":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_CATALOG=DB_NAME() AND TABLE_SCHEMA=schema_name() AND TABLE_NAME NOT LIKE 'systranschemas%'")
    elif dbvendor == "postgres":
        exttblcount = run_remote_query(dbvendor, f"SELECT count(*) FROM information_schema.tables where table_schema='{schema}' and table_type = 'BASE TABLE'")
    else:
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%' AND table_name <> 'SYNCHDB_OLR_FENCE'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    if dbvendor != "postgres":
        # check table name mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            id = row[0].split(".")
            if len(id) == 3:
                assert id[0].lower() + "." + id[2].lower() == row[1]
            else:
                assert row[0].lower() == row[1]

        # check attname mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert row[0].lower() == row[1]

        # check data type mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders;")
    assert int(pgrow[0]) == 4
    
    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    if dbvendor == "oracle23ai":
        assert row[1] == "oracle"
    else:
        assert row[1] == dbvendor
    assert int(row[2]) > 0
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
    assert row[4] == "polling"
    assert row[5] == "no error"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")



def test_ConnectorStartNodataModeDBZ(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_dbz_nodata"
    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result > 0

    if dbvendor in ("oracle", "oracle23ai", "olr"):
        time.sleep(30)
    else:
        time.sleep(10)

    # check table counts
    pgtblcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM information_schema.tables where table_schema='{dbname}' and table_type = 'BASE TABLE'")
    if dbvendor == "mysql":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()")
    elif dbvendor == "sqlserver":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_CATALOG=DB_NAME() AND TABLE_SCHEMA=schema_name() AND TABLE_NAME NOT LIKE 'systranschemas%'")
    elif dbvendor == "postgres":
        exttblcount = run_remote_query(dbvendor, f"SELECT count(*) FROM information_schema.tables where table_schema='{schema}' and table_type = 'BASE TABLE'")
    else:
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%' AND table_name <> 'SYNCHDB_OLR_FENCE'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    if dbvendor != "postgres":
        # check table name mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            id = row[0].split(".")
            if len(id) == 3:
                assert id[0].lower() + "." + id[2].lower() == row[1]
            else:
                assert row[0].lower() == row[1]

        # check attname mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert row[0].lower() == row[1]

        # check data type mappings
        if dbvendor == "oracle23ai":
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = 'oracle'")
        else:
            rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders;")
    assert int(pgrow[0]) == 0

    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    if dbvendor == "oracle23ai":
        assert row[1] == "oracle"
    else:
        assert row[1] == dbvendor
    assert int(row[2]) > 0
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
    assert row[4] == "polling"
    assert row[5] == "no error"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")



def test_ConnectorRestart(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result > 0

    if dbvendor in ("oracle", "oracle23ai", "olr"):
        time.sleep(10)
    else:
        time.sleep(5)

    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    if dbvendor == "oracle23ai":
        assert row[1] == "oracle"
    else:
        assert row[1] == dbvendor
    oldpid = row[2]
    assert row[2] > -1
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
    assert row[4] == "polling"
    assert row[5] == "no error"

    if dbvendor in ("postgres", "sqlserver"):
        # An idle no_data run has no acknowledged record to persist. Exercise
        # CDC before restarting so this generation owns a real durable offset.
        wait_for_connector_cdc(pg_cursor, name)
        if dbvendor == "sqlserver":
            inserted = run_remote_query(dbvendor, "INSERT INTO orders(order_date,purchaser,quantity,product_id) OUTPUT inserted.order_number VALUES ('2026-01-01',1003,2,107)")
            order_id = int(inserted[0][0])
        else:
            run_remote_query(dbvendor, "INSERT INTO orders VALUES (10005, '2026-01-01', 1003, 2, 107)")
            order_id = 10005
        assert wait_for_pg_row(pg_cursor, f"SELECT quantity FROM {dbname}.orders WHERE order_number = {order_id}") == (2,)

    restart_mode = "no_data" if dbvendor == "sqlserver" else "initial"
    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_restart_connector('{name}', '{restart_mode}')")
    wait_for_operation(pg_cursor, row[0])

    if dbvendor in ("oracle", "oracle23ai", "olr"):
        time.sleep(10)
    else:
        time.sleep(5)

    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    if dbvendor == "oracle23ai":
        assert row[1] == "oracle"
    else:
        assert row[1] == dbvendor
    assert row[2] > 0
    assert row[2] != oldpid
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
    if row[4] == "polling" or row[4] == "restarting":
        assert True
    else:
        assert False
    assert row[5] == "no error"

    if dbvendor in ("postgres", "sqlserver"):
        wait_for_connector_cdc(pg_cursor, name)
        if dbvendor == "sqlserver":
            inserted = run_remote_query(dbvendor, "INSERT INTO orders(order_date,purchaser,quantity,product_id) OUTPUT inserted.order_number VALUES ('2026-01-01',1003,3,107)")
            order_id = int(inserted[0][0])
        else:
            run_remote_query(dbvendor, "INSERT INTO orders VALUES (10006, '2026-01-01', 1003, 3, 107)")
            order_id = 10006
        assert wait_for_pg_row(pg_cursor, f"SELECT quantity FROM {dbname}.orders WHERE order_number = {order_id}") == (3,)

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    if dbvendor in ("postgres", "sqlserver"):
        run_remote_query(dbvendor, "DELETE FROM orders WHERE order_number > 10004")


def test_ConnectorStop(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()
    
    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result > 0

    if dbvendor == "oracle":
        time.sleep(10)
    else:
        time.sleep(5)

    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_stop_engine_bgw('{name}')")
    wait_for_operation(pg_cursor, row[0])

    time.sleep(5)
    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    if dbvendor == "oracle23ai":
        assert row[1] == "oracle"
    else:
        assert row[1] == dbvendor
    assert row[2] == -1
    assert row[4] == "stopped"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")


def test_ConnectorDelete(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)

    result = create_synchdb_connector(pg_cursor, dbvendor, name)
    assert result[0] == 0

    result = run_pg_query_one(pg_cursor, f"SELECT synchdb_del_conninfo('{name}')")
    assert result[0] == 0

    result = run_pg_query_one(pg_cursor, f"SELECT name FROM synchdb_conninfo WHERE name = '{name}'")
    assert result == None

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
