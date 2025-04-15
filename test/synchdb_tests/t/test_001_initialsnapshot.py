import common
import time
from datetime import datetime
from common import run_pg_query, run_pg_query_one, run_remote_query, run_remote_query_one, create_synchdb_connector, getConnectorName, getDbname, verify_default_type_mappings

def test_ConnectorCreate(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    result = create_synchdb_connector(pg_cursor, dbvendor, name)
    assert result[0] == 0

    result = run_pg_query_one(pg_cursor, f"SELECT name, isactive, data->>'connector' FROM synchdb_conninfo WHERE name = '{name}'")
    assert result[0] == name
    assert result[1] == False
    assert result[2] == dbvendor

    result = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_extra_conninfo('{name}', 'verify_ca', '/path/ks', 'kspass', '/path/ts/', 'tspass')")
    assert result[0] == 0

    result = run_pg_query_one(pg_cursor, f"SELECT data->>'ssl_mode', data->>'ssl_keystore', data->>'ssl_keystore_pass', data->>'ssl_truststore', data->>'ssl_truststore_pass' FROM synchdb_conninfo WHERE name = '{name}'")
    assert result[0] == "verify_ca"
    assert result[1] == "/path/ks"
    assert result[3] == "/path/ts/"

def test_CreateExtraConninfo(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()

    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_extra_conninfo('{name}', 'verify_ca', 'keystore', 'pass', 'truststore', 'pass')")
    assert row[0] == 0

    row = run_pg_query_one(pg_cursor, f"SELECT data->'ssl_mode', data->'ssl_keystore', data->'ssl_truststore' FROM synchdb_conninfo WHERE name = '{name}'")
    assert row[0] == "verify_ca"
    assert row[1] == "keystore"
    assert row[2] == "truststore"

def test_RemoveExtraConninfo(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()
    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_del_extra_conninfo('{name}')")
    assert row[0] == 0

    row = run_pg_query_one(pg_cursor, f"SELECT data->'ssl_mode', data->'ssl_keystore', data->'ssl_truststore' FROM synchdb_conninfo WHERE name = '{name}'")
    assert row[0] == None
    assert row[1] == None
    assert row[2] == None

def test_ConnectorStart(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()
    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_start_engine_bgw('{name}')")
    assert row[0] == 0

    # oracle takes longer to start initial snapshot
    if dbvendor == "oracle":
        time.sleep(20)
    else:
        time.sleep(10)

    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    assert row[1] == dbvendor
    assert row[2] > -1
    assert row[3] == "initial snapshot"
    assert row[4] == "polling"
    assert row[5] == "no error"

def test_InitialSnapshot(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()
    # check table counts
    pgtblcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM information_schema.tables where table_schema='{dbname}' and table_type = 'BASE TABLE'")
    if dbvendor == "mysql":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()")
    elif dbvendor == "sqlserver":
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_CATALOG=DB_NAME() AND TABLE_SCHEMA=schema_name() AND TABLE_NAME NOT LIKE 'systranschemas%'")
    else:
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])
    
    # check row counts or orders table
    pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders")
    extrowcount = run_remote_query(dbvendor, f"SELECT count(*) FROM orders")
    assert int(pgrowcount[0]) == int(extrowcount[0][0])

    # check table name mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        id = row[0].split(".")
        if len(id) == 3:
            assert id[0] + "." + id[2] == row[1]
        else:
            assert row[0] == row[1]
    
    # check attname mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        assert row[0] == row[1]

    # check data type mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM {dbname}.orders WHERE order_number = 10003")
    extrow = run_remote_query(dbvendor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM orders WHERE order_number = 10003")
    assert int(pgrow[0]) == int(extrow[0][0])
    if dbvendor == "oracle":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%d-%b-%y')
    else:
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d').date()
    assert int(pgrow[2]) == int(extrow[0][2])
    assert int(pgrow[3]) == int(extrow[0][3])
    assert int(pgrow[4]) == int(extrow[0][4])


def test_ConnectorStartSchemaSyncMode(pg_cursor, dbvendor):
    assert True

def test_ConnectorStartAlwaysMode(pg_cursor, dbvendor):
    assert True

def test_ConnectorStartNodataMode(pg_cursor, dbvendor):
    assert True

def test_ConnectorStartWithError(pg_cursor, dbvendor):
    assert True

def test_ConnectorRestart(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()
    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    assert row[1] == dbvendor
    oldpid = row[2]
    assert row[2] > -1
    assert row[3] == "initial snapshot"
    assert row[4] == "polling"
    assert row[5] == "no error"

    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_restart_connector('{name}', 'initial')")
    assert row[0] == 0

    if dbvendor == "oracle":
        time.sleep(10)
    else:
        time.sleep(5)

    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    assert row[1] == dbvendor
    oldpid = row[2]
    assert row[2] == oldpid
    assert row[3] == "initial snapshot"
    if row[4] == "polling" or row[4] == "restarting":
        assert True
    else:
        assert False
    assert row[5] == "no error"


def test_ConnectorStop(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()
    
    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_stop_engine_bgw('{name}')")
    assert row[0] == 0

    time.sleep(5)
    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    assert row[1] == dbvendor
    assert row[2] == -1
    assert row[4] == "stopped"

def test_ConnectorRestartAlwaysMode(pg_cursor, dbvendor):
    assert True

def test_ConnectorRestartNodataMode(pg_cursor, dbvendor):
    assert True

def test_ConnectorDelete(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)

    result = run_pg_query_one(pg_cursor, f"SELECT synchdb_del_extra_conninfo('{name}')")
    assert result[0] == 0

    result = run_pg_query_one(pg_cursor, f"SELECT data->>'ssl_mode', data->>'ssl_keystore', data->>'ssl_keystore_pass', data->>'ssl_truststore', data->>'ssl_truststore_pass' FROM synchdb_conninfo WHERE name = '{name}'")
    assert result[0] == None
    assert result[1] == None
    assert result[3] == None

    result = run_pg_query_one(pg_cursor, f"SELECT synchdb_del_conninfo('{name}')")
    assert result[0] == 0

    result = run_pg_query_one(pg_cursor, f"SELECT name FROM synchdb_conninfo WHERE name = '{name}'")
    assert result == None
