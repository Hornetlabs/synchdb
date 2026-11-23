import common
import time
from psycopg2.errors import UndefinedTable
from common import restart_remote_db, run_pg_query, run_pg_query_one, run_remote_query, create_synchdb_connector, getConnectorName, getDbname, create_and_start_synchdb_connector, stop_and_delete_synchdb_connector, drop_default_pg_schema, drop_repslot_and_pub

# import pytest
# pytestmark = pytest.mark.skip(reason="跳过此文件")

def _dml_delay(vendor, seconds):
    if vendor != "olr":
        time.sleep(seconds)


def _dml_query(cursor, vendor, name, query, expected, timeout=300):
    if vendor != "olr":
        return run_pg_query(cursor, query)
    expected_values = [(int(row[0]), str(row[1])) for row in expected]
    deadline = time.monotonic() + timeout
    while True:
        try:
            rows = run_pg_query(cursor, query)
        except UndefinedTable:
            rows = []  # The preceding source CREATE may still be in flight.
        if [(int(row[0]), str(row[1])) for row in rows] == expected_values:
            return rows
        cursor.execute("SELECT actual_state,last_error FROM synchdb_control_view WHERE name=%s", (name,))
        state = cursor.fetchone()
        assert state and state[0] not in ("FAILED", "RETRY_WAIT"), state
        assert time.monotonic() < deadline, f"OLR DML did not reach its source values: {rows}"
        time.sleep(0.2)


def test_Insert(pg_cursor, dbvendor):

    if dbvendor == "oracle23ai":
        restart_remote_db(dbvendor)

    name = getConnectorName(dbvendor) + "_insert"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result > 0

    if dbvendor == "mysql":
        query = """
        CREATE TABLE inserttable(
            a INT PRIMARY KEY,
            b VARCHAR(255));
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE inserttable(
            a INT NOT NULL PRIMARY KEY,
            b VARCHAR(255));
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'inserttable', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        _dml_delay(dbvendor, 10)
        query = """
        CREATE TABLE inserttable(
            a INT PRIMARY KEY,
            b VARCHAR(255));
        """
    else:
        query = """
        CREATE TABLE inserttable(
            a NUMBER PRIMARY KEY,
            b VARCHAR(255));
        """

    run_remote_query(dbvendor, query)
    if dbvendor in ("oracle", "oracle23ai", "olr"):
        _dml_delay(dbvendor, 30)
    else:
        _dml_delay(dbvendor, 10)
   
    out=run_remote_query(dbvendor, "INSERT INTO inserttable (a, b) VALUES (1, 'Hello')")
    out=run_remote_query(dbvendor, "COMMIT")
    if dbvendor in ("oracle", "oracle23ai", "olr"):
        _dml_delay(dbvendor, 75)
    else:
        _dml_delay(dbvendor, 15)
    
    extrows = run_remote_query(dbvendor, f"SELECT a, b FROM inserttable")
    rows = _dml_query(pg_cursor, dbvendor, name, f"SELECT a, b FROM {dbname}.inserttable", extrows)
    assert len(extrows) > 0
    assert len(rows) > 0
    assert len(extrows) == len(rows)

    for row, extrow in zip(rows, extrows):
        assert int(row[0]) == int(extrow[0])
        assert str(row[1]) == str(extrow[1])

    extrows = run_remote_query(dbvendor, f"DROP TABLE inserttable")
    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")

def test_InsertWithError(pg_cursor, dbvendor):
    assert True

def test_Update(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_update"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result > 0

    if dbvendor == "mysql":
        query = """
        CREATE TABLE updatetable(
            a INT PRIMARY KEY,
            b VARCHAR(255));
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE updatetable(
            a INT NOT NULL PRIMARY KEY,
            b VARCHAR(255));
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'updatetable', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        _dml_delay(dbvendor, 10)
        query = """
        CREATE TABLE updatetable(
            a INT PRIMARY KEY,
            b VARCHAR(255));
        """
    else:
        query = """
        CREATE TABLE updatetable(
            a NUMBER PRIMARY KEY,
            b VARCHAR(255));
        """

    run_remote_query(dbvendor, query)
    if dbvendor in ("oracle", "oracle23ai", "olr"):
        run_remote_query(dbvendor, "ALTER TABLE updatetable ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        _dml_delay(dbvendor, 30)
    else:
        _dml_delay(dbvendor, 10)
    
    run_remote_query(dbvendor, "INSERT INTO updatetable (a, b) VALUES (1, 'Hello')")
    run_remote_query(dbvendor, "UPDATE updatetable SET a = 2")
    run_remote_query(dbvendor, "UPDATE updatetable SET b = 'olleH'")
    run_remote_query(dbvendor, "COMMIT")

    if dbvendor in ("oracle", "oracle23ai", "olr"):
        _dml_delay(dbvendor, 90)
    else:
        _dml_delay(dbvendor, 10)

    extrows = run_remote_query(dbvendor, f"SELECT a, b FROM updatetable")
    rows = _dml_query(pg_cursor, dbvendor, name, f"SELECT a, b FROM {dbname}.updatetable", extrows)
    assert len(extrows) > 0
    assert len(rows) > 0
    assert len(extrows) == len(rows)

    for row, extrow in zip(rows, extrows):
        assert int(row[0]) == int(extrow[0])
        assert str(row[1]) == str(extrow[1])

    extrows = run_remote_query(dbvendor, f"DROP TABLE updatetable")
    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")

def test_UpdateWithError(pg_cursor, dbvendor):
    assert True


def test_Delete(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_delete"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result > 0

    if dbvendor == "mysql":
        query = """
        CREATE TABLE deletetable(
            a INT PRIMARY KEY,
            b VARCHAR(255));
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE deletetable(
            a INT NOT NULL PRIMARY KEY,
            b VARCHAR(255));
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'deletetable', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        _dml_delay(dbvendor, 10)
        query = """
        CREATE TABLE deletetable(
            a INT PRIMARY KEY,
            b VARCHAR(255));
        """
    else:
        query = """
        CREATE TABLE deletetable(
            a NUMBER PRIMARY KEY,
            b VARCHAR(255));
        """

    run_remote_query(dbvendor, query)
    if dbvendor in ("oracle", "oracle23ai", "olr"):
        run_remote_query(dbvendor, "ALTER TABLE deletetable ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        _dml_delay(dbvendor, 30)
    else:
        _dml_delay(dbvendor, 10)

    run_remote_query(dbvendor, "INSERT INTO deletetable (a, b) VALUES (1, 'Hello')")
    run_remote_query(dbvendor, "INSERT INTO deletetable (a, b) VALUES (2, 'SynchDB')")
    run_remote_query(dbvendor, "INSERT INTO deletetable (a, b) VALUES (3, 'Pytest')")
    run_remote_query(dbvendor, "COMMIT")

    if dbvendor in ("oracle", "oracle23ai", "olr"):
        _dml_delay(dbvendor, 90)
    else:
        _dml_delay(dbvendor, 15)

    extrows = run_remote_query(dbvendor, f"SELECT a, b FROM deletetable")
    rows = _dml_query(pg_cursor, dbvendor, name, f"SELECT a, b FROM {dbname}.deletetable", extrows)
    assert len(extrows) > 0 and len(extrows) == 3
    assert len(rows) > 0 and len(rows) == 3
    assert len(extrows) == len(rows)

    for row, extrow in zip(rows, extrows):
        assert int(row[0]) == int(extrow[0])
        assert str(row[1]) == str(extrow[1])

    run_remote_query(dbvendor, "DELETE FROM deletetable WHERE a = 2")
    if dbvendor in ("oracle", "oracle23ai", "olr"):
        _dml_delay(dbvendor, 80)
    else:
        _dml_delay(dbvendor, 15)

    extrows = run_remote_query(dbvendor, f"SELECT a, b FROM deletetable")
    rows = _dml_query(pg_cursor, dbvendor, name, f"SELECT a, b FROM {dbname}.deletetable", extrows)
    assert len(rows) > 0 and len(rows) == 2
    assert len(extrows) > 0 and len(extrows) == 2
    assert len(extrows) == len(rows)

    for row, extrow in zip(rows, extrows):
        assert int(row[0]) == int(extrow[0])
        assert str(row[1]) == str(extrow[1])

    extrows = run_remote_query(dbvendor, f"DROP TABLE deletetable")
    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")


def test_DeleteWithError(pg_cursor, dbvendor):
    assert True

def test_SPIInsert(pg_cursor, dbvendor):
    assert True

def test_SPIInsertWithError(pg_cursor, dbvendor):
    assert True

def test_SPIUpdate(pg_cursor, dbvendor):
    assert True

def test_SPIUpdateWithError(pg_cursor, dbvendor):
    assert True

def test_SPIDelete(pg_cursor, dbvendor):
    assert True

def test_SPIDeleteWithError(pg_cursor, dbvendor):
    assert True

