import common
import time
from common import run_pg_query, run_pg_query_one, run_remote_query, create_synchdb_connector, getConnectorName, getDbname, create_and_start_synchdb_connector, stop_and_delete_synchdb_connector, drop_default_pg_schema

def test_Insert(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_insert"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result == 0

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
    else:
        query = """
        CREATE TABLE inserttable(
            a NUMBER PRIMARY KEY,
            b VARCHAR(255));
        """

    run_remote_query(dbvendor, query)
    if dbvendor == "oracle":
        time.sleep(30)
    else:
        time.sleep(10)
   
    out=run_remote_query(dbvendor, "INSERT INTO inserttable (a, b) VALUES (1, 'Hello')")
    out=run_remote_query(dbvendor, "COMMIT")
    if dbvendor == "oracle":
        time.sleep(75)
    else:
        time.sleep(15)
    
    extrows = run_remote_query(dbvendor, f"SELECT a, b FROM inserttable")
    rows = run_pg_query(pg_cursor, f"SELECT a, b FROM {dbname}.inserttable")
    assert len(extrows) > 0
    assert len(rows) > 0
    assert len(extrows) == len(rows)

    for row, extrow in zip(rows, extrows):
        assert int(row[0]) == int(extrow[0])
        assert str(row[1]) == str(extrow[1])

    extrows = run_remote_query(dbvendor, f"DROP TABLE inserttable")
    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)

def test_InsertWithError(pg_cursor, dbvendor):
    assert True

def test_Update(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_update"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result == 0

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
    else:
        query = """
        CREATE TABLE updatetable(
            a NUMBER PRIMARY KEY,
            b VARCHAR(255));
        """

    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        run_remote_query(dbvendor, "ALTER TABLE updatetable ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        time.sleep(30)
    else:
        time.sleep(10)
    
    run_remote_query(dbvendor, "INSERT INTO updatetable (a, b) VALUES (1, 'Hello')")
    run_remote_query(dbvendor, "UPDATE updatetable SET a = 2")
    run_remote_query(dbvendor, "UPDATE updatetable SET b = 'olleH'")
    run_remote_query(dbvendor, "COMMIT")

    if dbvendor == "oracle":
        time.sleep(75)
    else:
        time.sleep(10)

    extrows = run_remote_query(dbvendor, f"SELECT a, b FROM updatetable")
    rows = run_pg_query(pg_cursor, f"SELECT a, b FROM {dbname}.updatetable")
    assert len(extrows) > 0
    assert len(rows) > 0
    assert len(extrows) == len(rows)

    for row, extrow in zip(rows, extrows):
        assert int(row[0]) == int(extrow[0])
        assert str(row[1]) == str(extrow[1])

    extrows = run_remote_query(dbvendor, f"DROP TABLE updatetable")
    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)

def test_UpdateWithError(pg_cursor, dbvendor):
    assert True

def test_Delete(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_delete"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result == 0

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
    else:
        query = """
        CREATE TABLE deletetable(
            a NUMBER PRIMARY KEY,
            b VARCHAR(255));
        """

    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        run_remote_query(dbvendor, "ALTER TABLE deletetable ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        time.sleep(30)
    else:
        time.sleep(10)

    run_remote_query(dbvendor, "INSERT INTO deletetable (a, b) VALUES (1, 'Hello')")
    run_remote_query(dbvendor, "INSERT INTO deletetable (a, b) VALUES (2, 'SynchDB')")
    run_remote_query(dbvendor, "INSERT INTO deletetable (a, b) VALUES (3, 'Pytest')")
    run_remote_query(dbvendor, "COMMIT")

    if dbvendor == "oracle":
        time.sleep(75)
    else:
        time.sleep(15)

    extrows = run_remote_query(dbvendor, f"SELECT a, b FROM deletetable")
    rows = run_pg_query(pg_cursor, f"SELECT a, b FROM {dbname}.deletetable")
    assert len(extrows) > 0 and len(extrows) == 3
    assert len(rows) > 0 and len(rows) == 3
    assert len(extrows) == len(rows)

    for row, extrow in zip(rows, extrows):
        assert int(row[0]) == int(extrow[0])
        assert str(row[1]) == str(extrow[1])

    run_remote_query(dbvendor, "DELETE FROM deletetable WHERE a = 2")
    if dbvendor == "oracle":
        time.sleep(75)
    else:
        time.sleep(15)

    extrows = run_remote_query(dbvendor, f"SELECT a, b FROM deletetable")
    rows = run_pg_query(pg_cursor, f"SELECT a, b FROM {dbname}.deletetable")
    assert len(rows) > 0 and len(rows) == 2
    assert len(extrows) > 0 and len(extrows) == 2
    assert len(extrows) == len(rows)

    for row, extrow in zip(rows, extrows):
        assert int(row[0]) == int(extrow[0])
        assert str(row[1]) == str(extrow[1])

    extrows = run_remote_query(dbvendor, f"DROP TABLE deletetable")
    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)

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

