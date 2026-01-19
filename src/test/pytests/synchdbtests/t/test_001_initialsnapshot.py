import common
import time
from datetime import datetime
from common import run_pg_query, run_pg_query_one, run_remote_query, create_synchdb_connector, getConnectorName, getDbname, verify_default_type_mappings, stop_and_delete_synchdb_connector, drop_default_pg_schema, create_and_start_synchdb_connector, update_guc_conf, getSchema, drop_repslot_and_pub

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
        update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'fdw'", True)

    result = create_synchdb_connector(pg_cursor, dbvendor, name)
    assert result[0] == 0

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
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
    assert row[4] == "polling"
    assert row[5] == "no error"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)

    if dbvendor == "postgres":
        update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'debezium'", True)

def test_InitialSnapshotDBZ(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_dbzsnap"
    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)
    
    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])
    
    # check row counts or orders table
    pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders")
    extrowcount = run_remote_query(dbvendor, f"SELECT count(*) FROM orders")
    assert int(pgrowcount[0]) == int(extrowcount[0][0])

    if dbvendor != "postgres":
        # check table name mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            id = row[0].split(".")
            if len(id) == 3:
                assert id[0].lower() + "." + id[2].lower() == row[1]
            else:
                assert row[0].lower() == row[1]
        # check attname mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert row[0].lower() == row[1]

        # check data type mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM {dbname}.orders WHERE order_number = 10003")
    extrow = run_remote_query(dbvendor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM orders WHERE order_number = 10003")
    assert int(pgrow[0]) == int(extrow[0][0])
    if dbvendor == "oracle" or dbvendor == "olr":
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

def test_InitialSnapshotFDW(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_fdwsnap"
    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)

    if dbvendor == "mysql":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'mysql_fdw' ) AS mysql_fdw_available")
        if isfdw[0] == False:
            print ("test_InitialSnapshotFDW skipped - mysql_fdw not available for install")
            assert True
            return
    elif dbvendor == "sqlserver":
        assert True
        return
    elif dbvendor == "postgres":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'postgres_fdw' ) AS postgres_fdw_available")
        if isfdw[0] == False:
            print ("test_InitialSnapshotFDW skipped - postgres_fdw not available for install")
            assert True
            return
    else:
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'oracle_fdw' ) AS oracle_fdw_available")
        if isfdw[0] == False:
            print ("test_InitialSnapshotFDW skipped - oracle_fdw not available for install")
            assert True
            return

    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'fdw'", True)

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
            assert id[0].lower() + "." + id[2].lower() == row[1]
        else:
            assert row[0].lower() == row[1]

    # check attname mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        assert row[0].lower() == row[1]

    # check data type mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM {dbname}.orders WHERE order_number = 10003")
    extrow = run_remote_query(dbvendor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM orders WHERE order_number = 10003")
    assert int(pgrow[0]) == int(extrow[0][0])
    if dbvendor == "oracle" or dbvendor == "olr":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%d-%b-%y')
    elif dbvendor == "postgres":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d %H:%M:%S')
    else:
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d').date()
    assert int(pgrow[2]) == int(extrow[0][2])
    assert int(pgrow[3]) == int(extrow[0][3])
    assert int(pgrow[4]) == int(extrow[0][4])

    # test cdc now
    if dbvendor == "postgres" or dbvendor == "mysql":
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
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

    pgrow = run_pg_query_one(pg_cursor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM {dbname}.orders WHERE order_number >= 10005")
    assert pgrow != None
    assert int(pgrow[3]) == 10000

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, f"DELETE FROM orders WHERE order_number > 10004")
    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'debezium'", True)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    time.sleep(10)

def test_InitialSnapshotDBZ_uppercase(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_dbzsnap_upper"
    dbname = getDbname(dbvendor).upper()
    schema = getSchema(dbvendor)

    update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", "'uppercase'", True)

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS \"{dbname}\"")
        run_pg_query_one(pg_cursor, f"CREATE TABLE \"{dbname}\".\"ORDERS\" (\"ORDER_NUMBER\" int primary key, \"ORDER_DATE\" timestamp without time zone, \"PURCHASER\" int, \"QUANTITY\" int , \"PRODUCT_ID\" int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    # check row counts or orders table
    pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM \"{dbname}\".\"ORDERS\"")
    extrowcount = run_remote_query(dbvendor, f"SELECT count(*) FROM orders")
    assert int(pgrowcount[0]) == int(extrowcount[0][0])

    if dbvendor != "postgres":
        # check table name mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            id = row[0].split(".")
            if len(id) == 3:
                assert id[0].upper() + "." + id[2].upper() == row[1]
            else:
                assert row[0].upper() == row[1]

        # check attname mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert row[0].upper() == row[1]

        # check data type mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT \"ORDER_NUMBER\", \"ORDER_DATE\", \"PURCHASER\", \"QUANTITY\", \"PRODUCT_ID\" FROM \"{dbname}\".\"ORDERS\" WHERE \"ORDER_NUMBER\" = 10003")
    extrow = run_remote_query(dbvendor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM orders WHERE order_number = 10003")
    assert int(pgrow[0]) == int(extrow[0][0])
    if dbvendor == "oracle" or dbvendor == "olr":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%d-%b-%y')
    elif dbvendor == "postgres":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d %H:%M:%S')
    else:
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d').date()
    assert int(pgrow[2]) == int(extrow[0][2])
    assert int(pgrow[3]) == int(extrow[0][3])
    assert int(pgrow[4]) == int(extrow[0][4])

    # test cdc now
    if dbvendor == "mysql":
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, '2025-12-12', 1002, 10000, 102)
        """
    elif dbvendor == "sqlserver":
        query = """
            INSERT INTO orders(order_date, purchaser, quantity, product_id) VALUES
            ("2025-12-12", 1002, 10000, 102)
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
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
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

def test_InitialSnapshotFDW_uppercase(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_fdwsnap_upper"
    dbname = getDbname(dbvendor).upper()
    schema = getSchema(dbvendor)

    if dbvendor == "mysql":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'mysql_fdw' ) AS mysql_fdw_available")
        if isfdw[0] == False:
            print ("test_InitialSnapshotFDW_uppercase skipped - mysql_fdw not available for install")
            assert True
            return
    elif dbvendor == "sqlserver":
        assert True
        return
    elif dbvendor == "postgres":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'postgres_fdw' ) AS postgres_fdw_available")
        if isfdw[0] == False:
            print ("test_InitialSnapshotFDW_uppercase skipped - postgres_fdw not available for install")
            assert True
            return
    else:
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'oracle_fdw' ) AS oracle_fdw_available")
        if isfdw[0] == False:
            print ("test_InitialSnapshotFDW_uppercase skipped - oracle_fdw not available for install")
            assert True
            return

    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'fdw'", True)
    update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", "'uppercase'", True)

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    # check row counts or orders table
    pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM \"{dbname}\".\"ORDERS\"")
    extrowcount = run_remote_query(dbvendor, f"SELECT count(*) FROM orders")
    assert int(pgrowcount[0]) == int(extrowcount[0][0])

    # check table name mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        id = row[0].split(".")
        if len(id) == 3:
            assert id[0].upper() + "." + id[2].upper() == row[1]
        else:
            assert row[0].upper() == row[1]

    # check attname mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        assert row[0].upper() == row[1]

    # check data type mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT \"ORDER_NUMBER\", \"ORDER_DATE\", \"PURCHASER\", \"QUANTITY\", \"PRODUCT_ID\" FROM \"{dbname}\".\"ORDERS\" WHERE \"ORDER_NUMBER\" = 10003")
    extrow = run_remote_query(dbvendor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM orders WHERE order_number = 10003")
    assert int(pgrow[0]) == int(extrow[0][0])
    if dbvendor == "oracle" or dbvendor == "olr":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%d-%b-%y')
    elif dbvendor == "postgres":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d %H:%M:%S')
    else:
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d').date()
    assert int(pgrow[2]) == int(extrow[0][2])
    assert int(pgrow[3]) == int(extrow[0][3])
    assert int(pgrow[4]) == int(extrow[0][4])

    # test cdc now
    if dbvendor == "mysql":
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, "2025-12-12", 1002, 10000, 102)
        """
    elif dbvendor == "sqlserver":
        query = """
            INSERT INTO orders(order_date, purchaser, quantity, product_id) VALUES
            ("2025-12-12", 1002, 10000, 102)
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
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

    pgrow = run_pg_query_one(pg_cursor, f"SELECT \"ORDER_NUMBER\", \"ORDER_DATE\", \"PURCHASER\", \"QUANTITY\", \"PRODUCT_ID\" FROM \"{dbname}\".\"ORDERS\" WHERE \"ORDER_NUMBER\" >= 10005")
    assert pgrow != None
    assert int(pgrow[3]) == 10000

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, f"DELETE FROM orders WHERE order_number > 10004")
    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'debezium'", True)
    update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", "'lowercase'", True)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    time.sleep(10)

def test_InitialSnapshotDBZ_asis(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_dbzsnap_asis"
    dbname = getDbname(dbvendor)
    schema = getSchema(dbvendor)

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", "'asis'", True)

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    # check row counts or orders table
    if dbvendor == "oracle" or dbvendor == "olr":
        pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM \"{dbname}\".\"ORDERS\"")
    else:
        pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM \"{dbname}\".\"orders\"")
    extrowcount = run_remote_query(dbvendor, f"SELECT count(*) FROM orders")
    assert int(pgrowcount[0]) == int(extrowcount[0][0])

    if dbvendor != "postgres":
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
    if dbvendor == "oracle" or dbvendor == "olr":
        pgrow = run_pg_query_one(pg_cursor, f"SELECT \"ORDER_NUMBER\", \"ORDER_DATE\", \"PURCHASER\", \"QUANTITY\", \"PRODUCT_ID\" FROM \"{dbname}\".\"ORDERS\" WHERE \"ORDER_NUMBER\" = 10003")
    else:
        pgrow = run_pg_query_one(pg_cursor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM \"{dbname}\".orders WHERE order_number = 10003")
    extrow = run_remote_query(dbvendor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM orders WHERE order_number = 10003")
    assert int(pgrow[0]) == int(extrow[0][0])
    if dbvendor == "oracle" or dbvendor == "olr":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%d-%b-%y')
    elif dbvendor == "postgres":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d %H:%M:%S')
    else:
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d').date()
    assert int(pgrow[2]) == int(extrow[0][2])
    assert int(pgrow[3]) == int(extrow[0][3])
    assert int(pgrow[4]) == int(extrow[0][4])

    # test cdc now
    if dbvendor == "mysql":
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, '2025-12-12', 1002, 10000, 102)
        """
    elif dbvendor == "sqlserver":
        query = """
            INSERT INTO orders(order_date, purchaser, quantity, product_id) VALUES
            ("2025-12-12", 1002, 10000, 102)
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
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

    if dbvendor == "oracle" or dbvendor == "olr":
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

def test_InitialSnapshotFDW_asis(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_fdwsnap_asis"
    dbname = getDbname(dbvendor)
    schema = getSchema(dbvendor)

    if dbvendor == "mysql":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'mysql_fdw' ) AS mysql_fdw_available")
        if isfdw[0] == False:
            print ("test_InitialSnapshotFDW_asis skipped - mysql_fdw not available for install")
            assert True
            return
    elif dbvendor == "sqlserver":
        assert True
        return
    elif dbvendor == "postgres":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'postgres_fdw' ) AS postgres_fdw_available")
        if isfdw[0] == False:
            print ("test_InitialSnapshotFDW_asis skipped - postgres_fdw not available for install")
            assert True
            return
    else:
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'oracle_fdw' ) AS oracle_fdw_available")
        if isfdw[0] == False:
            print ("test_InitialSnapshotFDW_asis skipped - oracle_fdw not available for install")
            assert True
            return

    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'fdw'", True)
    update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", "'asis'", True)

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    # check row counts or orders table
    if dbvendor == "oracle" or dbvendor == "olr":
        pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM \"{dbname}\".\"ORDERS\"")
    else:
        pgrowcount = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM \"{dbname}\".\"orders\"")
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
    if dbvendor == "oracle" or dbvendor == "olr":
        pgrow = run_pg_query_one(pg_cursor, f"SELECT \"ORDER_NUMBER\", \"ORDER_DATE\", \"PURCHASER\", \"QUANTITY\", \"PRODUCT_ID\" FROM \"{dbname}\".\"ORDERS\" WHERE \"ORDER_NUMBER\" = 10003")
    else:
        pgrow = run_pg_query_one(pg_cursor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM \"{dbname}\".orders WHERE order_number = 10003")
    extrow = run_remote_query(dbvendor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM orders WHERE order_number = 10003")
    assert int(pgrow[0]) == int(extrow[0][0])
    if dbvendor == "oracle" or dbvendor == "olr":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%d-%b-%y')
    elif dbvendor == "postgres":
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d %H:%M:%S')
    else:
        assert pgrow[1] == datetime.strptime(extrow[0][1], '%Y-%m-%d').date()
    assert int(pgrow[2]) == int(extrow[0][2])
    assert int(pgrow[3]) == int(extrow[0][3])
    assert int(pgrow[4]) == int(extrow[0][4])


    # test cdc now
    if dbvendor == "mysql":
        query = """
            INSERT INTO orders(order_number, order_date, purchaser, quantity,
            product_id) VALUES (10005, '2025-12-12', 1002, 10000, 102)
        """
    elif dbvendor == "sqlserver":
        query = """
            INSERT INTO orders(order_date, purchaser, quantity, product_id) VALUES
            ("2025-12-12", 1002, 10000, 102)
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
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

    if dbvendor == "oracle" or dbvendor == "olr":
        pgrow = run_pg_query_one(pg_cursor, f"SELECT \"ORDER_NUMBER\", \"ORDER_DATE\", \"PURCHASER\", \"QUANTITY\", \"PRODUCT_ID\" FROM \"{dbname}\".\"ORDERS\" WHERE \"ORDER_NUMBER\" >= 10005")
    else:
        pgrow = run_pg_query_one(pg_cursor, f"SELECT order_number, order_date, purchaser, quantity, product_id FROM \"{dbname}\".orders WHERE order_number >= 10005")
    assert pgrow != None
    assert int(pgrow[3]) == 10000

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, f"DELETE FROM orders WHERE order_number > 10004")
    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'debezium'", True)
    update_guc_conf(pg_cursor, "synchdb.letter_casing_strategy", "'lowercase'", True)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    time.sleep(10)

def test_ConnectorStartSchemaSyncModeDBZ(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_dbz_schemasync"
    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "schemasync")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    if dbvendor != "postgres":
        # check table name mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            id = row[0].split(".")
            if len(id) == 3:
                assert id[0].lower() + "." + id[2].lower() == row[1]
            else:
                assert row[0].lower() == row[1]

        # check attname mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert row[0].lower() == row[1]

        # check data type mappings
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
    assert row[1] == dbvendor
    assert int(row[2]) > 0
    assert row[3] == "schema sync" or row[3] == "change data capture"
    assert row[4] == "paused"
    assert row[5] == "no error"

    run_pg_query_one(pg_cursor, f"SELECT synchdb_resume_engine('{name}')")

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
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

    pgrow = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders;")
    assert int(pgrow[0]) == 1 or int(pgrow[0]) == 5 or int(pgrow[0]) == 4 # sqlserver would have 5 - fixme

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, f"DELETE FROM orders WHERE order_number > 10004")
    drop_repslot_and_pub(dbvendor, name, "postgres")
    time.sleep(10)

def test_ConnectorStartSchemaSyncModeFDW(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_fdw_schemasync"
    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)

    if dbvendor == "mysql":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'mysql_fdw' ) AS mysql_fdw_available")
        if isfdw[0] == False:
            print ("test_ConnectorStartSchemaSyncModeFDW skipped - mysql_fdw not available for install")
            assert True
            return
    elif dbvendor == "sqlserver":
        assert True
        return
    elif dbvendor == "postgres":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'postgres_fdw' ) AS postgres_fdw_available")
        if isfdw[0] == False:
            print ("test_ConnectorStartSchemaSyncModeFDW skipped - postgres_fdw not available for install")
            assert True
            return
    else:
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'oracle_fdw' ) AS oracle_fdw_available")
        if isfdw[0] == False:
            print ("test_ConnectorStartSchemaSyncModeFDW skipped - oracle_fdw not available for install")
            assert True
            return

    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'fdw'", True)

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "schemasync")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    # check table name mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        id = row[0].split(".")
        if len(id) == 3:
            assert id[0].lower() + "." + id[2].lower() == row[1]
        else:
            assert row[0].lower() == row[1]

    # check attname mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        assert row[0].lower() == row[1]

    # check data type mappings
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
    assert row[1] == dbvendor
    assert int(row[2]) > 0
    assert row[3] == "schema sync" or row[3] == "change data capture"
    assert row[4] == "paused"
    assert row[5] == "no error"

    run_pg_query_one(pg_cursor, f"SELECT synchdb_resume_engine('{name}')")

    time.sleep(10)
    # test a bit of cdc
    if dbvendor == "postgres" or dbvendor == "mysql":
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
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

    pgrow = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders;")
    assert int(pgrow[0]) == 1

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, f"DELETE FROM orders WHERE order_number > 10004")
    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'debezium'", True)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    time.sleep(10)

def test_ConnectorStartAlwaysModeDBZ(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_dbz_always"
    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "always")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    if dbvendor != "postgres":
        # check table name mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            id = row[0].split(".")
            if len(id) == 3:
                assert id[0].lower() + "." + id[2].lower() == row[1]
            else:
                assert row[0].lower() == row[1]

        # check attname mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert row[0].lower() == row[1]

        # check data type mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders;")
    assert int(pgrow[0]) == 4
    
    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    assert row[1] == dbvendor
    assert int(row[2]) > 0
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
    assert row[4] == "polling"
    assert row[5] == "no error"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")

def test_ConnectorStartAlwaysModeFDW(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_dbz_always"
    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)

    if dbvendor == "mysql":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'mysql_fdw' ) AS mysql_fdw_available")
        if isfdw[0] == False:
            print ("test_InitialSnapshotFDW_asis skipped - mysql_fdw not available for install")
            assert True
            return
    elif dbvendor == "sqlserver":
        assert True
        return
    elif dbvendor == "postgres":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'postgres_fdw' ) AS postgres_fdw_available")
        if isfdw[0] == False:
            print ("test_InitialSnapshotFDW_asis skipped - postgres_fdw not available for install")
            assert True
            return
    else:
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'oracle_fdw' ) AS oracle_fdw_available")
        if isfdw[0] == False:
            print ("test_ConnectorStartAlwaysModeFDW skipped - oracle_fdw not available for install")
            assert True
            return

    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'fdw'", True)

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "always")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    # check table name mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        id = row[0].split(".")
        if len(id) == 3:
            assert id[0].lower() + "." + id[2].lower() == row[1]
        else:
            assert row[0].lower() == row[1]

    # check attname mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        assert row[0].lower() == row[1]

    # check data type mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders;")
    assert int(pgrow[0]) == 4

    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    assert row[1] == dbvendor
    assert int(row[2]) > 0
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
    assert row[4] == "polling"
    assert row[5] == "no error"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'debezium'", True)
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
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    if dbvendor != "postgres":
        # check table name mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            id = row[0].split(".")
            if len(id) == 3:
                assert id[0].lower() + "." + id[2].lower() == row[1]
            else:
                assert row[0].lower() == row[1]

        # check attname mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert row[0].lower() == row[1]

        # check data type mappings
        rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
        assert len(rows) > 0
        for row in rows:
            assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders;")
    assert int(pgrow[0]) == 0

    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    assert row[1] == dbvendor
    assert int(row[2]) > 0
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
    assert row[4] == "polling"
    assert row[5] == "no error"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")

def test_ConnectorStartNodataModeFDW(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_dbz_nodata"
    dbname = getDbname(dbvendor).lower()
    schema = getSchema(dbvendor)

    if dbvendor == "mysql":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'mysql_fdw' ) AS mysql_fdw_available")
        if isfdw[0] == False:
            print ("test_ConnectorStartNodataModeFDW skipped - mysql_fdw not available for install")
            assert True
            return
    elif dbvendor == "sqlserver":
        assert True
        return
    elif dbvendor == "postgres":
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'postgres_fdw' ) AS postgres_fdw_available")
        if isfdw[0] == False:
            print ("test_ConnectorStartNodataModeFDW skipped - postgres_fdw not available for install")
            assert True
            return
    else:
        isfdw = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM pg_available_extensions WHERE name = 'oracle_fdw' ) AS oracle_fdw_available")
        if isfdw[0] == False:
            print ("test_ConnectorStartNodataModeFDW skipped - oracle_fdw not available for install")
            assert True
            return

    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'fdw'", True)

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
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
        exttblcount = run_remote_query(dbvendor, f"SELECT COUNT(*) FROM user_tables WHERE table_name NOT LIKE 'LOG_MINING%'")
    assert int(pgtblcount[0]) == int(exttblcount[0][0])

    # check table name mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_tbname, pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        id = row[0].split(".")
        if len(id) == 3:
            assert id[0].lower() + "." + id[2].lower() == row[1]
        else:
            assert row[0].lower() == row[1]

    # check attname mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        assert row[0].lower() == row[1]

    # check data type mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND type = '{dbvendor}'")
    assert len(rows) > 0
    for row in rows:
        assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # check data consistency of orders table
    pgrow = run_pg_query_one(pg_cursor, f"SELECT count(*) FROM {dbname}.orders;")
    assert int(pgrow[0]) == 0

    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    assert row[1] == dbvendor
    assert int(row[2]) > 0
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
    assert row[4] == "polling"
    assert row[5] == "no error"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'debezium'", True)
    drop_repslot_and_pub(dbvendor, name, "postgres")

def test_ConnectorRestart(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result == 0

    if dbvendor == "oracle":
        time.sleep(10)
    else:
        time.sleep(5)

    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
    assert row[1] == dbvendor
    oldpid = row[2]
    assert row[2] > -1
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
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
    assert row[3] == "initial snapshot" or row[3] == "change data capture"
    if row[4] == "polling" or row[4] == "restarting":
        assert True
    else:
        assert False
    assert row[5] == "no error"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")

def test_ConnectorStop(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor)
    dbname = getDbname(dbvendor).lower()
    
    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result == 0

    if dbvendor == "oracle":
        time.sleep(10)
    else:
        time.sleep(5)

    row = run_pg_query_one(pg_cursor, f"SELECT synchdb_stop_engine_bgw('{name}')")
    assert row[0] == 0

    time.sleep(5)
    row = run_pg_query_one(pg_cursor, f"SELECT name, connector_type, pid, stage, state, err FROM synchdb_state_view WHERE name = '{name}'")
    assert row[0] == name
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
