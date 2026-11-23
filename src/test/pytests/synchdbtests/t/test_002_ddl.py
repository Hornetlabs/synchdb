import common
import time
from common import restart_remote_db, run_pg_query, run_pg_query_one, run_remote_query, create_synchdb_connector, getConnectorName, getDbname, verify_default_type_mappings, create_and_start_synchdb_connector, stop_and_delete_synchdb_connector, drop_default_pg_schema, drop_repslot_and_pub

import pytest


@pytest.fixture(autouse=True)
def olr_ddl_metadata_interval(dbvendor):
    yield
    if dbvendor == "olr":
        # Leave the Oracle Flashback metadata interval after the last source
        # DDL before the next test starts its native AS OF snapshot.
        time.sleep(15)


def _ddl_query(cursor, vendor, name, query, ready, timeout=300):
    if vendor not in ("olr", "sqlserver"):
        return run_pg_query(cursor, query)
    deadline = time.monotonic() + timeout
    while True:
        rows = run_pg_query(cursor, query)
        try:
            if ready(rows):
                return rows
        except IndexError:
            pass  # The expected columns/constraints have not arrived yet.
        cursor.execute("SELECT actual_state,last_error FROM synchdb_control_view WHERE name=%s", (name,))
        state = cursor.fetchone()
        assert state and state[0] not in ("FAILED", "RETRY_WAIT"), state
        assert time.monotonic() < deadline, f"{vendor} DDL did not reach its expected target state: {rows}"
        time.sleep(0.2)


def _ddl_delay(vendor, oracle, oracle23ai, other):
    # Native OLR startup waits for its handoff, and _ddl_query polls DDL results.
    seconds = {"olr": 0, "oracle": oracle, "oracle23ai": oracle23ai}.get(vendor, other)
    if seconds:
        time.sleep(seconds)


def test_CreateTable(pg_cursor, dbvendor):

    if dbvendor == "oracle23ai":
        restart_remote_db(dbvendor)

    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    mode = "no_data" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    _ddl_delay(dbvendor, 30, 30, 10)

    if dbvendor == "mysql":
        query = """
        CREATE TABLE create_table_test (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            created_at DATETIME
        );
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE create_table_test (
            id INT PRIMARY KEY IDENTITY(1,1),
            name VARCHAR(255),
            created_at DATETIME
        );
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'create_table_test', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        query = """
        CREATE TABLE create_table_test (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            created_at TIMESTAMP WITHOUT TIME ZONE
        );
        """
    else:
        query = """
        CREATE TABLE create_table_test (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    _ddl_delay(dbvendor, 60, 100, 20)

    connecotrType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view 
            WHERE name = '{name}' AND 
            type = '{connecotrType}' 
            AND pg_tbname = '{dbname}.create_table_test'
        """,
        lambda rows: (len(rows) == 3))
    assert len(rows) == 3
    if dbvendor == "olr":
        assert run_pg_query_one(pg_cursor,
            f"SELECT bad_events FROM synchdb_genstats WHERE name='{name}'") == (0,)

    
    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, "DROP TABLE create_table_test")
    drop_repslot_and_pub(dbvendor, name, "postgres")

def test_CreateTableWithSpace(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    mode = "no_data" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    _ddl_delay(dbvendor, 30, 30, 10)

    if dbvendor == "mysql":
        query = """
        CREATE TABLE `create table test` (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            created_at DATETIME
        );
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE [create table test] (
            id INT PRIMARY KEY IDENTITY(1,1),
            name VARCHAR(255),
            created_at DATETIME
        );
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'create table test', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        query = """
        CREATE TABLE \"create table test\" (
            id int PRIMARY KEY,
            name VARCHAR(255),
            created_at TIMESTAMP WITHOUT TIME ZONE
        );
        """
    else:
        query = """
        CREATE TABLE "create table test" (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    _ddl_delay(dbvendor, 90, 120, 20)

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor

    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view 
            WHERE name = '{name}' AND 
            type = '{connectorType}' 
            AND pg_tbname = '{dbname}.create table test'
        """,
        lambda rows: (len(rows) == 3))

    assert len(rows) == 3

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")

    if dbvendor == "mysql":
        run_remote_query(dbvendor, "DROP TABLE `create table test`")
    elif dbvendor == "sqlserver":
        run_remote_query(dbvendor, "DROP TABLE [create table test]")
    elif dbvendor == "postgres":
        run_remote_query(dbvendor, "DROP TABLE \"create table test\"")
    else:
        run_remote_query(dbvendor, "DROP TABLE \"create table test\"")

def test_CreateTableWithNoPK(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    mode = "no_data" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    _ddl_delay(dbvendor, 60, 60, 10)

    if dbvendor == "mysql":
        query = """
        CREATE TABLE create_table_nopk (
            id INT,
            name VARCHAR(255),
            created_at DATETIME
        );
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE create_table_nopk (
            id INT,
            name VARCHAR(255),
            created_at DATETIME
        );
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'create_table_nopk', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        query = """
        CREATE TABLE create_table_nopk (
            id INT,
            name VARCHAR(255),
            created_at TIMESTAMP WITHOUT TIME ZONE
        );
        """
    else:
        query = """
        CREATE TABLE create_table_nopk (
            id NUMBER,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    _ddl_delay(dbvendor, 80, 120, 40)

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{connectorType}'
            AND pg_tbname = '{dbname}.create_table_nopk'
        """,
        lambda rows: (len(rows) == 3))
    assert len(rows) == 3

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    run_remote_query(dbvendor, "DROP TABLE create_table_nopk")

def test_CreateTableWithNotInlinePK(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    mode = "no_data" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    _ddl_delay(dbvendor, 30, 30, 10)

    if dbvendor == "mysql":
        query = """
        CREATE TABLE create_table_noinlinepk (
            id INT,
            name VARCHAR(255),
            created_at DATETIME,
            CONSTRAINT pk_create_table_test PRIMARY KEY (id)
        );
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE create_table_noinlinepk (
            id INT,
            name VARCHAR(255),
            created_at DATETIME,
            CONSTRAINT pk_create_table_test PRIMARY KEY (id)
        );
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'create_table_noinlinepk', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        query = """
        CREATE TABLE create_table_noinlinepk (
            id INT,
            name VARCHAR(255),
            created_at TIMESTAMP WITHOUT TIME ZONE,
            CONSTRAINT pk_create_table_test PRIMARY KEY (id)
        );
        """
    else:
        query = """
        CREATE TABLE create_table_noinlinepk (
            id NUMBER,
            name VARCHAR2(255),
            created_at DATE,
            CONSTRAINT pk_create_table_test PRIMARY KEY (id)
        );
        """
    run_remote_query(dbvendor, query)
    _ddl_delay(dbvendor, 90, 120, 20)

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{connectorType}'
            AND pg_tbname = '{dbname}.create_table_noinlinepk'
        """,
        lambda rows: (len(rows) == 3))
    assert len(rows) == 3

    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
    SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = '{dbname}'
        AND table_name = 'create_table_noinlinepk'
        AND constraint_type = 'PRIMARY KEY';
    """,
        lambda rows: (rows[0][0] == "create_table_noinlinepk_pkey"))
    assert rows[0][0] == "create_table_noinlinepk_pkey"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    run_remote_query(dbvendor, "DROP TABLE create_table_noinlinepk")

def test_DropTable(pg_cursor, dbvendor):

    if dbvendor == "oracle23ai":
        restart_remote_db(dbvendor)

    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    mode = "no_data" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    _ddl_delay(dbvendor, 30, 30, 10)

    if dbvendor == "mysql":
        query = """
        CREATE TABLE drop_table_test (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            created_at DATETIME
        );
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE drop_table_test (
            id INT PRIMARY KEY IDENTITY(1,1),
            name VARCHAR(255),
            created_at DATETIME
        );
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'drop_table_test', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        query = """
        CREATE TABLE drop_table_test (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            created_at TIMESTAMP WITHOUT TIME ZONE
        );
        """
    else:
        query = """
        CREATE TABLE drop_table_test (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    _ddl_delay(dbvendor, 60, 100, 20)

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{connectorType}'
            AND pg_tbname = '{dbname}.drop_table_test'
        """,
        lambda rows: (len(rows) == 3))
    assert len(rows) == 3

    run_remote_query(dbvendor, "DROP TABLE drop_table_test")

    _ddl_delay(dbvendor, 60, 60, 20)

    # SQL Server was already exempt from the DROP metadata assertion; do not
    # make its asynchronous DDL poll require that unsupported result either.
    if dbvendor != "sqlserver":
        connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
        rows = _ddl_query(pg_cursor, dbvendor, name, f"""
            SELECT ext_tbname, pg_tbname FROM synchdb_att_view
                WHERE name = '{name}' AND
                type = '{connectorType}'
                AND pg_tbname = '{dbname}.drop_table_test'
            """,
            lambda rows: (len(rows) == 0))
        assert len(rows) == 0

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")

def test_DropTableWithSpace(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()
    
    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    mode = "no_data" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    _ddl_delay(dbvendor, 30, 30, 10)

    if dbvendor == "mysql":
        query = """
        CREATE TABLE `drop with space` (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            created_at DATETIME
        );
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE [drop with space] (
            id INT PRIMARY KEY IDENTITY(1,1),
            name VARCHAR(255),
            created_at DATETIME
        );
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'drop with space', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        query = """
        CREATE TABLE \"drop with space\" (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            created_at TIMESTAMP WITHOUT TIME ZONE
        );
        """
    else:
        query = """
        CREATE TABLE "drop with space" (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    _ddl_delay(dbvendor, 60, 100, 20)

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{connectorType}'
            AND pg_tbname = '{dbname}.drop with space'
        """,
        lambda rows: (len(rows) == 3))
    assert len(rows) == 3

    if dbvendor == "mysql":
        run_remote_query(dbvendor, "DROP TABLE `drop with space`")
    elif dbvendor == "sqlserver":
        run_remote_query(dbvendor, "DROP TABLE [drop with space]")
    elif dbvendor == "postgres":
        run_remote_query(dbvendor, "DROP TABLE \"drop with space\"")
    else:
        run_remote_query(dbvendor, "DROP TABLE \"drop with space\"")

    _ddl_delay(dbvendor, 60, 100, 20)

    # Preserve the same SQL Server exception as the unquoted DROP case.
    if dbvendor != "sqlserver":
        connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
        rows = _ddl_query(pg_cursor, dbvendor, name, f"""
            SELECT ext_tbname, pg_tbname FROM synchdb_att_view
                WHERE name = '{name}' AND
                type = '{connectorType}'
                AND pg_tbname = '{dbname}.drop with space'
            """,
            lambda rows: (len(rows) == 0))
        assert len(rows) == 0

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")

def test_AlterTableAlterColumn(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    mode = "no_data" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    _ddl_delay(dbvendor, 30, 30, 10)

    if dbvendor == "mysql":
        query = """
        CREATE TABLE alter_table_alter_col (
            id INT PRIMARY KEY AUTO_INCREMENT,
            age INT,
            created_at DATETIME
        );
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE alter_table_alter_col (
            id INT PRIMARY KEY IDENTITY(1,1),
            age INT,
            created_at DATETIME
        );
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'alter_table_alter_col', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        query = """
        CREATE TABLE alter_table_alter_col (
            id INT PRIMARY KEY,
            age INT,
            created_at TIMESTAMP WITHOUT TIME ZONE
        );
        """
    else:
        query = """
        CREATE TABLE alter_table_alter_col (
            id NUMBER PRIMARY KEY,
            age NUMBER,
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    _ddl_delay(dbvendor, 60, 100, 20)

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{connectorType}'
            AND pg_tbname = '{dbname}.alter_table_alter_col'
        """,
        lambda rows: (len(rows) == 3))
    assert len(rows) == 3

    if dbvendor == "mysql":
        run_remote_query(dbvendor, "ALTER TABLE alter_table_alter_col MODIFY COLUMN age BIGINT")
    elif dbvendor == "sqlserver":
        run_remote_query(dbvendor, "ALTER TABLE alter_table_alter_col ALTER COLUMN age BIGINT")
    elif dbvendor == "postgres":
        run_remote_query(dbvendor, "ALTER TABLE alter_table_alter_col ALTER COLUMN age TYPE BIGINT")
    else:
        run_remote_query(dbvendor, "ALTER TABLE alter_table_alter_col MODIFY age NUMBER(10,0)")


    _ddl_delay(dbvendor, 60, 100, 20)

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname, ext_atttypename, pg_atttypename FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{connectorType}'
            AND pg_tbname = '{dbname}.alter_table_alter_col'
        """,
        lambda rows: (len(rows) == 3) and (rows[1][3] == "bigint"))
    assert len(rows) == 3
    assert rows[1][3] == "bigint"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    run_remote_query(dbvendor, "DROP TABLE alter_table_alter_col")

def test_AlterTableAlterColumnAddPK(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    mode = "no_data" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    _ddl_delay(dbvendor, 30, 30, 10)

    if dbvendor == "mysql":
        query = """
        CREATE TABLE alter_table_addpk (
            id INT,
            name VARCHAR(255),
            created_at DATETIME
        );
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE alter_table_addpk (
            id INT NOT NULL,
            name VARCHAR(255),
            created_at DATETIME
        );
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'alter_table_addpk', @role_name = NULL,
            @supports_net_changes = 0,
            @capture_instance='alter_table_add_pk_1';
        """
    elif dbvendor == "postgres":
        query = """
        CREATE TABLE alter_table_addpk (
            id INT,
            name VARCHAR(255),
            created_at TIMESTAMP WITHOUT TIME ZONE
        );
        """
    else:
        query = """
        CREATE TABLE alter_table_addpk (
            id NUMBER,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    _ddl_delay(dbvendor, 80, 120, 20)

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{connectorType}'
            AND pg_tbname = '{dbname}.alter_table_addpk'
        """,
        lambda rows: (len(rows) == 3))
    assert len(rows) == 3

    ## add pk here
    if dbvendor == "sqlserver":
        run_remote_query(dbvendor, """
            ALTER TABLE alter_table_addpk
                ADD CONSTRAINT pk_create_table_addpk PRIMARY KEY (id);
            """)

        rows = run_remote_query(dbvendor, """
            EXEC sys.sp_cdc_disable_table @source_schema='dbo',
            @source_name='alter_table_addpk',
            @capture_instance='alter_table_add_pk_1';
        """)

        rows = run_remote_query(dbvendor, """
            EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'alter_table_addpk', @role_name = NULL,
            @supports_net_changes = 0,
            @capture_instance = 'alter_table_add_pk_2';
        """)
    elif dbvendor == "postgres":
        run_remote_query(dbvendor, """
            ALTER TABLE alter_table_addpk
                ADD PRIMARY KEY (id);
            """)
    else:
        run_remote_query(dbvendor, """
            ALTER TABLE alter_table_addpk 
                ADD CONSTRAINT pk_create_table_addpk PRIMARY KEY (id);
            """)

    _ddl_delay(dbvendor, 60, 100, 20)

    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
    SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = '{dbname}'
        AND table_name = 'alter_table_addpk' 
        AND constraint_type = 'PRIMARY KEY';;
    """,
        lambda rows: (rows[0][0] == "alter_table_addpk_pkey" or rows[0][0] == "pk_create_table_addpk"))
    
    assert rows[0][0] == "alter_table_addpk_pkey" or rows[0][0] == "pk_create_table_addpk"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    run_remote_query(dbvendor, "DROP TABLE alter_table_addpk")
    assert True

def test_AlterTableiAddColumn(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    mode = "no_data" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    _ddl_delay(dbvendor, 30, 30, 10)

    if dbvendor == "mysql":
        query = """
        CREATE TABLE alter_table_add_col (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            created_at DATETIME
        );
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE alter_table_add_col (
            id INT PRIMARY KEY IDENTITY(1,1),
            name VARCHAR(255),
            created_at DATETIME
        );
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'alter_table_add_col', @role_name = NULL,
            @supports_net_changes = 0,
            @capture_instance='alter_table_add_col_1';
        """
    elif dbvendor == "postgres":
        query = """
        CREATE TABLE alter_table_add_col (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            created_at TIMESTAMP WITHOUT TIME ZONE
        );
        """
    else:
        query = """
        CREATE TABLE alter_table_add_col (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    _ddl_delay(dbvendor, 60, 100, 20)

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{connectorType}'
            AND pg_tbname = '{dbname}.alter_table_add_col'
        """,
        lambda rows: (len(rows) == 3))
    assert len(rows) == 3
    
    if dbvendor == "mysql":
        run_remote_query(dbvendor, "ALTER TABLE alter_table_add_col ADD COLUMN age INT")
    elif dbvendor == "sqlserver":
        rows = run_remote_query(dbvendor, "ALTER TABLE alter_table_add_col ADD age INT;")

        # Keep the reader's current capture table alive while it crosses the
        # new instance's start LSN. Dropping it first races an in-flight poll.
        # Both instances are removed with the source table after worker stop.
        rows = run_remote_query(dbvendor, """
            EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'alter_table_add_col', @role_name = NULL,
            @supports_net_changes = 0,
            @capture_instance = 'alter_table_add_col_2';
        """)

        rows = run_remote_query(dbvendor, "INSERT INTO alter_table_add_col(name, created_at, age) VALUES('s', '16-JAN-2025', 35);")
    elif dbvendor == "postgres":
        run_remote_query(dbvendor, "ALTER TABLE alter_table_add_col ADD COLUMN age INT")
    else:
        run_remote_query(dbvendor, "ALTER TABLE alter_table_add_col ADD age NUMBER")

    _ddl_delay(dbvendor, 60, 100, 20)

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname, ext_attname, pg_attname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{connectorType}'
            AND pg_tbname = '{dbname}.alter_table_add_col'
        ORDER BY attnum
        """,
        lambda rows: (len(rows) == 4) and (rows[3][3] == "age"))
    assert len(rows) == 4
    assert rows[3][3] == "age"

    if dbvendor == "sqlserver":
        rows = _ddl_query(pg_cursor, dbvendor, name,
            f"SELECT age FROM {dbname}.alter_table_add_col WHERE name='s'",
            lambda rows: rows == [(35,)])
        assert rows == [(35,)]

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    run_remote_query(dbvendor, "DROP TABLE alter_table_add_col")


def test_AlterTableDropColumn(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    mode = "no_data" if dbvendor == "sqlserver" else "initial"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, mode)
    assert result > 0

    _ddl_delay(dbvendor, 30, 30, 10)

    if dbvendor == "mysql":
        query = """
        CREATE TABLE alter_table_drop_col (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            created_at DATETIME
        );
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE alter_table_drop_col (
            id INT PRIMARY KEY IDENTITY(1,1),
            name VARCHAR(255),
            created_at DATETIME
        );
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'alter_table_drop_col', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        query = """
        CREATE TABLE alter_table_drop_col (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            created_at TIMESTAMP WITHOUT TIME ZONE
        );
        """
    else:
        query = """
        CREATE TABLE alter_table_drop_col (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    _ddl_delay(dbvendor, 80, 120, 20)

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{connectorType}'
            AND pg_tbname = '{dbname}.alter_table_drop_col'
        """,
        lambda rows: (len(rows) == 3))
    assert len(rows) == 3

    run_remote_query(dbvendor, "ALTER TABLE alter_table_drop_col DROP COLUMN created_at")

    _ddl_delay(dbvendor, 60, 100, 20)

    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT column_name FROM information_schema.columns
         WHERE table_schema = '{dbname}' AND table_name = 'alter_table_drop_col'
         ORDER BY ordinal_position
        """,
        lambda rows: (rows == [('id',), ('name',)]))
    assert rows == [('id',), ('name',)]

    connectorType = "oracle" if dbvendor == "oracle23ai" else dbvendor
    rows = _ddl_query(pg_cursor, dbvendor, name, f"""
        SELECT v.ext_tbname, v.pg_tbname, v.ext_attname, v.pg_attname
          FROM synchdb_att_view v
          JOIN pg_catalog.pg_attribute a
            ON a.attrelid = '{dbname}.alter_table_drop_col'::regclass
           AND a.attnum = v.attnum AND NOT a.attisdropped
         WHERE v.name = '{name}' AND v.type = '{connectorType}'
           AND v.pg_tbname = '{dbname}.alter_table_drop_col'
        """,
        lambda rows: (len(rows) == 2) and ({row[2].lower() for row in rows} == {'id', 'name'}) and ({row[3] for row in rows} == {'id', 'name'}))
    assert len(rows) == 2
    assert {row[2].lower() for row in rows} == {'id', 'name'}
    assert {row[3] for row in rows} == {'id', 'name'}

    if dbvendor == "olr":
        # Incremental OLR DDL retains the dropped physical slot as a binding
        # tombstone; it must not become an active source-column mapping.
        assert run_pg_query_one(pg_cursor, f"""
            SELECT count(*) FROM synchdb_att_view v
            JOIN pg_catalog.pg_attribute a
              ON a.attrelid = '{dbname}.alter_table_drop_col'::regclass
             AND a.attnum = v.attnum AND a.attisdropped
            WHERE v.name = '{name}' AND v.type = 'olr'
              AND v.pg_tbname = '{dbname}.alter_table_drop_col'
        """) == (1,)

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    run_remote_query(dbvendor, "DROP TABLE alter_table_drop_col")
