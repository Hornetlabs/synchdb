import common
import time
from common import run_pg_query, run_pg_query_one, run_remote_query, create_synchdb_connector, getConnectorName, getDbname, verify_default_type_mappings, create_and_start_synchdb_connector, stop_and_delete_synchdb_connector, drop_default_pg_schema

def test_CreateTable(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

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
    else:
        query = """
        CREATE TABLE create_table_test (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)
    
    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view 
            WHERE name = '{name}' AND 
            type = '{dbvendor}' 
            AND pg_tbname = '{dbname}.create_table_test'
        """)
    assert len(rows) == 3
    
    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, "DROP TABLE create_table_test")

def test_CreateTableWithSpace(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

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
    else:
        query = """
        CREATE TABLE "create table test" (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(90)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view 
            WHERE name = '{name}' AND 
            type = '{dbvendor}' 
            AND pg_tbname = '{dbname}.create table test'
        """)

    assert len(rows) == 3

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)

    if dbvendor == "mysql":
        run_remote_query(dbvendor, "DROP TABLE `create table test`")
    elif dbvendor == "sqlserver":
        run_remote_query(dbvendor, "DROP TABLE [create table test]")
    else:
        run_remote_query(dbvendor, "DROP TABLE \"create table test\"")

def test_CreateTableWithNoPK(pg_cursor, dbvendor):

    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

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
    else:
        query = """
        CREATE TABLE create_table_nopk (
            id NUMBER,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.create_table_nopk'
        """)
    assert len(rows) == 3

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, "DROP TABLE create_table_nopk")

def test_CreateTableWithNotInlinePK(pg_cursor, dbvendor):

    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

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
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.create_table_noinlinepk'
        """)
    assert len(rows) == 3

    rows = run_pg_query(pg_cursor, f"""
    SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = '{dbname}'
        AND table_name = 'create_table_noinlinepk'
        AND constraint_type = 'PRIMARY KEY';
    """)
    assert rows[0][0] == "create_table_noinlinepk_pkey"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, "DROP TABLE create_table_noinlinepk")

def test_DropTable(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

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
    else:
        query = """
        CREATE TABLE drop_table_test (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.drop_table_test'
        """)
    assert len(rows) == 3

    run_remote_query(dbvendor, "DROP TABLE drop_table_test")

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.drop_table_test'
        """)
    
    ### sqlserver treats drop table as alter table drop all columns
    if dbvendor != "sqlserver":
        assert len(rows) == 0

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)

def test_DropTableWithSpace(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

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
    else:
        query = """
        CREATE TABLE "drop with space" (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.drop with space'
        """)
    assert len(rows) == 3

    if dbvendor == "mysql":
        run_remote_query(dbvendor, "DROP TABLE `drop with space`")
    elif dbvendor == "sqlserver":
        run_remote_query(dbvendor, "DROP TABLE [drop with space]")
    else:
        run_remote_query(dbvendor, "DROP TABLE \"drop with space\"")

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.drop with space'
        """)
    
    ### sqlserver treats drop table as alter table drop all columns
    if dbvendor != "sqlserver":
        assert len(rows) == 0

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    
def test_AlterTableAlterColumn(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

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
    else:
        query = """
        CREATE TABLE alter_table_alter_col (
            id NUMBER PRIMARY KEY,
            age NUMBER,
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.alter_table_alter_col'
        """)
    assert len(rows) == 3

    if dbvendor == "mysql":
        run_remote_query(dbvendor, "ALTER TABLE alter_table_alter_col MODIFY COLUMN age BIGINT")
    elif dbvendor == "sqlserver":
        run_remote_query(dbvendor, "ALTER TABLE alter_table_alter_col ALTER COLUMN age BIGINT")
    else:
        run_remote_query(dbvendor, "ALTER TABLE alter_table_alter_col MODIFY age NUMBER(10,0)")


    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname, ext_atttypename, pg_atttypename FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.alter_table_alter_col'
        """)
    assert len(rows) == 3
    assert rows[1][3] == "bigint"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, "DROP TABLE alter_table_alter_col")

def test_AlterTableAlterColumnAddPK(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

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
    else:
        query = """
        CREATE TABLE alter_table_addpk (
            id NUMBER,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.alter_table_addpk'
        """)
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
    else:
        run_remote_query(dbvendor, """
            ALTER TABLE alter_table_addpk 
                ADD CONSTRAINT pk_create_table_addpk PRIMARY KEY (id);
            """)

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
    SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = '{dbname}'
        AND table_name = 'alter_table_addpk' 
        AND constraint_type = 'PRIMARY KEY';;
    """)
    
    assert rows[0][0] == "alter_table_addpk_pkey" or rows[0][0] == "pk_create_table_addpk"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, "DROP TABLE alter_table_addpk")


    assert True

def test_AlterTableiAddColumn(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

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
    else:
        query = """
        CREATE TABLE alter_table_add_col (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.alter_table_add_col'
        """)
    assert len(rows) == 3
    
    if dbvendor == "mysql":
        run_remote_query(dbvendor, "ALTER TABLE alter_table_add_col ADD COLUMN age INT")
    elif dbvendor == "sqlserver":
        rows = run_remote_query(dbvendor, "ALTER TABLE alter_table_add_col ADD age INT;")

        rows = run_remote_query(dbvendor, """
            EXEC sys.sp_cdc_disable_table @source_schema='dbo',
            @source_name='alter_table_add_col',
            @capture_instance='alter_table_add_col_1';
        """)

        rows = run_remote_query(dbvendor, """
            EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'alter_table_add_col', @role_name = NULL,
            @supports_net_changes = 0,
            @capture_instance = 'alter_table_add_col_2';
        """)

        rows = run_remote_query(dbvendor, "INSERT INTO alter_table_add_col(name, created_at, age) VALUES('s', '16-JAN-2025', 35);")
    else:
        run_remote_query(dbvendor, "ALTER TABLE alter_table_add_col ADD age NUMBER")

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname, ext_attname, pg_attname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.alter_table_add_col'
        """)
    assert len(rows) == 4
    assert rows[3][3] == "age"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, "DROP TABLE alter_table_add_col")

def test_AlterTableDropColumn(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_ddl"
    dbname = getDbname(dbvendor).lower()

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(10)

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
    else:
        query = """
        CREATE TABLE alter_table_drop_col (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(255),
            created_at DATE
        );
        """
    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.alter_table_drop_col'
        """)
    assert len(rows) == 3

    run_remote_query(dbvendor, "ALTER TABLE alter_table_drop_col DROP COLUMN created_at")
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    rows = run_pg_query(pg_cursor, f"""
        SELECT COUNT(*) from synchdb_att_view WHERE ext_attname LIKE '%synchdb.dropped.%'
        """)
    assert rows[0][0] == 1

    rows = run_pg_query(pg_cursor, f"""
        SELECT ext_tbname, pg_tbname, ext_attname, pg_attname FROM synchdb_att_view
            WHERE name = '{name}' AND
            type = '{dbvendor}'
            AND pg_tbname = '{dbname}.alter_table_drop_col'
        """)
    assert len(rows) == 3
    assert rows[2][2] == "........synchdb.dropped.3........" and rows[2][3] == "........pg.dropped.3........"

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    run_remote_query(dbvendor, "DROP TABLE alter_table_drop_col")
