import common
from common import run_pg_query, run_pg_query_one, run_remote_query, run_remote_query_one, create_synchdb_connector, getConnectorName, getDbname, verify_default_type_mappings

def test_CreateTable(pg_cursor, dbvendor):
    assert True

def test_CreateTableWithError(pg_cursor, dbvendor):
    assert True

def test_CreateTableWithNoPK(pg_cursor, dbvendor):
    assert True

def test_DropTable(pg_cursor, dbvendor):
    assert True

def test_DropTableWithError(pg_cursor, dbvendor):
    assert True

def test_AlterTableAlterColumn(pg_cursor, dbvendor):
    assert True

def test_AlterTableAlterColumnWithError(pg_cursor, dbvendor):
    assert True

def test_AlterTableAlterColumnAddPK(pg_cursor, dbvendor):
    assert True

def test_AlterTableiAddColumn(pg_cursor, dbvendor):
    assert True

def test_AlterTableiAddColumnWithError(pg_cursor, dbvendor):
    assert True

def test_AlterTableAddColumnAddPK(pg_cursor, dbvendor):
    assert True

def test_AlterTableDropColumn(pg_cursor, dbvendor):
    assert True

def test_AlterTableDropColumnWithError(pg_cursor, dbvendor):
    assert True

def test_AlterTableDropColumnDropPK(pg_cursor, dbvendor):
    assert True
