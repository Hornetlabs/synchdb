"""Check source-client statement boundaries before the replication suite."""
import pytest

import common


def test_OracleSQLPlusStatements(dbvendor):
    if dbvendor not in ("oracle", "oracle23ai", "olr"):
        pytest.skip("SQL*Plus source client")
    table = "synchdb_sqlplus_test"
    created = False
    try:
        common.run_remote_query(dbvendor,
            f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, payload VARCHAR(32))")
        created = True
        common.run_remote_statements(dbvendor, [
            f"INSERT INTO {table} VALUES (1,'literal; COMMIT')", "COMMIT"])
        # Every query uses a new SQL*Plus session. Verify the committed source
        # data, and ensure semicolons inside a string were not split/replaced.
        assert common.run_remote_query(dbvendor,
            f"SELECT id,payload FROM {table} ORDER BY id") == [("1", "literal; COMMIT")]
        common.run_remote_statements(dbvendor, [
            f"INSERT INTO {table} VALUES (2,'rolled back')", "ROLLBACK"])
        assert common.run_remote_query(dbvendor,
            f"SELECT id FROM {table} ORDER BY id") == [("1",)]
    finally:
        if created:
            common.run_remote_query(dbvendor, f"DROP TABLE {table}")
