import time
from contextlib import closing

import psycopg2
import pytest

import common


def test_PostgresArraySnapshotAndCDC(pg_cursor, dbvendor):
    if dbvendor != "postgres":
        pytest.skip("PostgreSQL array wire representation")

    name = common.getConnectorName(dbvendor) + "_arrays"
    dbname = common.getDbname(dbvendor).lower()
    definition = """(
        id integer PRIMARY KEY,
        ints integer[], words text[], docs jsonb[], dates date[],
        amounts numeric(10,2)[], flags boolean[], times timestamptz[]
    )"""
    common.run_remote_query(dbvendor, "CREATE TABLE array_codec_test " + definition)
    common.run_remote_query(dbvendor, r"""
        INSERT INTO array_codec_test VALUES (
            1, ARRAY[1,NULL,-2],
            ARRAY['NULL',NULL,'','a,b','{x}','a"b',E'a\\b','single''quote'],
            ARRAY['{"k":"a,b"}','null',NULL,'["x",1]']::jsonb[],
            ARRAY[DATE '2026-01-01',NULL], ARRAY[1.25,-0.50,NULL],
            ARRAY[true,NULL,false], ARRAY[TIMESTAMPTZ '2026-01-01 12:34:56+02',NULL]
        )
    """)
    common.run_pg_query(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
    common.run_pg_query(pg_cursor, f"CREATE TABLE {dbname}.array_codec_test " + definition)

    with closing(psycopg2.connect(host=common.POSTGRES_HOST, port=common.POSTGRES_PORT,
                                 user=common.POSTGRES_USER, password=common.POSTGRES_PASS,
                                 dbname=common.POSTGRES_DB)) as source:
        source.autocommit = True
        with source.cursor() as source_cursor:
            def assert_replicated():
                source_cursor.execute("SELECT * FROM array_codec_test ORDER BY id")
                expected = source_cursor.fetchall()
                deadline = time.monotonic() + 60
                while True:
                    pg_cursor.execute(f"SELECT * FROM {dbname}.array_codec_test ORDER BY id")
                    actual = pg_cursor.fetchall()
                    if actual == expected:
                        return
                    pg_cursor.execute("SELECT state, err FROM synchdb_state_view WHERE name = %s", (name,))
                    state = pg_cursor.fetchone()
                    assert state is None or state[1] == "no error", state
                    assert time.monotonic() < deadline, (actual, expected, state)
                    time.sleep(0.1)

            common.create_and_start_synchdb_connector(
                pg_cursor, dbvendor, name, "initial", srctable="public.array_codec_test")
            assert_replicated()
            pg_cursor.execute(f"SELECT docs[2] IS NULL, docs[3] IS NULL FROM {dbname}.array_codec_test")
            assert pg_cursor.fetchone() == (False, True)
            # Distinguish an empty array, a SQL NULL array and NULL elements.
            source_cursor.execute("""
                INSERT INTO array_codec_test VALUES
                    (2,'{}','{}','{}','{}','{}','{}','{}'),
                    (3,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            """)
            assert_replicated()
            source_cursor.execute("""
                UPDATE array_codec_test SET ints = ARRAY[42,NULL],
                    words = ARRAY['changed','NULL'], docs = ARRAY['{}']::jsonb[],
                    dates = '{}', amounts = ARRAY[99.99], flags = ARRAY[false], times = '{}'
                WHERE id = 1
            """)
            assert_replicated()
            source_cursor.execute("DELETE FROM array_codec_test WHERE id = 1")
            assert_replicated()

    common.stop_and_delete_synchdb_connector(pg_cursor, name)
    common.drop_default_pg_schema(pg_cursor, dbvendor)
    common.drop_repslot_and_pub(dbvendor, name, "postgres")
    common.run_remote_query(dbvendor, "DROP TABLE array_codec_test")
