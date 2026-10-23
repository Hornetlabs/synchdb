"""Compile and exercise production C code in an isolated PostgreSQL backend.

SYNCHDB_TARGET_BIN selects PostgreSQL; SYNCHDB_TEST_WITH_OLR=1 adds native OLR.
No installed extension, JVM, remote source database, or Docker is required.
"""
import os
from pathlib import Path
import subprocess

import psycopg2
import pytest
from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[4]
WITH_OLR = os.environ.get("SYNCHDB_TEST_WITH_OLR") == "1"


@pytest.fixture(scope="session")
def database(tmp_path_factory):
    work = tmp_path_factory.mktemp("synchdb_truncate")
    configured = os.environ.get("SYNCHDB_TARGET_BIN")
    bindir = Path(configured) if configured else Path(
        subprocess.check_output(["pg_config", "--bindir"], text=True).strip())
    config = bindir / "pg_config"
    includedir = subprocess.check_output([config, "--includedir-server"], text=True).strip()
    flags = ["-fPIC", "-O0", "-g", "-Wall", "-Werror=implicit-function-declaration",
             f"-I{includedir}", f"-I{ROOT / 'src/include'}"]
    if WITH_OLR:
        flags.append("-DWITH_OLR")
    sources = [Path(__file__).with_name("harness.c"),
               ROOT / "src/backend/converter/debezium_event_handler.c",
               ROOT / "src/backend/executor/replication_agent.c"]
    if WITH_OLR:
        sources.append(ROOT / "src/backend/converter/olr_event_handler.c")
    module = work / "truncate_test.so"
    subprocess.run(["cc", "-shared", "-Wl,-Bsymbolic", *flags, *map(str, sources),
                    "-o", str(module)], check=True)
    subprocess.run([bindir / "initdb", "-D", work / "data", "-A", "trust", "--no-locale"],
                   check=True, stdout=subprocess.DEVNULL)
    log = work / "postgres.log"
    subprocess.run([bindir / "pg_ctl", "-D", work / "data", "-l", log, "-o",
                    f"-k {work} -c listen_addresses=''", "start"], check=True)
    try:
        conn = psycopg2.connect(host=str(work), dbname="postgres")
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""CREATE FUNCTION test_batch(text[],text,text,integer,jsonb,boolean)
                RETURNS jsonb AS {}, 'synchdb_test_batch' LANGUAGE C STRICT""").format(sql.Literal(str(module))))
            cur.execute("""CREATE TABLE synchdb_attribute (
                name text, type text, attrelid oid, attnum integer, ext_tbname text,
                ext_attname text, ext_atttypename text,
                UNIQUE(name,type,attrelid,attnum))""")
        yield conn, log
        conn.close()
    finally:
        subprocess.run([bindir / "pg_ctl", "-D", work / "data", "stop", "-m", "fast"], check=True)


@pytest.fixture
def db(database):
    conn, log = database
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS inventory CASCADE; CREATE SCHEMA inventory")
        cur.execute("DROP SCHEMA IF EXISTS shadow CASCADE; CREATE SCHEMA shadow")
        cur.execute("TRUNCATE synchdb_attribute")
        yield cur
