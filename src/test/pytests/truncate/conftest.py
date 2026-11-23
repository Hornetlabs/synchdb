"""Build a private module and cluster; no installed SynchDB or source needed."""
import os
from pathlib import Path
import shutil
import subprocess

import psycopg2
from psycopg2 import sql
import pytest


ROOT = Path(__file__).resolve().parents[4]
WITH_OLR = os.environ.get("SYNCHDB_TEST_WITH_OLR", "0") == "1"


@pytest.fixture(scope="session")
def database(tmp_path_factory):
    work = tmp_path_factory.mktemp("synchdb_truncate")
    configured = os.environ.get("SYNCHDB_TARGET_BIN")
    bindir = Path(configured) if configured else Path(
        subprocess.check_output(["pg_config", "--bindir"], text=True).strip())
    config = bindir / "pg_config"
    version = subprocess.check_output([config, "--version"], text=True)
    module = work / "truncate_test.so"
    env = os.environ.copy()
    java = Path(shutil.which("java")).resolve().parents[1]
    env["LD_LIBRARY_PATH"] = str(java / "lib/server") + os.pathsep + env.get("LD_LIBRARY_PATH", "")
    # Compiler temporary files belong to this test run as well.
    env["TMPDIR"] = str(work)
    build_log = work / "build.log"
    with build_log.open("w") as log:
        result = subprocess.run([
            "make", "-j4", "USE_PGXS=1", f"PG_CONFIG={config}",
            f"WITH_OLR={int(WITH_OLR)}", f"TRUNCATE_TEST_MODULE={module}",
            "truncate-test-module"], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    assert result.returncode == 0, build_log.read_text()
    data = work / "data"
    subprocess.run([bindir / "initdb", "-D", data, "-A", "trust", "--no-locale",
                    "--encoding=UTF8"], env=env, check=True, stdout=subprocess.DEVNULL)
    with (data / "postgresql.conf").open("a") as conf:
        conf.write(f"\nshared_preload_libraries='{module}'\n"
                   f"unix_socket_directories='{work}'\nlisten_addresses=''\n"
                   "synchdb.synchdb_auto_launcher=off\nmax_worker_processes=24\n")
        if (os.environ.get("SYNCHDB_TEST_TARGET_FLAVOR") == "ivorysql" or
                "ivorysql" in version.lower()):
            # These tests assert PostgreSQL identifier semantics, matching the
            # live suite rather than IvorySQL's default Oracle-compatible mode.
            conf.write("ivorysql.compatible_mode='pg'\n")
    started = False
    try:
        subprocess.run([bindir / "pg_ctl", "-D", data, "-l", work / "postgres.log",
                        "-w", "start"], env=env, check=True, stdout=subprocess.DEVNULL)
        started = True
        connection = psycopg2.connect(host=str(work), dbname="postgres")
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("""CREATE FUNCTION truncate_fixture_batch(text[],boolean,text)
                RETURNS jsonb AS {}, 'truncate_fixture_batch' LANGUAGE C VOLATILE""").format(
                    sql.Literal(str(module))))
        yield connection, data
        connection.close()
    finally:
        if started:
            subprocess.run([bindir / "pg_ctl", "-D", data, "-w", "stop", "-m", "fast"],
                           env=env, check=True, stdout=subprocess.DEVNULL)


@pytest.fixture
def db(database):
    connection, _ = database
    with connection.cursor() as cursor:
        cursor.execute("DROP SCHEMA IF EXISTS fixture CASCADE; CREATE SCHEMA fixture")
        cursor.execute("CREATE TABLE fixture.items(id integer, value text)")
        cursor.execute("INSERT INTO fixture.items VALUES (1,'before')")
        yield cursor
