"""Build a private source-free OLR module and PostgreSQL cluster."""
import os
from pathlib import Path
import shutil
import subprocess

import psycopg2
from psycopg2 import sql
import pytest


ROOT = Path(__file__).resolve().parents[4]


@pytest.fixture(scope="session")
def database(tmp_path_factory):
    work = tmp_path_factory.mktemp("olr_legacy")
    configured = os.environ.get("SYNCHDB_TARGET_BIN")
    bindir = Path(configured) if configured else Path(
        subprocess.check_output(["pg_config", "--bindir"], text=True).strip())
    config = bindir / "pg_config" if configured else Path(shutil.which("pg_config"))
    module = work / "olr_legacy_test.so"
    env = os.environ.copy()
    java = Path(shutil.which("java")).resolve().parents[1]
    env["LD_LIBRARY_PATH"] = str(java / "lib/server") + os.pathsep + env.get("LD_LIBRARY_PATH", "")
    env["TMPDIR"] = str(work)
    with (work / "build.log").open("w") as output:
        result = subprocess.run([
            "make", "-j4", "USE_PGXS=1", f"PG_CONFIG={config}", "WITH_OLR=1",
            f"OLR_LEGACY_TEST_MODULE={module}", "olr-legacy-test-module"],
            cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT)
    assert result.returncode == 0, (work / "build.log").read_text()
    data = work / "data"
    subprocess.run([bindir / "initdb", "-D", data, "-A", "trust", "--no-locale",
                    "--encoding=UTF8"], env=env, check=True, stdout=subprocess.DEVNULL)
    with (data / "postgresql.conf").open("a") as output:
        output.write(f"\nshared_preload_libraries='{module}'\n"
                     f"unix_socket_directories='{work}'\nlisten_addresses=''\n"
                     "synchdb.synchdb_auto_launcher=off\nmax_worker_processes=24\n")
    started = False
    connection = None
    try:
        subprocess.run([bindir / "pg_ctl", "-D", data, "-l", work / "postgres.log",
                        "-w", "start"], env=env, check=True, stdout=subprocess.DEVNULL)
        started = True
        connection = psycopg2.connect(host=str(work), dbname="postgres")
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("""CREATE FUNCTION olr_fixture_scope(text,text,text,boolean)
                RETURNS boolean AS {}, 'olr_legacy_fixture_scope' LANGUAGE C STRICT""").format(sql.Literal(str(module))))
            cursor.execute(sql.SQL("""CREATE FUNCTION olr_fixture_allow_fragment(jsonb,bigint,bigint,bigint)
                RETURNS boolean AS {}, 'olr_legacy_fixture_allow_fragment' LANGUAGE C STRICT""").format(sql.Literal(str(module))))
            cursor.execute(sql.SQL("""CREATE FUNCTION olr_fixture_prefix(text[],boolean)
                RETURNS integer AS {}, 'olr_legacy_fixture_prefix' LANGUAGE C STRICT""").format(sql.Literal(str(module))))
            for function, returns in (("filter", "jsonb"), ("write", "void")):
                extra = ",boolean" if function == "write" else ""
                cursor.execute(sql.SQL("CREATE FUNCTION olr_fixture_{}(jsonb,bigint,bigint,bigint,bigint,jsonb[]{}) "
                    "RETURNS {} AS {}, {} LANGUAGE C STRICT").format(
                        sql.SQL(function), sql.SQL(extra), sql.SQL(returns), sql.Literal(str(module)),
                        sql.Literal("olr_legacy_fixture_" + function)))
        yield connection, work
    finally:
        if connection is not None:
            connection.close()
        if started:
            subprocess.run([bindir / "pg_ctl", "-D", data, "-w", "stop", "-m", "fast"],
                           env=env, check=True, stdout=subprocess.DEVNULL)


@pytest.fixture
def db(database):
    connection, _ = database
    with connection.cursor() as cursor:
        cursor.execute("DROP SCHEMA IF EXISTS fixture CASCADE; CREATE SCHEMA fixture")
        cursor.execute("CREATE TABLE fixture.items(id bigint PRIMARY KEY)")
        cursor.execute("""CREATE TABLE fixture.positions(
            database_oid oid, connector_id uuid, generation uuid, source_proof jsonb,
            snapshot_scn bigint, scn bigint, c_scn bigint, c_idx bigint)""")
        cursor.execute("""INSERT INTO fixture.positions
            SELECT oid,'00000000-0000-0000-0000-000000000000'::uuid,
            '00000000-0000-0000-0000-000000000000'::uuid,'{}'::jsonb,0,100,50,2
            FROM pg_database WHERE datname=current_database()""")
        yield cursor
