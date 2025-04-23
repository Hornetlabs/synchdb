import os
import subprocess
import tempfile
import time
import shutil
import psycopg2
import pytest

PG_PORT = "5432"
PG_HOST = "127.0.0.1"

@pytest.fixture(scope="session")
def pg_instance(request):
    #temp_dir = tempfile.mkdtemp(prefix="synchdb_pg_")
    temp_dir = "synchdb_testdir"
    data_dir = os.path.join(temp_dir, "data")
    log_file = os.path.join(temp_dir, "logfile")
    
    # remove dir if exists
    if os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir)

    # Init DB
    subprocess.run(["initdb", "-D", data_dir], check=True, stdout=subprocess.DEVNULL)
    
    conf_file = os.path.join(data_dir, "postgresql.conf")
    with open(conf_file, "a") as f:
        f.write("\nlog_min_messages = debug1\n")

    # Start Postgres
    #print("[setup] setting up postgresql for test...")
    subprocess.run([
        "pg_ctl", "-D", data_dir, "-o", f"-p {PG_PORT}", "-l", log_file, "start"
    ], check=True)

    # Wait for startup
    for _ in range(10):
        try:
            conn = psycopg2.connect(host=PG_HOST, dbname="postgres", port=PG_PORT)
            conn.close()
            break
        except Exception as e:
            time.sleep(1)
    else:
        with open(log_file) as f:
            print(f.read())
        raise RuntimeError("PostgreSQL failed to start")

    # Yield PostgreSQL runtime info
    yield {
        "host": PG_HOST,
        "port": PG_PORT,
        "dbname": "postgres",
        "temp_dir": temp_dir,
        "data_dir": data_dir,
        "log_file": log_file
    }
    
    # do not remove postgresql server dir if failed
    if request.session.testsfailed > 0:
        print(f"test failed: postgresql server dir and log retained at {data_dir} and {log_file}")
        subprocess.run(["pg_ctl", "-D", data_dir, "stop", "-m", "immediate"], check=True, stdout=subprocess.DEVNULL)
    else:
        subprocess.run(["pg_ctl", "-D", data_dir, "stop", "-m", "immediate"], check=True, stdout=subprocess.DEVNULL)
        shutil.rmtree(temp_dir)

@pytest.fixture(scope="session")
def pg_cursor(pg_instance):
    # Establish one shared connection + cursor
    conn = psycopg2.connect(
        host=pg_instance["host"],
        dbname=pg_instance["dbname"],
        port=pg_instance["port"]
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Create extension (only once)
    cur.execute("CREATE EXTENSION IF NOT EXISTS synchdb CASCADE;")

    yield cur
    #print("tearing down pg_cursor..")
    cur.close()
    conn.close()

def pytest_addoption(parser):
    parser.addoption(
        "--dbvendor", action="store", default="all",
        help="Source database vendor to test against (mysql, sqlserver, oracle)"
    )

@pytest.fixture(scope="session")
def dbvendor(pytestconfig):
    return pytestconfig.getoption("dbvendor")

@pytest.fixture(scope="session", autouse=True)
def setup_remote_instance(dbvendor, request):
    env = os.environ.copy()
    env["DBTYPE"] = dbvendor

    #print(f"[setup] setting up heterogeneous database {dbvendor}...")
    subprocess.run(["bash", "./ci/setup-remotedbs.sh"], check=True, env=env, stdout=subprocess.DEVNULL)
    
    yield

    teardown_remote_instance(dbvendor)

def teardown_remote_instance(dbvendor):
    env = os.environ.copy()
    env["DBTYPE"] = dbvendor

    subprocess.run(["bash", "./ci/teardown-remotedbs.sh"], check=True, env=env, stdout=subprocess.DEVNULL)
