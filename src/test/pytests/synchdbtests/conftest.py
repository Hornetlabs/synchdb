import os
import subprocess
import time
import shutil
import psycopg2
import pytest

from common import TEST_DATA_DIR

PG_PORT = "14141"
PG_HOST = "127.0.0.1"


# ===========================================================================
# Source database — what we replicate FROM.
#
# `vendor` is one of the six legacy strings the test files and common.py
# helpers key on (mysql/sqlserver/oracle/oracle23ai/olr/postgres), so the
# `dbvendor` fixture stays byte-compatible.  `setup_key` is the DBTYPE the
# proven ci/setup-remotedbs.sh expects (it carries the oracle->ora19c remap).
# ===========================================================================

class Source:
    #              vendor        setup_key      internal  version  container
    _REGISTRY = {
        "mysql":      ("mysql",      "mysql",      False, None,    "mysql"),
        "sqlserver":  ("sqlserver",  "sqlserver",  False, None,    "sqlserver"),
        "oracle":     ("oracle",     "ora19c",     True,  None,    "ora19c"),
        "oracle23ai": ("oracle23ai", "oracle23ai", True,  None,    "eztest_oracle23ai"),
        "olr":        ("olr",        "olr",        False, "1.8.5", "ora19c"),
        "postgres":   ("postgres",   "postgres",   False, None,    "postgres"),
    }

    def __init__(self, key: str):
        if key not in self._REGISTRY:
            raise ValueError(f"unknown source '{key}'. valid: {list(self._REGISTRY)}")
        self.key = key
        (self.vendor, self.setup_key, self.internal,
         self.version, self.container) = self._REGISTRY[key]

    @property
    def is_olr(self):
        return self.vendor == "olr"


# ===========================================================================
# Target database — what we replicate INTO.  First-class so PG and IvorySQL
# are selectable instead of "whatever initdb is on PATH".
# ===========================================================================

class Target:
    #              flavor        version  pg_compat
    _REGISTRY = {
        "pg16":      ("postgresql", "16", "16"),
        "pg17":      ("postgresql", "17", "17"),
        "pg18":      ("postgresql", "18", "18"),
        "ivorysql3": ("ivorysql",   "3",  "16"),
        "ivorysql4": ("ivorysql",   "4",  "17"),
        "ivorysql5": ("ivorysql",   "5",  "18"),
    }

    def __init__(self, key: str):
        if key not in self._REGISTRY:
            raise ValueError(f"unknown target '{key}'. valid: {list(self._REGISTRY)}")
        self.key = key
        self.flavor, self.version, self.pg_compat = self._REGISTRY[key]
        self.bin_dir = None  # filled in by the `target` fixture

    @property
    def is_ivorysql(self):
        return self.flavor == "ivorysql"

    @property
    def extra_conf(self):
        # IvorySQL defaults to oracle-compatible mode (upper-case identifier
        # folding), which breaks the PG-standard test SQL.  Pin to pg mode.
        if self.is_ivorysql:
            return ["ivorysql.compatible_mode = 'pg'"]
        return []


# Base GUCs every test cluster gets (unchanged from the original pg_instance).
_BASE_GUCS = [
    "synchdb.naptime = 10",
    "synchdb.dbz_batch_size = 16384",
    "synchdb.dbz_queue_size = 32768",
    "synchdb.jvm_max_heap_size = 2048",
    "synchdb.olr_read_buffer_size = 128",
    "log_min_messages = debug1",
]


class TargetInstance:
    """Manages one local target cluster: initdb -> conf -> start -> stop.

    Replaces the monolithic pg_instance fixture body.  Binaries come from an
    explicit bin_dir (so the target is not an ambient PATH property); bin_dir
    is None only when relying on PATH for a local run.
    """

    def __init__(self, target: Target):
        self.target   = target
        self.bin_dir  = target.bin_dir
        self.temp_dir = "synchdb_testdir"
        self.data_dir = TEST_DATA_DIR  # shared with common.update_guc_conf
        self.log_file = os.path.join(self.temp_dir, "logfile")
        self.host     = PG_HOST
        self.port     = PG_PORT

    def _bin(self, name: str) -> str:
        return os.path.join(self.bin_dir, name) if self.bin_dir else name

    def initdb(self):
        if self.bin_dir and not os.path.exists(self._bin("initdb")):
            raise RuntimeError(f"initdb not found under target bin dir: {self.bin_dir}")
        if os.path.isdir(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        subprocess.run([self._bin("initdb"), "-D", self.data_dir],
                       check=True, stdout=subprocess.DEVNULL)

    def write_conf(self, gucs):
        conf_file = os.path.join(self.data_dir, "postgresql.conf")
        with open(conf_file, "a") as f:
            for line in gucs:
                f.write(f"\n{line}\n")

    def start(self):
        subprocess.run(
            [self._bin("pg_ctl"), "-D", self.data_dir,
             "-o", f"-p {self.port}", "-l", self.log_file, "start"],
            check=True,
        )

    def wait_ready(self, timeout=30):
        for _ in range(timeout):
            try:
                psycopg2.connect(host=self.host, dbname="postgres", port=self.port).close()
                return
            except Exception:
                time.sleep(1)
        with open(self.log_file) as f:
            print(f.read())
        raise RuntimeError("PostgreSQL/IvorySQL failed to start")

    def stop(self):
        subprocess.run([self._bin("pg_ctl"), "-D", self.data_dir, "stop", "-m", "immediate"],
                       check=True, stdout=subprocess.DEVNULL)


# ---------------------------------------------------------------------------
# binary / target inference
# ---------------------------------------------------------------------------

def _pg_config(bin_dir, *args) -> str:
    exe = os.path.join(bin_dir, "pg_config") if bin_dir else "pg_config"
    return subprocess.check_output([exe, *args], text=True).strip()


def _infer_target_key(bin_dir) -> str:
    """Derive a Target key from pg_config when --target is not given.

    Detects IvorySQL from `pg_config --version` so a PATH-only local run
    still pins pg-compatible mode.  Falls back to a valid key on any error.
    """
    try:
        version = _pg_config(bin_dir, "--version")
        major   = _pg_config(bin_dir, "--majorversion")
    except Exception:
        return "pg17"
    if "ivorysql" in version.lower():
        return {"16": "ivorysql3", "17": "ivorysql4", "18": "ivorysql5"}.get(major, "ivorysql4")
    key = f"pg{major}"
    return key if key in Target._REGISTRY else "pg17"


# ---------------------------------------------------------------------------
# pytest options
# ---------------------------------------------------------------------------

def pytest_addoption(parser):
    parser.addoption(
        "--source", action="store", default=None,
        help="source DB: mysql, sqlserver, oracle, oracle23ai, olr, postgres",
    )
    parser.addoption(
        "--target", action="store", default=None,
        help="target DB: pg16/pg17/pg18/ivorysql3/ivorysql4/ivorysql5 "
             "(inferred from pg_config if omitted)",
    )
    parser.addoption(
        "--target-bin", action="store", default=None,
        help="bin dir of the target's initdb/pg_ctl (else $SYNCHDB_TARGET_BIN, else PATH)",
    )
    parser.addoption(
        "--dbvendor", action="store", default="mysql",
        help="(deprecated) legacy alias for --source",
    )
    parser.addoption(
        "--tpccmode", action="store", default="serial",
        help="tpcc running mode: serial or parallel",
    )


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def source(pytestconfig) -> Source:
    key = pytestconfig.getoption("source") or pytestconfig.getoption("dbvendor")
    return Source(key)


@pytest.fixture(scope="session")
def dbvendor(source: Source) -> str:
    # the six legacy strings the test files branch on — unchanged contract
    return source.vendor


@pytest.fixture(scope="session")
def tpccmode(pytestconfig) -> str:
    return pytestconfig.getoption("tpccmode")


@pytest.fixture(scope="session")
def target(pytestconfig) -> Target:
    bin_dir = pytestconfig.getoption("target_bin") or os.environ.get("SYNCHDB_TARGET_BIN")
    key = pytestconfig.getoption("target") or _infer_target_key(bin_dir)
    t = Target(key)
    t.bin_dir = bin_dir
    return t


@pytest.fixture(scope="session")
def target_instance(request, target: Target):
    inst = TargetInstance(target)
    inst.initdb()
    inst.write_conf(_BASE_GUCS + target.extra_conf)
    inst.start()
    try:
        inst.wait_ready()
    except RuntimeError:
        raise

    if target.is_ivorysql:
        print("[setup] IvorySQL target: pinned to pg-compatible mode")

    yield inst

    inst.stop()
    if request.session.testsfailed == 0:
        shutil.rmtree(inst.temp_dir)
    else:
        print(f"test failed: cluster kept at {inst.data_dir}, log at {inst.log_file}")


@pytest.fixture(scope="session")
def pg_instance(target_instance):
    """Backward-compatible view of the running target (dict form)."""
    inst = target_instance
    return {
        "host":     inst.host,
        "port":     inst.port,
        "dbname":   "postgres",
        "temp_dir": inst.temp_dir,
        "data_dir": inst.data_dir,
        "log_file": inst.log_file,
        "target":   inst.target,
    }


@pytest.fixture(scope="session")
def pg_cursor(target_instance):
    conn = psycopg2.connect(host=target_instance.host, dbname="postgres",
                            port=target_instance.port)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS synchdb CASCADE;")
    yield cur
    cur.close()
    conn.close()


@pytest.fixture(scope="session", autouse=True)
def setup_remote_instance(source: Source, request):
    # Provisioning stays on the proven ci/setup-remotedbs.sh, keyed by setup_key.
    env = os.environ.copy()
    env["DBTYPE"]   = source.setup_key
    env["WHICH"]    = "n/a"
    env["OLRVER"]   = source.version or ""
    env["INTERNAL"] = "1" if source.internal else "0"

    subprocess.run(["bash", "./ci/setup-remotedbs.sh"],
                   check=True, env=env, stdout=subprocess.DEVNULL)
    yield
    _teardown_remote_instance(source.setup_key)


@pytest.fixture(scope="session")
def hammerdb(source: Source):
    env = os.environ.copy()
    env["DBTYPE"]   = "hammerdb"
    env["WHICH"]    = source.vendor       # original dbvendor; setup_hammerdb maps olr->ora19c
    env["INTERNAL"] = "0"

    container = source.container          # ora19c for oracle/olr

    subprocess.run(["bash", "./ci/setup-remotedbs.sh"], check=True, env=env, stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "network", "create", "tpccnet"], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "network", "connect", "tpccnet", container], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "network", "connect", "tpccnet", "hammerdb"], check=True, stdout=subprocess.DEVNULL)

    yield

    subprocess.run(["docker", "network", "disconnect", "tpccnet", container], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "network", "disconnect", "tpccnet", "hammerdb"], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "network", "rm", "tpccnet"], check=True, stdout=subprocess.DEVNULL)
    _teardown_remote_instance("hammerdb")


def _teardown_remote_instance(setup_key: str):
    env = os.environ.copy()
    env["DBTYPE"] = setup_key
    subprocess.run(["bash", "./ci/teardown-remotedbs.sh"],
                   check=True, env=env, stdout=subprocess.DEVNULL)
