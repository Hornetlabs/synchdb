import os
import subprocess
import socket
import ipaddress
import time
from datetime import datetime

def get_container_ip(name: str, network: str = "synchdbnet") -> str | None:
    # Go template with the specific network:
    tmpl = f'{{{{(index .NetworkSettings.Networks "{network}").IPAddress}}}}'
    try:
        proc = subprocess.run(
            ["docker", "inspect", "--format", tmpl, name],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except FileNotFoundError:
        raise RuntimeError("docker is not installed or not on PATH")
    except subprocess.TimeoutExpired:
        raise RuntimeError("docker inspect timed out")

    if proc.returncode != 0:
        err = (proc.stderr or "").strip()
        if "No such object" in err:
            return None

    ip = proc.stdout.strip()
    try:
        ipaddress.ip_address(ip)
    except ValueError:
        return None
    return ip


# Canonical test cluster data dir.  Shared by conftest's TargetInstance (which
# runs initdb/pg_ctl here) and update_guc_conf() below (which appends GUCs to
# its postgresql.conf), so the two never drift apart.
TEST_DATA_DIR = os.path.join("synchdb_testdir", "data")


def resolve_host(container: str, max_tries: int = 20) -> str:
    """Resolve a container's synchdbnet IP lazily (NOT at import time).

    Replaces the old module-level get_container_ip() calls that ran
    `docker inspect` on every import regardless of which vendor was tested.
    """
    for _ in range(max_tries):
        ip = get_container_ip(name=container)
        if ip:
            return ip
        time.sleep(1)
    raise RuntimeError(f"could not resolve IP for container '{container}'")


MYSQL_HOST="127.0.0.1"
MYSQL_PORT=3306
MYSQL_USER="mysqluser"
MYSQL_PASS="mysqlpwd"
MYSQL_DB="inventory"

SQLSERVER_HOST="127.0.0.1"
SQLSERVER_PORT=1433
SQLSERVER_USER="sa"
SQLSERVER_PASS="Password!"
SQLSERVER_DB="testDB"
SQLSERVER_SCHEMA="dbo"

ORACLE_HOST=None  # resolved lazily via resolve_host("ora19c")
ORACLE_PORT=1521
ORACLE_USER="DBZUSER"
ORACLE_PASS="dbz"
ORACLE_DB="FREE"
ORACLE_SCHEMA="DBZUSER"

# ora19c and olr are put to a dedicated docker network called synchdb to test
# so we need to resolve their container IPs (lazily, on first use)
ORA19C_HOST=None
ORA19C_PORT=1521
ORA19C_USER="DBZUSER"
ORA19C_PASS="dbz"
ORA19C_DB="FREE"
ORA19C_SCHEMA="DBZUSER"

ORACLE23AI_HOST=None  # resolved lazily via resolve_host("eztest_oracle23ai")
ORACLE23AI_PORT=1521
ORACLE23AI_USER="DBZUSER"
ORACLE23AI_PASS="dbz"
ORACLE23AI_DB="FREEPDB1"
ORACLE23AI_SCHEMA="DBZUSER"
ORACLE23AI_CDB="FREE"
ORACLE23AI_COMMON_USER="c##dbzuser"
ORACLE23AI_COMMON_PASS="dbz"

OLR_HOST=None  # resolved lazily via resolve_host("OpenLogReplicator")
OLR_PORT="7070"
OLR_SERVICE="ORACLE"

POSTGRES_HOST="127.0.0.1"
POSTGRES_PORT=5432
POSTGRES_USER="postgres"
POSTGRES_PASS="pass"
POSTGRES_DB="postgres"
POSTGRES_SCHEMA="public"

# Connector rows are deleted before the legacy test cleanup call. Remember only
# the native generations owned by that connector, never scan source name prefixes.
_postgres_generations = {}

def getConnectorName(dbvendor):
    if dbvendor == "mysql":
        return "mysqlconn"
    elif dbvendor == "sqlserver":
        return "sqlserverconn"
    elif dbvendor in ("oracle", "oracle23ai"):
        return "oracleconn"
    elif dbvendor == "postgres":
        return "postgresconn"
    else:
        return "olrconn"

def getDbname(dbvendor):
    if dbvendor == "mysql":
        return MYSQL_DB
    elif dbvendor == "sqlserver":
        return SQLSERVER_DB
    elif dbvendor == "oracle":
        return ORACLE_DB
    elif dbvendor == "oracle23ai":
        return ORACLE23AI_DB
    elif dbvendor == "postgres":
        return POSTGRES_DB
    else:
        return ORA19C_DB

def getSchema(dbvendor):
    # returns the remote schema name in example databases
    if dbvendor == "mysql":
        return None
    elif dbvendor == "sqlserver":
        return SQLSERVER_SCHEMA
    elif dbvendor == "oracle":
        return ORACLE_SCHEMA
    elif dbvendor == "oracle23ai":
        return ORACLE23AI_SCHEMA
    elif dbvendor == "postgres":
        return POSTGRES_SCHEMA
    else:
        return ORA19C_SCHEMA

def run_pg_query(cursor, query):
    # print(f"[{datetime.now().strftime('%H:%M:%S')}][run_pg_query] {query}")  # Debug: print the query being executed
    cursor.execute(query)
    if cursor.description:  # Only fetch if query returns results
        return cursor.fetchall()
    return None

def run_pg_query_one(cursor, query):
    # print(f"[{datetime.now().strftime('%H:%M:%S')}][run_pg_query_one] {query}")  # Debug: print the query being executed
    cursor.execute(query)
    if cursor.description:
        return cursor.fetchone()
    return None

## default data type matrix for all supported database vendors
## todo: better ways to lookup
def verify_default_type_mappings(exttype, pgtype, dbvendor):

    # The refactored schema decoder preserves Debezium's source type spelling.
    exttype = exttype.lower()
    pgtype = pgtype.lower()
    if dbvendor == "olr":
        # The Oracle raw parser canonicalizes FLOAT(p) to a qualified PG type.
        exttype = {"pg_catalog.float4": "real",
                   "pg_catalog.float8": "double precision"}.get(exttype, exttype)
    if exttype == pgtype:
        return True
    
    default_mappings = {
        "int": "integer",
        "int identity": "integer",
        "varchar": "character varying",
        "enum": "text",
        "geometry": "text",
        "float": "real",
        "double": "double precision",
        "number": "numeric",
        "decimal": "numeric",
        "binary": "bytea",
        "smallint": "smallint",
        "tinyint": "smallint",
        "char": "character",
        "nchar": "character",
        "ntext": "text",
        "image": "bytea",
        "geography": "text",
        "nvarchar": "character varying",
        "xml": "text",
        "uniqueidentifier": "uuid",
        "binary_double": "double precision",
        "binary_float": "real",
        "long": "text",
        "interval day to second": "interval",
        "interval year to month": "interval",
        "timestamp with time zone": "timestamp with time zone",
        "timestamp with local time zone": "timestamp with time zone",
        "nvarchar2": "character varying",
        "varchar2": "character varying",
        "raw": "bytea",
        "bfile": "text",
        "clob": "text",
        "nclob": "text",
        "rowid": "text",
        "urowid": "text",
        "decimal unsigned": "numeric",
        "double unsigned": "double precision",
        "float unsigned": "real",
        "int unsigned": "bigint",
        "mediumint": "integer",
        "mediumint unsigned": "integer",
        "year": "integer",
        "smallint unsigned": "integer",
        "tinyint unsigned": "smallint",
        "varbinary": "bytea",
        "blob": "bytea",
        "longblob": "bytea",
        "tinyblob": "bytea",
        "mediumtext": "text",
        "tinytext": "text",
        "longtext": "text",
        "json": "jsonb",
        "bit": "boolean",
        "smallmoney": "money",
    }
    multi_target_mappings = {
        "date": {"timestamp without time zone", "timestamp with time zone"},
        "time": {"time without time zone", "time with time zone"},
        "datetime": {"timestamp without time zone", "timestamp with time zone"},
        "datetime2": {"timestamp without time zone", "timestamp with time zone"},
        "datetimeoffset": {"timestamp without time zone", "timestamp with time zone"},
        "smalldatetime": {"timestamp without time zone", "timestamp with time zone"},
        "timestamp": {"timestamp without time zone","timestamp with time zone"}
    }
    if exttype in default_mappings and pgtype == default_mappings[exttype]:
        return True
    
    if exttype in multi_target_mappings and pgtype in multi_target_mappings[exttype]:
        return True

    print(f"type unmatched: {exttype} : {pgtype} for {dbvendor}")
    return False

def run_remote_query(where, query, srcdb=None):
    db = srcdb or {
        "mysql": MYSQL_DB,
        "sqlserver": SQLSERVER_DB,
        "oracle": ORACLE_DB,
        "oracle23ai": ORACLE23AI_DB,
        "olr": ORA19C_DB,
        "postgres": POSTGRES_DB
    }[where]

    # print(f"[{datetime.now().strftime('%H:%M:%S')}][run_remote_query] Running on {db}: {query}")  # Debug: print the query being executed

    try:
        if where == "mysql":
            result = subprocess.check_output(["docker", "exec", "-i", "mysql", "mysql", f"-u{MYSQL_USER}", f"-p{MYSQL_PASS}", "-D", f"{db}", "-sN", "-e", f"{query}"], text=True , env={"LC_ALL": "C"}).strip()
            rows = []
            for line in result.splitlines():
                cols = line.strip().split("\t")
                rows.append(tuple(cols))
        
        elif where == "sqlserver":
            result = subprocess.check_output(["docker", "exec", "-i", "sqlserver", "/opt/mssql-tools18/bin/sqlcmd", f"-U{SQLSERVER_USER}", f"-P{SQLSERVER_PASS}", "-d", f"{db}", "-C", "-h", "-1", "-W", "-s", "\t", "-Q", f"{query}"], text=True).strip()
            
            rows = []
            for line in result.splitlines():
                line = line.strip()
                if not line or "rows affected" in line.lower():
                    continue  # skip empty lines and metadata
                cols = line.split("\t")
                rows.append(tuple(cols))
        elif where == "postgres":
            result = subprocess.check_output(["docker", "exec", "-i", "postgres", "psql", "-U", f"{POSTGRES_USER}", "-d", f"{POSTGRES_DB}", "-tA", "-c", f"{query}"], text=True , env={"LC_ALL": "C"}).strip()
            rows = []
            for line in result.splitlines():
                cols = line.split("|")
                rows.append(tuple(cols))
        else:
            sql = f"""
            WHENEVER OSERROR EXIT FAILURE;
            WHENEVER SQLERROR EXIT SQL.SQLCODE;
            SET HEADING OFF;
            SET FEEDBACK OFF;
            SET PAGESIZE 0;
            SET COLSEP '|';
            SET LINESIZE 32767;
            SET TRIMOUT ON;
            SET TRIMSPOOL ON;
            {query};
            exit
            """
            if where == "oracle":
                host = resolve_host("ora19c")
                result = subprocess.check_output(["docker", "exec", "-i", "ora19c", "sqlplus", "-S", f"{ORACLE_USER}/{ORACLE_PASS}@//{host}:{ORACLE_PORT}/{db}"], input=sql, text=True).strip()
            elif where == "oracle23ai":
                host = resolve_host("eztest_oracle23ai")
                result = subprocess.check_output(["docker", "exec", "-i", "eztest_oracle23ai", "sqlplus", "-S", f"{ORACLE23AI_USER}/{ORACLE23AI_PASS}@//{host}:{ORACLE23AI_PORT}/{db}"], input=sql, text=True).strip()
            else:
                host = resolve_host("ora19c")
                result = subprocess.check_output(["docker", "exec", "-i", "ora19c", "sqlplus", "-S", f"{ORA19C_USER}/{ORA19C_PASS}@//{host}:{ORA19C_PORT}/{db}"], input=sql, text=True).strip()
                
            rows = []
            for line in result.splitlines():
                line = line.strip()
                if not line:
                    continue  # skip empty lines
                rows.append(tuple(col.strip() for col in line.split('|')))

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed with return code {e.returncode}")
        print(f"[ERROR] Output:\n{e.output}")
        raise

    except Exception as ex:
        print(f"[ERROR] Unexpected error: {ex}")
        raise
    
    return rows

def create_synchdb_connector(cursor, vendor, name, srcdb=None, srcschema=None, srctable=None, snapshot_table=None):
    db = srcdb or {
        "mysql": MYSQL_DB,
        "sqlserver": SQLSERVER_DB,
        "oracle": ORACLE_DB,
        "oracle23ai": ORACLE23AI_DB,
        "olr": ORA19C_DB,
        "postgres": POSTGRES_DB
    }[vendor]
    
    schema = srcschema or {
        "mysql": "null",
        "sqlserver": SQLSERVER_SCHEMA,
        "oracle": ORACLE_SCHEMA,
        "oracle23ai": ORACLE23AI_SCHEMA,
        "olr": ORA19C_SCHEMA,
        "postgres": POSTGRES_SCHEMA
    }[vendor]

    table = srctable or "null"
    snapshot_table = table if snapshot_table is None else snapshot_table

    if vendor == "mysql":
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{MYSQL_HOST}', {MYSQL_PORT}, '{MYSQL_USER}', '{MYSQL_PASS}', '{db}', '{schema}', '{table}', '{snapshot_table}', 'mysql');")

    elif vendor == "sqlserver":
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{SQLSERVER_HOST}', {SQLSERVER_PORT}, '{SQLSERVER_USER}', '{SQLSERVER_PASS}', '{db}', '{schema}', '{table}', '{snapshot_table}', 'sqlserver');")

    elif vendor == "oracle":
        host = resolve_host("ora19c")
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{host}', {ORACLE_PORT}, '{ORACLE_USER}', '{ORACLE_PASS}', '{db}', '{schema}', '{table}', '{snapshot_table}', 'oracle');")
    elif vendor == "oracle23ai":
        host = resolve_host("eztest_oracle23ai")
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{host}', {ORACLE23AI_PORT}, '{ORACLE23AI_COMMON_USER}', '{ORACLE23AI_COMMON_PASS}', '{ORACLE23AI_CDB}/{db}', '{schema}', '{table}', '{snapshot_table}', 'oracle');")
    elif vendor == "postgres":
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{POSTGRES_HOST}', {POSTGRES_PORT}, '{POSTGRES_USER}', '{POSTGRES_PASS}', '{db}', '{schema}', '{table}', '{snapshot_table}', 'postgres');")

    else:
        ora_host = resolve_host("ora19c")
        olr_host = resolve_host("OpenLogReplicator")
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{ora_host}', {ORA19C_PORT}, '{ORA19C_USER}', '{ORA19C_PASS}', '{db}', '{schema}', '{table}', '{snapshot_table}', 'olr');")
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_olr_conninfo('{name}','{olr_host}', {OLR_PORT}, '{OLR_SERVICE}');")

    return result

def wait_for_operation(cursor, operation_id, timeout=300, expected_status="SUCCEEDED"):
    """Check the committed control receipt, including asynchronous failures."""
    assert operation_id > 0, f"invalid control operation ID: {operation_id}"
    assert cursor.connection.autocommit, "commit control requests before waiting"
    deadline = time.monotonic() + timeout
    while True:
        cursor.execute("SELECT synchdb_operation(%s, 1000)", (operation_id,))
        result = cursor.fetchone()[0]
        if result["status"] not in ("PENDING", "EXECUTING"):
            if result["status"] != expected_status and result["operation"] == "START":
                cursor.execute("""
                    SELECT result->'tables' FROM synchdb_bootstrap_runs
                    WHERE connector_id = %s AND finished_at >= %s::timestamptz
                    ORDER BY finished_at DESC LIMIT 1
                """, (result["connector_id"], result["started_at"]))
                bootstrap = cursor.fetchone()
                if bootstrap is not None:
                    result["bootstrap_tables"] = bootstrap[0]
            assert result["status"] == expected_status, f"unexpected control result: {result}"
            return result
        assert time.monotonic() < deadline, f"control operation timed out: {result}"


def get_bootstrap_result(cursor, name):
    """Read the latest committed FDW outcome for this connector identity."""
    cursor.execute("""
        SELECT r.result FROM synchdb_bootstrap_runs r
        JOIN synchdb_conninfo c USING (connector_id)
        WHERE c.name = %s ORDER BY r.finished_at DESC LIMIT 1
    """, (name,))
    row = cursor.fetchone()
    assert row is not None, f"no committed bootstrap result for {name}"
    return row[0]


def wait_for_snapshot_only(cursor, name, timeout=120):
    """Require a clean, durable terminal state for a native snapshot-only run."""
    deadline = time.monotonic() + timeout
    while True:
        cursor.execute("""
            SELECT actual_state, last_sqlstate, last_error
            FROM synchdb_control_view WHERE name = %s
        """, (name,))
        state = cursor.fetchone()
        assert state is not None, f"missing control state for {name}"
        assert state[0] not in ("FAILED", "RETRY_WAIT"), state
        if state[0] == "STOPPED":
            assert state[1] in (None, "00000"), state
            return state
        assert time.monotonic() < deadline, f"snapshot-only run did not finish: {state}"
        time.sleep(0.1)


def wait_for_native_snapshot(cursor, name, mode="initial", timeout=1200):
    """Wait for applied fixture rows, no_data metadata, or schemasync pause."""
    deadline = time.monotonic() + timeout
    while True:
        cursor.execute("""
            SELECT c.actual_state, c.last_sqlstate, c.last_error,
                   s.snapshot_end_ts,
                   EXISTS (SELECT 1 FROM synchdb_att_view a WHERE a.name = c.name)
            FROM synchdb_control_view c
            LEFT JOIN synchdb_snapstats s USING (name)
            WHERE c.name = %s
        """, (name,))
        state = cursor.fetchone()
        assert state is not None, f"missing control state for {name}"
        assert state[0] not in ("FAILED", "RETRY_WAIT", "STOPPED"), state
        assert state[1] in (None, "00000"), state
        if mode == "schemasync":
            ready = state[0] == "PAUSED" and state[4]
        elif mode == "no_data":
            ready = state[0] == "RUNNING" and state[4]
        else:
            ready = state[3] is not None and state[3] > 0
        if ready and name.startswith("olr"):
            cursor.execute("""
                SELECT p.snapshot_scn > 0 FROM synchdb_olr_positions p
                JOIN synchdb_artifacts a USING (database_oid,connector_id,generation)
                JOIN synchdb_conninfo c USING (connector_id)
                WHERE c.name=%s AND a.active
            """, (name,))
            handoff = cursor.fetchone()
            ready = handoff is not None and handoff[0]
        if ready:
            return state
        assert time.monotonic() < deadline, f"native snapshot did not finish: {name}: {state}"
        time.sleep(1)


def create_and_start_synchdb_connector(cursor, vendor, name, mode, srcdb=None, srctable=None, snapshot_table=None):
    create_synchdb_connector(cursor, vendor, name, srcdb, srctable=srctable, snapshot_table=snapshot_table)
    cursor.execute("SELECT synchdb_start_engine_bgw(%s, %s)", (name, mode))
    operation_id = cursor.fetchone()[0]
    wait_for_operation(cursor, operation_id)
    if vendor in ("oracle", "oracle23ai", "olr") and mode in ("initial", "always", "no_data", "schemasync"):
        cursor.execute("SHOW synchdb.snapshot_engine")
        if cursor.fetchone()[0] == "debezium":
            wait_for_native_snapshot(cursor, name, mode=mode)
    return operation_id

def stop_and_delete_synchdb_connector(cursor, name):
    cursor.execute("SELECT 1 FROM synchdb_conninfo WHERE name = %s", (name,))
    if cursor.fetchone() is not None:
        cursor.execute("SELECT synchdb_stop_engine_bgw(%s)", (name,))
        wait_for_operation(cursor, cursor.fetchone()[0])
        cursor.execute("""
            SELECT a.generation::text
              FROM synchdb_artifacts a
              JOIN synchdb_conninfo c USING (connector_id)
             WHERE c.name = %s AND c.data->>'connector' = 'postgres'
               AND a.origin = 'NATIVE'
        """, (name,))
        _postgres_generations[name] = [row[0] for row in cursor.fetchall()]
    row = run_pg_query_one(cursor, f"SELECT synchdb_del_conninfo('{name}')")
    return row[0]

def drop_default_pg_schema(cursor, vendor):
    if vendor == "mysql":
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS inventory CASCADE")
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS \"INVENTORY\" CASCADE")
    elif vendor == "sqlserver":
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS testdb CASCADE")
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS \"TESTDB\" CASCADE")
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS \"testDB\" CASCADE")
    elif vendor == "postgres":
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS postgres CASCADE")
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS \"POSTGRES\" CASCADE")
    elif vendor == "oracle23ai":
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS freepdb1 CASCADE")
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS \"FREEPDB1\" CASCADE")
    else:
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS free CASCADE")
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS \"FREE\" CASCADE")

def update_guc_conf(cursor, key, val, reload_conf=False):
    # TEST_DATA_DIR is the same dir conftest's TargetInstance ran initdb in,
    # so this appends to the live cluster's postgresql.conf.
    conf_file = os.path.join(TEST_DATA_DIR, "postgresql.conf")

    # Append parameter
    with open(conf_file, "a") as f:
        f.write(f"\n{key} = {val}\n")

    # Apply changes if requested
    if reload_conf:
        cursor.execute("SELECT pg_conf_load_time()")
        loaded_at = cursor.fetchone()[0]
        cursor.execute("SELECT pg_reload_conf()")
        deadline = time.monotonic() + 10
        while True:
            cursor.execute("SELECT pg_conf_load_time() > %s", (loaded_at,))
            if cursor.fetchone()[0]:
                break
            assert time.monotonic() < deadline, f"configuration reload timed out: {key}"
            time.sleep(0.05)

def drop_repslot_and_pub(dbvendor, name, dstdb):
    if dbvendor != "postgres":
        return

    for generation in _postgres_generations.pop(name, []):
        prefix = "synchdb_" + generation.replace("-", "")
        run_remote_query(dbvendor,
                         f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                         f"WHERE slot_name = '{prefix}_slot'")
        run_remote_query(dbvendor, f"DROP PUBLICATION IF EXISTS {prefix}_pub")


def restart_remote_db(dbvendor, wait_time=30):
    """
    TODO: Upgrade Debezium, and remove this workaround.
    BUG WORKAROUND: Restart the remote database container.

    Restart the remote database container.
    This is mainly used for oracle23ai, because it seems
    to have some stability issue after running for a while,
    and restart can help recover it.
    """
    if dbvendor == "mysql":
        subprocess.run(["docker", "restart", "mysql"], check=True)
    elif dbvendor == "sqlserver":
        subprocess.run(["docker", "restart", "sqlserver"], check=True)
    elif dbvendor == "oracle":
        subprocess.run(["docker", "restart", "ora19c"], check=True)
    elif dbvendor == "oracle23ai":
        subprocess.run(["docker", "restart", "eztest_oracle23ai"], check=True)
    elif dbvendor == "olr":
        subprocess.run(["docker", "restart", "OpenLogReplicator"], check=True)
    else:
        print(f"restart not supported for {dbvendor}")
        return

    if wait_time > 0:
        print(f"waiting {wait_time} seconds for {dbvendor} to restart...")
        time.sleep(wait_time)
