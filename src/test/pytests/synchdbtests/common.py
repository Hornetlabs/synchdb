
import subprocess
import socket
import time

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
        # e.g. "Error: No such object: <name>"
        err = (proc.stderr or "").strip()
        if "No such object" in err:
            # print(f"[ERROR] cannot get container IP for {name}")
            return None
        raise RuntimeError(f"docker inspect failed: {err}")

    ip = proc.stdout.strip()
    return ip or None  # None if not attached to that network

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

ORACLE_HOST="127.0.0.1"
ORACLE_PORT=1521
ORACLE_USER="c##dbzuser"
ORACLE_PASS="dbz"
ORACLE_DB="FREE"

# ora19c and olr are put to a dedicated docker network called synchdb to test
# so we need to resolve their container UPs
ORA19C_HOST=get_container_ip(name="ora19c")
ORA19C_PORT=1521
ORA19C_USER="DBZUSER"
ORA19C_PASS="dbz"
ORA19C_DB="FREE"

OLR_HOST=get_container_ip(name="OpenLogReplicator")
OLR_PORT="7070"
OLR_SERVICE="ORACLE"

def getConnectorName(dbvendor):
    if dbvendor == "mysql":
        return "mysqlconn"
    elif dbvendor == "sqlserver":
        return "sqlserverconn"
    elif dbvendor == "oracle":
        return "oracleconn"
    else:
        return "olrconn"

def getDbname(dbvendor):
    if dbvendor == "mysql":
        return MYSQL_DB
    elif dbvendor == "sqlserver":
        return SQLSERVER_DB
    elif dbvendor == "oracle":
        return ORACLE_DB
    else:
        return ORA19C_DB

def getSchema(dbvendor):
    # returns the remote schema name in example databases
    if dbvendor == "mysql":
        return None
    elif dbvendor == "sqlserver":
        return "dbo"
    elif dbvendor == "oracle":
        return "c##dbzuser"
    else:
        return "DBZUSER"

def run_pg_query(cursor, query):
    cursor.execute(query)
    if cursor.description:  # Only fetch if query returns results
        return cursor.fetchall()
    return None

def run_pg_query_one(cursor, query):
    cursor.execute(query)
    if cursor.description:
        return cursor.fetchone()
    return None

## default data type matrix for all supported database vendors
## todo: better ways to lookup
def verify_default_type_mappings(exttype, pgtype, dbvendor):

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
        "olr": ORA19C_DB
    }[where]

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
        else:
            sql = f"""
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
                result = subprocess.check_output(["docker", "exec", "-i", "oracle", "sqlplus", "-S", f"{ORACLE_USER}/{ORACLE_PASS}@//{ORACLE_HOST}:{ORACLE_PORT}/{db}"], input=sql, text=True).strip()
            else:
                global ORA19C_HOST
                max_tries = 20
                tries = 0

                while ORA19C_HOST is None and tries < max_tries:
                    ORA19C_HOST = get_container_ip(name="ora19c")
                    tries += 1
                    time.sleep(1)

                result = subprocess.check_output(["docker", "exec", "-i", "ora19c", "sqlplus", "-S", f"{ORA19C_USER}/{ORA19C_PASS}@//{ORA19C_HOST}:{ORA19C_PORT}/{db}"], input=sql, text=True).strip()
                
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

def create_synchdb_connector(cursor, vendor, name, srcdb=None):
    db = srcdb or {
        "mysql": MYSQL_DB,
        "sqlserver": SQLSERVER_DB,
        "oracle": ORACLE_DB,
        "olr": ORA19C_DB
    }[vendor]

    if vendor == "mysql":
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{MYSQL_HOST}', {MYSQL_PORT}, '{MYSQL_USER}', '{MYSQL_PASS}', '{db}', 'postgres', 'null', 'null', 'mysql');")

    elif vendor == "sqlserver":
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{SQLSERVER_HOST}', {SQLSERVER_PORT}, '{SQLSERVER_USER}', '{SQLSERVER_PASS}', '{db}', 'postgres', 'null', 'null', 'sqlserver');")

    elif vendor == "oracle":
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{ORACLE_HOST}', {ORACLE_PORT}, '{ORACLE_USER}', '{ORACLE_PASS}', '{db}', 'postgres', 'null', 'null', 'oracle');")
    else:
        global ORA19C_HOST
        global OLR_HOST
        max_tries = 20
        tries = 0

        while ORA19C_HOST is None and tries < max_tries:
            ORA19C_HOST = get_container_ip(name="ora19c")
            tries += 1
            time.sleep(1)
        
        tries = 0
        while OLR_HOST is None and tries < max_tries:
            OLR_HOST = get_container_ip(name="OpenLogReplicator")
            tries += 1
            time.sleep(1)
        
        assert ORA19C_HOST != None and OLR_HOST != None
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{ORA19C_HOST}', {ORA19C_PORT}, '{ORA19C_USER}', '{ORA19C_PASS}', '{db}', 'postgres', 'null', 'null', 'olr');")
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_olr_conninfo('{name}','{OLR_HOST}', {OLR_PORT}, '{OLR_SERVICE}');")

    return result

def create_and_start_synchdb_connector(cursor, vendor, name, mode, srcdb=None):
    create_synchdb_connector(cursor, vendor, name, srcdb)
    row = run_pg_query_one(cursor, f"SELECT synchdb_start_engine_bgw('{name}', '{mode}')")
    return row[0]

def stop_and_delete_synchdb_connector(cursor, name):
    # it shuts down before delete
    row = run_pg_query_one(cursor, f"SELECT synchdb_del_conninfo('{name}')")
    return row[0]

def drop_default_pg_schema(cursor, vendor):
    if vendor == "mysql":
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS inventory CASCADE")
    elif vendor == "sqlserver":
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS testdb CASCADE")
    else:
        row = run_pg_query_one(cursor, f"DROP SCHEMA IF EXISTS free CASCADE")
