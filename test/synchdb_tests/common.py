
import subprocess
import socket

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


def getConnectorName(dbvendor):
    if dbvendor == "mysql":
        return "mysqlconn"
    elif dbvendor == "sqlserver":
        return "sqlserverconn"
    else:
        return "oracleconn"

def getDbname(dbvendor):
    if dbvendor == "mysql":
        return MYSQL_DB
    elif dbvendor == "sqlserver":
        return SQLSERVER_DB
    else:
        return ORACLE_DB

def getSchema(dbvendor):
    # returns the remote schema name in example databases
    if dbvendor == "mysql":
        return None
    elif dbvendor == "sqlserver":
        return "dbo"
    else:
        return "c##dbzuser"

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

def run_remote_query(where, query):
    
    try:
        if where == "mysql":
            result = subprocess.check_output(["docker", "exec", "-i", "mysql", "mysql", f"-u{MYSQL_USER}", f"-p{MYSQL_PASS}", "-D", f"{MYSQL_DB}", "-sN", "-e", f"{query}"], text=True , env={"LC_ALL": "C"}).strip()
            rows = []
            for line in result.splitlines():
                cols = line.strip().split("\t")
                rows.append(tuple(cols))
        
        elif where == "sqlserver":
            container_id = subprocess.check_output(
            "docker ps | grep sqlserver | awk '{print $1}'",
            shell=True, text=True
            ).strip()

            if not container_id:
                raise RuntimeError("No running SQL Server container found.")

            result = subprocess.check_output(["docker", "exec", "-i", f"{container_id}", "/opt/mssql-tools18/bin/sqlcmd", f"-U{SQLSERVER_USER}", f"-P{SQLSERVER_PASS}", "-d", f"{SQLSERVER_DB}", "-C", "-h", "-1", "-W", "-s", "\t", "-Q", f"{query}"], text=True).strip()
            
            rows = []
            for line in result.splitlines():
                line = line.strip()
                if not line or "rows affected" in line.lower():
                    continue  # skip empty lines and metadata
                cols = line.split("\t")
                rows.append(tuple(cols))
        else:
            container_id = subprocess.check_output(
            "docker ps | grep oracle | awk '{print $1}'",
            shell=True, text=True
            ).strip()

            if not container_id:
                raise RuntimeError("No running SQL Server container found.")
            
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
            
            result = subprocess.check_output(["docker", "exec", "-i", f"{container_id}", "sqlplus", "-S", f"{ORACLE_USER}/{ORACLE_PASS}@//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_DB}"], input=sql, text=True).strip()

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

def run_remote_query_one(where, query):

    print(f"send query to {where}")
    try:
        if where == "mysql":
            container_id = subprocess.check_output(
            "docker ps | grep mysql | awk '{print $1}'",
            shell=True, text=True
            ).strip()

            if not container_id:
                raise RuntimeError("No running MySQL container found.")

            result = subprocess.check_output(["docker", "exec", "-i", "mysql", "mysql", f"-u{MYSQL_USER}", f"-p{MYSQL_PASS}", "-D", f"{MYSQL_DB}", "-sN", "-e", f"{query}"], text=True).strip()
            cleanres = result.strip().replace("\r", "").replace("\n", "\t")
            fields = cleanres.split("\t")

        elif where == "sqlserver":
            container_id = subprocess.check_output(
            "docker ps | grep sqlserver | awk '{print $1}'",
            shell=True, text=True
            ).strip()

            if not container_id:
                raise RuntimeError("No running SQL Server container found.")

            result = subprocess.check_output(["docker", "exec", "-i", f"{container_id}", "/opt/mssql-tools18/bin/sqlcmd", f"-U{SQLSERVER_USER}", f"-P{SQLSERVER_PASS}", "-d", f"{SQLSERVER_DB}", "-C", "-h", "-1", "-W", "-s", "\t", "-Q", f"{query}"], text=True).strip()
            cleanres = "".join(line for line in result.splitlines()if "rows affected" not in line.lower())
            fields = cleanres.strip().split("\t")

        else:
            container_id = subprocess.check_output(
            "docker ps | grep oracle | awk '{print $1}'",
            shell=True, text=True
            ).strip()

            if not container_id:
                raise RuntimeError("No running Oracle container found.")
            
            sql = f"""
            SET HEADING OFF;
            SET FEEDBACK OFF;
            SET PAGESIZE 0;
            {query};
            exit
            """
            
            result = subprocess.check_output(["docker", "exec", "-i", f"{container_id}", "sqlplus", "-S", f"{ORACLE_USER}/{ORACLE_PASS}@//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_DB}"], input=sql, text=True).strip()
            
            cleanres=result
            fields = cleanres.split()

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed with return code {e.returncode}")
        print(f"[ERROR] Output:\n{e.output}")
        raise

    except Exception as ex:
        print(f"[ERROR] Unexpected error: {ex}")
        raise

    return tuple(fields)

def create_synchdb_connector(cursor, vendor, name):
    if vendor == "mysql":
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{MYSQL_HOST}', {MYSQL_PORT}, '{MYSQL_USER}', '{MYSQL_PASS}', '{MYSQL_DB}', 'postgres', 'null', 'mysql');")

    elif vendor == "sqlserver":
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{SQLSERVER_HOST}', {SQLSERVER_PORT}, '{SQLSERVER_USER}', '{SQLSERVER_PASS}', '{SQLSERVER_DB}', 'postgres', 'null', 'sqlserver');")

    else:
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{ORACLE_HOST}', {ORACLE_PORT}, '{ORACLE_USER}', '{ORACLE_PASS}', '{ORACLE_DB}', 'postgres', 'null', 'oracle');")

    return result

def create_and_start_synchdb_connector(cursor, vendor, name, mode):
    create_synchdb_connector(cursor, vendor, name)
    row = run_pg_query_one(cursor, f"SELECT synchdb_start_engine_bgw('{name}', '{mode}')")
    return row[0]

def stop_and_delete_synchdb_connector(cursor, name):
    # it shuts down before delete
    row = run_pg_query_one(cursor, f"SELECT synchdb_del_conninfo('{name}')")
    return row[0]

def drop_default_pg_schema(cursor, vendor):
    if vendor == "mysql":
        row = run_pg_query_one(cursor, f"DROP SCHEMA inventory CASCADE")
    elif vendor == "sqlserver":
        row = run_pg_query_one(cursor, f"DROP SCHEMA testdb CASCADE")
    else:
        row = run_pg_query_one(cursor, f"DROP SCHEMA free CASCADE")

