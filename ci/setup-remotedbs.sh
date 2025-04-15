#!/bin/bash

# make bash behave
set -euo pipefail
IFS=$'\n\t'

# read pg major version, error if not provided
DBTYPE=${DBTYPE:?please provide database type}

# get codename from release file
. /etc/os-release
codename=${VERSION#*(}
codename=${codename%)*}

# we'll do everything with absolute paths
basedir="$(pwd)"

function setup_mysql()
{
	echo "setting up mysql..."
	docker-compose -f testenv/mysql/synchdb-mysql-test.yaml up -d
	echo "sleep to give container time to startup..."
	sleep 30  # Give containers time to fully start
	docker exec -i mysql mysql -uroot -pmysqlpwdroot -e "SELECT VERSION();" || { echo "Failed to connect to MySQL"; docker logs mysql; exit 1; }
	docker exec -i mysql mysql -uroot -pmysqlpwdroot << EOF
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mysqluser'@'%';
GRANT replication client ON *.* TO 'mysqluser'@'%';
GRANT replication slave ON *.* TO 'mysqluser'@'%';
GRANT RELOAD ON *.* TO 'mysqluser'@'%';
FLUSH PRIVILEGES;
EOF
	exit 0
}

function setup_sqlserver()
{

	echo "setting up sqlserver..."
	docker-compose -f testenv/sqlserver/synchdb-sqlserver-test.yaml up -d
	echo "sleep to give container time to startup..."
	sleep 30  # Give containers time to fully start
	id=$(docker ps | grep sqlserver | awk '{print $1}')
	docker cp ./testenv/sqlserver/inventory.sql $id:/
	docker exec -i $id /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -C -Q "SELECT @@VERSION" > /dev/null
	docker exec -i $id /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -C -i /inventory.sql > /dev/null
	exit 0
}

function setup_oracle()
{
	echo "setting up oracle..."
	docker run -d -p 1521:1521 container-registry.oracle.com/database/free:latest
	echo "sleep to give container time to startup..."
    sleep 30  # Give containers time to fully start
	id=$(docker ps | grep oracle | awk '{print $1}')
	docker exec -i $id mkdir /opt/oracle/oradata/recovery_area
	docker exec -i $id sqlplus / as sysdba <<EOF
Alter user sys identified by oracle;
exit;
EOF
	sleep 1
	docker exec -i $id sqlplus /nolog <<EOF
CONNECT sys/oracle as sysdba;
alter system set db_recovery_file_dest_size = 10G;
alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
shutdown immediate;
startup mount;
alter database archivelog;
alter database open;
archive log list;
exit;
EOF
	sleep 1
	docker exec -i $id sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<EOF
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER PROFILE DEFAULT LIMIT FAILED_LOGIN_ATTEMPTS UNLIMITED;
exit;
EOF
	docker exec -i $id sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<EOF
CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
exit;
EOF
	docker exec -i $id sqlplus sys/oracle@//localhost:1521/FREEPDB1 as sysdba <<EOF
CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
exit;
EOF
	docker exec -i $id sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<'EOF'
CREATE USER c##dbzuser IDENTIFIED BY dbz DEFAULT TABLESPACE LOGMINER_TBS QUOTA UNLIMITED ON LOGMINER_TBS CONTAINER=ALL;
GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY DICTIONARY TO c##dbzuser CONTAINER=ALL;
GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER=ALL; 
GRANT SELECT ON V_$MYSTAT TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$STATNAME TO c##dbzuser CONTAINER=ALL; 
GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO C##DBZUSER;
GRANT SELECT ON DBA_HIST_SNAPSHOT TO C##DBZUSER;
GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO PUBLIC;
ALTER DATABASE ADD LOGFILE GROUP 4 ('/opt/oracle/oradata/FREE/redo04.log') SIZE 1G;
ALTER DATABASE ADD LOGFILE GROUP 5 ('/opt/oracle/oradata/FREE/redo05.log') SIZE 1G;
ALTER DATABASE ADD LOGFILE GROUP 6 ('/opt/oracle/oradata/FREE/redo06.log') SIZE 1G;
exit;
EOF

	docker exec -i $id sqlplus 'c##dbzuser/dbz@//localhost:1521/FREE' <<EOF
CREATE TABLE orders (
order_number NUMBER PRIMARY KEY,
order_date DATE,
purchaser NUMBER,
quantity NUMBER,
product_id NUMBER);
commit;
ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
exit;
EOF

	docker exec -i $id sqlplus 'c##dbzuser/dbz@//localhost:1521/FREE' <<EOF
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10001, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10002, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10003, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10004, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
commit;
exit;
EOF
	exit 0
}

function setup_remotedb()
{
	dbtype="$1"

	case "$dbtype" in
		"mysql")
			setup_mysql
			;;
		"sqlserver")
			setup_sqlserver
			;;
		"oracle")
			setup_oracle
			;;
		*)
			echo "$dbtype not supported"
			exit 1
			;;
	esac
}

setup_remotedb "${DBTYPE}"
