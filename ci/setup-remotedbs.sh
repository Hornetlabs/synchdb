#!/bin/bash

# make bash behave
set -euo pipefail

DBTYPE=${DBTYPE:?please provide database type via DBTYPE env variable}
INTERNAL=${INTERNAL:?please indicate if containers should be deployed as internal via INTERNAL env variable (0 or 1)}

basedir="$(pwd)"
MAX_TRIES=200

if docker compose version >/dev/null 2>&1; then
  docker_compose() { docker compose "$@"; }
elif command -v docker-compose >/dev/null 2>&1; then
  docker_compose() { docker-compose "$@"; }
else
  echo "ERROR: neither 'docker compose' (v2) nor 'docker-compose' (v1) found." >&2
  exit 1
fi

function wait_for_container_ready()
{
	name=$1
	keyword=$2
	ready=0

	set +e
	if ! docker inspect $name >/dev/null 2>&1; then
		echo "$name is not running or failed to start!"
		return
	fi	

	for ((i=1; i<=MAX_TRIES; i++));
	do
		docker logs "$name" 2>&1 | grep "$keyword"
		if [ $? -eq 0 ]; then
		#if docker logs "$name" 2>/dev/null | grep "$keyword"; then
			echo "$name is ready to use"
			ready=1
			break
		else
			echo "Still waiting for $name to become ready..."
			sleep 10
		fi
	done

	if [ $ready -ne 1 ]; then
		echo "giving up after $MAX_TRIES on $name..."
		exit 1
	fi
	set -e
}

function setup_mysql()
{
	echo "setting up mysql..."
	if [ $INTERNAL -eq 1 ]; then
		docker_compose -f testenv/mysql/synchdb-mysql-test-internal.yaml up -d
	else
		docker_compose -f testenv/mysql/synchdb-mysql-test.yaml up -d
	fi
	echo "sleep to give container time to startup..."
	sleep 30  # Give containers time to fully start
	docker exec -i mysql mysql -uroot -pmysqlpwdroot -e "SELECT VERSION();" || { echo "Failed to connect to MySQL"; docker logs mysql; exit 1; }
	docker exec -i mysql mysql -uroot -pmysqlpwdroot << EOF
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mysqluser'@'%';
GRANT replication client ON *.* TO 'mysqluser'@'%';
GRANT replication slave ON *.* TO 'mysqluser'@'%';
GRANT BACKUP_ADMIN ON *.* TO 'mysqluser'@'%';
GRANT all privileges on tpcc.* to mysqluser;
GRANT RELOAD ON *.* TO 'mysqluser'@'%';
FLUSH PRIVILEGES;
EOF
	exit 0
}

function setup_sqlserver()
{

	echo "setting up sqlserver..."
	if [ $INTERNAL -eq 1 ]; then
		docker_compose -f testenv/sqlserver/synchdb-sqlserver-test-internal.yaml up -d
	else
		docker_compose -f testenv/sqlserver/synchdb-sqlserver-test.yaml up -d
	fi
	echo "sleep to give container time to startup..."
	sleep 30  # Give containers time to fully start
	docker cp ./testenv/sqlserver/inventory.sql sqlserver:/  >/dev/null 2>&1
	docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -C -Q "SELECT @@VERSION"  >/dev/null 2>&1
	docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -C -i /inventory.sql  >/dev/null 2>&1
	exit 0
}

function setup_oracle()
{
	needsetup=0

	set +e
	echo "setting up oracle..."
	#docker ps -a | grep "oracle" | grep -v oracle-19c > /dev/null
	#if [ $? -ne 0 ]; then
	if ! docker ps -a --format '{{.Names}}' | grep -q "oracle"; then
        # container has not run before. Run a new one
		if [ $INTERNAL -eq 1 ]; then
        	docker_compose -f testenv/oracle/synchdb-oracle-test-internal.yaml up -d
		else
			docker_compose -f testenv/oracle/synchdb-oracle-test.yaml up -d
		fi
		wait_for_container_ready "oracle" "DATABASE IS READY TO USE"
        needsetup=1
    else
		state=$(docker inspect -f '{{.State.Status}}' oracle 2>/dev/null || echo unknown)
		if [ $state == "running" ]; then
            echo "oracle23ai is already running..."
        elif [ $state == "exited" ]; then
            echo "oracle23ai exited, starting it again..."
            docker start oracle >/dev/null 2>&1
			wait_for_container_ready "oracle" "DATABASE IS READY TO USE"
        else
            echo "oracle23ai in unknown state. Remove it and try again..."
        fi
		return
    fi

	if [ $needsetup -ne 1 ]; then
        echo "skip setting up oracle, assuming it done..."
		return
    fi
    set -e

	# check if oracle has been initialized
	isinit=$(docker exec oracle sh -c '[ -d /opt/oracle/oradata/recovery_area ]' && echo exists || echo missing)
	if [ "$isinit" == "exists" ]; then
		echo "oracle23ai has been setup already. Skip setup..."
		return
	fi
	docker exec -i oracle mkdir /opt/oracle/oradata/recovery_area
	docker exec -i oracle sqlplus / as sysdba <<EOF
Alter user sys identified by oracle;
exit;
EOF
	sleep 1
	docker exec -i oracle sqlplus /nolog <<EOF
CONNECT sys/oracle as sysdba;
alter system set db_recovery_file_dest_size = 40G;
alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
shutdown immediate;
startup mount;
alter database archivelog;
alter database open;
archive log list;
exit;
EOF
	sleep 1
	docker exec -i oracle sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<EOF
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER PROFILE DEFAULT LIMIT FAILED_LOGIN_ATTEMPTS UNLIMITED;
exit;
EOF
	docker exec -i oracle sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<EOF
CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
exit;
EOF
	docker exec -i oracle sqlplus sys/oracle@//localhost:1521/FREEPDB1 as sysdba <<EOF
CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
exit;
EOF
	docker exec -i oracle sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<'EOF'
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
ALTER DATABASE ADD LOGFILE GROUP 4 ('/opt/oracle/oradata/FREE/redo04.log') SIZE 512M;
ALTER DATABASE ADD LOGFILE GROUP 5 ('/opt/oracle/oradata/FREE/redo05.log') SIZE 512M;
ALTER DATABASE ADD LOGFILE GROUP 6 ('/opt/oracle/oradata/FREE/redo06.log') SIZE 512M;
GRANT CONNECT, RESOURCE TO C##DBZUSER;
GRANT CREATE SESSION TO C##DBZUSER;
GRANT CREATE TABLE TO C##DBZUSER;
GRANT CREATE VIEW TO C##DBZUSER;
GRANT CREATE PROCEDURE TO C##DBZUSER;
GRANT CREATE SEQUENCE TO C##DBZUSER;
GRANT CREATE TRIGGER TO C##DBZUSER;
GRANT CREATE ANY INDEX TO C##DBZUSER;
GRANT CREATE ANY TABLE TO C##DBZUSER;
GRANT CREATE ANY PROCEDURE TO C##DBZUSER;
GRANT ALTER ANY TABLE TO C##DBZUSER;
GRANT EXECUTE ANY PROCEDURE TO C##DBZUSER;
GRANT DROP ANY TABLE TO C##DBZUSER;
GRANT DROP ANY INDEX TO C##DBZUSER;
GRANT DROP ANY SEQUENCE TO C##DBZUSER;
GRANT DROP ANY PROCEDURE TO C##DBZUSER;
GRANT DROP ANY VIEW TO C##DBZUSER;
GRANT DROP ANY TRIGGER TO C##DBZUSER;
GRANT DROP ANY SYNONYM TO C##DBZUSER;
GRANT DROP ANY MATERIALIZED VIEW TO C##DBZUSER;
exit;
EOF

	docker exec -i oracle sqlplus 'c##dbzuser/dbz@//localhost:1521/FREE' <<EOF
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

	docker exec -i oracle sqlplus 'c##dbzuser/dbz@//localhost:1521/FREE' <<EOF
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10001, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10002, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10003, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10004, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
commit;
exit;
EOF
	exit 0
}

function setup_ora19c()
{
	needsetup=0
	forolr=$1

	set +e
	echo "setting up oracle 19c..."
	#docker ps -a | grep "ora19c" > /dev/null
	#if [ $? -ne 0 ]; then
	if ! docker ps -a --format '{{.Names}}' | grep -q "ora19c"; then
		# container has not run before. Run a new one
		if [ "$forolr" == "olr" ]; then
			if [ $INTERNAL -eq 1 ]; then
				docker_compose -f testenv/ora19c/synchdb-ora19c-test-olr-internal.yaml up -d
			else
				docker_compose -f testenv/ora19c/synchdb-ora19c-test-olr.yaml up -d
			fi
		else
			if [ $INTERNAL -eq 1 ]; then
				docker_compose -f testenv/ora19c/synchdb-ora19c-test-internal.yaml up -d
			else
				docker_compose -f testenv/ora19c/synchdb-ora19c-test.yaml up -d
			fi
		fi
		wait_for_container_ready "ora19c" "DATABASE IS READY TO USE"
		needsetup=1	
	else
		state=$(docker inspect -f '{{.State.Status}}' ora19c 2>/dev/null || echo unknown)
		if [ $state == "running" ]; then
            echo "oracle19c is already running..."
        elif [ $state == "exited" ]; then
            echo "oracle19c exited, starting it again..."
            docker start ora19c >/dev/null 2>&1
			wait_for_container_ready "ora19c" "DATABASE IS READY TO USE"
        else
            echo "oracle19c in unknown state. Remove it and try again..."
        fi
		return
	fi

	if [ $needsetup -ne 1 ]; then
		echo "skip setting up oracle 19c, assuming it done..."
		return
	fi
	set -e

	# check if oracle has been initialized
    isinit=$(docker exec ora19c sh -c '[ -d /opt/oracle/oradata/recovery_area ]' && echo exists || echo missing)
    if [ "$isinit" == "exists" ]; then
		echo "oracle19c has been setup already. Skip setup..."
		return
    fi

	docker exec -i ora19c mkdir /opt/oracle/oradata/recovery_area
	sleep 1
	docker exec -i ora19c sqlplus /nolog <<EOF
CONNECT sys/oracle as sysdba;
alter system set db_recovery_file_dest_size = 40G;
alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
shutdown immediate;
startup mount;
alter database archivelog;
alter database open;
archive log list;
exit;
EOF

	sleep 1
	docker exec -i ora19c sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<EOF
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER PROFILE DEFAULT LIMIT FAILED_LOGIN_ATTEMPTS UNLIMITED;
exit;
EOF

docker exec -i ora19c sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<EOF
CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
exit;
EOF

docker exec -i ora19c sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<'EOF'
CREATE USER DBZUSER IDENTIFIED BY dbz DEFAULT TABLESPACE LOGMINER_TBS QUOTA UNLIMITED ON LOGMINER_TBS;
GRANT CREATE SESSION TO DBZUSER;
GRANT SET CONTAINER TO DBZUSER;
GRANT SELECT ON V_$DATABASE TO DBZUSER;
GRANT FLASHBACK ANY TABLE TO DBZUSER;
GRANT SELECT ANY TABLE TO DBZUSER;
GRANT SELECT_CATALOG_ROLE TO DBZUSER;
GRANT EXECUTE_CATALOG_ROLE TO DBZUSER;
GRANT SELECT ANY TRANSACTION TO DBZUSER;
GRANT LOGMINING TO DBZUSER;
GRANT SELECT ANY DICTIONARY TO DBZUSER;
GRANT CREATE TABLE TO DBZUSER;
GRANT LOCK ANY TABLE TO DBZUSER;
GRANT CREATE SEQUENCE TO DBZUSER;	
GRANT EXECUTE ON DBMS_LOGMNR TO DBZUSER;
GRANT EXECUTE ON DBMS_LOGMNR_D TO DBZUSER;	
GRANT SELECT ON V_$LOG TO DBZUSER;
GRANT SELECT ON V_$LOG_HISTORY TO DBZUSER;	
GRANT SELECT ON V_$LOGMNR_LOGS TO DBZUSER ;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO DBZUSER ;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO DBZUSER;
GRANT SELECT ON V_$LOGFILE TO DBZUSER ;
GRANT SELECT ON V_$ARCHIVED_LOG TO DBZUSER ;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO DBZUSER;
GRANT SELECT ON V_$TRANSACTION TO DBZUSER; 
GRANT SELECT ON V_$MYSTAT TO DBZUSER;
GRANT SELECT ON V_$STATNAME TO DBZUSER; 	
GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO DBZUSER;
GRANT SELECT ON DBA_HIST_SNAPSHOT TO DBZUSER;
GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO PUBLIC;	
GRANT CREATE ANY DIRECTORY TO DBZUSER;

GRANT SELECT, FLASHBACK ON SYS.CCOL$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.CDEF$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.COL$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.DEFERRED_STG$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.ECOL$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.LOB$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.LOBCOMPPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.LOBFRAG$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.OBJ$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TAB$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TABCOMPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TABPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TABSUBPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TS$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.USER$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON XDB.XDB$TTSET TO DBZUSER;
GRANT FLASHBACK ANY TABLE TO DBZUSER;
GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO DBZUSER;
GRANT SELECT ON SYS.V_$DATABASE TO DBZUSER;
GRANT SELECT ON SYS.V_$DATABASE_INCARNATION TO DBZUSER;
GRANT SELECT ON SYS.V_$LOG TO DBZUSER;
GRANT SELECT ON SYS.V_$LOGFILE TO DBZUSER;
GRANT SELECT ON SYS.V_$PARAMETER TO DBZUSER;
GRANT SELECT ON SYS.V_$STANDBY_LOG TO DBZUSER;
GRANT SELECT ON SYS.V_$TRANSPORTABLE_PLATFORM TO DBZUSER;

GRANT CREATE PROCEDURE TO DBZUSER;
GRANT CREATE ANY PROCEDURE TO DBZUSER;
GRANT EXECUTE ON DBMS_RANDOM TO DBZUSER;
GRANT CREATE SEQUENCE TO DBZUSER;
GRANT CREATE TRIGGER TO DBZUSER;
GRANT CREATE TYPE TO DBZUSER;
GRANT CREATE TABLE TO DBZUSER;
GRANT CREATE SESSION TO DBZUSER;
GRANT CREATE VIEW TO DBZUSER;
GRANT UNLIMITED TABLESPACE TO DBZUSER;

DECLARE
    CURSOR C1 IS SELECT TOKSUF FROM XDB.XDB$TTSET;
    CMD VARCHAR2(2000);
BEGIN
    FOR C IN C1 LOOP
        CMD := 'GRANT SELECT, FLASHBACK ON XDB.X$NM' || C.TOKSUF || ' TO DBZUSER';
        EXECUTE IMMEDIATE CMD;
        CMD := 'GRANT SELECT, FLASHBACK ON XDB.X$QN' || C.TOKSUF || ' TO DBZUSER';
        EXECUTE IMMEDIATE CMD;
        CMD := 'GRANT SELECT, FLASHBACK ON XDB.X$PT' || C.TOKSUF || ' TO DBZUSER';
        EXECUTE IMMEDIATE CMD;
    END LOOP;
END;
/
EOF

    docker exec -i ora19c sqlplus 'DBZUSER/dbz@//localhost:1521/FREE' <<EOF
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

    docker exec -i ora19c sqlplus 'DBZUSER/dbz@//localhost:1521/FREE' <<EOF
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10001, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10002, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10003, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10004, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
commit;
exit;
EOF

}

function setup_hammerdb()
{
	which=${WHICH:?please provide which hammerdb type, mysql, sqlserver or oracle}
	echo "setting up hammerdb for $which..."
	docker run -d --name hammerdb hgneon/hammerdb:5.0
	echo "sleep to give container time to startup..."
	sleep 5

	if [ $which == "olr" ]; then
		which="ora19c"
	fi

	docker cp ./testenv/hammerdb/${which}_buildschema.tcl hammerdb:/buildschema.tcl
	docker cp ./testenv/hammerdb/${which}_runtpcc.tcl hammerdb:/runtpcc.tcl
	exit 0
}

function setup_oradata()
{
	if [ ! -d ./testenv/olr/oradata ]; then
		mkdir ./testenv/olr/oradata
		sudo chown 54321:54321 -R ./testenv/olr/oradata
	fi
	
	if [ ! -d ./testenv/olr/checkpoint ]; then
		mkdir ./testenv/olr/checkpoint
		sudo chown 54321:54321 -R ./testenv/olr/checkpoint
	fi
	
	if [ ! -d ./testenv/olr/olrswap ]; then
		mkdir ./testenv/olr/olrswap
		sudo chown 54321:54321 -R ./testenv/olr/olrswap
	fi
	
	if [ ! -d ./testenv/olr/fast-recovery-area ]; then
		mkdir ./testenv/olr/fast-recovery-area
		sudo chown 54321:54321 -R ./testenv/olr/fast-recovery-area
	fi
}

function setup_olr()
{
	echo "setting up openlog replicator $OLRVER..."

	#docker ps -a | grep "OpenLogReplicator" >/dev/null
    #if [ $? -ne 0 ]; then
	if ! docker ps -a --format '{{.Names}}' | grep -q "OpenLogReplicator"; then
		docker_compose -f testenv/olr/synchdb-olr-test.yaml up -d
#		docker run -d --name OpenLogReplicator \
#			--network synchdbnet \
#			-p 7070:7070 \
#			-v /opt/fast-recovery-area:/opt/fast-recovery-area \
#			-v ./testenv/olr:/opt/OpenLogReplicator/scripts \
#			-v ./testenv/olr/checkpoint:/opt/OpenLogReplicator/checkpoint \
#			-v ./testenv/olr/oradata:/opt/oracle/oradata \
#			--user 54321 \
#			hgneon/openlogreplicator:$OLRVER
	else
		state=$(docker inspect -f '{{.State.Status}}' OpenLogReplicator 2>/dev/null || echo unknown)
        #docker ps -a | grep "OpenLogReplicator" | grep -i "Exited" > /dev/null
        if [ $state == "running" ]; then
			echo "openlog repliator is already running..."
		elif [ $state == "exited" ]; then
			echo "OpenLogReplicator exited, starting it again..."
            docker start OpenLogReplicator >/dev/null 2>&1
        else
			echo "OpenLogReplicator in unknown state. Remove it and try again..."
        fi
    fi
}

function setup_postgres()
{
    echo "setting up postgres..."
    if [ $INTERNAL -eq 1 ]; then
        docker_compose -f testenv/postgres/synchdb-postgres-test-internal.yaml up -d
    else
        docker_compose -f testenv/postgres/synchdb-postgres-test.yaml up -d
    fi
    echo "sleep to give container time to startup..."
    sleep 30  # Give containers time to fully start

	docker exec -i postgres psql -U postgres -d postgres -c "CREATE TABLE orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)"
	docker exec -i postgres psql -U postgres -d postgres -c "INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10001, '2026-01-01', 1003, 2, 107)"
	docker exec -i postgres psql -U postgres -d postgres -c "INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10002, '2026-01-01', 1003, 2, 107)"
	docker exec -i postgres psql -U postgres -d postgres -c "INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10003, '2026-01-01', 1003, 2, 107)"
	docker exec -i postgres psql -U postgres -d postgres -c "INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10004, '2026-01-01', 1003, 2, 107)"

	# install ddl trigger function
	docker exec -i postgres psql -U postgres -d postgres < ./postgres-connector-src-ddl-setup.sql
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
		"ora19c")
			setup_ora19c "notolr"
			;;
		"hammerdb")
			setup_hammerdb
			;;
		"olr")
			OLRVER=${OLRVER:?please provide OLR version via OLRVER env variable}
			setup_oradata
			setup_ora19c "olr"
			setup_olr
			;;
		"postgres")
			setup_postgres
			;;
		*)
			echo "$dbtype not supported"
			exit 1
			;;
	esac
	exit 0
}

setup_remotedb "${DBTYPE}"
