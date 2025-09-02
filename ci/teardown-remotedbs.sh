#!/bin/bash

# make bash behave
set -euo pipefail
IFS=$'\n\t'

# read pg major version, error if not provided
DBTYPE=${DBTYPE:?please provide database type}

# we'll do everything with absolute paths
basedir="$(pwd)"

function teardown_mysql()
{
	echo "tearing down mysql..."
	docker stop mysql >/dev/null 2>&1
	docker rm mysql >/dev/null 2>&1
	#docker-compose -f testenv/mysql/synchdb-mysql-test.yaml down
}

function teardown_sqlserver()
{

	echo "tearing down sqlserver..."
	docker stop sqlserver >/dev/null 2>&1
	docker rm sqlserver >/dev/null 2>&1
	#docker-compose -f testenv/sqlserver/synchdb-sqlserver-test.yaml down
}

function teardown_oracle()
{
	echo "tearing down oracle..."
	docker stop oracle >/dev/null 2>&1
	docker rm oracle >/dev/null 2>&1
	#docker-compose -f testenv/oracle/synchdb-oracle-test.yaml down
}

function teardown_ora19c()
{
	echo "tearing down ora19c..."
	docker stop ora19c >/dev/null 2>&1
	docker rm ora19c >/dev/null 2>&1
}

function teardown_hammerdb()
{
	echo "tearing down hammerdb..."
	docker stop	hammerdb >/dev/null 2>&1
	docker rm hammerdb >/dev/null 2>&1
}

function teardown_olr()
{
	echo "tearing down olr..."
	docker stop OpenLogReplicator >/dev/null 2>&1
	docker rm OpenLogReplicator >/dev/null 2>&1
	#docker-compose -f testenv/olr/synchdb-olr-test.yaml down
}

function teardown_oradata()
{
	echo "tearing down oradata..."
    if [ -d ./testenv/olr/oradata ]; then
        sudo rm -rf ./testenv/olr/oradata
    fi

    if [ -d ./testenv/olr/checkpoint ]; then
        sudo rm -rf ./testenv/olr/checkpoint
    fi
}

function teardown_synchdbnet()
{
	echo "tearing down synchdbnet..."
	docker network rm synchdbnet >/dev/null 2>&1
}

function teardown_remotedb()
{
	dbtype="$1"

	case "$dbtype" in
		"mysql")
			teardown_mysql
			;;
		"sqlserver")
			teardown_sqlserver
			;;
		"oracle")
			teardown_oracle
			;;
		"ora19c")
			teardown_ora19c
			;;
		"hammerdb")
			teardown_hammerdb
			;;
		"olr")
			teardown_olr
			teardown_ora19c
			;;
		"synchdbnet")
			teardown_synchdbnet
			;;
		"oradata")
			teardown_oradata
			;;
		*)
			echo "$dbtype not supported"
			exit 1
			;;
	esac
}

teardown_remotedb "${DBTYPE}"
