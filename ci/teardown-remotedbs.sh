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

function teardown_mysql()
{
	echo "tearing down mysql..."
	docker-compose -f testenv/mysql/synchdb-mysql-test.yaml down
	exit 0
}

function teardown_sqlserver()
{

	echo "tearing down sqlserver..."
	docker-compose -f testenv/sqlserver/synchdb-sqlserver-test.yaml down
	exit 0
}

function teardown_oracle()
{
	echo "tearing down oracle..."
	id=$(docker ps | grep oracle | awk '{print $1}')
	docker stop $id
	docker remove $id
	exit 0
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
		*)
			echo "$dbtype not supported"
			exit 1
			;;
	esac
}

teardown_remotedb "${DBTYPE}"
