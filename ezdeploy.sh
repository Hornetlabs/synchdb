#!/bin/bash

set -euo pipefail

OLRVER="1.8.5"

function have()
{
	command -v "$1" >/dev/null 2>&1;
}

function deploy-monitoring()
{
	echo "setting up prometheus..."
	if docker inspect prometheus >/dev/null 2>&1; then
                echo "prometheus already exists: skip it..."
        else
                docker_compose -f ./src/monitoring/docker-compose.yaml up -d prometheus
        fi
	echo "setting up grafana..."
	if docker inspect grafana >/dev/null 2>&1; then
                echo "grafaaana already exists: skip it..."
        else
                docker_compose -f ./src/monitoring/docker-compose.yaml up -d grafana
        fi
}

function teardown-monitoring()
{
	echo "tearing down grafana..."
	if docker inspect grafana >/dev/null 2>&1; then
		docker stop grafana >/dev/null 2>&1
		docker rm grafana >/dev/null 2>&1
	fi

	echo "tearing down prometheus"
	if docker inspect prometheus >/dev/null 2>&1; then
		docker stop prometheus >/dev/null 2>&1
		docker rm prometheus >/dev/null 2>&1
	fi
}

function deploy-synchdb()
{
	echo "setting up synchdb..."
	if docker inspect synchdb >/dev/null 2>&1; then
		echo "synchdb already exists: skip it..."
	else
		docker_compose -f ./testenv/synchdb/synchdb-test.yaml up -d
	fi
}

function teardown-synchdb()
{
	echo "tearing down synchdb..."
	if docker inspect synchdb >/dev/null 2>&1; then
		docker stop synchdb >/dev/null 2>&1
		docker rm synchdb >/dev/null 2>&1
	fi
}

function deploy-sourcedb()
{
	if [ $1 == "olr" ]; then
		DBTYPE=$1 INTERNAL=1 OLRVER=$OLRVER ./ci/setup-remotedbs.sh
	else
		DBTYPE=$1 INTERNAL=1 ./ci/setup-remotedbs.sh
	fi
}

function teardown-sourcedb()
{
	echo "tearing down $1 if active..."
	if [ $1 == "olr" ]; then
		name="OpenLogReplicator"
	else
		name=$1
	fi

	if docker inspect $name >/dev/null 2>&1; then
		DBTYPE=$1 ./ci/teardown-remotedbs.sh	
	fi
}

function clear-oradata()
{
	DBTYPE=oradata ./ci/teardown-remotedbs.sh
}

function custom-deployment()
{
	echo ""
	echo "please list source databases separate by comma"
	echo "possible values: (mysql, sqlserver, oracle23ai, oracle19c, olr, postgres)"

	read -rp "your selection: " RAW_CHOICES
	IFS=', ' read -r -a _tokens <<< "$RAW_CHOICES"
	declare -A PICKED=()
	for t in "${_tokens[@]}"; do
		[[ -z "$t" ]] && continue
		key="${t,,}"
		case "$key" in
			mysql|sqlserver|oracle23ai|oracle19c|olr|postgres) PICKED["$key"]=1 ;;
		*) echo "Ignoring unknown option: $t" ;;
		esac
	done

	if [[ "${#PICKED[@]}" -eq 0 ]]; then
		echo "No valid selections made. We'll deploy only 'synchdb'."
		FINAL_LIST="synchdb"
	else

		if [[ -n "${PICKED[oracle19c]+x}" && -n "${PICKED[olr]+x}" ]]; then
			echo "Ignoring oracle19c because olr will also deploy oracle19c"
			unset PICKED[oracle19c]
		fi

		FINAL_LIST="synchdb $(printf '%s ' "${!PICKED[@]}" | sed 's/ $//')"
	fi

	#echo "About to deploy: $FINAL_LIST"
	for x in $FINAL_LIST;
	do
		if [ $x == "synchdb" ]; then
			deploy-synchdb
			continue
		fi

		if [ $x == "oracle23ai" ]; then
			x="oracle"
		fi

		if [ $x == "oracle19c" ]; then
			x="ora19c"
		fi

		deploy-sourcedb $x
		
		#echo "deploying $x ..."
	done
}


# check required tools
if ! have docker; then
	echo "docker is missing. Exit..."
	exit 1
fi

if docker compose version >/dev/null 2>&1; then
	docker_compose() { docker compose "$@"; }
elif command -v docker-compose >/dev/null 2>&1; then
	docker_compose() { docker-compose "$@"; }
else
	echo "docker-compose or docker compose is missing. Exit..."
	exit 1
fi

# check required scripts
if [ ! -f ./ci/setup-remotedbs.sh ] || [ ! -f ./ci/teardown-remotedbs.sh ]; then
	echo "please run this script in the root directory of synchdb project"
	exit 1
fi

# prompts

echo "----------------------------------"
echo "-----> Welcome to ezdeploy! <-----"
echo "----------------------------------"
echo ""
echo "please select a quick deploy option:"
echo -e "\t 1) synchdb only"
echo -e "\t 2) synchdb + mysql"
echo -e "\t 3) synchdb + sqlserver"
echo -e "\t 4) synchdb + oracle23ai"
echo -e "\t 5) synchdb + oracle19c"
echo -e "\t 6) synchdb + olr(oracle19c)"
echo -e "\t 7) synchdb + postgres18"
echo -e "\t 8) synchdb + all source databases"
echo -e "\t 9) custom deployment"
echo -e "\t10) deploy monitoring"
echo -e "\t11) teardown deployment"

read -rp "enter your selection: " choice

case "$choice" in
  1) deploy-synchdb
	 ;;
  2) deploy-synchdb
	 deploy-sourcedb "mysql"
	 ;;
  3) deploy-synchdb
	 deploy-sourcedb "sqlserver"
	 ;;
  4) deploy-synchdb
	 deploy-sourcedb "oracle"
	 ;;
  5) deploy-synchdb
	 deploy-sourcedb "ora19c"
	 ;;
  6) deploy-synchdb
	 deploy-sourcedb "olr"
	 ;;
  7) deploy-synchdb
	 deploy-sourcedb "postgres"
	 ;;
  8) deploy-synchdb
	 deploy-sourcedb "mysql"
	 deploy-sourcedb "sqlserver" 
	 deploy-sourcedb "oracle" 
	 deploy-sourcedb "olr"
	 deploy-sourcedb "postgres"
	 ;;
  9) custom-deployment 
	 ;;
 10) deploy-monitoring
	 ;;
 11) teardown-synchdb
	 teardown-sourcedb "mysql"
	 teardown-sourcedb "sqlserver" 
	 teardown-sourcedb "oracle" 
	 teardown-sourcedb "ora19c" 
	 teardown-sourcedb "olr"
	 teardown-sourcedb "postgres"
	 teardown-monitoring
	 clear-oradata
	 teardown-sourcedb "synchdbnet"
	 ;;
  *) echo "Invalid choice"; exit 1
	 ;;
esac

echo "job done..."
