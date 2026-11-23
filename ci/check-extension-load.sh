#!/bin/bash

# Verify the installed server can resolve every SynchDB symbol before packaging.
set -euo pipefail

pgconfig=${1:?usage: check-extension-load.sh /path/to/pg_config}
bindir=$("$pgconfig" --bindir)
libdir=$("$pgconfig" --libdir)
java_path=$(readlink -f "$(command -v java)")
jdk_home=$(dirname "$(dirname "$java_path")")
export LD_LIBRARY_PATH="${libdir}:${jdk_home}/lib/server:${LD_LIBRARY_PATH:-}"

workdir=$(mktemp -d /tmp/synchdb-load.XXXXXX)
cleanup()
{
	local status=$?
	trap - EXIT
	"$bindir/pg_ctl" -D "$workdir/data" -m immediate -w stop >/dev/null 2>&1 || true
	if [ "$status" -ne 0 ] && [ -f "$workdir/server.log" ]; then
		cat "$workdir/server.log" >&2
	fi
	rm -rf "$workdir"
	exit "$status"
}
trap cleanup EXIT

"$bindir/initdb" -D "$workdir/data" -A trust --no-locale > "$workdir/initdb.log"
"$bindir/pg_ctl" -D "$workdir/data" -l "$workdir/server.log" \
	-o "-c listen_addresses='' -k '$workdir'" -w start
"$bindir/psql" -X -v ON_ERROR_STOP=1 -h "$workdir" -d postgres \
	-c "CREATE EXTENSION synchdb CASCADE;" \
	-c "SELECT extversion FROM pg_extension WHERE extname = 'synchdb';"
