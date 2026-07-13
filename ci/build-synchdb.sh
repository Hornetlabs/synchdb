#!/bin/bash

# make bash behave
set -euo pipefail
IFS=$'\n\t'

# TARGET_FLAVOR selects what to build SynchDB against:
#   postgres  (default) -> clone PostgreSQL at $PG_BRANCH, install under
#                          usr/lib/postgresql/$PG_MAJOR, artifact synchdb-install-$PG_MAJOR
#   ivorysql            -> clone IvorySQL at $IVORYSQL_TAG, install under
#                          usr/lib/ivorysql/ivorysql-$IVORYSQL_MAJOR,
#                          artifact synchdb-install-ivorysql$IVORYSQL_MAJOR
# IvorySQL is a PostgreSQL fork, so the protobuf-c / oracle_fdw / synchdb
# (WITH OLR) steps are identical against the resulting pg_config.
#
# NOTE: build/install commands are written as separate statements (not one
# long `a && b && c` chain).  Under `set -e`, a non-final command in a && list
# that fails does NOT abort the script, which previously masked a failed
# `make` and surfaced later as a confusing "pg_config: No such file" error.
TARGET_FLAVOR=${TARGET_FLAVOR:-postgres}

# we'll do everything with absolute paths
basedir="$(pwd)"

function build_synchdb()
{
	local srcdir repo branch prefix installdir artifact pgconfig

	if [ "$TARGET_FLAVOR" = "ivorysql" ]; then
		IVORYSQL_TAG=${IVORYSQL_TAG:?please provide the IvorySQL git tag, e.g. IvorySQL_4.6}
		IVORYSQL_MAJOR=${IVORYSQL_MAJOR:?please provide the IvorySQL major version, e.g. 4}
		srcdir="ivorysql"
		repo="https://github.com/IvorySQL/IvorySQL.git"
		branch="${IVORYSQL_TAG}"
		installdir="${basedir}/synchdb-install-ivorysql${IVORYSQL_MAJOR}"
		prefix="${installdir}/usr/lib/ivorysql/ivorysql-${IVORYSQL_MAJOR}"
		artifact="synchdb-install-ivorysql${IVORYSQL_MAJOR}"
		echo "Beginning build for IvorySQL ${IVORYSQL_MAJOR} (${IVORYSQL_TAG})..." >&2
	else
		PG_MAJOR=${PG_MAJOR:?please provide the postgres major version}
		PG_BRANCH=${PG_BRANCH:?please provide the postgres branch}
		srcdir="postgres"
		repo="https://github.com/postgres/postgres.git"
		branch="${PG_BRANCH}"
		installdir="${basedir}/synchdb-install-${PG_MAJOR}"
		prefix="${installdir}/usr/lib/postgresql/${PG_MAJOR}"
		artifact="synchdb-install-${PG_MAJOR}"
		echo "Beginning build for PostgreSQL ${PG_MAJOR} (${PG_BRANCH})..." >&2
	fi

	pgconfig="${prefix}/bin/pg_config"
	mkdir -p "$prefix"

	# ---- core server (PostgreSQL or IvorySQL) ----
	git clone "$repo" --branch "$branch" "$srcdir"
	(
		cd "$srcdir"
		./configure --prefix="${prefix}" \
			--enable-cassert \
			--enable-rpath \
			--enable-injection-points \
			--with-libedit-preferred \
			--with-libxml \
			--with-uuid=e2fs \
			--with-icu \
			--with-ssl=openssl
		make
		make install
		cd contrib
		make
		make install
	)

	# Fail loud (and early) if the core install did not produce pg_config,
	# instead of letting a masked build failure resurface downstream.
	if [ ! -x "${pgconfig}" ]; then
		echo "ERROR: ${pgconfig} missing after ${srcdir} build/install" >&2
		ls -la "$(dirname "${pgconfig}")" 2>/dev/null || true
		exit 1
	fi

	# TODO: currently build under IvorySQL 5.4 will failed: ExecTypeFromTL
	# ---- mysql_fdw (FDW-based MySQL snapshot) ----
	if [ "$TARGET_FLAVOR" != "ivorysql" ] || [ "$IVORYSQL_MAJOR" != "5" ]; then
		git clone https://github.com/EnterpriseDB/mysql_fdw.git --branch REL-2_9_3 "${srcdir}/contrib/mysql_fdw"
		(
			cd "${srcdir}/contrib/mysql_fdw"
			make PG_CONFIG="${pgconfig}"
			make install PG_CONFIG="${pgconfig}"
		)
	fi

	# ---- protobuf-c (OLR connector dependency) ----
	git clone https://github.com/protobuf-c/protobuf-c.git --branch v1.5.2
	(
		cd protobuf-c
		./autogen.sh
		./configure --prefix="${installdir}/usr/local"
		make
		make install
	)

	# ---- oracle_fdw (FDW-based Oracle/OLR snapshot) ----
	git clone https://github.com/laurenz/oracle_fdw.git --branch ORACLE_FDW_2_8_0 "${srcdir}/contrib/oracle_fdw"
	(
		cd "${srcdir}/contrib/oracle_fdw"
		sed -i -e 's|FIND_INCLUDE := $(wildcard /usr/include/oracle/\*/client64 /usr/include/oracle/\*/client)|FIND_INCLUDE := $(wildcard /usr/include/oracle/*/client64 /usr/include/oracle/*/client $(OCI_INC_DIR))|' \
			   -e 's|FIND_LIBDIRS := $(wildcard /usr/lib/oracle/\*/client64/lib /usr/lib/oracle/\*/client/lib)|FIND_LIBDIRS := $(wildcard /usr/lib/oracle/*/client64/lib /usr/lib/oracle/*/client/lib $(OCI_LIB_DIR))|' \
			Makefile
		make PG_CONFIG="${pgconfig}"
		make install PG_CONFIG="${pgconfig}"
	)

	# ---- SynchDB (in-tree, WITH OLR) ----
	mkdir -p "${srcdir}/contrib/synchdb"
	rsync -a --delete \
			--exclude '.git/' \
			--exclude='.github/' \
			--exclude='ci/' \
			--exclude='testenv/' \
			--exclude='postgres/' \
			--exclude='ivorysql/' \
			--exclude='protobuf-c/' \
			./ "${srcdir}/contrib/synchdb/"
	(
		cd "${srcdir}/contrib/synchdb"
		make oracle_parser
		make install_oracle_parser
		make WITH_OLR=1 build_dbz
		make WITH_OLR=1 PROTOBUF_C_INCLUDE_DIR="${installdir}/usr/local/include" PROTOBUF_C_LIB_DIR="${installdir}/usr/local/lib"
		make WITH_OLR=1 install
		make WITH_OLR=1 install_dbz
	)

	cd "$installdir"
	tar czvf "${artifact}.tar.gz" *
	mv "${artifact}.tar.gz" "$basedir"
}

build_synchdb
