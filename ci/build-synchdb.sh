#!/bin/bash

# make bash behave
set -euo pipefail
IFS=$'\n\t'

# read pg major version, error if not provided
PG_MAJOR=${PG_MAJOR:?please provide the postgres major version}
PG_BRANCH=${PG_BRANCH:?please provide the postgres branch}

# get codename from release file
. /etc/os-release
codename=${VERSION#*(}
codename=${codename%)*}

# we'll do everything with absolute paths
basedir="$(pwd)"

function build_synchdb()
{
	pg_major="$1"

	echo $PG_BRANCH
	installdir="${basedir}/synchdb-install-${pg_major}"
	mkdir -p $installdir
	echo "Beginning build for PostgreSQL ${pg_major}..." >&2

	git clone https://github.com/postgres/postgres.git --branch ${PG_BRANCH}
	(
		cd postgres && \
        	./configure --prefix=${installdir}/usr/lib/postgresql/${PG_MAJOR} \
            --enable-cassert \
            -enable-rpath \
            --enable-injection-points \
            --with-libedit-preferred \
            --with-libxml \
            --with-icu \
            --with-ssl=openssl && \
        	make && \
        	make install

		cd contrib && \
			make && \
			make install
	)
	
	git clone https://github.com/protobuf-c/protobuf-c.git --branch v1.5.2
	(
		cd protobuf-c && \
			./autogen.sh && \
        	./configure --prefix=${installdir}/usr/local && \
        	make && \
        	make install
	)

	mkdir -p postgres/contrib/synchdb
	rsync -a --delete \
			--exclude '.git/' \
			--exclude='.github/' \
			--exclude='ci/' \
			--exclude='testenv/' \
			--exclude='postgres/' \
			--exclude='protobuf-c/' \
			./ postgres/contrib/synchdb/
	(
		cd postgres/contrib/synchdb && \
			make oracle_parser && \
			make install_oracle_parser && \
			make WITH_OLR=1 build_dbz && \
			make WITH_OLR=1 PROTOBUF_C_INCLUDE_DIR=$installdir/usr/local/include PROTOBUF_C_LIB_DIR=$installdir/usr/local/lib && \
			make WITH_OLR=1 install && \
			make WITH_OLR=1 install_dbz
	)	

	#mkdir -p "${builddir}" && cd "${builddir}"
	
	
	#export USE_PGXS=1
	#make build_dbz PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config
	#make PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config

	#sudo USE_PGXS=1 make install DESTDIR=${installdir} PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config
	#sudo USE_PGXS=1 make install_dbz pkglibdir=${installdir}/usr/lib/postgresql/${pg_major}/lib  PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config

	cd $installdir
	tar czvf synchdb-install-${pg_major}.tar.gz *
	mv synchdb-install-${pg_major}.tar.gz $basedir
}

build_synchdb "${PG_MAJOR}"
