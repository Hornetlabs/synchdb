#!/bin/bash

PGDATA=${HOME}/synchdb-test
PGLOG=${HOME}/logfile
SOCKDIR=/var/run/postgresql

if [ ! -d $SOCKDIR ]; then
	mkdir -p $SOCKDIR
	chown ubuntu:ubuntu /var/run/postgresql
	chmod 775 /var/run/postgresql
fi

if [ -d "$PGDATA" ]; then
	echo "synchdb is already initialized"
	#pg_ctl -D $PGDATA -l $PGLOG start
	exec postgres -D $PGDATA
	exit 0
fi

initdb -D $PGDATA
#echo "unix_socket_directories = '/home/ubuntu/tmp'" >> $PGDATA/postgresql.conf
echo "listen_addresses ='*'" >> $PGDATA/postgresql.conf
#pg_ctl -D $PGDATA -l $PGLOG start
exec postgres -D $PGDATA
