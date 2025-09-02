#############################################
# DOCKER FILE FOR UBUNTU BIONIC
#############################################
FROM    ubuntu:24.04

# We don't want a prompt during installation
ENV     DEBIAN_FRONTEND=noninteractive
ENV 	POSTGRES_VERSION=17
ENV 	JAVA_VERSION=17
ENV		SYNCHDB_BRANCH=synchdb-devel
ENV		LIBPROTOBUF_C_VERSION=v1.5.2
ENV		POSTGRES_BRANCH=REL_17_6

# Install necessary
RUN     apt-get update
RUN     apt-get install -y vim wget gawk net-tools expect apt-utils openssh-client
RUN     apt-get install -y zlib1g-dev libxml2-utils xsltproc ccache pkg-config
RUN     apt-get install -y build-essential git lcov bison flex
RUN     apt-get install -y openjdk-${JAVA_VERSION}-jdk maven
RUN     apt-get install -y libkrb5-dev libssl-dev libldap-dev libpam-dev
RUN     apt-get install -y gettext libxml2-dev libxslt-dev
RUN     apt-get install -y libreadline-dev libedit-dev
RUN     apt-get install -y uuid-dev libossp-uuid-dev
RUN     apt-get install -y libipc-run-perl libtime-hires-perl libtest-simple-perl
RUN     apt-get install -y cppcheck
RUN     apt-get install -y chrpath sudo
RUN     apt-get install -y autoconf automake libtool
RUN     apt-get install -y protobuf-compiler libprotobuf-dev libprotoc-dev

#RUN		apt install -y postgresql-common
#RUN		echo -ne "\n" | sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
#RUN		apt-get install -y postgresql-client-${POSTGRES_VERSION}
#RUN		apt-get install -y postgresql-${POSTGRES_VERSION}
#RUN		apt-get install -y postgresql-server-dev-${POSTGRES_VERSION}

# Restore environment for manual usage
ENV     DEBIAN_FRONTEND=

# Arguments with default values
ARG     USERNAME=ubuntu
ARG     GROUPNAME=ubuntu
#ARG     UID=1000
#ARG     GID=1001
ARG     USER_HOME=/home/ubuntu

# Exposing interfaces
#VOLUME  ["/outputs"]

# We want a non-root user to validate certain build conditions
#RUN     groupadd -g $GID $USERNAME
#RUN     useradd -G $GROUPNAME -N -m -s /bin/bash $USERNAME
RUN     echo 'root:root' | chpasswd
RUN     echo "$USERNAME:$USERNAME" | chpasswd
RUN     adduser ubuntu sudo

# Gitlab/Github access
RUN     mkdir -p $USER_HOME/.ssh

RUN     ssh-keyscan github.com >> $USER_HOME/.ssh/known_hosts
#ADD     keys/github/* $USER_HOME/.ssh/

RUN     chown -R $USERNAME:$GROUPNAME $USER_HOME/.ssh
RUN     chmod 700 $USER_HOME/.ssh
RUN     chmod 600 $USER_HOME/.ssh/*

# dependency for Openlog Replicator Connector
WORKDIR $USER_HOME
RUN		git clone https://github.com/protobuf-c/protobuf-c.git --branch ${LIBPROTOBUF_C_VERSION}

WORKDIR $USER_HOME/protobuf-c
RUN		./autogen.sh && \
		./configure && \
		make && \
		make install && \
		ldconfig

# Copy the build script
WORKDIR $USER_HOME
RUN		git clone https://github.com/postgres/postgres.git --branch ${POSTGRES_BRANCH}
RUN		cd postgres && \
		./configure --prefix=/usr/lib/postgresql/${POSTGRES_VERSION} \
			--enable-cassert \
			-enable-rpath \
			--enable-injection-points \
			--with-libedit-preferred \
			--with-libxml \
			--with-libxslt \
			--with-icu \
			--with-ssl=openssl && \
		make && \
		make install

RUN		cd postgres/contrib && \
		make && \
		make install

RUN		cd $USER_HOME/postgres/contrib && \
		git clone https://github.com/Hornetlabs/synchdb.git --branch ${SYNCHDB_BRANCH}

WORKDIR $USER_HOME/postgres/contrib/synchdb
RUN		make oracle_parser && make install_oracle_parser
RUN		make WITH_OLR=1 build_dbz
RUN		make WITH_OLR=1
RUN		make WITH_OLR=1 install
RUN		make WITH_OLR=1 install_dbz
#RUN		make oracle_parser && make install_oracle_parser
#RUN		USE_PGXS=1 make WITH_OLR=1 build_dbz PG_CONFIG=/usr/lib/postgresql/${POSTGRES_VERSION}/bin/pg_config
#RUN		USE_PGXS=1 make WITH_OLR=1 PG_CONFIG=/usr/lib/postgresql/${POSTGRES_VERSION}/bin/pg_config
#RUN		USE_PGXS=1 make WITH_OLR=1 install PG_CONFIG=/usr/lib/postgresql/${POSTGRES_VERSION}/bin/pg_config
#RUN		USE_PGXS=1 make WITH_OLR=1 install_dbz PG_CONFIG=/usr/lib/postgresql/${POSTGRES_VERSION}/bin/pg_config

ENV 	PATH="/usr/lib/postgresql/${POSTGRES_VERSION}/bin:${PATH}"
COPY 	init-synchdb.sh /usr/local/bin
COPY	jmx_prometheus_javaagent-1.3.0.jar $USER_HOME
COPY	jmxexport.conf $USER_HOME
RUN		chmod +x /usr/local/bin/init-synchdb.sh

RUN		mkdir -p /var/run/postgresql \
		&& chown $USERNAME:$USERNAME /var/run/postgresql \
		&& chmod 755 /var/run/postgresql

RUN		echo "/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64/lib/server" >> /etc/ld.so.conf.d/x86_64-linux-gnu.conf \
		&& ldconfig

USER    $USERNAME
WORKDIR $USER_HOME

ENTRYPOINT ["/usr/local/bin/init-synchdb.sh"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]
CMD ["bash"]

