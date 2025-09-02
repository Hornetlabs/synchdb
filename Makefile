# contrib/synchdb/Makefile

MODULE_big = synchdb

EXTENSION = synchdb
DATA = synchdb--1.0.sql
PGFILEDESC = "synchdb - allows logical replication with heterogeneous databases"

REGRESS = synchdb
REGRESS_OPTS = --inputdir=./src/test/regress --outputdir=./src/test/regress/results --load-extension=pgcrypto

# flag to build with native openlog replicator connector support
WITH_OLR ?= 0

OBJS = src/backend/synchdb/synchdb.o \
       src/backend/converter/format_converter.o \
       src/backend/converter/debezium_event_handler.o \
       src/backend/executor/replication_agent.o

DBZ_ENGINE_PATH = src/backend/debezium

# Dynamically set JDK paths
JAVA_PATH := $(shell which java)
JDK_HOME_PATH := $(shell readlink -f $(JAVA_PATH) | sed 's:/bin/java::')
JDK_INCLUDE_PATH := $(JDK_HOME_PATH)/include

# default protobuf-c path (for OLR build)
PROTOBUF_C_INCLUDE_DIR ?= /usr/local/include
PROTOBUF_C_LIB_DIR ?= /usr/local/lib

# Detect the operating system
UNAME_S := $(shell uname -s)

# Set JDK_INCLUDE_PATH based on the operating system
ifeq ($(UNAME_S), Linux)
    JDK_INCLUDE_PATH_OS := $(JDK_INCLUDE_PATH)/linux
    $(info Detected OS: Linux)
else ifeq ($(UNAME_S), Darwin)
    JDK_INCLUDE_PATH_OS := $(JDK_INCLUDE_PATH)/darwin
    $(info Detected OS: Darwin)
else
    $(error Unsupported operating system: $(UNAME_S))
endif

JDK_LIB_PATH := $(JDK_HOME_PATH)/lib/server

PG_CFLAGS = -I$(JDK_INCLUDE_PATH) -I$(JDK_INCLUDE_PATH_OS) -I./src/include -I${PROTOBUF_C_INCLUDE_DIR}
PG_CPPFLAGS = -I$(JDK_INCLUDE_PATH) -I$(JDK_INCLUDE_PATH_OS) -I./src/include -I${PROTOBUF_C_INCLUDE_DIR}
PG_LDFLAGS = -L$(JDK_LIB_PATH) -ljvm


ifeq ($(WITH_OLR),1)
OBJS += src/backend/converter/olr_event_handler.o \
		src/backend/olr/OraProtoBuf.pb-c.o \
		src/backend/utils/netio_utils.o \
		src/backend/olr/olr_client.o

PG_LDFLAGS += -lprotobuf-c -L$(PROTOBUF_C_LIB_DIR)
PG_CFLAGS += -DWITH_OLR
PG_CPPFLAGS += -DWITH_OLR
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
PG_MAJOR := $(shell $(PG_CONFIG) --majorversion)
else
subdir = contrib/synchdb
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
PG_MAJOR := $(MAJORVERSION)
endif


check_protobufc:
	@echo "Checking protobuf-c installation"
	@if [ ! -d $(PROTOBUF_C_INCLUDE_DIR)/protobuf-c ]; then \
      echo "Error: protobuf-c include path $(PROTOBUF_C_INCLUDE_DIR) not found"; \
      echo "Hint: overwrite PROTOBUF_C_INCLUDE_DIR with correct path to protobuf-c include dir"; \
      exit 1; \
    fi
	@if [ ! -f $(PROTOBUF_C_LIB_DIR)/libprotobuf-c.so.1.0.0 ]; then \
      echo "Error:  $(PROTOBUF_C_LIB_DIR)/libprotobuf-c.so.1.0.0 not found"; \
      echo "Hint: overwrite PROTOBUF_C_LIB_DIR with correct path to /libprotobuf-c.so.1.0.0"; \
      exit 1; \
    fi

	@echo "protobuf-c Paths"
	@echo "$(PROTOBUF_C_INCLUDE_DIR)/protobuf-c"
	@echo "$(PROTOBUF_C_LIB_DIR)/libprotobuf-c.so.1.0.0"
	@echo "protobuf-c check passed"


# Target that checks JDK paths
check_jdk:
	@echo "Checking JDK environment"
	@if [ ! -d $(JDK_INCLUDE_PATH) ]; then \
	  echo "Error: JDK include path $(JDK_INCLUDE_PATH) not found"; \
	  exit 1; \
	fi
	@if [ ! -d $(JDK_INCLUDE_PATH_OS) ]; then \
	  echo "Error: JDK include path for OS $(JDK_INCLUDE_PATH_OS) not found"; \
	  exit 1; \
	fi
	@if [ ! -d $(JDK_LIB_PATH) ]; then \
	  echo "Error: JDK lib path $(JDK_LIB_PATH) not found"; \
	  exit 1; \
	fi

	@echo "JDK Paths"
	@echo "$(JDK_INCLUDE_PATH)"
	@echo "$(JDK_INCLUDE_PATH_OS)"
	@echo "$(JDK_LIB_PATH)"
	@echo "JDK check passed"

build_dbz:
	cd $(DBZ_ENGINE_PATH) && mvn clean install

clean_dbz:
	cd $(DBZ_ENGINE_PATH) && mvn clean

install_dbz:
	rm -rf $(pkglibdir)/dbz_engine
	install -d $(pkglibdir)/dbz_engine
	cp -rp $(DBZ_ENGINE_PATH)/target/* $(pkglibdir)/dbz_engine

oracle_parser:
	@echo "building against pgmajor ${PG_MAJOR}"
	 make -C src/backend/olr/oracle_parser${PG_MAJOR}

clean_oracle_parser:
	@echo "cleaning against pgmajor ${PG_MAJOR}"
	make clean -C src/backend/olr/oracle_parser${PG_MAJOR}

install_oracle_parser:
	@echo "installing against pgmajor ${PG_MAJOR}"
	make install -C src/backend/olr/oracle_parser${PG_MAJOR}

.PHONY: dbcheck mysqlcheck sqlservercheck oraclecheck dbcheck-tpcc mysqlcheck-tpcc sqlservercheck-tpcc oraclecheck-tpcc
dbcheck:
	@command -v pytest >/dev/null 2>&1 || { echo >&2 "❌ pytest not found in PATH."; exit 1; }
	@command -v docker >/dev/null 2>&1 || { echo >&2 "❌ docker not found in PATH."; exit 1; }
	@command -v docker-compose >/dev/null 2>&1 || command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 || { echo >&2 "❌ docker-compose not found in PATH"; exit 1; }
	@echo "Running tests against dbvendor=$(DB)"
	PYTHONPATH=./src/test/pytests/synchdbtests/ pytest -x -v -s --dbvendor=$(DB) --capture=tee-sys ./src/test/pytests/synchdbtests/
	rm -r .pytest_cache ./src/test/pytests/synchdbtests/__pycache__ ./src/test/pytests/synchdbtests/t/__pycache__

dbcheck-tpcc:
	@command -v pytest >/dev/null 2>&1 || { echo >&2 "❌ pytest not found in PATH."; exit 1; }
	@command -v docker >/dev/null 2>&1 || { echo >&2 "❌ docker not found in PATH."; exit 1; }
	@command -v docker-compose >/dev/null 2>&1 || command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 || { echo >&2 "❌ docker-compose not found in PATH"; exit 1; }
	@echo "Running hammerdb based tpcc tests against dbvendor=$(DB)"
	PYTHONPATH=./src/test/pytests/synchdbtests/ pytest -v -s --dbvendor=$(DB) --tpccmode=serial --capture=tee-sys ./src/test/pytests/hammerdb/
	rm -r .pytest_cache ./src/test/pytests/hammerdb/__pycache__

mysqlcheck:
	$(MAKE) dbcheck DB=mysql

sqlservercheck:
	$(MAKE) dbcheck DB=sqlserver

oraclecheck:
	$(MAKE) dbcheck DB=oracle

olrcheck:
	$(MAKE) dbcheck DB=olr

mysqlcheck-benchmark:
	$(MAKE) dbcheck-tpcc DB=mysql

sqlservercheck-benchmark:
	$(MAKE) dbcheck-tpcc DB=sqlserver

oraclecheck-benchmark:
	$(MAKE) dbcheck-tpcc DB=oracle


