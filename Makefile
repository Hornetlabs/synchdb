# contrib/synchdb/Makefile

MODULE_big = synchdb

EXTENSION = synchdb
DATA = synchdb--1.0.sql
PGFILEDESC = "synchdb - allows logical replication with heterogeneous databases"

REGRESS = synchdb
REGRESS_OPTS = --load-extension=pgcrypto

OBJS = synchdb.o \
       format_converter.o \
       replication_agent.o

DBZ_ENGINE_PATH = dbz-engine

# Dynamically set JDK paths
JAVA_PATH := $(shell which java)
JDK_HOME_PATH := $(shell readlink -f $(JAVA_PATH) | sed 's:/bin/java::')
JDK_INCLUDE_PATH := $(JDK_HOME_PATH)/include

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

PG_CFLAGS = -I$(JDK_INCLUDE_PATH) -I$(JDK_INCLUDE_PATH_OS)
PG_CPPFLAGS = -I$(JDK_INCLUDE_PATH) -I$(JDK_INCLUDE_PATH_OS)
PG_LDFLAGS = -L$(JDK_LIB_PATH) -ljvm

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/synchdb
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

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
	rm -rf $(libdir)/dbz_engine
	install -d $(libdir)/dbz_engine
	cp -rp $(DBZ_ENGINE_PATH)/target/* $(libdir)/dbz_engine

.PHONY: dbcheck mysqlcheck sqlservercheck oraclecheck
dbcheck:
	@command -v pytest >/dev/null 2>&1 || { echo >&2 "❌ pytest not found in PATH."; exit 1; }
	@command -v docker >/dev/null 2>&1 || { echo >&2 "❌ docker not found in PATH."; exit 1; }
	@command -v docker-compose >/dev/null 2>&1 || command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 || { echo >&2 "❌ docker-compose not found in PATH"; exit 1; }
	@echo "Running tests against dbvendor=$(DB)"
	PYTHONPATH=./test/synchdb_tests/ pytest -v -s --dbvendor=$(DB) --capture=tee-sys ./test/synchdb_tests/
	rm -r .pytest_cache ./test/synchdb_tests/__pycache__ ./test/synchdb_tests/t/__pycache__

mysqlcheck:
	$(MAKE) dbcheck DB=mysql

sqlservercheck:
	$(MAKE) dbcheck DB=sqlserver

oraclecheck:
	$(MAKE) dbcheck DB=oracle
