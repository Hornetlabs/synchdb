# TRUNCATE event regression tests

These tests compile the production converters, Debezium/native OLR parsers and
SPI executor into a test-only PostgreSQL module. The module replaces connector
shared memory and checkpoint transport with local values. It accepts synthetic
batches, catches failures in a PostgreSQL subtransaction, and exposes counters.
Real PostgreSQL tables, indexes, identities, foreign keys and rollback are used.
The module is never installed as part of SynchDB and does not add production SQL
entry points.

Requirements: PostgreSQL server binaries and headers, a C compiler, pytest and
psycopg2. Run from the repository root, using the same PostgreSQL installation
for binaries and headers:

```sh
SYNCHDB_TARGET_BIN=/path/to/postgres/bin python -m pytest -q src/test/pytests/truncate
SYNCHDB_TARGET_BIN=/path/to/postgres/bin SYNCHDB_TEST_WITH_OLR=1 python -m pytest -q src/test/pytests/truncate
```

Each run creates and stops a temporary cluster. These tests cover rowless events,
source metadata validation, quoted identifiers, table mappings and letter case,
search_path isolation, filtering, success/failure counters, batch rollback,
foreign keys, identity continuity, unchanged attribute mappings, and following
DML through both executors where supported. They do not start a JVM or assert
source-side capture or transport acknowledgements. The worker/client code owns
batch acknowledgement and committed-SCN restoration.

Native OLR checks also cover filtering Debezium's internal `LOG_MINING_FLUSH`
DML: valid events advance the checkpoint without target writes or event counts,
subsequent user events still run, and malformed events or failed batches remain
unconfirmed.

Live source capture and GUC propagation are tested in
`../synchdbtests/t/test_007_truncate.py`, using the repository's Docker fixtures:

```sh
make mysqlcheck TARGET=pg18 TARGET_BIN=/path/to/postgres/bin
make postgrescheck TARGET=pg18 TARGET_BIN=/path/to/postgres/bin
```

For a focused live run:

```sh
PYTHONPATH=src/test/pytests/synchdbtests python -m pytest -q src/test/pytests/synchdbtests/t/test_007_truncate.py --source=mysql --target=pg18 --target-bin=/path/to/postgres/bin
```

Build both variants with forced recompilation: make does not track WITH_OLR as
an object-file dependency.

```sh
make -B USE_PGXS=1 PG_CONFIG=/path/to/postgres/bin/pg_config WITH_OLR=0
make -B USE_PGXS=1 PG_CONFIG=/path/to/postgres/bin/pg_config WITH_OLR=1
```

If host port 5432 is occupied, set `SYNCHDB_TEST_POSTGRES_PORT=55439` for
the live PostgreSQL source tests. The source container and connector use the
same override; the default remains 5432.
