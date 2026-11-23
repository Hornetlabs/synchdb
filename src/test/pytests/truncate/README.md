# TRUNCATE event regression tests

Run from the repository root with a matching PostgreSQL binary/header installation,
a C compiler, JDK, pytest and psycopg2. No running source, JVM, Docker or installed
SynchDB extension is required.
The OLR build additionally needs protobuf-c headers and libraries; these TRUNCATE
cases do not need the Oracle parser library.

```sh
SYNCHDB_TARGET_BIN=/path/to/postgres/bin python -m pytest -q src/test/pytests/truncate
SYNCHDB_TARGET_BIN=/path/to/postgres/bin SYNCHDB_TEST_WITH_OLR=1 python -m pytest -q src/test/pytests/truncate
```

For IvorySQL, also set `SYNCHDB_TEST_TARGET_FLAVOR=ivorysql` to pin PostgreSQL
identifier semantics. Some IvorySQL versions report only the PostgreSQL version
through `pg_config --version`, so CI supplies the flavor explicitly.

The fixture builds production objects and a private test module, starts a fresh
local PostgreSQL cluster, and stops it on exit. It never runs `make install`.
Both build variants are supported; run them sequentially because they share
production object files. When switching PostgreSQL major versions, clean the
objects or use a separate source checkout.

The module uses the production decoders, source SQL grammar, table binding,
target executor, GUC registration and statistics. Two explicit fixtures supply
an already accepted rule snapshot and a native OLR handoff boundary. The latter
uses production JSON shape/numeric validation, but does not establish a real
Oracle snapshot proof, apply source selection or send a transport acknowledgement.
The tests therefore establish event and target semantics, not an end-to-end
source durability or capture guarantee.

Coverage includes typed `op=t`, explicit TRUNCATE schema changes and DDL text,
metadata/SQL agreement, quoted case, long source names and mapped destinations,
multiple-statement rejection, Oracle storage clauses, target permissions and
capabilities, inheritance scope, foreign keys, identity continuity, triggers,
whole-statement rollback after a later event fails, skip policies and GUC aliases.
Native OLR also exercises TRUNCATE followed by insert/update/delete through the same table
mapping, including spaces, embedded quotes, dots and case-sensitive names.

Live source capture, source rollback, both row writers and worker failure/replay
are covered by `../synchdbtests/t/test_009_truncate.py` using the normal source
fixtures. PostgreSQL source multi-table TRUNCATE still becomes separate Debezium
events; the original table group and CASCADE/identity flags are not reconstructed.
SQL text carrying unsupported options is rejected before target binding.

The Java batch settlement tests run with `mvn test` in `src/backend/debezium`.
They cover callback ownership, tombstones and filtered events, rejected partial
acknowledgements, stop/target failure without source acknowledgement, and session
freezing after a source acknowledgement fails.

CI runs both event build variants against each freshly built target before
publishing its installation artifact. Source-client contracts run earlier in
`../source_helpers`; the live Oracle/OLR suite first checks SQL*Plus statement,
commit and rollback handling in `../synchdbtests/t/test_000_source_sql.py`.
Pass multiple ordinary SQL statements to `common.run_remote_statements()` as a
list, so they share a session and each has its own input line. Never split SQL
text on semicolons: quoted values and procedural scripts can contain them.
