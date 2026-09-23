# Extension SQL sources

Edit the SQL files in this directory. The root `synchdb--1.0.sql` is a generated
installation script, built by `make` and installed by `make install`. It is not
maintained separately or checked into version control.

`SQL_SOURCES` in the root Makefile lists the fragments in installation order.
The order preserves the original script's definitions; the compatibility
wrappers are appended after their implementations. Keep each function and its
`COMMENT ON FUNCTION` together. When adding a fragment, add it to `SQL_SOURCES`
explicitly instead of relying on directory or wildcard ordering.

| File | Responsibility |
| --- | --- |
| `core.sql` | C function declarations, configuration tables, and status views |
| `fdw/prepare.sql` | Snapshot table lists, FDW servers, and user mappings |
| `fdw/postgres.sql` | PostgreSQL metadata and WAL LSN foreign tables |
| `fdw/mysql.sql` | MySQL metadata and binlog position foreign tables |
| `fdw/sqlserver.sql` | SQL Server metadata and CDC LSN foreign tables |
| `fdw/oracle.sql` | Oracle metadata and SCN foreign tables |
| `fdw/metadata.sql` | Materialize metadata for all connector types |
| `fdw/stage.sql` | Create staging foreign tables for all connector types |
| `fdw/schema.sql` | Materialize destination schemas and migrate primary keys |
| `fdw/column_mappings.sql` | Column name and data type mappings |
| `fdw/data.sql` | Data migration and transforms, with and without subtransactions |
| `fdw/table_mappings.sql` | Table renames and moves |
| `fdw/finalize.sql` | Snapshot cleanup |
| `fdw/snapshot.sql` | Initial snapshot workflow |
| `fdw/schema_sync.sql` | Schema-only workflow |
| `fdw/types.sql` | JDBC type mappings |
| `fdw/compat.sql` | Compatibility wrappers for historical function names |

To regenerate only the installation script in a PostgreSQL source tree:

```sh
make synchdb--1.0.sql
```

For a standalone PGXS build:

```sh
make USE_PGXS=1 PG_CONFIG=/path/to/pg_config synchdb--1.0.sql
```

Use `synchdb_materialize_metadata` and `synchdb_create_stage_fts` in new code.
The old `synchdb_materialize_ora_metadata` and `synchdb_create_ora_stage_fts`
names remain callable through wrappers with the same argument names, types,
and defaults. Historical default schema and server names are intentionally
unchanged, including on the new entry points.

Installing the generated file does not change functions in databases where the
extension is already installed. Changes for an existing released version still
need an extension upgrade script; splitting source files does not replace that
mechanism.
