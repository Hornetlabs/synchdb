# Automatic Upgrade from SynchDB v1.4 to v1.5

This procedure retains the existing databases, connectors, target tables, mappings, credentials, and synchronization positions. It upgrades SynchDB without changing the PostgreSQL major version.

**Keep the old PostgreSQL instance running until the upgrade script stops it. Do not install new libraries over the running deployment or convert position files by hand.**

## Procedure overview

| Stage | Your action | Proceed when |
| --- | --- | --- |
| 1. Prepare | Obtain the software and tools; check administrator access | Files are complete and the old instance is reachable |
| 2. Configure | Describe the old instance, new software, service, and backup location | Paths, accounts, and service ownership are correct |
| 3. Review | Run the read-only plan | The plan succeeds and lists the intended databases and connectors |
| 4. Enter the window | Freeze source DDL, preferably stop application writes, and catch up | Source and target data match and logs are retained |
| 5. Apply | Run the upgrade and wait | The command exits successfully with `Upgrade complete` |
| 6. Accept | Check state, data, new changes, and restart recovery | All required checks pass before application writes resume |

Use [Manual upgrade](manual-upgrade.md) if you need to perform the maintenance steps individually. Do not mix the two procedures during an execution.

## Stage 1: Prepare software, tools, and access

### 1.1 Check the source requirements

Read the applicable guide first: [MySQL](mysql.md), [SQL Server](sqlserver.md), [PostgreSQL](postgresql.md), [Oracle](oracle.md), [Oracle23ai](oracle23ai.md), [OLR](olr.md), or [Infinispan](infinispan.md).

The old initial snapshot must be complete. Preserve the source identity, database, capture scope, connector names, and matching target data. Retain source logs for the entire shutdown, catch-up, acceptance, and recovery window.

### 1.2 Prepare a separate v1.5 installation tree

Prepare matching new software in a separate directory, such as `/opt/synchdb-new`. Do not overwrite the current installation yet. The upgrade script expects this layout:

```text
/opt/synchdb-new/
  bin/postgres
  lib/synchdb.so
  lib/dbz_engine/
    dbz-engine-1.1.0.jar
    lib/                         # matching Java dependencies
  share/extension/
    synchdb.control
    synchdb--1.1.sql
```

The manuals and tools are supplied together in `upgrade-v1.4-to-v1.5/`. Keep its required executable files in their existing layout:

```text
upgrade-v1.4-to-v1.5/
  scripts/
    synchdb_upgrade.py
    synchdb_migrate.py
    upgrade_settings.py
    verify_bundle.py
    inventory.sql
    upgrade_preflight.sql
    baseline-v1.4.json
  sql/
    synchdb--1.0--1.1.sql
    manifest.json
```

This package contains the runtime tools, catalog checks, and update SQL required by this procedure. You do not need the old `migration/` directory, a full source checkout, a SQL generator, or test material. An OLR deployment additionally needs the new Oracle parser library compiled for the current PostgreSQL major version.

The new C library, Java package, dependencies, and SQL must belong to the same upgrade package. `bin/postgres` in the prepared tree must use the same major version as the existing server. The script leaves the existing PostgreSQL executable in place and installs the SynchDB artifacts.

If the new Java files use another location in this tree, set `java_subdir` in Stage 2.

### 1.3 Prepare the execution environment

Run on the target PostgreSQL host. You need Python 3.9 or later with `psycopg2`, plus the existing installation's PostgreSQL tools and matching `pg_config`.

Set these variables in the shell used for the following commands. Replace every example path:

```sh
REPO_ROOT=/absolute/path/to/synchdb
MIGRATION_DIR="$REPO_ROOT/upgrade-v1.4-to-v1.5"
PYTHON=/absolute/path/to/venv/bin/python3
UPGRADE_CONFIG=/srv/synchdb-upgrade/upgrade.json
PG_BIN=/opt/postgresql/18/bin
```

`MIGRATION_DIR` is the directory containing this manual, `scripts/`, and `sql/`; `MIGRATION_DIR/scripts/synchdb_upgrade.py` must exist. For a standalone copy, set `MIGRATION_DIR` directly to its absolute path, for example `/opt/upgrade-v1.4-to-v1.5`; `REPO_ROOT` is only a convenience for repository-based examples. Use the absolute path to the Python interpreter that contains `psycopg2`, including when using `sudo`.

Check the dependency and migration package:

```sh
"$PYTHON" -c 'import psycopg2; print("psycopg2 ready")'
"$PYTHON" "$MIGRATION_DIR/scripts/verify_bundle.py"
```

**Completion condition:** both commands exit with status `0`, with no missing-file or hash errors. Resolve failures before scheduling shutdown.

### 1.4 Configure database access

The administrator connection must use a PostgreSQL superuser that can connect to the instance's databases. Use libpq services and `.pgpass` rather than a password in the command line or upgrade JSON.

For example, in the PostgreSQL service account's `.pg_service.conf`:

```ini
[synchdb_upgrade]
host=127.0.0.1
port=5432
dbname=postgres
user=postgres
```

Add matching authentication entries in that account's `.pgpass`, with mode `0600`. Entries must cover the databases the upgrade needs to connect to.

Replace the operating-system account `postgres` as appropriate, and check the connection:

```sh
sudo -H -u postgres "$PG_BIN/psql" -X -w 'service=synchdb_upgrade' \
  -c "SELECT current_user, current_database(); SHOW data_directory; SHOW config_file;"
```

**Completion condition:** the connection succeeds and `data_directory` is the intended target PGDATA.

Normally run the upgrade script as root so it can install program files and preserve backups. The script runs PostgreSQL commands and database connections as the PGDATA owner. Do not start PostgreSQL directly as root. If one non-root account owns both the data and installation directories, that account may run the commands without `sudo`.

## Stage 2: Create the upgrade configuration

Create the configuration file in a private location. Replace the values below:

```json
{
  "pgdata": "/var/lib/postgresql/18/main",
  "pg_bin": "/opt/postgresql/18/bin",
  "pg_config": "/opt/postgresql/18/bin/pg_config",
  "new_prefix": "/opt/synchdb-new",
  "java_subdir": "lib/dbz_engine",
  "work_dir": "/var/backups/synchdb/upgrade-001",
  "dsn": "service=synchdb_upgrade",
  "service": "postgresql@18-main.service",
  "timeout": 300,
  "checks": []
}
```

| Setting | What to supply |
| --- | --- |
| `pgdata` | Absolute path to the currently running target's PGDATA |
| `pg_bin` | Directory containing that installation's `pg_ctl`, `psql`, `postgres`, and related tools |
| `pg_config` | Matching `pg_config`; package installations may place it in `/usr/bin` |
| `new_prefix` | The separate new program tree prepared in Stage 1 |
| `java_subdir` | Java directory relative to `new_prefix`; default `lib/dbz_engine` |
| `work_dir` | A new private execution directory for backups, state, and logs; it must not exist on first execution and must be outside PGDATA and both program installations |
| `dsn` | The administrator connection checked in Stage 1 |
| `service` | The actual systemd unit that owns this PostgreSQL process; remove this field if systemd is not used. This is different from the libpq service in `dsn` |
| `timeout` | Timeout in seconds for operations and waits; default `300` |
| `checks` | Optional business-data checks; an empty array means you must validate data separately |

Create the configuration's parent directory if necessary, but **do not create `work_dir`**. Protect the configuration file, for example with mode `0600`.

### Optional business-data checks

A check SQL file must return one row and one column containing JSON. For example, `/srv/synchdb-upgrade/check-orders.sql`:

```sql
SELECT jsonb_build_object('rows', count(*)) FROM app.orders;
```

Replace the empty `checks` array in your configuration with:

```json
"checks": [
  {
    "database": "targetdb",
    "sql_file": "/srv/synchdb-upgrade/check-orders.sql",
    "expected": {"rows": 1000}
  }
]
```

Use the actual target database, an absolute SQL file path, and a verified expected value. The script runs the query in a read-only transaction and waits for the result to match. This example checks only the row count; compare business fields separately. Do not use a stale fixed expectation if source writes continue.

## Stage 3: Review the plan

Leave the old PostgreSQL instance running and execute:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG"
```

This command only inspects and checks the deployment. It does not stop collection or PostgreSQL, and it does not create `work_dir`.

Confirm the output:

- `action` is `PLAN_ONLY`.
- PGDATA, databases, and connector names are correct.
- The restore plan matches the current running, paused, and stopped connectors.
- `installs` points to the existing installation, with new files coming from the separate new tree.
- `configuration_changes` retains the intended skipped-operation policy and sets snapshot threads to `1`.
- File ownership, caches, and external data locations have no unresolved conflicts.

Resolve errors and rerun the plan before continuing. Do not bypass checks, edit positions, or rename connectors to hide file conflicts.

Common blockers include an incorrect instance or service, another running cluster sharing the program installation, insufficient installation privileges, a command-line snapshot-thread override above `1`, role/database overrides of the relevant settings, `queue <= batch`, or a JVM heap below `1`.

## Stage 4: Enter the maintenance window

1. Freeze DDL on captured source tables.
2. Preferably stop application writes at the application boundary and wait for old collection to catch up. Keep source databases and required CDC, capture, and OLR services available.
3. Compare source and target rows by key. Record important field values, row counts, and the intended running/paused/stopped connector list.
4. Confirm source-log retention and enough space for PGDATA, external data, and old program backups.
5. Keep the old PostgreSQL instance running. Normally let the script stop the collectors. If a source-specific procedure required you to stop a connector earlier, retain its original intended state and restore it separately afterward; the script will keep an already stopped connector stopped.

If application writes must continue, retain and reconcile all changes produced during the upgrade window. A pre-shutdown row count alone is insufficient.

**Completion condition:** source prerequisites, data checks, upgrade materials, and the reviewed plan are ready.

## Stage 5: Apply the upgrade

Run with the same configuration and execution account:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply
```

Do not issue additional START/STOP, installation, or position-import commands while the script runs.

| Reported stage | Action performed |
| --- | --- |
| `stop_connectors` | Stop active old collectors and wait for their workers to exit |
| `stop_server` | Stop target PostgreSQL normally, through its systemd unit when configured |
| `cold_backup` | Back up PGDATA, recognized external WAL/tablespaces, configuration, and program files to be replaced |
| `install` | Install matching C, SQL, Java, and dependency files |
| `maintenance_start` | Start using actual configuration paths and connection settings, temporarily excluding SynchDB from preload |
| `compatibility_settings` | Persist the skipped-operation policy and set snapshot threads to `1` |
| `upgrade_catalogs` | Update the extension catalogs in each target database |
| `import_checkpoints` | Import original positions and required Oracle caches or OLR coordinates |
| `maintenance_stop` | Stop the maintenance instance |
| `normal_start` | Restore the original normal startup method and preload list |
| `restore_running` | Restore only connectors inventoried as running and wait for their operation receipts |
| `verify` | Run configured business checks and save results |

Paused and stopped connectors remain stopped. Existing credentials and original position files are retained. Do not generate a keyring, recreate tables or connectors, or rename position files.

**Completion condition:** the command exits with status `0` and prints `Upgrade complete. Report: .../state.json`. A notice that no business check was configured means you must still perform the data checks in Stage 6.

### Configuration changes made by the script

v1.4 defaults to skipping TRUNCATE, whereas an unconfigured v1.5 installation does not. The script preserves the existing policy. An explicitly configured `synchdb.dbz_skipped_operations` takes precedence; otherwise it preserves the old spelling `synchdb.dbz_skipped_oeprations`, including its default `t`.

The script writes the chosen policy and snapshot-thread value `1` through `ALTER SYSTEM` after the cold backup. These settings go into `PGDATA/postgresql.auto.conf` and are checked after normal startup, before START requests are submitted. The original file is in the backup.

This file overrides ordinary `postgresql.conf` values. Future changes should use `ALTER SYSTEM SET/RESET` and reload, followed by a connector restart when required for accepted run settings. A service command-line override or relevant role/database override must be resolved when reported by preflight.

### Where backups and records are stored

All locations below are under `work_dir`:

| Path | Contents |
| --- | --- |
| `backup/pgdata/` | Old PGDATA, positions, and original automatic configuration |
| `backup/external-*` | Recognized external WAL/tablespace data; original locations are recorded in `state.json` |
| `backup/config/` | Main configuration, HBA, and ident snapshots |
| `backup/artifact-*`, `backup/java/` | Replaced old program files and Java directory; destination mappings are in `state.json` |
| `claims.json` | Complete instance ownership inventory for old files |
| `state.json` | Completed/current stages, operation IDs, restore plan, and checks |
| `execution.jsonl` | Commands, output, stages, and errors in execution order |

Retain the whole directory. Do not copy an individual old offset over a new deployment that has already resumed.

## Stage 6: Validate before reopening application writes

### 6.1 Check each target database

Connect to a database containing SynchDB. Replace the service account and database as appropriate:

```sh
sudo -H -u postgres "$PG_BIN/psql" -X -w \
  'service=synchdb_upgrade dbname=targetdb'
```

Run:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'synchdb';

SELECT name, desired_state, actual_state, last_operation, last_sqlstate, last_error
FROM synchdb_control_view
ORDER BY name;

SELECT name, pid, stage, state, err
FROM synchdb_state_view
ORDER BY name;

SHOW synchdb.dbz_snapshot_thread_num;
SHOW synchdb.dbz_skipped_operations;
```

Expect extension version `1.1`, intended running connectors reaching `RUNNING`, snapshot threads `1`, and the planned skipped-operation policy. Originally paused/stopped connectors must not start unexpectedly. Wait and observe a connector in `STARTING`; resolve `FAILED` or persistent retries before acceptance.

### 6.2 Reconcile the data

1. Confirm that original target tables, mappings, local objects, and application dependencies remain available.
2. Compare important source and target fields by primary key, including NULL/empty strings, timestamps, precision, and large values.
3. Confirm that changes accumulated during downtime have arrived without an unexpected new initial snapshot.
4. In a permitted captured table, make identifiable INSERT, UPDATE, and DELETE changes and verify each target result and relevant side effects.
5. Check source-specific resources, such as the original PostgreSQL slot/publication, Oracle log availability, or SQL Server capture jobs.

A successful operation receipt, `RUNNING` state, or equal row count alone does not prove complete data correctness. The upgrade does not repair existing incorrect rows.

### 6.3 Confirm restart recovery

Arrange a normal target PostgreSQL restart during the maintenance window, preserving its configuration and Java environment, then verify another source change.

- With SynchDB preloaded, confirm automatic recovery of the intended running connectors.
- Without SynchDB preload, explicitly START the intended connectors after restart using the [command reference](commands.md): MySQL uses `initial`; the other sources use `no_data`.
- Keep originally paused/stopped connectors stopped.

After acceptance, resume application writes and end the DDL freeze according to your source-specific guide. Retain the backup, logs, and source recovery resources during the observation period.

## Failure handling

### Find the failed stage

Inspect `current_stage`, `failure`, `completed`, and `cdc_may_have_started` in `work_dir/state.json`, together with `execution.jsonl`.

| Failure point | Action |
| --- | --- |
| Read-only plan | Correct paths, service ownership, access, or configuration and rerun the plan; no shutdown has been performed |
| Shutdown, backup, installation | Preserve the state, check service status, disk space, and permissions; do not start a mixed old/new installation |
| Maintenance startup, catalog update, import | Keep collection stopped; correct the reported configuration, dependency, or matching-file issue |
| Normal startup or collection restore | Check PostgreSQL, source connectivity, and the original operation receipt; do not skip positions to bypass errors |
| Business check | Reconcile actual data and expected values; collection may already have advanced source positions |

### Resume the same execution

After correcting the cause, keep the original configuration, work directory, and staged artifacts. Run:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply --resume
```

The script continues unfinished stages and reconciles imported state and previously submitted operations. Do not delete the work directory or choose a fresh one for a retry. If an operation is still pending, query the original ID as described in [Command reference](commands.md); a timeout does not cancel it.

### If rollback is required

Stop new collection and confirm source consumers have exited. Restore a complete, matching set of old PGDATA, program files, external data, and configuration. Do not simply replace the shared library, remove new positions, or change the extension version manually.

After collection has advanced source positions, restoring the old target alone may not allow safe continuation. In particular, a source PostgreSQL replication slot does not move backward with a target restore. Preserve the state and evaluate source and target recovery together; see [Manual upgrade](manual-upgrade.md).
