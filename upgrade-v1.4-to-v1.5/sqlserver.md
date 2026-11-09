# SQL Server source: upgrade from SynchDB v1.4 to v1.5

Use this procedure to preserve existing connectors, PostgreSQL target tables, credentials, and SQL Server CDC positions. The original initial snapshot must be complete, and target data must match the saved offsets. The upgrade continues from those positions without a new full copy.

The SynchDB release changes from **v1.4 to v1.5**. The target extension version changes from **`1.0` to `1.1`**.

Follow the [automatic upgrade guide](automatic-upgrade.md) for the main procedure. If mapping or data repairs require individual maintenance steps, use the [manual upgrade guide](manual-upgrade.md) instead. Do not run both procedures concurrently.

## Stage 1: check source CDC

**Run in the actual SQL Server source database, as the source administrator:**

```sql
SELECT name, is_cdc_enabled, is_read_only
FROM sys.databases WHERE name = DB_NAME();

SELECT SCHEMA_NAME(schema_id) AS schema_name, name, is_tracked_by_cdc
FROM sys.tables WHERE is_ms_shipped = 0;

EXEC sys.sp_cdc_help_jobs;
EXEC sys.sp_cdc_help_change_data_capture;
```

Confirm that:

- CDC is enabled for the source database and every captured table.
- SQL Server Agent and the capture/cleanup jobs are operating as required by the existing deployment.
- The source remains `READ_WRITE`, with `is_read_only=0`. Pause application writes at the application boundary; do not make the entire database read-only.
- CDC retention covers the upgrade, catch-up, acceptance, and recovery windows. Do not disable and re-enable CDC or change capture instances.

Check the available range for the actual capture instance; replace `dbo_events`:

```sql
SELECT sys.fn_cdc_get_min_lsn(N'dbo_events') AS min_available_lsn,
       sys.fn_cdc_get_max_lsn() AS max_captured_lsn;
```

Confirm that the saved position can still be read. Non-NULL results alone do not prove that the old position remains available.

**Run in the PostgreSQL target database.** Replace `sqlsync` with the existing connector name:

```sql
SELECT name, pid, stage, state, err
FROM synchdb_state_view
WHERE name = 'sqlsync';
```

Confirm that the initial snapshot has completed and capture has no unresolved error. Record each connector's running, paused, or stopped state, along with its selected source tables, mappings, and target tables.

## Stage 2: prepare the upgrade window and data checks

1. Preferably pause application writes and freeze DDL on captured tables while leaving Agent and CDC operational. Wait for capture to process the final application changes and for the target to catch up.
2. Confirm that the original offset and schema history files are complete. Prepare enough space for a cold backup of PGDATA, external directories, old programs, and configuration.
3. Prepare the complete v1.5 programs in a separate directory. Configure `upgrade.json`, administrator authentication, and business `checks` using the [automatic upgrade guide](automatic-upgrade.md). Do not overwrite programs used by a running instance.
4. Keep the existing credentials, connectors, and business tables. No keyring or password re-entry is required.

A v1.4 deployment configured with `snapshot_engine=fdw` but actually capturing SQL Server through Debezium can retain that setting and its matching offset/history files. You do not need to recreate schema history manually.

Check these fields before upgrading; existing incorrect values are not repaired by replacing programs:

| Field or structure | Required check |
| --- | --- |
| `time(7)` and `datetime2(7)` | Compare historical fractional values by primary key. Establish the required business rounding rule when correcting microsecond values. |
| `varchar(max)` and `nvarchar(max)` | Check target types. If a default mapping produces an oversized varchar, prepare an explicit text mapping and matching target structure. |
| NULLs, empty strings, and decimals | Compare business column values, not just row counts. |

If mappings, structure, or historical values need repair, arrange a maintenance window after old capture has stopped and before new capture starts. Use the [manual procedure](manual-upgrade.md) for that window; do not modify mappings or target tables while the automatic script is executing.

Example MAX-column repair, after the extension update with capture still stopped; replace every source and target identifier:

```sql
-- Run in the PostgreSQL target database.
SELECT synchdb_add_objmap(
  'sqlsync'::name, 'datatype'::name,
  'SourceDb.dbo.BusinessTable.Note'::text, 'text|0'::text);

ALTER TABLE app.business_table
  ALTER COLUMN note TYPE text USING note::text;
```

**Completion condition:** target data and offsets match, required type repairs have a defined plan, source retention is sufficient, and backup space, programs, and authentication are ready.

## Stage 3: review the plan and perform the upgrade

**Run on the target PostgreSQL host in the same shell.** Replace all paths and service names:

```sh
REPO_ROOT=/absolute/path/to/synchdb
MIGRATION_DIR="$REPO_ROOT/upgrade-v1.4-to-v1.5"
PYTHON=/absolute/path/to/venv/bin/python3
UPGRADE_CONFIG=/private/upgrade.json
MIG_DSN='service=synchdb_target'
```

`PYTHON` must contain `psycopg2`. `MIG_DSN` must connect to the target database where SynchDB is installed. System installations normally use root for program installation; the script runs PostgreSQL commands and database connections as the PostgreSQL operating-system account.

Keep old target PostgreSQL running and review the read-only plan:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG"
```

Check the PGDATA, program and backup directories, connector restore states, and configuration changes. Resolve every blocking error, then execute during the upgrade window:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply
```

The script stops capture and PostgreSQL, creates a cold backup, installs matching programs, updates the extension in maintenance mode, imports the original offset/history, and restores normal operation. SQL Server continues with `no_data`; originally paused or stopped connectors remain stopped.

**Completion condition:** the command exits successfully and prints `Upgrade complete`. On failure, preserve the working directory and continue using the recovery instructions below, rather than creating another execution.

## Stage 4: verify before reopening application writes

**Run in the PostgreSQL target database:**

```sql
SELECT extversion FROM pg_extension WHERE extname = 'synchdb';

SELECT name, desired_state, actual_state, last_operation,
       last_sqlstate, last_error
FROM synchdb_control_view
WHERE name = 'sqlsync';

SELECT name, stage, state, err
FROM synchdb_state_view
WHERE name = 'sqlsync';

-- Replace 123 with the actual last_operation value.
SELECT synchdb_operation(123, 1000);
```

Expect extension version `1.1`, CDC running for the intended connectors, no failed or recurring error, and a `SUCCEEDED` receipt. Then:

1. Confirm that original target tables, mappings, and business dependencies remain, without an unexpected full snapshot.
2. Compare all relevant business columns by primary key and account for changes accumulated during downtime. Check timestamp precision, MAX columns, NULLs, empty strings, and decimals.
3. With application approval, perform an identifiable INSERT, UPDATE, and DELETE on a captured verification table and confirm each target result. Do not use TRUNCATE as a substitute for these checks.
4. Restart target PostgreSQL normally within the upgrade window, then verify another event. With SynchDB preload enabled, check automatic recovery; without preload, explicitly START the intended connectors.
5. Resume application writes and DDL only after acceptance. Retain the complete backup, old programs, and upgrade logs through the recovery window.

A successful receipt confirms the control operation, not that all business data is correct.

## Resume mode for manual maintenance

After updating the extension, applying necessary repairs, and importing the original offset/history, start only the connectors that should run:

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" control \
  --dsn "$MIG_DSN" --connector sqlsync \
  --operation START --mode no_data --timeout 180
```

Do not repeat START for a connector already restored automatically. `no_data` does not fill historical gaps. Do not switch an upgrade with complete target data to `always`.

## Failures and recovery

| Symptom | Action |
| --- | --- |
| No `1.0` to `1.1` update path | Check that the matching upgrade SQL is installed in the extension directory used by the running PostgreSQL installation. |
| Source is read-only or capture job is stopped | Keep application writes paused, but restore READ_WRITE and the services required by CDC. Confirm that required CDC records remain available before continuing. |
| Position is outside CDC retention | Keep capture stopped. Restore matching source records and target state, or arrange a separate complete data recovery. |
| `23505`, or UPDATE/DELETE cannot find the original row | Check target completeness, mappings, and capture overlap. Do not edit offsets or skip failed rows to conceal the discrepancy. |
| Conflicting ownership of old files | Use the complete instance inventory. Restore matching files if paths collided; renaming does not repair an overwritten file. |
| `42501` permission error | Check API and target-object privileges for the START caller, plus source-account permissions. Do not grant direct write access to control ledgers as a workaround. |
| START times out | Query the original operation ID. Timeout does not cancel the request; do not submit it again. |

After fixing the reported cause, continue the same automatic execution:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply --resume
```

Before rollback, confirm that the saved positions remain within CDC retention. Follow the recovery section of the [manual upgrade guide](manual-upgrade.md) to restore complete matching materials. Do not restore only old program files or an old offset.
