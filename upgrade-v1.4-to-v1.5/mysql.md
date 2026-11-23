# MySQL source: upgrade from SynchDB v1.4 to v1.5

Use this procedure to preserve the existing connector, PostgreSQL target tables, credentials, mappings, and MySQL replication position. The original initial snapshot must be complete, and the saved offset must match the target data.

The SynchDB release changes from **v1.4 to v1.5**. The extension version recorded in PostgreSQL changes from **`1.0` to `1.1`**.

Follow the automatic procedure below, together with the [automatic upgrade guide](automatic-upgrade.md). If you need to execute individual maintenance steps, use the [manual upgrade guide](manual-upgrade.md) instead. Do not mix the two procedures during one execution.

For a complete isolated exercise on this machine, including source checkout, compilation, v1.4 synchronization, manual upgrade, and data checks, use [Local MySQL rehearsal](mysql-local-rehearsal.md).

## Stage 1: check MySQL and the existing connector

**Run on the MySQL source, as the source database administrator:**

```sql
SELECT VERSION(), @@log_bin, @@binlog_format, @@binlog_row_image;
SHOW BINARY LOGS;
```

Confirm that binary logging is enabled, its format and row image meet the existing capture requirements, and the log containing the saved position is still available. Retain enough binlog to cover the upgrade, catch-up, acceptance checks, and any recovery window.

**Run in the PostgreSQL target database where SynchDB is installed.** Replace `mysqlsync` with the existing connector name:

```sql
SELECT name, pid, stage, state, err
FROM synchdb_state_view
WHERE name = 'mysqlsync';
```

Before proceeding, confirm that:

- The initial snapshot has completed, the target has caught up, and there are no unresolved capture errors.
- You have recorded which connectors were running, paused, or stopped.
- The source database, selected tables, mappings, and target tables will remain unchanged during the upgrade.
- The existing offset and schema history files are present, and you have planned a complete cold backup of the deployment.

If required logs are missing or the target is incomplete, first restore a consistent recovery state. Do not advance the offset to bypass the problem.

## Stage 2: prepare the upgrade window

1. Prepare the complete v1.5 program installation in a separate directory. Do not overwrite the running v1.4 installation yet.
2. Prepare `upgrade.json`, the administrator connection, and backup space as described in the [automatic upgrade guide](automatic-upgrade.md).
3. Freeze DDL on captured tables. Preferably pause application writes and wait for the existing connector to catch up. If writes must continue, retain and reconcile all changes produced during the upgrade window.
4. Configure a libpq service and `.pgpass` readable by the PostgreSQL operating-system account. Do not place passwords on the command line.
5. Record expected business data and configure the optional `checks` queries in `upgrade.json`.

Existing credentials are reused. You do not need a keyring, password re-entry, or a replacement connector.

**Run the following on the target PostgreSQL host in the same shell.** Replace all paths and service names:

```sh
REPO_ROOT=/absolute/path/to/synchdb
MIGRATION_DIR="$REPO_ROOT/upgrade-v1.4-to-v1.5"
PYTHON=/absolute/path/to/venv/bin/python3
UPGRADE_CONFIG=/private/upgrade.json
MIG_DSN='service=synchdb_target'
```

`PYTHON` must contain `psycopg2`. `MIG_DSN` is for single-connector commands and must connect to the actual target database where SynchDB is installed; it is not necessarily the administrator connection's default database.

## Stage 3: review the plan, then apply the upgrade

**Run on the target PostgreSQL host.** System installations normally require root to install program files. The script runs PostgreSQL commands and database connections as the PostgreSQL operating-system account.

Keep the old PostgreSQL instance running and review the read-only plan:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG"
```

Check the PGDATA, program and backup directories, connector restore states, and configuration changes. This command does not stop capture or PostgreSQL. Resolve every blocking error before continuing.

During the upgrade window, execute:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply
```

The script stops capture, stops PostgreSQL, creates the cold backup, installs the matching programs, starts maintenance mode, updates the extension, imports the original offset/history, and restores normal operation. It preserves the existing skipped-operation policy and sets the native snapshot thread count to `1`.

**Completion condition:** the command exits successfully and prints `Upgrade complete`. Originally paused or stopped connectors remain stopped.

If the execution fails, preserve the working directory and use the recovery instructions below. Do not create a new working directory to repeat the upgrade.

## Stage 4: verify continuation and business data

**Run in the PostgreSQL target database:**

```sql
SELECT extversion FROM pg_extension WHERE extname = 'synchdb';

SELECT name, desired_state, actual_state, last_operation,
       last_sqlstate, last_error
FROM synchdb_control_view
WHERE name = 'mysqlsync';

SELECT name, stage, state, err
FROM synchdb_state_view
WHERE name = 'mysqlsync';

-- Replace 123 with the actual last_operation value.
SELECT synchdb_operation(123, 1000);
```

The extension version must be `1.1`. A connector that should be running must resume CDC without a failed state or recurring error. Its operation receipt should be `SUCCEEDED`.

Then complete these checks:

1. Confirm that the original tables, mappings, and dependencies remain and that no unexpected full snapshot occurred.
2. Compare source and target by primary key and business column, including all changes accumulated while the target was stopped. Pay particular attention to decimal values, timestamps, empty strings, NULLs, and large fields.
3. With application approval, perform an identifiable INSERT, UPDATE, and DELETE on a captured verification table. Wait for and check each target change.
4. During the planned window, restart target PostgreSQL normally and verify another source write. If SynchDB is preloaded, check automatic recovery. Without preload, explicitly START only the connectors that should run.

After acceptance, resume application writes and DDL. Retain the cold backup, old programs, and execution directory through the recovery window. A successful receipt or matching row count alone is not a complete data check.

## Resume mode for manual maintenance

After the extension update and original offset/history import, MySQL uses `initial` to continue from its completed snapshot position:

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" control \
  --dsn "$MIG_DSN" --connector mysqlsync \
  --operation START --mode initial --timeout 180
```

Do not repeat START for a connector already restored by the automatic procedure. Do not use `always` or create an empty offset. Here, `initial` relies on the existing completed snapshot and valid offset; it does not request a new full copy.

## Failures and recovery

| Symptom | Action |
| --- | --- |
| Required binlog or schema history is missing | Keep capture stopped and restore matching logs and files, or arrange a separate complete data recovery. Do not edit the offset to skip ahead. |
| A Binlog Dump connection remains after the old STOP call | Wait for the old worker to exit and confirm normal target shutdown. Have the source DBA identify the connection; do not close unrelated connections in bulk. |
| Old files have conflicting owners or names | Check the full instance ownership inventory. Restore the correct files if one connector overwrote another's state; do not rename files to bypass the check. |
| Existing rows do not match after adding a required/defaulted column | Check source and target definitions, apply the required target DDL, and reconcile historical values. The upgrade does not backfill old rows. |
| A control command times out | Query the original operation ID. Timeout does not cancel the submitted request; do not submit START again. |

After fixing the reported cause, continue the same automatic execution:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply --resume
```

For rollback, follow the recovery section of the [manual upgrade guide](manual-upgrade.md). Restore a complete, matching deployment only after checking source-log availability. Replacing only the old `.so` or offset is not a safe rollback.
