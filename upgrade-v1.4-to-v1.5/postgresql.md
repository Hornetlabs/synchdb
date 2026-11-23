# PostgreSQL source: upgrade from SynchDB v1.4 to v1.5

Use this procedure to preserve existing connectors, target tables, saved offsets, and the original source replication slots and publications. Source PostgreSQL and target PostgreSQL are separate instances; each command below identifies where it runs. This procedure does not upgrade either PostgreSQL major version.

The SynchDB release changes from **v1.4 to v1.5**. The target extension version changes from **`1.0` to `1.1`**.

Use the [automatic upgrade guide](automatic-upgrade.md) for the main procedure. For individual maintenance commands, use the [manual upgrade guide](manual-upgrade.md) instead; do not mix the two procedures during one execution.

## Stage 1: check the source slot, publication, and target data

**Run on source PostgreSQL, as the source administrator:**

```sql
SELECT slot_name, plugin, database, active, restart_lsn,
       confirmed_flush_lsn, wal_status
FROM pg_replication_slots;

SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate
FROM pg_publication;

SELECT pubname, schemaname, tablename FROM pg_publication_tables;
```

Record the exact slot, publication, and captured tables used by the connector. Confirm that required WAL is available. Preserve the original resources: do not delete and recreate them under the same names, and do not let another consumer advance the slot. Retain sufficient WAL and monitor source disk space.

**Run in the PostgreSQL target database.** Replace `pgsync` with the existing connector name:

```sql
SELECT name, pid, stage, state, err
FROM synchdb_state_view
WHERE name = 'pgsync';
```

Before proceeding, confirm that the initial snapshot is complete, capture has no unresolved error, and the target has caught up. Record every connector's running, paused, or stopped state. Compare existing values by primary key, including text, empty strings, NULLs, timestamps, and large fields. The upgrade does not repair existing incorrect values.

If the source slot has advanced beyond the locally saved safe offset, or required WAL is gone, first establish matching recovery conditions. Do not continue by lowering the LSN or replacing the slot.

## Stage 2: arrange the write pause and row identity

1. Preferably pause source application writes, freeze DDL on captured tables, and wait for the target to catch up.
2. For tables that require complete row images, set `REPLICA IDENTITY FULL` after the old capture worker's final stop and before producing new source events. Starting the old worker again may alter row identity, so do not restart it after making this change.
3. If the tables already use FULL, preserve that setting. Check existing WAL and historical large-field values; changing row identity cannot repair older events.

Check each relevant source table, replacing the example schema and table:

```sql
-- Run on source PostgreSQL.
SELECT n.nspname AS schema_name, c.relname AS table_name, c.relreplident
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relname = 'your_table';
```

`relreplident='f'` means FULL. If a table needs FULL but does not have it, first stop that old connector on the target:

```sql
-- Run on the target; wait and confirm that the old worker has exited.
SELECT synchdb_stop_engine_bgw('pgsync');
SELECT name, pid, stage, state, err
FROM synchdb_state_view
WHERE name = 'pgsync';
```

Then change the source table:

```sql
ALTER TABLE public.your_table REPLICA IDENTITY FULL;
```

A connector stopped manually in this branch will remain stopped during automatic upgrade. Record whether it should run afterward and restore it explicitly in Stage 5. Other connectors can be stopped and restored by the automatic procedure.

**Completion condition:** required row identity is ready, the old worker will not change it back, and the write/DDL pause remains in effect. FULL does not supply TOAST values missing from earlier WAL.

## Stage 3: prepare the configuration and review the plan

Prepare matching v1.5 programs in a separate directory, backup space, administrator authentication, and `upgrade.json` as described in the [automatic upgrade guide](automatic-upgrade.md). Keep the existing credentials, connectors, business tables, slots, and publications.

**Run on the target PostgreSQL host in the same shell.** Replace all paths and service names:

```sh
REPO_ROOT=/absolute/path/to/synchdb
MIGRATION_DIR="$REPO_ROOT/upgrade-v1.4-to-v1.5"
PYTHON=/absolute/path/to/venv/bin/python3
UPGRADE_CONFIG=/private/upgrade.json
MIG_DSN='service=synchdb_target'
```

`PYTHON` must contain `psycopg2`. `MIG_DSN` must connect to the actual target database where SynchDB is installed.

Keep old target PostgreSQL running and review the read-only plan:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG"
```

Check PGDATA, installation and backup directories, connector restore states, and configuration changes. If Stage 2 stopped a connector manually, verify that the plan keeps it stopped and retain your separate record of its original running state.

Resolve blocking errors before proceeding. Do not delete the slot, rename databases, or edit LSNs to pass a check.

## Stage 4: execute the automatic upgrade

**Run on the target PostgreSQL host:**

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply
```

The script stops old capture and PostgreSQL, creates a cold backup, installs matching programs, updates the extension in maintenance mode, imports the original offset, and restores normal operation. It preserves the original slot and publication identities.

**Completion condition:** the command exits successfully and prints `Upgrade complete`. Connectors running at discovery resume; paused or stopped connectors remain stopped.

On failure, preserve the working directory and follow the recovery instructions below.

## Stage 5: restore selected connectors and verify data

If Stage 2 manually stopped a connector that should run, restore it after the directory update and offset import:

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" control \
  --dsn "$MIG_DSN" --connector pgsync \
  --operation START --mode no_data --timeout 180
```

Only run this for connectors that should be active. Do not repeat START for a connector already restored automatically. `no_data` continues from existing state; it does not supply missing historical data.

**Run in the PostgreSQL target database:**

```sql
SELECT extversion FROM pg_extension WHERE extname = 'synchdb';

SELECT name, desired_state, actual_state, last_operation,
       last_sqlstate, last_error
FROM synchdb_control_view
WHERE name = 'pgsync';

SELECT name, stage, state, err
FROM synchdb_state_view
WHERE name = 'pgsync';

-- Replace 123 with the actual last_operation value.
SELECT synchdb_operation(123, 1000);
```

Expect extension version `1.1`, CDC running for the intended connectors, no failed or recurring error, and a `SUCCEEDED` operation receipt. Then:

1. Recheck source slots and publications. Their names, plugin, database, and table coverage must still match the original deployment.
2. Compare source and target by primary key and business column, including changes accumulated during downtime. Check numeric values, arrays, text, and external large fields.
3. With application approval, perform an identifiable INSERT, UPDATE, and DELETE on a captured verification table. Confirm each target result.
4. Restart target PostgreSQL normally during the upgrade window and verify another new event. With SynchDB preload enabled, check automatic recovery; without preload, explicitly START the intended connectors.
5. Confirm that the original target tables, mappings, and dependencies remain and that no unexpected full snapshot occurred. Then resume application writes and DDL.

## Failures and recovery

| Symptom | Action |
| --- | --- |
| `slot has advanced beyond the persisted engine offset` | Stop capture and check whether the slot matches the saved target and offset. Restore matching source/target state or arrange complete data recovery; do not lower the LSN or recreate an empty slot. |
| `unavailable` / `0A000` for a TOAST value | The event lacks the complete value. Setting FULL afterward cannot repair that WAL. Preserve the state and arrange source/target recovery; do not write placeholder strings. |
| Missing target table or column | Check source definitions and mappings, then prepare matching target structure. Do not discard the old offset to bypass the problem. |
| Row counts match but column values differ | Reconcile historical empty strings, text NULLs, large fields, and conversions by primary key. The upgrade does not recalculate existing rows. |
| START times out | Query the original operation receipt. Timeout does not cancel the request; do not submit START again. |

After fixing the reported cause, continue the same automatic execution:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply --resume
```

**Restoring an old target backup does not rewind the source slot.** After v1.5 starts consuming, do not restore only old PGDATA and start the old connector. Check source resources and log positions before rollback, following the recovery section of the [manual upgrade guide](manual-upgrade.md).
