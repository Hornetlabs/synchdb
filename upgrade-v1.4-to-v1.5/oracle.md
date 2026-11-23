# Oracle / LogMiner: upgrade from v1.4 to v1.5

This procedure keeps the existing PostgreSQL target tables, connector configuration, credentials, Oracle offsets, and schema history. It resumes CDC from the saved position without taking another full snapshot. The PostgreSQL extension catalog version changes from `1.0` to `1.1`; these catalog versions differ from the SynchDB release versions.

Use the [automatic upgrade guide](automatic-upgrade.md) for the common procedure. For Oracle23ai CDB/PDB connections, also read [Oracle23ai](oracle23ai.md). For Oracle embedded Infinispan, complete the [transaction cache checks](infinispan.md).

## Phase 1: confirm that the existing position can be resumed

**Run these checks against the existing PostgreSQL target and Oracle source.**

Before the upgrade:

- The original full snapshot must be complete and the target data must be usable.
- Keep the same Oracle instance, service, CDB/PDB, account, business owner, captured tables, and mappings.
- Retain all redo, archived logs, and dictionary information required from the saved position through the upgrade and catch-up window.
- Freeze DDL on captured tables. Prefer stopping application writes and waiting for the existing connector to catch up.
- Keep supplemental logging and LogMiner privileges enabled.

On the PostgreSQL database containing the connector, save the current status:

```sql
SELECT name, pid, stage, state, err, last_dbz_offset
FROM synchdb_state_view;
```

Record which connectors should run, remain paused, or remain stopped after the upgrade. Resolve an unfinished snapshot, connector error, or data mismatch before proceeding.

On Oracle, have the source DBA check:

```sql
SELECT name, log_mode, supplemental_log_data_min, force_logging, current_scn
FROM v$database;

-- Replace APP with the actual business owner.
SELECT owner, table_name, log_group_type, always
FROM dba_log_groups
WHERE owner = 'APP'
ORDER BY table_name, log_group_type;

SELECT thread#, sequence#, first_change#, next_change#, deleted, status, name
FROM v$archived_log
WHERE name IS NOT NULL
ORDER BY thread#, sequence#;
```

**Proceed when:** the existing connector is healthy, target data has caught up, and the DBA confirms that the logs required by the saved offset remain available. `current_scn` is an observation, not a replacement offset. A nonempty archive listing alone does not prove that every required log exists.

## Phase 2: prepare files and the upgrade window

**Work on the target PostgreSQL host.**

1. Prepare a matching v1.5 C library, JAR, Java dependencies, and extension SQL in a separate installation tree, as described in the automatic guide. Do not overwrite the running v1.4 installation.
2. Preserve the existing Oracle connection information and target mappings. Do not change accounts, services, owners, or capture scope during the upgrade.
3. Arrange source log retention and freeze captured-table DDL for the entire upgrade and catch-up window. Stop application writes at the application boundary; do not mechanically switch the source database to read-only mode.
4. Reserve space for a complete cold backup of PGDATA, external WAL/tablespaces, configuration, programs, and the entire `pg_synchdb/` directory.

The original Oracle files are under PGDATA:

```text
pg_synchdb/oracle_<connector>_<target_database>_offsets.dat
pg_synchdb/oracle_<connector>_<target_database>_schemahistory.dat
```

Do not rename these files, empty schema history, or set the offset to the current SCN. If Infinispan is enabled, preserve its matching cache directory as well.

Existing ordinary credentials remain usable. No keyring generation, password re-entry, connector recreation, or target-table rebuild is required.

## Phase 3: review the plan and run the upgrade

**Run on the target PostgreSQL host. Keep the old PostgreSQL instance running until the script stops it.**

Use the [automatic guide](automatic-upgrade.md) to create `upgrade.json`. In one shell, set the following variables to actual absolute paths. `PYTHON` must contain `psycopg2`. `MIGRATION_DIR` points to this `upgrade-v1.4-to-v1.5/` directory, containing the manuals, scripts, and SQL. For a standalone copy, set it directly to the package's absolute path.

```sh
REPO_ROOT=/absolute/path/to/synchdb
MIGRATION_DIR="$REPO_ROOT/upgrade-v1.4-to-v1.5"
PYTHON=/absolute/path/to/venv/bin/python3
UPGRADE_CONFIG=/private/upgrade.json

sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG"
```

Confirm that the plan identifies the correct PGDATA and all connectors, with no blocking errors. This command does not stop the service. Use the same interpreter, configuration, and execution identity for all subsequent commands. When run as root, the script runs PostgreSQL commands and database connections as the PGDATA owner.

During the upgrade window, execute:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply
```

The script stops capture and PostgreSQL, creates a cold backup, installs matching programs, starts maintenance mode, updates the extension catalogs, imports the original offset/history, and restores normal operation. Oracle resumes using `no_data`. Previously paused or stopped connectors remain stopped.

**Proceed when:** the command exits successfully and prints `Upgrade complete` with the report path. On error or timeout, follow Phase 5; do not create another work directory and start over.

For a manual upgrade, follow [manual steps](manual-upgrade.md) and use `no_data` to resume Oracle. Do not interleave automatic and manual procedures.

## Phase 4: validate before reopening application writes

**Run SQL in the upgraded PostgreSQL target database.** Replace `orasync` with the original connector name:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'synchdb';

SELECT name, desired_state, actual_state, last_operation, last_sqlstate, last_error
FROM synchdb_control_view
WHERE name = 'orasync';

SELECT name, pid, stage, state, err, last_dbz_offset
FROM synchdb_state_view
WHERE name = 'orasync';

SELECT * FROM synchdb_snapstats WHERE name = 'orasync';
```

Check each item:

1. The extension version is `1.1`. Connectors intended to run have resumed without persistent errors.
2. Original tables, historical rows, and mappings remain in place, without an unexpected new full snapshot.
3. Compare downtime changes by primary key and business field. Check timestamp precision, empty strings, NULLs, and large values separately.
4. On an approved captured table, perform controlled INSERT, UPDATE, and DELETE operations. Confirm each target value and expected application side effect.
5. During the maintenance window, restart the target PostgreSQL normally and verify another source change. With SynchDB preload enabled, check automatic recovery. Without preload, explicitly START the original running connectors using `no_data`, following [manual steps](manual-upgrade.md).
6. Preserve the planned paused/stopped states. Reopen application writes and DDL only after acceptance.

Do not submit another START for a connector already restored by the automatic flow. A successful operation receipt, equal row counts, or zero snapshot statistics does not replace field-by-field reconciliation. `no_data` neither fills an empty target nor repairs incorrect historical values.

## Phase 5: failure handling

Keep the original configuration, work directory, and all position files. Read the failed stage and error, correct the specific cause, then continue:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply --resume
```

| Symptom | Action |
| --- | --- |
| No `1.0 -> 1.1` update path | Check that the matching upgrade SQL is installed in the current PostgreSQL extension directory. Resolve preflight errors before continuing. |
| `ORA-01017` | Check the original service, CDB/PDB, account, and matching JDBC driver. Confirm that connection details have not changed before considering credential repair. |
| Ambiguous or colliding old file paths | Use the complete instance ownership inventory and recover matching backups. Do not rename files to bypass the check. |
| Missing archives, unavailable SCN, or dictionary mismatch | Restore the required logs or dictionary conditions. Adding a target column does not fix source mining; do not skip the SCN. |
| Unavailable LOB UPDATE or `0A000` | Stop the affected connector and investigate the source event and target data. Do not write a missing LOB as an ordinary value or ignore the error. |
| Incorrect historical timestamp or other value | The upgrade does not recalculate old rows. Stop affected writes, repair from the source by primary key, and reconcile before resuming. |
| `42501` | Check API, schema, target-table, and function privileges for the role that submits START. |

Rollback requires matching PGDATA, programs, external files, offsets, and source logs. Once v1.5 has captured changes, stop capture and inspect source progress before restoring anything. Replacing only the old `.so` or offset is not a complete rollback.
