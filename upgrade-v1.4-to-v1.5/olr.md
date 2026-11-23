# Oracle / OpenLogReplicator: upgrade from v1.4 to v1.5

Use this procedure for connectors that have completed their full snapshot and capture Oracle CDC through OpenLogReplicator (OLR). The upgrade keeps target tables, mappings, credentials, and the original `scn/c_scn/c_idx` position, then continues from that position.

Follow the [automatic guide](automatic-upgrade.md) for the common upgrade. For manual execution, use [manual steps](manual-upgrade.md) and resume with `no_data`. The SynchDB release changes from v1.4 to v1.5; its PostgreSQL extension catalog changes from `1.0` to `1.1`.

## Phase 1: confirm that capture can stop and resume

**Check the existing PostgreSQL target, Oracle source, and OLR service.**

1. Confirm that the original full snapshot is complete, CDC is healthy, and target data has caught up.
2. Record the connector name, target database, Oracle instance/container, business owner, captured tables, mappings, and OLR address/port.
3. Keep the original platform and PostgreSQL major version. Do not change CPU byte order during this upgrade.
4. Freeze captured-table DDL. Prefer stopping application writes and letting capture catch up. Otherwise retain all redo and archives required across downtime and recovery.
5. Preserve OLR checkpoints, dictionary, and configuration. Do not run old and new consumers against the same OLR stream simultaneously.

Save target status:

```sql
SELECT name, pid, stage, state, err, last_dbz_offset
FROM synchdb_state_view;
```

Have the Oracle administrator check the source identity and redo threads:

```sql
SELECT name, dbid, log_mode, resetlogs_change#, current_scn
FROM v$database;

SELECT thread#, status, enabled
FROM v$thread
ORDER BY thread#;

SELECT COUNT(DISTINCT thread#) AS redo_threads FROM v$log;
```

**Proceed when:** source identity is unchanged, original target data is complete, logs cover the saved position, and the source meets the single-redo-thread recovery requirement. Resolve an unfinished snapshot, damaged position, or missing logs before upgrading; do not raise the SCN to skip the problem.

## Phase 2: preserve complete recovery materials

**Prepare on the target PostgreSQL and OLR hosts.**

Before the downtime window, reserve cold-backup space and prepare matching v1.5 programs. Include OLR support, JAR and dependencies, the Oracle parser built for the target PostgreSQL major version, and extension SQL.

| Material | What must be retained |
| --- | --- |
| Target PGDATA and external data | Business tables, extension configuration, all `pg_synchdb/` files, and external WAL/tablespaces |
| Programs and configuration | C library, JAR/dependencies, Oracle parser, PostgreSQL configuration, and service environment |
| OLR recovery materials | OLR configuration, checkpoints, dictionary, and required source redo/archives |

After the old connector stops normally, these final files must remain under PGDATA:

| File | Requirement |
| --- | --- |
| `pg_synchdb/olr_<connector>_<target_database>_offsets.dat` | Exactly 24 bytes containing the original platform's `scn/c_scn/c_idx`; preserve all three values |
| `pg_synchdb/olr_<connector>_<target_database>_schemahistory.dat` | Exactly one byte, `t`, indicating the full snapshot completed |

`openlog_replicator_d_*` files belong to the snapshot path; they do not replace the native `olr_*` position. Back up the whole `pg_synchdb/` directory. Do not select only one SCN or copy fragments while capture is writing.

Existing ordinary credentials remain usable. No keyring generation, password re-entry, connector recreation, or target rebuild is required.

## Phase 3: check the OLR stream and JDBC verification connection

Keep one exact, uppercase Oracle owner and the existing business-table capture set. Do not expand business scope as part of the upgrade. Check the required OLR protocol fields below; do not overwrite the rest of the service configuration with this fragment:

```json
{
  "format.message": 2,
  "format.schema": 7,
  "format.scn-type": 3,
  "flags.include": 4096
}
```

Source verification also uses an Oracle JDBC connection. Confirm network access and privileges to read source identity/log views and operate the fence table. This verification connection currently does not support TLS; confirm that the deployment meets this condition before proceeding.

The OLR stream must include the internal `SYNCHDB_OLR_FENCE` table under the existing business owner. This table verifies the source and is not replicated to the target. Keep the original business filters and include this internal table as well. The Oracle DBA should confirm access to `V$DATABASE`, `V$LOG`, and `V$ARCHIVED_LOG`, plus creation or writing of the fence table.

If OLR configuration needs changing, the source administrator must apply it using the existing service procedure and confirm readiness, preserving checkpoints and dictionary. The target upgrade script does not reconfigure or restart the OLR service.

Check the table in the actual business PDB or non-CDB source; replace `APP`:

```sql
SELECT owner, table_name, dependencies
FROM all_tables
WHERE owner = 'APP' AND table_name = 'SYNCHDB_OLR_FENCE';
```

Verification creates the table automatically when the account has the required schema privileges. If it is absent and the capture account cannot create it, the owner can create it in advance and grant SELECT/INSERT. Replace `APP` and `OLR_READER`. Do not drop an existing table; check its exact columns and `DEPENDENCIES=ENABLED` first:

```sql
CREATE TABLE APP.SYNCHDB_OLR_FENCE (
    TOKEN VARCHAR2(36) PRIMARY KEY,
    SOURCE_DBID NUMBER(20) NOT NULL,
    RESETLOGS_SCN NUMBER(20) NOT NULL,
    CON_ID NUMBER(10) NOT NULL
) ROWDEPENDENCIES;

GRANT SELECT, INSERT ON APP.SYNCHDB_OLR_FENCE TO OLR_READER;
```

Verification buffers replayed events. `synchdb.olr_read_buffer_size` defaults to 128 MB and accepts 8-512 MB. Catch up before shutdown to reduce backlog. If adjustment is necessary, budget against actual event volume and available memory; do not reduce backlog by changing the SCN.

**Proceed when:** protocol, JDBC access, source identity, required logs, new programs, and cold-backup space are ready.

## Phase 4: run the upgrade

**Run on the target PostgreSQL host.**

Prepare `upgrade.json` using the [automatic guide](automatic-upgrade.md). Keep the old PostgreSQL instance running for discovery. Set actual absolute paths in one shell. `PYTHON` must include `psycopg2`. Programs invoked with `sudo` use the same interpreter and configuration; the script runs database connections and `pg_ctl` as the PGDATA owner.

```sh
REPO_ROOT=/absolute/path/to/synchdb
MIGRATION_DIR="$REPO_ROOT/upgrade-v1.4-to-v1.5"
PYTHON=/absolute/path/to/venv/bin/python3
UPGRADE_CONFIG=/private/upgrade.json

sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG"
```

Confirm the intended instance and OLR connectors appear without blocking errors. During the upgrade window:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply
```

The script stops old capture and waits for exit, stops PostgreSQL, makes a cold backup, replaces programs, starts maintenance mode, updates the catalogs, validates/imports the old three-part position, then starts normally and restores previously running connectors using `no_data`. Original files and target data remain.

**Proceed when:** the command exits successfully and prints `Upgrade complete` with a report path. Do not START an already restored connector. Previously paused/stopped connectors remain stopped.

With the manual procedure, position import still uses `migrate-offsets`. Do not rename files, insert position rows manually, or manufacture a snapshot-complete marker.

## Phase 5: check positions, state, and data

**Run on the upgraded PostgreSQL target database.** Replace `olrsync`:

```sql
SELECT name, desired_state, actual_state, last_operation, last_sqlstate, last_error
FROM synchdb_control_view
WHERE name = 'olrsync';

SELECT name, pid, stage, state, err, last_dbz_offset
FROM synchdb_state_view
WHERE name = 'olrsync';

-- Read the registered position as an administrator.
SELECT c.name, a.generation, a.origin, p.scn, p.c_scn, p.c_idx
FROM synchdb_conninfo c
JOIN synchdb_artifacts a USING (connector_id)
JOIN synchdb_olr_positions p USING (database_oid, connector_id, generation)
WHERE c.name = 'olrsync' AND a.active;
```

Accept the upgrade only after:

1. Intended running connectors enter CDC without identity, missing-log, or persistent apply errors.
2. Original tables, rows, and mappings remain without a new full snapshot. The imported three-part position is registered and advances with committed work.
3. Downtime changes have caught up, with primary-key/field reconciliation and checks for duplicate side effects.
4. Controlled INSERT, UPDATE, and DELETE on an approved captured table produce the expected target changes.
5. A normal target restart still resumes capture. Check automatic recovery with SynchDB preload; otherwise explicitly START using `no_data` as described in [manual steps](manual-upgrade.md).

Then reopen application writes and DDL. Import retains existing data and recovery position; it does not repair missing changes from an earlier snapshot-to-CDC handoff or incorrect historical values.

## Phase 6: recover from a failed stage

Keep the existing work directory and configuration, fix the cause, then resume:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply --resume
```

| Symptom | Action |
| --- | --- |
| Position length is not 24 bytes, values are out of range, or completion state is not `t` | Preserve evidence and restore files from a matching clean-shutdown backup. Do not fabricate a position or completion marker. |
| Ambiguous ownership or colliding paths | Check the complete instance inventory and original names. Restore the correct backup rather than renaming files. |
| Source identity/incarnation mismatch | Check the instance, resetlogs identity, container, and owner. Do not overwrite identity records to force continuation. |
| Required redo is gone | Restore matching logs or plan a complete data recovery. Do not advance the offset to the current SCN. |
| OLR or JDBC verification connection fails | Check privileges, network, protocol, matching JAR, and other consumers. |
| `OLR source verification exceeded the configured replay buffer` | Check backlog and memory. If necessary, adjust the buffer within its limits and resume the same work directory; do not change the SCN. |
| Capture-scope check fails | Confirm original connector position, `no_data`, and unchanged scope. Read the operation receipt before retrying. |
| Duplicate key or missing target row on UPDATE/DELETE | Reconcile original target data, position, and mappings; do not hide the error by skipping a record. |

After v1.5 capture starts, rollback must reconcile the target backup, old position, OLR checkpoints, and source logs together. Restoring only the old C library or offset is insufficient.
