# Manual upgrade from SynchDB v1.4 to v1.5

Use this procedure to preserve the existing connector configuration, target tables, credentials, and source positions. Keep the PostgreSQL major version, target databases, source identity, and capture scope unchanged.

The SynchDB release changes from **v1.4 to v1.5**. Its PostgreSQL extension SQL version changes from **`1.0` to `1.1`**; the update command therefore uses `UPDATE TO '1.1'`.

The [automatic upgrade](automatic-upgrade.md) is recommended. Choose one procedure for an upgrade; do not run manual steps while the automatic command is executing.

## Stages and completion conditions

| Stage | Target PostgreSQL state | Continue only when |
| --- | --- | --- |
| 1. Prepare and inventory | v1.4 running | Programs, inventory, and recovery plan are ready |
| 2. Stop capture and PostgreSQL | Stopped | Old workers and the target server have exited |
| 3. Make a cold backup | Stopped | Data, external directories, configuration, and old programs are backed up |
| 4. Install v1.5 programs | Stopped | C, Java, dependencies, and SQL have been updated together |
| 5. Start maintenance and update catalogs | Maintenance only | Each target database reports extension version `1.1` |
| 6. Import original positions | Maintenance only | Every connector that must resume has an imported position |
| 7. Start normally and restore capture | v1.5 running | Previously running connectors have successful START receipts |
| 8. Verify and reopen application writes | v1.5 running | Data, new changes, and restart recovery are correct |

## Stage 1: Prepare and inventory

### 1.1 Check prerequisites

Before stopping anything:

- Complete the old initial snapshot and reconcile the existing target with the source.
- Confirm the extension is installed in `public` and the upgrade preflight accepts the deployment.
- Prepare matching v1.5 C, Java, dependencies, control, installation SQL, and upgrade SQL in a separate directory. OLR also needs an Oracle parser library matching the target PostgreSQL major version.
- Retain source logs throughout shutdown, catch-up, and verification. Read the relevant source instructions from the [guide index](README.md).
- Schedule an application write pause and freeze DDL on captured source tables. If writes must continue, retain and reconcile all changes made during the upgrade.
- Arrange sufficient space for a complete cold backup, including external PostgreSQL data and old programs.

Do not overwrite the files used by the running v1.4 instance yet.

### 1.2 Set paths and connection details

Use Bash and replace every example value. Run `pg_ctl` as the PostgreSQL service OS account. Database operations require a PostgreSQL superuser. Commands prefixed with `sudo` require an administrator who can install program files.

`MIGRATION_DIR` points to this `upgrade-v1.4-to-v1.5/` directory, which contains the manuals, `scripts/`, and `sql/`. For a standalone copy, set it directly to that directory's absolute path; no full checkout is required.

```sh
REPO_ROOT=/absolute/path/to/synchdb
MIGRATION_DIR="$REPO_ROOT/upgrade-v1.4-to-v1.5"
PYTHON=/absolute/path/to/venv/bin/python3
PG_CONFIG=/path/to/current/postgresql/bin/pg_config
PGDATA=/path/to/current/pgdata
NEW_PREFIX=/path/to/prepared/v1.5/postgresql
BACKUP=/private/synchdb-v1.4-to-v1.5-backup
TARGET_DSN='service=synchdb_maintenance'
CONNECTOR=existing_connector

PGBIN="$("$PG_CONFIG" --bindir)"
PKGLIB="$("$PG_CONFIG" --pkglibdir)"
EXTENSION_DIR="$("$PG_CONFIG" --sharedir)/extension"
NEW_JAVA="$NEW_PREFIX/lib/dbz_engine"

# Replace with the directory used by the old PostgreSQL service.
# If that service sets DBZ_ENGINE_DIR, use its actual resolved directory.
JAVA_DIR="$PKGLIB/dbz_engine"

# Actual systemd unit, or empty for a deployment not managed by systemd.
PG_SERVICE=''

# Original config_file, port, socket, and other required startup overrides.
START_OPTIONS=''

# Original preload list with only synchdb removed.
MAINT_PRELOAD=''

# MySQL: initial. SQL Server, Oracle, PostgreSQL, and OLR: no_data.
RESUME_MODE=initial
```

`PYTHON` must have `psycopg2` installed. Put database authentication in libpq service files and `.pgpass`; do not put passwords in the command line. `TARGET_DSN` must connect to a database containing the extension.

`PG_CONFIG` belongs to the current running installation, not the new staging directory. `BACKUP` must be a new directory outside PGDATA, program directories, and external data directories.

For an external main configuration and a command-line port, preserve the original options, for example:

```sh
START_OPTIONS="-c config_file='/etc/postgresql/my cluster/postgresql.conf' -p 55432"
```

Save the complete original service command and Java environment. Keep the systemd unit stopped during maintenance; use it again for normal startup.

### 1.3 Save inventory and original running states

Create the private backup directory and check the supplied tools:

```sh
install -d -m 700 "$BACKUP"
"$PYTHON" "$MIGRATION_DIR/scripts/verify_bundle.py"

"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" inspect \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --output "$BACKUP/old-inventory.json"

"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" preflight \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --output "$BACKUP/old-preflight.json"
```

For each database containing SynchDB, repeat the inventory with that database's DSN and distinct output filenames. Save these query results in each target database:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'synchdb';
SELECT name, isactive FROM synchdb_conninfo ORDER BY name;
SELECT name, pid, stage, state, err FROM synchdb_state_view ORDER BY name;

SHOW data_directory;
SHOW config_file;
SHOW hba_file;
SHOW ident_file;
SHOW shared_preload_libraries;

SELECT current_setting('synchdb.dbz_skipped_operations', true) AS skipped_operations,
       current_setting('synchdb.dbz_skipped_oeprations', true) AS legacy_skipped_operations,
       current_setting('synchdb.dbz_snapshot_thread_num', true) AS snapshot_threads;
```

Record which connectors were running, paused, or stopped. Record the source replication resources, target structure, mappings, and required grants. The inventory omits secrets and is not a database backup.

### 1.4 Export the complete file ownership inventory

While v1.4 is still running, prepare `upgrade.json` as described in the [automatic guide](automatic-upgrade.md), then export the cluster-wide ownership inventory:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config /private/upgrade.json \
  --claims-output "$BACKUP/legacy-claims.json"

sudo chown --reference="$BACKUP" "$BACKUP/legacy-claims.json"
```

This command is read-only, but also checks installation permissions, so it normally runs with administrative privileges. The file is created with mode `0600`; the ownership change makes it readable by the OS account that owns the backup directory and will run the import. Ensure its parent directories are also accessible.

After export, do not change connector names, target database names, or cache types.

**Expected result:** All commands succeed, the inventory has no unresolved blockers, and the original running plan is recorded. Resolve errors before stopping the old instance.

## Stage 2: Stop capture, then stop PostgreSQL

### 2.1 Stop old workers

Pause application writes as planned and wait for the target to catch up. In every target database, stop each connector that has an old worker:

```sql
SELECT synchdb_stop_engine_bgw('existing_connector');
```

The v1.4 return value `0` does not prove the process has exited. Check activity:

```sql
SELECT name, isactive FROM synchdb_conninfo ORDER BY name;

SELECT pid, datname, backend_type
FROM pg_stat_activity
WHERE backend_type ILIKE '%synchdb%'
  AND backend_type NOT ILIKE '%launcher%';
```

Confirm the affected configuration rows are inactive, the saved worker PIDs have exited, and the corresponding source consumers have disconnected. Preserve the original plan for paused or stopped connectors; do not start them for the upgrade.

### 2.2 Stop the whole target instance

```sh
if [ -n "$PG_SERVICE" ]; then
  sudo systemctl stop "$PG_SERVICE"
else
  "$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop
fi

"$PGBIN/pg_ctl" -D "$PGDATA" status
```

**Expected result:** `pg_ctl status` reports that the server is not running. Its nonzero exit status in this state is expected. No other running instance may still use the program files that will be replaced.

## Stage 3: Make a complete cold backup

Keep PostgreSQL stopped. Use an account that can read all original files and preserve their ownership and permissions. The backup destinations below must not already exist.

```sh
(
  set -e
  test ! -e "$BACKUP/pgdata"
  cp -a "$PGDATA" "$BACKUP/pgdata"
  cp -a "$PKGLIB/synchdb.so" "$BACKUP/synchdb.so"
  cp -a "$EXTENSION_DIR" "$BACKUP/extension-files"
  cp -a "$JAVA_DIR" "$BACKUP/java"
)
```

For OLR, also back up the current Oracle parser library.

Back up these external items separately:

- The real WAL directory if `pg_wal` is a symbolic link.
- Every real tablespace directory referenced by `pg_tblspc`.
- The main configuration, HBA, ident, and external include files recorded in stage 1.
- Service definitions, Java environment, certificates, and other external runtime files.

Copying PGDATA preserves symbolic links but does not copy their external contents. For example:

```sh
install -d -m 700 "$BACKUP/external"
cp -a /data/pg_tablespace/orders "$BACKUP/external/orders-tablespace"
```

Record each original path, backup path, owner, and permission. Do not place a backup inside a directory being copied. Neither `pg_dump` nor the inventory JSON alone can replace the cold backup.

**Expected result:** Every copy succeeds, and the backup includes the original business data, positions, caches, external data, configuration, and matching old programs. A failed copy must be resolved before installation.

## Stage 4: Install the matching v1.5 programs

Keep PostgreSQL stopped. Run installation commands with the privileges required by the current program directories. Adjust source paths if the prepared package has a different layout.

```sh
(
  set -e
  sudo install -m 644 "$NEW_PREFIX/lib/synchdb.so" \
    "$PKGLIB/synchdb.so"
  sudo install -m 644 "$NEW_PREFIX/share/extension/synchdb.control" \
    "$EXTENSION_DIR/synchdb.control"
  sudo install -m 644 "$NEW_PREFIX/share/extension/synchdb--1.1.sql" \
    "$EXTENSION_DIR/synchdb--1.1.sql"
  sudo install -m 644 "$MIGRATION_DIR/sql/synchdb--1.0--1.1.sql" \
    "$EXTENSION_DIR/synchdb--1.0--1.1.sql"
)
```

Replace the whole Java installation, not just the main JAR. Confirm that `JAVA_DIR` is a dedicated Java directory and its preservation path does not exist:

```sh
(
  set -e
  test ! -e "$JAVA_DIR.before-upgrade"
  sudo mv "$JAVA_DIR" "$JAVA_DIR.before-upgrade"
  sudo cp -a "$NEW_JAVA" "$JAVA_DIR"
  sudo chmod -R a+rX "$JAVA_DIR"
)
```

Retain the original service's `DBZ_ENGINE_DIR` when it was configured. Otherwise use the current installation's `dbz_engine` directory. For OLR, install the matching Oracle parser now.

**Expected result:** C, the Java JAR and dependencies, control, and both SQL files are installed together and readable by the PostgreSQL service account. If any installation fails, keep the server stopped until the files are repaired.

## Stage 5: Start maintenance and update each database

### 5.1 Start without SynchDB preload

Run `pg_ctl` as the PostgreSQL service OS account. Set `MAINT_PRELOAD` to the original preload list with only `synchdb` removed; retain all other extensions. Preserve the original external configuration and connection options in `START_OPTIONS`.

```sh
"$PGBIN/pg_ctl" -D "$PGDATA" \
  -l "$BACKUP/maintenance.log" -w \
  -o "$START_OPTIONS -c shared_preload_libraries='$MAINT_PRELOAD'" start

"$PGBIN/psql" -X -v ON_ERROR_STOP=1 "$TARGET_DSN" \
  -c "SHOW shared_preload_libraries;"
```

Keep the original systemd unit stopped. Setting `synchdb_auto_launcher=false` is not a substitute for removing SynchDB preload during this stage.

### 5.2 Save compatible runtime settings

Set snapshot threads to `1`. Preserve the original skipped-operation policy recorded in stage 1. The following `t` continues to skip TRUNCATE; replace it if the original policy differs:

```sh
SKIPPED_OPERATIONS='t'

"$PGBIN/psql" -X -v ON_ERROR_STOP=1 \
  -v skipped="$SKIPPED_OPERATIONS" "$TARGET_DSN" <<'SQL'
ALTER SYSTEM SET synchdb.dbz_snapshot_thread_num = '1';
ALTER SYSTEM SET synchdb.dbz_skipped_operations = :'skipped';
SELECT pg_reload_conf();
SQL
```

These settings are written to `postgresql.auto.conf` and survive normal startup and later restarts. The correctly spelled `synchdb.dbz_skipped_operations` takes precedence over the old spelling. Resolve conflicting service command-line settings before proceeding.

Also confirm `dbz_queue_size > dbz_batch_size`, `jvm_max_heap_size >= 1`, and enough worker capacity for the deployment. Session `SET` statements cannot replace the persistent settings above.

### 5.3 Update every database containing SynchDB

Set `TARGET_DSN` to each target database in turn and run:

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" preflight \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --maintenance --require-legacy-resume

"$PGBIN/psql" -X -v ON_ERROR_STOP=1 "$TARGET_DSN" \
  -c "ALTER EXTENSION synchdb UPDATE TO '1.1';"

"$PGBIN/psql" -X -v ON_ERROR_STOP=1 "$TARGET_DSN" \
  -c "SELECT extversion FROM pg_extension WHERE extname='synchdb';"
```

The preflight should report `MAINTENANCE_CHECKS_PASSED`; the final query should return `1.1`.

Ordinary v1.4 credentials remain compatible without a keyring or password re-entry. There is no separate credential conversion step. Confirm the existing source account still connects when capture resumes in Stage 7.

**Expected result:** Every target database is updated. If an external dependency, permission, or catalog error occurs, stay in maintenance and resolve that error. Do not drop and recreate the extension, change its version manually, or use `CASCADE` to bypass dependencies.

## Stage 6: Import original source positions

Use a PostgreSQL superuser. Keep all connectors stopped and the instance in maintenance. For each connector that has a saved position and must retain it, use its original name and target database.

First inspect the import plan, then apply it:

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" migrate-offsets \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --connector "$CONNECTOR" \
  --claims-file "$BACKUP/legacy-claims.json"

"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" migrate-offsets \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --connector "$CONNECTOR" \
  --claims-file "$BACKUP/legacy-claims.json" --apply
```

**Expected result:** The plan reports `PLAN_ONLY`; applying reports `IMPORTED` with a generation. `ALREADY_IMPORTED` means the connector has already been imported and needs no repeated file operation.

The tool preserves the old files. Oracle embedded Infinispan imports the matching complete transaction cache; OLR imports its original three-part position and completed-snapshot state.

Skip connectors that have never started and have no position. A missing offset with remaining history or cache is a recovery problem, not evidence that the connector never ran. Do not rename files, edit LSN/SCN values, or create empty offsets to bypass checks.

If the connection is lost during import, inspect the result before retrying:

```sql
SELECT name, synchdb_artifact_manifest(connector_id)
FROM synchdb_conninfo
WHERE name = 'existing_connector';
```

Continue only when all connectors that need their original positions have successful import records.

## Stage 7: Start normally and restore the original running plan

### 7.1 End maintenance

The preload override used for maintenance was temporary. Keep the original normal preload and service configuration intact.

```sh
"$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop

if [ -n "$PG_SERVICE" ]; then
  sudo systemctl start "$PG_SERVICE"
else
  "$PGBIN/pg_ctl" -D "$PGDATA" \
    -l "$BACKUP/postgresql.log" -w -o "$START_OPTIONS" start
fi
```

Check each target database:

```sql
SHOW shared_preload_libraries;
SHOW synchdb.dbz_snapshot_thread_num;
SHOW synchdb.dbz_skipped_operations;
SELECT extversion FROM pg_extension WHERE extname = 'synchdb';
```

The normal preload should be restored, threads should be `1`, the skipped policy should match stage 5, and the extension version should be `1.1`.

### 7.2 Start only connectors that were previously running

Choose the correct continuation mode:

| Source | `RESUME_MODE` |
| --- | --- |
| MySQL | `initial`, using the completed original position |
| SQL Server, Oracle, PostgreSQL, OLR | `no_data` |

For each connector to restore:

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" control \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --connector "$CONNECTOR" --operation START \
  --mode "$RESUME_MODE" --timeout 180
```

**Expected result:** A committed `operation_id`, then `status: SUCCEEDED`. Keep originally paused and stopped connectors stopped until separately authorized to resume.

A timeout does not cancel a committed request. Query its original ID instead of sending another START:

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" operation \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --id 123 --timeout 60
```

If submission disconnects before returning an ID, query the latest request first:

```sql
SELECT name, last_operation, actual_state, last_sqlstate, last_error
FROM synchdb_control_view
WHERE name = 'existing_connector';
```

Successful receipts permit data verification; they do not by themselves prove that the target has caught up.

## Stage 8: Verify and reopen application writes

Complete these checks for the real captured tables:

1. Existing target tables, mappings, permissions, and application dependencies remain available; the original credentials still connect.
2. Compare source and target by primary key and business fields, including changes made during downtime. Check timestamp precision, large values, empty strings, and NULLs.
3. Commit identifiable INSERT, UPDATE, and DELETE operations on an approved captured source table and confirm each target result.
4. Check for unexpected snapshots, persistent errors, missing rows, or duplicate side effects.
5. Perform a normal target restart and confirm subsequent CDC. Deployments without SynchDB preload must explicitly START the intended connectors again.
6. For PostgreSQL sources, retain the original slot and publication. For other sources, confirm required logs and consumers remain usable.
7. Preserve the original paused and stopped plan.

Reopen application writes after these checks pass. Retain backups, original position files, and execution notes until the recovery window closes. An upgrade does not automatically repair historical incorrect values or earlier missed changes.

## Failure handling and rollback

| Failure | Action |
| --- | --- |
| Inventory or preflight | Leave the old server running; resolve connection, configuration, permission, or ownership issues |
| Backup or installation | Keep PostgreSQL stopped; repair the files or restore a complete matching old program set |
| Extension update | Stay in maintenance; resolve the reported dependency, permission, or catalog error before retrying |
| Position import | Keep capture stopped; inspect the import result and restore matching original files if needed |
| START timeout | Query the original operation ID; do not submit a replacement |
| Failure after capture starts | Stop affected capture and preserve the environment; reconcile source positions, retained logs, and target data first |

The manual procedure has no overall `--resume` command. Record the last successful stage and continue only after its prerequisites are satisfied. The automatic `--apply --resume` command applies only to an execution originally managed by the [automatic tool](automatic-upgrade.md).

Rollback requires the complete old PGDATA, matching old programs, external data, and original configuration. Once v1.5 capture has advanced source positions, restoring old target data alone may not permit continuation. In particular, a PostgreSQL source slot does not move backward when the target is restored. Confirm a coordinated source and target recovery plan before starting the old capture again.
