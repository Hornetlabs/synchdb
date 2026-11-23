# Command reference for the v1.4 to v1.5 upgrade

Use the [automatic guide](automatic-upgrade.md) for a complete upgrade, or the [manual guide](manual-upgrade.md) for individual stages. These commands preserve existing configuration, data, and positions; they are not a fresh-install procedure.

## Before running commands

The commands are in `scripts/` beside these manuals, with the required update SQL in `sql/`. `MIGRATION_DIR` points to this standalone `upgrade-v1.4-to-v1.5/` package. Set these variables to real absolute paths:

```sh
REPO_ROOT=/absolute/path/to/synchdb
MIGRATION_DIR="$REPO_ROOT/upgrade-v1.4-to-v1.5"
PYTHON=/absolute/path/to/venv/bin/python3
PGBIN=/path/to/current/postgresql/bin
TARGET_DSN='service=synchdb_target'
```

If the package was copied outside a repository, set `MIGRATION_DIR` directly to its absolute path, for example `/opt/upgrade-v1.4-to-v1.5`. Neither the old `migration/` directory nor a full checkout is needed.

`TARGET_DSN` must connect to the actual database containing SynchDB, not an unrelated administrative database. Use a database superuser for upgrade and position maintenance. Run `pg_ctl` as the PostgreSQL service OS account.

The tools require Python 3.9 or later and `psql`. Automatic upgrade also requires `psycopg2` in `PYTHON`. Use libpq service files and `.pgpass` for authentication; do not put passwords in commands. Add `--psql "$PGBIN/psql"` as below when `psql` is not in PATH.

## 1. Check the supplied migration files

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/verify_bundle.py"
```

**Expected result:** Verification information and exit status `0`.

For a missing file or `Hash mismatch`, obtain a complete matching package. Do not bypass the check or mix C, Java, dependencies, and SQL from different packages.

## 2. Inventory the running v1.4 deployment

Run before stopping the old target:

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" inspect \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --output /private/old-inventory.json

"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" preflight \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --output /private/old-preflight.json
```

| Output | Meaning |
| --- | --- |
| `inspect` output file | Visible connector configuration, mappings, and target bindings; connection secrets are omitted |
| `INVENTORY_ONLY` | Running-instance inventory completed; maintenance requirements have not yet been checked |
| `REVIEW_REQUIRED` | Resolve entries in `blockers` before continuing |
| `warnings` | Items requiring confirmation, such as file ownership or matching cache state |

Output files are mode `0600`, and existing files are not overwritten. Use distinct filenames for each database. The inventory is neither a full backup nor an importable replacement for configuration.

### Export the complete file ownership inventory

Prepare the deployment configuration described in the [automatic guide](automatic-upgrade.md), then export while the old instance is running:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config /private/upgrade.json \
  --claims-output /private/legacy-claims.json
```

**Expected result:** A read-only plan and a complete mode-`0600` ownership file. The command does not stop PostgreSQL or create the upgrade working directory. `--claims-output` cannot be combined with `--apply`.

The plan checks program-directory write permissions, so it normally requires installation privileges. Give the resulting file to the OS account that will execute the import, while retaining mode `0600`. For example, replace the account and group here with the actual PostgreSQL service account:

```sh
sudo chown postgres:postgres /private/legacy-claims.json
```

Its parent directories must also allow that account to access the file. Do not change connector names, database names, or cache types after export, or remove databases from the file to bypass an ownership conflict.

## 3. Check maintenance readiness

Complete the manual shutdown, cold backup, installation, and maintenance startup first:

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" preflight \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --maintenance --require-legacy-resume
```

**Expected result:** `MAINTENANCE_CHECKS_PASSED`, exit status `0`.

The check requires a superuser, no SynchDB preload or active workers, no unfinished control requests, and stopped connectors. Before the extension update, it also checks that the update SQL is installed.

| Exit status | Action |
| --- | --- |
| `0` | The requested checks passed; continue with the next manual stage |
| `2` | Read and resolve `blockers`, then check again |
| `1` | Resolve the reported client, connection, or database error |

Passing preflight does not establish that the source logs, server files, or business data are correct. Names containing underscores are allowed, but real file ownership collisions must be resolved.

## 4. Import the original positions

The current target database must already report extension SQL version `1.1`. Keep the server in maintenance and all capture stopped.

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" migrate-offsets \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --connector existing_connector \
  --claims-file /private/legacy-claims.json

"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" migrate-offsets \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --connector existing_connector \
  --claims-file /private/legacy-claims.json --apply
```

| Output `action` | Meaning |
| --- | --- |
| `PLAN_ONLY` | No position has been copied or registered; inspect the plan before applying |
| `IMPORTED` | Import succeeded; retain the reported generation and continue to normal startup |
| `ALREADY_IMPORTED` | The connector already has its imported position; use the existing generation |

Original offset/history files are preserved. Oracle embedded Infinispan also copies the matching complete cache; OLR checks the original three-part position and snapshot completion state.

Keep capture stopped when files or cache state are missing or inconsistent. Do not edit LSN/SCN values, rename files, or create empty state to bypass checks.

If import disconnects before returning a result, inspect the target database before deciding whether to retry:

```sql
SELECT name, synchdb_artifact_manifest(connector_id)
FROM synchdb_conninfo
WHERE name = 'existing_connector';
```

Without a complete ownership file, the server may request one. Resolve ownership rather than changing connector names.

## 5. Submit a control request and wait for its receipt

After normal PostgreSQL startup, restore only connectors that should be running. MySQL continuation uses `initial`; other sources use `no_data`:

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" control \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --connector existing_connector \
  --operation START --mode initial --timeout 180
```

For SQL Server, Oracle, PostgreSQL, and OLR, replace `--mode initial` with `--mode no_data`. Do not resubmit START for connectors already restored by the automatic upgrade.

**Expected result:** `Submitted operation_id=... (committed)`, followed by `status: SUCCEEDED`.

Supported control operations are START, STOP, PAUSE, RESUME, RESTART, and RELOAD. `--mode` is valid only for START and RESTART. The helper submits once, commits, and waits.

| Exit status | Meaning and action |
| --- | --- |
| `0` | Successful receipt; continue checking capture and target data |
| `1` | Failed/cancelled request or client error; inspect the receipt and database error |
| `2` | Waiting timed out; the request may still execute, so query the same ID |

Query a previously submitted operation, replacing `123` with its actual ID:

```sh
"$PYTHON" "$MIGRATION_DIR/scripts/synchdb_migrate.py" operation \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --id 123 --timeout 60
```

If submission disconnects before an ID is received, query the latest request:

```sql
SELECT name, last_operation, actual_state, last_sqlstate, last_error
FROM synchdb_control_view
WHERE name = 'existing_connector';
```

A successful receipt is not a data reconciliation result. Compare business fields by primary key and confirm new INSERT, UPDATE, DELETE, and restart continuation.

## 6. Existing credentials

Ordinary v1.4 passwords remain compatible. The upgrade retains the stored credentials; no keyring, password re-entry, or separate conversion command is required. Confirm source connectivity and inspect the original START operation receipt when capture resumes. If authentication fails, resolve the reported source account, permissions, or connection issue rather than rewriting position files or replacing credentials blindly.

## 7. Read automatic execution records and continue an interrupted run

The automatic upgrade's configured `work_dir` contains:

| Path | Contents |
| --- | --- |
| `state.json` | Current/completed stages, imported positions, operation IDs, and recovery plan |
| `execution.jsonl` | Commands, exit statuses, errors, and stage records |
| `backup/` | Cold data, configuration, and replaced original programs |
| `claims.json` | Complete file ownership inventory |

After resolving the cause of failure, continue with the same configuration and working directory:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config /private/upgrade.json --apply --resume
```

Do not create a new `work_dir`, delete saved state, or repeatedly submit START to retry an interrupted upgrade. If rollback is necessary, first determine whether source positions have advanced, then follow [manual recovery guidance](manual-upgrade.md#failure-handling-and-rollback).
