# Oracle23ai / LogMiner: upgrade from v1.4 to v1.5

This page adds CDB/PDB connection checks to the [Oracle / LogMiner procedure](oracle.md). Use the [automatic upgrade guide](automatic-upgrade.md) for the common sequence.

The upgrade retains target tables, connector configuration, credentials, offsets, and schema history. It does not recreate the connector or repeat the full snapshot. The PostgreSQL extension catalog changes from `1.0` to `1.1` as part of the v1.4-to-v1.5 release upgrade.

## Phase 1: record the original containers and accounts

**Check the existing PostgreSQL configuration and Oracle source.**

| Item | Example | Upgrade requirement |
| --- | --- | --- |
| CDB/PDB | `FREE/FREEPDB1` | Preserve the actual pair; do not replace a PDB connection with a CDB-only connection. |
| LogMiner account | `C##SYNCHDB` | Keep the existing account and mining privileges. |
| Business owner | `APP` | Check it separately from the capture account; preserve table scope and mappings. |
| Host, port, service | Existing connection values | Keep the actual deployment values. |

These names illustrate the format. Do not rename a CDB/PDB or reset passwords to match these examples.

## Phase 2: check the correct container and retained logs

**Run on Oracle as an authorized source administrator.**

In the CDB root:

```sql
SHOW CON_NAME;
SELECT name, log_mode, supplemental_log_data_min, force_logging
FROM v$database;
SELECT name, open_mode FROM v$pdbs;
```

Confirm that the business PDB is open and that log and privilege conditions still support the existing capture. Switch to the actual business PDB and check captured tables. Replace both `FREEPDB1` and `APP`:

```sql
ALTER SESSION SET CONTAINER = FREEPDB1;
SHOW CON_NAME;

SELECT owner, table_name, log_group_type, always
FROM dba_log_groups
WHERE owner = 'APP'
ORDER BY table_name, log_group_type;
```

**Proceed when:** the root/PDB identity is correct, the business PDB is available, privileges and supplemental logging remain valid, and all redo/archives needed from the saved offset are retained. Freeze captured-table DDL through catch-up; preferably stop application writes and let the old target catch up first.

## Phase 3: run the upgrade

**Run on the target PostgreSQL host.**

Prepare programs and `upgrade.json` using the [automatic guide](automatic-upgrade.md). Keep the original CDB/PDB, capture account, and business owner. Leave the old PostgreSQL instance running for discovery.

Set actual absolute paths in one shell. `PYTHON` must have `psycopg2`; all commands use the same interpreter and configuration. When run as root, the script performs database connections and `pg_ctl` operations as the PGDATA owner.

```sh
REPO_ROOT=/absolute/path/to/synchdb
MIGRATION_DIR="$REPO_ROOT/upgrade-v1.4-to-v1.5"
PYTHON=/absolute/path/to/venv/bin/python3
UPGRADE_CONFIG=/private/upgrade.json

sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG"
```

Once the plan identifies the correct instance and connectors without blocking errors, execute during the upgrade window:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply
```

The script imports the original offset/history and resumes previously running connectors with `no_data`. Existing ordinary passwords remain usable without a keyring. With embedded Infinispan, also follow the [cache requirements](infinispan.md).

**Proceed when:** the command exits successfully and prints `Upgrade complete`. Do not send another START for connectors already restored automatically. Previously paused or stopped connectors remain stopped.

For manual execution, follow [manual steps](manual-upgrade.md) and resume with `no_data`, not `always`.

## Phase 4: accept the upgrade and reopen writes

**Check the upgraded PostgreSQL database and the original Oracle business PDB.**

1. Query `synchdb_control_view` on the target. Confirm that intended running connectors have resumed and there are no persistent errors.
2. Reconcile original rows and downtime changes by primary key and business field.
3. In the original business PDB, use an approved business account to perform controlled INSERT, UPDATE, and DELETE operations. Confirm each target result. DDL executed by SYS/SYSTEM does not necessarily have the same capture behavior as business-owner DDL.
4. Restart target PostgreSQL normally within the maintenance window and verify further changes. Check automatic recovery when SynchDB preload is enabled; otherwise explicitly START intended running connectors with `no_data` as described in [manual steps](manual-upgrade.md).
5. Reopen application writes and DDL after acceptance, retaining backups and logs through the observation period.

See the [Oracle guide](oracle.md) for LOB behavior, historical-value repair, operation timeouts, and rollback conditions. On a service, container, or privilege error, retain the work directory, fix the cause, then continue in the same shell:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply --resume
```
