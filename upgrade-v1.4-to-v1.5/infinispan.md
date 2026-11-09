# Oracle / Infinispan transaction cache: upgrade from v1.4 to v1.5

Use this guide for Oracle LogMiner connectors with an `embedded` Infinispan buffer. Keep the existing target tables, credentials, offset, schema history, and transaction cache together, then continue CDC without a new full snapshot.

Complete the checks below before using the [automatic guide](automatic-upgrade.md). For Oracle logs, accounts, and containers, also follow the [Oracle / LogMiner guide](oracle.md). The SynchDB release changes from v1.4 to v1.5; the PostgreSQL extension catalog changes from `1.0` to `1.1`.

## Phase 1: confirm the existing cache configuration

**Run on the existing PostgreSQL target database.** Replace `orasync` with the original connector name:

```sql
SELECT name,
       data->>'connector' AS connector,
       data->>'ispn_cache_type' AS cache_type,
       data->>'ispn_memory_type' AS memory_type,
       data->>'ispn_memory_size' AS memory_size
FROM synchdb_conninfo
WHERE name = 'orasync';

SELECT name, pid, stage, state, err
FROM synchdb_state_view
WHERE name = 'orasync';
```

Confirm:

- The capture backend is Oracle LogMiner and cache type is `embedded`.
- The original full snapshot completed, CDC runs normally, and the process can stop cleanly.
- Preserve the exact `heap` or `off heap` value, memory size, snapshot engine, and connection configuration. Do not change case or cache type during the upgrade.
- The old serialization/dependency versions match the new package. The supported embedded format uses Debezium `3.5.2.Final`, Infinispan `15.2.1.Final`, and Protostream `5.0.13.Final`. Do not force a directory copy between incompatible dependencies.

If the old process cannot start because `log.mining.buffer.infinispan.cache.rollbacks` is missing, first establish a recoverable state with matching position and cache. An empty cache does not establish that state. A normally running deployment does not need an old-version reinstall just to upgrade.

**Proceed when:** capture is healthy, cache configuration is known, and the target, position, and cache belong to the same deployment.

## Phase 2: prepare the complete cache and cold backup

**Work on the target PostgreSQL host.**

The original cache is under PGDATA:

```text
pg_synchdb/ispn_<connector>_<target_database>/
```

Retain all of the following together:

| Content | Requirement |
| --- | --- |
| Original offset and schema history | They must match this cache, not another backup's position. |
| `events`, `transactions`, `processed`, `schema` | Preserve each group's data/index directories completely. |
| Existing `rollbacks`, extra directories, and metadata | Keep them as part of the original tree; do not select individual files. |
| Business tables and existing rows | Continue using the original tables rather than an empty target. |

The cache can contain Oracle transactions already read but not yet committed. Moving only the offset loses that recovery state. An empty directory cannot substitute for the original cache.

Cache backup must follow a clean connector stop, confirmed process exit, and PostgreSQL shutdown. The automatic flow performs these steps; do not copy a live cache manually beforehand.

Reserve enough space for the whole PGDATA, external data, programs, and configuration. Retain source redo/archives through downtime and catch-up. Freeze captured-table DDL; preferably stop application writes and let the old target catch up.

## Phase 3: run the automatic upgrade

**Run on the target PostgreSQL host. Keep the old PostgreSQL instance running for discovery.**

Prepare matching C, JAR, dependencies, SQL, and `upgrade.json` using the [automatic guide](automatic-upgrade.md). Do not add an independent cache-copy step.

Set actual absolute paths in one shell. `PYTHON` must contain `psycopg2`. Plan, apply, and resume use the same interpreter/configuration; the script uses the PGDATA owner for PostgreSQL operations and database connections.

```sh
REPO_ROOT=/absolute/path/to/synchdb
MIGRATION_DIR="$REPO_ROOT/upgrade-v1.4-to-v1.5"
PYTHON=/absolute/path/to/venv/bin/python3
UPGRADE_CONFIG=/private/upgrade.json

sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG"
```

Confirm that the plan identifies the Oracle embedded cache, its recovery details, and correct directory ownership without collisions. During the upgrade window:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply
```

After stopping, backing up, and updating the catalogs, the script copies the original offset/history and full cache into matching v1.5 recovery paths. It preserves the old files and resumes previously running connectors using `no_data`. Ordinary existing credentials need no keyring.

**Proceed when:** the command exits successfully and prints `Upgrade complete`; connectors reach their intended state. Do not submit another START for automatically restored connectors. Previously paused or stopped connectors remain stopped.

For [manual steps](manual-upgrade.md), run `migrate-offsets` after the catalog upgrade; that tool checks and copies the cache. Its plan should include `infinispan_resume`. Pass the complete ownership inventory through `--claims-file` when required. Do not rename cache directories or remove inventory entries.

## Phase 4: check recovery and business data

**Check the upgraded PostgreSQL target and Oracle business source.** Replace `orasync`:

```sql
SELECT name, desired_state, actual_state, last_operation, last_sqlstate, last_error
FROM synchdb_control_view
WHERE name = 'orasync';

SELECT name, pid, stage, state, err
FROM synchdb_state_view
WHERE name = 'orasync';

SELECT c.name, a.generation, a.origin, a.phase, a.active
FROM synchdb_conninfo c
JOIN synchdb_artifacts a USING (connector_id)
WHERE c.name = 'orasync' AND a.active;
```

Check in order:

1. Original tables, rows, and mappings remain; the connector continues CDC without a new full snapshot.
2. After backlog catch-up, reconcile by primary key and business field. Perform approved INSERT, UPDATE, and DELETE checks.
3. If an uncommitted transaction crosses the upgrade, wait for its actual commit or rollback. Confirm all committed changes reach the target and rolled-back changes do not. Do not create large production transactions solely for acceptance.
4. During the maintenance window, restart the target normally and confirm further CDC and recovery state remain usable. With SynchDB preload, check automatic recovery; without it, explicitly START using `no_data` as described in [manual steps](manual-upgrade.md).
5. Reopen application writes and DDL after acceptance. Keep the cold backup and original cache through the observation period.

## Phase 5: handle a failed stage

Keep the work directory, original cache, and positions. Correct the specific cause and continue using the same configuration:

```sh
sudo "$PYTHON" "$MIGRATION_DIR/scripts/synchdb_upgrade.py" \
  --config "$UPGRADE_CONFIG" --apply --resume
```

| Symptom | Action |
| --- | --- |
| Cache is absent or a data/index directory is incomplete | Restore the matching clean-shutdown backup; do not create empty directories to pass checks. |
| Cache/history exists but offset is missing | Restore the complete matching state. Do not delete the cache and classify the connector as never started. |
| Cache type or memory configuration is rejected | Confirm Oracle embedded capture and unchanged configuration; do not switch backend or remote-cache type during this upgrade. |
| Ownership, permission, link, or directory collision error | Check the indicated path, PostgreSQL service account, original ownership, and complete inventory. Do not bypass by renaming or pruning the inventory. |
| `ALREADY_IMPORTED` | The original state is already imported. Continue from the current recovery stage rather than overwriting cache state that may have advanced. |
| Connection lost and commit status is unknown | Check saved stages and the registered generation. Resume the same work directory; do not reconstruct a recovery directory. |
| Missing/duplicate data or unavailable source logs | Stop the affected capture and reconcile target data, position, cache, and redo before continuing. |

Once v1.5 captures changes, rollback must treat target data, positions, cache, and source logs as one recovery set. Do not restore only an offset or clear the cache before restarting.
