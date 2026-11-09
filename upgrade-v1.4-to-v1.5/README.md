# SynchDB v1.4 to v1.5 Migration Guide

Use this guide to upgrade an existing SynchDB v1.4 deployment to v1.5 while preserving connector settings, target tables, object mappings, credentials, and synchronization positions. Keep the same PostgreSQL major version, target databases, and source identities throughout the upgrade.

The product versions and PostgreSQL extension versions use different numbering:

| Item | Before upgrade | After upgrade |
| --- | --- | --- |
| SynchDB release | v1.4 | v1.5 |
| `pg_extension.extversion` | `1.0` | `1.1` |

The extension update command is therefore `ALTER EXTENSION synchdb UPDATE TO '1.1'`, not `UPDATE TO '1.5'`.

## Choose your procedure

**Start with [Automatic upgrade](automatic-upgrade.md).** Prepare the new software, review the plan, and let the script stop collection, stop PostgreSQL, make a cold backup, install the software, update catalogs, import positions, and restore collection.

Keep the old PostgreSQL instance running until the automatic script stops it. Do not replace loaded libraries or manually rename position files before running the script.

For a step-by-step maintenance procedure, use [Manual upgrade](manual-upgrade.md). Choose one procedure for an upgrade; do not insert manual installation or import operations into a running automatic upgrade.

To practice from source checkout through old synchronization, manual upgrade, and data verification on an isolated local environment, use [Local MySQL rehearsal](mysql-local-rehearsal.md).

## Migration stages

| Stage | What you do |
| --- | --- |
| 1. Prepare | Obtain matching v1.5 software and migration tools; check access, source logs, and backup space |
| 2. Review | Fill in the configuration and run the read-only plan; resolve reported problems |
| 3. Enter the maintenance window | Freeze captured-table DDL, preferably stop application writes, wait for collection to catch up, and compare source and target data |
| 4. Upgrade | Run the selected procedure and wait for installation, catalog updates, position import, and startup to finish |
| 5. Validate | Check connector status, accumulated changes, field values, new changes, and restart recovery |
| 6. Resume applications | Reopen application writes after acceptance; retain backups and source logs during the observation period |

Existing credentials remain usable. You do not need a keyring, password re-entry, new connectors, or replacement target tables for an ordinary in-place upgrade. Connectors that are paused or stopped when the automatic script inventories the instance remain stopped.

## Read your source-specific requirements

Read the relevant page before performing either upgrade procedure:

| Source or storage option | Guide |
| --- | --- |
| MySQL | [MySQL](mysql.md) |
| SQL Server | [SQL Server](sqlserver.md) |
| PostgreSQL | [PostgreSQL source](postgresql.md) |
| Oracle / LogMiner | [Oracle](oracle.md) |
| Oracle23ai | [Oracle23ai connection and container requirements](oracle23ai.md) |
| Oracle / OpenLogReplicator | [OpenLogReplicator](olr.md) |
| Oracle / embedded Infinispan | [Infinispan transaction buffers](infinispan.md), together with the Oracle guide |

## Before proceeding

- The original initial snapshot must be complete, and the target data must match the saved source position. An upgrade does not repair earlier missing or incorrect rows.
- Retain the binlog, CDC records, redo/archive logs, or WAL needed from the saved position through recovery and acceptance.
- Prepare matching C libraries, Java packages, dependencies, extension SQL, and the Oracle parser if OLR is used.
- Schedule all SynchDB databases sharing this PostgreSQL installation together. Do not run old and new collection against the same source identity.
- Back up the full PGDATA, external WAL and tablespaces, configuration, position files, transaction buffers, and old program files. An inventory JSON or `pg_dump` alone is not a complete recovery backup.
- The supplied in-place update expects the extension and its member objects in `public`. Resolve preflight failures before stopping the deployment.

## Package contents and tools

This directory contains the English migration manuals and the required upgrade tools together:

```text
upgrade-v1.4-to-v1.5/
  README.md                    # start here
  automatic-upgrade.md
  manual-upgrade.md
  commands.md
  compatibility.md
  mysql.md, sqlserver.md, ...   # source-specific requirements
  scripts/                     # upgrade commands and required support files
  sql/                         # extension update SQL and integrity manifest
```

Copy this entire directory and your prepared v1.5 program files to the target host. Keep `scripts/` and `sql/` beside the manuals. The package runs independently; the old `migration/` directory and a full repository checkout are not required.

Examples locate the package beneath `REPO_ROOT`. For a separately copied package, set `MIGRATION_DIR` directly to its absolute path, such as `/opt/upgrade-v1.4-to-v1.5`. Keep the package's directory structure intact.

Use [Command reference](commands.md) for individual operations and receipts. Review [Compatibility changes](compatibility.md) if you maintain custom control scripts, monitoring queries, file tools, or objects that depend directly on extension catalogs.
