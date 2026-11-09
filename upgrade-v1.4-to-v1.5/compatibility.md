# Compatibility Changes to Review Before Upgrading

This page describes changes that affect a v1.4 deployment moving to v1.5. Use it to update custom control scripts, monitoring, file-management tools, or database objects that depend directly on SynchDB catalogs. Perform the actual upgrade using [Automatic upgrade](automatic-upgrade.md) or [Manual upgrade](manual-upgrade.md).

The supplied procedure updates catalog structures and imports positions. Ordinary users do not need to edit internal tables or rewrite position-file contents.

## Required actions at a glance

| Area | What changes | What the procedure handles | What you must review |
| --- | --- | --- | --- |
| C and Java software | Internal interfaces and runtime structures | Installs matching software after stopping PostgreSQL | Supply matching C, JAR, dependencies, and SQL; do not replace just one component |
| Position/history paths | Database OID, connector UUID, and generation replace name-based paths | Copies old state and registers the new recovery generation | Update tools that locate, back up, or delete files by the old prefix |
| File signals | Signal paths follow the active generation | Old signal queues are not automatically copied as part of position import | Stop old writers, reconcile pending signals, and update the path |
| OLR coordinates | Durable coordinates are stored in a database table | Imports the complete old `scn/c_scn/c_idx` tuple | Update monitoring and backup procedures that read only the old binary file |
| Infinispan | Buffers move to a generation-specific directory | Copies the complete matching buffer tree with the positions | Preserve the old buffers and compatible dependencies; never substitute an empty cache |
| Connector catalog | Stable identity and revision columns are added | Populates the new columns while retaining configuration and credentials | Update fixed-column readers and direct-write code |
| Source identifiers | Selected `name` columns become `text` | Changes types during the extension update | Review direct catalog dependencies and casts that truncate source names |
| Control APIs | Control requests return operation IDs and execute after commit | Maintains requests and receipts | Replace zero-success checks, fixed delays, and blind resubmission |
| Runtime settings | Skipped operations and snapshot-thread defaults differ | Preserves the chosen skip policy and sets one snapshot thread | Review explicit overrides and apply settings persistently in the manual procedure |
| Permissions | Explicit function access and caller execution identity | Updates extension privileges and runtime records | Grant the actual operator the APIs and object permissions it needs |

## 1. Replace program files together

Install a matching set of:

- `synchdb.so`;
- `dbz-engine-1.1.0.jar` and its entire dependency directory;
- `synchdb.control`, `synchdb--1.1.sql`, and `synchdb--1.0--1.1.sql`;
- the Oracle parser for the current PostgreSQL major version when OLR is used.

The main JAR can have the same filename in both releases. The name alone does not establish compatibility. The current C/Java bridge requires JNI protocol `3`; old and new components must not be mixed.

Stop the old PostgreSQL instance before replacing installed programs. Reloading configuration does not replace already loaded C libraries or old runtime memory structures.

The project upgrade is v1.4 to v1.5, but the PostgreSQL extension update is:

```sql
ALTER EXTENSION synchdb UPDATE TO '1.1';
```

Do not execute the fresh-install `synchdb--1.1.sql` directly over an existing deployment.

## 2. Position and cache files

### 2.1 Path changes do not mean the Debezium offset format became JSON

An old MySQL deployment, for example, uses:

```text
pg_synchdb/mysql_mysync_targetdb_offsets.dat
pg_synchdb/mysql_mysync_targetdb_schemahistory.dat
```

The new paths use:

```text
pg_synchdb/<database_oid>_<connector_uuid>_<generation_uuid>_offsets.dat
pg_synchdb/<database_oid>_<connector_uuid>_<generation_uuid>_schemahistory.dat
```

Debezium offsets still use Java-serialized `HashMap<byte[], byte[]>` data. Schema history remains Debezium line-oriented history. Do not open an offset as a JSON document or convert it to JSON.

A matching `synchdb_artifacts` record is also required. Renaming a file alone does not create the recovery state needed by the new version. Use the position-import procedure, which preserves the original files.

As an administrator, inspect recovery identity in the target database:

```sql
SELECT c.name, c.connector_id, a.database_oid, a.generation,
       a.origin, a.phase, a.active
FROM synchdb_conninfo c
JOIN synchdb_artifacts a USING (connector_id)
WHERE c.name = 'mysync'
ORDER BY a.created_at;

SELECT synchdb_artifact_manifest(connector_id)
FROM synchdb_conninfo
WHERE name = 'mysync';
```

File tools should use the active generation and its `base_path`. A manifest can contain several generations; do not assume the first entry is active. Stop deleting files by connector-name prefix or copying files from a different database or generation.

### 2.2 Other paths

In this table, `<base>` is `pg_synchdb/<database_oid>_<connector_uuid>_<generation_uuid>`.

| Content | v1.4 location | v1.5 location and action |
| --- | --- | --- |
| Signal file | Source/name/database prefix; PostgreSQL uses `pg_` rather than its offset prefix `postgres_` | `<base>_signal.dat`; update direct file writers |
| Embedded Infinispan | `pg_synchdb/ispn_<connector>_<target_database>/` | `<base>_ispn/`; migrate the complete buffer tree |
| External captured-table selection | A `file:` reference in `table` configuration | `<base>_tables.json`; import prepares the matching copy |
| External snapshot-table selection | A `file:` reference in `snapshottable` configuration | `<base>_snapshot_tables.json`; keep it paired with the generation |

Selection files are relevant when configuration uses external `file:` inputs. Inline table lists do not necessarily produce these files.

Old signal files are not automatically copied by position import. Stop signal producers before the upgrade, reconcile signals that have not completed, and resume producers using the active new path. Do not blindly replay an old queue into a new generation.

### 2.3 OLR positions move into the catalog

The old native OLR files are:

```text
pg_synchdb/olr_<connector>_<target_database>_offsets.dat
pg_synchdb/olr_<connector>_<target_database>_schemahistory.dat
```

They contain the original-platform 24-byte tuple of three 64-bit values `scn/c_scn/c_idx` and the one-byte snapshot-complete marker `t`. Preserve all three coordinates and the original platform byte order. The `openlog_replicator_d_*` snapshot files are not replacements for native `olr_*` coordinates.

After import, ongoing durable native OLR positions are maintained in `synchdb_olr_positions`. The old file is retained but should not be used as the only source of current progress:

```sql
SELECT c.name, a.generation, p.scn, p.c_scn, p.c_idx
FROM synchdb_conninfo c
JOIN synchdb_artifacts a USING (connector_id)
JOIN synchdb_olr_positions p USING (database_oid, connector_id, generation)
WHERE c.name = 'olrsync' AND a.active;
```

See [OLR requirements](olr.md) before upgrading this source.

### 2.4 Infinispan requires the whole buffer tree

Preserve `events`, `transactions`, `processed`, and `schema` data/index directories, existing `rollbacks`, and other metadata. The upgrade copies them into the new generation directory; it does not transcode an arbitrary cache format.

Keep the original cache settings and compatible dependency versions. Stop the old writer normally before migration. Moving only the offset, choosing a few files, or creating an empty cache can lose uncommitted transaction state. See [Infinispan requirements](infinispan.md).

### 2.5 File and ownership checks

Ordinary required offset/history files must be server-owned regular single-link files, nonempty, no larger than 64 MiB, and not group/world writable. Symbolic links are not accepted. Cache files use separate tree checks and can legitimately be empty or exceed that size limit.

Underscores in connector/database names are allowed when ownership can be established. If the old naming scheme caused two owners to share one physical path, restore the correct matching files rather than rename them to bypass inspection. Use the complete ownership inventory described in [Command reference](commands.md).

## 3. Existing catalog columns

### 3.1 `synchdb_conninfo`

The original columns remain in order:

```text
name text
isactive boolean
data jsonb
```

Two managed fields are appended:

| Column | Type | Meaning |
| --- | --- | --- |
| `connector_id` | Non-null, unique `uuid` | Stable connector identity used by state and file records |
| `config_revision` | Non-null `bigint` | Configuration version incremented when configuration content changes |

The update populates existing rows automatically. It retains the original configuration JSON and ordinary encrypted credentials.

Update custom code as follows:

- Replace fixed three-column `SELECT *` decoding with explicit columns, such as `SELECT name, isactive, data FROM synchdb_conninfo`.
- Do not assign `connector_id` or `config_revision` manually, or rename a connector by updating `name`.
- Do not start or stop workers by updating `isactive`; use control APIs.
- The old three-value insert form can use defaults for appended fields, but reinserting old configuration is not a substitute for preserving connector identity and importing its matching position.

For durable running intent and current status, read `synchdb_control_view` rather than inferring both from `isactive` or a PID.

### 3.2 Source identifiers become text

| Column | Old type | New type |
| --- | --- | --- |
| `synchdb_attribute.ext_tbname` | `name` | `text COLLATE "C"` |
| `synchdb_attribute.ext_attname` | `name` | `text COLLATE "C"` |
| `synchdb_attribute.ext_atttypename` | `name` | `text COLLATE "C"` |
| `synchdb_objmap.srcobj` | `name` | `text COLLATE "C"` |

Use complete source identifiers and text parameters instead of first casting long names to PostgreSQL `name`. Source identities use exact byte comparison and a 2400-byte bound, with an additional bound on qualified column identities. Connector names and PostgreSQL target identifiers retain their own limits.

For example:

```sql
SELECT synchdb_add_objmap(
    'mysync'::name,
    'table'::name,
    'SourceDb.VeryLongSourceTableName'::text,
    'app.target_table'::text
);
```

Changing the type cannot recover names already truncated in old stored values. Check affected mappings against the actual source.

### 3.3 Custom dependencies can block the update

A custom view, function, policy, or index that depends directly on a changing catalog column can cause `2BP01` during the update. Save its definition and grants, review and remove the specific blocking dependency, update the extension, and recreate the object with the new text types. Do not use `CASCADE` against the extension.

The built-in `synchdb_att_view` retains its compatibility output types. Objects depending only on that view do not need to be removed indiscriminately. For complete long source names, use:

```sql
SELECT name, ext_tbname, ext_attname, ext_atttypename
FROM synchdb_att_view_text;
```

## 4. Added persistent records and monitoring

| Object | Purpose and impact |
| --- | --- |
| `synchdb_control_state` | Accepted configuration, execution role, desired state, runtime options, and last operation |
| `synchdb_control_commands` | Operation IDs, commands, status, errors, and execution timestamps |
| `synchdb_control_view` | Recommended view for durable intent, actual state, and recent errors |
| `synchdb_artifacts` | Generation ownership, source identity, and active recovery state |
| `synchdb_olr_positions` | Durable native OLR replay coordinates |
| `synchdb_bootstrap_targets` | Snapshot target ownership and structure |
| `synchdb_bootstrap_runs` | Snapshot results, not CDC positions |

The extension update creates these objects. Use the APIs to maintain them; direct writes are not a substitute for control operations or recovery.

Configuration export also includes connector identity and revision. `synchdb_export_configuration()` exports configuration and mappings, not business rows, offsets, source logs, or execution grants. Exporting into an empty database is not an equivalent in-place position-preserving upgrade.

## 5. Control-script changes

### 5.1 A returned zero is no longer the control success convention

START, STOP, PAUSE, RESUME, RESTART, RELOAD, offset changes, logging controls, and statistics reset now return a positive operation ID.

The old compatibility control functions still have SQL return type `int`; the new unified `synchdb_control()` returns `bigint`. Creating connection configuration and adding mappings still use their existing zero return convention. Do not change every API's success check indiscriminately.

Change this logic:

```text
Call a start function.
Treat return value 0 as completed startup.
```

To:

```text
Submit the control request and retain its operation ID.
Commit the database transaction.
Read that ID's receipt until it reaches a final status.
Then check collection state and business data.
```

For a client with autocommit enabled:

```sql
-- MySQL resumes a completed position with initial.
-- The other sources use no_data for this upgrade.
SELECT synchdb_control('mysync', 'START', 'initial');

-- Replace 123 with the returned ID.
SELECT synchdb_operation(123, 1000);
```

Wait on `PENDING` or `EXECUTING`. Inspect `FAILED` or `CANCELLED`. `SUCCEEDED` confirms the command, not complete data catch-up.

### 5.2 Transactions and retries

The controller can execute a request only after its PostgreSQL transaction commits. A rollback removes the request. Do not wait for completion inside the same uncommitted transaction that submitted it.

After a timeout, query the original operation ID rather than issuing another START. A connector with an unfinished ordinary request rejects another ordinary request; do not use a resubmission loop as a queue. The [command tools](commands.md) handle one committed submission followed by receipt polling.

### 5.3 Direct FDW helper calls

Only custom callers of these SQL helpers need these changes:

| Helper | Required adjustment |
| --- | --- |
| `synchdb_prepare_initial_snapshot` | The old `(name, text)` secret argument is replaced. Use `(name)` or the new `(text, name)` form, whose second argument is a server name, not a key |
| `synchdb_do_initial_snapshot` | Remove `p_secret` and rebind arguments; the default target policy changes from `replace` to `error` |
| `synchdb_do_schema_sync` | Remove `p_secret` and review target-policy handling |

Also review these old options:

- `skip/drop/validate_existing` policies are rejected. Explicit `replace` only applies to a registered target owned by the connector with matching structure and safe replacement conditions, not an arbitrary old business table.
- Non-null `p_offset` overrides and `p_write_schema_hist=true` are rejected. Do not use old arguments to force history or coordinate publication.
- Compatibility arguments such as `p_source_schema/p_stage_schema` no longer determine the actual temporary workspace. Update tools that read fixed intermediate schemas or temporary tables.

The snapshot/schema helpers still return SQL `text`, but that text contains a structured JSON result. Do not parse it as a single SCN, LSN, or old delimited position string. Use `synchdb_bootstrap(jsonb)` when a JSON interface is needed.

The update removes old function identities with `RESTRICT`. Review and migrate custom objects that depend on those signatures; do not add `CASCADE` to force the update.

## 6. Configuration and account permissions

### Configuration changes

| Setting or behavior | Required action |
| --- | --- |
| Old `synchdb.dbz_skipped_oeprations` default `t`; new correctly spelled `synchdb.dbz_skipped_operations` default `none` | Preserve the chosen policy. The old spelling remains accepted; an explicitly configured correct spelling takes precedence |
| Snapshot threads default `2` to `1`; current native continuation also requires `1` for `no_data` | Persist `synchdb.dbz_snapshot_thread_num=1` |
| Versioned run settings accepted at START/RESTART | Reload configuration, then restart a connector when needed to accept changed runtime options; do not assume a running collector changed policy immediately |

The automatic upgrade persists the skip policy and single-thread setting using `ALTER SYSTEM`. Manual upgrades must perform the corresponding configuration steps. These SIGHUP settings cannot be replaced by a one-off session `SET`.

`postgresql.auto.conf` overrides ordinary configuration files. For later changes, use `ALTER SYSTEM SET/RESET`, reload, and restart connectors as necessary. Resolve service command-line and relevant role/database overrides when preflight reports them.

### Permissions

The update revokes extension-function execution from `PUBLIC`. Accounts that relied only on that default need explicit grants. Existing direct role grants are not all removed.

START/RESTART accepts the caller as the worker execution role. Check database access, configuration visibility, target schema/table permissions, conversion functions, and any required DDL rights. Maintenance operations must use the accepted role or an account allowed to switch to it. PostgreSQL 16 execution roles also need LOGIN.

For example, an administrator can grant API access to an actual operator:

```sql
GRANT USAGE ON SCHEMA public TO sync_operator;
GRANT EXECUTE ON FUNCTION synchdb_control(name, text, text) TO sync_operator;
GRANT EXECUTE ON FUNCTION synchdb_operation(bigint, integer) TO sync_operator;
```

These grants alone are not a complete worker authorization. Do not try to replace API permissions with write access to the control ledgers.

## 7. What should remain in place

- Ordinary encrypted credentials remain compatible. Do not create a keyring or re-enter passwords for a standard upgrade.
- Keep existing target tables, mappings, connectors, and complete saved positions. Do not perform a fresh snapshot just to adapt the interface.
- PostgreSQL upgrades keep the original source slot/publication; do not replace them with new empty resources.
- Old control function names and `synchdb_state_view` remain available, but control return values and status handling need the changes described above.
- RELOAD still loads mappings and handles bound target-structure changes. It now returns an operation receipt and does not backfill historical rows.
- FDW-to-CDC continuation and SQL Server `initial/always` remain available. Their known snapshot handoff limitations are not repaired by this upgrade. Follow the source-specific guide for a completed existing target.

Earlier missed changes, incorrect NULL/empty-string values, timestamp precision, TOAST, or LOB values require data reconciliation. Retaining a position does not automatically repair those rows.
