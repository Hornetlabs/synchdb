# Local MySQL rehearsal: build v1.4, synchronize, and upgrade to v1.5

This procedure creates a separate MySQL source and PostgreSQL target on this machine. It leaves the existing PostgreSQL installation and its running databases alone. Run the commands as the `lct` operating-system user in **Bash**, in the order shown, in the same shell.

The main workflow was exercised on this machine on **2026-11-09**. The sample data passed the initial snapshot, old CDC, downtime replay, new CDC, and restart checks. Follow each completion check when running your own rehearsal. This procedure reuses a copy of the machine's existing PostgreSQL binaries; it does not rebuild PostgreSQL.

The old source is the upstream `v1.4` tag, commit `0756e247ca07083244559cb4616c5dedf1b3da97`. At the time this example was prepared, **2026-11-09**, an official `v1.5` tag had not been verified. The new source below is the pinned candidate `40c6a58a49fc5f4745f5594584e226e8c4138618` from `hanjianqiao/synchdb`'s `refactor` branch. Use its matching migration package. For a released deployment, obtain the actual release and matching package instead of assuming that this candidate is the release.

The product upgrade is **v1.4 to v1.5**; PostgreSQL's extension version changes from **`1.0` to `1.1`**. PostgreSQL itself stays at **18.6**.

| Item | Value in this example |
| --- | --- |
| Working directory | `/home/lct/synchdb-mysql-upgrade-lab` |
| Private PostgreSQL programs | `pgsql/`, copied from `/home/lct/repos/postgres/inst` |
| Private PostgreSQL data | `pgdata/` |
| Target port / database | `55432` / `upgradedb` |
| Target socket directory | `socket/` |
| MySQL container / host port | `synchdb-upgrade-mysql-lab` / `23306` |
| MySQL source table | `upsource.events` |
| Connector / target table | `mysqlsync` / `app.events` |

The passwords and `trust` authentication below are only for this isolated rehearsal. Do not reuse them for a production deployment. This example uses the native Debezium snapshot path, not FDW.

## 1. Open Bash and check the machine

Optionally record the full interactive terminal, including entered commands and output, before proceeding:

```bash
umask 077
script -q -f -c 'bash --noprofile --norc' /tmp/synchdb-mysql-upgrade-lab-session.log
```

If not recording, open Bash with `bash`. Then run:

```bash
set -euo pipefail
umask 077
test "$(id -un)" = lct

LAB=/home/lct/synchdb-mysql-upgrade-lab
BASE_PG=/home/lct/repos/postgres/inst
PG_PREFIX="$LAB/pgsql"
PGDATA="$LAB/pgdata"
PGSOCKET="$LAB/socket"
PGPORT=55432
MYSQL_CONTAINER=synchdb-upgrade-mysql-lab
MYSQL_PORT=23306
OLD_SRC="$LAB/src-v1.4"
NEW_SRC="$LAB/src-v1.5"
OLD_COMMIT=0756e247ca07083244559cb4616c5dedf1b3da97
NEW_COMMIT=40c6a58a49fc5f4745f5594584e226e8c4138618

export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$JAVA_HOME/lib/server${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
unset DBZ_ENGINE_DIR

# Use a new directory; do not overwrite an earlier rehearsal.
test ! -e "$LAB"
install -d -m 700 "$LAB" "$LAB/logs" "$PGSOCKET"

git --version
java -version
mvn -version
python3 --version
docker version
"$BASE_PG/bin/pg_config" --version
ss -ltn '( sport = :55432 or sport = :23306 )'
if docker container inspect "$MYSQL_CONTAINER" >/dev/null 2>&1; then
  echo "Container name already exists; stop here and choose a new lab name."
  exit 1
fi
```

The PostgreSQL version should be `18.6`. Both selected ports must be free: `ss` should show only its heading. Stop and choose unused ports if it lists a listener. Maven uses the JDK selected by `PATH`; the Java project compiles with release 17, and this machine's JDK 25 can build it.

## 2. Download both versions and prepare private PostgreSQL programs

```bash
git clone --branch v1.4 --single-branch \
  https://github.com/Hornetlabs/synchdb.git "$OLD_SRC"
git -C "$OLD_SRC" checkout --detach "$OLD_COMMIT"
test "$(git -C "$OLD_SRC" rev-parse HEAD)" = "$OLD_COMMIT"

git clone --branch refactor --single-branch \
  https://github.com/hanjianqiao/synchdb.git "$NEW_SRC"
git -C "$NEW_SRC" checkout --detach "$NEW_COMMIT"
test "$(git -C "$NEW_SRC" rev-parse HEAD)" = "$NEW_COMMIT"

install -d -m 700 "$PG_PREFIX"
cp -a "$BASE_PG/." "$PG_PREFIX/"
PG_CONFIG="$PG_PREFIX/bin/pg_config"
PGBIN="$PG_PREFIX/bin"
MIGRATION_DIR="$NEW_SRC/upgrade-v1.4-to-v1.5"
TARGET_DSN="host=$PGSOCKET port=$PGPORT dbname=upgradedb user=lct connect_timeout=5"

"$PG_CONFIG" --bindir
"$PG_CONFIG" --pkglibdir
"$PG_CONFIG" --sharedir
"$PG_CONFIG" --includedir-server
"$PG_CONFIG" --pgxs
test "$("$PG_CONFIG" --bindir)" = "$PGBIN"
test "$("$PG_CONFIG" --pkglibdir)" = "$PG_PREFIX/lib"
test "$("$PG_CONFIG" --sharedir)" = "$PG_PREFIX/share"
test -f "$("$PG_CONFIG" --pgxs)"
```

All displayed paths must point into the private `pgsql/` directory. If they do not, stop before building or installing. The original installation has been copied; every `make` command below explicitly selects the private `pg_config`.

## 3. Build and install v1.4 in the private prefix

```bash
cd "$OLD_SRC"
make -j4 USE_PGXS=1 PG_CONFIG="$PG_CONFIG" PG_MAJOR=18 WITH_OLR=0 \
  2>&1 | tee "$LAB/logs/v1.4-build-c.log"
make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" PG_MAJOR=18 WITH_OLR=0 build_dbz \
  2>&1 | tee "$LAB/logs/v1.4-build-java.log"
make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" PG_MAJOR=18 WITH_OLR=0 install \
  2>&1 | tee "$LAB/logs/v1.4-install-c.log"
make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" PG_MAJOR=18 WITH_OLR=0 install_dbz \
  2>&1 | tee "$LAB/logs/v1.4-install-java.log"

test -f "$PG_PREFIX/lib/synchdb.so"
test -f "$PG_PREFIX/lib/dbz_engine/dbz-engine-1.1.0.jar"
test -d "$PG_PREFIX/lib/dbz_engine/lib"
```

Keep `PG_MAJOR=18` in these commands to select the PostgreSQL major version explicitly. The v1.4 Makefile still evaluates its `pg_config --majorversion` query, so each old-version `make` invocation can print `invalid argument: --majorversion` even with this parameter. PostgreSQL 18 does not support that option. For this MySQL build with `WITH_OLR=0`, the message does not prevent a successful build or installation; no patch to the old source is required. Continue only if each command succeeds and the file checks above pass.

`WITH_OLR=0` is sufficient for MySQL. `install_dbz` replaces the Java directory, including dependencies; do not use it against the installation of a running server.

## 4. Start a separate MySQL source

```bash
cat > "$LAB/mysql.env" <<'ENV'
MYSQL_ROOT_PASSWORD=LabRoot42
MYSQL_USER=mysqluser
MYSQL_PASSWORD=LabExample42
ENV
chmod 600 "$LAB/mysql.env"

docker run -d --name "$MYSQL_CONTAINER" \
  --env-file "$LAB/mysql.env" \
  -p "127.0.0.1:$MYSQL_PORT:3306" \
  quay.io/debezium/example-mysql:2.6 \
  --server-id=11092026 --log-bin=mysql-bin \
  --binlog-format=ROW --binlog-row-image=FULL

mysql_sql() {
  docker exec -i "$MYSQL_CONTAINER" sh -c \
    'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -h127.0.0.1 -P3306 -uroot --batch --raw --skip-column-names "$@"' sh "$@"
}

mysql_ready=0
for attempt in {1..90}; do
  if mysql_sql -e 'SELECT 1' >/dev/null 2>&1; then
    mysql_ready=1
    break
  fi
  sleep 2
done
if [ "$mysql_ready" != 1 ]; then
  docker logs --tail 100 "$MYSQL_CONTAINER"
  echo "MySQL TCP startup timed out; resolve the startup error before continuing."
  exit 1
fi

mysql_sql <<'SQL'
SELECT VERSION(), @@log_bin, @@binlog_format, @@binlog_row_image;
CREATE DATABASE upsource;
CREATE USER 'upreader'@'%' IDENTIFIED BY 'LabReader42';
GRANT SELECT, LOCK TABLES ON upsource.* TO 'upreader'@'%';
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'upreader'@'%';
CREATE TABLE upsource.events (
  id int PRIMARY KEY,
  label varchar(80),
  qty int,
  amount decimal(12,2),
  stamp datetime
) ENGINE=InnoDB;
INSERT INTO upsource.events VALUES
  (1,'alpha',10,10.25,'2026-11-09 01:02:03'),
  (2,'beta',20,20.50,'2026-11-09 01:02:03'),
  (3,'gamma',30,30.75,'2026-11-09 01:02:03');
SELECT * FROM upsource.events ORDER BY id;
SQL
```

The first result must show binlog enabled, `ROW`, and `FULL`. The source table contains IDs `1,2,3`.

## 5. Start the private PostgreSQL target and v1.4 synchronization

```bash
"$PGBIN/initdb" -D "$PGDATA" --locale=C --encoding=UTF8 --auth=trust
cat >> "$PGDATA/postgresql.conf" <<EOF
listen_addresses = '127.0.0.1'
port = $PGPORT
unix_socket_directories = '$PGSOCKET'
shared_preload_libraries = 'synchdb'
max_worker_processes = 16
synchdb.max_connector_workers = 4
synchdb.snapshot_engine = 'debezium'
synchdb.dbz_snapshot_thread_num = 1
synchdb.jvm_max_heap_size = 256
synchdb.jvm_max_direct_buffer_size = 64
synchdb.error_handling_strategy = 'exit'
synchdb.dbz_skipped_oeprations = 't'
EOF

unset DBZ_ENGINE_DIR
"$PGBIN/pg_ctl" -D "$PGDATA" -l "$LAB/logs/v1.4-postgresql.log" -w start
"$PGBIN/createdb" -w -h "$PGSOCKET" -p "$PGPORT" -U lct upgradedb

pg_sql() {
  "$PGBIN/psql" -w -X -v ON_ERROR_STOP=1 --dbname="$TARGET_DSN" "$@"
}

pg_sql <<SQL
CREATE EXTENSION synchdb VERSION '1.0' CASCADE;
CREATE SCHEMA app;
SELECT synchdb_add_conninfo(
  'mysqlsync', '127.0.0.1', $MYSQL_PORT,
  'upreader', 'LabReader42',
  'upsource', 'null', 'upsource.events', 'null', 'mysql'
);
SELECT synchdb_add_objmap('mysqlsync', 'table', 'upsource.events'::name, 'app.events');
SELECT synchdb_start_engine_bgw('mysqlsync', 'initial');
SQL
```

The three old control functions return `0`. The strings `'null'` in the old API are intentional. Do not replace them with SQL `NULL`: the old function is strict. Create only the target schema; the MySQL snapshot creates `app.events`.

## 6. Wait for full field equality and check old CDC

Define the comparison once. It compares all rows, ordered by primary key, with explicit decimal and timestamp formats. A row-count match alone is insufficient. These functions only poll; they do not submit control requests or perform an upgrade.

```bash
wait_equal() {
  local stage="$1"
  for attempt in {1..90}; do
    mysql_sql -e "SELECT id,label,qty,CAST(amount AS DECIMAL(12,2)),DATE_FORMAT(stamp,'%Y-%m-%d %H:%i:%s') FROM upsource.events ORDER BY id" \
      > "$LAB/logs/$stage-source.tsv"
    if pg_sql -qAt -F $'\t' -c "SELECT id,label,qty,amount::numeric(12,2),to_char(stamp,'YYYY-MM-DD HH24:MI:SS') FROM app.events ORDER BY id" \
      > "$LAB/logs/$stage-target.tsv" 2> "$LAB/logs/$stage-query-error.log"; then
      if cmp -s "$LAB/logs/$stage-source.tsv" "$LAB/logs/$stage-target.tsv"; then
        echo "$stage: all rows and fields match"
        cat "$LAB/logs/$stage-target.tsv"
        return 0
      fi
    fi
    sleep 2
  done
  echo "$stage: timed out; do not continue to the next stage"
  diff -u "$LAB/logs/$stage-source.tsv" "$LAB/logs/$stage-target.tsv" || true
  cat "$LAB/logs/$stage-query-error.log"
  pg_sql -c "SELECT name,pid,stage,state,err FROM synchdb_state_view WHERE name='mysqlsync';"
  return 1
}

wait_equal old-initial
snapshot_complete=0
for attempt in {1..90}; do
  if [ "$(pg_sql -qAt -c "SELECT snapshot_end_ts>0 FROM synchdb_snapstats WHERE name='mysqlsync'")" = t ]; then
    snapshot_complete=1
    break
  fi
  sleep 2
done
test "$snapshot_complete" = 1
pg_sql -c "SELECT * FROM synchdb_snapstats WHERE name='mysqlsync';"
pg_sql -c '\d app.events'

mysql_sql <<'SQL'
INSERT INTO upsource.events VALUES (4,'delta',40,40.25,'2026-11-09 01:02:04');
UPDATE upsource.events SET label='legacy-cdc',qty=21,amount=22.00 WHERE id=2;
DELETE FROM upsource.events WHERE id=3;
SQL
wait_equal old-cdc
```

After the initial snapshot, IDs are `1,2,3`. After the CDC operations, IDs are `1,2,4`; row 2 is `legacy-cdc`, quantity `21`, amount `22.00`. Both `wait_equal` calls must succeed.

## 7. Build the new C and Java code while v1.4 is running

```bash
cd "$NEW_SRC"
python3 "$MIGRATION_DIR/scripts/verify_bundle.py" --repo "$NEW_SRC"

make -j4 USE_PGXS=1 PG_CONFIG="$PG_CONFIG" PG_MAJOR=18 WITH_OLR=0 \
  2>&1 | tee "$LAB/logs/v1.5-build-c.log"
make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" PG_MAJOR=18 WITH_OLR=0 build_dbz \
  2>&1 | tee "$LAB/logs/v1.5-build-java.log"
```

Do **not** run either installation target yet. The private PostgreSQL server still has the old C library and Java engine loaded.

Save the original target table OID, encrypted-password hash, mapping, and connector inventory:

```bash
pg_sql -qAt -F $'\t' -c "SELECT 'app.events'::regclass::oid,md5(data->>'pwd') FROM synchdb_conninfo WHERE name='mysqlsync'" \
  > "$LAB/logs/before-identity.tsv"
pg_sql -c "SELECT * FROM synchdb_objmap WHERE name='mysqlsync';" \
  > "$LAB/logs/before-mapping.txt"
python3 "$MIGRATION_DIR/scripts/synchdb_migrate.py" inspect \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --output "$LAB/logs/before-inventory.json"
```

The helper uses the supplied `psql`; these manual helper commands do not require `psycopg2`. No passwords need to be re-entered during the upgrade.

## 8. Stop old collection, stop PostgreSQL, and make a cold backup

Stop writing to the source except for the explicit rehearsal statements below. Keep captured-table DDL unchanged.

```bash
wait_equal before-stop
OLD_PID="$(pg_sql -qAt -c "SELECT pid FROM synchdb_state_view WHERE name='mysqlsync'")"
[[ "$OLD_PID" =~ ^[0-9]+$ ]]
test "$OLD_PID" -gt 0
pg_sql -c "SELECT synchdb_stop_engine_bgw('mysqlsync');"

old_stopped=0
for attempt in {1..90}; do
  if [ "$(pg_sql -qAt -c "SELECT (NOT isactive) AND NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$OLD_PID) FROM synchdb_conninfo WHERE name='mysqlsync'")" = t ]; then
    old_stopped=1
    break
  fi
  sleep 2
done
test "$old_stopped" = 1
pg_sql -c "SELECT name,isactive FROM synchdb_conninfo WHERE name='mysqlsync';"

"$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop
if "$PGBIN/pg_ctl" -D "$PGDATA" status; then
  echo "The target is still running; stop here."
  exit 1
fi

BACKUP="$LAB/backup-v1.4"
test ! -e "$BACKUP"
install -d -m 700 "$BACKUP"
cp -a "$PGDATA" "$BACKUP/pgdata"
cp -a "$PG_PREFIX" "$BACKUP/pgsql"

LEGACY_OFFSET="$PGDATA/pg_synchdb/mysql_mysqlsync_upgradedb_offsets.dat"
LEGACY_HISTORY="$PGDATA/pg_synchdb/mysql_mysqlsync_upgradedb_schemahistory.dat"
test -s "$LEGACY_OFFSET"
test -s "$LEGACY_HISTORY"
sha256sum "$LEGACY_OFFSET" "$LEGACY_HISTORY" > "$LAB/logs/legacy-files.sha256"
cat "$LAB/logs/legacy-files.sha256"
```

Continue only when the worker has exited, the connector is inactive, PostgreSQL is stopped, both copies succeeded, and the original offset and history are nonempty. This fresh lab has no external tablespaces or WAL; a real deployment must back up those external directories separately.

## 9. Produce identifiable changes during downtime

This deliberately leaves the old target behind the source. The upgraded connector must replay these changes from the original position.

```bash
mysql_sql <<'SQL'
INSERT INTO upsource.events VALUES (5,'downtime',50,50.25,'2026-11-09 01:02:05');
UPDATE upsource.events SET label='during-stop',qty=11 WHERE id=1;
DELETE FROM upsource.events WHERE id=4;
SELECT * FROM upsource.events ORDER BY id;
SQL
```

The source now has IDs `1,2,5`. Keep the MySQL container running and retain its binlogs.

## 10. Install the new programs while PostgreSQL is stopped

```bash
cd "$NEW_SRC"
make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" PG_MAJOR=18 WITH_OLR=0 install \
  2>&1 | tee "$LAB/logs/v1.5-install-c.log"
make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" PG_MAJOR=18 WITH_OLR=0 install_dbz \
  2>&1 | tee "$LAB/logs/v1.5-install-java.log"

test -f "$PG_PREFIX/share/extension/synchdb--1.0--1.1.sql"
test -f "$PG_PREFIX/share/extension/synchdb--1.1.sql"
cmp "$NEW_SRC/synchdb.so" "$PG_PREFIX/lib/synchdb.so"
cmp "$NEW_SRC/src/backend/debezium/target/dbz-engine-1.1.0.jar" \
  "$PG_PREFIX/lib/dbz_engine/dbz-engine-1.1.0.jar"
```

The main JAR filename is `dbz-engine-1.1.0.jar` in both versions, so its filename does not identify the installed release. Install the complete Java output and dependencies together with the new C and SQL files.

## 11. Start maintenance mode, update catalogs, and import the old position

```bash
unset DBZ_ENGINE_DIR
"$PGBIN/pg_ctl" -D "$PGDATA" \
  -l "$LAB/logs/maintenance-postgresql.log" -w \
  -o "-c shared_preload_libraries=''" start

pg_sql -c 'SHOW shared_preload_libraries;'

# Separate autocommit commands: ALTER SYSTEM cannot run in a transaction block.
pg_sql -c "ALTER SYSTEM SET synchdb.dbz_snapshot_thread_num = '1';"
pg_sql -c "ALTER SYSTEM SET synchdb.dbz_skipped_operations = 't';"
pg_sql -c 'SELECT pg_reload_conf();'

python3 "$MIGRATION_DIR/scripts/synchdb_migrate.py" preflight \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --maintenance --require-legacy-resume

pg_sql -c "ALTER EXTENSION synchdb UPDATE TO '1.1';"
pg_sql -c "SELECT extversion FROM pg_extension WHERE extname='synchdb';"

python3 "$MIGRATION_DIR/scripts/synchdb_migrate.py" migrate-offsets \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" --connector mysqlsync
python3 "$MIGRATION_DIR/scripts/synchdb_migrate.py" migrate-offsets \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" --connector mysqlsync --apply

pg_sql -c "SELECT generation,origin,phase,active FROM synchdb_artifacts WHERE connector_id=(SELECT connector_id FROM synchdb_conninfo WHERE name='mysqlsync');"
ls -l "$PGDATA/pg_synchdb"
sha256sum --check "$LAB/logs/legacy-files.sha256"

pg_sql -qAt -F $'\t' -c "SELECT 'app.events'::regclass::oid,md5(data->>'pwd') FROM synchdb_conninfo WHERE name='mysqlsync'" \
  > "$LAB/logs/after-identity.tsv"
cmp "$LAB/logs/before-identity.tsv" "$LAB/logs/after-identity.tsv"
pg_sql -c "SELECT * FROM synchdb_objmap WHERE name='mysqlsync';" \
  > "$LAB/logs/after-mapping.txt"
cmp "$LAB/logs/before-mapping.txt" "$LAB/logs/after-mapping.txt"
```

Expected results:

- Maintenance preload is empty, and preflight reports `MAINTENANCE_CHECKS_PASSED`.
- The extension version is `1.1`.
- The import plan reports `PLAN_ONLY`; applying reports `IMPORTED` with a generation UUID.
- The artifact has `origin=LEGACY_COPY`, `phase=ENGINE_OWNED`, and `active=t`.
- Original-file checksums, target table OID, encrypted-password hash, and mapping all match.

The old skipped-operation spelling is still accepted for compatibility. The correctly spelled persistent setting above takes precedence and preserves this rehearsal's original policy of skipping TRUNCATE.

This example has one connector in one isolated SynchDB database and unambiguous legacy filenames. The server can check that inventory directly, so no `--claims-file` is needed here. For an existing deployment with multiple SynchDB databases or ambiguous names, follow the ownership-inventory steps in [Manual upgrade](manual-upgrade.md).

Do not rename raw offset files or replace them with empty files. The import copies and registers the complete old state while preserving the original files.

The directory listing should contain both original files and new files named `<database_oid>_<connector_id>_<generation>_offsets.dat` and `<database_oid>_<connector_id>_<generation>_schemahistory.dat`. The connector ID and generation are UUIDs assigned by the new catalog; do not construct these filenames manually.

## 12. Leave maintenance, start normally, and resume CDC

Stop maintenance, then issue a fresh normal start. Do not use `pg_ctl restart`: it can reuse the maintenance command-line override and leave preload disabled.

```bash
"$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop
unset DBZ_ENGINE_DIR
"$PGBIN/pg_ctl" -D "$PGDATA" -l "$LAB/logs/v1.5-postgresql.log" -w start

pg_sql -c 'SHOW shared_preload_libraries;'
pg_sql -c 'SHOW synchdb.dbz_snapshot_thread_num;'
pg_sql -c 'SHOW synchdb.dbz_skipped_operations;'

python3 "$MIGRATION_DIR/scripts/synchdb_migrate.py" control \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --connector mysqlsync --operation START --mode initial --timeout 180 \
  2>&1 | tee "$LAB/logs/v1.5-start-receipt.log"

wait_equal downtime-catchup
pg_sql -c "SELECT name,desired_state,actual_state,last_operation,last_sqlstate,last_error FROM synchdb_control_view WHERE name='mysqlsync';"
pg_sql -c "SELECT name,pid,stage,state,err FROM synchdb_state_view WHERE name='mysqlsync';"
pg_sql -c "SELECT * FROM synchdb_snapstats WHERE name='mysqlsync';"
test "$(pg_sql -qAt -c "SELECT rows=0 AND tables=0 AND snapshot_begin_ts=0 AND snapshot_end_ts=0 FROM synchdb_snapstats WHERE name='mysqlsync'")" = t
test "$(pg_sql -qAt -c "SELECT count(*)=1 FROM synchdb_artifacts WHERE connector_id=(SELECT connector_id FROM synchdb_conninfo WHERE name='mysqlsync') AND active AND origin='LEGACY_COPY' AND phase='ENGINE_OWNED'")" = t
```

Preload must be `synchdb`, snapshot threads `1`, and skipped operations `t`. The START receipt must succeed. Then the target must catch up to IDs `1,2,5`, with row 1 changed to `during-stop`, quantity `11`. Snapshot counters must remain zero: MySQL `initial` uses the completed imported offset and continues CDC here.

A `SUCCEEDED` receipt means the control operation completed; it does not prove that all source changes have reached the target. That is why the full field comparison follows it.

## 13. Verify new CDC and restart recovery

First exercise INSERT, UPDATE, and DELETE after the upgrade:

```bash
mysql_sql <<'SQL'
INSERT INTO upsource.events VALUES (6,'new-row',60,60.25,'2026-11-09 01:02:06');
UPDATE upsource.events SET label='new-cdc',qty=22 WHERE id=2;
DELETE FROM upsource.events WHERE id=5;
SQL
wait_equal new-cdc

GENERATION_BEFORE="$(pg_sql -qAt -c "SELECT generation FROM synchdb_artifacts WHERE active AND connector_id=(SELECT connector_id FROM synchdb_conninfo WHERE name='mysqlsync')")"
test -n "$GENERATION_BEFORE"
printf '%s\n' "$GENERATION_BEFORE" > "$LAB/logs/generation-before-restart.txt"

# Do not issue a connector STOP: preserve its desired running state.
"$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop
unset DBZ_ENGINE_DIR
"$PGBIN/pg_ctl" -D "$PGDATA" -l "$LAB/logs/v1.5-postgresql.log" -w start

mysql_sql <<'SQL'
INSERT INTO upsource.events VALUES (7,'after-restart',70,70.25,'2026-11-09 01:02:07');
UPDATE upsource.events SET label='restart-cdc',qty=61 WHERE id=6;
DELETE FROM upsource.events WHERE id=2;
SQL
wait_equal restart-cdc

GENERATION_AFTER="$(pg_sql -qAt -c "SELECT generation FROM synchdb_artifacts WHERE active AND connector_id=(SELECT connector_id FROM synchdb_conninfo WHERE name='mysqlsync')")"
test "$GENERATION_BEFORE" = "$GENERATION_AFTER"
test "$(pg_sql -qAt -c "SELECT rows=0 AND tables=0 AND snapshot_begin_ts=0 AND snapshot_end_ts=0 FROM synchdb_snapstats WHERE name='mysqlsync'")" = t
sha256sum --check "$LAB/logs/legacy-files.sha256"
pg_sql -c "SELECT name,pid,stage,state,err FROM synchdb_state_view WHERE name='mysqlsync';"
```

There is no new START request after this normal restart: preloaded SynchDB restores the connector's running intent. The artifact generation must remain unchanged.

The final source and target must both contain exactly:

```text
1  during-stop    11  10.25  2026-11-09 01:02:03
6  restart-cdc    61  60.25  2026-11-09 01:02:06
7  after-restart  70  70.25  2026-11-09 01:02:07
```

If every comparison and assertion passed, this rehearsal has verified the old initial snapshot and CDC, preservation of the target table and credentials, replay of downtime changes, new CDC, and restart continuation for these rows and column types. It does not establish that every production table or type is correct; compare the actual business data before accepting a real upgrade.

## 14. Common errors

| Symptom | Check or correction |
| --- | --- |
| The lab directory, container, or port already exists | Stop before creating anything else; select a fresh lab directory/name and unused ports. Do not remove an existing deployment to make the example fit. |
| Compilation uses another PostgreSQL version or installation | Pass `USE_PGXS=1 PG_CONFIG="$PG_CONFIG" PG_MAJOR=18 WITH_OLR=0` on every `make` invocation and check the private paths. |
| Old-version `make` prints `invalid argument: --majorversion` | The v1.4 Makefile queries an option PostgreSQL 18 does not support, even when `PG_MAJOR=18` is supplied. Keep the commands unchanged; no source patch is needed for this MySQL `WITH_OLR=0` build. Check that `make` exits successfully and the expected files exist before continuing. |
| `libjvm.so` cannot be loaded | Restore the JDK's `lib/server` in `LD_LIBRARY_PATH` before starting PostgreSQL. |
| Wrong Java code is loaded, or the JAR cannot be found | `unset DBZ_ENGINE_DIR` before server startup and install the complete new Java output into the private prefix. Replacing only the C library is insufficient. |
| MySQL never becomes ready | Inspect `docker logs "$MYSQL_CONTAINER"`; readiness must use TCP because the image can start a temporary local initialization server. |
| `ALTER SYSTEM cannot run inside a transaction block` | Execute each `ALTER SYSTEM` separately as above; do not combine them in one `psql -c` string or `BEGIN`. |
| Maintenance preflight rejects the deployment | Check preload is empty, old workers have exited, the connector is inactive, and matching upgrade SQL was installed. Stay in maintenance until it passes. |
| Normal startup still has empty preload | Stop, then use a fresh `pg_ctl ... start` without the maintenance `-o` override. |
| Startup logs `No active engine artifact generation` | This pinned candidate can read the displayed position before the engine has attached its saved state. In the rehearsal, this appeared once at the first START and once after restart; capture then ran normally and all data checks passed. Confirm the imported artifact exists, the engine is `RUNNING`, and source and target match. Do not recreate offsets based on this message alone. If it persists or catch-up fails, stop and investigate. |
| The START helper times out | Query its original operation ID using the command below. A timeout does not cancel the request; do not submit another START. |
| The target has the right count but wrong values | Inspect the TSV diff and connector errors. Do not accept the upgrade on counts or a successful receipt alone. |
| A new snapshot appears or required binlog/history is missing | Keep collection stopped and investigate the imported state and retained source logs. Do not use `always`, drop/recreate the connector or extension, or edit/rename offsets to bypass the problem. |

To inspect an existing request, replace `123` with the operation ID from the saved receipt:

```bash
python3 "$MIGRATION_DIR/scripts/synchdb_migrate.py" operation \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" --id 123 --timeout 60
```

If submission disconnected before showing an ID, read the existing `last_operation` first:

```bash
pg_sql -c "SELECT name,last_operation,actual_state,last_sqlstate,last_error FROM synchdb_control_view WHERE name='mysqlsync';"
```

Keep `$LAB/logs`, the cold backup, and MySQL binlogs while investigating. A rollback requires the matching old programs and old PGDATA together, with the necessary source logs still available; replacing only an old `.so` is insufficient.

## 15. Optionally stop the rehearsal and keep its data

Run this only after the preceding checks are complete:

```bash
python3 "$MIGRATION_DIR/scripts/synchdb_migrate.py" control \
  --dsn "$TARGET_DSN" --psql "$PGBIN/psql" \
  --connector mysqlsync --operation STOP --timeout 180
"$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop
docker stop "$MYSQL_CONTAINER"
```

These commands keep the lab directories, backup, logs, and stopped MySQL container for inspection. No command in this guide removes the original PostgreSQL installation or the lab data. If you started `script` in stage 1, use `exit` to finish the terminal recording.
