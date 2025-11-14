import os
import subprocess
import time
import re
from common import run_pg_query, run_pg_query_one, getConnectorName, create_and_start_synchdb_connector, run_remote_query

def output_tpcc_buildschema_results(dbvendor, pg_diff_ms, tables, rows):
    rows_per_sec = rows * 1000 / pg_diff_ms if pg_diff_ms > 0 else 0
    
    with open(f"{dbvendor}_hammerdb_benchmark_result.txt", "a") as f:
        f.write(f"\n--- SynchDB Initial Snapshot Result ({dbvendor}) ---\n")
        f.write(f"replication duration: {pg_diff_ms} ms\n")
        f.write(f"tables migrated: {tables}\n")
        f.write(f"rows migrated: {rows}\n")
        f.write(f"rows per second: {rows_per_sec:.2f}\n")


def output_tpcc_and_synchdb_results(dbvendor, nopm, tpm, pg_diff_ms, avgbatchsize, dmls, batchesdone):
    dmls_per_sec = dmls * 1000 / pg_diff_ms if pg_diff_ms > 0 else 0

    with open(f"{dbvendor}_hammerdb_benchmark_result.txt", "a") as f:
        if nopm == 0 and tpm == 0:
            f.write(f"\n--- SynchDB Initial Snapshot Result ({dbvendor}) ---\n")
            f.write(f"replication duration: {pg_diff_ms} ms\n")
            f.write(f"average batch size: {avgbatchsize}\n")
            f.write(f"number of DMLs: {dmls}\n")
            f.write(f"total batches: {batchesdone}\n")
            f.write(f"DMLs per second: {dmls_per_sec:.2f}\n")
        else:
            f.write(f"\n--- SynchDB CDC Result ({dbvendor}) ---\n")
            f.write(f"tpm: {tpm}\n")
            f.write(f"nopm: {nopm}\n")
            f.write(f"replication duration: {pg_diff_ms} ms\n")
            f.write(f"average batch size: {avgbatchsize}\n")
            f.write(f"number of DMLs: {dmls}\n")
            f.write(f"total batches: {batchesdone}\n")
            f.write(f"DMLs per second: {dmls_per_sec:.2f}\n")

def test_tpcc_buildschema(pg_cursor, dbvendor, hammerdb):
    name = getConnectorName(dbvendor) + "_tpcc"
    
    if os.path.exists(f"{dbvendor}_hammerdb_benchmark_result.txt"):
        os.remove(f"{dbvendor}_hammerdb_benchmark_result.txt")

    if dbvendor == "mysql":
        subprocess.run(["docker", "exec", "hammerdb", "./hammerdbcli", "auto", "/buildschema.tcl"], check=True) 
        result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial", srcdb="tpcc")
        assert result == 0
    elif dbvendor == "sqlserver":
        subprocess.run(["docker", "exec", "-e", "LD_LIBRARY_PATH=/usr/local/unixODBC/lib:/home/instantclient_21_18/", "-e", "TMP=/tmp", "-e", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/mssql-tools18/bin:/opt/mssql-tools18/bin:/usr/local/unixODBC/bin", "hammerdb", "./hammerdbcli", "auto", "/buildschema.tcl"], check=True)

        run_remote_query(dbvendor, "EXEC sys.sp_cdc_enable_db", srcdb="tpcc")
        run_remote_query(dbvendor, "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customer', @role_name = NULL, @supports_net_changes = 0", srcdb="tpcc")
        run_remote_query(dbvendor, "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'district', @role_name = NULL, @supports_net_changes = 0", srcdb="tpcc")
        run_remote_query(dbvendor, "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'history', @role_name = NULL, @supports_net_changes = 0", srcdb="tpcc")
        run_remote_query(dbvendor, "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'item', @role_name = NULL, @supports_net_changes = 0", srcdb="tpcc")
        run_remote_query(dbvendor, "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'new_order', @role_name = NULL, @supports_net_changes = 0", srcdb="tpcc")
        run_remote_query(dbvendor, "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'order_line', @role_name = NULL, @supports_net_changes = 0", srcdb="tpcc")
        run_remote_query(dbvendor, "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'orders', @role_name = NULL, @supports_net_changes = 0", srcdb="tpcc")
        run_remote_query(dbvendor, "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'stock', @role_name = NULL, @supports_net_changes = 0", srcdb="tpcc")
        run_remote_query(dbvendor, "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'warehouse', @role_name = NULL, @supports_net_changes = 0", srcdb="tpcc")
        result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial", srcdb="tpcc")
        assert result == 0
    else:
        # oracle: drop orders table that were creasted as default. Hammerdb has aconflicting table
        run_remote_query(dbvendor, "DROP TABLE orders")

        subprocess.run(["docker", "exec", "-e", "LD_LIBRARY_PATH=/usr/local/unixODBC/lib:/home/instantclient_21_18/", "-e", "TMP=/tmp", "hammerdb", "./hammerdbcli", "auto", "/buildschema.tcl"], check=True) 

        # enable update and delete by enabling supplemental log data on all columns
        run_remote_query(dbvendor, "ALTER TABLE customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        run_remote_query(dbvendor, "ALTER TABLE district ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        run_remote_query(dbvendor, "ALTER TABLE history ADD SUPPLEMENTAL LOG DATA (ALL) COLUMN")
        run_remote_query(dbvendor, "ALTER TABLE item ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        run_remote_query(dbvendor, "ALTER TABLE new_order ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        run_remote_query(dbvendor, "ALTER TABLE order_line ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        run_remote_query(dbvendor, "ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        run_remote_query(dbvendor, "ALTER TABLE stock ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        run_remote_query(dbvendor, "ALTER TABLE warehouse ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        # give oracle some time to archive redo logs
        time.sleep(30)
        result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial", srcdb="FREE")
        assert result == 0
    
    # try to get the first non-zero timestammps from synchdb_stats_view
    loopcnt=0
    while True:
        rows = run_pg_query_one(pg_cursor, f"SELECT tables, rows, snapshot_begin_ts FROM synchdb_snapstats WHERE name = '{name}'")
        if rows != None and rows[2] != 0:
            tables = rows[0]
            nrows = rows[1]
            begin_ts = rows[2]
            break
        else:
            if loopcnt <= 240:
                print("sleep 1s before trying to fetch snapshot begin timestamps again...")
                loopcnt = loopcnt + 1
            else:
                print(f"no timestamps are fetched after {loopcnt} fetches...")
                break
            time.sleep(1)

    assert begin_ts > 0
    print(f"snapshot begin timetamp is {begin_ts}, current tables = {tables} and rows = {nrows}")

    time.sleep(20)
    
    stopcount=0
    lasttup=None
    while True:
        currtup = run_pg_query_one(pg_cursor, f"SELECT tables, rows, snapshot_end_ts FROM synchdb_snapstats WHERE name = '{name}'")
        print(currtup)

        if currtup == lasttup:
            if stopcount > 5:
                print("no new updates in stats. Considering current sync session as done")
                tables = currtup[0]
                nrows = currtup[1]
                end_ts = currtup[2]
                print(f"snapshot end timestamp is {end_ts}, current tables = {tables} and rows = {nrows}")
                break    
            else:
                stopcount = stopcount + 1
        else:
            lasttup = currtup
            stopcount = 0
        time.sleep(20)

    assert end_ts > 0
    
    pg_diff_ms = end_ts - begin_ts
    print(f"initial snapshotting tpcc tables takes {pg_diff_ms} ms that migrates {tables} tables totalling {nrows} rows")

    ## fixme ##
    output_tpcc_buildschema_results(dbvendor, pg_diff_ms, tables, nrows)
    assert True

# TPCC test is running at the same time as synchdb replication
def test_tpcc_run(pg_cursor, dbvendor, hammerdb, tpccmode):
    name = getConnectorName(dbvendor) + "_tpcc"
    
    # this test depends on test_tpcc_buildschema, so connector must be running now as a result from previous test run
    rows = run_pg_query_one(pg_cursor, f"SELECT state FROM synchdb_state_view WHERE name ='{name}'")
    assert rows[0] == "polling"
    
    # reset stats
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_reset_stats('{name}')")
    assert rows[0] == 0

    #subprocess.Popen(["docker", "exec", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"], stdout=subprocess.DEVNULL)
    if tpccmode == "serial":
        # we pause connector first, and resume only after tpcc test run has completed 
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_pause_engine('{name}')")
        assert rows[0] == 0

        #subprocess.run(["docker", "exec", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"], check=True)
        if dbvendor == "mysql":
            #nopm, tpm = map(int, re.search(r"achieved (\d+) NOPM from (\d+)",
            #    subprocess.run(
            #        ["docker", "exec", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"],
            #        check=True, capture_output=True, text=True
            #    ).stdout
            #).groups())
            result = subprocess.run(
                    ["docker", "exec", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"],
                    check=True, capture_output=True  # no text=True -> returns bytes
                    )
            out = result.stdout.decode("utf-8", errors="replace").replace("\r", "")
            m = re.search(r"achieved\s+(\d+)\s+NOPM\s+from\s+(\d+)", out, re.I)
            nopm, tpm = map(int, m.groups())
        else:
            result = subprocess.run(
                ["docker", "exec", "-e", "LD_LIBRARY_PATH=/usr/local/unixODBC/lib:/home/instantclient_21_18/", "-e", "TMP=/tmp", "-e", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/mssql-tools18/bin:/opt/mssql-tools18/bin:/usr/local/unixODBC/bin", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"],
                check=True, capture_output=True
            )
            result = result.stdout.decode('utf-8', errors='replace')

            print("HammerDB output:\n", result)

            match = re.search(r"achieved (\d+) NOPM from (\d+)", result)
            assert match, "Regex did not match HammerDB output"
            nopm, tpm = map(int, match.groups())
            #nopm, tpm = map(int, re.search(r"achieved (\d+) NOPM from (\d+)",
            #    subprocess.run(
            #        ["docker", "exec", "-e", "LD_LIBRARY_PATH=/usr/local/unixODBC/lib:/home/instantclient_21_18/", "-e", "TMP=/tmp", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"],
            #        check=True, capture_output=True, text=True
            #    ).stdout
            #).groups())
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_resume_engine('{name}')")
        assert rows[0] == 0
    else:
        # run tpcc test in background while synchdb is polling
        if dbvendor == "mysql":
            proc = subprocess.Popen(["docker", "exec", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)
        else:
            proc = subprocess.Popen(["docker", "exec", "-e", "LD_LIBRARY_PATH=/usr/local/unixODBC/lib:/home/instantclient_21_18/", "-e", "TMP=/tmp", "-e", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/mssql-tools18/bin:/opt/mssql-tools18/bin:/usr/local/unixODBC/bin", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)

            stdout, stderr = proc.communicate()
            stdout = stdout.decode("utf-8", errors="replace")
            stderr = stderr.decode("utf-8", errors="replace")

    # try to get the first non-zero timestammps from synchdb_stats_view
    loopcnt=0
    while True:
        rows = run_pg_query_one(pg_cursor, f"SELECT ddls, dmls, average_batch_size, batches_done, first_src_ts, first_pg_ts FROM synchdb_genstats, synchdb_cdcstats WHERE synchdb_genstats.name = synchdb_cdcstats.name and synchdb_cdcstats.name = '{name}'")
        #rows = run_pg_query_one(pg_cursor, f"SELECT ddls, dmls, avg_batch_size, batches_done, first_src_ts, first_dbz_ts, first_pg_ts FROM synchdb_stats_view WHERE name = '{name}'")
        if rows != None and rows[4] != 0 and rows[5] != 0:
            ddls = rows[0]
            dmls = rows[1]
            avgbatchsize = rows[2]
            batchesdone = rows[3]
            first_src_ts = rows[4]
            first_pg_ts = rows[5]
            break
        else:
            if loopcnt <= 240:
                print("sleep 1s before trying to fetch first timestamps again...")
                loopcnt = loopcnt + 1
            else:
                print(f"no timestamps are fetched after {loopcnt} fetches...")
                break
            time.sleep(1)
    
    assert first_src_ts > 0 and first_pg_ts > 0
    print(f"first timetamps are: {first_src_ts} {first_pg_ts}, total batches done = {batchesdone}")

    time.sleep(5)
    stopcount=0
    lasttup=None
    while True:
        currtup = run_pg_query_one(pg_cursor, f"SELECT ddls, dmls, average_batch_size, batches_done, last_src_ts, last_pg_ts FROM synchdb_genstats, synchdb_cdcstats WHERE synchdb_genstats.name = synchdb_cdcstats.name and synchdb_cdcstats.name = '{name}'")
        #currtup = run_pg_query_one(pg_cursor, f"SELECT ddls, dmls, avg_batch_size, batches_done, last_src_ts, last_dbz_ts, last_pg_ts FROM synchdb_stats_view WHERE name = '{name}'")
        print(currtup)

        if currtup == lasttup:
            if stopcount > 5:
                print("no new updates in stats. Considering current sync session as done")
                ddls = currtup[0]
                dmls = currtup[1]
                avgbatchsize = currtup[2]
                batchesdone = currtup[3]
                last_src_ts = currtup[4]
                last_pg_ts = currtup[5]
                print(f"last timestamps are {last_src_ts} {last_pg_ts}")
                break
            else:
                stopcount = stopcount + 1
        else:
            lasttup = currtup
            stopcount = 0
        time.sleep(20)

    assert last_src_ts > 0 and last_pg_ts > 0

    pg_diff_ms = last_pg_ts - first_pg_ts

    # in non-serial mode, read nopm and tpm from PIPE
    if tpccmode == "parallel":
        stdout, stderr = proc.communicate()
        stdout = stdout.decode("utf-8", errors="replace")
        stderr = stderr.decode("utf-8", errors="replace")
        match = re.search(r"achieved (\d+) NOPM from (\d+)", stdout)
        if match:
            nopm, tpm = map(int, match.groups())
        else:
            raise RuntimeError("TPCC result not found in output")

    print(f"tpcc CDC test takes {pg_diff_ms} ms with average batch size = {avgbatchsize}, DML processed = {dmls}, total batches done = {batchesdone}. {dbvendor} tpcc results: nopm = {nopm} tpm = {tpm}")
    output_tpcc_and_synchdb_results(dbvendor, nopm, tpm, pg_diff_ms, avgbatchsize, dmls, batchesdone)

    assert True
