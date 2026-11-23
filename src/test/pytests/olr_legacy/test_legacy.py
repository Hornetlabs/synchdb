"""Regression cases for legacy cursors, transaction boundaries and rollback."""
import json

import psycopg2
import pytest


LEGACY = {"protocol": "UNVERIFIED_LEGACY_OLR_CDC_V1", "owner": "DBZUSER"}
FDW = {"protocol": "UNVERIFIED_FDW_CDC_HANDOFF_V1", "owner": "DBZUSER"}
NATIVE = {"owner": "DBZUSER"}


def record(op="c", scn=100, c_scn=50, index=3, rowid=1):
    return {"scn": scn, "c_scn": c_scn, "c_idx": index, "rowid": rowid,
            "payload": [{"op": op, "schema": {"owner": "DBZUSER", "table": "ITEMS"}}]}


def prefix(db, records, legacy):
    db.execute("SELECT olr_fixture_prefix(%s::text[],%s)",
               ([json.dumps(item) for item in records], legacy))
    return db.fetchone()[0]


def filtered(db, records, proof=LEGACY, snapshot=0):
    db.execute("SELECT olr_fixture_filter(%s::jsonb,%s,100,50,2,%s::jsonb[])",
               (json.dumps(proof), snapshot, [json.dumps(item) for item in records]))
    return db.fetchone()[0]


def write(db, records, fail=False):
    db.execute("SELECT olr_fixture_write(%s::jsonb,0,100,50,2,%s::jsonb[],%s)",
               (json.dumps(LEGACY), [json.dumps(item) for item in records], fail))


def test_native_still_requires_complete_transactions(db):
    assert prefix(db, [record("begin"), record(), record("commit")], False) == 3
    with pytest.raises(psycopg2.Error, match="requires transaction commit SCN"):
        prefix(db, [record(), record("commit")], False)


def test_legacy_can_finish_first_transaction_without_begin(db):
    records = [record(), record("u", index=4), record("commit", index=5),
               record("begin", scn=101, index=6), record(scn=101, index=7),
               record("commit", scn=101, index=8)]
    assert prefix(db, records, True) == 6


def test_only_unchanged_imported_cursor_can_have_leading_fragment(db):
    proof = dict(LEGACY, legacy_scn="100", legacy_c_scn="50", legacy_c_idx="2")
    for position, expected in (((100, 50, 2), True), ((101, 50, 2), False),
                               ((100, 51, 2), False), ((100, 50, 3), False)):
        db.execute("SELECT olr_fixture_allow_fragment(%s::jsonb,%s,%s,%s)",
                   (json.dumps(proof), *position))
        assert db.fetchone()[0] is expected


def test_native_cursor_never_permits_a_leading_fragment(db):
    db.execute("SELECT olr_fixture_allow_fragment(%s::jsonb,100,50,2)", (json.dumps(NATIVE),))
    assert db.fetchone()[0] is False


@pytest.mark.parametrize("table,snapshot_table", [("DBZUSER.ITEMS", "null"), ("null", "DBZUSER.ITEMS")])
def test_no_data_subset_check_is_deferred_to_actual_generation(db, table, snapshot_table):
    db.execute("SELECT olr_fixture_scope('no_data',%s,%s,false)", (table, snapshot_table))
    assert db.fetchone()[0] is True


@pytest.mark.parametrize("mode", ["initial", "always"])
def test_new_native_subset_snapshot_is_rejected(db, mode):
    with pytest.raises(psycopg2.Error, match="complete configured schema"):
        db.execute("SELECT olr_fixture_scope(%s,'DBZUSER.ITEMS','null',false)", (mode,))


@pytest.mark.parametrize("fdw", [False, True])
def test_initial_only_subset_remains_available(db, fdw):
    db.execute("SELECT olr_fixture_scope('initial_only','DBZUSER.ITEMS','null',%s)", (fdw,))
    assert db.fetchone()[0] is True


def test_native_no_data_full_schema_remains_available(db):
    db.execute("SELECT olr_fixture_scope('no_data','null','null',false)")
    assert db.fetchone()[0] is True


def test_legacy_fragment_waits_for_commit(db):
    assert prefix(db, [record(), record("u", index=4)], True) == 0


def test_legacy_permits_only_one_leading_fragment(db):
    records = [record(), record("commit", index=4), record("u", scn=101, index=5)]
    with pytest.raises(psycopg2.Error, match="requires transaction commit SCN"):
        prefix(db, records, True)


def test_legacy_fragment_cannot_cross_commit_scn(db):
    with pytest.raises(psycopg2.Error, match="requires transaction commit SCN"):
        prefix(db, [record(), record("commit", scn=101)], True)


def test_checkpoint_cannot_split_legacy_fragment(db):
    with pytest.raises(psycopg2.Error, match="checkpoint split"):
        prefix(db, [record(), record("chkpt")], True)


def test_same_oracle_scn_later_olr_index_is_applied_once(db):
    result = filtered(db, [record(index=2), record(index=3), record(index=3), record(index=4)])
    assert result == {"apply": [False, True, False, True], "scn": 100, "c_scn": 50, "c_idx": 4}


def test_next_lwn_can_reset_index_to_zero(db):
    result = filtered(db, [record(c_scn=51, index=0), record(c_scn=50, index=999)])
    assert result == {"apply": [True, False], "scn": 100, "c_scn": 51, "c_idx": 0}


@pytest.mark.parametrize("proof", [NATIVE, FDW])
def test_zero_snapshot_is_rejected_outside_legacy_protocol(db, proof):
    with pytest.raises(psycopg2.Error, match="without a completed snapshot"):
        filtered(db, [record()], proof=proof)


def test_native_snapshot_filter_remains_in_effect(db):
    result = filtered(db, [record(scn=100), record(scn=101, index=4)], proof=NATIVE, snapshot=100)
    assert result["apply"] == [False, True]


def test_legacy_excludes_source_fence_but_advances_cursor(db):
    item = record()
    item["payload"][0]["schema"]["table"] = "SYNCHDB_OLR_FENCE"
    result = filtered(db, [item])
    assert result["apply"] == [False]
    assert result["c_idx"] == 3


def test_rows_and_production_cursor_update_commit_together(db):
    write(db, [record(index=2), record(index=3, rowid=10)])
    db.execute("SELECT id FROM fixture.items")
    assert db.fetchall() == [(10,)]
    db.execute("SELECT snapshot_scn,scn,c_scn,c_idx FROM fixture.positions")
    assert db.fetchone() == (0, 100, 50, 3)


def test_failure_after_cursor_write_rolls_back_rows_and_cursor(db):
    with pytest.raises(psycopg2.Error, match="failure after row and cursor writes"):
        write(db, [record(rowid=10)], fail=True)
    db.execute("SELECT count(*) FROM fixture.items")
    assert db.fetchone()[0] == 0
    db.execute("SELECT scn,c_scn,c_idx FROM fixture.positions")
    assert db.fetchone() == (100, 50, 2)


def test_explicit_rollback_preserves_old_cursor(db):
    db.execute("BEGIN")
    write(db, [record(rowid=10)])
    db.execute("ROLLBACK")
    db.execute("SELECT count(*) FROM fixture.items")
    assert db.fetchone()[0] == 0
    db.execute("SELECT scn,c_scn,c_idx FROM fixture.positions")
    assert db.fetchone() == (100, 50, 2)
