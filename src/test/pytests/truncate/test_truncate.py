import json

import pytest
from psycopg2 import sql
from psycopg2.extras import Json
from conftest import WITH_OLR


def event(connector="mysql", table="items", db="inventory", schema=None, **extra):
    source = {"connector": connector, "db": db, "table": table,
              "snapshot": False, "ts_ms": 1000}
    if connector != "mysql":
        source["schema"] = schema or "OWNER"
    return {"payload": {"source": source, "op": "t", "ts_ms": 2000, **extra}}


def olr(table="items", db="inventory", ddl=None, owner="OWNER", **extra):
    return {"scn": 100, "c_scn": 101, "c_idx": 0, "db": db, "tm": 1000000000,
            "payload": [{"op": "ddl", "schema": {"owner": owner, "table": table},
                         "sql": ddl or f'TRUNCATE TABLE "{table}"', **extra}]}


def olr_internal_dml(op="u", table="LOG_MINING_FLUSH"):
    message = olr(table=table)
    payload = message["payload"][0]
    payload.pop("sql")
    payload.update(op=op, before={"LAST_SCN": 99}, after={"LAST_SCN": 100})
    payload["schema"]["columns"] = [
        {"name": "LAST_SCN", "type": "number", "precision": 19, "scale": 0, "nullable": True}]
    return message


def process(cur, events, connector="mysql", skipped="", casing=1, mapping=None, spi=False):
    if not isinstance(events, list):
        events = [events]
    encoded = [e if isinstance(e, str) else json.dumps(e, ensure_ascii=False) for e in events]
    cur.execute("SELECT test_batch(%s,%s,%s,%s,%s,%s)",
                (encoded, connector, skipped, casing, Json(mapping or {}), spi))
    return cur.fetchone()[0]


def rows(cur, schema="inventory", table="items"):
    cur.execute(sql.SQL("SELECT * FROM {} ORDER BY 1").format(sql.Identifier(schema, table)))
    return cur.fetchall()


def target(cur, schema="inventory", table="items", pk=True):
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    cur.execute(sql.SQL("CREATE TABLE {} (id integer {}, val text)").format(
        sql.Identifier(schema, table), sql.SQL("PRIMARY KEY" if pk else "")))
    cur.execute(sql.SQL("INSERT INTO {} VALUES (1,'old'),(2,'older')").format(sql.Identifier(schema, table)))


@pytest.mark.parametrize("connector", ["mysql", "postgresql", "oracle"])
@pytest.mark.parametrize("pk", [False, True])
def test_rowless_empty_and_repeat(db, connector, pk):
    target(db, pk=pk)
    result = process(db, [event(connector)] * 3, connector=connector)
    assert rows(db) == []
    assert result["truncates"] == result["ddls"] == 3
    assert result["dmls"] == result["bad_events"] == 0
    assert result["confirmed"]


@pytest.mark.parametrize("casing,source_schema,source_table,dest_schema,dest_table", [
    (0, "Mixed DB", 'a.b" c', "Mixed DB", 'a.b" c'),
    (1, "Mixed DB", 'A.B" C', "mixed db", 'a.b" c'),
    (2, "Mixed DB", 'a.b" c', "MIXED DB", 'A.B" C'),
])
def test_source_components_and_case(db, casing, source_schema, source_table, dest_schema, dest_table):
    target(db, dest_schema, dest_table)
    result = process(db, event(table=source_table, db=source_schema), casing=casing)
    assert result["truncates"] == 1
    assert rows(db, dest_schema, dest_table) == []
    db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(dest_schema)))


@pytest.mark.parametrize("destination", ["inventory.MiXeD", '"inventory"."a.b"',
    '"s.ch ema"."a""b c"', '"a.b"', "items"])
def test_mapping_and_search_path(db, destination):
    if destination == '"s.ch ema"."a""b c"':
        schema, table = "s.ch ema", 'a"b c'
    elif destination == '"inventory"."a.b"':
        schema, table = "inventory", "a.b"
    elif destination == '"a.b"':
        schema, table = "public", "a.b"
    elif destination == "items":
        schema, table = "public", "items"
    else:
        schema, table = "inventory", "MiXeD"
    db.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(schema, table))) if schema != "s.ch ema" else None
    target(db, schema, table)
    target(db, "shadow", table)
    db.execute("SET search_path=shadow,public")
    try:
        result = process(db, event(db="Source", table='x.y" z'),
                         mapping={'Source.x.y" z': destination}, casing=2)
        assert result["truncates"] == 1
        assert rows(db, schema, table) == []
        assert len(rows(db, "shadow", table)) == 2
    finally:
        db.execute("RESET search_path")
        db.execute(sql.SQL("DROP TABLE {} CASCADE").format(sql.Identifier(schema, table)))
        if schema == "s.ch ema":
            db.execute(sql.SQL("DROP SCHEMA {}").format(sql.Identifier(schema)))


@pytest.mark.parametrize("skipped", ["t", "c,u,t", " c , t , d "])
def test_filtered(db, skipped):
    target(db)
    result = process(db, event(), skipped=skipped)
    assert len(rows(db)) == 2
    assert result["bad_events"] == result["truncates"] == result["ddls"] == 0
    assert result["confirmed"]


@pytest.mark.parametrize("field,value", [("db", None), ("db", ""), ("db", 12),
    ("table", None), ("table", ""), ("table", []), ("connector", None),
    ("connector", 12), ("connector", "unknown"), ("snapshot", []),
    ("schema", None), ("schema", 4)])
def test_invalid_source(db, field, value):
    target(db)
    e = event("oracle")
    e["payload"]["source"][field] = value
    result = process(db, e, connector="oracle")
    assert result["bad_events"] == 1
    assert result["truncates"] == result["ddls"] == 0
    assert not result["confirmed"]
    assert len(rows(db)) == 2


@pytest.mark.parametrize("invalid", ["{", {"payload": {"source": 1, "op": "t"}},
    {"payload": {"op": "t"}}, {"payload": {"op": "t", "status": "BEGIN"}}, {"payload": None}])
def test_invalid_json_and_envelope(db, invalid):
    result = process(db, invalid)
    assert result["bad_events"] == 1
    assert not result["confirmed"]


@pytest.mark.parametrize("op", [12, True, "tt", "", None])
def test_invalid_operation(db, op):
    result = process(db, event(op=op))
    assert result["bad_events"] == 1
    assert result["truncates"] == 0
    assert not result["confirmed"]


@pytest.mark.parametrize("constraint", ["missing", "foreign_key"])
def test_execution_failure_and_batch_rollback(db, constraint):
    target(db, table="first")
    target(db)
    if constraint == "foreign_key":
        db.execute("CREATE TABLE inventory.child (parent integer REFERENCES inventory.items(id))")
        bad = event()
    else:
        bad = event(table="missing")
    result = process(db, [event(table="first"), bad, event()])
    assert result["failed"] and not result["confirmed"]
    assert result["truncates"] == result["ddls"] == 0
    assert len(rows(db, table="first")) == len(rows(db)) == 2


@pytest.mark.parametrize("action", ["TRUNCATE", "TRUNCATE TABLE"])
@pytest.mark.parametrize("skipped", ["", "t"])
def test_explicit_schema_change(db, action, skipped):
    target(db)
    e = event()
    del e["payload"]["op"]
    e["payload"]["tableChanges"] = [{"type": action, "id": '"inventory"."items"', "table": None}]
    result = process(db, e, skipped=skipped)
    assert result["bad_events"] == 0
    assert result["truncates"] == result["ddls"] == (0 if skipped else 1)
    assert len(rows(db)) == (2 if skipped else 0)


@pytest.mark.parametrize("connector,ddl", [("mysql", "TRUNCATE TABLE `inventory`.`items`;"),
    ("mysql", "/* change */ TRUNCATE inventory.items"),
    ("oracle", 'TRUNCATE TABLE "OWNER"."items" REUSE STORAGE')])
def test_ddl_text(db, connector, ddl):
    target(db)
    e = event(connector)
    del e["payload"]["op"]
    del e["payload"]["source"]["table"]
    e["payload"]["ddl"] = ddl
    e["payload"]["tableChanges"] = []
    result = process(db, e, connector=connector)
    assert result["truncates"] == 1
    assert rows(db) == []


def test_structure_mapping_sequence_preserved(db):
    db.execute("""CREATE TABLE inventory.items(id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, val text);
        CREATE INDEX value_idx ON inventory.items(val);
        INSERT INTO inventory.items(val) VALUES ('old');
        INSERT INTO synchdb_attribute VALUES ('truncate_test','mysql','inventory.items'::regclass,1,
            'inventory.items','id','int')""")
    db.execute("SELECT oid FROM pg_class WHERE oid='inventory.items'::regclass OR oid='inventory.value_idx'::regclass")
    oids = db.fetchall()
    db.execute("SELECT * FROM synchdb_attribute")
    attributes = db.fetchall()
    result = process(db, event())
    assert result["truncates"] == 1
    db.execute("SELECT oid FROM pg_class WHERE oid='inventory.items'::regclass OR oid='inventory.value_idx'::regclass")
    assert db.fetchall() == oids
    db.execute("SELECT * FROM synchdb_attribute")
    assert db.fetchall() == attributes
    db.execute("INSERT INTO inventory.items(val) VALUES ('new') RETURNING id")
    assert db.fetchone()[0] == 2


def dml(op, before=None, after=None):
    e = event(op=op, before=before, after=after)
    e["schema"] = {"fields": [{"field": "before", "fields": [
        {"field": "id", "type": "int32"}, {"field": "val", "type": "string"}]}]}
    return e


@pytest.mark.parametrize("pk,spi", [(False, False), (True, False), (True, True)])
def test_following_dml(db, spi, pk):
    target(db, pk=pk)
    create = dml("c", after={"id": 3, "val": "new"})
    update = dml("u", before={"id": 3, "val": "new"}, after={"id": 3, "val": "updated"})
    delete = dml("d", before={"id": 3, "val": "updated"})
    result = process(db, [event(), create, update, delete], spi=spi)
    assert result["truncates"] == result["ddls"] == 1
    assert result["creates"] == result["updates"] == result["deletes"] == 1
    assert result["bad_events"] == 0
    assert rows(db) == []


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
@pytest.mark.parametrize("skipped", ["", "t"])
@pytest.mark.parametrize("ddl", ['TRUNCATE TABLE "items"', 'TRUNCATE TABLE "OWNER"."items" DROP STORAGE'])
def test_olr_truncate(db, skipped, ddl):
    target(db)
    result = process(db, olr(ddl=ddl), connector="olr", skipped=skipped)
    assert result["confirmed"] and result["scn"] == 101
    assert result["truncates"] == result["ddls"] == (0 if skipped else 1)
    assert result["bad_events"] == 0
    assert len(rows(db)) == (2 if skipped else 0)


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
def test_olr_original_case_and_mapping(db):
    target(db, "Mixed DB", 'a.b" c')
    e = olr(table='a.b" c', db="Mixed DB", ddl='TRUNCATE TABLE "a.b"" c"')
    result = process(db, e, connector="olr", casing=0)
    assert result["truncates"] == 1
    assert rows(db, "Mixed DB", 'a.b" c') == []
    db.execute('DROP SCHEMA "Mixed DB" CASCADE')


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
def test_olr_failure_no_confirm(db):
    target(db)
    result = process(db, [olr(), olr(table="missing"), olr()], connector="olr")
    assert not result["confirmed"] and result["scn"] == 0
    assert result["truncates"] == result["ddls"] == 0
    assert len(rows(db)) == 2


def test_raw_entry_logging(db, database):
    target(db)
    e = event()
    process(db, e)
    log = database[1].read_text()
    assert "connector truncate_test Debezium event: " + json.dumps(e) in log
    if WITH_OLR:
        e = olr()
        process(db, e, connector="olr")
        assert "connector truncate_test native OLR event: " + json.dumps(e) in database[1].read_text()


@pytest.mark.parametrize("mapping", ["a.b.c", '"unterminated', '"escaped""', "a..b", "", "x" * 64,
                                      "items; TRUNCATE inventory.items"])
def test_invalid_mapping_cannot_target_another_table(db, mapping):
    target(db)
    result = process(db, event(), mapping={"inventory.items": mapping})
    assert result["failed"] and not result["confirmed"]
    assert result["bad_events"] == 1
    assert result["truncates"] == result["ddls"] == 0
    assert len(rows(db)) == 2


@pytest.mark.parametrize("snapshot", [False, "false"])
def test_snapshot_encodings(db, snapshot):
    target(db)
    e = event()
    e["payload"]["source"]["snapshot"] = snapshot
    result = process(db, e)
    assert result["truncates"] == 1


def test_mapping_lookup_long_composite_source(db):
    schema, table = "d" * 63, "t" * 63
    target(db, schema, table)
    result = process(db, event("oracle", db=schema, table=table, schema="s" * 63),
                     connector="oracle", mapping={"other.items": "unused"})
    assert result["truncates"] == 1
    assert rows(db, schema, table) == []
    db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.parametrize("ddl", ["TRUNCATE TABLE items; TRUNCATE TABLE other", "TRUNCATE TABLE items,other"])
def test_ddl_not_silently_partially_applied(db, ddl):
    target(db)
    e = event()
    del e["payload"]["op"]
    e["payload"]["ddl"] = ddl
    e["payload"]["tableChanges"] = []
    result = process(db, e)
    assert result["truncates"] == 0
    assert not result["confirmed"]
    assert len(rows(db)) == 2


def test_schema_change_special_identifiers(db):
    target(db, "s.ch ema", 'a"b.c')
    e = event(db="s.ch ema", table='a"b.c')
    del e["payload"]["op"]
    e["payload"]["tableChanges"] = [{"type": "TRUNCATE", "id": '"s.ch ema"."a""b.c"'}]
    result = process(db, e)
    assert result["truncates"] == 1
    assert rows(db, "s.ch ema", 'a"b.c') == []
    db.execute('DROP SCHEMA "s.ch ema" CASCADE')


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
@pytest.mark.parametrize("key,value", [("db", None), ("db", 1), ("scn", None), ("c_scn", "101"),
                                      ("c_idx", []), ("payload", None)])
def test_olr_invalid_event_no_confirm(db, key, value):
    e = olr()
    e[key] = value
    result = process(db, e, connector="olr")
    assert result["bad_events"] == 1
    assert not result["confirmed"] and result["scn"] == 0
    assert result["truncates"] == 0


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
def test_olr_mapping_stats_and_attributes(db):
    target(db, table="Mapped")
    db.execute("INSERT INTO synchdb_attribute VALUES ('truncate_test','olr','inventory.\"Mapped\"'::regclass,1,'inventory.OWNER.items','id','number')")
    db.execute("SELECT * FROM synchdb_attribute")
    before = db.fetchall()
    result = process(db, olr(), connector="olr", casing=2,
                     mapping={"inventory.OWNER.items": 'inventory."Mapped"'})
    assert result["truncates"] == result["ddls"] == 1
    assert rows(db, table="Mapped") == []
    db.execute("SELECT * FROM synchdb_attribute")
    assert db.fetchall() == before


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
@pytest.mark.parametrize("ddl", ["  /*ddl*/ TRUNCATE TABLE items,other", "TRUNCATE TABLE items; DROP TABLE items"])
def test_olr_bad_truncate_no_ack(db, ddl):
    target(db)
    result = process(db, olr(ddl=ddl), connector="olr")
    assert result["bad_events"] == 1
    assert result["truncates"] == result["ddls"] == 0
    assert not result["confirmed"]
    assert len(rows(db)) == 2


def test_minimal_truncate_metadata(db):
    target(db)
    result = process(db, {"payload": {"op": "t", "source": {
        "connector": "mysql", "db": "inventory", "table": "items"}}})
    assert result["truncates"] == result["ddls"] == 1
    assert result["bad_events"] == 0


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
def test_olr_skip_requires_no_table_data(db):
    e = olr()
    del e["db"]
    del e["payload"][0]["schema"]
    result = process(db, e, connector="olr", skipped="t")
    assert result["confirmed"] and result["scn"] == 101
    assert result["bad_events"] == result["truncates"] == result["ddls"] == 0


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
def test_olr_multiple_payloads_not_partially_applied(db):
    target(db)
    e = olr()
    e["payload"].append(olr(table="missing")["payload"][0])
    result = process(db, e, connector="olr")
    assert not result["confirmed"]
    assert result["bad_events"] == 1 and result["truncates"] == 0
    assert len(rows(db)) == 2


def test_empty_schema_metadata_consumed(db):
    e = event()
    del e["payload"]["op"]
    e["payload"]["ddl"] = "SET character_set_server=utf8mb4"
    e["payload"]["tableChanges"] = []
    result = process(db, e)
    assert result["confirmed"]
    assert result["bad_events"] == result["ddls"] == result["truncates"] == 0


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
@pytest.mark.parametrize("owner,table", [("OTHER_OWNER", "items"), ("OWNER", "LOG_MINING_FLUSH")])
def test_olr_source_filters_are_consumed(db, owner, table):
    target(db)
    result = process(db, olr(owner=owner, table=table), connector="olr")
    assert result["confirmed"] and result["scn"] == 101
    assert result["bad_events"] == result["truncates"] == result["ddls"] == 0
    assert len(rows(db)) == 2


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
@pytest.mark.parametrize("op", ["c", "u", "d"])
@pytest.mark.parametrize("table", ["LOG_MINING_FLUSH", "log_mining_flush"])
def test_olr_internal_dml_is_consumed(db, op, table):
    # Debezium's flush table has no target table or attribute mappings.
    result = process(db, olr_internal_dml(op=op, table=table), connector="olr")
    assert not result["failed"] and result["confirmed"] and result["scn"] == 101
    assert result["bad_events"] == result["dmls"] == result["ddls"] == result["truncates"] == 0
    assert result["creates"] == result["updates"] == result["deletes"] == 0


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
@pytest.mark.parametrize("key,value", [("scn", None), ("c_scn", "101"), ("c_idx", []), ("db", 1)])
def test_olr_internal_dml_requires_valid_metadata(db, key, value):
    message = olr_internal_dml()
    message[key] = value
    result = process(db, message, connector="olr")
    assert result["failed"] and not result["confirmed"] and result["scn"] == 0
    assert result["bad_events"] == 1


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
def test_olr_internal_dml_does_not_block_following_event(db):
    target(db)
    result = process(db, [olr_internal_dml(), olr()], connector="olr")
    assert not result["failed"] and result["confirmed"] and result["scn"] == 101
    assert result["truncates"] == result["ddls"] == 1
    assert result["bad_events"] == result["dmls"] == 0
    assert rows(db) == []


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
def test_olr_internal_dml_does_not_confirm_failed_batch(db):
    target(db)
    result = process(db, [olr(), olr_internal_dml(), olr(table="missing")], connector="olr")
    assert result["failed"] and not result["confirmed"] and result["scn"] == 0
    assert result["truncates"] == result["ddls"] == 0
    assert len(rows(db)) == 2


@pytest.mark.parametrize("ddl", ["TRUNCATE TABLE other.items", "TRUNCATE TABLE inventory.schema.items",
                                 "TRUNCATE TABLE inventory.items; DROP TABLE inventory.items"])
def test_conflicting_ddl_metadata_does_not_fall_through(db, ddl):
    target(db)
    e = event()
    del e["payload"]["op"]
    e["payload"]["ddl"] = ddl
    e["payload"]["tableChanges"] = [{"type": "ALTER", "id": '"inventory"."items"',
        "table": {"columns": [], "primaryKeyColumnNames": []}}]
    result = process(db, e)
    assert not result["confirmed"] and result["bad_events"] == 1
    assert result["truncates"] == result["ddls"] == 0
    assert len(rows(db)) == 2


@pytest.mark.parametrize("spi", [False, True])
@pytest.mark.parametrize("mapped", [False, True])
def test_special_destination_following_dml(db, spi, mapped):
    schema, table = "s.ch ema", 'a.b" c'
    target(db, schema, table)
    mapping = {"inventory.items": '\"s.ch ema\".\"a.b\"\" c\"'} if mapped else {}
    source_db, source_table = ("inventory", "items") if mapped else (schema, table)
    messages = [event(db=source_db, table=source_table),
                dml("c", after={"id": 3, "val": "new"}),
                dml("u", before={"id": 3, "val": "new"}, after={"id": 3, "val": "updated"}),
                dml("d", before={"id": 3, "val": "updated"})]
    for message in messages:
        message["payload"]["source"].update(db=source_db, table=source_table)
    result = process(db, messages, spi=spi, casing=0, mapping=mapping)
    assert result["truncates"] == result["creates"] == result["updates"] == result["deletes"] == 1, result
    assert result["bad_events"] == 0
    assert rows(db, schema, table) == []
    db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
@pytest.mark.parametrize("spi", [False, True])
def test_olr_following_dml_with_special_mapping(db, spi):
    target(db, table='a.b" c')
    messages = [olr()]
    for op, before, after in [("c", None, {"id": 3, "val": "new"}),
                              ("u", {"id": 3, "val": "new"}, {"id": 3, "val": "updated"}),
                              ("d", {"id": 3, "val": "updated"}, None)]:
        message = olr()
        message["payload"][0].update(op=op, before=before, after=after)
        message["payload"][0]["schema"]["columns"] = [{"name": "id", "type": "number"},
                                                       {"name": "val", "type": "varchar2"}]
        messages.append(message)
    result = process(db, messages, connector="olr", spi=spi,
                     mapping={"inventory.OWNER.items": 'inventory.\"a.b\"\" c\"'})
    assert result["truncates"] == result["creates"] == result["updates"] == result["deletes"] == 1, result
    assert result["bad_events"] == 0
    assert rows(db, table='a.b" c') == []


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
def test_olr_keywords_inside_identifier(db):
    target(db, table="drop table as x")
    result = process(db, olr(table="drop table as x"), connector="olr")
    assert result["truncates"] == result["ddls"] == 1
    assert rows(db, table="drop table as x") == []
