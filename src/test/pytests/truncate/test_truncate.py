"""Event and target semantics; live capture/replay is in test_009_truncate.py."""
import copy
import json
import os
import time

import psycopg2
import pytest

WITH_OLR = os.environ.get("SYNCHDB_TEST_WITH_OLR", "0") == "1"


def event(connector="postgresql", database="fixture", schema="public", table="items"):
    return {"payload": {"op": "t", "source": {"connector": connector,
        "db": database, "schema": schema, "table": table, "snapshot": False}}}


def native(statement="TRUNCATE TABLE SRC.ITEMS", table="ITEMS"):
    return {"db": "fixture", "scn": 100, "c_scn": 100, "c_idx": 1,
            "payload": [{"op": "ddl", "schema": {"owner": "SRC", "table": table},
                         "sql": statement}]}


def schema_event(connector="postgresql", identifier='"public"."items"', ddl=None):
    result = event(connector)
    del result["payload"]["op"]
    result["payload"]["tableChanges"] = [{"type": "TRUNCATE", "id": identifier}]
    if ddl is not None:
        result["payload"]["ddl"] = ddl
    return result


def apply(db, record, native=False, mapped=None):
    records = record if isinstance(record, list) else [record]
    db.execute("SELECT truncate_fixture_batch(%s,%s,%s)",
               ([json.dumps(item) for item in records], native, mapped))
    return db.fetchone()[0]


def rows(db, target="fixture.items"):
    db.execute("SELECT * FROM " + target + " ORDER BY 1")
    return db.fetchall()


def rejected(db, record, native=False):
    with pytest.raises(psycopg2.Error):
        apply(db, record, native=native)
    assert rows(db) == [(1, "before")]


@pytest.mark.parametrize("connector", ["mysql", "postgresql", "oracle"])
@pytest.mark.parametrize("images", [False, True])
def test_rowless_repeated_empty(db, connector, images):
    record = event(connector)
    if images:
        record["payload"].update(before=None, after=None)
    for _ in range(2):
        assert apply(db, record) == {"truncates": 1, "ddls": 1, "bad": 0}
        assert rows(db) == []


@pytest.mark.parametrize("field", ["db", "table", "schema"])
@pytest.mark.parametrize("value", [None, "", 1, [], {}])
def test_malformed_source(db, field, value):
    record = event()
    record["payload"]["source"][field] = value
    rejected(db, record)


@pytest.mark.parametrize("field", ["before", "after"])
@pytest.mark.parametrize("value", [{"id": 1}, [], 1])
def test_truncate_never_accepts_a_row_image(db, field, value):
    record = event()
    record["payload"][field] = value
    rejected(db, record)


@pytest.mark.parametrize("snapshot", [True, "true", "last", "incremental"])
def test_snapshot_cannot_clear_a_table(db, snapshot):
    record = event()
    record["payload"]["source"]["snapshot"] = snapshot
    rejected(db, record)


@pytest.mark.parametrize("snapshot", [False, "false", None])
def test_snapshot_encodings(db, snapshot):
    record = event()
    record["payload"]["source"]["snapshot"] = snapshot
    assert apply(db, record)["truncates"] == 1


@pytest.mark.parametrize("connector,identifier,ddl", [
    ("mysql", '"fixture"."items"', None),
    ("mysql", '"fixture"."items"', "TRUNCATE TABLE `fixture`.`items`;"),
    ("postgresql", '"public"."items"', "TRUNCATE TABLE public.items"),
    ("oracle", '"SRC"."ITEMS"', "TRUNCATE TABLE src.items DROP ALL STORAGE")])
def test_explicit_schema_truncate(db, connector, identifier, ddl):
    record = schema_event(connector, identifier, ddl)
    if connector == "oracle":
        record["payload"]["source"].update(schema="SRC", table="ITEMS")
    assert apply(db, record)["truncates"] == 1
    assert rows(db) == []


@pytest.mark.parametrize("ddl", ["/*ddl*/ TRUNCATE TABLE public.items; --tail",
    "TRUNCATE items", "TRUNCATE TABLE fixture.public.items"])
def test_empty_schema_change_truncate_is_applied(db, ddl):
    record = schema_event(ddl=ddl)
    record["payload"]["tableChanges"] = []
    del record["payload"]["source"]["table"]
    assert apply(db, record)["truncates"] == 1
    assert rows(db) == []


def test_ddl_only_truncate_is_applied(db):
    record = event()
    del record["payload"]["op"]
    record["payload"]["ddl"] = "TRUNCATE TABLE public.items"
    assert apply(db, record)["truncates"] == 1
    assert rows(db) == []


@pytest.mark.parametrize("ddl", ["SET character_set_server=utf8mb4", "/*!40101 SET SQL_MODE='' */"])
def test_other_empty_schema_metadata_remains_a_noop(db, ddl):
    record = schema_event("mysql", '"fixture"."items"', ddl)
    record["schema"] = {"name": "io.debezium.connector.mysql.SchemaChangeValue"}
    record["payload"]["tableChanges"] = []
    record["payload"]["ts_ms"] = 1
    assert apply(db, record) == {"truncates": 0, "ddls": 0, "bad": 0}
    assert rows(db) == [(1, "before")]


@pytest.mark.parametrize("mutation", ["table", "schema", "database", "id_database", "ddl", "operation", "multi"])
def test_inconsistent_schema_metadata_cannot_clear_a_table(db, mutation):
    record = schema_event(ddl="TRUNCATE TABLE public.items")
    payload = record["payload"]
    if mutation == "table":
        payload["tableChanges"][0]["id"] = '"public"."other"'
    elif mutation == "schema":
        payload["schemaName"] = "other"
    elif mutation == "database":
        payload["databaseName"] = "other"
    elif mutation == "id_database":
        payload["tableChanges"][0]["id"] = '"other"."public"."items"'
    elif mutation == "ddl":
        payload["ddl"] = "DROP TABLE public.items"
    elif mutation == "operation":
        payload["tableChanges"][0]["type"] = "ALTER"
    else:
        payload["tableChanges"].append(copy.deepcopy(payload["tableChanges"][0]))
    rejected(db, record)


@pytest.mark.parametrize("ddl", ["TRUNCATE TABLE items,other", "TRUNCATE TABLE items CASCADE",
    "TRUNCATE TABLE items RESTART IDENTITY", "TRUNCATE TABLE items; TRUNCATE TABLE other",
    "TRUNCATE TABLE items; DROP TABLE other", 'TRUNCATE TABLE "unterminated',
    "TRUNCATE TABLE items /*unterminated", "TRUNCATE TABLE items REUSE STORAGE UNKNOWN"])
def test_unsupported_sql_has_no_partial_effect(db, ddl):
    rejected(db, schema_event(ddl=ddl))


def test_special_mapping_and_long_exact_source(db):
    db.execute('CREATE SCHEMA "target schema"; CREATE TABLE "target schema"."a.b""c"(id int)')
    db.execute('INSERT INTO "target schema"."a.b""c" VALUES(2)')
    mapped = '"target schema"."a.b""c"'
    try:
        record = event(database="d" * 100, schema="s.ch ema", table='t"' + "x" * 100)
        assert apply(db, record, mapped=mapped)["truncates"] == 1
        assert rows(db, mapped) == []
        assert rows(db) == [(1, "before")]
    finally:
        db.execute('DROP SCHEMA "target schema" CASCADE')


def test_schema_change_quoted_source_components(db):
    record = event(database="d.b", schema='s"c', table="t.x")
    del record["payload"]["op"]
    record["payload"]["tableChanges"] = [{"type": "TRUNCATE", "id": '"d.b"."s""c"."t.x"'}]
    record["payload"]["ddl"] = 'TRUNCATE TABLE "s""c"."t.x"'
    assert apply(db, record, mapped="fixture.items")["truncates"] == 1
    assert rows(db) == []


def test_schema_id_cannot_override_source_database(db):
    db.execute("CREATE SCHEMA otherdb; CREATE TABLE otherdb.items(id int)")
    db.execute("INSERT INTO otherdb.items VALUES(9)")
    try:
        record = schema_event("mysql", '"otherdb"."items"')
        rejected(db, record)
        assert rows(db, "otherdb.items") == [(9,)]
    finally:
        db.execute("DROP SCHEMA otherdb CASCADE")


@pytest.mark.parametrize("target", ["x" * 100, "other_database.public.items"])
def test_invalid_destination_is_rejected(db, target):
    with pytest.raises(psycopg2.Error):
        apply(db, event(), mapped=target)
    assert rows(db) == [(1, "before")]


@pytest.mark.parametrize("definition", ["CREATE UNLOGGED TABLE fixture.unsupported(id int)",
    "CREATE VIEW fixture.unsupported AS SELECT 1 AS id", "CREATE TABLE fixture.unsupported(id int)"])
def test_target_capabilities_are_explicit(db, definition):
    db.execute(definition)
    if definition.startswith("CREATE TABLE"):
        db.execute("ALTER TABLE fixture.unsupported ENABLE ROW LEVEL SECURITY")
    rejected(db, event(table="unsupported"))


def test_explicit_target_does_not_clear_inheritance_children(db):
    db.execute("CREATE TABLE fixture.child() INHERITS(fixture.items)")
    db.execute("INSERT INTO fixture.child VALUES(2,'local')")
    apply(db, event())
    assert rows(db, "ONLY fixture.items") == []
    assert rows(db, "fixture.child") == [(2, "local")]


def test_identity_and_relation_definition_survive(db):
    db.execute("CREATE TABLE fixture.identity_items(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY)")
    db.execute("INSERT INTO fixture.identity_items DEFAULT VALUES")
    db.execute("SELECT 'fixture.identity_items'::regclass::oid")
    oid = db.fetchone()[0]
    apply(db, event(table="identity_items"))
    db.execute("INSERT INTO fixture.identity_items DEFAULT VALUES RETURNING id")
    assert db.fetchone() == (2,)
    db.execute("SELECT 'fixture.identity_items'::regclass::oid")
    assert db.fetchone() == (oid,)


def test_fk_failure_rolls_back_all_earlier_events(db):
    db.execute("CREATE TABLE fixture.parent(id int PRIMARY KEY); INSERT INTO fixture.parent VALUES(1)")
    db.execute("CREATE TABLE fixture.child(id int REFERENCES fixture.parent)")
    rejected(db, [event(), event(table="parent")])
    assert rows(db, "fixture.parent") == [(1,)]


def test_trigger_effects_roll_back_with_later_failure(db):
    db.execute("CREATE TABLE fixture.audit(op text)")
    db.execute("""CREATE FUNCTION fixture.audit_truncate() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO fixture.audit VALUES(TG_OP); RETURN NULL; END $$""")
    db.execute("CREATE TRIGGER audit AFTER TRUNCATE ON fixture.items EXECUTE FUNCTION fixture.audit_truncate()")
    rejected(db, [event(), event(table="missing")])
    assert rows(db, "fixture.audit") == []
    apply(db, event())
    assert rows(db, "fixture.audit") == [("TRUNCATE",)]


def test_truncate_permission_is_checked(db):
    db.execute("CREATE ROLE truncate_limited; GRANT USAGE ON SCHEMA fixture TO truncate_limited")
    try:
        db.execute("SET ROLE truncate_limited")
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            apply(db, event())
        db.execute("RESET ROLE")
        assert rows(db) == [(1, "before")]
        db.execute("GRANT TRUNCATE ON fixture.items TO truncate_limited; SET ROLE truncate_limited")
        assert apply(db, event())["truncates"] == 1
    finally:
        db.execute("RESET ROLE; REVOKE ALL ON fixture.items FROM truncate_limited")
        db.execute("REVOKE ALL ON SCHEMA fixture FROM truncate_limited; DROP ROLE truncate_limited")


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
@pytest.mark.parametrize("suffix", ["", " DROP STORAGE", " DROP ALL STORAGE", " REUSE STORAGE",
    " /* comment */ DROP /*nested /*comment*/ */ ALL STORAGE; --tail"])
def test_native_oracle_storage_options(db, suffix):
    assert apply(db, native("TRUNCATE TABLE SRC.ITEMS" + suffix), native=True)["truncates"] == 1
    assert rows(db) == []


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
@pytest.mark.parametrize("ddl", ['TRUNCATE TABLE OTHER', 'TRUNCATE TABLE OTHER_OWNER.ITEMS',
    'TRUNCATE TABLE "SRC"."items"', 'TRUNCATE TABLE SRC.ITEMS; TRUNCATE TABLE SRC.OTHER',
    'TRUNCATE TABLE SRC.ITEMS, SRC.OTHER', 'TRUNCATE TABLE SRC.ITEMS CASCADE',
    'TRUNCATE TABLE SRC.ITEMS RESTART IDENTITY', 'TRUNCATE TABLE SRC.ITEMS DROP ALL STORAGE UNKNOWN'])
def test_native_mismatch_or_extra_sql_has_no_effect(db, ddl):
    rejected(db, native(ddl), native=True)


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
def test_native_quoted_case_is_preserved(db):
    db.execute('CREATE TABLE fixture."MiXeD"(id int); INSERT INTO fixture."MiXeD" VALUES(2)')
    assert apply(db, native('TRUNCATE TABLE "SRC"."MiXeD"', "MiXeD"),
                 native=True, mapped='fixture."MiXeD"')["truncates"] == 1
    assert rows(db, 'fixture."MiXeD"') == []
    assert rows(db) == [(1, "before")]


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
@pytest.mark.parametrize("schema,table", [
    ("truncate target", 'table "data'),
    ("truncate.target", "table.data"),
    ("MiXeD", "Case")])
def test_native_truncate_followed_by_mapped_insert(db, schema, table):
    from psycopg2 import sql

    target = sql.Identifier(schema, table).as_string(db)
    db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        db.execute(f"CREATE TABLE {target}(id integer PRIMARY KEY,value text)")
        db.execute(f"INSERT INTO {target} VALUES (1,'before')")
        insert = native()
        insert.update(scn=101, c_scn=101, c_idx=2)
        insert["payload"][0] = {"op": "c", "schema": {
            "owner": "SRC", "table": "ITEMS", "columns": [
                {"name": "ID", "type": "number", "precision": -1,
                 "scale": 0, "nullable": False},
                {"name": "VALUE", "type": "varchar2", "length": 32,
                 "nullable": True}]}, "after": {"ID": 2, "VALUE": "after"}}
        update = copy.deepcopy(insert)
        update["payload"][0].update(op="u", before={"ID": 2, "VALUE": "after"},
            after={"ID": 2, "VALUE": "updated"})
        delete = copy.deepcopy(insert)
        delete["payload"][0].update(op="d", before={"ID": 2, "VALUE": "updated"})
        del delete["payload"][0]["after"]
        last = copy.deepcopy(insert)
        last["payload"][0]["after"] = {"ID": 3, "VALUE": "final"}
        for position, record in enumerate([insert, update, delete, last], start=101):
            record.update(scn=position, c_scn=position, c_idx=1)
        assert apply(db, [native(), insert, update, delete, last], native=True, mapped=target) == {
            "truncates": 1, "ddls": 1, "bad": 0}
        assert rows(db, target) == [(3, "final")]
        assert rows(db) == [(1, "before")]
    finally:
        db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.skipif(not WITH_OLR, reason="requires SYNCHDB_TEST_WITH_OLR=1")
def test_native_keyword_as_table_name(db):
    db.execute("CREATE TABLE fixture.reuse(id int); INSERT INTO fixture.reuse VALUES(2)")
    assert apply(db, native("TRUNCATE TABLE SRC.REUSE DROP STORAGE", "REUSE"), native=True)["truncates"] == 1
    assert rows(db, "fixture.reuse") == []
    assert rows(db) == [(1, "before")]


def test_skip_policy_alias_and_validation(db, database):
    _, data = database
    config = data / "postgresql.conf"
    original = config.read_text()

    def settings(values):
        config.write_text(original + "".join(
            f"\nsynchdb.{key}='{value}'\n" for key, value in values.items()))
        db.execute("SELECT pg_conf_load_time(), pg_reload_conf()")
        before = db.fetchone()[0]
        deadline = time.monotonic() + 10
        while True:
            db.execute("SELECT pg_conf_load_time()")
            if db.fetchone()[0] > before:
                return
            assert time.monotonic() < deadline
            time.sleep(0.02)

    try:
        db.execute("SHOW synchdb.dbz_skipped_operations")
        assert db.fetchone() == ("none",)
        settings({"dbz_skipped_oeprations": " c , t "})
        assert apply(db, event())["truncates"] == 0
        if WITH_OLR:
            assert apply(db, native(), native=True)["truncates"] == 0
        assert rows(db) == [(1, "before")]
        settings({"dbz_skipped_oeprations": "t", "dbz_skipped_operations": "none"})
        assert apply(db, event())["truncates"] == 1
        for value in ("", "c, u, d"):
            settings({"dbz_skipped_operations": value})
            assert apply(db, event())["truncates"] == 1
        settings({"dbz_skipped_operations": "none,t"})
        db.execute("SELECT error FROM pg_file_settings WHERE name='synchdb.dbz_skipped_operations'")
        assert db.fetchone()[0] is not None
    finally:
        settings({})
