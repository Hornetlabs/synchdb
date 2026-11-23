"""Credential compatibility without a source database or a keyring file.

Use the 1.0 SQL expressions as an independent reader/writer, rather than
comparing randomized PGP ciphertext. All secrets below are synthetic test data.
"""
import os
import uuid

import psycopg2
from psycopg2.extras import Json
import pytest


# The public, compiled-in passphrase in SynchDB 1.0; do not generate a new key.
LEGACY_SECRET = "930e62fb8c40086c23f543357a023c0c"
FIELDS = (
    "pwd", "ssl_keystore_pass", "ssl_truststore_pass",
    "jmx_ssl_keystore_pass", "jmx_ssl_truststore_pass", "fdw_ssl_cipher",
)
VALUES = {field: f"secret{i}-'\\中文\n\t" for i, field in enumerate(FIELDS)}


@pytest.fixture(scope="session", autouse=True)
def setup_remote_instance():
    """Override source provisioning: these tests only need the target cluster."""
    yield


@pytest.fixture
def credentials_cursor(pg_cursor, target_instance):
    # pg_cursor installs the extension. A separate session isolates settings
    # such as bytea_output from the replication tests sharing pg_cursor.
    connection = psycopg2.connect(host=target_instance.host, dbname="postgres",
                                  port=target_instance.port)
    connection.autocommit = True
    with connection.cursor() as cursor:
        yield cursor
    connection.close()


def _add(cursor, name, password):
    cursor.execute(
        "SELECT synchdb_add_conninfo(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (name, "127.0.0.1", 3306, "offline", password, "offline",
         "null", "null", "null", "mysql"),
    )
    return cursor.fetchone()[0]


@pytest.fixture
def credential_connector(credentials_cursor, request):
    name = "credential_" + uuid.uuid4().hex[:16]
    password = getattr(request, "param", VALUES["pwd"])
    assert _add(credentials_cursor, name, password) == 0
    try:
        yield credentials_cursor, name
    finally:
        # Identity lookup/deletion does not require decrypting a damaged secret.
        credentials_cursor.execute("SELECT synchdb_del_conninfo(%s)", (name,))


def _write_options(cursor, name, values):
    cursor.execute("SELECT synchdb_add_extra_conninfo(%s,%s,%s,%s,%s,%s)",
                   (name, "disabled", "null", values["ssl_keystore_pass"],
                    "null", values["ssl_truststore_pass"]))
    assert cursor.fetchone()[0] == 0
    cursor.execute(
        "SELECT synchdb_add_jmx_conninfo(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (name, "127.0.0.1", 19991, "127.0.0.1", 19992, False,
         "null", "null", True, "null", values["jmx_ssl_keystore_pass"],
         "null", values["jmx_ssl_truststore_pass"]),
    )
    assert cursor.fetchone()[0] == 0
    cursor.execute("SELECT synchdb_add_fdw_conninfo(%s,%s,%s,%s,%s)",
                   (name, "null", "null", "null", values["fdw_ssl_cipher"]))
    assert cursor.fetchone()[0] == 0


def _read(cursor, name, field):
    cursor.execute("SELECT synchdb_connector_credential(%s,%s)", (name, field))
    return cursor.fetchone()[0]


def _replace(cursor, name, field, value):
    cursor.execute(
        "UPDATE synchdb_conninfo SET data=jsonb_set(data,ARRAY[%s],%s::jsonb) "
        "WHERE name=%s", (field, Json(value), name),
    )
    assert cursor.rowcount == 1


def _snapshot(cursor, name):
    cursor.execute("SELECT connector_id::text,config_revision,data "
                   "FROM synchdb_conninfo WHERE name=%s", (name,))
    return cursor.fetchone()


def test_credentials_need_no_keyring(credential_connector, target_instance):
    cursor, name = credential_connector
    assert not os.path.exists(os.path.join(target_instance.temp_dir,
                                           "credentials.keyring"))
    cursor.execute("SELECT count(*) FROM pg_settings "
                   "WHERE name='synchdb.credential_keyfile'")
    assert cursor.fetchone()[0] == 0
    assert _read(cursor, name, "pwd") == VALUES["pwd"]


@pytest.mark.parametrize("output", ["hex", "escape"])
def test_legacy_sql_secrets_readable(credential_connector, output):
    cursor, name = credential_connector
    cursor.execute("SET bytea_output=%s", (output,))
    # This is the old jsonb_build_object(..., pgp_sym_encrypt(...)) storage
    # convention, extended over all six fields in one atomic update.
    cursor.execute(
        "UPDATE synchdb_conninfo SET data=data || "
        "(SELECT jsonb_object_agg(field,pgp_sym_encrypt(plaintext,%s)) "
        " FROM unnest(%s::text[],%s::text[]) AS input(field,plaintext)) "
        "WHERE name=%s",
        (LEGACY_SECRET, list(FIELDS), [VALUES[field] for field in FIELDS], name),
    )
    assert cursor.rowcount == 1
    assert {field: _read(cursor, name, field) for field in FIELDS} == VALUES


@pytest.mark.parametrize("output", ["hex", "escape"])
def test_new_api_secrets_readable_by_legacy_sql(credentials_cursor, output):
    cursor = credentials_cursor
    name = "credential_" + uuid.uuid4().hex[:16]
    cursor.execute("SET bytea_output=%s", (output,))
    try:
        assert _add(cursor, name, VALUES["pwd"]) == 0
        _write_options(cursor, name, VALUES)
        for field in FIELDS:
            cursor.execute(
                "SELECT jsonb_typeof(data->%s),"
                "pgp_sym_decrypt((data->>%s)::bytea,%s) "
                "FROM synchdb_conninfo WHERE name=%s",
                (field, field, LEGACY_SECRET, name),
            )
            assert cursor.fetchone() == ("string", VALUES[field])
            assert _read(cursor, name, field) == VALUES[field]
    finally:
        cursor.execute("SELECT synchdb_del_conninfo(%s)", (name,))


@pytest.mark.parametrize("marker", ["", "null"])
def test_optional_secret_defaults(credential_connector, marker):
    cursor, name = credential_connector
    # Missing optional fields and explicitly cleared fields both read as NULL.
    assert all(_read(cursor, name, field) is None for field in FIELDS[1:])
    _write_options(cursor, name, dict.fromkeys(FIELDS[1:], marker))
    cursor.execute("SELECT data FROM synchdb_conninfo WHERE name=%s", (name,))
    data = cursor.fetchone()[0]
    assert all(field in data and data[field] is None for field in FIELDS[1:])
    assert all(_read(cursor, name, field) is None for field in FIELDS[1:])
    assert _read(cursor, name, "pwd") == VALUES["pwd"]


@pytest.mark.parametrize("credential_connector", ["null"], indirect=True)
def test_required_literal_null_is_a_password(credential_connector):
    cursor, name = credential_connector
    assert _read(cursor, name, "pwd") == "null"
    cursor.execute("SELECT pgp_sym_decrypt((data->>'pwd')::bytea,%s) "
                   "FROM synchdb_conninfo WHERE name=%s", (LEGACY_SECRET, name))
    assert cursor.fetchone()[0] == "null"


def test_required_password_null_and_empty_do_not_create(credentials_cursor):
    cursor = credentials_cursor
    name = "credential_" + uuid.uuid4().hex[:16]
    # The SQL function is STRICT: an SQL NULL skips the function entirely.
    assert _add(cursor, name, None) is None
    with pytest.raises(psycopg2.Error) as error:
        _add(cursor, name, "")
    assert error.value.pgcode == "22023"
    cursor.execute("SELECT count(*) FROM synchdb_conninfo WHERE name=%s", (name,))
    assert cursor.fetchone()[0] == 0


def test_builtin_key_v1_envelope_remains_readable(credential_connector):
    cursor, name = credential_connector
    cursor.execute("SELECT encode(pgp_sym_encrypt(%s,%s),'hex')",
                   (VALUES["pwd"], LEGACY_SECRET))
    envelope = {"version": 1, "key_id": "legacy",
                "ciphertext": cursor.fetchone()[0]}
    _replace(cursor, name, "pwd", envelope)
    assert _read(cursor, name, "pwd") == VALUES["pwd"]


@pytest.mark.parametrize("value", [False, 17, [], {}, ""])
def test_invalid_secret_types_rejected(credential_connector, value):
    cursor, name = credential_connector
    _replace(cursor, name, "pwd", value)
    with pytest.raises(psycopg2.Error) as error:
        _read(cursor, name, "pwd")
    assert error.value.pgcode == "22023"


@pytest.mark.parametrize("value", [r"\x0", r"\xzz", r"\bad"])
def test_invalid_bytea_rejected(credential_connector, value):
    cursor, name = credential_connector
    # Hex decoding and malformed escape input have different SQLSTATEs.
    # The new reader must return the same failure as the original bytea cast.
    with pytest.raises(psycopg2.Error) as legacy_error:
        cursor.execute("SELECT %s::bytea", (value,))
    _replace(cursor, name, "pwd", value)
    with pytest.raises(psycopg2.Error) as error:
        _read(cursor, name, "pwd")
    assert error.value.pgcode == legacy_error.value.pgcode


def test_wrong_key_ciphertext_rejected(credential_connector):
    cursor, name = credential_connector
    cursor.execute("SELECT to_jsonb(pgp_sym_encrypt(%s,%s))",
                   (VALUES["pwd"], "different synthetic passphrase"))
    _replace(cursor, name, "pwd", cursor.fetchone()[0])
    with pytest.raises(psycopg2.Error) as error:
        _read(cursor, name, "pwd")
    assert error.value.pgcode == "39000"


@pytest.mark.parametrize("change,code", [
    ({"version": 2}, "22023"),
    ({"key_id": "external"}, "55000"),
    ({"extra": True}, "22023"),
])
def test_unsupported_v1_envelope_rejected(credential_connector, change, code):
    cursor, name = credential_connector
    cursor.execute("SELECT encode(pgp_sym_encrypt(%s,%s),'hex')",
                   (VALUES["pwd"], LEGACY_SECRET))
    envelope = {"version": 1, "key_id": "legacy",
                "ciphertext": cursor.fetchone()[0]}
    envelope.update(change)
    _replace(cursor, name, "pwd", envelope)
    with pytest.raises(psycopg2.Error) as error:
        _read(cursor, name, "pwd")
    assert error.value.pgcode == code


def test_reencrypt_preserves_six_secrets_and_configuration(credential_connector):
    cursor, name = credential_connector
    _write_options(cursor, name, VALUES)
    before = _snapshot(cursor, name)
    cursor.execute("BEGIN")
    try:
        cursor.execute("SELECT synchdb_reencrypt_credentials()")
        assert cursor.fetchone()[0] >= 1
        after = _snapshot(cursor, name)
        assert after[0] == before[0]
        assert after[1] == before[1] + 1
        assert {k: v for k, v in after[2].items() if k not in FIELDS} == {
            k: v for k, v in before[2].items() if k not in FIELDS
        }
        for field in FIELDS:
            cursor.execute("SELECT pgp_sym_decrypt((data->>%s)::bytea,%s) "
                           "FROM synchdb_conninfo WHERE name=%s",
                           (field, LEGACY_SECRET, name))
            assert cursor.fetchone()[0] == VALUES[field]
            assert _read(cursor, name, field) == VALUES[field]
    finally:
        # Re-encryption visits every connector. Roll back its writes even when
        # this module is run together with the replication suite.
        cursor.execute("ROLLBACK")
    assert _snapshot(cursor, name) == before


def test_reencrypt_failure_rolls_back_all_rows(credential_connector):
    cursor, name = credential_connector
    _write_options(cursor, name, VALUES)
    broken = "credential_" + uuid.uuid4().hex[:16]
    cursor.execute("BEGIN")
    try:
        assert _add(cursor, broken, "synthetic broken credential") == 0
        cursor.execute("SELECT to_jsonb(pgp_sym_encrypt(%s,%s))",
                       ("synthetic secret", "a different synthetic key"))
        _replace(cursor, broken, "pwd", cursor.fetchone()[0])
        before = {n: _snapshot(cursor, n) for n in (name, broken)}
        cursor.execute("SAVEPOINT before_reencrypt")
        with pytest.raises(psycopg2.Error) as error:
            cursor.execute("SELECT synchdb_reencrypt_credentials()")
        assert error.value.pgcode == "39000"
        cursor.execute("ROLLBACK TO SAVEPOINT before_reencrypt")
        assert {n: _snapshot(cursor, n) for n in (name, broken)} == before
        assert {field: _read(cursor, name, field) for field in FIELDS} == VALUES
    finally:
        # This also removes the deliberately broken connector.
        cursor.execute("ROLLBACK")


def test_configuration_roundtrip_preserves_credentials(credential_connector):
    cursor, name = credential_connector
    _write_options(cursor, name, VALUES)
    cursor.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
    try:
        cursor.execute("SELECT synchdb_export_configuration()")
        before = cursor.fetchone()[0]
        # Import requires every configuration/runtime catalog to be empty.
        # The single transaction restores their original contents at the end.
        cursor.execute(
            "TRUNCATE synchdb_conninfo,synchdb_objmap,synchdb_attribute,"
            "synchdb_control_state,synchdb_control_commands,synchdb_artifacts,"
            "synchdb_olr_positions,synchdb_bootstrap_targets,synchdb_bootstrap_runs"
        )
        cursor.execute("SELECT synchdb_import_configuration(%s::jsonb)",
                       (Json(before),))
        assert cursor.fetchone()[0] == len(before["connectors"])
        cursor.execute("SELECT synchdb_export_configuration()")
        # Includes connector UUIDs, revisions, all non-secret fields and the
        # six stored ciphertext strings, as well as object mappings.
        assert cursor.fetchone()[0] == before
        assert {field: _read(cursor, name, field) for field in FIELDS} == VALUES
    finally:
        cursor.execute("ROLLBACK")
    cursor.execute("SELECT synchdb_export_configuration()")
    assert cursor.fetchone()[0] == before
