"""Source-client contracts; no database, Docker or installed extension needed."""
import subprocess

import pytest

import common


@pytest.mark.parametrize("vendor", ["oracle", "olr", "oracle23ai"])
@pytest.mark.parametrize("terminator", ["", ";"])
def test_sqlplus_statement_boundaries(monkeypatch, vendor, terminator):
    calls = []
    monkeypatch.setattr(common, "resolve_host", lambda container: "127.0.0.1")

    def sqlplus(args, **kwargs):
        calls.append((args, kwargs))
        script = kwargs["input"]
        lines = [line.strip() for line in script.splitlines()]
        assert "INSERT INTO items VALUES (1,'literal; COMMIT');" in lines
        assert "COMMIT;" in lines
        assert "WHENEVER SQLERROR EXIT SQL.SQLCODE;" in lines
        assert "WHENEVER OSERROR EXIT FAILURE;" in lines
        assert kwargs["text"] is True
        return "1 | literal; COMMIT\n"

    monkeypatch.setattr(common.subprocess, "check_output", sqlplus)
    result = common.run_remote_statements(vendor, [
        "INSERT INTO items VALUES (1,'literal; COMMIT')" + terminator,
        "COMMIT" + terminator], srcdb="SOURCE_TEST")
    assert result == [("1", "literal; COMMIT")]
    assert len(calls) == 1, "all statements must use the same source session"
    assert "sqlplus" in calls[0][0]
    assert calls[0][0][-1].endswith("/SOURCE_TEST")


@pytest.mark.parametrize("vendor,flag", [
    ("postgres", "-c"), ("mysql", "-e"), ("sqlserver", "-Q")])
def test_other_clients_preserve_statement_boundaries(monkeypatch, vendor, flag):
    calls = []

    def client(args, **kwargs):
        calls.append(args)
        assert args[args.index(flag) + 1] == "INSERT INTO items VALUES (1);\nCOMMIT"
        return ""

    monkeypatch.setattr(common.subprocess, "check_output", client)
    common.run_remote_statements(vendor, ["INSERT INTO items VALUES (1)", "COMMIT"])
    assert len(calls) == 1


def test_sqlplus_failure_is_not_swallowed(monkeypatch):
    monkeypatch.setattr(common, "resolve_host", lambda container: "127.0.0.1")

    def sqlplus(args, **kwargs):
        raise subprocess.CalledProcessError(165, ["sqlplus"],
            output="ORA-00933: SQL command not properly ended")

    monkeypatch.setattr(common.subprocess, "check_output", sqlplus)
    with pytest.raises(subprocess.CalledProcessError):
        common.run_remote_statements("oracle", ["INVALID SQL", "COMMIT"])
