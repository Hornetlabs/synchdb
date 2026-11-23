"""Exercise the actual readiness shell functions without starting containers."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import pytest


SCRIPT = Path(__file__).resolve().parents[4] / "ci/setup-remotedbs.sh"
READY = "DATABASE IS READY TO USE"
STARTED_AT = "2026-11-04T00:28:26.4773883Z"

# These executables model Docker state changes and log retention. Running the
# real shell functions also checks errexit, pipefail and exit propagation.
MOCK_DOCKER = r'''
import json
import os
from pathlib import Path
import sys

root = Path(os.environ["READINESS_MOCK_DIR"])
scenario = json.loads((root / "scenario.json").read_text())
args = sys.argv[1:]
with (root / "calls.jsonl").open("a") as calls:
    calls.write(json.dumps(["docker", *args]) + "\n")
position_file = root / "position"
position = int(position_file.read_text()) if position_file.exists() else -1
if args[0] == "inspect" and args[2] != "{{json .State}}":
    position = min(position + 1, len(scenario["states"]) - 1)
    position_file.write_text(str(position))
state = scenario["states"][max(position, 0)]
if args[0] == "inspect":
    if state.get("inspect_error"):
        print("Error: No such container: ora19c", file=sys.stderr)
        sys.exit(1)
    if args[2] == "{{json .State}}":
        info = {"Status": state.get("status", "running"),
                "ExitCode": state.get("exit_code", 0),
                "OOMKilled": state.get("oom", False),
                "StartedAt": state.get("started_at", scenario["started_at"])}
        if "health" in state:
            info["Health"] = {"Status": state["health"]}
        print(json.dumps(info))
    else:
        print(state.get("status", "running"), state.get("exit_code", 0),
              str(state.get("oom", False)).lower(),
              state.get("started_at", scenario["started_at"]))
elif args[0] == "logs":
    if "--tail" in args:
        print(scenario["diagnostic_logs"])
    else:
        logs = state.get("logs", "")
        if "--since" not in args:
            logs = state.get("previous_logs", "") + logs
        elif args[args.index("--since") + 1] != state.get("started_at", scenario["started_at"]):
            sys.exit("unexpected log start timestamp")
        print(logs)
        sys.exit(state.get("logs_exit", 0))
else:
    sys.exit("unexpected docker command")
'''

MOCK_SLEEP = r'''
import json
import os
from pathlib import Path
import sys

with (Path(os.environ["READINESS_MOCK_DIR"]) / "calls.jsonl").open("a") as calls:
    calls.write(json.dumps(["sleep", *sys.argv[1:]]) + "\n")
'''


def _function(name):
    match = re.search(
        rf"(?ms)^function {name}\(\)\n\{{\n.*?^\}}", SCRIPT.read_text())
    assert match, f"readiness function {name} was not found"
    return match.group(0)


def _run(tmp_path, states, *, attempts=3, errexit=False, fail_after_ready=False):
    (tmp_path / "scenario.json").write_text(json.dumps({
        "states": states,
        "started_at": STARTED_AT,
        "diagnostic_logs": "ORA-01034: diagnostic source failure",
    }))
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for name, body in [("docker", MOCK_DOCKER), ("sleep", MOCK_SLEEP)]:
        executable = bindir / name
        executable.write_text(f"#!{sys.executable}\n{body}")
        executable.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = str(bindir) + os.pathsep + env["PATH"]
    env["READINESS_MOCK_DIR"] = str(tmp_path)
    shell = "\n".join([
        "set -uo pipefail", "set -e" if errexit else "set +e",
        _function("container_readiness_failed"),
        _function("wait_for_container_ready"), f"MAX_TRIES={attempts}",
        f"wait_for_container_ready ora19c '{READY}'",
        "false" if fail_after_ready else ":",
        "echo SOURCE_CONFIGURATION_CONTINUED",
    ])
    result = subprocess.run(
        ["bash", "-c", shell], env=env, text=True, capture_output=True,
        timeout=10)
    calls = [json.loads(line) for line in
             (tmp_path / "calls.jsonl").read_text().splitlines()]
    return result, calls


def _sleeps(calls):
    return [call for call in calls if call[0] == "sleep"]


def _assert_failure_diagnostics(result, calls):
    assert result.returncode == 1
    assert "SOURCE_CONFIGURATION_CONTINUED" not in result.stdout
    assert "Container state for ora19c:" in result.stderr
    assert "Last 100 log lines for ora19c:" in result.stderr
    assert "ORA-01034: diagnostic source failure" in result.stderr
    assert ["docker", "inspect", "--format", "{{json .State}}", "ora19c"] in calls
    assert ["docker", "logs", "--tail", "100", "ora19c"] in calls


@pytest.mark.parametrize("errexit", [False, True])
def test_log_marker_allows_configuration_to_continue(tmp_path, errexit):
    result, calls = _run(tmp_path, [{"logs": READY}], errexit=errexit)
    assert result.returncode == 0
    assert "ora19c is ready to use" in result.stdout
    assert "SOURCE_CONFIGURATION_CONTINUED" in result.stdout
    assert not result.stderr
    assert not _sleeps(calls)
    assert ["docker", "logs", "--since", STARTED_AT, "ora19c"] in calls


def test_success_restores_errexit_for_following_source_configuration(tmp_path):
    result, calls = _run(tmp_path, [{"logs": READY}], fail_after_ready=True)
    assert result.returncode == 1
    assert "ora19c is ready to use" in result.stdout
    assert "SOURCE_CONFIGURATION_CONTINUED" not in result.stdout
    assert not result.stderr
    assert not _sleeps(calls)


@pytest.mark.parametrize("state", [
    {"status": "exited", "exit_code": 137},
    {"status": "dead", "exit_code": 0},
    {"status": "removing"},
    {"oom": True},
])
@pytest.mark.parametrize("errexit", [False, True])
def test_terminal_or_oom_state_stops_before_configuration(tmp_path, state, errexit):
    # Even a retained marker must not make a stopped/OOM container ready.
    result, calls = _run(tmp_path, [{**state, "logs": READY}], errexit=errexit)
    _assert_failure_diagnostics(result, calls)
    assert "exit_code=" in result.stderr
    assert "OOMKilled=" in result.stderr
    assert '"Status"' in result.stderr
    assert not _sleeps(calls)


def test_inspect_failure_is_fatal_even_with_errexit_disabled(tmp_path):
    result, calls = _run(tmp_path, [{"inspect_error": True}])
    _assert_failure_diagnostics(result, calls)
    assert "could not be inspected" in result.stderr
    assert not _sleeps(calls)


@pytest.mark.parametrize("health", [None, "unhealthy"])
def test_timeout_preserves_attempt_limit_and_reports_diagnostics(tmp_path, health):
    pending = {} if health is None else {"health": health}
    result, calls = _run(tmp_path, [pending])
    _assert_failure_diagnostics(result, calls)
    assert "giving up after 3 attempts" in result.stderr
    assert _sleeps(calls) == [["sleep", "10"]] * 3
    assert '"Status": "running"' in result.stderr


def test_exit_during_startup_stops_at_next_poll(tmp_path):
    result, calls = _run(tmp_path, [
        {}, {"status": "exited", "exit_code": 137, "logs": READY},
    ])
    _assert_failure_diagnostics(result, calls)
    assert "state=exited, exit_code=137" in result.stderr
    assert _sleeps(calls) == [["sleep", "10"]]


def test_retained_marker_from_previous_start_is_not_ready(tmp_path):
    result, calls = _run(tmp_path, [
        {"previous_logs": READY, "logs": "database is starting"},
        {"logs": READY},
    ])
    assert result.returncode == 0
    assert _sleeps(calls) == [["sleep", "10"]]


@pytest.mark.parametrize("health", [None, "unhealthy", "starting"])
def test_no_health_or_unhealthy_keeps_waiting_for_readiness(tmp_path, health):
    pending = {} if health is None else {"health": health}
    result, calls = _run(tmp_path, [pending, {**pending, "logs": READY}])
    assert result.returncode == 0
    assert _sleeps(calls) == [["sleep", "10"]]


def test_failed_log_read_cannot_report_ready(tmp_path):
    result, calls = _run(tmp_path, [
        {"logs": READY, "logs_exit": 1},
        {"logs": READY},
    ])
    assert result.returncode == 0
    assert _sleeps(calls) == [["sleep", "10"]]
