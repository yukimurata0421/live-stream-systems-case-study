from __future__ import annotations

import re
import shlex
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
CHECKOUT_SHA = "3d3c42e5aac5ba805825da76410c181273ba90b1"
SETUP_PYTHON_SHA = "5fda3b95a4ea91299a34e894583c3862153e4b97"
WORKFLOWS = (
    (
        "github",
        ROOT / ".github/workflows/owner-verification-traceability.yml",
        "ubuntu-24.04",
        [f"actions/checkout@{CHECKOUT_SHA}", f"actions/setup-python@{SETUP_PYTHON_SHA}"],
        re.compile(r"actions/[a-z-]+@[0-9a-f]{40}"),
    ),
    (
        "forgejo",
        ROOT / ".forgejo/workflows/owner-verification-traceability.yml",
        "docker",
        [f"https://data.forgejo.org/actions/checkout@{CHECKOUT_SHA}"],
        re.compile(r"https://data\.forgejo\.org/actions/[a-z-]+@[0-9a-f]{40}"),
    ),
)


@pytest.mark.parametrize(("provider", "workflow", "runner", "expected_uses", "action_pattern"), WORKFLOWS)
def test_owner_traceability_workflow_is_a_thin_read_only_adapter(
    provider: str,
    workflow: Path,
    runner: str,
    expected_uses: list[str],
    action_pattern: re.Pattern[str],
) -> None:
    value: dict[str, Any] = yaml.load(workflow.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert value["permissions"] == {"contents": "read"}
    assert set(value["on"]) == {"pull_request", "push"}

    job = value["jobs"]["owner-verification-traceability"]
    assert job["name"] == "owner-verification-traceability"
    assert job["runs-on"] == runner
    assert job["timeout-minutes"] == "15"
    steps = job["steps"]
    uses = [step["uses"] for step in steps if "uses" in step]
    assert uses == expected_uses
    assert all(action_pattern.fullmatch(action) for action in uses), provider
    checkout = steps[0]
    assert checkout["with"] == {"fetch-depth": "0", "persist-credentials": "false"}

    if provider == "forgejo":
        isolation = steps[1]
        assert isolation["name"] == "Verify runner isolation boundary"
        for forbidden_path in (
            "/var/run/docker.sock",
            "/etc/forgejo/app.ini",
            "/etc/forgejo-runner",
            "/var/lib/forgejo/data/forgejo.db",
            "/srv/forgejo-data",
            "/root/.ssh",
        ):
            assert f"test ! -e {forbidden_path}" in isolation["run"]
        assert "CapEff:" in isolation["run"]
        assert "NoNewPrivs:" in isolation["run"]
        assert "/sys/fs/cgroup/memory.max" in isolation["run"]
        assert "/sys/fs/cgroup/pids.max" in isolation["run"]

        bootstrap = steps[2]
        assert bootstrap["name"] == "Bootstrap Python"
        assert "APT::Sandbox::User=root" in bootstrap["run"]
        assert "python3 -m venv /tmp/cra-venv" in bootstrap["run"]
        assert 'echo /tmp/cra-venv/bin >> "$GITHUB_PATH"' in bootstrap["run"]

    command_steps = [(index, step, shlex.split(step["run"])) for index, step in enumerate(steps) if "run" in step]
    owner = [(index, tokens) for index, _, tokens in command_steps if "tools/run_owner_observation_chaos.py" in tokens]
    gates = [(index, step, tokens) for index, step, tokens in command_steps if "verification-gate" in tokens]
    adapter_tests = [(index, tokens) for index, _, tokens in command_steps if "tests/harness/unit/test_traceability_ci.py" in tokens]
    assert len(owner) == len(gates) == len(adapter_tests) == 1
    assert "tests/harness/unit/test_fixed_sqlite_entrypoint.py" in adapter_tests[0][1]
    owner_index, owner_tokens = owner[0]
    gate_index, gate_step, gate_tokens = gates[0]
    assert adapter_tests[0][0] < owner_index < gate_index
    owner_output = owner_tokens[owner_tokens.index("--output") + 1]
    assert gate_tokens[:3] == ["PYTHONPATH=src:.", "python", "tools/cra_task.py"]
    assert gate_tokens[gate_tokens.index("--result") + 1] == owner_output
    assert gate_step["if"] == "always()"
    assert "continue-on-error" not in gate_step

    adapter_text = workflow.read_text(encoding="utf-8")
    assert "check-traceability" not in adapter_text
    assert not {"STALE", "FAILED", "INCONCLUSIVE"} & set(re.findall(r"[A-Z_]+", adapter_text))
