from __future__ import annotations

import copy
from datetime import UTC, datetime

import pytest

from tools.prepare_bounded_termination_rollout import EXPECTED_IMAGE, EXPECTED_UID, PREFIX, VALUES, build_plan


def fixture_deployment() -> dict:
    return {
        "metadata": {"uid": EXPECTED_UID, "generation": 353},
        "spec": {
            "replicas": 1,
            "strategy": {"type": "Recreate"},
            "template": {
                "metadata": {"annotations": {"unrelated": "preserved"}},
                "spec": {
                    "containers": [
                        {"name": "stream-engine", "image": EXPECTED_IMAGE, "env": [{"name": "KEEP", "value": "yes"}]},
                        *({"name": name, "image": "unchanged"} for name in ("a", "b", "c")),
                    ]
                },
            },
        },
    }


def test_single_atomic_rollout_and_exact_env_rollback_preserve_other_containers() -> None:
    original = fixture_deployment()
    snapshot = copy.deepcopy(original)
    candidate, rollback = build_plan(original, datetime.now(UTC))
    assert original == snapshot
    before = original["spec"]["template"]
    after = candidate[-1]["value"]
    restored = rollback[-1]["value"]
    assert candidate[-2] == {"op": "test", "path": "/spec/template", "value": before}
    assert rollback[-2] == {"op": "test", "path": "/spec/template", "value": after}
    assert candidate[1]["value"] == 353 and rollback[1]["value"] == 354
    assert after["spec"]["containers"][1:] == before["spec"]["containers"][1:]
    assert restored["spec"] == before["spec"]
    env = after["spec"]["containers"][0]["env"]
    assert env == [{"name": "KEEP", "value": "yes"}, *({"name": k, "value": v} for k, v in VALUES.items())]
    assert after["metadata"]["annotations"]["unrelated"] == "preserved"
    assert len([k for k in after["metadata"]["annotations"] if k.startswith(PREFIX)]) == 4
    assert sum(op["op"] != "test" for op in candidate) == 1


@pytest.mark.parametrize("field,value", [("uid", "other"), ("generation", 354)])
def test_rejects_deployment_drift(field: str, value: object) -> None:
    deployment = fixture_deployment()
    deployment["metadata"][field] = value
    with pytest.raises(ValueError, match="DEPLOYMENT_IDENTITY_DRIFT"):
        build_plan(deployment, datetime.now(UTC))


@pytest.mark.parametrize("key", [*VALUES, "FFMPEG_RW_TIMEOUT_ENABLED", "FFMPEG_RW_TIMEOUT_USEC"])
def test_rejects_preexisting_gate(key: str) -> None:
    deployment = fixture_deployment()
    deployment["spec"]["template"]["spec"]["containers"][0]["env"].append({"name": key, "value": "0"})
    with pytest.raises(ValueError, match="EXPECTED_UNSET_GATES_DRIFT"):
        build_plan(deployment, datetime.now(UTC))
