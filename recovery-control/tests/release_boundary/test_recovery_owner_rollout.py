from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Any

import pytest

from tools.prepare_recovery_owner_rollout import BASE_IMAGE, DEPLOYMENT_UID, build_plan, build_upgrade_plan


def deployment() -> dict[str, Any]:
    return {
        "metadata": {"uid": DEPLOYMENT_UID, "generation": 354},
        "spec": {
            "replicas": 1,
            "strategy": {"type": "Recreate"},
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "stream-engine",
                            "image": BASE_IMAGE,
                            "env": [
                                {"name": "FR_FFMPEG_FORCE_KILL_ENABLED", "value": "1"},
                                {"name": "FR_FFMPEG_TERM_GRACE_SEC", "value": "2"},
                                {"name": "FR_FFMPEG_KILL_WAIT_SEC", "value": "1"},
                            ],
                            "volumeMounts": [],
                        },
                        *[{"name": name, "image": "unchanged"} for name in ("auto-dj", "precipitation-fetcher", "fast-recovery-loop")],
                    ],
                    "volumes": [],
                }
            },
        },
    }


def test_patch_is_one_exact_template_replacement_and_rollback_restores_profile() -> None:
    original = deployment()
    before = copy.deepcopy(original)
    apply, rollback = build_plan(original, image="stream-v3:recovery-owner-test", release_id="recovery-owner-test", now=datetime.now(UTC))
    assert original == before
    assert [x["op"] for x in apply] == ["test"] * 5 + ["replace"]
    assert apply[-1]["path"] == "/spec/template"
    expected = before["spec"]["template"]
    candidate = apply[-1]["value"]
    assert candidate["spec"]["containers"][1:] == expected["spec"]["containers"][1:]
    assert rollback[1]["value"] == 355
    assert rollback[-2]["value"] == candidate
    assert rollback[-1]["value"]["spec"] == expected["spec"]


@pytest.mark.parametrize("change", ["uid", "generation", "image", "scale", "strategy", "termination", "mount"])
def test_concurrent_or_unapproved_drift_rejects_plan(change: str) -> None:
    value = deployment()
    if change in {"uid", "generation"}:
        value["metadata"][change] = "drift"
    elif change == "image":
        value["spec"]["template"]["spec"]["containers"][0]["image"] = "other"
    elif change == "scale":
        value["spec"]["replicas"] = 2
    elif change == "strategy":
        value["spec"]["strategy"] = {"type": "RollingUpdate"}
    elif change == "termination":
        value["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = "0"
    else:
        value["spec"]["template"]["spec"]["volumes"] = [{"name": "recovery-config"}]
    with pytest.raises(ValueError):
        build_plan(value, image="stream-v3:recovery-owner-test", release_id="recovery-owner-test", now=datetime.now(UTC))


@pytest.mark.parametrize("drift", [None, "generation", "image", "config-path", "volume", "mount-mode"])
def test_existing_owner_upgrade_is_conflict_checked_and_restores_exact_spec(drift: str | None) -> None:
    current = deployment()
    patch, _ = build_plan(current, image="stream-v3:recovery-owner-old", release_id="recovery-owner-old", now=datetime.now(UTC))
    current["spec"]["template"] = patch[-1]["value"]
    current["metadata"]["generation"] = 355
    original = copy.deepcopy(current)
    engine = current["spec"]["template"]["spec"]["containers"][0]
    if drift == "generation":
        current["metadata"]["generation"] += 1
    elif drift == "image":
        engine["image"] = "other"
    elif drift == "config-path":
        engine["env"][-1]["value"] = "/other/config.json"
    elif drift == "volume":
        current["spec"]["template"]["spec"]["volumes"][0]["hostPath"]["path"] = "/other"
    elif drift == "mount-mode":
        engine["volumeMounts"][0]["readOnly"] = False

    def plan() -> tuple[list[Any], list[Any]]:
        return build_upgrade_plan(
            current,
            image="stream-v3:recovery-owner-new",
            release_id="recovery-owner-new",
            expected_image="stream-v3:recovery-owner-old",
            expected_generation=355,
            now=datetime.now(UTC),
        )

    if drift is not None:
        with pytest.raises(ValueError):
            plan()
        return
    apply, rollback = plan()
    assert current == original
    assert [x["op"] for x in apply] == ["test"] * 5 + ["replace"]
    assert apply[-1]["value"]["spec"]["containers"][1:] == original["spec"]["template"]["spec"]["containers"][1:]
    assert apply[-1]["value"]["spec"]["containers"][0]["env"] == engine["env"]
    assert rollback[1]["value"] == 356
    assert rollback[-2]["value"] == apply[-1]["value"]
    assert rollback[-1]["value"]["spec"] == original["spec"]["template"]["spec"]
