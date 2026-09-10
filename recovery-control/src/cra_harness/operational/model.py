from __future__ import annotations

import hashlib
import json
import random
from dataclasses import asdict, dataclass
from typing import Any

AXES: dict[str, tuple[str, ...]] = {
    "authority": (
        "CENTRAL_ACTIVE",
        "CENTRAL_SUSPECT",
        "LOCAL_FALLBACK",
        "RECONCILING",
        "SAFE_BLOCKED",
        "MAINTENANCE",
    ),
    "target": ("VALID", "STALE", "UNSTABLE", "MISSING", "UNOBSERVABLE", "IDENTITY_CONFLICT"),
    "monitoring": ("FRESH", "STALE", "UNAVAILABLE", "RECOVERING"),
    "credential": (
        "VALID",
        "NEAR_EXPIRY",
        "EXPIRED",
        "ROTATING",
        "OLD_VALID",
        "OLD_KEY_IN_FLIGHT",
        "OLD_REVOKED",
        "NEW_VALID",
        "REVOKED",
    ),
    "network": (
        "HEALTHY",
        "HIGH_LATENCY",
        "JITTER",
        "PARTIAL_LOSS",
        "ASYMMETRIC_REQUEST_LOSS",
        "ASYMMETRIC_RESPONSE_LOSS",
        "TLS_DELAY",
        "COMPLETE_PARTITION",
    ),
    "maintenance": ("NONE", "ENTERING", "ACTIVE", "EXITING"),
    "legacy_recovery": ("IDLE", "DETECTED", "WOULD_MUTATE", "EXTERNAL_MUTATION_OBSERVED"),
}

DEFAULT_FLAGS: dict[str, bool] = {
    "late_central_command": False,
    "command_in_flight": False,
    "physical_outcome_unknown": False,
    "target_mutation": False,
    "authorization_pending": False,
    "external_mutation": False,
    "confirmed_tcp_stall": False,
    "local_evidence_valid": False,
    "cra_healthy": True,
    "stream_healthy": False,
    "reconciliation_complete": False,
    "rotation_success": False,
    "legacy_quiesced": False,
    "durable_accept_recorded": True,
}


@dataclass(frozen=True)
class OperationalScenario:
    scenario_id: str
    profile: str
    seed: int | None
    case_index: int | None
    authority: str
    target: str
    monitoring: str
    credential: str
    network: str
    maintenance: str
    legacy_recovery: str
    flags: dict[str, bool]
    elapsed_ms: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def cell(self) -> dict[str, str]:
        return {axis: str(getattr(self, axis)) for axis in AXES}

    @property
    def cell_id(self) -> str:
        canonical = json.dumps(self.cell, sort_keys=True, separators=(",", ":"))
        return f"cell-{hashlib.sha256(canonical.encode()).hexdigest()[:16]}"


HIGH_RISK_CELLS: tuple[dict[str, Any], ...] = (
    {
        "risk_id": "HR-01",
        "title": "LOCAL_FALLBACK + late central command",
        "match": {"authority": "LOCAL_FALLBACK", "flag:late_central_command": True},
    },
    {
        "risk_id": "HR-02",
        "title": "MAINTENANCE + legacy recovery WOULD_MUTATE",
        "match": {"maintenance": "ACTIVE", "legacy_recovery": "WOULD_MUTATE"},
    },
    {
        "risk_id": "HR-03",
        "title": "TARGET_UNOBSERVABLE + CENTRAL_ACTIVE",
        "match": {"target": "UNOBSERVABLE", "authority": "CENTRAL_ACTIVE"},
    },
    {
        "risk_id": "HR-04",
        "title": "TARGET_UNOBSERVABLE + LOCAL_FALLBACK",
        "match": {"target": "UNOBSERVABLE", "authority": "LOCAL_FALLBACK"},
    },
    {
        "risk_id": "HR-05",
        "title": "CREDENTIAL_EXPIRED + heartbeat lease active",
        "match": {"credential": "EXPIRED", "authority": "CENTRAL_ACTIVE"},
    },
    {
        "risk_id": "HR-06",
        "title": "CREDENTIAL_ROTATING + command in-flight",
        "match": {"credential": "ROTATING", "flag:command_in_flight": True},
    },
    {
        "risk_id": "HR-07",
        "title": "ASYMMETRIC_RESPONSE_LOSS + physical outcome unknown",
        "match": {"network": "ASYMMETRIC_RESPONSE_LOSS", "flag:physical_outcome_unknown": True},
    },
    {
        "risk_id": "HR-08",
        "title": "CENTRAL_SUSPECT + target mutation + authorization pending",
        "match": {
            "authority": "CENTRAL_SUSPECT",
            "flag:target_mutation": True,
            "flag:authorization_pending": True,
        },
    },
    {
        "risk_id": "HR-09",
        "title": "RECONCILING + external mutation",
        "match": {"authority": "RECONCILING", "flag:external_mutation": True},
    },
    {
        "risk_id": "HR-10",
        "title": "Monitoring STALE + target VALID + CRA healthy",
        "match": {"monitoring": "STALE", "target": "VALID", "flag:cra_healthy": True},
    },
    {
        "risk_id": "HR-11",
        "title": "Monitoring FRESH + target UNOBSERVABLE",
        "match": {"monitoring": "FRESH", "target": "UNOBSERVABLE"},
    },
)


def matches_risk(scenario: OperationalScenario, risk: dict[str, Any]) -> bool:
    for key, expected in dict(risk["match"]).items():
        observed: Any = scenario.flags.get(key.removeprefix("flag:")) if key.startswith("flag:") else getattr(scenario, key)
        if observed != expected:
            return False
    return True


def _base_values(generator: random.Random) -> dict[str, Any]:
    weights: dict[str, tuple[int, ...]] = {
        "authority": (32, 14, 14, 14, 10, 16),
        "target": (42, 10, 10, 8, 22, 8),
        "monitoring": (52, 18, 18, 12),
        "credential": (30, 10, 10, 14, 8, 8, 8, 6, 6),
        "network": (28, 10, 10, 12, 12, 14, 8, 6),
        "maintenance": (46, 14, 26, 14),
        "legacy_recovery": (42, 16, 26, 16),
    }
    values: dict[str, Any] = {axis: generator.choices(choices, weights=weights[axis], k=1)[0] for axis, choices in AXES.items()}
    flags = dict(DEFAULT_FLAGS)
    flags.update(
        {
            "late_central_command": generator.random() < 0.22,
            "command_in_flight": generator.random() < 0.30,
            "physical_outcome_unknown": generator.random() < 0.18,
            "target_mutation": generator.random() < 0.22,
            "authorization_pending": generator.random() < 0.26,
            "external_mutation": generator.random() < 0.22,
            "confirmed_tcp_stall": generator.random() < 0.38,
            "local_evidence_valid": generator.random() < 0.48,
            "stream_healthy": generator.random() < 0.35,
            "reconciliation_complete": generator.random() < 0.28,
            "rotation_success": generator.random() < 0.70,
            "legacy_quiesced": generator.random() < 0.25,
        }
    )
    values["flags"] = flags
    values["elapsed_ms"] = generator.randint(0, 45_000)
    return values


def _force_risk(values: dict[str, Any], risk: dict[str, Any]) -> None:
    for key, expected in dict(risk["match"]).items():
        if key.startswith("flag:"):
            values["flags"][key.removeprefix("flag:")] = expected
        else:
            values[key] = expected
    risk_id = str(risk["risk_id"])
    if risk_id in {"HR-01", "HR-04"}:
        values["flags"]["confirmed_tcp_stall"] = True
        values["flags"]["local_evidence_valid"] = True
    if risk_id == "HR-05":
        values["monitoring"] = "FRESH"
        values["target"] = "VALID"
    if risk_id == "HR-06":
        values["target"] = "VALID"
        values["monitoring"] = "FRESH"
        values["network"] = "HEALTHY"
        values["maintenance"] = "NONE"
        values["legacy_recovery"] = "IDLE"
        values["flags"]["rotation_success"] = True
    if risk_id == "HR-07":
        values["authority"] = "CENTRAL_ACTIVE"
        values["target"] = "VALID"
        values["monitoring"] = "FRESH"
        values["credential"] = "VALID"
        values["maintenance"] = "NONE"
        values["legacy_recovery"] = "IDLE"
        values["flags"]["command_in_flight"] = True
        values["flags"]["confirmed_tcp_stall"] = True
    if risk_id == "HR-09":
        values["legacy_recovery"] = "EXTERNAL_MUTATION_OBSERVED"
    if risk_id == "HR-10":
        values["credential"] = "VALID"
        values["network"] = "HEALTHY"


def build_operational_scenarios(
    seeds: tuple[int, ...],
    *,
    cases_per_seed: int = 100,
    mandatory_hits_per_seed: int = 5,
) -> list[OperationalScenario]:
    mandatory_count = len(HIGH_RISK_CELLS) * mandatory_hits_per_seed
    if cases_per_seed < mandatory_count:
        raise ValueError("cases_per_seed cannot satisfy mandatory high-risk hit schedule")
    scenarios: list[OperationalScenario] = []
    for seed in seeds:
        generator = random.Random(seed)
        schedule: list[dict[str, Any] | None] = [risk for risk in HIGH_RISK_CELLS for _ in range(mandatory_hits_per_seed)]
        schedule.extend([None] * (cases_per_seed - len(schedule)))
        generator.shuffle(schedule)
        for case_index, risk in enumerate(schedule, start=1):
            values = _base_values(generator)
            if risk is not None:
                _force_risk(values, risk)
            scenarios.append(
                OperationalScenario(
                    scenario_id=f"OSV3-{seed}-{case_index:03d}",
                    profile="operational_randomized",
                    seed=seed,
                    case_index=case_index,
                    **values,
                )
            )
    return scenarios


def mandatory_scenarios() -> list[OperationalScenario]:
    scenarios: list[OperationalScenario] = []
    for index, risk in enumerate(HIGH_RISK_CELLS, start=1):
        generator = random.Random(31_000 + index)
        values = _base_values(generator)
        _force_risk(values, risk)
        scenarios.append(
            OperationalScenario(
                scenario_id=f"OSV3-HR-{index:02d}",
                profile="operational_deterministic",
                seed=None,
                case_index=None,
                **values,
            )
        )
    return scenarios


def total_possible_cells() -> int:
    total = 1
    for values in AXES.values():
        total *= len(values)
    return total
