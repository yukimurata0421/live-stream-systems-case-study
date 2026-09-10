from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from cra_harness.maintenance.coordinator import MaintenanceCoordinator, NoActionPhysicalAdapter
from cra_harness.maintenance.model import (
    REQUIRED_MUTATORS,
    MaintenanceExecutorId,
    MaintenanceState,
    MutatorId,
)
from cra_harness.maintenance.store import MaintenanceStore
from cra_harness.oracles import maintenance as maintenance_oracle
from cra_harness.oracles import maintenance_authorization as authorization_oracle
from cra_harness.runner.maintenance import (
    AUTH_VALID_AT,
    RESOURCE_IDENTITY,
    ack,
    authorization_negative_controls,
    authorization_scenarios,
    db_deadline_result,
    deterministic_scenarios,
    intent,
    mutation_authorization,
    negative_controls,
    release_ack,
)
from dell_recovery_agent.deadline import AuthorityOperationDeadline

ROOT = Path(__file__).resolve().parents[3]


def test_all_required_deterministic_interleavings_pass(tmp_path: Path) -> None:
    outcomes = deterministic_scenarios(tmp_path)
    assert [item["scenario_id"] for item in outcomes] == [f"M-{index:02d}" for index in range(1, 11)]
    assert all(item["classification"] == "PASS" for item in outcomes)
    assert all(item["oracle_violations"] == [] for item in outcomes)
    assert all(item["observation"]["physical_effect_count"] == 0 for item in outcomes)


@pytest.mark.parametrize("index", range(10))
def test_each_maintenance_negative_control_is_detected(index: int) -> None:
    outcome = negative_controls()[index]
    assert outcome["scenario_id"] == f"NC-M{index + 1:02d}"
    assert outcome["classification"] == "EXPECTED_INJECTED_FAILURE"
    assert outcome["oracle_violations"]


def test_maintenance_oracle_does_not_import_sut_model() -> None:
    source = inspect.getsource(maintenance_oracle) + inspect.getsource(authorization_oracle)
    assert "cra_harness.maintenance" not in source
    assert "MaintenanceCoordinator" not in source


def test_actual_mutation_inventory_has_no_unknown_path_and_represents_all_required_mutators() -> None:
    inventory = json.loads((ROOT / "harness/fixtures/maintenance_mutation_paths.json").read_text(encoding="utf-8"))
    assert inventory["unknown_mutation_paths"] == []
    represented = {item.get("model_actor") for item in inventory["paths"]}
    assert {item.value for item in REQUIRED_MUTATORS} <= represented
    assert all(
        item["representation"]
        in {
            "PLANNED_EXECUTOR",
            "MODEL_VERIFIED_PRODUCTION_NO_ACTION",
            "PRODUCTION_INTEGRATION_PENDING",
            "UNSUPPORTED_CONDITION",
        }
        for item in inventory["paths"]
    )


def test_db_deadline_gate_is_strictly_inside_suspect_threshold() -> None:
    result = db_deadline_result()
    assert result["result"] == "PASS"
    assert result["critical_db_deadline_seconds"] < result["authority_suspect_threshold_seconds"]
    assert result["stale_heartbeat_commit"] == 0
    assert result["stale_command_commit"] == 0
    assert result["stale_reconciliation_commit"] == 0
    assert result["stale_maintenance_authorization_execution_commit"] == 0
    assert result["stale_authority_restoration"] == 0
    assert result["physical_effect_count"] == 0


def test_deterministic_replay_is_semantically_identical(tmp_path: Path) -> None:
    first = [*deterministic_scenarios(tmp_path / "first"), *authorization_scenarios(tmp_path / "first")]
    second = [*deterministic_scenarios(tmp_path / "second"), *authorization_scenarios(tmp_path / "second")]
    assert first == second


def test_maintenance_wire_contracts_match_model_and_exact_target_semantics() -> None:
    intent_schema = json.loads((ROOT / "contracts/maintenance/v1/maintenance_intent.schema.json").read_text(encoding="utf-8"))
    ack_schema = json.loads((ROOT / "contracts/maintenance/v1/quiesce_ack.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(intent_schema)
    Draft202012Validator.check_schema(ack_schema)
    assert intent_schema["$defs"]["target_identity"] == ack_schema["$defs"]["target_identity"]
    assert set(ack_schema["properties"]["mutator_id"]["enum"]) == {item.value for item in REQUIRED_MUTATORS}
    value = intent("contract")
    Draft202012Validator(intent_schema).validate(value.to_contract_dict())
    Draft202012Validator(ack_schema).validate(ack(value, MutatorId.DELL_FAST_RECOVERY).to_contract_dict())


def test_mutation_authorization_and_release_wire_contracts_match_model() -> None:
    authorization_schema = json.loads((ROOT / "contracts/maintenance/v2/mutation_authorization.schema.json").read_text(encoding="utf-8"))
    release_schema = json.loads((ROOT / "contracts/maintenance/v2/fence_release_ack.schema.json").read_text(encoding="utf-8"))
    intent_schema = json.loads((ROOT / "contracts/maintenance/v1/maintenance_intent.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(authorization_schema)
    Draft202012Validator.check_schema(release_schema)
    assert authorization_schema["$defs"]["target_identity"] == intent_schema["$defs"]["target_identity"]
    assert release_schema["$defs"]["target_identity"] == intent_schema["$defs"]["target_identity"]
    value = intent("v2-contract")
    authorization = mutation_authorization(value, "v2-contract")
    Draft202012Validator(authorization_schema).validate(authorization.to_contract_dict())
    Draft202012Validator(release_schema).validate(release_ack(value, MutatorId.DELL_FAST_RECOVERY).to_contract_dict())


def test_normal_recovery_authorization_kind_cannot_be_used_as_maintenance_authorization() -> None:
    schema = json.loads((ROOT / "contracts/maintenance/v2/mutation_authorization.schema.json").read_text(encoding="utf-8"))
    value = intent("normal-kind-boundary")
    payload = mutation_authorization(value, "normal-kind-boundary").to_contract_dict()
    payload["authorization_kind"] = "NORMAL_RECOVERY"
    assert list(Draft202012Validator(schema).iter_errors(payload))


def test_maintenance_authorization_cannot_be_supplied_to_normal_recovery_interface() -> None:
    parameters = inspect.signature(MaintenanceCoordinator.normal_recovery_mutation).parameters
    assert "authorization_id" not in parameters
    assert "authorization_kind" not in parameters


def test_authorization_interleavings_m11_through_m22_pass(tmp_path: Path) -> None:
    outcomes = authorization_scenarios(tmp_path)
    assert [item["scenario_id"] for item in outcomes] == [f"M-{index:02d}" for index in range(11, 23)]
    assert all(item["classification"] == "PASS" for item in outcomes)
    assert all(item["oracle_violations"] == [] for item in outcomes)
    assert all(item["observation"]["physical_effect_count"] == 0 for item in outcomes)


@pytest.mark.parametrize("index", range(12))
def test_each_authorization_negative_control_is_detected(index: int) -> None:
    outcome = authorization_negative_controls()[index]
    assert outcome["scenario_id"] == f"NC-M{index + 11:02d}"
    assert outcome["classification"] == "EXPECTED_INJECTED_FAILURE"
    assert outcome["oracle_violations"]


def test_manual_planned_executor_uses_same_single_use_contract_and_normal_path_cannot_reuse_it(tmp_path: Path) -> None:
    store = MaintenanceStore(tmp_path / "maintenance.sqlite3")
    coordinator = MaintenanceCoordinator(store, NoActionPhysicalAdapter())
    value = intent("manual-planned")
    authorization = mutation_authorization(value, "manual-planned", executor_id=MaintenanceExecutorId.MANUAL_PLANNED)
    try:
        coordinator.request(value)
        coordinator.begin_quiesce(value.maintenance_id)
        for mutator in REQUIRED_MUTATORS:
            assert coordinator.acknowledge(value.maintenance_id, ack(value, mutator))
        assert coordinator.establish(value.maintenance_id)
        assert coordinator.issue_mutation_authorization(authorization, now=AUTH_VALID_AT) == "ISSUED"
        assert (
            coordinator.accept_mutation_authorization(
                authorization.authorization_id,
                executor_id=MaintenanceExecutorId.PLANNED_ROLLOUT,
                operation=authorization.operation,
                resource_identity=RESOURCE_IDENTITY,
                requested_generation=value.generation,
                observed_target=value.target_identity,
                now=AUTH_VALID_AT,
            )
            == "AUTHORIZATION_EXECUTOR_MISMATCH"
        )
        assert not coordinator.normal_recovery_mutation(value.maintenance_id, MutatorId.CRA_COMMAND)
        assert not coordinator.planned_mutation(value.maintenance_id)
        assert coordinator.adapter.physical_effect_count == 0
    finally:
        store.close()


def test_maintenance_authorization_effect_boundary_honors_db_deadline(tmp_path: Path) -> None:
    class Clock:
        value = 100.0

        def __call__(self) -> float:
            return self.value

    clock = Clock()
    store = MaintenanceStore(tmp_path / "maintenance.sqlite3")
    coordinator = MaintenanceCoordinator(store, NoActionPhysicalAdapter())
    value = intent("authorization-deadline")
    authorization = mutation_authorization(value, "authorization-deadline")
    try:
        coordinator.request(value)
        coordinator.begin_quiesce(value.maintenance_id)
        for mutator in REQUIRED_MUTATORS:
            assert coordinator.acknowledge(value.maintenance_id, ack(value, mutator))
        assert coordinator.establish(value.maintenance_id)
        assert coordinator.issue_mutation_authorization(authorization, now=AUTH_VALID_AT) == "ISSUED"
        assert (
            coordinator.accept_mutation_authorization(
                authorization.authorization_id,
                executor_id=authorization.executor_id,
                operation=authorization.operation,
                resource_identity=authorization.resource_identity,
                requested_generation=value.generation,
                observed_target=value.target_identity,
                now=AUTH_VALID_AT,
            )
            == "ACCEPTED"
        )
        deadline = AuthorityOperationDeadline.start(deadline_seconds=3.0, monotonic=clock)
        clock.value += 4.25
        decision = coordinator.execute_authorized_mutation(
            authorization.authorization_id,
            executor_id=authorization.executor_id,
            operation=authorization.operation,
            resource_identity=authorization.resource_identity,
            requested_generation=value.generation,
            observed_target=value.target_identity,
            now=AUTH_VALID_AT,
            deadline=deadline,
        )
        assert not decision.allowed
        assert decision.reason_code == "DB_DEADLINE_AUTHORITY_INVALIDATED"
        assert store.row(value.maintenance_id)["planned_would_mutate_count"] == 0
        assert coordinator.adapter.physical_effect_count == 0
    finally:
        store.close()


def test_in_flight_ack_is_rejected_and_cannot_establish(tmp_path: Path) -> None:
    store = MaintenanceStore(tmp_path / "maintenance.sqlite3")
    coordinator = MaintenanceCoordinator(store, NoActionPhysicalAdapter())
    value = intent("in-flight")
    try:
        coordinator.request(value)
        coordinator.begin_quiesce(value.maintenance_id)
        for mutator in REQUIRED_MUTATORS:
            accepted = coordinator.acknowledge(
                value.maintenance_id,
                ack(value, mutator, in_flight=1 if mutator == MutatorId.DELL_FAST_RECOVERY else 0),
            )
            assert accepted is (mutator != MutatorId.DELL_FAST_RECOVERY)
        assert not coordinator.establish(value.maintenance_id)
        assert store.row(value.maintenance_id)["state"] == MaintenanceState.QUIESCE_FAILED.value
    finally:
        store.close()


def test_missing_persistent_fence_after_restart_safe_blocks(tmp_path: Path) -> None:
    store = MaintenanceStore(tmp_path / "maintenance.sqlite3")
    coordinator = MaintenanceCoordinator(store, NoActionPhysicalAdapter())
    value = intent("missing-fence")
    try:
        coordinator.request(value)
        coordinator.begin_quiesce(value.maintenance_id)
        for mutator in REQUIRED_MUTATORS:
            assert coordinator.acknowledge(value.maintenance_id, ack(value, mutator))
        assert coordinator.establish(value.maintenance_id)
        with store.connection:
            store.connection.execute(
                "UPDATE maintenance_fences SET active=0 WHERE maintenance_id=? AND mutator_id=?",
                (value.maintenance_id, MutatorId.ARENA_REMOTE_RECOVERY.value),
            )
        assert coordinator.recover_after_restart(value.maintenance_id) == MaintenanceState.SAFE_BLOCKED
        assert store.row(value.maintenance_id)["authority_state"] == "SAFE_BLOCKED"
    finally:
        store.close()


def test_abort_without_reconciliation_keeps_fences_and_safe_blocks(tmp_path: Path) -> None:
    store = MaintenanceStore(tmp_path / "maintenance.sqlite3")
    coordinator = MaintenanceCoordinator(store, NoActionPhysicalAdapter())
    value = intent("abort-unknown")
    try:
        coordinator.request(value)
        coordinator.begin_quiesce(value.maintenance_id)
        assert not coordinator.abort(value.maintenance_id, reconciliation_complete=False)
        assert store.row(value.maintenance_id)["state"] == MaintenanceState.SAFE_BLOCKED.value
        assert store.active_fence_count(value.maintenance_id) == len(REQUIRED_MUTATORS)
    finally:
        store.close()


def test_reconciled_abort_supersedes_issued_authorization_before_releasing_fences(tmp_path: Path) -> None:
    store = MaintenanceStore(tmp_path / "maintenance.sqlite3")
    coordinator = MaintenanceCoordinator(store, NoActionPhysicalAdapter())
    value = intent("abort-supersede")
    authorization = mutation_authorization(value, "abort-supersede")
    try:
        coordinator.request(value)
        coordinator.begin_quiesce(value.maintenance_id)
        for mutator in REQUIRED_MUTATORS:
            assert coordinator.acknowledge(value.maintenance_id, ack(value, mutator))
        assert coordinator.establish(value.maintenance_id)
        assert coordinator.issue_mutation_authorization(authorization, now=AUTH_VALID_AT) == "ISSUED"
        assert coordinator.abort(value.maintenance_id, reconciliation_complete=True)
        assert store.authorization_summary(value.maintenance_id)["states"][authorization.authorization_id] == "SUPERSEDED"
        assert store.active_fence_count(value.maintenance_id) == 0
        assert coordinator.adapter.physical_effect_count == 0
    finally:
        store.close()
