from __future__ import annotations

import json
import sqlite3
from typing import Any

from cra_harness.controls.environment import HarnessEnvironment
from cra_harness.observers.evidence import EvidenceCollector


def _rows(connection: sqlite3.Connection, query: str) -> list[dict[str, Any]]:
    # sqlite3.Row iterates values, not column names; keys() is required here.
    return [{key: row[key] for key in row.keys()} for row in connection.execute(query).fetchall()]  # noqa: SIM118


class SutObserver:
    """Collect raw projections only. No expected-state or pass/fail logic lives here."""

    def capture(
        self,
        collector: EvidenceCollector,
        environment: HarnessEnvironment,
        *,
        protocol_messages: list[dict[str, Any]],
        process_exit: dict[str, Any],
    ) -> None:
        central_commands = _rows(environment.central.connection, "SELECT * FROM commands ORDER BY command_id")
        central_outbox = _rows(environment.central.connection, "SELECT * FROM outbox_messages ORDER BY outbox_id")
        central_identity = _rows(environment.central.connection, "SELECT * FROM control_plane_identity")
        dell_commands = _rows(environment.dell.connection, "SELECT * FROM agent_commands ORDER BY command_id")
        attempts = _rows(environment.dell.connection, "SELECT * FROM execution_attempts ORDER BY execution_id")
        fences = _rows(environment.dell.connection, "SELECT * FROM authority_fences ORDER BY target_id")
        local_actions = _rows(environment.dell.connection, "SELECT * FROM local_actions ORDER BY local_action_id")
        targets = _rows(environment.central.connection, "SELECT * FROM targets ORDER BY target_id")
        agent_identity = _rows(environment.dell.connection, "SELECT * FROM agent_identity")
        collector.append(
            "central_db_snapshot",
            "observer.central.sqlite",
            {
                "commands": central_commands,
                "outbox": central_outbox,
                "identity": central_identity,
                "command_count": len(central_commands),
                "pending_outbox_count": sum(row["state"] in {"PENDING", "RETRY", "IN_FLIGHT"} for row in central_outbox),
            },
        )
        collector.append(
            "dell_db_snapshot",
            "observer.dell.sqlite",
            {
                "commands": dell_commands,
                "attempts": attempts,
                "fences": fences,
                "local_actions": local_actions,
                "unresolved_count": sum(row["state"] in {"ACCEPTED", "EXECUTION_STARTED", "OUTCOME_UNKNOWN"} for row in dell_commands),
            },
        )
        collector.append(
            "authority_lease",
            "observer.dell.fence",
            {
                "central_state": None if not fences else fences[0]["authority_state"],
                "process_lease_valid": environment.dell.process_lease_valid("stream-target"),
            },
        )
        collector.append(
            "central_state",
            "observer.central.state",
            {
                "authority_state": None if not targets else targets[0]["authority_state"],
                "restore_state": None if not central_identity else central_identity[0]["restore_state"],
            },
        )
        collector.append(
            "dell_state",
            "observer.dell.state",
            {
                "authority_state": None if not fences else fences[0]["authority_state"],
                "ledger_state": None if not agent_identity else agent_identity[0]["ledger_state"],
            },
        )
        heartbeat_messages = [message for message in protocol_messages if message.get("message_type") == "authority_heartbeat"]
        collector.append(
            "heartbeat_events",
            "observer.protocol.heartbeat",
            {"count": len(heartbeat_messages), "messages": json.loads(json.dumps(heartbeat_messages))},
        )
        collector.append(
            "lease_events",
            "observer.dell.lease",
            {
                "current": None if not fences else fences[0],
                "process_lease_valid": environment.dell.process_lease_valid("stream-target"),
            },
        )
        collector.append("target_before", "observer.target", environment.target.to_dict())
        after = environment.target.to_dict()
        if attempts and attempts[-1].get("after_target_json"):
            after = json.loads(str(attempts[-1]["after_target_json"]))
        collector.append("target_after", "observer.target", after)
        collector.append(
            "physical_attempts",
            "observer.fake_adapter",
            {"count": environment.adapter.attempt_count, "adapter": type(environment.adapter).__name__},
        )
        collector.append(
            "protocol_messages",
            "observer.protocol",
            {"count": len(protocol_messages), "messages": json.loads(json.dumps(protocol_messages))},
        )
        collector.append("process_exit", "observer.runner", dict(process_exit))
