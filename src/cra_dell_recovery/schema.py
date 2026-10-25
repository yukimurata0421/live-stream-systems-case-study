from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from .errors import ProtocolValidationError, ProtocolVersionError

PROTOCOL_TO_SCHEMA = {
    "cra_dell_recovery.command.v1": "command.schema.json",
    "cra_dell_recovery.command_receipt.v1": "command_receipt.schema.json",
    "cra_dell_recovery.command_status.v1": "command_status.schema.json",
    "cra_dell_recovery.heartbeat.v1": "heartbeat.schema.json",
    "cra_dell_recovery.agent_state.v1": "agent_state.schema.json",
    "cra_dell_recovery.reconciliation_challenge.v1": "reconciliation_challenge.schema.json",
    "cra_dell_recovery.reconciliation_commit.v1": "reconciliation_commit.schema.json",
    "cra_dell_recovery.local_action_journal_page.v1": "local_action_journal_page.schema.json",
}


class SchemaRegistry:
    def __init__(self, contract_dir: Path):
        self.contract_dir = contract_dir
        self._validators: dict[str, Draft202012Validator] = {}

    def validate(self, payload: dict[str, Any]) -> None:
        protocol = str(payload.get("protocol") or "")
        filename = PROTOCOL_TO_SCHEMA.get(protocol)
        if filename is None:
            raise ProtocolVersionError(f"unsupported protocol: {protocol or '<missing>'}")
        validator = self._validators.get(protocol)
        if validator is None:
            schema_path = self.contract_dir / filename
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            validator = Draft202012Validator(schema, format_checker=FormatChecker())
            self._validators[protocol] = validator
        errors = sorted(validator.iter_errors(payload), key=lambda item: list(item.absolute_path))
        if errors:
            first: ValidationError = errors[0]
            location = ".".join(str(item) for item in first.absolute_path) or "$"
            raise ProtocolValidationError(f"{location}: {first.message}")
