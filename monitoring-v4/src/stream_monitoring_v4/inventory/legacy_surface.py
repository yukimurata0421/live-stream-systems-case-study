from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA = "monitoring_v4.legacy_monitoring_surface.v1"
CONTRACT_SCHEMA = "monitoring_v4.legacy_monitoring_surface_contract.v1"
DISPOSITIONS = frozenset({"ported_v4_shadow", "retained_v3", "diagnostic_only", "retired"})
_ALERT = re.compile(r"(?m)^\s*-\s+alert:\s*([A-Za-z][A-Za-z0-9_]{1,127})\s*$")


def scan_prometheus_alerts(paths: Iterable[Path]) -> tuple[str, ...]:
    names: list[str] = []
    for path in sorted(Path(value) for value in paths):
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Prometheus rule source is missing or linked: {path}")
        names.extend(_ALERT.findall(path.read_text(encoding="utf-8")))
    if len(names) != len(set(names)):
        duplicates = sorted(name for name in set(names) if names.count(name) > 1)
        raise ValueError(f"duplicate Prometheus alert names: {duplicates}")
    return tuple(sorted(names))


def _template(value: ast.expr) -> str | None:
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    if not isinstance(value, ast.JoinedStr):
        return None
    parts: list[str] = []
    for item in value.values:
        if isinstance(item, ast.Constant):
            parts.append(str(item.value))
        elif isinstance(item, ast.FormattedValue):
            parts.append("{" + ast.unparse(item.value) + "}")
    return "".join(parts)


def scan_incident_templates(path: Path) -> tuple[str, ...]:
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError("legacy incident source is missing or linked")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    templates: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = node.func.id if isinstance(node.func, ast.Name) else (
                node.func.attr if isinstance(node.func, ast.Attribute) else ""
            )
            if name != "incident":
                continue
            for keyword in node.keywords:
                if keyword.arg == "ident" and (value := _template(keyword.value)):
                    templates.add(value)
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "report_specs" for target in node.targets)
            and isinstance(node.value, (ast.List, ast.Tuple))
        ):
            for row in node.value.elts:
                if isinstance(row, (ast.List, ast.Tuple)) and row.elts:
                    value = _template(row.elts[0])
                    if value:
                        templates.add(value)
    return tuple(sorted(templates))


def _declared_surface(raw: object, *, field: str) -> tuple[dict[str, str], list[str]]:
    if not isinstance(raw, Mapping) or set(raw) - DISPOSITIONS:
        raise ValueError(f"{field} disposition map is invalid")
    declared: dict[str, str] = {}
    duplicates: list[str] = []
    for disposition, values in raw.items():
        if disposition not in DISPOSITIONS or not isinstance(values, list):
            raise ValueError(f"{field} disposition is invalid")
        for value in values:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} name is invalid")
            if value in declared:
                duplicates.append(value)
            declared[value] = disposition
    return declared, sorted(set(duplicates))


def evaluate_legacy_surface(
    contract: Mapping[str, Any],
    *,
    observed_alerts: Iterable[str],
    observed_incident_templates: Iterable[str],
) -> dict[str, Any]:
    if contract.get("schema") != CONTRACT_SCHEMA:
        raise ValueError("legacy surface contract schema is unsupported")
    alerts, alert_duplicates = _declared_surface(contract.get("alerts"), field="alerts")
    incidents, incident_duplicates = _declared_surface(
        contract.get("incident_templates"),
        field="incident_templates",
    )
    actual_alerts = set(observed_alerts)
    actual_incidents = set(observed_incident_templates)
    missing_alerts = sorted(set(alerts) - actual_alerts)
    unexpected_alerts = sorted(actual_alerts - set(alerts))
    missing_incidents = sorted(set(incidents) - actual_incidents)
    unexpected_incidents = sorted(actual_incidents - set(incidents))
    retained = sorted(
        [name for name, disposition in alerts.items() if disposition == "retained_v3"]
        + [name for name, disposition in incidents.items() if disposition == "retained_v3"]
    )
    static_ok = not any(
        (
            alert_duplicates,
            incident_duplicates,
            missing_alerts,
            unexpected_alerts,
            missing_incidents,
            unexpected_incidents,
        )
    )
    return {
        "schema": SCHEMA,
        "static_ok": static_ok,
        "cutover_ready": static_ok and not retained,
        "alert_count": len(actual_alerts),
        "incident_template_count": len(actual_incidents),
        "missing_alerts": missing_alerts,
        "unexpected_alerts": unexpected_alerts,
        "missing_incident_templates": missing_incidents,
        "unexpected_incident_templates": unexpected_incidents,
        "duplicate_contract_alerts": alert_duplicates,
        "duplicate_contract_incident_templates": incident_duplicates,
        "retained_v3_count": len(retained),
        "retained_v3": retained,
        "v4_first_class_domains": sorted(
            value
            for value in contract.get("v4_first_class_domains", [])
            if isinstance(value, str)
        ),
    }


def load_legacy_surface_contract(path: Path) -> Mapping[str, Any]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("legacy surface contract must be an object")
    return raw
