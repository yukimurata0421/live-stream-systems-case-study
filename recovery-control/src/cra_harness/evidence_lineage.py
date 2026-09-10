"""Validated CRA evidence-lineage and restart-domain map.

The canonical map describes repository structure only.  Live identity, health,
restart counts and soak verdicts belong to a separately captured overlay and
are never inferred from the checkout.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import json
import re
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

MAP_PATH = "harness/scenarios/cra_evidence_lineage_v1.json"
SCHEMA_PATH = "harness/contracts/evidence_lineage.v1.schema.json"
DOC_PATH = "docs/oracle/05_evidence_lineage_and_restart_domains.md"

_FORTY_HEX = re.compile(r"(?<![0-9a-f])[0-9a-f]{40}(?![0-9a-f])")
_LIVE_EPOCH = re.compile(r"recovery-[a-z0-9]{8,}-20\d{6}")
_REQUIRED_RESTART_DOMAINS = frozenset(
    {
        "restart.dell_host",
        "restart.arena_host",
        "restart.cra_host",
        "restart.dell_pod",
        "restart.dell_container",
        "restart.owner_process",
        "restart.ffmpeg_child",
        "restart.auxiliary_child",
        "restart.dell_observation",
        "restart.arena_observation",
        "restart.cra_observation",
        "restart.cra_runtime",
        "restart.soak_collector",
        "restart.gate_watchdog",
        "restart.resilient_status_soak",
        "restart.no_action_breach",
    }
)
_REQUIRED_ROUTE_EDGES = {
    "host_status.dell_source": ("dell.host", "dell.resilient_host_status"),
    "host_status.dell_serve": ("dell.resilient_host_status", "dell.monitoring_server"),
    "host_status.dell_arena": ("dell.monitoring_server", "arena.dell_resilient_pull"),
    "host_status.arena_source": ("arena.host", "arena.resilient_host_status"),
    "host_status.arena_relay": ("arena.dell_resilient_pull", "arena.resilient_host_status"),
    "host_status.arena_pipeline": ("arena.live_adapter", "arena.resilient_host_status"),
    "host_status.arena_serve": ("arena.resilient_host_status", "arena.projection_server"),
    "host_status.arena_cra": ("arena.projection_server", "cra.arena_resilient_pull"),
    "host_status.cra_soak": ("cra.arena_resilient_pull", "cra.resilient_status_soak"),
    "control.cra_host_runtime": ("cra.host", "cra.runtime"),
}


def _read_json(root: Path, relative: str) -> Any:
    path = root / relative
    if Path(relative).is_absolute() or ".." in Path(relative).parts or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"EVIDENCE_MAP_PATH_ESCAPE:{relative}")
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError(f"EVIDENCE_MAP_FILE_INVALID:{relative}")
    return json.loads(path.read_text(encoding="utf-8"))


def canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def mapping_digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _implementation_path(reference: str) -> Path:
    module, separator, symbol = reference.partition("::")
    if not separator or not module or not symbol:
        raise ValueError(f"EVIDENCE_MAP_IMPLEMENTATION_REF_INVALID:{reference}")
    prefix = Path() if module.startswith("tools.") else Path("src")
    return prefix / Path(*module.split(".")).with_suffix(".py")


def _symbol_exists(root: Path, reference: str) -> bool:
    relative = _implementation_path(reference)
    path = root / relative
    if path.is_symlink() or not path.is_file():
        return False
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(relative))
    except (OSError, SyntaxError, UnicodeError):
        return False
    body: Sequence[ast.stmt] = tree.body
    for part in reference.split("::", 1)[1].split("."):
        match = next(
            (node for node in body if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == part),
            None,
        )
        if match is None:
            return False
        body = match.body if isinstance(match, ast.ClassDef) else ()
    return True


def _safe_repo_path(root: Path, relative: object) -> bool:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute() or ".." in Path(relative).parts:
        return False
    path = root / relative
    return path.resolve().is_relative_to(root.resolve()) and path.is_file() and not path.is_symlink()


def _duplicates(values: Sequence[object]) -> list[str]:
    seen: set[object] = set()
    result: set[str] = set()
    for value in values:
        if value in seen:
            result.add(str(value))
        seen.add(value)
    return sorted(result)


def _graph_cycle(node_ids: set[str], edges: Sequence[Mapping[str, Any]]) -> bool:
    incoming = dict.fromkeys(node_ids, 0)
    downstream: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        source, target = edge.get("from"), edge.get("to")
        if source in node_ids and target in node_ids:
            downstream[str(source)].append(str(target))
            incoming[str(target)] += 1
    queue = deque(sorted(node for node, count in incoming.items() if count == 0))
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for target in downstream[node]:
            incoming[target] -= 1
            if incoming[target] == 0:
                queue.append(target)
    return visited != len(node_ids)


def validate_map(root: Path, value: Mapping[str, Any]) -> list[str]:
    """Return deterministic structural/semantic issues without touching live state."""
    root = root.resolve()
    errors: list[str] = []
    try:
        schema = _read_json(root, SCHEMA_PATH)
        for error in sorted(Draft202012Validator(schema).iter_errors(value), key=lambda item: list(item.absolute_path)):
            location = ".".join(str(part) for part in error.absolute_path) or "$"
            errors.append(f"SCHEMA:{location}:{error.validator}")
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        return [f"SCHEMA_LOAD:{type(error).__name__}"]
    if errors:
        return errors

    hosts = {item["id"]: item for item in value["hosts"]}
    layers = {item["id"]: item for item in value["layers"]}
    nodes = {item["id"]: item for item in value["nodes"]}
    edges = {item["id"]: item for item in value["edges"]}
    restarts = {item["id"]: item for item in value["restart_domains"]}
    for label, items in (
        ("HOST", value["hosts"]),
        ("LAYER", value["layers"]),
        ("NODE", value["nodes"]),
        ("EDGE", value["edges"]),
        ("RESTART", value["restart_domains"]),
    ):
        for duplicate in _duplicates([item["id"] for item in items]):
            errors.append(f"{label}_ID_DUPLICATE:{duplicate}")
    orders = [item["order"] for item in value["layers"]]
    if sorted(orders) != list(range(1, len(orders) + 1)):
        errors.append("LAYER_ORDER_NOT_CONTIGUOUS")

    owner = value["owner_verification"]
    if not _safe_repo_path(root, owner["catalog"]):
        errors.append(f"OWNER_CATALOG_MISSING:{owner['catalog']}")
    else:
        try:
            catalog = _read_json(root, owner["catalog"])
            components = catalog["assurance"]["traceability"]["system_ir"]["components"]
            if owner["component_id"] not in {item.get("id") for item in components}:
                errors.append(f"OWNER_COMPONENT_UNKNOWN:{owner['component_id']}")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            errors.append("OWNER_CATALOG_INVALID")

    serialized = canonical_bytes(value).decode()
    if _FORTY_HEX.search(serialized):
        errors.append("CURRENT_SOURCE_COMMIT_EMBEDDED")
    if _LIVE_EPOCH.search(serialized):
        errors.append("CURRENT_EPOCH_EMBEDDED")

    incident_nodes: set[str] = set()
    for node in value["nodes"]:
        node_id = node["id"]
        if node["host"] not in hosts:
            errors.append(f"NODE_HOST_UNKNOWN:{node_id}:{node['host']}")
        if node["layer"] not in layers:
            errors.append(f"NODE_LAYER_UNKNOWN:{node_id}:{node['layer']}")
        if node["current_value_policy"] != "LIVE_OVERLAY_ONLY":
            errors.append(f"NODE_CURRENT_VALUE_POLICY:{node_id}")
        for path in node["source_refs"]:
            if not _safe_repo_path(root, path):
                errors.append(f"NODE_SOURCE_MISSING:{node_id}:{path}")
        for reference in node["implementation_refs"]:
            try:
                exists = _symbol_exists(root, reference)
            except ValueError:
                exists = False
            if not exists:
                errors.append(f"NODE_SYMBOL_MISSING:{node_id}:{reference}")
        for unit in node["unit_templates"]:
            if "<release>" not in unit["template"]:
                errors.append(f"UNIT_TEMPLATE_NOT_GENERIC:{node_id}:{unit['template']}")
            if not _safe_repo_path(root, unit["definition"]):
                errors.append(f"UNIT_DEFINITION_MISSING:{node_id}:{unit['definition']}")

    for edge in value["edges"]:
        edge_id = edge["id"]
        if edge["from"] not in nodes or edge["to"] not in nodes:
            errors.append(f"EDGE_NODE_UNKNOWN:{edge_id}")
        elif edge["from"] == edge["to"]:
            errors.append(f"EDGE_SELF_LOOP:{edge_id}")
        else:
            incident_nodes.update((edge["from"], edge["to"]))
        for path in edge["contract_refs"]:
            if not _safe_repo_path(root, path):
                errors.append(f"EDGE_CONTRACT_MISSING:{edge_id}:{path}")
        if edge["assertion"] == "MUST_REMAIN_DISABLED" and edge["mode"] != "DISABLED_PATH":
            errors.append(f"DISABLED_ASSERTION_MODE:{edge_id}")
        if edge["mode"] == "DISABLED_PATH" and edge["assertion"] != "MUST_REMAIN_DISABLED":
            errors.append(f"DISABLED_PATH_ASSERTION:{edge_id}")
        policy = edge["failure_policy"]
        if policy["current_sut_state"] == "NOT_OBSERVABLE" and policy["soak_impact"] == "FAIL":
            errors.append(f"EVIDENCE_LOSS_ESCALATES_TO_FAILURE:{edge_id}")
    for edge_id, expected in _REQUIRED_ROUTE_EDGES.items():
        edge = edges.get(edge_id)
        if edge is None:
            errors.append(f"REQUIRED_ROUTE_EDGE_MISSING:{edge_id}")
        elif (edge["from"], edge["to"]) != expected:
            errors.append(f"REQUIRED_ROUTE_EDGE_MISMATCH:{edge_id}")
    for node_id in sorted(set(nodes) - incident_nodes):
        errors.append(f"NODE_DISCONNECTED:{node_id}")
    if not errors and _graph_cycle(set(nodes), value["edges"]):
        errors.append("EVIDENCE_GRAPH_CYCLE")

    missing_domains = sorted(_REQUIRED_RESTART_DOMAINS - set(restarts))
    extra_domains = sorted(set(restarts) - _REQUIRED_RESTART_DOMAINS)
    errors.extend(f"RESTART_DOMAIN_MISSING:{item}" for item in missing_domains)
    errors.extend(f"RESTART_DOMAIN_UNREVIEWED:{item}" for item in extra_domains)
    for restart in value["restart_domains"]:
        restart_id = restart["id"]
        for node_id in restart["scope_nodes"]:
            if node_id not in nodes:
                errors.append(f"RESTART_NODE_UNKNOWN:{restart_id}:{node_id}")
        for edge_id in restart["observed_by_edges"]:
            if edge_id not in edges:
                errors.append(f"RESTART_EDGE_UNKNOWN:{restart_id}:{edge_id}")
    restart_scoped_nodes = {node_id for restart in value["restart_domains"] for node_id in restart["scope_nodes"]}
    for node in value["nodes"]:
        if node["unit_templates"] and node["id"] not in restart_scoped_nodes:
            errors.append(f"UNIT_NODE_RESTART_DOMAIN_MISSING:{node['id']}")
    return sorted(set(errors))


def load_map(root: Path) -> dict[str, Any]:
    value = _read_json(root.resolve(), MAP_PATH)
    if not isinstance(value, dict):
        raise ValueError("EVIDENCE_MAP_NOT_OBJECT")
    errors = validate_map(root, value)
    if errors:
        raise ValueError("EVIDENCE_MAP_INVALID:" + ",".join(errors))
    return value


def check_map(root: Path) -> dict[str, Any]:
    try:
        value = _read_json(root.resolve(), MAP_PATH)
        if not isinstance(value, dict):
            raise ValueError("EVIDENCE_MAP_NOT_OBJECT")
        errors = validate_map(root, value)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        return {
            "schema": "cra.evidence_lineage_check.v1",
            "status": "HOLD",
            "errors": [f"MAP_LOAD:{type(error).__name__}"],
        }
    return {
        "schema": "cra.evidence_lineage_check.v1",
        "status": "PASS" if not errors else "HOLD",
        "mapping_sha256": mapping_digest(value),
        "errors": errors,
        "counts": {
            "hosts": len(value["hosts"]),
            "layers": len(value["layers"]),
            "nodes": len(value["nodes"]),
            "edges": len(value["edges"]),
            "restart_domains": len(value["restart_domains"]),
        },
    }


def build_view(
    root: Path,
    *,
    layer: str | None = None,
    node: str | None = None,
    restart_domain: str | None = None,
) -> dict[str, Any]:
    value = load_map(root)
    nodes_by_id = {item["id"]: item for item in value["nodes"]}
    edges_by_id = {item["id"]: item for item in value["edges"]}
    restarts_by_id = {item["id"]: item for item in value["restart_domains"]}
    if layer is not None and layer not in {item["id"] for item in value["layers"]}:
        raise ValueError(f"EVIDENCE_MAP_LAYER_UNKNOWN:{layer}")
    if node is not None and node not in nodes_by_id:
        raise ValueError(f"EVIDENCE_MAP_NODE_UNKNOWN:{node}")
    if restart_domain is not None and restart_domain not in restarts_by_id:
        raise ValueError(f"EVIDENCE_MAP_RESTART_UNKNOWN:{restart_domain}")

    selected_nodes = set(nodes_by_id)
    selected_edges = set(edges_by_id)
    selected_restarts = set(restarts_by_id)
    filtered = any(item is not None for item in (layer, node, restart_domain))
    if filtered:
        selected_nodes = set()
        selected_edges = set()
        selected_restarts = set()
        if layer is not None:
            selected_nodes.update(item["id"] for item in value["nodes"] if item["layer"] == layer)
        if node is not None:
            selected_nodes.add(node)
        if restart_domain is not None:
            restart = restarts_by_id[restart_domain]
            selected_restarts.add(restart_domain)
            selected_nodes.update(restart["scope_nodes"])
            selected_edges.update(restart["observed_by_edges"])
        selected_edges.update(item["id"] for item in value["edges"] if item["from"] in selected_nodes or item["to"] in selected_nodes)
        for edge_id in tuple(selected_edges):
            edge = edges_by_id[edge_id]
            selected_nodes.update((edge["from"], edge["to"]))
        selected_restarts.update(item["id"] for item in value["restart_domains"] if selected_nodes.intersection(item["scope_nodes"]))
        if restart_domain is not None and layer is None and node is None:
            selected_restarts = {restart_domain}
    return {
        "schema": "cra.evidence_lineage_view.v1",
        "status": "PASS",
        "mapping_sha256": mapping_digest(value),
        "filters": {"layer": layer, "node": node, "restart_domain": restart_domain},
        "boundaries": copy.deepcopy(value["boundaries"]),
        "hosts": copy.deepcopy(value["hosts"]),
        "layers": copy.deepcopy(value["layers"]),
        "nodes": [copy.deepcopy(item) for item in value["nodes"] if item["id"] in selected_nodes],
        "edges": [copy.deepcopy(item) for item in value["edges"] if item["id"] in selected_edges],
        "restart_domains": [copy.deepcopy(item) for item in value["restart_domains"] if item["id"] in selected_restarts],
        "owner_verification": copy.deepcopy(value["owner_verification"]),
        "live_overlay_contract": copy.deepcopy(value["live_overlay_contract"]),
    }


def _parse_jst(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.endswith("+09:00"):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def validate_live_overlay(root: Path, overlay: Mapping[str, Any]) -> list[str]:
    """Validate a supplied read-only live snapshot; this function collects nothing."""
    value = load_map(root)
    errors: list[str] = []
    expected_top = {
        "schema",
        "mapping_sha256",
        "window",
        "components",
        "restart_observations",
        "conclusions",
    }
    if set(overlay) != expected_top or overlay.get("schema") != value["live_overlay_contract"]["schema"]:
        return ["OVERLAY_SHAPE_OR_SCHEMA_INVALID"]
    if overlay.get("mapping_sha256") != mapping_digest(value):
        errors.append("OVERLAY_MAPPING_DRIFT")
    window = overlay.get("window")
    if not isinstance(window, dict) or set(window) != {"started_at", "ended_at", "timezone"} or window.get("timezone") != "JST":
        errors.append("OVERLAY_WINDOW_INVALID")
    else:
        started, ended = _parse_jst(window.get("started_at")), _parse_jst(window.get("ended_at"))
        if started is None or ended is None or ended < started:
            errors.append("OVERLAY_WINDOW_INVALID")

    nodes = {item["id"]: item for item in value["nodes"]}
    statuses = set(value["live_overlay_contract"]["component_statuses"])
    components = overlay.get("components")
    if not isinstance(components, dict):
        errors.append("OVERLAY_COMPONENTS_INVALID")
    else:
        if set(components) != set(nodes):
            errors.append("OVERLAY_COMPONENT_COVERAGE")
        for node_id, item in components.items():
            if node_id not in nodes:
                errors.append(f"OVERLAY_NODE_UNKNOWN:{node_id}")
                continue
            if not isinstance(item, dict) or set(item) != {"status", "observed_at", "fresh_until", "identity", "source", "reason_code"}:
                errors.append(f"OVERLAY_COMPONENT_SHAPE:{node_id}")
                continue
            if item["status"] not in statuses:
                errors.append(f"OVERLAY_COMPONENT_STATUS:{node_id}")
            if not isinstance(item["source"], str) or not item["source"].strip():
                errors.append(f"OVERLAY_SOURCE_MISSING:{node_id}")
            if _parse_jst(item["observed_at"]) is None:
                errors.append(f"OVERLAY_COMPONENT_TIME:{node_id}")
            if item["status"] == "OBSERVED":
                fresh_until = None if item["fresh_until"] is None else _parse_jst(item["fresh_until"])
                observed_at = _parse_jst(item["observed_at"])
                if item["fresh_until"] is not None and fresh_until is None:
                    errors.append(f"OVERLAY_COMPONENT_TIME:{node_id}")
                elif fresh_until is not None and observed_at is not None and fresh_until < observed_at:
                    errors.append(f"OVERLAY_COMPONENT_TIME_ORDER:{node_id}")
                identity = item["identity"]
                if not isinstance(identity, dict) or set(identity) != set(nodes[node_id]["identity_fields"]):
                    errors.append(f"OVERLAY_IDENTITY_FIELDS:{node_id}")
                elif any(value is None or (isinstance(value, str) and not value.strip()) for value in identity.values()):
                    errors.append(f"OVERLAY_IDENTITY_VALUE:{node_id}")
                if item["reason_code"] is not None:
                    errors.append(f"OVERLAY_OBSERVED_REASON:{node_id}")
            else:
                if item["identity"] != {}:
                    errors.append(f"OVERLAY_UNOBSERVABLE_IDENTITY:{node_id}")
                if item["fresh_until"] is not None:
                    errors.append(f"OVERLAY_UNOBSERVABLE_FRESHNESS:{node_id}")
                if not isinstance(item["reason_code"], str) or not item["reason_code"].strip():
                    errors.append(f"OVERLAY_REASON_MISSING:{node_id}")

    restart_ids = {item["id"] for item in value["restart_domains"]}
    observations = overlay.get("restart_observations")
    if not isinstance(observations, dict):
        errors.append("OVERLAY_RESTARTS_INVALID")
    else:
        if set(observations) != restart_ids:
            errors.append("OVERLAY_RESTART_COVERAGE")
        for restart_id, item in observations.items():
            if restart_id not in restart_ids:
                errors.append(f"OVERLAY_RESTART_UNKNOWN:{restart_id}")
                continue
            if not isinstance(item, dict) or set(item) != {"status", "count", "latest_at", "source", "reason_code"}:
                errors.append(f"OVERLAY_RESTART_SHAPE:{restart_id}")
                continue
            if item["status"] not in statuses:
                errors.append(f"OVERLAY_RESTART_STATUS:{restart_id}")
            if item["status"] == "OBSERVED":
                if type(item["count"]) is not int or item["count"] < 0:
                    errors.append(f"OVERLAY_RESTART_COUNT:{restart_id}")
                if item["latest_at"] is not None and _parse_jst(item["latest_at"]) is None:
                    errors.append(f"OVERLAY_RESTART_TIME:{restart_id}")
                if item["reason_code"] is not None:
                    errors.append(f"OVERLAY_RESTART_OBSERVED_REASON:{restart_id}")
            elif item["count"] is not None or item["latest_at"] is not None:
                errors.append(f"OVERLAY_RESTART_UNOBSERVABLE_VALUE:{restart_id}")
            elif not isinstance(item["reason_code"], str) or not item["reason_code"].strip():
                errors.append(f"OVERLAY_RESTART_REASON_MISSING:{restart_id}")
            if not isinstance(item["source"], str) or not item["source"].strip():
                errors.append(f"OVERLAY_RESTART_SOURCE:{restart_id}")

    conclusions = overlay.get("conclusions")
    required_axes = set(value["live_overlay_contract"]["required_conclusion_axes"])
    if not isinstance(conclusions, dict) or set(conclusions) != required_axes:
        errors.append("OVERLAY_CONCLUSIONS_INVALID")
    else:
        for axis, conclusion in conclusions.items():
            if not isinstance(conclusion, dict) or set(conclusion) != {"status", "failure_domain", "reason_codes"}:
                errors.append(f"OVERLAY_CONCLUSION_SHAPE:{axis}")
                continue
            status = conclusion["status"]
            domain = conclusion["failure_domain"]
            reasons = conclusion["reason_codes"]
            if status == "UNKNOWN":
                errors.append(f"OVERLAY_CONCLUSION_UNCLASSIFIED:{axis}")
            elif status not in value["live_overlay_contract"]["conclusion_statuses"][axis]:
                errors.append(f"OVERLAY_CONCLUSION_STATUS:{axis}")
            if domain not in value["live_overlay_contract"]["failure_domains"]:
                errors.append(f"OVERLAY_CONCLUSION_DOMAIN:{axis}")
            if (
                not isinstance(reasons, list)
                or any(not isinstance(reason, str) or not reason.strip() or reason == "UNKNOWN" for reason in reasons)
                or len(reasons) != len(set(reasons))
            ):
                errors.append(f"OVERLAY_CONCLUSION_REASONS:{axis}")
            elif status not in {"VERIFIED_HEALTHY", "PASS"} and not reasons:
                errors.append(f"OVERLAY_CONCLUSION_REASON_MISSING:{axis}")
            if status in {"VERIFIED_HEALTHY", "PASS"} and domain != "NONE":
                errors.append(f"OVERLAY_CONCLUSION_HEALTHY_DOMAIN:{axis}")
            if axis == "current_stream_health" and status == "CONFIRMED_FAILURE" and domain != "SUT":
                errors.append("OVERLAY_STREAM_FAILURE_DOMAIN")
    return sorted(set(errors))


def load_live_overlay(root: Path, path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("OVERLAY_FILE_INVALID")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("OVERLAY_NOT_OBJECT")
    return value


def render_check(result: Mapping[str, Any]) -> str:
    lines = [
        "# CRA evidence map 検証",
        "",
        f"判定: {result.get('status', 'HOLD')}",
    ]
    if result.get("mapping_sha256"):
        lines.append(f"mapping sha256: `{result['mapping_sha256']}`")
    counts = result.get("counts")
    if isinstance(counts, dict):
        lines.append("対象: " + ", ".join(f"{name}={count}" for name, count in counts.items()))
    errors = result.get("errors", [])
    if errors:
        lines += ["", "検出:"] + [f"- `{item}`" for item in errors]
    return "\n".join(lines) + "\n"


def render_view(view: Mapping[str, Any]) -> str:
    lines = [
        "# CRA evidence lineage task view",
        "",
        f"判定: {view['status']}",
        f"mapping sha256: `{view['mapping_sha256']}`",
        "",
        "## Nodes",
        "",
        "| ID | layer / host | kind | authority | identity |",
        "|---|---|---|---|---|",
    ]
    lines += [
        f"| `{item['id']}` | `{item['layer']}` / `{item['host']}` | `{item['kind']}` | `{item['authority']}` | "
        f"{', '.join(f'`{field}`' for field in item['identity_fields']) or '-'} |"
        for item in view["nodes"]
    ]
    lines += ["", "## Edges", "", "| ID | from → to | mode / assertion | evidence loss |", "|---|---|---|---|"]
    lines += [
        f"| `{item['id']}` | `{item['from']}` → `{item['to']}` | `{item['mode']}` / `{item['assertion']}` | "
        f"`{item['failure_policy']['evidence_loss_domain']}` → `{item['failure_policy']['soak_impact']}` |"
        for item in view["edges"]
    ]
    lines += ["", "## Restart domains", "", "| ID | scope | event | soak rule |", "|---|---|---|---|"]
    lines += [
        f"| `{item['id']}` | {', '.join(f'`{node}`' for node in item['scope_nodes'])} | {item['event']} | `{item['soak_rule']}` |"
        for item in view["restart_domains"]
    ]
    return "\n".join(lines) + "\n"


def render_document(value: Mapping[str, Any]) -> str:
    layers = sorted(value["layers"], key=lambda item: item["order"])
    nodes_by_layer: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for node in value["nodes"]:
        nodes_by_layer[node["layer"]].append(node)
    lines = [
        "# 05 証拠lineageとrestart domainの正本マップ",
        "",
        "状態: `CURRENT_STATE_SPEC / MACHINE_VALIDATED`",
        "",
        "この文書は `harness/scenarios/cra_evidence_lineage_v1.json` から機械生成する。正本はJSONであり、",
        "owner invariant・fault・probeは `harness/scenarios/owner_observation_chaos_v1.json` を参照して重複定義しない。",
        "",
        "## 境界",
        "",
        f"- 対象: {value['boundaries']['canonical_scope']}",
        "- 実環境の現在値はJSON正本へ書かず、mapping digest付きのread-only live overlayへ置く。",
        "- このマップと文書に配備・restart・action・epoch変更の操作許可はない。",
        "- 証拠欠損は `NOT_OBSERVABLE / CERTIFICATION_PAUSED` とし、有効なSUT故障証拠なしにSUT failureへ変換しない。",
        "",
        "## 全体構造",
        "",
        "```mermaid",
        "flowchart LR",
        "  SUT[Dell host → Deployment → Pod → container → owner → FFmpeg]",
        "  OWNER[owner evidence export]",
        "  MON[Dell mTLS → arena facts/projection → CRA pull]",
        "  STATUS[Dell signed host status → arena signed relay → CRA resilient-status soak]",
        "  OBS[Dell signed packet → arena byte-preserving relay → CRA inbox]",
        "  SOAK[samples.jsonl + state → full replay → saved gate/watchdog]",
        "  CORE[CRA NO_ACTION runtime ↔ Central DB]",
        "  STOP[delivery disabled → Dell effect fence]",
        "  SUT --> OWNER --> OBS --> SOAK",
        "  SUT --> MON --> CORE --> OBS",
        "  SUT --> STATUS",
        "  MON --> STATUS",
        "  CORE -. MUST_REMAIN_DISABLED .-> STOP",
        "```",
        "",
        "## Host authority",
        "",
        "| host key | expected host id | role | authority |",
        "|---|---|---|---|",
    ]
    lines += [f"| `{item['id']}` | `{item['expected_host_id']}` | {item['role']} | {item['authority']} |" for item in value["hosts"]]
    for layer in layers:
        lines += [
            "",
            f"## Layer {layer['order']}: {layer['label']}",
            "",
            "| node | kind / host | authority / failure domain | identity | freshness | source |",
            "|---|---|---|---|---|---|",
        ]
        for node in nodes_by_layer[layer["id"]]:
            sources = "<br>".join(f"`{item}`" for item in node["source_refs"])
            identities = ", ".join(f"`{item}`" for item in node["identity_fields"]) or "-"
            freshness = ", ".join(f"`{item}`" for item in node["freshness_fields"]) or "-"
            lines.append(
                f"| `{node['id']}` {node['label']} | `{node['kind']}` / `{node['host']}` | "
                f"`{node['authority']}` / `{node['failure_domain']}` | {identities} | {freshness} | {sources} |"
            )
    lines += [
        "",
        "## Evidence edges",
        "",
        "| edge | from → to | transport / assertion | required binding | evidence loss |",
        "|---|---|---|---|---|",
    ]
    for edge in value["edges"]:
        bindings = ", ".join(f"`{item}`" for item in edge["required_bindings"])
        policy = edge["failure_policy"]
        lines.append(
            f"| `{edge['id']}` | `{edge['from']}` → `{edge['to']}` | `{edge['mode']}` / `{edge['assertion']}` | "
            f"{bindings} | `{policy['evidence_loss_domain']}` / `{policy['current_sut_state']}` / `{policy['soak_impact']}` |"
        )
    lines += [
        "",
        "## Restart domains",
        "",
        "| domain | scope | identity change | raw observation source | must preserve | formal soak rule | new epoch condition |",
        "|---|---|---|---|---|---|---|",
    ]
    for restart in value["restart_domains"]:
        lines.append(
            f"| `{restart['id']}` {restart['label']} | {', '.join(f'`{item}`' for item in restart['scope_nodes'])} | "
            f"{'; '.join(restart['identity_changes'])} | {'; '.join(restart['observation_sources'])} | "
            f"{'; '.join(restart['must_preserve'])} | "
            f"`{restart['soak_rule']}` | {'; '.join(restart['new_epoch_when'])} |"
        )
    lines += [
        "",
        "## Current-state overlay",
        "",
        "現在報告では、正本JSONのsha256に束縛した `cra.evidence_lineage_live_overlay.v1` を別artifactとして作る。",
        "観測窓はJSTの開始・終了を持ち、全nodeを `OBSERVED / NOT_OBSERVABLE / NOT_APPLICABLE` のいずれかで明示し、",
        "exact identity、freshness、raw source pointerを記録する。全restart domainも同じ状態語彙とsourceへ結び、次の結論を分離する。",
        "",
        "- `current_stream_health`",
        "- `evidence_harness_status`",
        "- `formal_soak_status`",
        "",
        "各結論は `status / failure_domain / reason_codes` を持つ。裸の `UNKNOWN` は結論値に使わない。",
        "ただしraw contractのfail-closedな `UNKNOWN` は削除・改変せず、`NOT_OBSERVABLE`と具体的な",
        "evidence status / failure domain / soak impactへ投影する。",
        "",
        "## Validation / task view",
        "",
        "```bash",
        "PYTHONPATH=src:. .venv/bin/python tools/cra_task.py evidence-map",
        "PYTHONPATH=src:. .venv/bin/python tools/cra_task.py evidence-map --layer soak_observation",
        "PYTHONPATH=src:. .venv/bin/python tools/cra_task.py evidence-map --node dell.ffmpeg",
        "PYTHONPATH=src:. .venv/bin/python tools/cra_task.py evidence-map --restart-domain restart.ffmpeg_child",
        "PYTHONPATH=src:. .venv/bin/python tools/cra_task.py evidence-map --snapshot <read-only-overlay.json>",
        "```",
        "",
        "validatorはJSON Schema、ID一意性、全source/contract/unit path、Python symbol、graph接続とacyclic性、",
        "owner catalog参照、Dell→arena→CRA host-status経路、systemd unitのrestart-domain完全性、current commit/epoch値の混入、",
        "live overlayのmapping/window/identity/source/classified conclusionを検査する。",
    ]
    return "\n".join(lines) + "\n"
