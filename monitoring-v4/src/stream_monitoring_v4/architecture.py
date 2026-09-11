from __future__ import annotations

import ast
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


@dataclass(frozen=True)
class ImportEdge:
    path: Path
    line: int
    module: str
    relative_level: int = 0


@dataclass(frozen=True)
class ArchitectureViolation:
    code: str
    path: Path
    line: int
    module: str
    detail: str

    def to_dict(self, implementation_root: Path, source_root: Path) -> dict[str, Any]:
        resolved = self.path.resolve()
        for label, root in (("implementation", implementation_root), ("source", source_root)):
            try:
                relative = resolved.relative_to(root.resolve())
            except ValueError:
                continue
            rendered_path = f"{label}:{relative}"
            break
        else:
            rendered_path = str(resolved)
        return {
            "code": self.code,
            "path": rendered_path,
            "line": self.line,
            "module": self.module,
            "detail": self.detail,
        }


def implementation_python_files(root: Path) -> list[Path]:
    selected: list[Path] = []
    for base in (root / "src" / "stream_contracts", root / "src" / "stream_monitoring_v4"):
        if base.exists():
            selected.extend(path for path in base.rglob("*.py") if "__pycache__" not in path.parts)
    return sorted(set(selected))


def publisher_python_files(source_root: Path) -> list[Path]:
    publisher = source_root / "ops" / "public-publisher"
    if not publisher.exists():
        return []
    return sorted(path for path in publisher.rglob("*.py") if "__pycache__" not in path.parts)


def _module_name(path: Path, implementation_root: Path) -> str:
    relative = path.relative_to(implementation_root / "src").with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve_relative(module_name: str, imported: str, level: int, *, is_package: bool) -> str:
    package = module_name.split(".") if is_package else module_name.split(".")[:-1]
    keep = len(package) - max(0, level - 1)
    if keep < 0:
        return imported
    parts = package[:keep]
    if imported:
        parts.extend(imported.split("."))
    return ".".join(parts)


def imports_for(path: Path, *, implementation_root: Path | None = None) -> list[ImportEdge]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    edges: list[ImportEdge] = []
    module_name = ""
    if implementation_root is not None:
        try:
            module_name = _module_name(path, implementation_root)
        except ValueError:
            module_name = ""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            edges.extend(ImportEdge(path, node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level > 0 and module_name:
                module = _resolve_relative(
                    module_name,
                    module,
                    node.level,
                    is_package=path.name == "__init__.py",
                )
            edges.append(ImportEdge(path, node.lineno, module, relative_level=node.level))
    return edges


def _starts(module: str, prefixes: Iterable[str]) -> bool:
    return any(module == prefix or module.startswith(prefix + ".") for prefix in prefixes)


def forbidden_imports(implementation_root: Path, *, source_root: Path | None = None) -> list[ArchitectureViolation]:
    violations: list[ArchitectureViolation] = []
    source_root = implementation_root if source_root is None else source_root
    contracts = implementation_root / "src" / "stream_contracts"
    monitoring = implementation_root / "src" / "stream_monitoring_v4"
    publisher = source_root / "ops" / "public-publisher"
    implementation_files = implementation_python_files(implementation_root)
    implementation_file_set = set(implementation_files)
    files = implementation_files + publisher_python_files(source_root)
    for path in files:
        for edge in imports_for(
            path,
            implementation_root=implementation_root if path in implementation_file_set else None,
        ):
            module = edge.module
            code = ""
            detail = ""
            if contracts in path.parents and _starts(
                module,
                ("stream_monitoring_v4", "stream_runtime_executor", "stream_core", "stream_v2", "stream_v3", "watchers"),
            ):
                code = "contracts_import_implementation"
                detail = "contract package must not import runtime or monitoring implementation"
            elif monitoring in path.parents and _starts(
                module,
                ("stream_runtime_executor", "stream_core", "stream_v2", "stream_v3", "watchers"),
            ):
                code = "monitoring_import_runtime_or_legacy_cli"
                detail = "arena monitoring must not import Dell actions or legacy implementation"
            elif "domains" in path.parts and _starts(
                module,
                (
                    "stream_monitoring_v4.adapters",
                    "stream_monitoring_v4.storage",
                    "stream_monitoring_v4.notifications",
                    "stream_monitoring_v4.runtime",
                ),
            ):
                code = "domain_import_application_io"
                detail = "pure domain reducers must not import adapters, storage, notification, or runtime layers"
            elif "domains" in path.parts and _starts(
                module,
                ("os", "pathlib", "subprocess", "socket", "urllib", "requests", "httpx", "stream_core"),
            ):
                code = "domain_import_io"
                detail = "pure domain reducers must not perform filesystem, network, process, or legacy I/O"
            elif "adapters" in path.parts and _starts(
                module,
                (
                    "stream_monitoring_v4.incidents",
                    "stream_monitoring_v4.notifications",
                    "stream_monitoring_v4.storage",
                    "stream_runtime_executor",
                    "stream_core.notifications",
                ),
            ):
                code = "adapter_import_delivery_or_mutation"
                detail = "read-only adapters must not send notifications or mutate runtime"
            elif path.name == "observer.py" and _starts(
                module,
                (
                    "stream_monitoring_v4.current",
                    "stream_monitoring_v4.incidents",
                    "stream_monitoring_v4.notifications",
                ),
            ):
                code = "observer_import_decision_or_delivery"
                detail = "observer process must stop at validated observation and rejection writes"
            elif "providers" in path.parts and _starts(
                module,
                ("socket", "urllib", "requests", "httpx", "aiohttp"),
            ):
                code = "r5_provider_import_network"
                detail = "R5 isolated providers must not implement real network delivery"
            elif publisher in path.parents and _starts(
                module,
                ("stream_contracts.monitoring_v4", "stream_monitoring_v4", "stream_runtime_executor"),
            ):
                code = "publisher_import_internal_v4"
                detail = "Raspberry Pi publisher must remain outside Monitoring v4 internals"
            if code:
                violations.append(ArchitectureViolation(code, path, edge.line, module, detail))
    return violations


REQUIRED_INVENTORY_FIELDS = frozenset(
    {"id", "host_role", "entrypoint", "reads", "writes", "credentials", "mutations", "target_owner"}
)


def load_inventory(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != "monitoring_v4.current_surface_inventory.v1":
        raise ValueError("unsupported monitoring inventory schema")
    entries = payload.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ValueError("inventory entries must be a non-empty list")
    seen: set[str] = set()
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(f"inventory entry {index} is not an object")
        missing = REQUIRED_INVENTORY_FIELDS - set(entry)
        if missing:
            raise ValueError(f"inventory entry {index} missing fields: {sorted(missing)}")
        ident = str(entry["id"])
        if ident in seen:
            raise ValueError(f"duplicate inventory id: {ident}")
        seen.add(ident)
        for field in ("reads", "writes", "credentials", "mutations"):
            if not isinstance(entry[field], list):
                raise ValueError(f"inventory {ident} field {field} must be a list")
    return payload


def git_scoped_python_surface(source_root: Path) -> dict[str, int]:
    completed = subprocess.run(
        ["git", "ls-files", "-z", "--", "src", "ops/scripts", "ops/systemd"],
        cwd=source_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    paths = [source_root / item.decode("utf-8") for item in completed.stdout.split(b"\0") if item]
    files = [path for path in paths if path.exists() and path.suffix in {".py", ".service", ".timer"}]
    physical = 0
    nonblank = 0
    for path in files:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        physical += len(lines)
        nonblank += sum(1 for line in lines if line.strip())
    return {"files": len(files), "physical_loc": physical, "nonblank_loc": nonblank}


def filesystem_python_surface(implementation_root: Path) -> dict[str, int]:
    files = implementation_python_files(implementation_root)
    physical = 0
    nonblank = 0
    for path in files:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        physical += len(lines)
        nonblank += sum(1 for line in lines if line.strip())
    return {"files": len(files), "physical_loc": physical, "nonblank_loc": nonblank}


def _inventory_path_checks(inventory: dict[str, Any], source_root: Path) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for entry in inventory["entries"]:
        raw = str(entry["entrypoint"])
        path = Path(raw)
        resolved = path if path.is_absolute() else source_root / path
        checks.append({"id": str(entry["id"]), "entrypoint": raw, "exists": resolved.is_file()})
    return checks


def migration_source_contamination(source_root: Path) -> list[str]:
    candidates = (
        source_root / "src" / "stream_contracts" / "monitoring_v4",
        source_root / "src" / "stream_monitoring_v4",
        source_root / "tests" / "monitoring_v4",
        source_root / "config" / "monitoring_v4",
        source_root / "ops" / "scripts" / "stream_monitoring_v4_inventory.py",
    )
    return [str(path.relative_to(source_root)) for path in candidates if path.exists()]


def inventory_report(
    implementation_root: Path,
    source_root: Path,
    manifest: Path,
    *,
    live_unit_contract: Path | None = None,
    systemd_snapshots: Sequence[Mapping[str, Any]] = (),
    systemd_collection_errors: Sequence[str] = (),
) -> dict[str, Any]:
    implementation_root = implementation_root.resolve()
    source_root = source_root.resolve()
    inventory = load_inventory(manifest)
    violations = forbidden_imports(implementation_root, source_root=source_root)
    path_checks = _inventory_path_checks(inventory, source_root)
    missing_entrypoints = [item for item in path_checks if not item["exists"]]
    separate_roots = implementation_root != source_root
    source_contamination = migration_source_contamination(source_root)
    structural_ok = separate_roots and not violations and not missing_entrypoints and not source_contamination
    live_surface: dict[str, Any] | None = None
    if live_unit_contract is not None:
        from stream_monitoring_v4.inventory.contract import (
            evaluate_live_unit_contract,
            load_live_unit_contract,
        )

        live_surface = evaluate_live_unit_contract(
            load_live_unit_contract(live_unit_contract),
            systemd_snapshots,
            repositories={"stream_v3": source_root, "stream_v4": implementation_root},
            collection_errors=systemd_collection_errors,
        )
    return {
        "schema": "monitoring_v4.inventory_report.v1",
        "implementation_repository": str(implementation_root),
        "migration_source_repository": str(source_root),
        "separate_roots": separate_roots,
        "inventory_scope": inventory["inventory_scope"],
        "live_enabled_state_verified": bool(inventory.get("live_enabled_state_verified", False)),
        "entry_count": len(inventory["entries"]),
        "entries": inventory["entries"],
        "entrypoint_checks": path_checks,
        "missing_entrypoints": missing_entrypoints,
        "migration_source_contamination": source_contamination,
        "migration_source_git_surface": git_scoped_python_surface(source_root),
        "implementation_surface": filesystem_python_surface(implementation_root),
        "architecture_violations": [
            item.to_dict(implementation_root, source_root) for item in violations
        ],
        "live_surface": live_surface,
        "ok": structural_ok,
        "evidence_complete": structural_ok
        and live_surface is not None
        and live_surface.get("evidence_complete") is True,
    }
