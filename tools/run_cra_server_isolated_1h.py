from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import re
import resource
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCHEMA_EVENT = "cra.autonomous_repair_event.v1"
SCHEMA_SUMMARY = "cra.autonomous_repair_summary.v1"
JST = timezone(timedelta(hours=9))
REQUIRED_SQLITE = "3.51.3"
NAME_RE = re.compile(r"[^a-zA-Z0-9_.-]+")
PRODUCTION_PATHS = (
    Path("/opt/stream-recovery-control/releases"),
    Path("/opt/stream-recovery-control/runtimes"),
    Path("/var/lib/stream-recovery-control/monitoring-inbox"),
    Path("/var/lib/stream-recovery-control/cra"),
)
CREDENTIAL_PATHS = (Path("/srv/cra-private/.ssh"), Path("/srv/cra-private/.operator-config"))
TARGETED_TESTS = (
    "tests/authority/test_cra_no_action_runtime.py",
    "tests/runtime_boundary/test_effect_protocol.py",
    "tests/runtime_boundary/test_option_bd_policy_acceptance.py",
    "tests/harness/unit/test_no_action_soak_gate.py",
    "tests/harness/unit/test_no_action_soak_sample.py",
    "tests/harness/integration/test_real_sqlite_concurrency_replay.py",
)
REQUIRED_EVIDENCE_PATHS = (
    Path("artifacts/phase4-shadow/20260823T1604JST_sqlite_concurrency_live_v2/real_replay.json"),
    Path("artifacts/phase4-shadow/20260823T1604JST_sqlite_concurrency_live_v2/artifact_hashes.json"),
)


class SafetyViolation(RuntimeError):
    pass


def iso_utc(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z")


def iso_jst(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).astimezone(JST).isoformat()


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    try:
        encoded = (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    try:
        os.write(descriptor, value.encode())
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def loaded_sqlite_library() -> str | None:
    maps = Path("/proc/self/maps")
    if not maps.exists():
        return None
    for line in maps.read_text(encoding="utf-8", errors="replace").splitlines():
        if "libsqlite3.so" in line:
            return line.rsplit(maxsplit=1)[-1]
    return None


def current_rss_bytes() -> int:
    for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    return 0


def credential_accessible(path: Path) -> bool:
    try:
        os.listdir(path)
    except (FileNotFoundError, PermissionError):
        return False
    return True


def network_snapshot() -> dict[str, Any]:
    interface_probe_errno: int | None = None
    try:
        interfaces = [name for _, name in socket.if_nameindex()]
    except OSError as error:
        interfaces = []
        interface_probe_errno = error.errno
    probe_errno: int | None = None
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.settimeout(0.25)
    try:
        probe_errno = probe.connect_ex(("192.0.2.1", 9))
    finally:
        probe.close()
    return {
        "interfaces": interfaces,
        "interface_probe_errno": interface_probe_errno,
        "only_loopback_visible": interface_probe_errno is None and set(interfaces) <= {"lo"},
        "external_probe_errno": probe_errno,
        "external_route_unavailable": probe_errno in {errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ETIMEDOUT},
    }


def resource_snapshot(evidence_root: Path) -> dict[str, Any]:
    disk = shutil.disk_usage(evidence_root)
    return {
        "observed_at": iso_utc(),
        "observed_at_jst": iso_jst(),
        "pid": os.getpid(),
        "rss_bytes": current_rss_bytes(),
        "rss_high_water_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "fd_count": len(list(Path("/proc/self/fd").iterdir())),
        "load_average": list(os.getloadavg()),
        "disk_total_bytes": disk.total,
        "disk_used_bytes": disk.used,
        "disk_free_bytes": disk.free,
    }


def guard_snapshot() -> dict[str, Any]:
    present_production_paths = [str(path) for path in PRODUCTION_PATHS if path.exists()]
    readable_credential_paths = [str(path) for path in CREDENTIAL_PATHS if credential_accessible(path)]
    network = network_snapshot()
    return {
        "production_paths_present": present_production_paths,
        "readable_credential_paths": readable_credential_paths,
        "network": network,
        "safe": (
            not present_production_paths
            and not readable_credential_paths
            and network["only_loopback_visible"]
            and network["external_route_unavailable"]
        ),
    }


def summary_result(summary: dict[str, Any]) -> str:
    accepted = (
        summary["wall_duration_seconds"] >= summary["required_duration_seconds"]
        and summary["maximum_heartbeat_gap_seconds"] <= summary["maximum_heartbeat_gap_allowed_seconds"]
        and summary["unexpected_command_failure_count"] == 0
        and summary["safety_gate_failure_count"] == 0
        and summary["production_mutation_count"] == 0
        and summary["fixed_sqlite_version"] == REQUIRED_SQLITE
        and summary["missing_closeout_count"] == 0
    )
    return "ISOLATED_1H_ACCEPTED" if accepted else "ISOLATED_1H_BLOCKED"


@dataclass
class Recorder:
    source: Path
    evidence: Path
    state: Path
    run_id: str
    started_wall: datetime
    started_monotonic: float
    heartbeat_seconds: float
    sequence: int = 0
    command_sequence: int = 0
    unexpected_failures: int = 0
    safety_failures: int = 0
    heartbeat_monotonic: list[float] = field(default_factory=list)
    last_heartbeat: float = 0.0

    def event(
        self,
        event_type: str,
        summary: str,
        *,
        classification: str = "PASS",
        fact_level: str = "OBSERVED",
        episode_id: str | None = None,
        repair_id: str | None = None,
        evidence_refs: list[str] | None = None,
        command_ref: int | None = None,
        exit_code: int | None = None,
    ) -> None:
        self.sequence += 1
        append_jsonl(
            self.evidence / "events.jsonl",
            {
                "schema": SCHEMA_EVENT,
                "run_id": self.run_id,
                "episode_id": episode_id,
                "repair_id": repair_id,
                "sequence": self.sequence,
                "observed_at": iso_utc(),
                "observed_at_jst": iso_jst(),
                "event_type": event_type,
                "actor": "cra_server_isolated_runner",
                "classification": classification,
                "fact_level": fact_level,
                "summary": summary,
                "evidence_refs": evidence_refs or [],
                "command_ref": command_ref,
                "exit_code": exit_code,
            },
        )

    def heartbeat(self, *, force: bool = False) -> None:
        current = time.monotonic()
        if not force and current - self.last_heartbeat < self.heartbeat_seconds:
            return
        guard = guard_snapshot()
        sample = resource_snapshot(self.evidence)
        sample.update({"run_id": self.run_id, "sequence": len(self.heartbeat_monotonic) + 1, "guard": guard})
        append_jsonl(self.evidence / "heartbeats.jsonl", sample)
        append_jsonl(self.evidence / "resources.jsonl", {key: value for key, value in sample.items() if key != "guard"})
        self.heartbeat_monotonic.append(current)
        self.last_heartbeat = current
        if not guard["safe"]:
            self.safety_failures += 1
            self.event("STOPPED", "isolation safety guard failed", classification="SAFETY_GATE_FAILURE")
            raise SafetyViolation(json.dumps(guard, sort_keys=True))

    def run_command(
        self,
        name: str,
        argv: list[str],
        *,
        allowed_exit_codes: set[int] | None = None,
        remove_environment: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        allowed = allowed_exit_codes or {0}
        safe_name = NAME_RE.sub("-", name).strip("-")
        self.command_sequence += 1
        command_id = self.command_sequence
        output_dir = self.evidence / "commands"
        output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        stdout_path = output_dir / f"{command_id:03d}-{safe_name}.stdout"
        stderr_path = output_dir / f"{command_id:03d}-{safe_name}.stderr"
        environment = dict(os.environ)
        for key in remove_environment:
            environment.pop(key, None)
        environment.update(
            {
                "HOME": str(self.state / "home"),
                "XDG_CACHE_HOME": str(self.state / "xdg-cache"),
                "XDG_CONFIG_HOME": str(self.state / "xdg-config"),
                "HYPOTHESIS_STORAGE_DIRECTORY": str(self.state / "hypothesis"),
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONPATH": f"{self.source / 'src'}:{self.source}",
            }
        )
        started = datetime.now(UTC)
        self.event("TEST_STARTED", name, command_ref=command_id)
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            process = subprocess.Popen(argv, cwd=self.source, env=environment, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
            try:
                while process.poll() is None:
                    self.heartbeat()
                    time.sleep(1)
            except BaseException:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
                raise
        ended = datetime.now(UTC)
        exit_code = int(process.returncode)
        result = {
            "schema": "cra.autonomous_repair_command.v1",
            "run_id": self.run_id,
            "sequence": command_id,
            "name": name,
            "argv": argv,
            "cwd": str(self.source),
            "started_at": iso_utc(started),
            "ended_at": iso_utc(ended),
            "duration_seconds": round((ended - started).total_seconds(), 6),
            "exit_code": exit_code,
            "allowed_exit_codes": sorted(allowed),
            "stdout": f"commands/{stdout_path.name}#sha256:{sha256_file(stdout_path)}",
            "stderr": f"commands/{stderr_path.name}#sha256:{sha256_file(stderr_path)}",
        }
        append_jsonl(self.evidence / "commands.jsonl", result)
        classification = "PASS" if exit_code in allowed else "UNKNOWN_AMBIGUOUS"
        if exit_code not in allowed:
            self.unexpected_failures += 1
        self.event(
            "TEST_FINISHED",
            name,
            classification=classification,
            command_ref=command_id,
            exit_code=exit_code,
            evidence_refs=[result["stdout"], result["stderr"]],
        )
        return result


def write_environment_closeouts(recorder: Recorder, stock_sqlite: str, initial_guard: dict[str, Any], unshare_exit: int) -> None:
    sqlite_episode = recorder.evidence / "episodes" / "cra01-sqlite-runtime"
    sqlite_state = "ISOLATED_ENVIRONMENT_MITIGATED" if sqlite3.sqlite_version == REQUIRED_SQLITE else "ISOLATED_NOT_FIXED"
    library = loaded_sqlite_library()
    library_hash = sha256_file(Path(library)) if library and Path(library).is_file() else "UNKNOWN"
    write_text(
        sqlite_episode / "closeout_v1.md",
        f"""# CRA-01 SQLite Runtime Episode Closeout v1

## 結論

- State: `{sqlite_state}`
- Production status: `NOT_TOUCHED`
- OS標準PythonはSQLite `{stock_sqlite}`で、CRA production gate `{REQUIRED_SQLITE}`を満たさない。
- 隔離unitはproject固定SQLite `{sqlite3.sqlite_version}`を`LD_LIBRARY_PATH`で読み込んだ。

## 原因と対策

- `OBSERVED`: cra-01のOS標準SQLiteは`{stock_sqlite}`。
- Systemic cause: OS package runtimeをCRA production constraintと同一視できない。
- 対策: source-bound固定SQLiteをunit単位でloadし、process内versionとloaded library identityを同時検証する。
- Loaded library: `{library or "UNKNOWN"}`
- Loaded library SHA-256: `{library_hash}`
- Intentionally unchanged: OS SQLite、production release/runtime/DB、他service。

## 一般化と残存risk

- host package upgrade/downgrade、別Python起動、`LD_LIBRARY_PATH`脱落を同じruntime identity gateで検出する。
- この対策はSQLite以外のABI差分を証明しない。1時間runのfull regressionとresource evidenceを別gateとする。
""",
    )
    network_episode = recorder.evidence / "episodes" / "cra01-network-isolation"
    write_text(
        network_episode / "closeout_v1.md",
        f"""# CRA-01 Network Isolation Episode Closeout v1

## 結論

- State: `ISOLATED_ENVIRONMENT_MITIGATED`
- Production status: `NOT_TOUCHED`
- 非特権`unshare --user --map-root-user --net`はexit `{unshare_exit}`で利用不能。
- root-owned transient systemd sandboxの`PrivateNetwork`とresource/capability制限へ切り替えた。

## 検証

- Visible interfaces: `{json.dumps(initial_guard["network"]["interfaces"])}`
- Interface probe errno: `{initial_guard["network"]["interface_probe_errno"]}`
- Only loopback visible: `{initial_guard["network"]["only_loopback_visible"]}`
- External probe errno: `{initial_guard["network"]["external_probe_errno"]}`
- External route unavailable: `{initial_guard["network"]["external_route_unavailable"]}`
- Readable credential paths: `{json.dumps(initial_guard["readable_credential_paths"])}`
- Production paths present: `{json.dumps(initial_guard["production_paths_present"])}`

## Systemic causeと対策

- Systemic cause: cra-01ではunprivileged user namespace作成がkernel/security policyで拒否される。
- 対策: isolation失敗時にnetwork制限を外さず、root-owned transient unitを明示的sandboxとして使う。interface inventory用の
  `AF_NETLINK`だけをprivate namespace/capability 0内で許可し、inventory不能はSafety Gate failureにする。
- Intentionally unchanged: firewall、network config、SSH、production systemd unit、credential。
""",
    )


def artifact_manifest(evidence: Path) -> None:
    lines: list[str] = []
    for path in sorted(item for item in evidence.rglob("*") if item.is_file() and item.name != "artifacts.sha256"):
        lines.append(f"{sha256_file(path)}  {path.relative_to(evidence)}")
    write_text(evidence / "artifacts.sha256", "\n".join(lines) + "\n")


def require_source_evidence(recorder: Recorder) -> None:
    missing = [str(path) for path in REQUIRED_EVIDENCE_PATHS if not (recorder.source / path).is_file()]
    if not missing:
        return
    recorder.unexpected_failures += 1
    recorder.event(
        "CLASSIFIED",
        f"required source evidence missing: {missing}",
        classification="MISSING_EVIDENCE",
        episode_id="source-evidence-closure",
    )
    write_text(
        recorder.evidence / "episodes" / "source-evidence-closure" / "closeout_v1.md",
        f"""# Source Evidence Closure Episode v1

- State: `HARNESS_BLOCKED`
- Classification: `MISSING_EVIDENCE`
- Missing paths: `{json.dumps(missing)}`
- Production status: `NOT_TOUCHED`
- Required action: build a replacement immutable source bundle containing the exact terminal replay evidence,
  then verify its hashes before test execution.
""",
    )
    raise RuntimeError("required source evidence is missing")


def parse_stock_sqlite(stdout_ref: str, evidence: Path) -> str:
    relative = stdout_ref.split("#sha256:", 1)[0]
    lines = (evidence / relative).read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-1].strip() if lines else "UNKNOWN"


def build_summary(
    recorder: Recorder,
    *,
    required_duration: float,
    maximum_gap: float,
    stock_sqlite: str,
    ended: datetime,
) -> dict[str, Any]:
    gaps = [right - left for left, right in zip(recorder.heartbeat_monotonic, recorder.heartbeat_monotonic[1:], strict=False)]
    duration = (ended - recorder.started_wall).total_seconds()
    closeouts = list((recorder.evidence / "episodes").glob("*/closeout_v*.md"))
    summary: dict[str, Any] = {
        "schema": SCHEMA_SUMMARY,
        "run_id": recorder.run_id,
        "host_id": "cra-01-central-authority",
        "started_at": iso_utc(recorder.started_wall),
        "started_at_jst": iso_jst(recorder.started_wall),
        "ended_at": iso_utc(ended),
        "ended_at_jst": iso_jst(ended),
        "wall_duration_seconds": round(duration, 6),
        "required_duration_seconds": required_duration,
        "heartbeat_count": len(recorder.heartbeat_monotonic),
        "maximum_heartbeat_gap_seconds": round(max(gaps, default=0.0), 6),
        "maximum_heartbeat_gap_allowed_seconds": maximum_gap,
        "command_count": recorder.command_sequence,
        "unexpected_command_failure_count": recorder.unexpected_failures,
        "safety_gate_failure_count": recorder.safety_failures,
        "production_mutation_count": 0,
        "physical_effect_count": 0,
        "credential_access_count": 0,
        "stock_sqlite_version": stock_sqlite,
        "fixed_sqlite_version": sqlite3.sqlite_version,
        "fixed_sqlite_library": loaded_sqlite_library(),
        "episode_closeout_count": len(closeouts),
        "missing_closeout_count": 0 if len(closeouts) >= 2 else 2 - len(closeouts),
    }
    summary["result"] = summary_result(summary)
    return summary


def run(args: argparse.Namespace) -> int:
    source = args.source.resolve()
    evidence = args.evidence.resolve()
    state = args.state.resolve()
    evidence.mkdir(parents=True, exist_ok=False, mode=0o700)
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    for name in ("home", "xdg-cache", "xdg-config", "hypothesis"):
        (state / name).mkdir(parents=True, exist_ok=True, mode=0o700)
    started_wall = datetime.now(UTC)
    recorder = Recorder(source, evidence, state, args.run_id, started_wall, time.monotonic(), args.heartbeat_seconds)
    write_json(
        evidence / "manifest.json",
        {
            "schema": "cra.autonomous_repair_manifest.v1",
            "run_id": args.run_id,
            "host_id": "cra-01-central-authority",
            "hostname": socket.gethostname(),
            "source": str(source),
            "repository_commit": args.repository_commit,
            "source_bundle_sha256": args.source_bundle_sha256,
            "required_duration_seconds": args.duration_seconds,
            "heartbeat_seconds": args.heartbeat_seconds,
            "focused_interval_seconds": args.focused_interval_seconds,
            "isolation": {
                "owner": "root-owned transient systemd unit",
                "private_network_required": True,
                "credential_paths_inaccessible_required": True,
                "production_paths_must_remain_missing": [str(path) for path in PRODUCTION_PATHS],
            },
            "started_at": iso_utc(started_wall),
            "started_at_jst": iso_jst(started_wall),
        },
    )
    recorder.event("DETECTED", "cra-01 isolated one-hour run started")
    recorder.heartbeat(force=True)
    initial_guard = guard_snapshot()
    stock = recorder.run_command(
        "stock-sqlite-probe",
        ["/usr/bin/python3", "-c", "import sqlite3; print(sqlite3.sqlite_version)"],
        remove_environment=("LD_LIBRARY_PATH",),
    )
    stock_sqlite = parse_stock_sqlite(stock["stdout"], evidence)
    unshare = recorder.run_command(
        "unprivileged-userns-probe",
        ["/usr/bin/unshare", "--user", "--map-root-user", "--net", "/bin/true"],
        allowed_exit_codes={0, 1},
    )
    write_environment_closeouts(recorder, stock_sqlite, initial_guard, unshare["exit_code"])
    recorder.event(
        "CLASSIFIED",
        f"stock SQLite {stock_sqlite}; fixed SQLite {sqlite3.sqlite_version}",
        classification="ENVIRONMENT_FAILURE" if stock_sqlite != REQUIRED_SQLITE else "PASS",
        episode_id="cra01-sqlite-runtime",
        repair_id="fixed-sqlite-runtime",
    )
    if sqlite3.sqlite_version != REQUIRED_SQLITE:
        raise SafetyViolation(f"fixed SQLite runtime is {sqlite3.sqlite_version}, expected {REQUIRED_SQLITE}")
    require_source_evidence(recorder)

    python = sys.executable
    targeted = [python, "-m", "pytest", "-q", "-p", "no:cacheprovider", *TARGETED_TESTS]
    recorder.run_command("targeted-acceptance-initial", targeted)
    recorder.run_command(
        "sqlite-concurrency",
        [
            python,
            "-m",
            "cra_harness.runner.sqlite_concurrency",
            "--project-root",
            str(source),
            "--artifact-root",
            str(evidence / "native-artifacts"),
            "--run-id",
            f"{args.run_id}-sqlite",
            "--operations-per-seed",
            str(args.sqlite_operations),
            "--seeds",
            "101",
            "202",
        ],
    )
    recorder.run_command(
        "operational-state-space-v3",
        [
            python,
            "-m",
            "cra_harness.runner.cli_v3",
            "--project-root",
            str(source),
            "--artifact-root",
            str(evidence / "native-artifacts"),
            "--run-id",
            f"{args.run_id}-state-space",
        ],
    )
    recorder.run_command("full-regression-initial", [python, "-m", "pytest", "-q", "-p", "no:cacheprovider"])

    next_focused = recorder.started_monotonic + args.focused_interval_seconds
    while time.monotonic() - recorder.started_monotonic < args.duration_seconds:
        recorder.heartbeat()
        if time.monotonic() >= next_focused:
            index = int((time.monotonic() - recorder.started_monotonic) // args.focused_interval_seconds)
            recorder.run_command(f"targeted-acceptance-periodic-{index:02d}", targeted)
            next_focused += args.focused_interval_seconds
        time.sleep(1)

    recorder.run_command("targeted-acceptance-final", targeted)
    recorder.run_command("full-regression-final", [python, "-m", "pytest", "-q", "-p", "no:cacheprovider"])
    recorder.heartbeat(force=True)
    ended = datetime.now(UTC)
    summary = build_summary(
        recorder,
        required_duration=args.duration_seconds,
        maximum_gap=args.maximum_heartbeat_gap_seconds,
        stock_sqlite=stock_sqlite,
        ended=ended,
    )
    write_json(evidence / "summary.json", summary)
    recorder.event("CLOSEOUT", summary["result"], classification="PASS" if summary["result"].endswith("ACCEPTED") else "UNKNOWN_AMBIGUOUS")
    write_text(
        evidence / "run_closeout_v1.md",
        f"""# CRA-01 Isolated Autonomous Repair Run Closeout v1

## 結論

- Result: `{summary["result"]}`
- JST window: `{summary["started_at_jst"]} -- {summary["ended_at_jst"]}`
- Wall duration: `{summary["wall_duration_seconds"]}` seconds
- Production status: `NOT_TOUCHED`

## Identity

- Run ID: `{args.run_id}`
- Repository commit: `{args.repository_commit}`
- Source bundle SHA-256: `{args.source_bundle_sha256}`
- Host ID: `cra-01-central-authority`
- Fixed SQLite: `{summary["fixed_sqlite_version"]}`
- Loaded library: `{summary["fixed_sqlite_library"]}`

## Gates

- Heartbeats: `{summary["heartbeat_count"]}`
- Maximum heartbeat gap: `{summary["maximum_heartbeat_gap_seconds"]}` seconds
- Commands: `{summary["command_count"]}`
- Unexpected command failures: `{summary["unexpected_command_failure_count"]}`
- Safety gate failures: `{summary["safety_gate_failure_count"]}`
- Production mutation / physical effect / credential access: `0 / 0 / 0`

## Boundary

このrunはcra-01固有runtime/SQLite/filesystem/systemd sandbox上の隔離evidenceであり、CRA NO_ACTION production配備、24時間soak、
Dell effect、arena facts transportを証明しない。
""",
    )
    artifact_manifest(evidence)
    return 0 if summary["result"] == "ISOLATED_1H_ACCEPTED" else 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a persistent one-hour CRA server isolated verification")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--repository-commit", required=True)
    parser.add_argument("--source-bundle-sha256", required=True)
    parser.add_argument("--duration-seconds", type=float, default=3600.0)
    parser.add_argument("--heartbeat-seconds", type=float, default=30.0)
    parser.add_argument("--maximum-heartbeat-gap-seconds", type=float, default=90.0)
    parser.add_argument("--focused-interval-seconds", type=float, default=300.0)
    parser.add_argument("--sqlite-operations", type=int, default=1_000)
    args = parser.parse_args()
    if args.duration_seconds < 1 or not 1 <= args.heartbeat_seconds <= 60:
        raise SystemExit("invalid duration or heartbeat interval")
    if args.focused_interval_seconds < args.heartbeat_seconds or args.sqlite_operations < 1:
        raise SystemExit("invalid focused interval or SQLite operation count")
    try:
        result = run(args)
    except BaseException as error:
        evidence = args.evidence.resolve()
        if evidence.is_dir():
            write_json(
                evidence / "terminal_failure.json",
                {
                    "schema": "cra.autonomous_repair_terminal_failure.v1",
                    "run_id": args.run_id,
                    "observed_at": iso_utc(),
                    "observed_at_jst": iso_jst(),
                    "error_type": type(error).__name__,
                    "message": str(error),
                    "production_mutation_count": 0,
                    "physical_effect_count": 0,
                },
            )
            artifact_manifest(evidence)
        raise
    raise SystemExit(result)


if __name__ == "__main__":
    main()
