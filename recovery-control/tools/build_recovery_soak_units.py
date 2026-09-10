"""Render separate host-local observation units; never install or start them."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

PROFILES = {
    "dell": ("stream-recovery-observation", "stream-recovery-observer", "stream-recovery"),
    "arena": ("stream-monitoring-cra-projection", "stream-monitoring-projection", "stream-monitoring-projection"),
    "cra": ("stream-recovery-control", "stream-recovery", "stream-recovery"),
}


def render(component: str, release_id: str) -> dict[str, str]:
    root, user, group = PROFILES[component]
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{7,127}", release_id):
        raise ValueError("RECOVERY_UNIT_RELEASE_INVALID")
    source = f"/opt/{root}/releases/{release_id}"
    runtime = f"/opt/{root}/runtimes/{release_id}"
    config = f"/etc/{root}/releases/{release_id}"
    state = f"/var/lib/{root}/recovery-observation/{release_id}"
    inbox = f"/var/lib/{root}/recovery-inbox/{release_id}"
    result = {}
    kinds = ["observer"] + (["serve"] if component == "dell" else ["pull"] if component == "cra" else ["pull", "serve"])
    for kind in kinds:
        name = f"{component}-recovery-{kind}-{release_id}"
        module = "recovery_observer" if kind == "observer" else "recovery_transport"
        local = kind == "observer"
        lines = [
            "[Unit]",
            f"Description=Recovery soak {component} {kind} {release_id}",
            f"ConditionPathExists={source}/release_manifest.json",
            f"ConditionPathExists={runtime}/runtime_manifest.json",
            "",
            "[Service]",
            f"Type={'simple' if kind == 'serve' else 'oneshot'}",
            f"User={user}",
            f"Group={group}",
            "Environment=PYTHONDONTWRITEBYTECODE=1",
            "Environment=PYTHONNOUSERSITE=1",
            f"Environment=PYTHONPATH={source}/src",
            f"Environment=CRA_IMMUTABLE_RELEASE_ID={release_id}",
            f"ExecStart={runtime}/.venv/bin/python -m cra_no_action_soak.{module} --config {config}/recovery-{kind}.json",
            "TimeoutStartSec=12s",
            "TimeoutStopSec=5s",
            "UMask=0077",
            "Nice=10",
            "IOSchedulingClass=idle",
            "MemoryMax=128M",
            "CPUQuota=20%",
            "NoNewPrivileges=true",
            "PrivateTmp=true",
            "PrivateDevices=true",
            "ProtectSystem=strict",
            f"ProtectHome={'read-only' if component == 'arena' and local else 'true'}",
            "ProtectProc=invisible",
            "ProtectClock=true",
            "ProtectHostname=true",
            "ProtectKernelTunables=true",
            "ProtectKernelModules=true",
            "ProtectKernelLogs=true",
            "ProtectControlGroups=true",
            "RestrictSUIDSGID=true",
            "RestrictRealtime=true",
            "LockPersonality=true",
            "MemoryDenyWriteExecute=true",
            "CapabilityBoundingSet=",
            "RestrictAddressFamilies=" + ("AF_UNIX" if local else "AF_UNIX AF_INET AF_INET6"),
            f"ReadOnlyPaths={source} {runtime} {config} /etc/stream-recovery-control/host-contract.json",
            "InaccessiblePaths=-/var/lib/rancher -/run/k3s -/run/stream-v3-control -/var/lib/postgresql",
            "InaccessiblePaths=-/etc/stream-monitoring-v4/cra-projection-postgres.conninfo",
        ]
        if local:
            lines.append("PrivateNetwork=true")
        if component == "cra":
            lines.append(f"Environment=LD_LIBRARY_PATH={runtime}/sqlite/lib")
        if kind == "observer":
            lines.append(f"ReadWritePaths={state} {inbox}")
        elif kind == "pull":
            lines.append(f"ReadWritePaths={inbox}")
        else:
            lines.extend([f"ReadOnlyPaths={state} {inbox}", "Restart=on-failure", "RestartSec=5s"])
        if component != "cra" or kind != "observer":
            lines.append("InaccessiblePaths=-/var/lib/stream-recovery-control/cra")
        if component == "dell":
            lines.append(
                "InaccessiblePaths=-/var/lib/stream-recovery-control/dell/agent.sqlite3 "
                "-/var/lib/stream-recovery-control/dell/agent.sqlite3-wal -/var/lib/stream-recovery-control/dell/agent.sqlite3-shm"
            )
            if local:
                # Mount the directory, not the atomically replaced file inode.
                lines.append("BindReadOnlyPaths=/var/lib/stream-v3/wan-observer:/run/recovery-inputs/network")
            else:
                lines.append("InaccessiblePaths=/etc/stream-recovery-observation/credentials/dell-observation-ed25519-private.pem")
        if component == "arena" and not local:
            lines.append("InaccessiblePaths=/etc/stream-monitoring-cra-projection/credentials/monitoring-ed25519-private.pem")
        if component == "cra" and not local:
            lines.append("InaccessiblePaths=/etc/stream-recovery-control/cra-archive-signing-private.pem")
        if kind == "serve":
            lines.extend(["", "[Install]", "WantedBy=multi-user.target"])
        result[name + ".service"] = "\n".join(lines) + "\n"
        if kind != "serve":
            result[name + ".timer"] = "\n".join(
                [
                    "[Unit]",
                    f"Description=Autonomous recovery {kind} timer {release_id}",
                    "",
                    "[Timer]",
                    "OnBootSec=10s",
                    "OnUnitActiveSec=10s",
                    "AccuracySec=500ms",
                    "RandomizedDelaySec=0",
                    "Persistent=false",
                    f"Unit={name}.service",
                    "",
                    "[Install]",
                    "WantedBy=timers.target",
                    "",
                ]
            )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--component", choices=PROFILES, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    files = render(args.component, args.release_id)
    args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
    for name, contents in files.items():
        with (args.output / name).open("x") as target:
            target.write(contents)
    print(json.dumps({"component": args.component, "release_id": args.release_id, "units": sorted(files), "installed": False}))


if __name__ == "__main__":
    main()
