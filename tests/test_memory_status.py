from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from stream_core.cli_support.memory_status import MemoryStatusContext, memory_status, memory_status_payload


def write_cgroup(root: Path, group: str, *, current: int, peak: int, stat: dict[str, int], events: dict[str, int]) -> None:
    path = root / group.lstrip("/")
    path.mkdir(parents=True)
    (path / "memory.current").write_text(f"{current}\n", encoding="utf-8")
    (path / "memory.peak").write_text(f"{peak}\n", encoding="utf-8")
    (path / "memory.stat").write_text("".join(f"{key} {value}\n" for key, value in stat.items()), encoding="utf-8")
    (path / "memory.events").write_text("".join(f"{key} {value}\n" for key, value in events.items()), encoding="utf-8")


class MemoryStatusTests(unittest.TestCase):
    def test_memory_status_splits_file_cache_from_anonymous_memory(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meminfo = root / "meminfo"
            meminfo.write_text(
                "\n".join(
                    [
                        "MemTotal:       16777216 kB",
                        "MemFree:         9000000 kB",
                        "MemAvailable:  10485760 kB",
                        "Buffers:          100000 kB",
                        "Cached:          4000000 kB",
                        "SReclaimable:     300000 kB",
                        "Shmem:            100000 kB",
                        "SwapTotal:       4194304 kB",
                        "SwapFree:        4194304 kB",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            cgroup_root = root / "cgroup"
            write_cgroup(
                cgroup_root,
                "/system.slice/adsb-streamnew-auto-dj.service",
                current=2_000_000_000,
                peak=2_100_000_000,
                stat={
                    "anon": 30_000_000,
                    "file": 1_930_000_000,
                    "inactive_file": 1_900_000_000,
                    "active_file": 30_000_000,
                    "kernel": 10_000_000,
                    "slab": 5_000_000,
                },
                events={"low": 0, "high": 0, "max": 0, "oom": 0, "oom_kill": 0, "oom_group_kill": 0},
            )

            def run_systemctl(args: list[str], check: bool) -> subprocess.CompletedProcess[str]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="\n".join(
                        [
                            "LoadState=loaded",
                            "ActiveState=active",
                            "SubState=running",
                            "ControlGroup=/system.slice/adsb-streamnew-auto-dj.service",
                            "MemoryCurrent=2000000000",
                            "MemoryPeak=2100000000",
                            "TasksCurrent=14",
                            "NRestarts=0",
                            "ExecMainStatus=0",
                        ]
                    )
                    + "\n",
                    stderr="",
                )

            ctx = MemoryStatusContext(
                memory_status_file=root / "memory_status.json",
                memory_status_events_file=root / "logs" / "memory_status.jsonl",
                service_units=("adsb-streamnew-auto-dj.service",),
                run_systemctl_readonly=run_systemctl,
                proc_meminfo_path=meminfo,
                cgroup_root=cgroup_root,
            )

            payload = memory_status_payload(ctx, now_ts=1_770_000_000)
            service = payload["services"][0]

        self.assertEqual(payload["overall"]["severity"], "ok")
        self.assertEqual(service["memory_breakdown"]["dominant_category"], "file_cache_reclaimable")
        self.assertEqual(service["classification"]["severity"], "ok")
        self.assertIn("file cache dominant", service["classification"]["reasons"][0])
        self.assertEqual(payload["host"]["evaluation_basis"], "absolute_bytes_primary; host_percentages_reference_only")
        self.assertIsNotNone(payload["host"]["non_reclaimable_estimate_bytes"])
        self.assertEqual(payload["operational_adequacy"]["severity"], "ok")
        self.assertEqual(service["memory_breakdown"]["non_reclaimable_estimate_bytes"], 40_000_000)
        self.assertIsNotNone(service["host_total_reference_pct"])

    def test_operational_adequacy_warns_on_non_reclaimable_budget_not_file_cache(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meminfo = root / "meminfo"
            meminfo.write_text(
                "\n".join(
                    [
                        "MemTotal:       16777216 kB",
                        "MemFree:         2500000 kB",
                        "MemAvailable:   6291456 kB",
                        "Buffers:           50000 kB",
                        "Cached:          2000000 kB",
                        "SReclaimable:     100000 kB",
                        "Shmem:             50000 kB",
                        "SwapTotal:       4194304 kB",
                        "SwapFree:        4194304 kB",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            def run_systemctl(args: list[str], check: bool) -> subprocess.CompletedProcess[str]:
                return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr="missing")

            ctx = MemoryStatusContext(
                memory_status_file=root / "memory_status.json",
                memory_status_events_file=root / "logs" / "memory_status.jsonl",
                service_units=(),
                run_systemctl_readonly=run_systemctl,
                proc_meminfo_path=meminfo,
                cgroup_root=root / "cgroup",
            )

            payload = memory_status_payload(ctx, now_ts=1_770_000_000)

        self.assertEqual(payload["overall"]["severity"], "ok")
        self.assertEqual(payload["operational_adequacy"]["severity"], "warn")
        self.assertFalse(payload["operational_adequacy"]["contributes_to_current_incident"])
        self.assertGreater(payload["host"]["non_reclaimable_estimate_bytes"], 10 * 1024 * 1024 * 1024)
        self.assertTrue(
            any("10GiB operational budget" in reason for reason in payload["operational_adequacy"]["reasons"])
        )

    def test_memory_status_keeps_inactive_oneshot_peak_as_history(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meminfo = root / "meminfo"
            meminfo.write_text(
                "MemTotal: 16777216 kB\nMemAvailable: 10485760 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n",
                encoding="utf-8",
            )

            def run_systemctl(args: list[str], check: bool) -> subprocess.CompletedProcess[str]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="\n".join(
                        [
                            "LoadState=loaded",
                            "ActiveState=inactive",
                            "SubState=dead",
                            "ControlGroup=",
                            "MemoryCurrent=[not set]",
                            "MemoryPeak=1900000000",
                            "TasksCurrent=0",
                            "NRestarts=0",
                            "ExecMainStatus=0",
                        ]
                    )
                    + "\n",
                    stderr="",
                )

            ctx = MemoryStatusContext(
                memory_status_file=root / "memory_status.json",
                memory_status_events_file=root / "logs" / "memory_status.jsonl",
                service_units=("adsb-streamnew-subsystems-status.service",),
                run_systemctl_readonly=run_systemctl,
                proc_meminfo_path=meminfo,
                cgroup_root=root / "cgroup",
            )

            payload = memory_status_payload(ctx, now_ts=1_770_000_000)

        self.assertEqual(payload["overall"]["severity"], "ok")
        self.assertEqual(payload["overall"]["systemd_peak_history_severity"], "warn")
        self.assertTrue(payload["overall"]["historical_peak_warn"])
        self.assertEqual(payload["services"][0]["classification"]["severity"], "ok")
        self.assertEqual(payload["services"][0]["peak_classification"]["severity"], "warn")
        self.assertEqual(payload["services"][0]["peak_classification"]["scope"], "systemd_memory_peak_history")
        self.assertFalse(payload["services"][0]["peak_classification"]["contributes_to_current_severity"])
        self.assertEqual(payload["services"][0]["memory_breakdown"]["dominant_category"], "systemd_peak_only")

    def test_active_oneshot_around_two_gib_is_peak_guardrail_not_current_incident(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meminfo = root / "meminfo"
            meminfo.write_text(
                "MemTotal: 16777216 kB\nMemAvailable: 10485760 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n",
                encoding="utf-8",
            )
            cgroup_root = root / "cgroup"
            write_cgroup(
                cgroup_root,
                "/system.slice/adsb-streamnew-subsystems-status.service",
                current=2_050_000_000,
                peak=2_080_000_000,
                stat={
                    "anon": 2_040_000_000,
                    "file": 0,
                    "inactive_file": 0,
                    "active_file": 0,
                    "kernel": 10_000_000,
                    "slab": 5_000_000,
                },
                events={"low": 0, "high": 0, "max": 0, "oom": 0, "oom_kill": 0, "oom_group_kill": 0},
            )

            def run_systemctl(args: list[str], check: bool) -> subprocess.CompletedProcess[str]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="\n".join(
                        [
                            "LoadState=loaded",
                            "ActiveState=activating",
                            "SubState=start",
                            "ControlGroup=/system.slice/adsb-streamnew-subsystems-status.service",
                            "MemoryCurrent=2050000000",
                            "MemoryPeak=2080000000",
                            "TasksCurrent=1",
                            "NRestarts=0",
                            "ExecMainStatus=0",
                        ]
                    )
                    + "\n",
                    stderr="",
                )

            ctx = MemoryStatusContext(
                memory_status_file=root / "memory_status.json",
                memory_status_events_file=root / "logs" / "memory_status.jsonl",
                service_units=("adsb-streamnew-subsystems-status.service",),
                run_systemctl_readonly=run_systemctl,
                proc_meminfo_path=meminfo,
                cgroup_root=cgroup_root,
            )

            payload = memory_status_payload(ctx, now_ts=1_770_000_000)

        self.assertEqual(payload["overall"]["severity"], "ok")
        self.assertEqual(payload["overall"]["active_oneshot_peak_severity"], "warn")
        self.assertEqual(payload["overall"]["peak_guardrail_severity"], "warn")
        self.assertEqual(payload["overall"]["systemd_peak_history_severity"], "ok")
        self.assertFalse(payload["overall"]["current_incident"])
        self.assertEqual(payload["services"][0]["classification"]["severity"], "ok")
        self.assertEqual(payload["services"][0]["peak_classification"]["scope"], "active_oneshot_run")
        self.assertFalse(payload["services"][0]["peak_classification"]["contributes_to_current_severity"])
        self.assertTrue(
            any(
                reason.startswith("oneshot peak above 1GiB")
                for reason in payload["services"][0]["peak_classification"]["reasons"]
            )
        )

    def test_memory_status_no_record_prints_json_without_writing_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meminfo = root / "meminfo"
            meminfo.write_text(
                "MemTotal: 16777216 kB\nMemAvailable: 10485760 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n",
                encoding="utf-8",
            )

            def run_systemctl(args: list[str], check: bool) -> subprocess.CompletedProcess[str]:
                return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr="missing")

            ctx = MemoryStatusContext(
                memory_status_file=root / "memory_status.json",
                memory_status_events_file=root / "logs" / "memory_status.jsonl",
                service_units=("missing.service",),
                run_systemctl_readonly=run_systemctl,
                proc_meminfo_path=meminfo,
                cgroup_root=root / "cgroup",
            )

            with mock.patch("builtins.print") as printed:
                rc = memory_status(ctx, json_output=True, record=False)
            payload = json.loads(str(printed.call_args.args[0]))

            self.assertEqual(rc, 0)
            self.assertFalse(ctx.memory_status_file.exists())
            self.assertFalse(ctx.memory_status_events_file.exists())
            self.assertEqual(payload["source"], "stream-new memory-status")


if __name__ == "__main__":
    unittest.main()
