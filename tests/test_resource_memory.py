from __future__ import annotations

import json
import gzip
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from stream_core.cli_support.resource_memory import (
    ResourceMemoryContext,
    history_items,
    parse_psi_text,
    resource_memory,
    resource_memory_payload,
)


def write_proc(root: Path) -> None:
    (root / "pressure").mkdir(parents=True)
    (root / "sys" / "kernel" / "random").mkdir(parents=True)
    (root / "meminfo").write_text(
        "\n".join(
            [
                "MemTotal:       16777216 kB",
                "MemFree:         9000000 kB",
                "MemAvailable:  10485760 kB",
                "Buffers:          100000 kB",
                "Cached:          4000000 kB",
                "SwapTotal:       4194304 kB",
                "SwapFree:        4194304 kB",
                "Dirty:                12 kB",
                "Writeback:             0 kB",
                "Slab:             580000 kB",
                "SReclaimable:     410000 kB",
                "SUnreclaim:       170000 kB",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "pressure" / "memory").write_text(
        "some avg10=0.00 avg60=0.00 avg300=0.00 total=123456\n"
        "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
        encoding="utf-8",
    )
    (root / "vmstat").write_text("pgmajfault 100\npswpin 0\npswpout 0\noom_kill 0\n", encoding="utf-8")
    (root / "uptime").write_text("1000.00 900.00\n", encoding="utf-8")
    (root / "sys" / "kernel" / "random" / "boot_id").write_text("boot-test\n", encoding="utf-8")


def write_pid(root: Path, pid: str, *, comm: str, cmdline: str, rss_kb: int, pss_kb: int) -> None:
    pid_dir = root / pid
    (pid_dir / "fd").mkdir(parents=True)
    (pid_dir / "cmdline").write_bytes(cmdline.encode("utf-8") + b"\x00")
    (pid_dir / "comm").write_text(comm + "\n", encoding="utf-8")
    (pid_dir / "status").write_text(f"Name:\t{comm}\nVmRSS:\t{rss_kb} kB\nThreads:\t2\n", encoding="utf-8")
    (pid_dir / "smaps_rollup").write_text(f"Pss: {pss_kb} kB\nSwap: 0 kB\n", encoding="utf-8")
    stat_tail = ["S"] + ["0"] * 18 + ["100"] + ["0"] * 5
    (pid_dir / "stat").write_text(f"{pid} ({comm}) " + " ".join(stat_tail) + "\n", encoding="utf-8")
    (pid_dir / "fd" / "0").touch()


def write_cgroup(root: Path, group: str) -> None:
    path = root / group.lstrip("/")
    path.mkdir(parents=True)
    (path / "memory.current").write_text("734003200\n", encoding="utf-8")
    (path / "memory.peak").write_text("838860800\n", encoding="utf-8")
    (path / "memory.swap.current").write_text("0\n", encoding="utf-8")
    (path / "memory.stat").write_text(
        "anon 314572800\nfile 104857600\nkernel 52428800\n"
        "slab_reclaimable 10485760\nslab_unreclaimable 5242880\nsock 0\nshmem 0\npgmajfault 3\n",
        encoding="utf-8",
    )
    (path / "memory.events").write_text("low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n", encoding="utf-8")
    (path / "memory.pressure").write_text(
        "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
        "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
        encoding="utf-8",
    )


class ResourceMemoryTests(unittest.TestCase):
    def test_history_items_reads_full_seven_day_baseline_across_rotation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "resource_memory.jsonl"
            now_ts = 1_770_000_000

            def row(ts: int) -> str:
                utc = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                return json.dumps({"ts_utc": utc, "host_memory": {"mem_available_mb": 8192.0}}) + "\n"

            path.write_text(row(now_ts - 60), encoding="utf-8")
            with gzip.open(path.with_name(path.name + ".1.gz"), "wt", encoding="utf-8") as fh:
                fh.write(row(now_ts - 7 * 24 * 3600 - 120))

            rows = history_items(path, now_ts)

        self.assertGreaterEqual(now_ts - min(int(item["_ts"]) for item in rows), 7 * 24 * 3600)

    def test_static_swap_capacity_does_not_become_current_pressure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            proc = root / "proc"
            state = root / "state"
            logs = state / "logs"
            proc.mkdir()
            logs.mkdir(parents=True)
            write_proc(proc)
            meminfo = proc / "meminfo"
            meminfo.write_text(
                meminfo.read_text(encoding="utf-8").replace("SwapFree:        4194304 kB", "SwapFree:        1048576 kB"),
                encoding="utf-8",
            )
            ctx = ResourceMemoryContext(
                resource_memory_file=state / "resource_memory.json",
                resource_memory_events_file=logs / "resource_memory.jsonl",
                resource_memory_assessment_file=state / "resource_memory_assessment.json",
                memory_status_events_file=logs / "memory_status.jsonl",
                service_units=(),
                run_systemctl_readonly=lambda args, check: subprocess.CompletedProcess(
                    args=args, returncode=0, stdout="", stderr=""
                ),
                state_base_dir=state,
                log_base_dir=logs,
                proc_root=proc,
                cgroup_root=root / "cgroup",
            )

            payload = resource_memory_payload(ctx, now_ts=1_770_000_000)

        self.assertEqual(payload["assessment"]["status"], "observe")
        self.assertTrue(payload["assessment"]["swap_capacity"]["warn"])
        self.assertFalse(payload["assessment"]["current_pressure"]["swap_growth"])
        self.assertFalse(payload["assessment"]["restart_allowed_by_memory_alone"])

    def test_low_mem_available_warns_before_seven_day_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            proc = root / "proc"
            state = root / "state"
            logs = state / "logs"
            proc.mkdir()
            logs.mkdir(parents=True)
            write_proc(proc)
            meminfo = proc / "meminfo"
            meminfo.write_text(
                meminfo.read_text(encoding="utf-8").replace("MemAvailable:  10485760 kB", "MemAvailable:   3145728 kB"),
                encoding="utf-8",
            )
            ctx = ResourceMemoryContext(
                resource_memory_file=state / "resource_memory.json",
                resource_memory_events_file=logs / "resource_memory.jsonl",
                resource_memory_assessment_file=state / "resource_memory_assessment.json",
                memory_status_events_file=logs / "memory_status.jsonl",
                service_units=(),
                run_systemctl_readonly=lambda args, check: subprocess.CompletedProcess(
                    args=args, returncode=0, stdout="", stderr=""
                ),
                state_base_dir=state,
                log_base_dir=logs,
                proc_root=proc,
                cgroup_root=root / "cgroup",
            )

            payload = resource_memory_payload(ctx, now_ts=1_770_000_000)

        self.assertFalse(payload["assessment"]["baseline_ready"])
        self.assertEqual(payload["assessment"]["status"], "warn")
        self.assertTrue(payload["assessment"]["current_pressure"]["mem_available_warn"])

    def test_parse_psi_memory_pressure_shape(self) -> None:
        parsed = parse_psi_text("some avg10=1.50 avg60=0.20 avg300=0.10 total=123\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=4\n")
        self.assertEqual(parsed["some_avg10"], 1.5)
        self.assertEqual(parsed["some_total_us"], 123)
        self.assertEqual(parsed["full_total_us"], 4)

    def test_resource_memory_payload_records_diagnostic_layers_without_recovery_permission(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            proc = root / "proc"
            cgroup = root / "cgroup"
            state = root / "state"
            logs = state / "logs"
            proc.mkdir()
            logs.mkdir(parents=True)
            write_proc(proc)
            write_pid(proc, "123", comm="ffmpeg", cmdline="/usr/bin/ffmpeg -i test", rss_kb=100000, pss_kb=90000)
            write_pid(proc, "124", comm="python3", cmdline="python3 src/dj/auto_dj.py --player ffmpeg", rss_kb=50000, pss_kb=40000)
            write_pid(
                proc,
                "125",
                comm="ffmpeg",
                cmdline="ffmpeg -i /opt/stream_v2/ncs_music/time_tags/evening/example.mp3 -f pulse stream_sink",
                rss_kb=25000,
                pss_kb=20000,
            )
            write_cgroup(cgroup, "/system.slice/adsb-streamnew-youtube-stream.service")

            def systemctl_show(args: list[str], check: bool) -> subprocess.CompletedProcess[str]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="\n".join(
                        [
                            "LoadState=loaded",
                            "ActiveState=active",
                            "SubState=running",
                            "ControlGroup=/system.slice/adsb-streamnew-youtube-stream.service",
                            "MainPID=123",
                            "NRestarts=0",
                        ]
                    )
                    + "\n",
                    stderr="",
                )

            ctx = ResourceMemoryContext(
                resource_memory_file=state / "resource_memory.json",
                resource_memory_events_file=logs / "resource_memory.jsonl",
                resource_memory_assessment_file=state / "resource_memory_assessment.json",
                memory_status_events_file=logs / "memory_status.jsonl",
                service_units=("adsb-streamnew-youtube-stream.service",),
                run_systemctl_readonly=systemctl_show,
                state_base_dir=state,
                log_base_dir=logs,
                proc_root=proc,
                cgroup_root=cgroup,
            )

            payload = resource_memory_payload(ctx, now_ts=1_770_000_000)
            with mock.patch("time.time", return_value=1_770_000_000), mock.patch("builtins.print"):
                rc = resource_memory(ctx, json_output=True, record=False)

        self.assertEqual(rc, 0)
        self.assertEqual(payload["schema_version"], "resource_memory.v1")
        self.assertFalse(payload["assessment"]["memory_is_sli"])
        self.assertFalse(payload["assessment"]["restart_allowed_by_memory_alone"])
        self.assertIn("ffmpeg", payload["process_groups"])
        self.assertEqual(payload["process_groups"]["ffmpeg"]["process_count"], 1)
        self.assertEqual(payload["process_groups"]["auto_dj"]["process_count"], 1)
        self.assertEqual(payload["process_groups"]["audio_player"]["process_count"], 1)
        self.assertEqual(payload["cgroups"]["adsb-streamnew-youtube-stream.service"]["memory_swap_current_mb"], 0.0)
        self.assertIn("current_runtime_state", payload)
        self.assertTrue(payload["current_runtime_state"]["ffmpeg_alive"])
        self.assertTrue(payload["current_runtime_state"]["local_ffmpeg_alive"])
        self.assertEqual(payload["current_runtime_state"]["ffmpeg_alive_source"], "local_process")
        self.assertIn("recent_events", payload)
        self.assertIn("stream_session_id", payload)
        self.assertIn("rendering", payload["subsystems"])

    def test_resource_memory_treats_remote_k8s_ffmpeg_evidence_as_alive(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            proc = root / "proc"
            cgroup = root / "cgroup"
            state = root / "state"
            logs = state / "logs"
            proc.mkdir()
            cgroup.mkdir()
            logs.mkdir(parents=True)
            write_proc(proc)
            (state / "youtube_watchdog_stats.json").write_text(
                json.dumps(
                    {
                        "local_ok": True,
                        "oauth_ok": True,
                        "api_ok": True,
                        "public_ok": True,
                        "expected_video_id": "video-1",
                        "video_id": "video-1",
                        "ingest_connected": True,
                        "ffmpeg_pid": 1955202,
                        "ffmpeg_uptime_sec": 55446,
                        "ffmpeg_generation": "ffmpeg_pid=1955202",
                        "stream_active": True,
                        "api_live_state": "live",
                    }
                ),
                encoding="utf-8",
            )

            ctx = ResourceMemoryContext(
                resource_memory_file=state / "resource_memory.json",
                resource_memory_events_file=logs / "resource_memory.jsonl",
                resource_memory_assessment_file=state / "resource_memory_assessment.json",
                memory_status_events_file=logs / "memory_status.jsonl",
                service_units=(),
                run_systemctl_readonly=lambda args, check: subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr=""),
                state_base_dir=state,
                log_base_dir=logs,
                proc_root=proc,
                cgroup_root=cgroup,
            )

            payload = resource_memory_payload(ctx, now_ts=1_770_000_000)

        runtime = payload["current_runtime_state"]
        self.assertTrue(runtime["ffmpeg_alive"])
        self.assertFalse(runtime["local_ffmpeg_alive"])
        self.assertEqual(runtime["ffmpeg_alive_source"], "remote_runtime_evidence")
        self.assertIn("youtube_watchdog.ingest_connected", runtime["ffmpeg_alive_evidence"])


if __name__ == "__main__":
    unittest.main()
