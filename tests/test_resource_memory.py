from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from stream_core.cli_support.resource_memory import ResourceMemoryContext, parse_psi_text, resource_memory, resource_memory_payload


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
                cmdline="ffmpeg -i /home/yuki/projects/stream_v2/ncs_music/time_tags/evening/example.mp3 -f pulse stream_sink",
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
        self.assertIn("recent_events", payload)
        self.assertIn("stream_session_id", payload)
        self.assertIn("rendering", payload["subsystems"])


if __name__ == "__main__":
    unittest.main()
