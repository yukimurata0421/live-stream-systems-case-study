from __future__ import annotations

import gzip
import json
import tempfile
import unittest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_core.common.json_io import iter_jsonl, iter_jsonl_recent, latest_jsonl
from stream_v2.jsonio import latest_jsonl as latest_v2_jsonl
from stream_v2.jsonio import latest_jsonl_where


def write_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


class JsonIoTailTests(unittest.TestCase):
    def test_v2_latest_reads_current_file_from_tail(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "events.jsonl"
            rows = [{"seq": value, "kind": "sample"} for value in range(5000)]
            rows.append({"seq": 5000, "kind": "restart"})
            write_rows(path, rows)

            self.assertEqual(latest_v2_jsonl(path), {"seq": 5000, "kind": "restart"})
            self.assertEqual(
                latest_jsonl_where(path, lambda item: item.get("kind") == "restart"),
                {"seq": 5000, "kind": "restart"},
            )

    def test_v2_latest_falls_back_to_rotated_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "events.jsonl"
            write_rows(path.with_name(path.name + ".1"), [{"seq": 1}, {"seq": 2}])
            path.touch()

            self.assertEqual(latest_v2_jsonl(path), {"seq": 2})

    def test_common_latest_supports_predicate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "events.jsonl"
            write_rows(path, [{"target": "a", "seq": 1}, {"target": "b", "seq": 2}])

            self.assertEqual(
                latest_jsonl(path, predicate=lambda item: item.get("target") == "a"),
                {"target": "a", "seq": 1},
            )

    def test_rotations_use_numeric_generation_order_and_ignore_lock_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "events.jsonl"
            write_rows(path.with_name(path.name + ".2"), [{"seq": 1}])
            write_rows(path.with_name(path.name + ".1"), [{"seq": 2}])
            write_rows(path, [{"seq": 3}])
            write_rows(path.with_name(path.name + ".lock"), [{"seq": 999}])

            self.assertEqual([item["seq"] for item in iter_jsonl(path)], [1, 2, 3])
            self.assertEqual(latest_jsonl(path), {"seq": 3})

    def test_recent_reader_stops_at_cutoff_and_yields_newest_first(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "events.jsonl"
            write_rows(
                path,
                [
                    {"ts": 10, "seq": 1},
                    {"ts": 20, "seq": 2},
                    {"ts": 30, "seq": 3},
                    {"ts": 40, "seq": 4},
                ],
            )

            rows = list(iter_jsonl_recent(path, cutoff_ts=25, timestamp=lambda item: int(item["ts"])))
            self.assertEqual([item["seq"] for item in rows], [4, 3])

    def test_recent_reader_projects_rows_before_retaining_them(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "events.jsonl.1.gz"
            with gzip.open(path, "wt", encoding="utf-8") as fh:
                fh.write(json.dumps({"ts": 30, "seq": 3, "large": "x" * 1000}) + "\n")
                fh.write(json.dumps({"ts": 40, "seq": 4, "large": "y" * 1000}) + "\n")

            rows = list(
                iter_jsonl_recent(
                    Path(td) / "events.jsonl",
                    cutoff_ts=25,
                    timestamp=lambda item: int(item["ts"]),
                    project=lambda item: {"ts": item["ts"], "seq": item["seq"]},
                )
            )

            self.assertEqual(rows, [{"ts": 40, "seq": 4}, {"ts": 30, "seq": 3}])
            self.assertTrue(all("large" not in item for item in rows))

    def test_arena_logrotate_policy_is_bounded_and_compressed(self) -> None:
        config = ROOT / "ops" / "logrotate" / "stream-v3-observability-monitor"
        text = config.read_text(encoding="utf-8")
        self.assertIn("/var/lib/stream-v3/observability-monitor/logs/*.jsonl", text)
        self.assertIn("daily", text)
        self.assertIn("maxsize 256M", text)
        self.assertIn("rotate 14", text)
        self.assertIn("compress", text)
        self.assertIn("copytruncate", text)


if __name__ == "__main__":
    unittest.main()
