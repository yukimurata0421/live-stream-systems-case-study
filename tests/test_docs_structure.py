from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
README = ROOT / "README.md"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class PublicDocsStructureTests(unittest.TestCase):
    def test_readme_frontloads_public_review_evidence(self) -> None:
        text = read(README)

        for marker in (
            "docs/assets/live-stream-screenshot.png",
            "https://www.youtube.com/@yukimurata0421/live",
            "What breaks",
            "How stream_v3 protects it",
            "```mermaid",
            "Raspberry Pi",
            "Dell workstation",
            "HP ProDesk",
            "## Why k3s",
            "## Reviewer Shortcuts",
            "src/stream_v2/recovery_orchestrator/gate.py",
            "tests/test_v3_shadow_acceptance.py",
            "tests/test_youtube_video_id_resolver_cache_freshness.py",
            "tests/test_youtube_watchdog_cache_freshness.py",
            "## External Validation",
            "r/ADSB",
            "stats reuse bug",
            "not a supported OSS project",
            "## What This Repository Demonstrates",
            "docs/hiring-reviewer-guide.md",
            "docs/design-decisions-for-review.md",
        ):
            self.assertIn(marker, text)

        self.assertTrue((ROOT / "docs" / "assets" / "live-stream-screenshot.png").exists())

    def test_readme_has_support_and_contribution_sections(self) -> None:
        text = read(README)

        for marker in (
            "## Support",
            "## Contributions",
            "not a supported package",
            "Issues, if enabled",
            "Do not post",
            "validate_k3s_manifests.py",
            "v3_shadow_acceptance.py",
            "public-snapshot-check.yml",
            "public evidence check",
            "delivery-plane / observability-plane",
        ):
            self.assertIn(marker, text)

    def test_public_docs_are_english_entrypoints(self) -> None:
        required = (
            "00_INDEX.md",
            "README.md",
            "hiring-reviewer-guide.md",
            "design-decisions-for-review.md",
            "evolution.md",
            "architecture.md",
            "physical-topology.md",
            "runtime-contract.md",
            "observability.md",
            "operations.md",
            "security-and-secrets.md",
            "support.md",
            "contributing.md",
            "public-release.md",
            "archive-note.md",
            "v2/README.md",
            "v3/README.md",
            "v3/current-runtime-contract.md",
            "v3/runtime-state-and-evidence.md",
            "v3/sli-and-dashboard.md",
            "v3/runbooks.md",
            "v3/decisions.md",
            "v3/program-map.md",
            "v3/open-followups.md",
        )

        for relative in required:
            with self.subTest(path=relative):
                path = DOCS / relative
                self.assertTrue(path.exists(), f"missing {relative}")
                self.assertGreater(len(read(path).strip()), 80)

    def test_docs_index_points_to_public_reading_order(self) -> None:
        text = read(DOCS / "00_INDEX.md")

        for marker in (
            "Documentation Index",
            "Reading Order",
            "hiring-reviewer-guide.md",
            "design-decisions-for-review.md",
            "evolution.md",
            "architecture.md",
            "physical-topology.md",
            "runtime-contract.md",
            "observability.md",
            "operations.md",
            "security-and-secrets.md",
            "support.md",
            "contributing.md",
            "v2/README.md",
            "v3/README.md",
        ):
            self.assertIn(marker, text)

    def test_k3s_readme_points_to_public_docs(self) -> None:
        text = read(ROOT / "deploy" / "k3s" / "README.md")

        for marker in (
            "docs/v3/current-runtime-contract.md",
            "docs/v3/decisions.md",
            "docs/v3/runbooks.md",
            "HP ProDesk observability host",
            "Dell delivery node",
        ):
            self.assertIn(marker, text)

        for stale in (
            "docs/v3/10_current/",
            "docs/v3/25_decisions/",
            "docs/v3/50_ops_logs/",
        ):
            self.assertNotIn(stale, text)

    def test_runtime_docs_preserve_core_architecture_claims(self) -> None:
        runtime = read(DOCS / "runtime-contract.md")
        architecture = read(DOCS / "architecture.md")
        topology = read(DOCS / "physical-topology.md")
        observability = read(DOCS / "observability.md")

        for marker in (
            "delivery-plane",
            "observability-plane",
            "stream-v3-runtime",
            "h264_nvenc",
            "VIDEO_BITRATE=3300k",
            "STREAM_V3_CUTOVER_ENABLE=1",
            "STREAM_K8S_DRY_RUN=1",
            "Dell workstation",
            "HP ProDesk",
            "Raspberry Pi",
        ):
            self.assertIn(marker, runtime)

        for marker in (
            "Plane Split",
            "shadow",
            "streaming",
            "v3-observer",
            "Recovery is staged",
            "Physical Topology",
            "Dell workstation",
            "HP ProDesk",
            "Raspberry Pi",
        ):
            self.assertIn(marker, architecture)

        for marker in (
            "three-tier physical system",
            "Dell workstation",
            "HP ProDesk",
            "Raspberry Pi",
            "k3s is used for the delivery workload",
            "Failure-Domain Boundary",
        ):
            self.assertIn(marker, topology)

        for marker in (
            "stream_v3_upload_latest_mbps",
            "stream_v3_network_ffmpeg_socket_lastsnd_ms",
            "stream_v3_recovery_action_executable",
            "API Cost Guard",
        ):
            self.assertIn(marker, observability)

    def test_v3_docs_capture_current_operational_model(self) -> None:
        current = read(DOCS / "v3" / "current-runtime-contract.md")
        evidence = read(DOCS / "v3" / "runtime-state-and-evidence.md")
        sli = read(DOCS / "v3" / "sli-and-dashboard.md")
        decisions = read(DOCS / "v3" / "decisions.md")
        program_map = read(DOCS / "v3" / "program-map.md")

        for marker in (
            "Delivery Owner",
            "Monitoring Owner",
            "h264_nvenc",
            "--disable-shm=yes",
            "--enable-memfd=no",
            "Dell workstation",
            "HP ProDesk",
            "Raspberry Pi",
        ):
            self.assertIn(marker, current)

        for marker in (
            "/state/overlay/now_playing.json",
            "Fresh local delivery evidence wins",
            "shadow_mode",
        ):
            self.assertIn(marker, evidence)

        for marker in (
            "YouTube availability",
            "same URL preservation",
            "Error Budget Rule",
        ):
            self.assertIn(marker, sli)

        for marker in (
            "Delivery / Observability Split",
            "NVENC CBR Baseline",
            "Host Freeze Recovery",
        ):
            self.assertIn(marker, decisions)

        for marker in (
            "Delivery Plane",
            "Observability Plane",
            "stream_v3_prometheus_exporter.py",
        ):
            self.assertIn(marker, program_map)

    def test_docs_and_readme_do_not_contain_japanese_text(self) -> None:
        targets = [README, *sorted(DOCS.rglob("*.md"))]
        for path in targets:
            text = read(path)
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertNotRegex(text, r"[ぁ-んァ-ヶ一-龠]")


if __name__ == "__main__":
    unittest.main()
