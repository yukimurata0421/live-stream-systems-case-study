from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
README = ROOT / "README.md"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def parse_assignment_lines(text: str, keys: tuple[str, ...]) -> dict[str, str]:
    pattern = re.compile(rf"^\s*({'|'.join(map(re.escape, keys))})\s*[:=]\s*(.+?)\s*$")
    values: dict[str, str] = {}
    for line in text.splitlines():
        match = pattern.match(line)
        if not match:
            continue
        raw = match.group(2).strip().strip('"').strip("'")
        values[match.group(1)] = raw
    return values


class PublicDocsStructureTests(unittest.TestCase):
    def test_readme_frontloads_public_review_evidence(self) -> None:
        text = read(README)

        for marker in (
            "docs/assets/live-stream-screenshot.png",
            "https://www.youtube.com/@yukimurata0421/live",
            "What breaks",
            "How stream_v3 protects it",
            "```mermaid",
            "### Delivery Path",
            "### Observability & Recovery",
            "Airspy",
            "airspy_adsb",
            "Dell workstation readsb",
            "modified tar1090 map",
            "beast feed to :30104",
            "browser map URL",
            "Data API + OAuth + public watch",
            "arena-monitor",
            "recovery-orchestrator",
            "guarded k8s recovery",
            "`kubectl exec`",
            "Raspberry Pi public gateway role",
            "nginx :8088",
            "`/grafana/` proxy",
            "Prometheus and Loki remain on HP ProDesk",
            "Prometheus :9090",
            "Loki :3100",
            "Grafana :3000",
            "`ops/monitoring` evidence path",
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
            "Reddit Post Insights",
            "24/7 ADS-B livestream from Japan with custom evaluation pipeline (ARENA)",
            "1.2K views",
            "docs/assets/reddit-adsb-post-insights-2026-05.png",
            "stats reuse bug",
            "open-source code published as a case study",
            "not a supported OSS product",
            "## What This Repository Demonstrates",
            "docs/hiring-reviewer-guide.md",
            "docs/design-decisions-for-review.md",
        ):
            self.assertIn(marker, text)

        self.assertEqual(text.count("```mermaid"), 2)
        self.assertTrue((ROOT / "docs" / "assets" / "live-stream-screenshot.png").exists())
        self.assertTrue((ROOT / "docs" / "assets" / "reddit-adsb-post-insights-2026-05.png").exists())

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
            "sli-methodology.md",
            "28-day-same-url-sli-case-study.md",
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
            "sli-methodology.md",
            "28-day-same-url-sli-case-study.md",
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
            "VIDEO_BITRATE=3400k",
            "STREAM_V3_CUTOVER_ENABLE=1",
            "STREAM_K8S_DRY_RUN=1",
            "Dell workstation",
            "HP ProDesk",
            "Airspy USB on HP ProDesk",
            "airspy_adsb",
            "readsb on Dell workstation",
            "modified tar1090",
            "browser map upstream environment contract",
            "read-only YouTube Data API, OAuth, and public watch-page probes",
            "k3s runtime, state-file, and log evidence collection",
            "Raspberry Pi",
            "public nginx status/dashboard gateway",
            "Prometheus and Loki remain on HP ProDesk",
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
            "Airspy USB on HP ProDesk",
            "Dell-side readsb",
            "Source Boundary",
            "YouTube Data API / OAuth / public watch-page probes",
            "k3s runtime, state, and log evidence",
            "Raspberry Pi",
            "/grafana/ proxy to HP ProDesk Grafana",
            "Prometheus and Loki are not migrated",
        ):
            self.assertIn(marker, architecture)

        for marker in (
            "Airspy USB on HP ProDesk",
            "airspy_adsb",
            "readsb on HP ProDesk",
            "readsb on Dell workstation",
            "Dell workstation",
            "HP ProDesk",
            "k3s is used for the `stream_v3` delivery workload",
            "Visualization Boundary",
            "Prometheus, Loki, Grafana, and Alloy",
            "YouTube API/public watch evidence",
            "Raspberry Pi",
            "Prometheus `:9090` and Loki `:3100` are not migrated",
            "Failure-Domain Boundary",
        ):
            self.assertIn(marker, topology)

        for marker in (
            "stream_v3_upload_latest_mbps",
            "stream_v3_network_ffmpeg_socket_lastsnd_ms",
            "stream_v3_recovery_action_executable",
            "YouTube Data API, OAuth, and public watch-page state",
            "Raspberry Pi exposes the public nginx status UI",
            "does not host Prometheus or Loki",
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
            "Airspy USB",
            "airspy_adsb",
            "Dell readsb",
            "modified tar1090",
            "Raspberry Pi",
            "Prometheus and Loki are not hosted on the Raspberry Pi",
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

    def test_sli_methodology_captures_measured_baseline(self) -> None:
        text = read(DOCS / "sli-methodology.md")

        for marker in (
            "v2 baseline, 2026-05, 14-day observation snapshot",
            "They are not presented as current `stream_v3` uptime",
            "Do not collapse every signal into one availability percentage",
            "Production Invariant",
            "Primary SLI",
            "Guardrail",
            "Secondary SLI",
            "Event / Incident Metric",
            "`3608 / 3656` samples, `98.687%`",
            "`850586 / 852128` seconds, `99.819%`",
            "`3561 / 3563` definitive samples, `99.944%`",
            "replacement broadcasts observed locally: `0`",
            "FFmpeg child self-recovery, exit `224`",
            "Fast-recovery clusters, `tcp_stall` primary trigger",
            "Limitations And Unknowns",
            "Viewer-visible interruption seconds were unknown",
            "Direct ADS-B age was unknown",
            "should not copy the v2 numbers as current production status",
            "28-Day Follow-Up",
            "28-day-same-url-sli-case-study.md",
        ):
            self.assertIn(marker, text)

    def test_same_url_case_study_captures_followup_and_risks(self) -> None:
        text = read(DOCS / "28-day-same-url-sli-case-study.md")

        for marker in (
            "28-Day Same-URL SLI Case Study",
            "What Was Ported",
            "Did the public YouTube Live identity survive 28 days without creating a",
            "Replacement broadcasts",
            "observed selected replacement actions: 0",
            "observed allowed replacement decisions: 0",
            "observed candidate-new-URL evidence: 0",
            "`3561 / 3563`, `99.944%`",
            "`27486 / 27626`, `99.493%`",
            "`6558 / 6568`, `99.848%`",
            "What Got Worse Versus The 14-Day Baseline",
            "Upload Headroom",
            "Notification Delivery",
            "What Improved Or Stayed Stable",
            "Remaining Gaps",
            "Viewer-visible interruption seconds were still not measured directly",
            "The live URL identity survived the review window",
        ):
            self.assertIn(marker, text)

    def test_documented_encoder_contract_matches_public_config_examples(self) -> None:
        keys = ("FRAME_RATE", "VIDEO_BITRATE", "VIDEO_MAXRATE", "VIDEO_BUFSIZE", "AUDIO_BITRATE")
        documented = parse_assignment_lines(read(DOCS / "runtime-contract.md"), keys)
        self.assertEqual(
            documented,
            {
                "FRAME_RATE": "5",
                "VIDEO_BITRATE": "3400k",
                "VIDEO_MAXRATE": "3400k",
                "VIDEO_BUFSIZE": "6800k",
                "AUDIO_BITRATE": "192k",
            },
        )

        sources = (
            ROOT / "configs" / "production.env.example",
            ROOT / "configs" / "v3.shadow.env.example",
            ROOT / "ops" / "systemd" / "adsb-streamnew.env.example",
            ROOT / "deploy" / "k3s" / "base" / "configmap-shadow.yaml",
        )
        for path in sources:
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertEqual(parse_assignment_lines(read(path), keys), documented)

    def test_docs_and_readme_do_not_contain_japanese_text(self) -> None:
        targets = [README, *sorted(DOCS.rglob("*.md"))]
        for path in targets:
            text = read(path)
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertNotRegex(text, r"[ぁ-んァ-ヶ一-龠]")

    def test_public_docs_do_not_claim_raspberry_pi_source_or_monitoring_backend(self) -> None:
        targets = [README, *sorted(DOCS.rglob("*.md"))]
        forbidden = (
            "Raspberry Pi source",
            "Raspberry Pi ADS-B source",
            "Raspberry Pi owns Prometheus",
            "Raspberry Pi owns Loki",
            "Prometheus and Loki run on Raspberry Pi",
        )
        for path in targets:
            text = read(path)
            with self.subTest(path=path.relative_to(ROOT)):
                for phrase in forbidden:
                    self.assertNotIn(phrase, text)

    def test_public_docs_do_not_use_legacy_map_project_name(self) -> None:
        targets = [README, *sorted(DOCS.rglob("*.md"))]
        required_modified_tar1090_docs = {
            README,
            DOCS / "architecture.md",
            DOCS / "physical-topology.md",
            DOCS / "runtime-contract.md",
        }
        forbidden = (
            "tar1090/stream1090",
            "tar1090 / stream1090",
            "stream1090 endpoint",
            "stream1090 path",
        )

        for path in targets:
            text = read(path)
            with self.subTest(path=path.relative_to(ROOT)):
                if path in required_modified_tar1090_docs:
                    self.assertIn("modified tar1090", text)
                self.assertNotRegex(text, r"(?i)stream1090")
                for phrase in forbidden:
                    self.assertNotIn(phrase, text)


if __name__ == "__main__":
    unittest.main()
