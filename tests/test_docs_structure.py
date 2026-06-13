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
            "### Observability Path",
            "### Recovery Path",
            "### Public Status Publication",
            "Airspy",
            "airspy_adsb",
            "Dell workstation readsb",
            "modified tar1090 map",
            "beast feed to :30104",
            "browser map URL",
            "fast-recovery-loop",
            "scoped local FFmpeg recovery",
            "deploy/k3s/streaming",
            "stream_v3.control_loop --mode streaming",
            "HP ProDesk k3s observability role",
            "`stream-v3-control` for the monitor loop",
            "`stream-v3-observer` for the Prometheus exporter",
            "The Raspberry Pi is not the k3s node",
            "HP ProDesk / k3s observability",
            "stream-v3-observer<br/>exporter :9108",
            "API + public watch",
            "v3 monitor",
            "recovery-orchestrator",
            "guard",
            "guarded k3s recovery",
            "`kubectl exec`",
            "Raspberry Pi public publisher role",
            "Pi-local nginx `/grafana/` proxy",
            "public snapshot collector",
            "GCS bucket",
            "outbound upload",
            "GCS + Cloudflare",
            "pull: HTTP GET",
            "127.0.0.1:8088/grafana/...",
            "proxy_pass",
            "192.168.0.60:3000/grafana",
            "JSON response",
            "ProDesk does not push monitoring data to Raspberry Pi",
            "home uplink bandwidth",
            "Non-static operational access",
            "not named as a public endpoint here",
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
            "docs/executive-summary.md",
            "docs/operational-scorecard.md",
            "docs/implementation-review-map.md",
            "docs/test-strategy-and-safety-boundary.md",
            "docs/incident-review-template.md",
            "docs/v3/migration-cutover-case-study.md",
            "docs/v3/youtube-lifecycle-safety.md",
            "docs/v3/encoder-upload-case-study.md",
            "docs/v3/tcp-stall-case-study.md",
            "docs/v3/visual-audio-health-model.md",
            "docs/v3/memory-guard-case-study.md",
            "docs/v3/failure-taxonomy.md",
            "docs/v3/single-node-dr-case-study.md",
            "docs/v3/runbook-validation.md",
            "ops/scripts/wan_address_observer.py",
            "ops/scripts/persistent_tcp_anchor_observer.py",
            "ops/systemd/stream-v3-wan-address-observer.timer",
            "ops/systemd/stream-v3-wan-address-observer-burst.timer",
            "## External Validation",
            "r/ADSB",
            "Reddit Post Insights",
            "24/7 ADS-B livestream from Japan with custom evaluation pipeline (ARENA)",
            "1.2K views",
            "docs/assets/reddit-adsb-post-insights-2026-05.png",
            "stats reuse bug",
            "open-source code published as a case study",
            "not a supported OSS product",
            "## Reviewer Summary",
            "monthly-window same-URL review",
            "## Evidence Snapshot",
            "10.7 seconds from fault injection to stream_v3 observability metrics OK",
            "`bytes_sent` advanced by 37,503,068 bytes",
            "Same-URL continuity review",
            "selected replacement actions `0`",
            "three-home-host personal 24/7",
            "## What This Repository Demonstrates",
            "### Production-style Operation",
            "### Runtime And Recovery Architecture",
            "### Observability And Public Evidence",
            "### Reliability Evidence",
            "The important design point is separation of authority",
            "YouTube lifecycle mutation is not triggered from ambiguous transport",
            "## Reviewer Reading Path",
            "Non-technical interviewer",
            "Backend / infrastructure reviewer",
            "SRE / platform reviewer",
            "## What This Does Not Claim",
            "docs/executive-summary.md",
            "docs/operational-scorecard.md",
            "docs/hiring-reviewer-guide.md",
            "docs/implementation-review-map.md",
            "docs/design-decisions-for-review.md",
        ):
            self.assertIn(marker, text)

        self.assertEqual(text.count("```mermaid"), 4)
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

    def test_readme_mermaid_topology_matches_current_host_split(self) -> None:
        text = read(README)
        blocks = re.findall(r"```mermaid\n(.*?)\n```", text, flags=re.S)

        self.assertEqual(len(blocks), 4)
        delivery, observability, recovery, public_status = blocks

        for marker in (
            'subgraph PD["HP ProDesk (.60)"]',
            'subgraph DELL["Dell / k3s node (.35)"]',
            'subgraph K3S["k3s ns stream-v3 / stream-v3-runtime (3/3)"]',
            'FR["fast-recovery-loop<br/>fast_recovery.py"]',
            "FR -. scoped local FFmpeg recovery .-> ENG",
        ):
            self.assertIn(marker, delivery)

        for marker in (
            'subgraph PD["HP ProDesk / k3s observability (.60)"]',
            'MON["v3 monitor<br/>stream-v3-control"]',
            'EXP["stream-v3-observer<br/>exporter :9108"]',
            "RUN -. runtime evidence .-> MON",
            "MON --> EXP --> PROM --> GRAF",
        ):
            self.assertIn(marker, observability)

        for marker in (
            'subgraph PD["HP ProDesk / k3s observability (.60)"]',
            'MON["v3 monitor<br/>stream-v3-control"]',
            'subgraph DELL["Dell / k3s (.35)"]',
            'GUARD -->|"guarded k3s recovery"| RUN',
            "RUN -. runtime evidence .-> MON",
        ):
            self.assertIn(marker, recovery)

        for marker in (
            'subgraph PD["HP ProDesk / k3s observability (.60)"]',
            'OBS["stream-v3-observer<br/>exporter :9108"]',
            'subgraph RPI["Raspberry Pi (.50)"]',
            'subgraph EDGE["Public static edge"]',
            "OBS --> PROM --> GRAF",
            'PUB -->|"outbound upload"| GCS --> CF',
        ):
            self.assertIn(marker, public_status)

        for block in blocks:
            self.assertNotIn("Raspberry Pi / k3s", block)
            self.assertNotIn("Raspberry Pi k3s", block)

    def test_public_docs_are_english_entrypoints(self) -> None:
        required = (
            "00_INDEX.md",
            "README.md",
            "executive-summary.md",
            "hiring-reviewer-guide.md",
            "operational-scorecard.md",
            "implementation-review-map.md",
            "design-decisions-for-review.md",
            "compliance-and-licensing-boundary.md",
            "evolution.md",
            "architecture.md",
            "physical-topology.md",
            "runtime-contract.md",
            "sli-methodology.md",
            "28-day-same-url-sli-case-study.md",
            "observability.md",
            "operations.md",
            "test-strategy-and-safety-boundary.md",
            "incident-review-template.md",
            "security-and-secrets.md",
            "support.md",
            "contributing.md",
            "public-release.md",
            "archive-note.md",
            "v2/README.md",
            "v3/README.md",
            "v3/current-runtime-contract.md",
            "v3/public-status-snapshot.md",
            "v3/runtime-state-and-evidence.md",
            "v3/sli-and-dashboard.md",
            "v3/rolling-sli-error-budget-feedback.md",
            "v3/observability-plane-self-check.md",
            "v3/fast-recovery-classifier-replay.md",
            "v3/migration-cutover-case-study.md",
            "v3/youtube-lifecycle-safety.md",
            "v3/encoder-upload-case-study.md",
            "v3/encoder-fps-tuning-2026-05-31.md",
            "v3/tcp-stall-case-study.md",
            "v3/tcp-stall-resolution-depth.md",
            "v3/visual-audio-health-model.md",
            "v3/memory-guard-case-study.md",
            "v3/failure-taxonomy.md",
            "v3/notification-and-auto-recovery.md",
            "v3/single-node-dr-case-study.md",
            "v3/runbook-validation.md",
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

    def test_docs_index_points_to_public_catalog(self) -> None:
        text = read(DOCS / "00_INDEX.md")

        for marker in (
            "Documentation Catalog",
            "not a required reading order",
            "Use the rest of this page as a reference catalog",
            "Core Entry Points",
            "Architecture And Runtime",
            "Reliability Evidence",
            "Recovery And Safety",
            "Design Review",
            "Governance And Release Boundary",
            "top-level `README.md`",
            "executive-summary.md",
            "hiring-reviewer-guide.md",
            "operational-scorecard.md",
            "implementation-review-map.md",
            "design-decisions-for-review.md",
            "compliance-and-licensing-boundary.md",
            "evolution.md",
            "architecture.md",
            "physical-topology.md",
            "runtime-contract.md",
            "sli-methodology.md",
            "28-day-same-url-sli-case-study.md",
            "observability.md",
            "operations.md",
            "test-strategy-and-safety-boundary.md",
            "incident-review-template.md",
            "security-and-secrets.md",
            "support.md",
            "contributing.md",
            "v2/README.md",
            "v3/README.md",
            "v3/observability-plane-self-check.md",
            "v3/public-status-snapshot.md",
            "v3/rolling-sli-error-budget-feedback.md",
            "v3/fast-recovery-classifier-replay.md",
            "v3/migration-cutover-case-study.md",
            "v3/youtube-lifecycle-safety.md",
            "v3/tcp-stall-case-study.md",
            "v3/tcp-stall-resolution-depth.md",
            "v3/encoder-upload-case-study.md",
            "v3/visual-audio-health-model.md",
            "v3/memory-guard-case-study.md",
            "v3/failure-taxonomy.md",
            "v3/notification-and-auto-recovery.md",
            "v3/single-node-dr-case-study.md",
            "v3/runbook-validation.md",
        ):
            self.assertIn(marker, text)

        indexed = set(re.findall(r"`([^`]+\.md)`", text))
        expected = {
            str(path.relative_to(DOCS)).replace("\\", "/")
            for path in DOCS.rglob("*.md")
            if path.name != "00_INDEX.md"
        }
        self.assertFalse(expected - indexed)

    def test_docs_readme_keeps_review_paths_lightweight(self) -> None:
        text = read(DOCS / "README.md")

        for marker in (
            "Most reviewers should then read only",
            "hiring-reviewer-guide.md",
            "executive-summary.md",
            "operational-scorecard.md",
            "Review Paths",
            "Compact Technical Pass",
            "Architecture Pass",
            "Reliability / SRE Pass",
            "Deep Design Review",
            "Use `00_INDEX.md` as the full documentation catalog",
        ):
            self.assertIn(marker, text)

    def test_hiring_reviewer_guide_has_reader_specific_paths(self) -> None:
        text = read(DOCS / "hiring-reviewer-guide.md")

        for marker in (
            "Hiring Reviewer Guide",
            "30-Second Summary",
            "same-URL continuity",
            "public-safe observability publication through GCS + Cloudflare",
            "If You Are A Non-Technical Interviewer",
            "If You Are A Backend / Infrastructure Reviewer",
            "If You Are An SRE / Platform Reviewer",
            "monthly-window same-URL operation evidence",
            "k3s runtime boundary",
            "private Prometheus/Loki/Grafana staying outside the public path",
            "fault-layer classification",
            "same-watch-URL continuity treated as a production invariant",
            "This Is Not",
            "This Is",
            "Key Design Decisions",
            "Suggested Review Paths",
            "10-Minute Review",
            "30-Minute Technical Review",
            "Deep Review / Audit",
            "docs/00_INDEX.md",
            "Most hiring reviewers",
            "What To Evaluate",
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
            "HP ProDesk applies the observability/control workloads",
            "Raspberry Pi is not a k3s node",
            "`stream-v3-control` and `stream-v3-observer`",
            "legacy/reference host-side entrypoint",
        ):
            self.assertIn(marker, text)

        for stale in (
            "docs/v3/10_current/",
            "docs/v3/25_decisions/",
            "docs/v3/50_ops_logs/",
            "stream_v2` remains the production owner",
            "current v2 production host",
            "new server",
            "first concrete build target for v3",
            "sends RTMP,",
            "production RTMP URL",
            "Install\n`ops/systemd/stream-v3-observability-monitor.service` there for",
        ):
            self.assertNotIn(stale, text)

    def test_public_review_docs_capture_maturity_and_safety_boundaries(self) -> None:
        executive = read(DOCS / "executive-summary.md")
        scorecard = read(DOCS / "operational-scorecard.md")
        test_strategy = read(DOCS / "test-strategy-and-safety-boundary.md")
        incident_template = read(DOCS / "incident-review-template.md")
        migration = read(DOCS / "v3" / "migration-cutover-case-study.md")
        operations = read(DOCS / "operations.md")
        release = read(DOCS / "public-release.md")

        for marker in (
            "Executive Summary",
            "same-watch-URL preservation",
            "Highest-Signal Evidence",
            "What This Repository Does Not Claim",
            "docs/operational-scorecard.md",
        ):
            self.assertIn(marker, executive)

        for marker in (
            "Operational Scorecard",
            "Scope Calibration",
            "single-operator, three-home-host personal 24/7 stream",
            "Measured",
            "Tested",
            "Documented",
            "Not publicly measured",
            "24-hour production smoke test",
            "v2 already provided the long-running behavior baseline",
            "migration confidence, not a replacement",
        ):
            self.assertIn(marker, scorecard)

        for marker in (
            "Test Strategy And Safety Boundary",
            "Public CI must not",
            "publish to YouTube",
            "mutate a production k3s cluster",
            "24-Hour Smoke Test",
            "v2 already established the stable long-running behavior model",
            "not a replacement for 14-day or 28-day SLI review",
        ):
            self.assertIn(marker, test_strategy)

        for marker in (
            "Incident Review Template",
            "Actions explicitly not taken",
            "Why destructive YouTube lifecycle mutation was or was not allowed",
            "Sanitized Example",
            "RTMPS TCP stall with WAN identity refresh signature",
        ):
            self.assertIn(marker, incident_template)

        for marker in (
            "Migration And Cutover Case Study",
            "a green Pod is not production authority",
            "Authority Transfer Model",
            "24-Hour Smoke-Test Rationale",
            "v2 already had stable long-running behavior",
            "not a substitute for the 28-day same-URL review",
            "Automatic rollback is intentionally avoided",
        ):
            self.assertIn(marker, migration)

        for marker in (
            "test-strategy-and-safety-boundary.md",
            "24-hour smoke test",
            "v3/migration-cutover-case-study.md",
        ):
            self.assertIn(marker, operations)

        for marker in (
            "executive summary",
            "operational scorecard",
            "test safety",
            "migration cutover",
            "public CI non-mutating",
        ):
            self.assertIn(marker, release)

    def test_runtime_docs_preserve_core_architecture_claims(self) -> None:
        runtime = read(DOCS / "runtime-contract.md")
        architecture = read(DOCS / "architecture.md")
        topology = read(DOCS / "physical-topology.md")
        observability = read(DOCS / "observability.md")

        for marker in (
            "delivery-plane",
            "observability-plane",
            "stream-v3-runtime",
            "`stream-v3-control` k3s deployment for the monitor loop",
            "`stream-v3-observer` k3s deployment and service for the exporter",
            "ProDesk-side observability/control",
            "h264_nvenc",
            "VIDEO_BITRATE=3400k",
            "STREAM_V3_CUTOVER_ENABLE=1",
            "STREAM_K8S_DRY_RUN=1",
            "shadow_budget_not_enforced",
            "Production enforcement lives",
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
            "http://127.0.0.1:8088/grafana",
            "Pi-initiated pull path",
            "datasource JSON",
            "returns to the Pi collector",
            "Public Status Publication",
            "pushed outbound from Raspberry Pi to GCS",
            "Cloudflare serves the `yukimurata0421.dev` public domain",
            "home uplink bandwidth",
            "Non-static operational access",
        ):
            self.assertIn(marker, runtime)

        for marker in (
            "Plane Split",
            "shadow",
            "streaming",
            "v3-observer",
            "`deploy/k3s/v3-control`",
            "HP ProDesk `192.168.0.60` k3s observability role",
            "as the k3s `stream-v3-control` workload",
            "Recovery is staged",
            "Physical Topology",
            "Dell workstation",
            "HP ProDesk",
            "Airspy USB on HP ProDesk",
            "Dell-side readsb",
            "Source Boundary",
            "YouTube Data API / OAuth / public watch-page probes",
            "k3s runtime, state, and log evidence",
            "public status publication",
            "Raspberry Pi collector",
            "initiates HTTP GET",
            "Pi-local /grafana/ proxy",
            "datasource JSON response returns to Raspberry Pi collector",
            "outbound upload to GCS",
            "Cloudflare",
            "offloading public reads away from the",
            "Non-static operational access",
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
            "k3s observability/control host",
            "k3s `stream-v3-control`",
            "k3s `stream-v3-observer`",
            "Visualization Boundary",
            "Prometheus, Loki, Grafana, and Alloy",
            "YouTube API/public watch evidence",
            "Raspberry Pi",
            "Public snapshot publisher",
            "GCS + Cloudflare",
            "Public static edge",
            "outbound upload",
            "Failure-Domain Boundary",
        ):
            self.assertIn(marker, topology)

        for marker in (
            "stream_v3_upload_latest_mbps",
            "stream_v3_network_ffmpeg_socket_lastsnd_ms",
            "stream_v3_recovery_action_executable",
            "YouTube Data API, OAuth, and public watch-page state",
            "visual correctness checks",
            "capture-helper memory guardrail",
            "ops/scripts/wan_address_observer.py",
            "ops/scripts/persistent_tcp_anchor_observer.py",
            "TCP stall case study",
            "visual-audio-health-model.md",
            "memory-guard-case-study.md",
            "youtube-lifecycle-safety.md",
            "Raspberry Pi",
            "http://127.0.0.1:8088/grafana",
            "ProDesk does not push this evidence to the Pi",
            "datasource JSON",
            "outbound to GCS",
            "Cloudflare serves",
            "keep public status reads off the home uplink",
            "Non-static operational access",
            "API Cost Guard",
            "ProDesk-side k3s\nmonitor guard",
            "k3s `stream-v3-control` and `stream-v3-observer`",
        ):
            self.assertIn(marker, observability)

    def test_v3_docs_capture_current_operational_model(self) -> None:
        current = read(DOCS / "v3" / "current-runtime-contract.md")
        evidence = read(DOCS / "v3" / "runtime-state-and-evidence.md")
        sli = read(DOCS / "v3" / "sli-and-dashboard.md")
        observability_self_check = read(DOCS / "v3" / "observability-plane-self-check.md")
        rolling_sli = read(DOCS / "v3" / "rolling-sli-error-budget-feedback.md")
        decisions = read(DOCS / "v3" / "decisions.md")
        program_map = read(DOCS / "v3" / "program-map.md")
        tcp_stall = read(DOCS / "v3" / "tcp-stall-case-study.md")
        tcp_depth = read(DOCS / "v3" / "tcp-stall-resolution-depth.md")
        review_map = read(DOCS / "implementation-review-map.md")
        migration = read(DOCS / "v3" / "migration-cutover-case-study.md")
        youtube_lifecycle = read(DOCS / "v3" / "youtube-lifecycle-safety.md")
        encoder_upload = read(DOCS / "v3" / "encoder-upload-case-study.md")
        visual_audio = read(DOCS / "v3" / "visual-audio-health-model.md")
        memory_guard = read(DOCS / "v3" / "memory-guard-case-study.md")
        failure_taxonomy = read(DOCS / "v3" / "failure-taxonomy.md")
        notifications = read(DOCS / "v3" / "notification-and-auto-recovery.md")
        single_node_dr = read(DOCS / "v3" / "single-node-dr-case-study.md")
        runbook_validation = read(DOCS / "v3" / "runbook-validation.md")

        for marker in (
            "Delivery Owner",
            "Monitoring Owner",
            "observability/control path on HP ProDesk k3s",
            "`stream-v3-control` deployment",
            "`stream-v3-observer` deployment and service",
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
            "pulls datasource JSON",
            "scheduled GCS",
            "GCS + Cloudflare",
            "home uplink",
            "Public readers do not reach Grafana",
        ):
            self.assertIn(marker, current)

        for marker in (
            "/state/overlay/now_playing.json",
            "Fresh local delivery evidence wins",
            "shadow_mode",
            "tcp-stall-case-study.md",
        ):
            self.assertIn(marker, evidence)

        for marker in (
            "YouTube availability",
            "same URL preservation",
            "Error Budget Rule",
            "Measured Results To Read First",
            "37,503,068 bytes",
            "encoder-upload-case-study.md",
            "Visual correctness, audio correctness, ADS-B source freshness",
            "observability-plane-self-check.md",
            "rolling-sli-error-budget-feedback.md",
        ):
            self.assertIn(marker, sli)

        for marker in (
            "Rolling SLI And Error-Budget Feedback",
            "rolling 24h, 7d, and available 30d windows",
            "`7.0 / 100.8 min` burned",
            "metric-zero `19.0 min`; actual URL burn `0`; replacement count `0`",
            "raw over-budget `195 sec`",
            "same-URL metric-zero samples",
            "not by creating a replacement broadcast",
            "Rolling feedback is labeled separately from long-window public SLI claims",
        ):
            self.assertIn(marker, rolling_sli)

        for marker in (
            "Observability Plane Self-Check",
            "Prometheus target reachable",
            "exporter /metrics or /healthz slow or unavailable",
            "dashboard panels missing stream_v3_* series",
            "raw delivery evidence still healthy",
            "observability-plane incident, not a delivery-plane incident",
            "stream_v3_health_snapshot.py",
            "stream_v3_monitoring_watchdog.py",
            "stream_v3_exporter_snapshot_fallback",
            "stream_v3_monitoring_watchdog_ok",
            "Snapshot fallback is not a new uptime claim",
        ):
            self.assertIn(marker, observability_self_check)

        for marker in (
            "Delivery / Observability Split",
            "HP ProDesk observability side also runs k3s",
            "`stream-v3-control` and `stream-v3-observer`",
            "Shadow Gate Semantics",
            "not production policy",
            "Migration Smoke Test",
            "NVENC CBR Baseline",
            "Encoder Upload Budget",
            "YouTube Lifecycle Mutation Safety",
            "Visual / Audio / Memory Boundaries",
            "Host Freeze Recovery",
            "Single-Node DR Honesty",
        ):
            self.assertIn(marker, decisions)

        for marker in (
            "Delivery Plane",
            "Observability Plane",
            "ProDesk k3s `stream-v3-control`",
            "ProDesk k3s `stream-v3-observer`",
            "stream_v3_prometheus_exporter.py",
            "stream_v3_health_snapshot.py",
            "stream_v3_monitoring_watchdog.py",
            "tcp-stall-resolution-depth.md",
        ):
            self.assertIn(marker, program_map)

        for marker in (
            "TCP Stall Root-Cause Case Study",
            "Cause Split",
            "Cloudflare AS13335",
            "Google AS15169",
            "WAN or carrier session refresh",
            "Observer Granularity Fix",
            "Failure-triggered WAN snapshot",
            "short WAN/session/route outage",
            "same-URL continuity",
            "ops/scripts/wan_address_observer.py",
            "ops/scripts/persistent_tcp_anchor_observer.py",
            "report-only",
            "stream-v3-wan-address-observer.timer",
            "stream-v3-wan-address-observer-burst.timer",
            "stream-v3-persistent-anchor-observer.service",
            "tcp-stall-resolution-depth.md",
        ):
            self.assertIn(marker, tcp_stall)

        for marker in (
            "TCP Stall Resolution Depth",
            "Resolution Ladder",
            "Public-retained state",
            "Retained through `ops/scripts/rtmps_tcp_burst_observer.py`",
            "Retained through `ops/scripts/netlink_wan_event_observer.py`",
            "Retained through `ops/scripts/cpe_event_ingest.py`",
            "Retained through `ops/scripts/rtmps_tcpdump_ring.py`",
            "DNS success is supporting evidence",
            "Cloudflare AS13335 and Google AS15169",
            "bounded packet metadata",
            "CPE scheduled reconnect",
            "carrier-side mobile session refresh",
        ):
            self.assertIn(marker, tcp_depth)

        for marker in (
            "Implementation Review Map",
            "How is upload tuning decided?",
            "How are rolling SLI windows read without overclaiming?",
            "How does the system keep dashboard `No data` out of delivery recovery?",
            "How are stale caches prevented from authorizing bad decisions?",
            "How is v2 to v3 cutover authority scoped?",
            "What does a 24-hour smoke test prove?",
            "How are incidents reviewed without leaking private evidence?",
            "What Not To Infer",
        ):
            self.assertIn(marker, review_map)

        for marker in (
            "Migration And Cutover Case Study",
            "green Pod is not production authority",
            "Authority Transfer Model",
            "24-Hour Smoke-Test Rationale",
            "stable long-running behavior",
            "Rollback Rule",
        ):
            self.assertIn(marker, migration)

        for marker in (
            "YouTube Lifecycle Safety",
            "preserve same watch URL when recoverable",
            "Cache Freshness Bug",
            "per-probe checked timestamps",
            "quota exhaustion is treated as degraded evidence",
            "Destructive actions require explicit permission",
        ):
            self.assertIn(marker, youtube_lifecycle)

        for marker in (
            "Encoder And Upload Budget Case Study",
            "h264_nvenc",
            "about p50 4.87 Mbps",
            "higher measured RTMPS send envelope",
            "VBR/CQ reduced upload",
            "YouTube low-bitrate / not-enough-video warnings",
            "30fps/3300k",
        ):
            self.assertIn(marker, encoder_upload)

        for marker in (
            "Visual And Audio Health Model",
            "YouTube ingest being connected does not prove",
            "Audio is validated by route and energy",
            "RTMPS connected",
            "ADS-B source freshness",
        ):
            self.assertIn(marker, visual_audio)

        for marker in (
            "Memory Guard Case Study",
            "Xvfb shared memory",
            "capture_helper_memory_guard_triggered",
            "memory alone never authorizes YouTube broadcast replacement",
            "process-level capture-helper guard",
        ):
            self.assertIn(marker, memory_guard)

        for marker in (
            "Failure Taxonomy",
            "same_url_changed",
            "memory_guard_warn",
            "YouTube lifecycle mutation is the highest-risk class",
            "Required Evidence",
        ):
            self.assertIn(marker, failure_taxonomy)

        for marker in (
            "Notification And Auto-Recovery Events",
            "active incident notification",
            "auto-recovered delivery event",
            "Single FFmpeg child recovery",
            "notification failure is not proof of stream failure",
        ):
            self.assertIn(marker, notifications)

        for marker in (
            "Single-Node DR Case Study",
            "measured RTO upper bound: 10.7 seconds",
            "same PID and same TCP socket survived the drill",
            "`bytes_sent` advanced by 37,503,068 bytes",
            "not an RTMPS reconnect drill",
            "Node and disk lost",
            "same-URL safety constraints",
        ):
            self.assertIn(marker, single_node_dr)

        for marker in (
            "Runbook Validation",
            "PVC deletion, URL replacement, and destructive YouTube actions",
            "live stream keys",
            "real production mutation",
            "If the reviewer hesitates",
        ):
            self.assertIn(marker, runbook_validation)

        for relative in (
            "ops/scripts/wan_address_observer.py",
            "ops/scripts/persistent_tcp_anchor_observer.py",
            "ops/systemd/stream-v3-wan-address-observer.service",
            "ops/systemd/stream-v3-wan-address-observer.timer",
            "ops/systemd/stream-v3-wan-address-observer-burst.service",
            "ops/systemd/stream-v3-wan-address-observer-burst.timer",
            "ops/systemd/stream-v3-persistent-anchor-observer.service",
            "ops/scripts/rtmps_tcp_burst_observer.py",
            "ops/scripts/netlink_wan_event_observer.py",
            "ops/scripts/cpe_event_ingest.py",
            "ops/scripts/rtmps_tcpdump_ring.py",
            "ops/systemd/stream-v3-rtmps-tcp-burst.service",
            "ops/systemd/stream-v3-rtmps-tcp-burst.timer",
            "ops/systemd/stream-v3-netlink-wan-event-observer.service",
            "ops/systemd/stream-v3-cpe-event-ingest.service",
            "ops/systemd/stream-v3-tcpdump-ring.service",
            "ops/systemd/stream-v3-tcpdump-ring.timer",
            "ops/systemd/stream-v3-wan-observers.env.example",
            "tests/test_wan_observer_scripts.py",
        ):
            self.assertTrue((ROOT / relative).exists(), f"missing {relative}")

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
            "Raspberry Pi public gateway role",
            "Raspberry Pi exposes the public nginx status UI",
            "public nginx status/dashboard gateway",
            "public browsers reach Grafana",
            "public readers reach Grafana",
            "adsb-open.addevlab.com",
            "/stream-v3-grafana",
            "guarded k8s recovery",
            "k8s container restart count",
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
