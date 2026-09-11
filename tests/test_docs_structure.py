from __future__ import annotations

import re
import unittest
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
RECOVERY_CONTROL = ROOT / "recovery-control"
MONITORING_V4 = ROOT / "monitoring-v4"
README = ROOT / "README.md"
MARKDOWN_LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
MERMAID_BLOCK_RE = re.compile(r"^```mermaid\s*$\n(.*?)^```\s*$", re.MULTILINE | re.DOTALL)
CORE_CLAIM_IDS = {
    "SV3-SAME-URL",
    "SV3-RECOVERY-GUARD",
    "SV3-PUBLIC-BOUNDARY",
    "SV3-EVIDENCE-STRENGTH",
}


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def markdown_files() -> list[Path]:
    return [README, *sorted(DOCS.rglob("*.md"))]


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


def local_link_target(source: Path, raw_target: str) -> Path | None:
    target = raw_target.strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1]
    target = target.split(maxsplit=1)[0]
    if not target or target.startswith(("#", "http://", "https://", "mailto:")):
        return None
    target = unquote(target.split("#", 1)[0].split("?", 1)[0])
    if not target:
        return None
    return (source.parent / target).resolve()


class PublicDocsStructureTests(unittest.TestCase):
    def test_required_public_documents_exist(self) -> None:
        required = (
            README,
            DOCS / "00_INDEX.md",
            DOCS / "executive-summary.md",
            DOCS / "hiring-reviewer-guide.md",
            DOCS / "operational-scorecard.md",
            DOCS / "implementation-review-map.md",
            DOCS / "architecture.md",
            DOCS / "physical-topology.md",
            DOCS / "runtime-contract.md",
            DOCS / "sli-methodology.md",
            DOCS / "28-day-same-url-sli-case-study.md",
            DOCS / "test-strategy-and-safety-boundary.md",
            DOCS / "public-release.md",
            DOCS / "v3" / "README.md",
            DOCS / "v3" / "current-runtime-contract.md",
            DOCS / "v3" / "public-status-snapshot.md",
            DOCS / "v3" / "rolling-sli-error-budget-feedback.md",
            DOCS / "v3" / "tcp-stall-case-study.md",
            DOCS / "v3" / "tcp-stall-resolution-depth.md",
            DOCS / "v3" / "notification-and-auto-recovery.md",
            DOCS / "v3" / "notification-diagnostic-boundary.md",
            DOCS / "v3" / "map-rendering-and-monitoring.md",
            DOCS / "v3" / "map-production-cutover-case-study.md",
            DOCS / "v3" / "map-mobile-legibility-review.md",
            DOCS / "v3" / "operational-reliability-and-external-evidence.md",
            DOCS / "v3" / "public-publisher-boundary.md",
            DOCS / "v3" / "three-host-source-consolidation.md",
            DOCS / "v3" / "scoped-recovery-authority.md",
            DOCS / "v3" / "failure-injection-and-chaos-draft.md",
            RECOVERY_CONTROL / "README.md",
            RECOVERY_CONTROL / "docs" / "why-cra.md",
            RECOVERY_CONTROL / "docs" / "architecture.md",
            RECOVERY_CONTROL / "docs" / "safety-model.md",
            RECOVERY_CONTROL / "docs" / "harness-trust.md",
            MONITORING_V4 / "README.md",
            MONITORING_V4 / "docs" / "architecture.md",
            MONITORING_V4 / "docs" / "hardening.md",
            MONITORING_V4 / "docs" / "harness-engineering-draft.md",
            MONITORING_V4 / "docs" / "status.md",
            MONITORING_V4 / "docs" / "public-release.md",
        )

        for path in required:
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertTrue(path.exists())
                self.assertGreater(len(read(path).strip()), 80)

    def test_internal_markdown_links_resolve(self) -> None:
        for source in markdown_files():
            for raw_target in MARKDOWN_LINK_RE.findall(read(source)):
                target = local_link_target(source, raw_target)
                if target is None:
                    continue
                with self.subTest(source=source.relative_to(ROOT), target=raw_target):
                    self.assertTrue(target.exists(), f"broken local link: {raw_target}")

    def test_docs_index_covers_every_document(self) -> None:
        index = read(DOCS / "00_INDEX.md")
        indexed = set(re.findall(r"`([^`]+\.md)`", index))
        expected = {
            str(path.relative_to(DOCS)).replace("\\", "/")
            for path in DOCS.rglob("*.md")
            if path.name != "00_INDEX.md"
        }
        self.assertFalse(expected - indexed, f"unindexed docs: {sorted(expected - indexed)}")

    def test_readme_is_a_bounded_review_entrypoint(self) -> None:
        text = read(README)
        headings = re.findall(r"^## (.+)$", text, flags=re.MULTILINE)

        self.assertLessEqual(len(text.splitlines()), 200)
        self.assertEqual(
            headings,
            [
                "30-Second Summary",
                "Evidence Snapshot",
                "System Architecture",
                "Key Design Decisions",
                "Claims And Limits",
                "Review Paths",
            ],
        )
        self.assertEqual(text.count("```mermaid"), 1)
        self.assertTrue((DOCS / "assets" / "live-stream-screenshot.png").exists())

        review_paths = text.split("## Review Paths", 1)[1]
        review_links = {
            raw_target
            for raw_target in MARKDOWN_LINK_RE.findall(review_paths)
            if local_link_target(README, raw_target) is not None
        }
        self.assertEqual(
            review_links,
            {
                "docs/hiring-reviewer-guide.md",
                "docs/operational-scorecard.md",
                "docs/implementation-review-map.md",
            },
        )

    def test_readme_same_url_checkpoint_has_required_evidence_fields(self) -> None:
        text = read(README)
        evidence_snapshot = text.split("## Evidence Snapshot", 1)[1].split(
            "## System Architecture", 1
        )[0]

        for label in (
            "Measurement start",
            "Measurement endpoint",
            "Expected video ID",
            "Selected video ID at endpoint",
            "Observed replacement actions",
            "Candidate-new-URL samples",
            "V2 stopped",
            "First retained V3 production-send evidence",
            "Cutover video ID check",
            "Daily ledger checkpoints",
            "Ledger URL transitions / candidate-new-URL / force-live",
        ):
            self.assertIn(label, evidence_snapshot)

        for checkpoint in (
            "2026-05-06 10:36:17 JST",
            "2026-09-11 00:04:12 JST",
            "127 days, 13 hours, 27 minutes, 55 seconds",
            "107 / 107",
            "2026-05-28 22:29:43 JST",
            "2026-05-28 22:41:31 JST",
            "OpMzOBFwM7M",
        ):
            self.assertIn(checkpoint, evidence_snapshot)

        self.assertIn("uninterrupted frame delivery", evidence_snapshot)
        self.assertIn("At least 127 days", evidence_snapshot)
        self.assertNotIn("At least 109 days", evidence_snapshot)

        case_study = read(DOCS / "28-day-same-url-sli-case-study.md")
        self.assertIn("At-Least-127-Day Continuity Checkpoint", case_study)
        self.assertIn("One baseline plus 106 daily checkpoints", case_study)
        self.assertIn("Database integrity was `ok`", case_study)
        self.assertIn("Current Specification Mapping", case_study)
        self.assertIn("Rolling 24h, 7d, and available-30d", case_study)
        self.assertIn("`OUTCOME_UNKNOWN`", case_study)
        self.assertRegex(case_study, r"public CRA policy\s+remains production-disabled")
        self.assertNotIn("The next SLO view should use rolling 30 days", case_study)

    def test_current_docs_separate_retained_topology_from_current_authority(self) -> None:
        runtime = read(DOCS / "runtime-contract.md")
        physical = read(DOCS / "physical-topology.md")
        current = read(DOCS / "v3" / "current-runtime-contract.md")
        taxonomy = read(DOCS / "v3" / "failure-taxonomy.md")
        notifications = read(DOCS / "v3" / "notification-and-auto-recovery.md")
        encoder = read(DOCS / "v3" / "encoder-upload-case-study.md")
        connectivity = read(DOCS / "v3" / "connectivity-aware-delivery-recovery.md")

        for text in (runtime, physical, current):
            self.assertIn("retained", text.lower())
            self.assertIn("cra-01", text)

        self.assertIn("Current Cross-Host Recovery Contract", runtime)
        self.assertIn("one exact, target-wide fenced FFmpeg-child effect", runtime)
        self.assertIn("production-disabled", runtime)
        self.assertIn("not proof", physical)
        self.assertIn("not proof of live deployment", current)
        self.assertIn("Four-container Pod not `4/4 Running`", taxonomy)
        self.assertNotIn("Pod not `3/3 Running`", taxonomy)
        self.assertIn("`network_down` suppresses repeated child launches", notifications)
        self.assertIn("about 36 hours", encoder)
        self.assertIn("source-time interval", encoder)
        self.assertIn("had not been deployed", encoder)
        self.assertIn("`AF_NETLINK`", connectivity)
        self.assertIn("`ERROR_OR_UNOBSERVED`", connectivity)
        self.assertIn("cannot authorize recovery", connectivity)

        self.assertIn("continuously unavailable for 180 seconds", notifications)
        self.assertIn("unnecessary second restart", notifications)

        operations = read(DOCS / "operations.md")
        self.assertIn("Release Provenance Gate", operations)
        self.assertIn("verify_report_release_contract.py", operations)
        self.assertIn("rollback target", operations)

    def test_core_claim_ids_are_stable(self) -> None:
        readme = read(README)
        scorecard = read(DOCS / "operational-scorecard.md")

        for claim_id in CORE_CLAIM_IDS:
            with self.subTest(claim_id=claim_id):
                self.assertIn(f"`{claim_id}`", readme)
                self.assertIn(f"`{claim_id}`", scorecard)

    def test_map_cutover_case_study_preserves_measured_boundaries(self) -> None:
        text = read(DOCS / "v3" / "map-production-cutover-case-study.md")
        scorecard = read(DOCS / "operational-scorecard.md")

        for evidence in (
            "17,280 / 17,280",
            "289 / 289",
            "18,000 / 18,000",
            "13 / 13",
            "h264_nvenc",
            "Remote Recovery Collision",
            "WebGL2 Blocklist",
            "Render-Ready Idle Starvation",
            "two consecutive failed checks",
            "local null muxer",
            "not a current health report",
        ):
            with self.subTest(evidence=evidence):
                self.assertIn(evidence, text)

        for private_artifact in ("/home/", ".state/", "Pod UID", "rollback image SHA"):
            with self.subTest(private_artifact=private_artifact):
                self.assertNotIn(private_artifact, text)

        self.assertIn("Track-free renderer isolated soak | Measured", scorecard)
        self.assertIn("Renderer NVENC soak | Measured", scorecard)
        self.assertIn("Renderer production cutover | Measured", scorecard)
        self.assertIn("24-hour full production smoke test | Documented gate", scorecard)

    def test_hiring_guide_owns_role_specific_routes(self) -> None:
        text = read(DOCS / "hiring-reviewer-guide.md")
        role_headings = set(re.findall(r"^### (.+)$", text, flags=re.MULTILINE))

        self.assertLessEqual(len(text.splitlines()), 120)
        self.assertEqual(
            role_headings,
            {
                "Non-Technical Interviewer",
                "Backend Or Infrastructure Reviewer",
                "SRE Or Platform Reviewer",
            },
        )
        self.assertNotIn("Suggested Review Paths", text)

        sre_path = text.split("### SRE Or Platform Reviewer", 1)[1].split(
            "## Evaluation Rubric", 1
        )[0]
        for target in (
            "../recovery-control/docs/why-cra.md",
            "../recovery-control/docs/architecture.md",
            "../recovery-control/docs/harness-trust.md",
            "../monitoring-v4/docs/architecture.md",
            "operational-scorecard.md",
        ):
            with self.subTest(target=target):
                self.assertIn(target, sre_path)

        scorecard = read(DOCS / "operational-scorecard.md")
        self.assertIn(
            "Central Restart Authority | Tested / documented / not deployed",
            scorecard,
        )
        self.assertIn("No production authority is enabled", scorecard)
        self.assertIn("no live effect or completed production-soak claim", scorecard)
        self.assertIn("Monitoring v4 arena evidence plane | Tested / documented", scorecard)

        music_contract = read(DOCS / "v3" / "music-provider-and-loudness-contract.md")
        self.assertIn("Public Evidence Boundary", music_contract)
        self.assertIn("does not independently prove permission", music_contract)
        self.assertIn("Permission-To-Operation Workflow", music_contract)
        self.assertIn("asked the rights holder", music_contract)
        self.assertIn("credit requirement became an activation gate", music_contract)
        self.assertIn("non-technical condition is traceable", music_contract)

        recovery_contract = read(DOCS / "v3" / "scoped-recovery-authority.md")
        self.assertIn("runtime_boundary_entrypoint.py", recovery_contract)
        self.assertNotIn("does not yet publish a self-contained", recovery_contract)

    def test_mermaid_blocks_have_supported_declarations_and_balanced_fences(self) -> None:
        supported = (
            "flowchart ",
            "graph ",
            "sequenceDiagram",
            "stateDiagram",
            "classDiagram",
            "erDiagram",
            "journey",
            "gantt",
            "pie",
        )
        total_blocks = 0
        for path in markdown_files():
            text = read(path)
            blocks = MERMAID_BLOCK_RE.findall(text)
            self.assertEqual(text.count("```mermaid"), len(blocks), f"unclosed Mermaid fence in {path}")
            for block in blocks:
                total_blocks += 1
                first_line = next((line.strip() for line in block.splitlines() if line.strip()), "")
                with self.subTest(path=path.relative_to(ROOT), declaration=first_line):
                    self.assertTrue(first_line.startswith(supported))
                    self.assertNotIn("\t", block)
        self.assertGreater(total_blocks, 0)

    def test_readme_mermaid_preserves_plane_ownership(self) -> None:
        block = MERMAID_BLOCK_RE.findall(read(README))[0]

        for owner in (
            "HP ProDesk / source + private observability",
            "arena-server / Monitoring evidence",
            "cra-01 / central recovery authority",
            "Dell / k3s delivery",
            "Raspberry Pi / public snapshot publisher",
            "Public static edge",
            "legacy recovery plan",
            "exact FFmpeg-child executor",
            "facts-only projection",
            "fenced command",
            "outbound upload",
        ):
            with self.subTest(owner=owner):
                self.assertIn(owner, block)

    def test_public_docs_preserve_private_public_boundary(self) -> None:
        forbidden = (
            "Raspberry Pi ADS-B source",
            "Raspberry Pi owns Prometheus",
            "Raspberry Pi owns Loki",
            "Prometheus and Loki run on Raspberry Pi",
            "public browsers reach Grafana",
            "public readers reach Grafana",
            "public nginx status/dashboard gateway",
            "guarded k8s recovery",
            "k8s container restart count",
            "adsb-open.addevlab.com",
            "/stream-v3-grafana",
        )
        for path in markdown_files():
            text = read(path)
            with self.subTest(path=path.relative_to(ROOT)):
                for phrase in forbidden:
                    self.assertNotIn(phrase, text)

    def test_public_docs_use_current_adsb_source_name(self) -> None:
        required_modified_tar1090_docs = {
            README,
            DOCS / "architecture.md",
            DOCS / "physical-topology.md",
            DOCS / "runtime-contract.md",
        }
        for path in markdown_files():
            text = read(path)
            with self.subTest(path=path.relative_to(ROOT)):
                if path in required_modified_tar1090_docs:
                    self.assertIn("modified tar1090", text)
                self.assertNotRegex(text, r"(?i)stream1090")

    def test_public_docs_are_english(self) -> None:
        for path in markdown_files():
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertNotRegex(read(path), r"[ぁ-んァ-ヶ一-龠]")

    def test_new_source_consolidation_docs_keep_public_and_live_boundaries(self) -> None:
        mobile = read(DOCS / "v3" / "map-mobile-legibility-review.md")
        external = read(DOCS / "v3" / "operational-reliability-and-external-evidence.md")
        publisher = read(DOCS / "v3" / "public-publisher-boundary.md")
        consolidation = read(DOCS / "v3" / "three-host-source-consolidation.md")

        self.assertRegex(mobile, r"has not been\s+deployed")
        self.assertIn("390x219", mobile)
        self.assertIn("844x475", mobile)
        self.assertIn("youtube_public_video", external)
        self.assertIn("two consecutive five-minute", external)
        self.assertIn("cannot create formal SLO burn", external)
        self.assertIn("last-good", publisher)
        self.assertIn("No live service", consolidation)

        combined = "\n".join(read(path) for path in markdown_files())
        self.assertNotIn("/home/yuki", combined)
        self.assertNotRegex(combined, r"\b192\.168\.\d{1,3}\.\d{1,3}\b")

    def test_project_metadata_matches_case_study_scope(self) -> None:
        self.assertEqual(
            parse_assignment_lines(read(ROOT / "pyproject.toml"), ("description",))["description"],
            "Reliability engineering case study for a self-built 24/7 YouTube Live pipeline",
        )

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


if __name__ == "__main__":
    unittest.main()
