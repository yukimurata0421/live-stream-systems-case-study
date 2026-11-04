# Documentation Guide

Start with a question, then follow its current contract to code and dated
evidence. The documents are in English. The [catalog](00_INDEX.md) lists every
Stream document by role; component guides own their detailed setup.

## First Pass: Two To Five Minutes

1. [README](../README.md): what runs, the dated results, and two diagrams.
2. [Operational case studies](operational-case-studies.md): pick one failure.
3. [Current status](current-status.md): latest retained checkpoints and open
   gaps, with their observation dates.

Interviewers can then use the [hiring reviewer guide](hiring-reviewer-guide.md)
for a path that fits their role.

## Technical Pass: One Question At A Time

| Question | Read next |
| --- | --- |
| Can an output recover without replacing the encoder? | [Runtime contract and layer choice](v3/ffmpeg-fifo-recovery.md), then the [measured FIFO case](v3/ffmpeg-fifo-measurements.md). |
| Which hosts exist, and who can act on what? | [Architecture](architecture.md). |
| How do normal delivery, fallback, and return work? | [Self-healing OCI fan-out](v3/self-healing-oci-fanout.md), then the [fallback decision contract](v3/fallback-decision-contract.md). |
| How was a failover rule corrected when each fix exposed a new gap? | [One fallback rule, revised under test](v3/fallback-policy-revisions.md). |
| How are timestamps compared across hosts with imperfect clocks? | [Cross-host time contract](v3/cross-host-observation-time.md). |
| What is the difference between a missing observation and a failed service? | [Observation contract](v3/observation-standardization.md), then [notification time](v3/notification-observation-time.md). |
| Does the running source match the reviewed release? | [Release provenance](v3/observation-release-provenance.md). |
| Why was the previous recovery design replaced? | [Decision record](v3/oci-route-simplification-decision.md). |
| Why were these hosts, CPU, and GPU selected? | [Hardware selection](hardware-selection.md). |
| What do the 160 days actually measure? | [Continuity evidence](same-url-continuity-evidence.md). |
| What was measured, tested, or left unknown? | [Scorecard](operational-scorecard.md). |
| Where are the implementation and tests? | [Implementation review map](implementation-review-map.md). |
| Is a moving clock proof of public playback? | [Observation layers](observability.md#video-observation-layers), then the [return-feed](v3/relay-clock-monitor.md) and [thumbnail](v3/twitch-thumbnail-monitor.md) contracts. |
| What happens when the monitor itself stops observing? | [Clock observation case study](v3/clock-observation-case-study.md). |
| How can relay files be recovered without starting services? | [OCI backup and restore](v3/oci-backup-and-restore.md). |
| How are network failure and missing evidence distinguished? | [TCP investigation](v3/tcp-stall-case-study.md). |
| How does public status stay separate from control? | [Publisher boundary](v3/public-publisher-boundary.md), then [publisher setup](../ops/public-publisher/site/README.md). |

## Credential-Free Local Checks

From the repository root, in an isolated development environment:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pip install -e ./recovery-control
.venv/bin/python ops/scripts/validate_public_port.py
.venv/bin/python -m pytest -q tests/test_docs_structure.py tests/test_public_publisher_build.py tests/test_selfheal.py
```

These use temporary local state, synthetic facts, and mocked provider calls.
They do not upload a site, run a live recovery, or touch a provider. Component
suites are described in their own READMEs and the
[test strategy](test-strategy-and-safety-boundary.md).

## Everything Else

The [catalog](00_INDEX.md) lists every document by topic, including dated case
studies, superseded designs, and the CRA design study. Private operational
logs, credentials, raw evidence, and runtime databases are excluded.

## Reading A Claim

A current contract describes the intended rule. A source test checks an
implementation. A dated observation establishes a result only for its revision,
window and measured layer. The [scorecard](operational-scorecard.md) connects
those claims; [current status](current-status.md) identifies the latest retained
checkpoint, without turning it into a live health guarantee.

Authors should use the [documentation responsibilities](documentation-model.md)
when changing a rule or adding evidence. That page assigns owners and explains
how summaries, contracts, procedures and history stay consistent.
