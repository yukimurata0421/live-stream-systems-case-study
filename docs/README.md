# Documentation Guide

This is the one page that suggests a reading order. The documents are in
English and arranged so a reviewer can see results before implementation.

## First Pass: Two To Five Minutes

1. [README](../README.md): what runs, the dated results, and two diagrams.
2. [Operational case studies](operational-case-studies.md): pick one failure.
3. [Current status](current-status.md): what changed recently and what has not
   been observed yet.

Interviewers can then use the [hiring reviewer guide](hiring-reviewer-guide.md)
for a path that fits their role.

## Technical Pass: One Question At A Time

| Question | Read next |
| --- | --- |
| Which hosts exist, and who can act on what? | [Architecture](architecture.md). |
| How do normal delivery, fallback, and return work? | [Self-healing OCI fan-out](v3/self-healing-oci-fanout.md). |
| Why was the previous recovery design replaced? | [Decision record](v3/oci-route-simplification-decision.md). |
| Why were these hosts, CPU, and GPU selected? | [Hardware selection](hardware-selection.md). |
| What do the 160 days actually measure? | [Continuity evidence](same-url-continuity-evidence.md). |
| What was measured, tested, or left unknown? | [Scorecard](operational-scorecard.md). |
| Where are the implementation and tests? | [Implementation review map](implementation-review-map.md). |
| Is a moving clock proof of public playback? | [Return-feed clock monitor](v3/relay-clock-monitor.md). |
| What can Twitch's provider image add? | [Thumbnail clock observation](v3/twitch-thumbnail-monitor.md), separate from player E2E. |
| How can relay files be recovered without starting services? | [OCI backup and restore](v3/oci-backup-and-restore.md). |
| How are network failure and missing evidence distinguished? | [TCP investigation](v3/tcp-stall-case-study.md). |
| How does public status stay separate from control? | [Publisher boundary](v3/public-publisher-boundary.md). |

## Credential-Free Local Checks

From the repository root, in an isolated development environment:

```bash
python3 -m pip install -e '.[dev]'
python3 -m pip install -e ./recovery-control
python3 ops/scripts/validate_public_port.py
python3 -m pytest -q tests/test_docs_structure.py tests/test_public_publisher_build.py tests/test_selfheal.py
python3 ops/scripts/route_network_observer.py --replay examples/route-observation/replay.json
```

These use temporary local state, synthetic facts, and mocked provider calls.
They do not upload a site, run a live recovery, or touch a provider. Component
suites are described in their own READMEs and the
[test strategy](test-strategy-and-safety-boundary.md).

## Everything Else

The [catalog](00_INDEX.md) lists every document by topic, including dated case
studies, superseded designs, and the CRA design study. Private operational
logs, credentials, raw evidence, and runtime databases are excluded.
