# Documentation Guide

The public documentation is in English. It is arranged for a hiring reviewer to
understand the result before inspecting the full implementation.

## First Pass: Two To Five Minutes

1. [README](../README.md): what runs, the dated result, and two small diagrams.
2. [Operational case studies](operational-case-studies.md): choose one failure.
3. [Hiring reviewer guide](hiring-reviewer-guide.md): choose a role-specific next step.

## Technical Pass: One Question At A Time

| Question | Read next |
| --- | --- |
| What was built and what is the engineering challenge? | [Executive summary](executive-summary.md). |
| Why were these hosts, CPU, and GPU selected? | [Hardware selection](hardware-selection.md). |
| What do the 160 days actually measure? | [Continuity evidence](28-day-same-url-sli-case-study.md). |
| What was measured, tested, or left unknown? | [Scorecard](operational-scorecard.md). |
| Where are the implementation and tests? | [Implementation review map](implementation-review-map.md). |
| How does the OCI route work? | [OCI delivery](v3/oci-relay-and-twitch.md). |
| How are network failure and missing evidence distinguished? | [Route observation](v3/route-network-observation.md) and [TCP investigation](v3/tcp-stall-case-study.md). |
| How does public status stay separate from control? | [Publisher boundary](v3/public-publisher-boundary.md). |

## Credential-Free Local Checks

From the repository root, in an isolated development environment:

```bash
python3 -m pip install -e '.[dev]'
python3 -m pip install -e ./recovery-control
python3 ops/scripts/validate_public_port.py
python3 -m pytest -q tests/test_docs_structure.py tests/test_public_publisher_build.py
python3 ops/scripts/route_network_observer.py --replay examples/route-observation/replay.json
```

The replay uses synthetic facts and performs no network or signal effects.
Focused checks use temporary local state and mocked provider calls. They do not
upload a site, run a live recovery, or establish native qualification. Component
suites and further dependencies are described in their own READMEs and the
[test strategy](test-strategy-and-safety-boundary.md).

## Reference Catalog

[00_INDEX.md](00_INDEX.md) lists the remaining architecture, safety, and historical
evidence documents. It is a reference, not a required reading order. Private
operational logs, credentials, raw evidence, and runtime databases are excluded.
