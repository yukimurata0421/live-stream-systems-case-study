# Hiring Reviewer Guide

This project is a small, personally operated streaming system. Review it for
engineering judgment: investigating failures, limiting recovery, preserving
useful evidence, and correcting mistakes. It does not demonstrate enterprise
traffic volume or a commercial service's availability guarantee.

## Choose One Path

### Non-Technical Interviewer

Spend two minutes on the summary and results in the [README](../README.md), then choose one story from
[operational case studies](operational-case-studies.md).
The architecture and code paths can wait for a technical follow-up.

The key questions are straightforward:

- What was the service meant to keep working?
- What went wrong, and how was the cause identified?
- What changed, and what evidence showed it helped?
- What did the operator avoid claiming without enough evidence?

The 160-day headline measures the same YouTube URL at reviewed checkpoints.
It does not say that every frame was delivered without interruption.

### Backend Or Infrastructure Reviewer

Read the [five-minute executive summary](executive-summary.md), then the
[watchdog or stale-cache incident](operational-case-studies.md). Use the
[implementation review map](implementation-review-map.md) to inspect one
relevant code path and its tests.

Look for clear component ownership, durable state, handling of partial failure,
and protection against stale data. You do not need to read every collector,
recovery rule, or deployment file before discussing the design.

### SRE Or Platform Reviewer

Start with two [incident stories](operational-case-studies.md) and the
[operational scorecard](operational-scorecard.md). Then follow the question that
matters for the interview:

| Question | Focused reading |
| --- | --- |
| How was network failure distinguished from provider failure? | [TCP stall case study](v3/tcp-stall-case-study.md). |
| Why split observation from recovery authorization? | [CRA rationale](../recovery-control/docs/why-cra.md) and [CRA architecture](../recovery-control/docs/architecture.md). |
| What prevents uncertain actions from being retried? | [Harness and effect evidence](../recovery-control/docs/harness-trust.md). |
| Who owns incident and measurement state? | [Monitoring architecture](../monitoring-v4/docs/architecture.md). |
| Why are the uptime and URL-continuity claims different? | [Dated continuity evidence](28-day-same-url-sli-case-study.md). |

Choose one failure to explore in depth. The record includes operational failures,
a failed test harness, and corrected observations; these have different meanings.

## Evaluation Rubric

| Area | Evidence to assess |
| --- | --- |
| Diagnosis | Independent observations are used before assigning a cause. |
| Recovery | The action targets the failing component and has a stop condition. |
| Measurement | Dates, denominators, and missing observations are explicit. |
| Testing | A test-tool failure or missing result is not turned into a pass. |
| Communication | A short result has a traceable technical explanation behind it. |

For source validation, run the local commands in the
[documentation guide](README.md). The [full catalog](00_INDEX.md) is a reference,
not an interview reading assignment.
