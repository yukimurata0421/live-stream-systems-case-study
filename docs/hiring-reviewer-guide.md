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

The dated continuity headline measures the same YouTube URL at reviewed checkpoints.
It does not say that every frame was delivered without interruption.

### Backend Or Infrastructure Reviewer

Read [architecture](architecture.md) for the hosts and who can act on what,
then the [watchdog or stale-cache incident](operational-case-studies.md). Use the
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
| How were compute, cooling, cost, and migration risk weighed? | [Hardware selection](hardware-selection.md). |
| How was network failure distinguished from provider failure? | [TCP stall case study](v3/tcp-stall-case-study.md). |
| How does the media route recover, and why without central authorization? | [Self-healing OCI fan-out](v3/self-healing-oci-fanout.md) and [decision record](v3/oci-route-simplification-decision.md). |
| How should a failover act when its own observations are uncertain? | [One fallback rule, revised under test](v3/fallback-policy-revisions.md), then the [decision contract](v3/fallback-decision-contract.md). |
| How are observation meaning, source identity and viewer evidence checked? | [Observation contract](v3/observation-standardization.md), [release provenance](v3/observation-release-provenance.md), then [current evidence](current-status.md). |
| Why split observation from recovery authorization (design study, not enabled)? | [CRA rationale](../recovery-control/docs/why-cra.md) and [CRA architecture](../recovery-control/docs/architecture.md). |
| What prevents uncertain actions from being retried? | [Local reflex invariants](../recovery-control/docs/local-reflex-authority.md#invariants), then the [implementation and test path below](#no-retry-review-path). |
| Who owns incident and measurement state? | [Monitoring architecture](../monitoring-v4/docs/architecture.md). |
| Why are the uptime and URL-continuity claims different? | [Dated continuity evidence](same-url-continuity-evidence.md). |

Choose one failure to explore in depth. The record includes operational failures,
a failed test harness, and corrected observations; these have different meanings.

## No-Retry Review Path

Follow the rule through its implementation before assessing the test evidence:

1. **Contract:** [Local Reflex Authority](../recovery-control/docs/local-reflex-authority.md#conditions-for-a-restart)
   defines durable claims, generation limits, and read-only queries for unknown outcomes.
2. **Implementation:** the [agent store](../recovery-control/src/cra/store.py)
   reserves an attempt before sending; [request construction](../recovery-control/src/cra/effect.py)
   derives its identity from the target generation. The
   [runtime boundary](../src/runtime_boundary/server.py) and
   [EffectLedger](../src/runtime_boundary/ledger.py) enforce admission and persistent fences.
3. **Tests and limits:** [executor compatibility tests](../recovery-control/tests/reflex/test_reflex_executor_compat.py)
   exercise duplicate requests and lost replies;
   [fence reconciliation tests](../recovery-control/tests/reflex/test_reflex_executor_fence_reconciliation.py)
   check the transition to a distinct successor. [Harness trust](../recovery-control/docs/harness-trust.md)
   explains the isolated effect boundary and what these tests cannot establish.

The shipped public configuration remains `dry_run` and production-disabled.
The [dated operated checkpoint](../recovery-control/docs/local-reflex-authority.md)
is separate from these source and test paths, which do not establish new
production `enforce` authority or viewer recovery.

## Evaluation Rubric

| Area | Evidence to assess |
| --- | --- |
| Diagnosis | Independent observations are used before assigning a cause. |
| Recovery | The action targets the failing component and has a stop condition. |
| Measurement | Dates, denominators, and missing observations are explicit. |
| Testing | A test-tool failure or missing result is not turned into a pass. |
| Communication | A short result has a traceable technical explanation behind it. |

For source validation, run the local commands in the
[documentation guide](README.md). [Current status](current-status.md) separates
what was verified recently from what has not been observed yet.
