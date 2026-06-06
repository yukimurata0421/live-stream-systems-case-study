# Runbook Validation

Runbooks are only useful if someone other than the original author can follow
them under pressure. `stream_v3` treats runbook validation as an operational
quality gate.

## Goal

The goal is to verify that a reviewer can start from public or internal
runbooks, identify the correct failure branch, stop before destructive actions,
and explain the evidence needed for recovery.

## Validation Rules

- The reviewer starts from the runbook links, not from private memory.
- Secret values are never requested in plaintext.
- Production-impacting actions stop at dry-run or read-only unless explicitly
  approved.
- PVC deletion, URL replacement, and destructive YouTube actions are never
  normal first steps.
- If the reviewer hesitates for more than a few minutes, the runbook has a bug.

## Scenarios

| Scenario | Reviewer should be able to explain |
| --- | --- |
| current health check | Pod, FFmpeg, capture, audio, raw metrics, same URL |
| YouTube warning | encoder contract, upload metrics, YouTube input health |
| dashboard false fail | raw metric and stale dashboard split |
| TCP stall | fast recovery, WAN anchors, same URL, YouTube state |
| node alive restart | rollout restart path and post-check |
| disk survives | recovery paths and private-state boundary |
| node/disk lost | RTO/RPO loss and observability evidence |
| YouTube lifecycle risk | why replacement is blocked until identity gates pass |

## Public Boundary

The public repository can validate:

- documentation structure;
- manifest safety;
- shadow acceptance;
- recovery and lifecycle policy tests;
- observer and diagnostic code shape.

It cannot validate:

- live stream keys;
- OAuth credentials;
- private media files;
- local runtime state;
- real production mutation.

That boundary is intentional.

## Evidence To Record

A validation run should record:

- date and timezone;
- reviewer role;
- scenario;
- runbook name;
- commands attempted;
- point of hesitation;
- missing prerequisite;
- unsafe or ambiguous wording;
- pass/fail result;
- patches made;
- remaining follow-up.

For incident-specific records, use `../incident-review-template.md` so the
runbook validation result and the incident decision record stay separate.

## Review Signal

This repo is not trying to pretend that documentation alone creates
reliability. It treats documentation as an artifact that must be tested against
real failure branches and explicit safety boundaries.
