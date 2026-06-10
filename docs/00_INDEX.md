# Documentation Catalog

This page lists the English public documentation set for `stream_v3`.
It is not a required reading order. The private operational history was
intentionally reduced to a curated public set so readers can understand the
architecture without paging through raw incident logs.

Most reviewers should start with:

1. top-level `README.md`
2. `hiring-reviewer-guide.md`
3. `executive-summary.md`
4. `operational-scorecard.md`

Use the rest of this page as a reference catalog.

## Core Entry Points

- `hiring-reviewer-guide.md`
- `executive-summary.md`
- `operational-scorecard.md`
- `implementation-review-map.md`
- `docs/README.md`
- `v3/README.md`

## Architecture And Runtime

- `architecture.md`
- `physical-topology.md`
- `runtime-contract.md`
- `observability.md`
- `operations.md`
- `evolution.md`
- `v2/README.md`
- `v3/current-runtime-contract.md`
- `v3/runtime-state-and-evidence.md`
- `v3/program-map.md`

## Reliability Evidence

- `sli-methodology.md`
- `28-day-same-url-sli-case-study.md`
- `v3/sli-and-dashboard.md`
- `v3/tcp-stall-case-study.md`
- `v3/fast-recovery-classifier-replay.md`
- `v3/single-node-dr-case-study.md`
- `v3/encoder-upload-case-study.md`
- `v3/encoder-fps-tuning-2026-05-31.md`

## Recovery And Safety

- `v3/scoped-recovery-authority.md`
- `v3/youtube-lifecycle-safety.md`
- `v3/migration-cutover-case-study.md`
- `v3/failure-taxonomy.md`
- `v3/visual-audio-health-model.md`
- `v3/memory-guard-case-study.md`
- `v3/notification-and-auto-recovery.md`
- `v3/runbook-validation.md`
- `v3/runbooks.md`
- `v3/open-followups.md`

## Design Review

- `design-decisions-for-review.md`
- `v3/decisions.md`
- `implementation-review-map.md`

## Governance And Release Boundary

- `compliance-and-licensing-boundary.md`
- `security-and-secrets.md`
- `public-release.md`
- `test-strategy-and-safety-boundary.md`
- `incident-review-template.md`
- `support.md`
- `contributing.md`
- `archive-note.md`

## Scope

The public docs explain the system shape, design decisions, validation flow, and
security boundary. They do not include production state, private runbooks,
unsanitized incident logs, or credential-bearing environment snapshots.
