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

Start with `operational-case-studies.md` for a short account of the recent failures.
Use the rest of this page as a reference catalog.

## Core Entry Points

- `public-language-policy.md`: [English prose and Unicode inputs](public-language-policy.md).
- `hiring-reviewer-guide.md`
- `executive-summary.md`
- `operational-case-studies.md`
- `operational-scorecard.md`
- `implementation-review-map.md`
- `docs/README.md`
- `v3/README.md`

## OCI And Twitch Source Update

- `v3/oci-relay-and-twitch.md`: single sender, relay ownership, and reception evidence.
- `v3/route-network-observation.md`: multiple probes, causal baselines, and offline replay.
- `v3/twitch-session-renewal.md`: daily ownership and the retained failed exercise.
- `v3/public-source-port-2026-10-13.md`: source decisions, provenance, and validation limits.

## Architecture And Runtime

- `hardware-selection.md`: [CPU, GPU, workload separation, and the cost of changing a working system](hardware-selection.md).
- `architecture.md`
- `physical-topology.md`
- `runtime-contract.md`
- `observability.md`
- `operations.md`
- `evolution.md`
- `v2/README.md`
- `v3/current-runtime-contract.md`
- `v3/public-status-snapshot.md`
- `v3/public-publisher-boundary.md`
- `v3/pi-publisher-source-port-2026-10-14.md`: [Pi publisher source reconciliation and offline validation](v3/pi-publisher-source-port-2026-10-14.md).
- `v3/runtime-state-and-evidence.md`
- `v3/map-rendering-and-monitoring.md`
- `v3/map-mobile-legibility-review.md`
- `v3/three-host-source-consolidation.md`
- `v3/program-map.md`

## Reliability Evidence

- `v3/twitch-sli-resilience.md`: [Twitch collector resilience and visible missing-data states](v3/twitch-sli-resilience.md).
- `v3/public-diagram-and-trend-display.md`: [Observed links, inferred direct delivery, and missing chart data](v3/public-diagram-and-trend-display.md).

- `sli-methodology.md`
- `28-day-same-url-sli-case-study.md`
- `v3/sli-and-dashboard.md`
- `v3/rolling-sli-error-budget-feedback.md`
- `v3/operational-reliability-and-external-evidence.md`
- `v3/observability-plane-self-check.md`
- `v3/notification-diagnostic-boundary.md`
- `v3/tcp-stall-case-study.md`
- `v3/tcp-stall-resolution-depth.md`
- `v3/fast-recovery-classifier-replay.md`
- `v3/single-node-dr-case-study.md`
- `v3/encoder-upload-case-study.md`
- `v3/encoder-fps-tuning-2026-05-31.md`
- `v3/map-production-cutover-case-study.md`
- `v3/map-mobile-legibility-review.md`

## Recovery And Safety

- `v3/connectivity-aware-delivery-recovery.md`
- `v3/scoped-recovery-authority.md`
- `../recovery-control/docs/why-cra.md`
- `../recovery-control/docs/architecture.md`
- `../recovery-control/docs/safety-model.md`
- `../recovery-control/docs/harness-trust.md`
- `../recovery-control/docs/harness-engineering-draft.md`
- `../recovery-control/docs/failure-injection-draft.md`
- `../recovery-control/docs/chaos-testing-draft.md`
- `v3/youtube-lifecycle-safety.md`
- `v3/migration-cutover-case-study.md`
- `v3/failure-taxonomy.md`
- `v3/visual-audio-health-model.md`
- `v3/music-provider-and-loudness-contract.md`
- `v3/memory-guard-case-study.md`
- `v3/notification-and-auto-recovery.md`
- `v3/notification-diagnostic-boundary.md`
- `v3/runbook-validation.md`
- `v3/runbooks.md`
- `v3/open-followups.md`
- `v3/failure-injection-and-chaos-draft.md`

## Monitoring v4 / arena-server

- `../monitoring-v4/README.md`
- `../monitoring-v4/docs/architecture.md`
- `../monitoring-v4/docs/hardening.md`
- `../monitoring-v4/docs/harness-engineering-draft.md`
- `../monitoring-v4/docs/soak-checkpoints.md`
- `../monitoring-v4/docs/status.md`
- `../monitoring-v4/docs/public-release.md`

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
