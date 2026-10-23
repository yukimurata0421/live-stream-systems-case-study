# Documentation Catalog

Every public document, grouped by what it describes. This is a reference list,
not a reading order; the [documentation guide](README.md) suggests where to
start.

## Start Here

- `README.md`: [documentation guide](README.md), the suggested reading order.
- `hiring-reviewer-guide.md`: [reading paths by reviewer role](hiring-reviewer-guide.md).
- `current-status.md`: [recent changes, unobserved items, and disabled features](current-status.md).
- `operational-case-studies.md`: [nine short incident stories](operational-case-studies.md).
- `operational-scorecard.md`: [measured results, tests, and remaining gaps](operational-scorecard.md).
- `implementation-review-map.md`: [where each claim's code and tests live](implementation-review-map.md).

## Current System

- `v3/twitch-thumbnail-monitor.md`: [provider-image clocks, API reuse, RAM processing and warning scope](v3/twitch-thumbnail-monitor.md).
- `v3/relay-clock-monitor.md`: [returned video, paired OCR, native evidence and bounded observer state](v3/relay-clock-monitor.md).
- `v3/oci-backup-and-restore.md`: [encrypted relay backup, offline verification and files-only restore](v3/oci-backup-and-restore.md).

- `architecture.md`: [hosts, data paths, who can act on what, failure domains](architecture.md).
- `runtime-contract.md`: [delivery and monitoring runtimes, encoder, restart gates](runtime-contract.md).
- `v3/self-healing-oci-fanout.md`: [the media route since 2026-10-20, its thresholds, and the relay-stop drill](v3/self-healing-oci-fanout.md).
- `v3/oci-route-simplification-decision.md`: [why the unfinished recovery design was replaced](v3/oci-route-simplification-decision.md).
- `hardware-selection.md`: [why each host, CPU, and GPU was chosen](hardware-selection.md).
- `observability.md`: [monitoring signals, OCI-aware Twitch assessment, and evidence boundaries](observability.md).
- `v3/runtime-state-and-evidence.md`: [runtime state files and their meaning](v3/runtime-state-and-evidence.md).
- `v3/map-rendering-and-monitoring.md`: [map renderer, precipitation, probes, and alert routing](v3/map-rendering-and-monitoring.md).
- `v3/music-provider-and-loudness-contract.md`: [music provider permissions, credit, and loudness](v3/music-provider-and-loudness-contract.md).
- `v3/youtube-lifecycle-safety.md`: [same-URL preservation and gates on destructive YouTube actions](v3/youtube-lifecycle-safety.md).
- `v3/failure-taxonomy.md`: [failure classes, owners, and what must not trigger a restart](v3/failure-taxonomy.md).
- `v3/visual-audio-health-model.md`: [viewer-visible video and audio correctness](v3/visual-audio-health-model.md).
- `v3/connectivity-aware-delivery-recovery.md`: [suppressing restarts during a network outage](v3/connectivity-aware-delivery-recovery.md).
- `v3/notification-and-auto-recovery.md`: [incident, restart, and recovery notifications](v3/notification-and-auto-recovery.md).
- `v3/notification-diagnostic-boundary.md`: [what a notification can and cannot diagnose](v3/notification-diagnostic-boundary.md).
- `v3/program-map.md`: [programs and their owning plane](v3/program-map.md).
- `v3/decisions.md`: [accepted v3 design decisions](v3/decisions.md).
- `design-decisions-for-review.md`: [compact decision ledger with rejected alternatives](design-decisions-for-review.md).
- `operations.md`: [local checks, shadow mode, release provenance](operations.md).
- `v3/runbooks.md`: [operator commands](v3/runbooks.md).
- `v3/runbook-validation.md`: [how runbooks are checked by someone else](v3/runbook-validation.md).

## Public Status Page

- `v3/public-status-snapshot.md`: [what the public site shows and why it is static](v3/public-status-snapshot.md).
- `v3/public-publisher-boundary.md`: [Raspberry Pi collectors, redaction, last-good, and cache](v3/public-publisher-boundary.md).
- `v3/public-status-page-2026-10-20.md`: [page redesign around current state and 24-hour trends](v3/public-status-page-2026-10-20.md).
- `v3/public-diagram-and-trend-display.md`: [observed links, inferred paths, and missing chart data](v3/public-diagram-and-trend-display.md).
- `v3/twitch-sli-resilience.md`: [Twitch collection and visible missing-data states](v3/twitch-sli-resilience.md).

## Measurements And Evidence

- `same-url-continuity-evidence.md`: [160-day same-URL checkpoint and earlier SLI windows](same-url-continuity-evidence.md).
- `sli-methodology.md`: [how SLIs were defined and the measured baseline](sli-methodology.md).
- `v3/sli-and-dashboard.md`: [dashboard and SLI contract](v3/sli-and-dashboard.md).
- `v3/adsb-source-coverage.md`: [time-based ADS-B evidence coverage, UNKNOWN boundaries, and count compatibility](v3/adsb-source-coverage.md).
- `v3/rolling-sli-error-budget-feedback.md`: [rolling windows used as operator feedback](v3/rolling-sli-error-budget-feedback.md).
- `v3/operational-reliability-and-external-evidence.md`: [durable rollups and external checks](v3/operational-reliability-and-external-evidence.md).
- `v3/observability-plane-self-check.md`: [when monitoring itself is wrong](v3/observability-plane-self-check.md).
- `v3/fast-recovery-classifier-replay.md`: [replaying past restarts through the current classifier](v3/fast-recovery-classifier-replay.md).

## Case Studies (Dated)

- `v3/tcp-stall-case-study.md`: [recurring upload stalls traced to the home network](v3/tcp-stall-case-study.md).
- `v3/tcp-stall-resolution-depth.md`: [the finer-grained evidence ladder for those stalls](v3/tcp-stall-resolution-depth.md).
- `v3/encoder-upload-case-study.md`: [NVENC CBR and the upload budget](v3/encoder-upload-case-study.md).
- `v3/encoder-fps-tuning-2026-05-31.md`: [the 4, 5, and 10 fps trial](v3/encoder-fps-tuning-2026-05-31.md).
- `v3/map-production-cutover-case-study.md`: [August 2026 renderer cutover, failures, and soaks](v3/map-production-cutover-case-study.md).
- `v3/map-mobile-legibility-review.md`: [brightness and phone-size proposal, not deployed](v3/map-mobile-legibility-review.md).
- `v3/memory-guard-case-study.md`: [memory as diagnostic evidence](v3/memory-guard-case-study.md).
- `v3/single-node-dr-case-study.md`: [single-node k3s restart drill and untested DR](v3/single-node-dr-case-study.md).
- `v3/migration-cutover-case-study.md`: [v2 to v3 cutover and the 24-hour gate](v3/migration-cutover-case-study.md).
- `v3/three-host-source-consolidation.md`: [comparing three hosts' source trees](v3/three-host-source-consolidation.md).
- `v3/clock-observation-case-study.md`: [from pixel comparison to paired clocks, a blind reader, and provider thumbnails](v3/clock-observation-case-study.md).

## History And Superseded Designs

- `evolution.md`: [from the first host to the current route](evolution.md).
- `v2/README.md`: [stream_v2 summary](v2/README.md).
- `v3/oci-relay-and-twitch.md`: [2026-10-13 signed-lease relay, superseded](v3/oci-relay-and-twitch.md).
- `v3/twitch-session-renewal.md`: [2026-10-13 daily renewal owner, superseded](v3/twitch-session-renewal.md).
- `v3/route-network-observation.md`: [2026-10-13 route diagnostics and baselines](v3/route-network-observation.md).

## CRA And Recovery Design Studies (Not Enabled In Production)

- `v3/scoped-recovery-authority.md`: [the CRA effect boundary](v3/scoped-recovery-authority.md).
- `v3/failure-injection-and-chaos-draft.md`: [failure-injection plan, draft](v3/failure-injection-and-chaos-draft.md).
- [Why CRA exists](../recovery-control/docs/why-cra.md), [CRA architecture](../recovery-control/docs/architecture.md),
  [safety model](../recovery-control/docs/safety-model.md), [harness trust](../recovery-control/docs/harness-trust.md),
  and the [recovery-control catalog](../recovery-control/docs/00_INDEX.md).
- [CRA local reflex path](../recovery-control/docs/local-reflex-authority.md): local restart decision under signed central policy, audit verification, `dry_run` only.
- [Monitoring v4 README](../monitoring-v4/README.md), [architecture](../monitoring-v4/docs/architecture.md),
  and [implementation status](../monitoring-v4/docs/status.md). Its relay observer and public projection run
  in production; the rest is not enabled.

## Publication Records

- `v3/public-source-port-2026-10-23-observability.md`: [thumbnail cache timing, current quality, notification delivery and Monitoring v4 history](v3/public-source-port-2026-10-23-observability.md).
- `v3/public-source-port-2026-10-23-relay-clock.md`: [return-feed native clock evidence, non-monotonic ROI widening and a current-ROI-first UTC read](v3/public-source-port-2026-10-23-relay-clock.md).
- `v3/thumbnail-ocr-rollout-2026-10-22.md`: [operated counter activation, live OCR evidence and preserved concurrent work](v3/thumbnail-ocr-rollout-2026-10-22.md).
- `v3/public-source-port-2026-10-22-thumbnail.md`: [thumbnail monitoring and return-reader quality corrections](v3/public-source-port-2026-10-22-thumbnail.md).
- `v3/public-source-port-2026-10-22-thumbnail-review.md`: [stream rollover, missing metrics, OCR retry and source-only validation](v3/public-source-port-2026-10-22-thumbnail-review.md).
- `v3/public-source-port-2026-10-22-thumbnail-evidence.md`: [OCR replay, ROI comparison and bounded evidence follow-up](v3/public-source-port-2026-10-22-thumbnail-evidence.md).
- `v3/public-source-port-2026-10-22.md`: [clock-monitor review, dated evidence and public source adaptations](v3/public-source-port-2026-10-22.md).

- `v3/public-source-port-2026-10-20.md`: [self-healing route port](v3/public-source-port-2026-10-20.md).
- `v3/public-source-port-2026-10-13.md`: [OCI, Twitch, and route evidence port](v3/public-source-port-2026-10-13.md).
- `v3/pi-publisher-source-port-2026-10-14.md`: [Pi publisher source reconciliation](v3/pi-publisher-source-port-2026-10-14.md).
- `public-release.md`: [what this snapshot includes and excludes](public-release.md).
- `archive-note.md`: [how the private history was reduced](archive-note.md).

## Governance

- `security-and-secrets.md`: [credential handling](security-and-secrets.md).
- `compliance-and-licensing-boundary.md`: [ADS-B, privacy, and music licensing](compliance-and-licensing-boundary.md).
- `test-strategy-and-safety-boundary.md`: [what public tests can and cannot prove](test-strategy-and-safety-boundary.md).
- `public-language-policy.md`: [English prose and Unicode inputs](public-language-policy.md).
- `incident-review-template.md`: [incident record template](incident-review-template.md).
- `contributing.md`: [contribution notes](contributing.md).
- `support.md`: [support expectations](support.md).
- `v3/README.md`: [v3 folder listing](v3/README.md).
