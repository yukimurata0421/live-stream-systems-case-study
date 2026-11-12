# Documentation Catalog

Every Stream document has one primary place here, grouped by its job for the
reader. Use the [guide](README.md) for reading order and the
[documentation model](documentation-model.md) for ownership and update rules.
A page can contain supporting history without becoming the current rule for
that historical result. Component documentation is linked separately below.

## Start Here

- `README.md`: [documentation guide](README.md), the suggested reading order.
- `hiring-reviewer-guide.md`: [reading paths by reviewer role](hiring-reviewer-guide.md).
- `current-status.md`: [recent changes, unobserved items, and disabled features](current-status.md).
- `operational-case-studies.md`: [short incident stories and their evidence](operational-case-studies.md).
- `operational-scorecard.md`: [measured results, tests, and remaining gaps](operational-scorecard.md).
- `implementation-review-map.md`: [where each claim's code and tests live](implementation-review-map.md).
- `v3/README.md`: [v3 folder listing](v3/README.md).

## Current Contracts: Delivery And Authority

These pages own present rules. A dated exercise inside one is evidence for its named revision only.

- `v3/fallback-decision-contract.md`: [when the direct YouTube backup starts and stops: evidence, decision table, timers, tradeoffs and evidence status](v3/fallback-decision-contract.md).
- `v3/route-causality.md`: [durable cause identity and arena's secondary-recovery veto](v3/route-causality.md).
- `v3/ffmpeg-fifo-recovery.md`: [why output reconnection was chosen over ride-through or process restart, bounded FIFO recovery and process/connection generations](v3/ffmpeg-fifo-recovery.md).
- `architecture.md`: [hosts, data paths, who can act on what, failure domains](architecture.md).
- `runtime-contract.md`: [delivery and monitoring runtimes, encoder, restart gates](runtime-contract.md).
- `v3/map-rendering-and-monitoring.md`: [map renderer, precipitation, probes, and alert routing](v3/map-rendering-and-monitoring.md).
- `v3/music-provider-and-loudness-contract.md`: [music provider permissions, credit, and loudness](v3/music-provider-and-loudness-contract.md).
- `v3/youtube-lifecycle-safety.md`: [same-URL preservation and gates on destructive YouTube actions](v3/youtube-lifecycle-safety.md).
- `v3/failure-taxonomy.md`: [failure classes, owners, and what must not trigger a restart](v3/failure-taxonomy.md).
- `v3/visual-audio-health-model.md`: [viewer-visible video and audio correctness](v3/visual-audio-health-model.md).
- `v3/connectivity-aware-delivery-recovery.md`: [suppressing restarts during a network outage](v3/connectivity-aware-delivery-recovery.md).
- `v3/program-map.md`: [programs and their owning plane](v3/program-map.md).
- `v3/scoped-recovery-authority.md`: [the CRA effect boundary](v3/scoped-recovery-authority.md).

## Current Contracts: Observation And Publication

Observation meaning, freshness and publication have separate owners from recovery decisions.

- `v3/cra-observability-closure.md`: [CRA observation, retained failures and four-host closure](v3/cra-observability-closure.md).
- [CRA monitoring guide](../monitoring-v4/docs/cra-observability.md): read-only facts, audit ingestion and route-specific notification delivery.
- `../publication/cra-observability-port-20261113.json`: [sanitized source port and validation manifest](../publication/cra-observability-port-20261113.json).

- `v3/observation-standardization.md`: [shared observation meaning and the direct-scrape connection](v3/observation-standardization.md).
- `v3/observation-release-provenance.md`: [committed source releases, producer provenance and player qualification gates](v3/observation-release-provenance.md).
- `v3/external-watchdog.md`: [independent heartbeat detection and direct notification delivery](v3/external-watchdog.md).
- `v3/cross-host-observation-time.md`: [request-bound clock intervals, finite caches, public timestamp normalization, and verification limits](v3/cross-host-observation-time.md).
- `v3/transport-flight-recorder.md`: [event windows of FIFO transport evidence in SQLite, recording triggers, missing-data rules and limits](v3/transport-flight-recorder.md).
- `v3/twitch-thumbnail-monitor.md`: [provider-image clocks, API reuse, the K3s collector and its Service boundaries, and warning scope](v3/twitch-thumbnail-monitor.md).
- `v3/relay-clock-monitor.md`: [returned video, paired OCR, native evidence and bounded observer state](v3/relay-clock-monitor.md).
- `observability.md`: [monitoring signals, OCI-aware Twitch assessment, and evidence boundaries](observability.md).
- `v3/runtime-state-and-evidence.md`: [runtime state files and their meaning](v3/runtime-state-and-evidence.md).
- `v3/notification-and-auto-recovery.md`: [incident, restart, and recovery notifications](v3/notification-and-auto-recovery.md).
- `v3/notification-diagnostic-boundary.md`: [what a notification can and cannot diagnose](v3/notification-diagnostic-boundary.md).
- `v3/notification-observation-time.md`: [producer identity, UTC and boottime freshness, and recovery notifications](v3/notification-observation-time.md).
- `v3/public-status-snapshot.md`: [what the public site shows and why it is static](v3/public-status-snapshot.md).
- `v3/public-publisher-boundary.md`: [owner-generated bundles, Pi validation, sanitization and publication limits](v3/public-publisher-boundary.md).
- `v3/public-diagram-and-trend-display.md`: [observed links, inferred paths, and missing chart data](v3/public-diagram-and-trend-display.md).
- `v3/upload-measurement-source.md`: [independent read-only upload sampling, original evidence and source-recovery verification](v3/upload-measurement-source.md).
- `v3/sli-and-dashboard.md`: [dashboard and SLI contract](v3/sli-and-dashboard.md).
- `v3/adsb-source-coverage.md`: [time-based ADS-B evidence coverage, UNKNOWN boundaries, and count compatibility](v3/adsb-source-coverage.md).
- `v3/operational-reliability-and-external-evidence.md`: [durable rollups and external checks](v3/operational-reliability-and-external-evidence.md).
- `v3/observability-plane-self-check.md`: [when monitoring itself is wrong](v3/observability-plane-self-check.md).

## Procedures And Source Guides

Commands and installation prerequisites are reference procedures, not authorization to operate the live system.

- `v3/manual-maintenance.md`: [one-command planned maintenance, a relay-hosted slate, bounded notification classification and explicit return gates](v3/manual-maintenance.md).
- `v3/airspy-reception-guard.md`: [optional receiver-host reception checks, a durable one-extra-restart cap, isolated real-systemd tests and publication limits](v3/airspy-reception-guard.md).
- `v3/oci-backup-and-restore.md`: [encrypted relay backup, offline verification and files-only restore](v3/oci-backup-and-restore.md).
- `operations.md`: [local checks, shadow mode, release provenance](operations.md).
- `v3/runbooks.md`: [operator commands](v3/runbooks.md).
- `v3/runbook-validation.md`: [how runbooks are checked by someone else](v3/runbook-validation.md).
- `v3/r2-publication-transport.md`: [portable R2 publisher, durable receipts, IPv6 preference, IPv4 fallback and isolated DNS](v3/r2-publication-transport.md).

## Decisions And Rationale

- `v3/route-baseline-retirement-decision.md`: [why the learned throughput baseline was retired, the data behind it, and when to revisit](v3/route-baseline-retirement-decision.md).
- `v3/oci-route-simplification-decision.md`: [why the unfinished recovery design was replaced](v3/oci-route-simplification-decision.md).
- `hardware-selection.md`: [why each host, CPU, and GPU was chosen](hardware-selection.md).
- `v3/decisions.md`: [accepted v3 design decisions](v3/decisions.md).
- `design-decisions-for-review.md`: [compact decision ledger with rejected alternatives](design-decisions-for-review.md).

## Measurements And Dated Evidence

Read the window and policy revision before applying a result to the current system. The fallback revision story links its detailed experiments.

- `v3/clock-safe-media-progress.md`: [counter-proven clock holds, bounded alternating uncertainty, retained baselines and transition audit](v3/clock-safe-media-progress.md).
- `v3/boottime-media-health.md`: [boottime media health, independent UTC diagnostics and removal of transition grace](v3/boottime-media-health.md).
- `v3/ffmpeg-fifo-measurements.md`: [restart versus FIFO timing, where the time went, the audio-loss rollout and the corrected exercise](v3/ffmpeg-fifo-measurements.md).
- `v3/fifo-timestamp-continuity-case-study.md`: [how same-process recovery carried old timestamps to YouTube, the per-connection origin fix, and the comparison that isolated it](v3/fifo-timestamp-continuity-case-study.md).
- `v3/self-healing-oci-fanout.md`: [the media route since 2026-10-20, its components, and the relay-stop drill](v3/self-healing-oci-fanout.md).
- `v3/r2-publication-and-freshness-case-study.md`: [operated R2 migration, stale-data checks, archive verification, and the later clock-skew diagnosis](v3/r2-publication-and-freshness-case-study.md).
- `v3/public-status-page-2026-10-20.md`: [page redesign around current state and 24-hour trends](v3/public-status-page-2026-10-20.md).
- `v3/twitch-sli-resilience.md`: [Twitch collection and visible missing-data states](v3/twitch-sli-resilience.md).
- `same-url-continuity-evidence.md`: [dated same-URL checkpoints and earlier SLI windows](same-url-continuity-evidence.md).
- `sli-methodology.md`: [how SLIs were defined and the measured baseline](sli-methodology.md).
- `v3/rolling-sli-error-budget-feedback.md`: [rolling windows used as operator feedback](v3/rolling-sli-error-budget-feedback.md).
- `v3/fast-recovery-classifier-replay.md`: [replaying past restarts through the current classifier](v3/fast-recovery-classifier-replay.md).
- `v3/thumbnail-k3s-migration-2026-11-01.md`: [Service-based thumbnail collection, retained state and public-screen verification](v3/thumbnail-k3s-migration-2026-11-01.md).
- `v3/thumbnail-transfer-recovery-2026-11-01.md`: [slow thumbnail transfer, missing current values and verified public recovery](v3/thumbnail-transfer-recovery-2026-11-01.md).
- `v3/tcp-stall-case-study.md`: [recurring upload stalls traced to the home network](v3/tcp-stall-case-study.md).
- `v3/tcp-stall-resolution-depth.md`: [the finer-grained evidence ladder for those stalls](v3/tcp-stall-resolution-depth.md).
- `v3/encoder-upload-case-study.md`: [NVENC CBR and the upload budget](v3/encoder-upload-case-study.md).
- `v3/encoder-fps-tuning-2026-05-31.md`: [the 4, 5, and 10 fps trial](v3/encoder-fps-tuning-2026-05-31.md).
- `v3/map-production-cutover-case-study.md`: [August 2026 renderer cutover, failures, and soaks](v3/map-production-cutover-case-study.md).
- `v3/memory-guard-case-study.md`: [memory as diagnostic evidence](v3/memory-guard-case-study.md).
- `v3/single-node-dr-case-study.md`: [single-node k3s restart drill and untested DR](v3/single-node-dr-case-study.md).
- `v3/migration-cutover-case-study.md`: [v2 to v3 cutover and the 24-hour gate](v3/migration-cutover-case-study.md).
- `v3/three-host-source-consolidation.md`: [comparing three hosts' source trees](v3/three-host-source-consolidation.md).
- `v3/clock-observation-case-study.md`: [from pixel comparison to paired clocks, a blind reader, and provider thumbnails](v3/clock-observation-case-study.md).
- `v3/fallback-policy-revisions.md`: [one fallback rule revised under test, and what each check found](v3/fallback-policy-revisions.md).
- `v3/fallback-clock-recording.md`: [the October 30 clock-skew false start, the provisional fix, its interval replacement and bounded HDD recording](v3/fallback-clock-recording.md).
- `v3/route-failover-validation.md`: [isolated input-loss switching, the fixed producer gap and failed continuity measurements](v3/route-failover-validation.md).
- `v3/relay-stop-main-loop.md`: [the failed relay-stop test under hold-on-observation-loss](v3/relay-stop-main-loop.md).
- `v3/relay-recovery-deadline.md`: [explicit relay-stop facts and a deadline that survives FFmpeg generations](v3/relay-recovery-deadline.md).
- `v3/youtube-egress-failover.md`: [separating upstream ACKs from YouTube confirmation, copy failure facts, and the clock-hold transition fix](v3/youtube-egress-failover.md).

## History And Superseded Designs

These preserve earlier states; follow their successor links for current rules.

- `evolution.md`: [from the first host to the current route](evolution.md).
- `v2/README.md`: [stream_v2 summary](v2/README.md).
- `v3/oci-relay-and-twitch.md`: [2026-10-13 signed-lease relay, superseded](v3/oci-relay-and-twitch.md).
- `v3/twitch-session-renewal.md`: [2026-10-13 daily renewal owner, superseded](v3/twitch-session-renewal.md).
- `v3/route-network-observation.md`: [2026-10-13 route diagnostics and baselines, retired 2026-10-29](v3/route-network-observation.md).
- `status-history-2026-11-03.md`: [preserved checkpoints and then-open items through November 3](status-history-2026-11-03.md).

## Proposals

These describe proposed work or explicitly limited local checks.

- `v3/map-mobile-legibility-review.md`: [brightness and phone-size proposal, not deployed](v3/map-mobile-legibility-review.md).
- `v3/failure-injection-and-chaos-draft.md`: [failure-injection plan, draft](v3/failure-injection-and-chaos-draft.md).

## Publication Records

These identify selected source ports and validation, not automatic deployment.

- `../publication/cra-observability-port-20261113.json`: [Pod API observer, fixed CRA facts, monitoring integration and four-host checkpoint](../publication/cra-observability-port-20261113.json).

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

## Governance And Documentation

- `archive-note.md`: [how the private history was reduced](archive-note.md).
- `security-and-secrets.md`: [credential handling](security-and-secrets.md).
- `compliance-and-licensing-boundary.md`: [ADS-B, privacy, and music licensing](compliance-and-licensing-boundary.md).
- `test-strategy-and-safety-boundary.md`: [what public tests can and cannot prove](test-strategy-and-safety-boundary.md).
- `public-language-policy.md`: [English prose and Unicode inputs](public-language-policy.md).
- `incident-review-template.md`: [incident record template](incident-review-template.md).
- `contributing.md`: [contribution notes](contributing.md).
- `support.md`: [support expectations](support.md).
- `documentation-model.md`: [document roles, rule owners and update conventions](documentation-model.md).

## Component Guides

The component owns its local setup and detailed catalog. These are optional
branches from the Stream review, not additional required reading.

| Component | Entry and scope |
| --- | --- |
| CRA | [Source guide](../recovery-control/README.md), [catalog](../recovery-control/docs/00_INDEX.md), [current contract](../recovery-control/docs/local-reflex-authority.md); public defaults remain `dry_run`, with a separately dated operated enforce checkpoint. |
| Monitoring v4 | [Source guide](../monitoring-v4/README.md), [architecture](../monitoring-v4/docs/architecture.md), [status](../monitoring-v4/docs/status.md); read-only observation is distinct from recovery authority. |
| Public publisher | [Bundle binding, upload and Worker setup](../ops/public-publisher/site/README.md). |
| Kubernetes examples | [Manifest guide](../deploy/k3s/README.md). |
| Host maintenance | [Readiness and maintenance examples](../ops/host-maintenance/README.md). |
| Monitoring extracts | [Sanitized extracts](../ops/prodesk-monitoring/README.md). |
| Renderer attribution | [Map data and library attribution](../ui/overlay/adsb-map/ATTRIBUTION.md). |
