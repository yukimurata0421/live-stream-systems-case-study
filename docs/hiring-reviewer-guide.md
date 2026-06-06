# Hiring Reviewer Guide

This repository is meant to be reviewed as an operational case study, not as a
plug-and-play streaming package.

## This Is Not

- a generic OSS starter;
- a supported YouTube streaming product;
- a commercial SaaS project;
- an installer for another operator's environment.

## This Is

- a reliability engineering case study;
- a 24/7 media delivery system operated under real hardware constraints;
- an example of separating delivery ownership from monitoring ownership;
- an example of publishing a public-safe status snapshot without exposing the
  private monitoring backend;
- an example of treating the ADS-B source chain as evidence instead of hiding it
  inside the renderer;
- a record of safety decisions around recovery, stale evidence, API quota,
  encoder/upload trade-offs, and single-node DR boundaries.

## Key Design Decisions

1. The delivery plane and observability plane are separated.
2. Monitors do not directly own FFmpeg or the live RTMPS process.
3. The production ADS-B data path is Airspy on HP ProDesk -> `airspy_adsb` ->
   ProDesk readsb -> Dell readsb -> Dell modified tar1090 -> `stream_v3`
   delivery.
4. Recovery is staged through guards before destructive actions.
5. API quota exhaustion is treated as degraded evidence, not immediate stream
   failure.
6. Shadow mode existed before destructive cutover and remains the safe
   validation path.
7. NVENC CBR was accepted even though measured upload increased, because
   lower-upload VBR/CQ trials damaged YouTube input health.
8. Single-node k3s recovery is documented with measured and unmeasured RTO/RPO
   boundaries instead of being presented as ideal HA.
9. A 24-hour smoke test is treated as a migration confidence gate, grounded in
   v2's stable behavior, not as a long-window SLO proof.
10. The public status site is a reduced static snapshot, not a public Grafana or
    raw-log mirror.
11. Public validation excludes secrets, live YouTube mutation, and production
   k3s apply.

## Suggested Review Path

1. `README.md`
2. `docs/executive-summary.md`
3. `docs/operational-scorecard.md`
4. `docs/implementation-review-map.md`
5. `docs/design-decisions-for-review.md`
6. `docs/v3/decisions.md`
7. `docs/runtime-contract.md`
8. `docs/sli-methodology.md`
9. `docs/28-day-same-url-sli-case-study.md`
10. `docs/test-strategy-and-safety-boundary.md`
11. `docs/v3/public-status-snapshot.md`
12. `docs/v3/migration-cutover-case-study.md`
13. `docs/v3/youtube-lifecycle-safety.md`
14. `docs/v3/tcp-stall-case-study.md`
15. `docs/v3/encoder-upload-case-study.md`
16. `docs/v3/memory-guard-case-study.md`
17. `docs/v3/single-node-dr-case-study.md`
18. `docs/v3/failure-taxonomy.md`
19. `docs/incident-review-template.md`
20. `docs/v3/runtime-state-and-evidence.md`
21. `src/stream_v2/recovery_orchestrator/gate.py`
22. `ops/scripts/v3_shadow_acceptance.py`
23. `ops/scripts/wan_address_observer.py`
24. `ops/scripts/persistent_tcp_anchor_observer.py`
25. `tests/test_v3_shadow_acceptance.py`
26. `tests/test_youtube_video_id_resolver_cache_freshness.py`
27. `.github/workflows/public-snapshot-check.yml`

## What To Evaluate

- Whether the failure domains are named clearly.
- Whether recovery actions are blocked when evidence is stale, ambiguous, or in
  shadow mode.
- Whether the SLI story includes measured windows, denominators, and explicit
  unknowns instead of only conceptual dashboard language.
- Whether recurring transport symptoms are split by falsifiable evidence:
  delivery TCP state, WAN identity, non-YouTube anchors, YouTube lifecycle, and
  same-URL recovery safety.
- Whether encoder/upload tuning uses measured wire behavior and YouTube input
  health instead of nominal bitrate alone.
- Whether the 28-day same-URL case study separates URL identity, availability,
  upload guardrails, notification quality, and known unresolved risks.
- Whether visual correctness, audio correctness, memory pressure, and ADS-B
  source freshness remain separate from generic "stream is up" language.
- Whether single-node DR claims distinguish measured control-plane recovery
  from unmeasured node, disk, and viewer-facing RTMPS recovery.
- Whether the 24-hour smoke-test rationale is appropriately scoped to migration
  confidence from v2 stability instead of overstated as a reliability proof.
- Whether the public status page communicates freshness, guardrails, and
  recovery ownership without exposing the private monitoring stack.
- Whether incident review records decisions that were intentionally not taken,
  especially YouTube lifecycle mutation and rollback.
- Whether public validation proves the snapshot boundary without requiring
  credentials or live production mutation.
- Whether the system shows operational judgment rather than only code volume.
