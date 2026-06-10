# Hiring Reviewer Guide

This repository is meant to be reviewed as an operational case study, not as a
plug-and-play streaming package.

## 30-Second Summary

`stream_v3` is a 24/7 YouTube Live delivery system operated as a public
reliability case study.

The main achievement is not simple uptime. The system shows same-URL continuity
and public-safe observability publication through GCS + Cloudflare, plus
automated recovery, SLI-based monitoring, and k3s runtime operation.

The strongest review signal is operational judgment: the system names failure
domains, keeps production invariants separate from availability ratios, and
blocks destructive actions when evidence is stale, ambiguous, or outside the
same-URL recovery contract.

## If You Are A Non-Technical Interviewer

Read:

1. `README.md` Reviewer Summary
2. `README.md` Evidence Snapshot
3. `docs/operational-scorecard.md`
4. `docs/executive-summary.md`

Look for:

- monthly-window same-URL operation evidence;
- automated recovery instead of manual babysitting;
- a conservative scorecard that separates measured, tested, documented, and
  not-publicly-measured claims;
- explicit limits on scale, support, and SLO claims.

## If You Are A Backend / Infrastructure Reviewer

Read:

1. `README.md` architecture diagrams
2. `docs/v3/public-status-snapshot.md`
3. `docs/implementation-review-map.md`
4. `docs/runtime-contract.md`
5. `docs/physical-topology.md`

Look for:

- k3s runtime boundary for delivery;
- delivery-plane / observability-plane separation;
- GCS + Cloudflare public-safe status publication;
- private Prometheus/Loki/Grafana staying outside the public path;
- YouTube API quota-aware monitoring and stale-evidence handling;
- code and tests mapped to reliability claims.

## If You Are An SRE / Platform Reviewer

Read:

1. `docs/v3/sli-and-dashboard.md`
2. `docs/v3/tcp-stall-case-study.md`
3. `docs/v3/scoped-recovery-authority.md`
4. `docs/v3/fast-recovery-classifier-replay.md`
5. `docs/v3/single-node-dr-case-study.md`
6. `docs/28-day-same-url-sli-case-study.md`

Look for:

- same-watch-URL continuity treated as a production invariant;
- fault-layer classification across RTMPS, TCP, WAN/session, YouTube lifecycle,
  upload budget, and runtime memory signals;
- MTTR and incident clustering kept separate from raw restart attempts;
- report-only observers separated from mutating recovery authority;
- recovery actions blocked by stale, ambiguous, or upload-only evidence;
- known unknowns left visible instead of converted into broad uptime claims.

## This Is Not

- a generic OSS starter;
- a supported YouTube streaming product;
- a commercial SaaS project;
- an installer for another operator's environment;
- proof that every delivered frame was externally audited.

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
5. Same-watch-URL continuity is a production invariant, not an availability
   average.
6. API quota exhaustion is treated as degraded evidence, not immediate stream
   failure.
7. Shadow mode existed before destructive cutover and remains the safe
   validation path.
8. NVENC CBR was accepted even though measured upload increased, because
   lower-upload VBR/CQ trials damaged YouTube input health.
9. Single-node k3s recovery is documented with measured and unmeasured RTO/RPO
   boundaries instead of being presented as ideal HA.
10. A 24-hour smoke test is treated as a migration confidence gate, grounded in
    v2's stable behavior, not as a long-window SLO proof.
11. The public status site is a reduced static snapshot, not a public Grafana or
    raw-log mirror.
12. Historical shadow gaps are not rewritten; current classifier remediation is
    exposed as replay over retained production events.
13. Public validation excludes secrets, live YouTube mutation, and production
    k3s apply.

## Suggested Review Paths

### 10-Minute Review

1. `README.md`
2. `docs/hiring-reviewer-guide.md`
3. `docs/operational-scorecard.md`

### 30-Minute Technical Review

1. `docs/executive-summary.md`
2. `docs/implementation-review-map.md`
3. `docs/v3/sli-and-dashboard.md`
4. `docs/v3/public-status-snapshot.md`
5. `docs/v3/tcp-stall-case-study.md`

### Deep Review / Audit

Use `docs/00_INDEX.md` as the full documentation catalog. Most hiring reviewers
do not need to read every file.

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
- Whether historical shadow gaps remain visible while current classifier replay
  shows what is now covered.
- Whether incident review records decisions that were intentionally not taken,
  especially YouTube lifecycle mutation and rollback.
- Whether public validation proves the snapshot boundary without requiring
  credentials or live production mutation.
- Whether the system shows operational judgment rather than only code volume.
