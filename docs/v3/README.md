# stream_v3 Documentation

`stream_v3` contains both retained v3 deployment evidence and the current
public source contract. A dated observation describes the former; an
unqualified current contract describes repository behavior. Monitoring v4 and
CRA source integration does not itself prove live deployment or production
authorization.

## Documents

- Current contract:
  - `current-runtime-contract.md`
  - `runtime-state-and-evidence.md`
- Evidence and SLI:
  - `sli-and-dashboard.md`
  - `rolling-sli-error-budget-feedback.md`
  - `public-status-snapshot.md`
  - `public-publisher-boundary.md`
  - `operational-reliability-and-external-evidence.md`
  - `observability-plane-self-check.md`
  - `notification-diagnostic-boundary.md`
  - `map-rendering-and-monitoring.md`
  - `map-production-cutover-case-study.md`
  - `map-mobile-legibility-review.md`
  - `fast-recovery-classifier-replay.md`
  - `youtube-lifecycle-safety.md`
  - `encoder-upload-case-study.md`
  - `encoder-fps-tuning-2026-05-31.md`
  - `tcp-stall-case-study.md`
  - `tcp-stall-resolution-depth.md`
  - `connectivity-aware-delivery-recovery.md`
  - `memory-guard-case-study.md`
  - `scoped-recovery-authority.md`
- Safety and operation:
  - `runbooks.md`
  - `runbook-validation.md`
  - `decisions.md`
  - `migration-cutover-case-study.md`
  - `failure-taxonomy.md`
  - `failure-injection-and-chaos-draft.md`
  - `visual-audio-health-model.md`
  - `music-provider-and-loudness-contract.md`
  - `notification-and-auto-recovery.md`
  - `single-node-dr-case-study.md`
  - `program-map.md`
  - `three-host-source-consolidation.md`
  - `open-followups.md`
- `../sli-methodology.md`
- `../compliance-and-licensing-boundary.md`

## Core Claim

The system is easier to operate when delivery, observation, authorization, and
publication are split: Dell keeps video and audio moving, arena keeps facts and
incidents coherent, CRA owns one durable recovery decision lifecycle, and
Raspberry Pi publishes only an allowlisted static view.

The retained topology also names the production data flow: Airspy on HP ProDesk
feeds `airspy_adsb`, ProDesk readsb, Dell readsb, Dell modified tar1090, and
then the `stream_v3` k3s delivery workload. In that retained deployment record,
HP ProDesk is also the observability host, with `stream-v3-control` and
`stream-v3-observer` as k3s observability/control workloads.

For a focused reliability review:

- `youtube-lifecycle-safety.md` explains same-URL preservation, stale-cache
  prevention, quota guards, and destructive-action gates.
- `tcp-stall-case-study.md` shows how recurring RTMPS transport stalls were
  split across delivery TCP state, WAN identity, non-YouTube TCP anchors,
  YouTube lifecycle evidence, and same-URL recovery policy.
- `tcp-stall-resolution-depth.md` records the later evidence ladder for RTMPS
  socket bursts, netlink route events, CPE evidence, and bounded packet
  metadata. The observer code is public; generated evidence artifacts are not.
- `connectivity-aware-delivery-recovery.md` explains why a physical outage
  suppresses repeated FFmpeg launches and Pod recreation while preserving the
  short retry path for an ordinary child-only exit.
- `rolling-sli-error-budget-feedback.md` shows how dated dashboard feedback
  windows are read without replacing the historical 14-day and 28-day SLI
  reviews or the later exact-identity ledger endpoint.
- `encoder-upload-case-study.md` explains why the move to NVENC CBR increased
  measured upload while preserving YouTube input health, why a temporary 2500k
  normal-profile override was corrected, and why new Mbps samples use producer
  source time.
- `migration-cutover-case-study.md` explains why a healthy Pod was not treated
  as production authority, and why the v3 smoke-test gate is 24 hours.
- `scoped-recovery-authority.md` documents why legacy broad recovery surfaces
  are not the current authority contract. The current CRA path admits only an
  exact fenced FFmpeg-child effect, and upload pressure does not authorize
  restart.
- `visual-audio-health-model.md` and `memory-guard-case-study.md` keep viewer
  correctness and capture-stack memory pressure separate from generic stream
  availability.
- `single-node-dr-case-study.md` documents the measured and unmeasured parts of
  the single-node k3s DR model.
- `public-status-snapshot.md` documents why the public site exposes a
  sanitized static operational view instead of the private monitoring backend.
- `observability-plane-self-check.md` documents why exporter timeouts,
  snapshot fallback, and `No data` dashboards are observability-plane incidents
  unless fresh delivery evidence also fails.
- `notification-diagnostic-boundary.md` documents what notification messages
  can diagnose directly, what evidence they add for fast recovery and report
  incidents, and where raw logs remain necessary for ownership.
- `map-rendering-and-monitoring.md` documents the custom aircraft map,
  analysis-only precipitation, render warmup, GPU readiness, read-only map and
  viewer probes, Prometheus alerts, and Discord/Slack routing boundary.
- `map-production-cutover-case-study.md` records the failed renderer and
  production windows, rollback thresholds, root repairs, isolated 24-hour and
  NVENC soaks, and the accepted one-hour production cutover.
- `map-mobile-legibility-review.md` records the later brightness and mobile
  scaling proposal while keeping it explicitly separate from live rollout.
- `operational-reliability-and-external-evidence.md` explains durable SLI
  rollups, external-check uncertainty, the oEmbed correction, notification
  timing, and the no-restart/no-SLO authority boundary.
- `public-publisher-boundary.md` maps the Raspberry Pi source, redaction,
  last-good, cache, and static-edge ownership contract.
- `../compliance-and-licensing-boundary.md` and
  `music-provider-and-loudness-contract.md` document how ADS-B publication,
  receiver privacy, provider-specific credit, and playback loudness were
  treated as design constraints rather than informal operator memory.
- `fast-recovery-classifier-replay.md` documents how historical
  fast-recovery restarts are replayed by the current classifier without
  backfilling old shadow logs.
