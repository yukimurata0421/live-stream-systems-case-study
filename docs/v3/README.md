# stream_v3 Documentation

`stream_v3` is the current architecture described by this public repository.

## Documents

- Current contract:
  - `current-runtime-contract.md`
  - `runtime-state-and-evidence.md`
- Evidence and SLI:
  - `sli-and-dashboard.md`
  - `public-status-snapshot.md`
  - `fast-recovery-classifier-replay.md`
  - `youtube-lifecycle-safety.md`
  - `encoder-upload-case-study.md`
  - `encoder-fps-tuning-2026-05-31.md`
  - `tcp-stall-case-study.md`
  - `memory-guard-case-study.md`
- Safety and operation:
  - `runbooks.md`
  - `runbook-validation.md`
  - `decisions.md`
  - `migration-cutover-case-study.md`
  - `failure-taxonomy.md`
  - `visual-audio-health-model.md`
  - `notification-and-auto-recovery.md`
  - `single-node-dr-case-study.md`
  - `program-map.md`
  - `open-followups.md`
- `../sli-methodology.md`
- `../compliance-and-licensing-boundary.md`

## Core Claim

The system is easier to operate when delivery and observation are split:
delivery keeps video and audio moving; observation keeps evidence, SLI, and
recovery decisions coherent.

The public topology also names the production data flow: Airspy on HP ProDesk
feeds `airspy_adsb`, ProDesk readsb, Dell readsb, Dell modified tar1090, and
then the `stream_v3` k3s delivery workload. The HP ProDesk is also the
observability host.

For a focused reliability review:

- `youtube-lifecycle-safety.md` explains same-URL preservation, stale-cache
  prevention, quota guards, and destructive-action gates.
- `tcp-stall-case-study.md` shows how recurring RTMPS transport stalls were
  split across delivery TCP state, WAN identity, non-YouTube TCP anchors,
  YouTube lifecycle evidence, and same-URL recovery policy.
- `encoder-upload-case-study.md` explains why the move to NVENC CBR increased
  measured upload while preserving YouTube input health.
- `migration-cutover-case-study.md` explains why a healthy Pod was not treated
  as production authority, and why the v3 smoke-test gate is 24 hours.
- `visual-audio-health-model.md` and `memory-guard-case-study.md` keep viewer
  correctness and capture-stack memory pressure separate from generic stream
  availability.
- `single-node-dr-case-study.md` documents the measured and unmeasured parts of
  the single-node k3s DR model.
- `public-status-snapshot.md` documents why the public site exposes a
  sanitized static operational view instead of the private monitoring backend.
- `../compliance-and-licensing-boundary.md` documents how ADS-B publication,
  receiver privacy, and NCS attribution were treated as design constraints
  rather than informal operator memory.
- `fast-recovery-classifier-replay.md` documents how historical
  fast-recovery restarts are replayed by the current classifier without
  backfilling old shadow logs.
