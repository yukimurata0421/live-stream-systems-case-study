# Runbooks

## Shadow Validation

```bash
python3 ops/scripts/validate_k3s_manifests.py
python3 ops/scripts/v3_shadow_acceptance.py
```

Expected result: manifests pass, control-loop tasks pass, and the shadow action
plan has `execute=false`.

## Runtime Health Check

Check:

- Pod readiness
- FFmpeg ingest connection
- YouTube public state
- YouTube health
- audio route
- now-playing freshness
- memory guardrail
- recovery action plan

## Encoder Fps Change Check

For fps changes, hold the video bitrate and maxrate constant during the trial,
then compare fresh upload samples, YouTube health, same-URL state, and
per-frame bit budget before changing the env-synced contract.

The retained encoder/upload decision model is in
`encoder-upload-case-study.md`. The important rule is that lower upload is not
accepted if it creates YouTube low-bitrate or not-enough-video warnings.

## YouTube Lifecycle Safety Check

Before any destructive YouTube action, verify:

- expected video ID and public watch URL
- public live state
- Data API state and checked timestamp
- OAuth channel authority and checked timestamp
- quota state
- resolver/watchdog cache freshness
- explicit action gate

The retained public model is in `youtube-lifecycle-safety.md`.

## Migration Smoke Test

For v3 changes that affect runtime ownership, encoder behavior, recovery, or
cutover authority, use a 24-hour smoke-test gate before treating the change as
ordinary production behavior.

The retained public model is in `migration-cutover-case-study.md`; the test
boundary is in `../test-strategy-and-safety-boundary.md`.

Check:

- same watch URL preserved
- no replacement broadcast created by recovery logic
- YouTube public/live/ingest evidence acceptable
- upload p95 inside the accepted budget
- no persistent YouTube low-bitrate or not-enough-video warning
- visual and audio checks healthy
- no unresolved memory guard or cgroup OOM evidence
- recovery actions attributable and inside budget
- notifications classify current incidents separately from auto-recovered events

## TCP Stall Validation

For recurring RTMPS transport stalls, compare delivery TCP state with
non-YouTube WAN evidence before changing YouTube lifecycle or encoder policy.
The retained public model is in `tcp-stall-case-study.md`.

Check:

- FFmpeg TCP `lastsnd_ms`, `notsent`, `unacked`, and send throughput
- fast-recovery trigger and restart budget
- same-URL state and YouTube public/live evidence
- public IPv4 identity and IPv6 delegated prefix changes
- fresh TCP anchors to independent AS paths
- persistent TCP/TLS anchors and immediate reconnect-after-failure result
- `stream-v3-wan-address-observer.timer` and
  `stream-v3-persistent-anchor-observer.service` are installed if the host is
  expected to retain cause-layer evidence

## Visual / Audio / Memory Check

Treat RTMPS connected, YouTube public live, visual correctness, audio
correctness, ADS-B freshness, and memory guardrails as separate signals.

Check:

- capture frame and map source health
- now-playing metadata freshness
- PulseAudio sink/source and monitor energy
- Xvfb RSS/shared memory guard state
- cgroup current/peak/events
- whether the symptom is current, recovered, or only historical

The retained public models are in `visual-audio-health-model.md`,
`memory-guard-case-study.md`, and `failure-taxonomy.md`.

## k3s / Node Recovery

For a single-node k3s deployment, distinguish:

- k3s service restart
- Pod restart
- node reboot
- disk loss
- v2 rollback

RTO and RPO should be measured from fault injection to externally visible
recovery, not only from process restart completion.

The retained public DR model is in `single-node-dr-case-study.md`.

## Runbook Validation

Runbooks should be validated by a reviewer who does not depend on private
operator memory. Destructive actions stop at read-only or dry-run unless
explicitly approved. The retained public validation model is in
`runbook-validation.md`.
