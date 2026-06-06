# Incident Review Template

This template defines the minimum useful incident record for `stream_v3`.
It is designed for sanitized public or internal reviews: enough evidence to
learn from the event, without publishing secrets, raw private state, or exact
network identity.

## Template

```text
Title:
Date / timezone:
Duration:
Severity:
Failure taxonomy name:

User-facing impact:
- YouTube public live state:
- Same watch URL preserved:
- Visual correctness:
- Audio correctness:
- ADS-B source freshness:

Detection:
- First signal:
- Current vs historical signal:
- Dashboard panel / raw metric source:
- Alert or routine check:

Evidence:
- Delivery-plane observations:
- Observability-plane observations:
- YouTube Data API / OAuth / public watch evidence:
- TCP / upload evidence:
- Memory / cgroup / process evidence:
- Capture / audio evidence:
- Logs or state files reviewed:

Timeline:
- T+00:
- T+..:
- Recovery confirmed:

Decision record:
- Actions taken:
- Actions explicitly not taken:
- Why destructive YouTube lifecycle mutation was or was not allowed:
- Why rollback was or was not used:

Root cause:
- Confirmed:
- Most likely:
- Excluded hypotheses:
- Unknowns:

Recovery result:
- Same URL:
- Public live:
- RTMPS ingest:
- Visual:
- Audio:
- Upload:
- Notifications:

Follow-up:
- Code:
- Tests:
- Runbook:
- Dashboard:
- Documentation:
- Owner:
```

## Review Rules

- Do not treat dashboard red state as root cause without raw source and
  freshness checks.
- Do not collapse API, OAuth, public watch, RTMPS ingest, visual correctness,
  audio correctness, and ADS-B freshness into one health bit.
- Do not authorize YouTube broadcast replacement from memory pressure, audio
  faults, visual faults, stale API evidence, or report-only WAN probes alone.
- Record what was deliberately not changed. The absence of a risky action is
  often the most important recovery decision.

## Sanitized Example

Title: RTMPS TCP stall with WAN identity refresh signature

Date / timezone: sanitized JST morning window

Severity: delivery degradation, same URL preserved

Failure taxonomy name: `tcp_stall`

User-facing impact:

- YouTube public live state: recovered in the observed window.
- Same watch URL preserved: yes.
- Visual correctness: not the primary fault.
- Audio correctness: not the primary fault.
- ADS-B source freshness: not the primary fault.

Detection:

- First signal: delivery TCP send samples showed growing `lastsnd_ms`, queued
  bytes, and low send throughput.
- Current vs historical signal: fast recovery reacted to current delivery
  evidence; dashboard history was supporting context only.
- Alert or routine check: recurring daily-window diagnosis.

Evidence:

- Delivery plane: FFmpeg RTMPS socket stalled, fast recovery classified
  `network_down` or `tcp_stall`.
- Observability plane: same-URL state remained the highest-priority invariant.
- YouTube evidence: no evidence justified broadcast replacement.
- TCP / WAN evidence: Cloudflare AS13335 and Google AS15169 anchors failed
  together; immediate reconnect after persistent-flow failure also failed.
- Identity evidence: public IPv4 identity and IPv6 delegated prefix changed in
  the same daily window.

Decision record:

- Actions taken: keep delivery-plane fast recovery active; retain WAN observers
  as report-only; inspect CPE/carrier session settings.
- Actions explicitly not taken: no YouTube broadcast replacement; no encoder
  retuning based on the WAN signature; no promotion of report-only probes to
  destructive recovery authority.

Root cause:

- Confirmed: recurring transport stall outside the YouTube lifecycle layer.
- Most likely: WAN or carrier session refresh.
- Excluded: YouTube-only ingest edge failure, Google-only path issue,
  DNS-only failure, simple server-side keepalive close.
- Unknowns: whether the initiator was CPE policy or carrier policy.

Follow-up:

- Code: keep WAN identity and persistent anchor observers available.
- Tests: cover observer parsing and result shape.
- Runbook: route recurring RTMPS stalls through TCP, WAN identity, anchor, and
  same-URL checks before changing YouTube policy.
- Documentation: `docs/v3/tcp-stall-case-study.md`.
