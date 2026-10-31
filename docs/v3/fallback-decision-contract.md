# Fallback Decision Contract

This is the current rule for starting and stopping Dell's temporary direct
YouTube backup upload. Deployment and current validation are in
[current status](../current-status.md). The [revision history](fallback-policy-revisions.md)
explains the changes; dated records retain their original policy and results.

## Purpose And Constraints

Normal delivery sends one home upload to OCI, which forwards it to YouTube and
Twitch. The backup protects the OCI route to YouTube. An unnecessary backup
competes for the home uplink; a missed start can leave YouTube without media
until the relay recovers. Prolonged loss of evidence therefore has a bounded
start deadline. The backup cannot repair Dell, the home WAN, or Twitch.

## Evidence

| Source | What it establishes | Limit |
| --- | --- | --- |
| Bound relay input bytes and YouTube copy output bytes, video and audio packets | Complete-route media progress across two observations | Does not establish YouTube ingest selection or viewer playback |
| Fixed relay/copy cgroup absent or empty in two reads | STOPPED, a confirmed media failure | Startup races and read errors remain missing evidence |
| Copy record unchanged for more than 20 s of relay boottime | STALLED, a confirmed media failure | A changing record still needs identity and counter validation |
| Local transport ACKs, pending TCP data and FIFO state | Progress or a confirmed stall on Dell-to-OCI | Cannot establish OCI-to-YouTube progress |

**Freshness uses boottime.** Every request carries a new nonce and requires the
pinned machine and matching boot. Source and response boottimes belong to the
same relay boot. The controller adds the entire client exchange delay to their
age difference; it never subtracts different hosts' monotonic epochs. The
source bound must fit 20 s. Positive copy facts also need confirmed producer
freshness: copy-record age plus source age must fit the same bound.

**Health needs media progress.** Input connection, relay PID, copy PID/session,
boot and destination must match across samples. Typed input, output, video and
audio counters must all advance, with source and receiver gaps at most 45 s.
Freshness alone, a new timestamp, upstream ACKs or a generation change cannot
prove health. A brief missing sample retains this bounded baseline but resets
healthy time; the first resumed comparison can confirm progress, and another
consecutive sample is needed to earn healthy time.

**UTC is diagnostic.** Offset intervals and source/copy/progress UTC labels are
still inspected and recorded. Their rejection neither creates a media failure
nor prevents HEALTHY or backup retirement when boottime and media evidence
pass. A missing or invalid nonce/boot/boottime proof remains observation loss,
even if UTC looks correct. The production facts consumer has one freshness
path; it does not retry the payload through a second acceptance policy.

**Conflicting evidence has an order.** Verified complete-route progress takes
precedence over a local STOPPED export and sets `transport_evidence_conflict`.
Without that downstream proof, a fresh local transport stall still establishes
MEDIA_FAILURE. ACK advancement alone never renews downstream availability.

## Decision Table

The controller polls every 15 s.

| Observation | Assessment | Decision |
| --- | --- | --- |
| Fresh bound complete-route counters advance | HEALTHY | Never start. Renew the progress deadline. Stop backup after 300 continuous healthy seconds, including during UTC rejection. |
| Confirmed stopped/stalled process, non-live input, stalled media counters, or local stall without contradictory downstream progress | MEDIA_FAILURE | Start after 30 continuous failure seconds. |
| Missing/invalid facts, first baseline, changed identity, counter regression or an excessive comparison gap | OBSERVATION_LOST / INITIALIZING | Start once complete-route progress has been unconfirmed for 90 s. Labels and ACKs do not renew it. |
| Manual maintenance inhibition | Any | Never start. Reset failure, healthy and uncertainty windows. |
| Backup replica count cannot be read | Any | Take no action. |

`CLOCK_FAULT` is accepted only when reading older causal projections during a
rolling upgrade. UTC rejection no longer selects that assessment in the
production facts consumer.

This contract covers the pinned `OCI_ROUTE_FACTS_URL` production entry. The
older direct MediaMTX API reader remains for legacy configurations and is
outside this deployment and its isolated validation. A boottime-capable facts
producer must be installed first; roll back consumers before producers.

## Timers

| Timer | Value | Origin and reset |
| --- | --- | --- |
| Confirmed failure | 30 s | First MEDIA_FAILURE poll; a different assessment resets the window. |
| Unconfirmed downstream progress | 90 s | Last verified complete-route progress, or controller start. Maintenance resets it. There is no transition grace. |
| Healthy backup retirement | 300 s | First healthy poll after the baseline; every non-healthy poll resets it. UTC diagnostics do not reset it. |
| UTC diagnostic alert | 300 s | First UTC rejection. Gaps retain the age but cannot create a new alert without a fresh fault observation. A verified UTC observation clears it. |

`progress_deadline.unconfirmed_for_sec` is the single downstream age.
`route_progress_confirmed` records the evidence used to renew it.
`clock_diagnostic` separately reports VERIFIED, FAULT or UNAVAILABLE and
`fault_for_sec`; that duration has no input to backup decisions.

All durations use local boottime. Restarting the controller starts new timers.
The uncertainty deadline is an availability choice, not clock-skew tolerance.
Polling, Pod startup and connection setup add to the decision thresholds.

## Accepted Tradeoffs

- Facts loss can start backup while media actually flows. If neither source can
  establish downstream progress, the bounded uncertainty policy takes priority.
- Backup can run indefinitely through a long media-observation outage. The
  retirement window needs continuous verified media health, not an accumulated
  total. UTC rejection alone no longer prolongs duplicate uploads.
- A fresh counter proof can bridge short missing observations for availability;
  those missing polls still prevent retirement credit.
- A cold backup is not seamless. The dated [player study](route-failover-validation.md)
  measured a local reception gap around 48-49 s under its policy and setup.

## Adjacent Responsibilities

Arena keeps its existing secondary-recovery veto for an active cause, fallback,
unconfirmed media **or rejected/unavailable UTC diagnostics**. A healthy media
assessment during clock trouble does not grant watchdog, provider, CRA, lease
or other recovery authority. The controller can scale only the backup between
0 and 1, under the existing maintenance lock.

The existing critical notifier reports prolonged UTC diagnostic failure
independently of media health. Fresh verified UTC diagnostics recover that
incident; neither media health alone nor missing context clears it. The same
incident ID, routing, durable outbox and ordered recovery delivery remain in
use, including compatibility with older hold projections during upgrade.
A notification recovery does not establish viewer recovery.

## Evidence Status

The [boottime audit](boottime-media-health.md) records the frozen consumer,
transition/rejection checks, actual notification pipeline with fake senders,
and isolated native media tests. Current counts, production identities and
representative measured start/stop times are also recorded in current status.
The earlier [transition audit](clock-safe-media-progress.md) remains historical
evidence for the previous policy.

Representative isolated results on the frozen consumer:

| Condition | Measured outcome |
| --- | --- |
| Relay stop across FFmpeg generations | Backup start 47.5 s after fault. |
| Copy heartbeat suspended | Backup start 78.6 s after fault. |
| Facts listener lost while media flows | Conservative backup start 94.3 s after fault. |
| UTC/503 alternation without downstream proof | Backup start 94.7 s after fault. |
| Healthy media with a running backup and rejected UTC | Backup stop after 303.0 s of continuous health; local receiver kept advancing. |

Production fault injection, YouTube's backup selection, gap-free public
playback, real alert delivery and long-term false-start rate remain unverified.

## Sources

[Controller](../../ops/selfheal/fallback_controller.py) ·
[progress deadline and UTC diagnostics](../../ops/selfheal/route_assessment.py) ·
[facts producer](../../ops/selfheal/route_facts_http.py) ·
[clock protocol](../../ops/selfheal/clock_contract.py) ·
[notification adapter](../../src/stream_core/notifications/route_observation.py)
