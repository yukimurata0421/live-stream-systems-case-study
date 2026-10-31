# Upstream ACKs Versus YouTube Route Confirmation

Dated record, October 31, 2026. This page covers steps 6 and 7 of the
[revision history](fallback-policy-revisions.md). The rule they produced is
defined in the [fallback decision contract](fallback-decision-contract.md);
this page records why it changed and how the change was checked.

The subsequent [media-progress audit](clock-safe-media-progress.md) found and
corrected transition gaps in step 7. Its current rule is in the linked contract.

## The Finding

The previous revision ([relay stop across FFmpeg generations](relay-recovery-deadline.md))
let local ACK progress renew the 90-second uncertainty deadline. ACK growth from
the home sender proves delivery to the relay. It does not prove the relay's
YouTube output, and the backup exists to protect exactly that output.

A stopped copy unit, a frozen copy heartbeat or a missing copy record therefore
looked like observation loss with healthy upstream progress, and the route was
held indefinitely. A deterministic trace of the operated controller reproduced
this: `copy_facts_missing` with advancing ACKs produced no start in 600 logical
seconds. This was a policy gap, not a media-retry problem.

## The Change

- **Explicit copy failure facts.** The read-only producer reports a copy unit
  as STOPPED only when its fixed cgroup is absent or empty in two reads and the
  relay process has not changed. Permission errors, ambiguous PIDs, wrong
  identity and start races remain observation loss. A live copy process is
  reported STALLED only after its bound record stops changing for more than
  20 s. Identity includes boot, PID, session, destination and source path, so a
  restart starts a new baseline.
- **YouTube confirmation deadline.** Only complete-route health renews the
  90-second deadline. ACK growth renews a separate upstream deadline that
  applies only during a clock rejection.
- **Clock hold with alert.** A clock rejection with advancing ACKs still holds
  the route, and after 300 s it raises a critical observation alert.

The accepted cost: if only the facts listener fails while media still flows,
the backup now starts after about 90 s.

## Clocks Inside One Host

Comparing a heartbeat's UTC timestamp with the same host's UTC clock still
fails during a wall-clock step. Record changes are therefore timed with the
relay host's boottime. If a record's own counters advance while its UTC label
is old, in the future or fixed, the result is CLOCK_FAULT rather than a stopped
heartbeat.

Negative facts (STOPPED and STALLED) can still be used during a clock rejection
when a nonce-, machine- and boot-bound boottime age check succeeds, with the
whole exchange delay added. If that check also fails, the evidence stays
unknown. This exception can never promote ordinary output to healthy or
authorize recovery.

A boundary regression found one more problem before the final rollout. When a
recorder read the facts first, it consumed the producer's `new_observation`
flag, and a fixed future copy timestamp then looked like `copy_stale`. The
final consumer compares fresh same-identity output, video and audio counters
with its own previous sample instead. A copied body with no counter growth is
still not progress.

## Follow-Up: A Gap After A Clock Hold

A follow-up review traced a transition bug. After 300 s of clock rejection, a
single HTTP 503 met a YouTube deadline that had kept running during the hold,
and the backup started immediately (logical time 315 s in the deterministic
trace). Time spent holding for a clock fault was being counted as non-clock
outage time.

The correction starts a separate 90-second grace at the first non-clock gap
after a clock hold. A continuing gap does not renew it. A returning clock
rejection ends that gap, and a later gap gets its own grace. The clock-hold age
survives brief gaps while ACKs stay recent, so the alert timer is not lost.

This deliberately requires a *continuous* non-clock gap. Alternating clock
rejections and brief gaps can therefore hold the route while ACKs advance, and
the alert is the signal for that case. During a non-clock gap, a new alert can
wait until a clock response returns; a sustained gap starts the backup at its
own deadline instead. Status records `youtube_unconfirmed_total_sec` (real age
since complete-route confirmation), `youtube_unconfirmed_for_sec` (the effective
deadline age) and `youtube_grace_active`.

## Notification Path

The controller publishes the clock-hold duration as facts. Arena validates
source and receiver freshness, recent ACK evidence and boot identity, and the
existing v3 notifier raises a critical observation incident after 300 s. Its
existing routing, persisted state, retry outbox and recovery pairing are reused.
No new webhook, credential, provider or recovery authority is added.

Missing or stale context cannot clear the alert. Only fresh verified
complete-route health produces the observation-recovery message, and that
message does not claim viewer recovery. Source hashes pin both the notification
extension and its read-only reader. Existing watcher admission hooks and
disabled restart flags are preserved.

## Isolated Native Checks

Owned real FFmpeg 5.1.9 senders, read-only facts HTTP with nonce exchanges,
kernel ACK exports, the controller's actual main loop and an independent local
RTMP backup receiver ran in isolated network namespaces with bounded resources.
No production fault or provider lifecycle action was used.

The first candidate passed eleven conditions, and its duplicate-reader recheck
added two more runs. After the clock-hold follow-up, the final candidate reran
all nine earlier start conditions plus four clock-hold conditions, all on the
same frozen consumer source:

| Owned condition | Backup start from initial fault |
| --- | --- |
| Relay stop, operated retries | 47.159 s |
| Relay stop, longer same-generation control | 46.858 s |
| Relay stop, frozen export | 47.647 s |
| Relay and facts stop | 94.202 s |
| Copy stop | 47.162 s |
| Copy stop plus clock rejection | 46.986 s |
| Copy hang | 78.547 s |
| Copy hang plus clock rejection | 78.817 s |
| Copy state missing | 95.717 s |
| Facts listener stop with upstream ACKs | 94.548 s |
| Clock rejection only | No start during 135 s |
| Clock hold followed by a 20 s HTTP 503 gap | No start during 150 s |
| Clock hold followed by sustained HTTP 503 | 205.450 s |

In the two gap cases, the HTTP gap began 105 s after the clock fault. Measured
from its first missing sample, the sustained gap started the backup after
95.160 s, including poll and execution delay. The transient gap kept its
clock-hold age. Every start reached the receiver with advancing bytes.

The final set needed sixteen native attempts. One harness failure after its
facts listener was removed interrupted two other owned cases; the failed and
partial records were kept and those conditions rerun. The controller and
producer candidate did not change during that harness fix.

Related checks on the final candidate: 194 public tests / 111 subtests and 192
private tests / 107 subtests. The operated notification overlay passed 13
integration tests that replace only the external sender. They cover the
299/300 s boundary, repeats, source loss, process recreation, failed send and
retry, and recovery only on HEALTHY. No external test notification was sent.

## Operated Application

The first candidate updated the facts producer, the fallback consumer, the
read-only arena projection and the existing v3 notification loop. Real
service-UID cold reads and imports through the actual notification CLI were
checked. The notification extension was bound to that CLI's resolved package
root; an older configured bind target was not assumed to be the active reader.
The follow-up changed only the Dell controller.

Media process and container identities were preserved, ACKs advanced, primary
observations returned HEALTHY and the production backup stayed at zero.
Postimage-checked rollback preimages were kept for each step.

## Limits

These results establish isolated detection and local reception. Actual YouTube
backup selection and uninterrupted public-player video and audio remain
unverified. The earlier [player experiment](route-failover-validation.md) still
records a roughly 48-49 s local reception gap. This fix does not remove
cold-start delay, prove that a live heartbeat means provider reception, or
handle provider-wide outages. Raw and failed evidence stays in bounded private
storage.

Sources: [controller](../../ops/selfheal/fallback_controller.py),
[producer](../../ops/selfheal/route_facts_http.py),
[deadlines](../../ops/selfheal/route_assessment.py),
[notification adapter](../../src/stream_core/notifications/route_observation.py),
[main-loop tests](../../tests/test_fallback_relay_stop_loop.py) and
[notification integration tests](../../tests/test_route_observation_notify.py).
